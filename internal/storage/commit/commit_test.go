package commit

import (
	"context"
	"crypto/sha256"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/akzj/streamd/internal/storage/format"
	"github.com/akzj/streamd/internal/storage/memtable"
)

type fakeLog struct {
	mu          sync.Mutex
	appended    int
	appendCalls int
	syncs       int
	appendErr   error
	syncErr     error
}

type fakeReplica struct {
	mu             sync.Mutex
	ack            uint64
	replicateErr   error
	advanceErr     error
	replicated     int
	advanced       []uint64
	replicateBlock chan struct{}
}

func (f *fakeReplica) Replicate(ctx context.Context, entries [][]byte) (uint64, error) {
	if f.replicateBlock != nil {
		select {
		case <-f.replicateBlock:
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.replicated += len(entries)
	return f.ack, f.replicateErr
}

func (f *fakeReplica) AdvanceCommit(_ context.Context, entryID uint64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.advanced = append(f.advanced, entryID)
	return f.advanceErr
}

func (f *fakeLog) Append(entries ...[]byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.appended += len(entries)
	f.appendCalls++
	return f.appendErr
}

func (f *fakeLog) Sync() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.syncs++
	return f.syncErr
}

func encodedBatch(t *testing.T) [][]byte {
	t.Helper()
	return [][]byte{encodedEntry(t, 0, 1, 0, 0)}
}

func TestCommitDurableThenVisible(t *testing.T) {
	log := &fakeLog{}
	table := memtable.New(0)
	c := New(log, table)
	defer c.Close()
	result, err := c.Commit(context.Background(), encodedBatch(t))
	if err != nil {
		t.Fatal(err)
	}
	log.mu.Lock()
	appended := log.appended
	log.mu.Unlock()
	if result.ResultUncertain || appended != 1 {
		t.Fatalf("result %+v", result)
	}
	water := c.Watermarks()
	if !water.HasValue || water.Applied != 0 {
		t.Fatalf("watermarks %+v", water)
	}
	if tail, ok := table.Tail(1); !ok || tail.NextSequence != 1 {
		t.Fatalf("tail %+v %v", tail, ok)
	}
}

func TestSyncFailurePoisonsCommitter(t *testing.T) {
	stop := errors.New("sync failed")
	c := New(&fakeLog{syncErr: stop}, memtable.New(0))
	defer c.Close()
	result, err := c.Commit(context.Background(), encodedBatch(t))
	if !errors.Is(err, stop) || !result.ResultUncertain {
		t.Fatalf("result %+v error %v", result, err)
	}
	water := c.Watermarks()
	if water.HasLocalDurable || water.HasCommitted || water.HasApplied {
		t.Fatalf("failed Sync advanced durable watermarks: %+v", water)
	}
	if _, err = c.Commit(context.Background(), encodedBatch(t)); !errors.Is(err, stop) {
		t.Fatalf("poisoned Committer error = %v", err)
	}
}

func TestAppendFailurePoisonsCommitter(t *testing.T) {
	stop := errors.New("append failed")
	c := New(&fakeLog{appendErr: stop}, memtable.New(0))
	defer c.Close()
	result, err := c.Commit(context.Background(), encodedBatch(t))
	if !errors.Is(err, stop) || !result.ResultUncertain {
		t.Fatalf("result %+v error %v", result, err)
	}
	if _, err = c.Commit(context.Background(), encodedBatch(t)); !errors.Is(err, stop) {
		t.Fatalf("poisoned Committer error = %v", err)
	}
}

func TestStrictCommitWaitsForReplicaDurability(t *testing.T) {
	release := make(chan struct{})
	replica := &fakeReplica{ack: 0, replicateBlock: release}
	table := memtable.New(0)
	c := NewWithOptions(&fakeLog{}, table, Options{Replica: replica, ReplicaTimeout: time.Second})
	defer c.Close()
	future, err := c.Enqueue(encodedBatch(t))
	if err != nil {
		t.Fatal(err)
	}
	select {
	case completed := <-future.completion:
		t.Fatalf("commit completed before replica durability: %+v", completed)
	case <-time.After(20 * time.Millisecond):
	}
	if _, ok := table.Tail(1); ok {
		t.Fatal("Entry became visible before replica durability")
	}
	water := c.Watermarks()
	if !water.HasLocalDurable || water.HasReplicated || water.HasCommitted || water.HasApplied {
		t.Fatalf("watermarks before ACK = %+v", water)
	}
	close(release)
	if _, err = future.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	water = c.Watermarks()
	if !water.HasReplicated || water.Replicated != 0 || !water.HasCommitted || !water.HasApplied {
		t.Fatalf("watermarks after ACK = %+v", water)
	}
	replica.mu.Lock()
	defer replica.mu.Unlock()
	if len(replica.advanced) != 1 || replica.advanced[0] != 0 {
		t.Fatalf("commit advances = %v", replica.advanced)
	}
}

func TestStrictReplicaFailureDoesNotCommit(t *testing.T) {
	stop := errors.New("standby unavailable")
	c := NewWithOptions(&fakeLog{}, memtable.New(0), Options{Replica: &fakeReplica{replicateErr: stop}})
	defer c.Close()
	result, err := c.Commit(context.Background(), encodedBatch(t))
	if !errors.Is(err, stop) || !result.ResultUncertain {
		t.Fatalf("result %+v, error %v", result, err)
	}
	water := c.Watermarks()
	if !water.HasLocalDurable || water.HasReplicated || water.HasCommitted || water.HasApplied {
		t.Fatalf("failed replication watermarks = %+v", water)
	}
}

func TestStrictRejectsMismatchedAck(t *testing.T) {
	c := NewWithOptions(&fakeLog{}, memtable.New(0), Options{Replica: &fakeReplica{ack: 9}})
	defer c.Close()
	result, err := c.Commit(context.Background(), encodedBatch(t))
	if err == nil || !result.ResultUncertain {
		t.Fatalf("result %+v, error %v", result, err)
	}
	if c.Watermarks().HasCommitted {
		t.Fatal("mismatched ACK committed the group")
	}
}

func TestStrictCommitAdvanceFailureDoesNotRevokeCommit(t *testing.T) {
	stop := errors.New("commit advance lost")
	replica := &fakeReplica{ack: 0, advanceErr: stop}
	c := NewWithOptions(&fakeLog{}, memtable.New(0), Options{Replica: replica})
	defer c.Close()
	if _, err := c.Commit(context.Background(), encodedBatch(t)); err != nil {
		t.Fatalf("committed write failed after lost CommitAdvance: %v", err)
	}
	if fatal := c.FatalError(); fatal != nil {
		t.Fatalf("CommitAdvance poisoned Committer: %v", fatal)
	}
}

func TestGroupCommitCombinesDifferentStreams(t *testing.T) {
	log := &fakeLog{}
	table := memtable.New(0)
	c := NewWithOptions(log, table, Options{MaxDelay: 20 * time.Millisecond, MaxRequests: 8, MaxBytes: 1 << 20, QueueCapacity: 8})
	defer c.Close()
	first := encodedEntry(t, 0, 1, 0, 0)
	decoded, err := format.UnmarshalWALEntry(first)
	if err != nil {
		t.Fatal(err)
	}
	second := encodedEntry(t, 1, 2, 0, decoded.CRC32C)
	firstFuture, err := c.Enqueue([][]byte{first})
	if err != nil {
		t.Fatal(err)
	}
	secondFuture, err := c.Enqueue([][]byte{second})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = firstFuture.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err = secondFuture.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	log.mu.Lock()
	appendCalls, syncs := log.appendCalls, log.syncs
	log.mu.Unlock()
	if appendCalls != 1 || syncs != 1 {
		t.Fatalf("append calls = %d, syncs = %d", appendCalls, syncs)
	}
	if tail, ok := table.Tail(2); !ok || tail.LastEntryID != 1 {
		t.Fatalf("stream 2 tail = %+v, ok = %v", tail, ok)
	}
}

func TestGroupCommitSeparatesSameStream(t *testing.T) {
	log := &fakeLog{}
	c := NewWithOptions(log, memtable.New(0), Options{MaxDelay: 5 * time.Millisecond, MaxRequests: 8, MaxBytes: 1 << 20, QueueCapacity: 8})
	defer c.Close()
	first := encodedEntry(t, 0, 1, 0, 0)
	decoded, err := format.UnmarshalWALEntry(first)
	if err != nil {
		t.Fatal(err)
	}
	second := encodedEntryAt(t, 1, 1, 1, uint64(len(decoded.Frame)), decoded.CRC32C)
	a, err := c.Enqueue([][]byte{first})
	if err != nil {
		t.Fatal(err)
	}
	b, err := c.Enqueue([][]byte{second})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = a.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err = b.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	log.mu.Lock()
	syncs := log.syncs
	log.mu.Unlock()
	if syncs != 2 {
		t.Fatalf("syncs = %d", syncs)
	}
}

func encodedEntry(t *testing.T, entryID, streamID, sequence uint64, previous uint32) []byte {
	return encodedEntryAt(t, entryID, streamID, sequence, 0, previous)
}

func encodedEntryAt(t *testing.T, entryID, streamID, sequence, offset uint64, previous uint32) []byte {
	t.Helper()
	hash := sha256.Sum256([]byte{byte(entryID), byte(streamID)})
	frame, err := format.MarshalRecordFrame(format.RecordFrame{EntryID: entryID, StreamID: streamID, Sequence: sequence, ByteOffset: offset, RecordedAt: int64(entryID), BatchCount: 1, RequestHash: hash, Producer: "commit-test"})
	if err != nil {
		t.Fatal(err)
	}
	entry, err := format.MarshalWALEntry(0, previous, frame)
	if err != nil {
		t.Fatal(err)
	}
	return entry
}
