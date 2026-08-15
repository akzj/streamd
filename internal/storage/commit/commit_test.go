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
