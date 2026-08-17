package engine

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/akzj/streamd/internal/storage/errdefs"
	"github.com/akzj/streamd/internal/storage/format"
)

type engineGuard struct {
	err error
}

func (g *engineGuard) CanCommit() error { return g.err }

type engineReplica struct {
	terms []uint64
}

func (r *engineReplica) Replicate(_ context.Context, encoded [][]byte) (uint64, error) {
	var last uint64
	for _, value := range encoded {
		entry, err := format.UnmarshalWALEntry(value)
		if err != nil {
			return 0, err
		}
		r.terms = append(r.terms, entry.Term)
		last = entry.EntryID
	}
	return last, nil
}

func (*engineReplica) AdvanceCommit(context.Context, uint64) error { return nil }

func TestAppendBatchDeduplicateAndRestart(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	request := AppendRequest{Namespace: "agent", Stream: "events", ExpectedSequence: 0, RequestID: []byte("request-1"), Producer: "test", Records: []InputRecord{{Payload: []byte("one")}, {Payload: []byte("two")}}}
	result, err := store.Append(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if result.FirstSequence != 0 || result.NextSequence != 2 || result.RecordCount != 2 || result.Deduplicated {
		t.Fatalf("result %+v", result)
	}
	duplicate, err := store.Append(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if !duplicate.Deduplicated || duplicate.FirstEntryID != result.FirstEntryID {
		t.Fatalf("duplicate %+v", duplicate)
	}
	conflict := request
	conflict.Records = []InputRecord{{Payload: []byte("changed")}, {Payload: []byte("two")}}
	if _, err = store.Append(context.Background(), conflict); err == nil {
		t.Fatal("conflicting retry accepted")
	}
	ahead := request
	ahead.ExpectedSequence = 3
	ahead.RequestID = []byte("ahead")
	if _, err = store.Append(context.Background(), ahead); err == nil {
		t.Fatal("ahead request accepted")
	}
	read, err := store.Read("agent", "events", 0, 10, 0)
	if err != nil || len(read.Records) != 2 {
		t.Fatalf("read %+v %v", read, err)
	}
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	next := AppendRequest{Namespace: "agent", Stream: "events", ExpectedSequence: 2, RequestID: []byte("request-2"), Producer: "test", Records: []InputRecord{{Payload: []byte("three")}}}
	result, err = store.Append(context.Background(), next)
	if err != nil || result.FirstSequence != 2 {
		t.Fatalf("restart append %+v %v", result, err)
	}
}

func TestReplicatedEngineUsesTermGuardAndStrictWatermarks(t *testing.T) {
	identity := format.NodeIdentity{ClusterID: engineID(1), GroupID: engineID(2), NodeID: engineID(3), CreatedAt: 1}
	guard := &engineGuard{}
	replica := &engineReplica{}
	store, err := OpenReplicated(t.TempDir(), identity, ReplicationOptions{Term: 7, Role: format.ReplicationRolePrimary, Durability: format.ReplicationDurabilityStrict, Replica: replica, Guard: guard})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	_, err = store.Append(context.Background(), AppendRequest{Namespace: "strict", Stream: "events", RequestID: []byte("one"), Producer: "test", Records: []InputRecord{{Payload: []byte("value")}}})
	if err != nil {
		t.Fatal(err)
	}
	if len(replica.terms) != 2 || replica.terms[0] != 7 || replica.terms[1] != 7 {
		t.Fatalf("replicated Terms = %v", replica.terms)
	}
	health := store.Health()
	if health.Role != format.ReplicationRolePrimary || health.Durability != format.ReplicationDurabilityStrict || health.Term != 7 || !health.Watermarks.HasReplicated || health.Watermarks.Replicated != health.Watermarks.Committed {
		t.Fatalf("Health = %+v", health)
	}
	guard.err = errors.New("lease expired")
	_, err = store.Append(context.Background(), AppendRequest{Namespace: "strict", Stream: "events", ExpectedSequence: 1, RequestID: []byte("two"), Producer: "test", Records: []InputRecord{{}}})
	if !errors.Is(err, errdefs.ErrNotLeader) {
		t.Fatalf("expired Lease error = %v", err)
	}
}

func engineID(value byte) format.UUID {
	var id format.UUID
	id[15] = value
	return id
}

func TestConcurrentAppendsAcrossStreams(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	const streams = 24
	for i := 0; i < streams; i++ {
		name := fmt.Sprintf("stream-%02d", i)
		if _, err = store.Append(context.Background(), AppendRequest{Namespace: "concurrent", Stream: name, RequestID: []byte("initial"), Producer: "test", Records: []InputRecord{{Payload: []byte("zero")}}}); err != nil {
			t.Fatal(err)
		}
	}
	start := make(chan struct{})
	errorsByStream := make(chan error, streams)
	var wait sync.WaitGroup
	for i := 0; i < streams; i++ {
		wait.Add(1)
		go func(i int) {
			defer wait.Done()
			<-start
			name := fmt.Sprintf("stream-%02d", i)
			_, appendErr := store.Append(context.Background(), AppendRequest{Namespace: "concurrent", Stream: name, ExpectedSequence: 1, RequestID: []byte("second"), Producer: "test", Records: []InputRecord{{Payload: []byte("one")}}})
			errorsByStream <- appendErr
		}(i)
	}
	close(start)
	wait.Wait()
	close(errorsByStream)
	for appendErr := range errorsByStream {
		if appendErr != nil {
			t.Fatal(appendErr)
		}
	}
	for i := 0; i < streams; i++ {
		name := fmt.Sprintf("stream-%02d", i)
		result, readErr := store.Read("concurrent", name, 0, 10, 0)
		if readErr != nil || len(result.Records) != 2 || result.Records[1].Sequence != 1 {
			t.Fatalf("%s Read = %+v, error = %v", name, result, readErr)
		}
	}
}

func TestConcurrentSameSequenceHasSingleWinner(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, err = store.Append(context.Background(), AppendRequest{Namespace: "n", Stream: "s", RequestID: []byte("initial"), Producer: "test", Records: []InputRecord{{Payload: []byte("zero")}}}); err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	results := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func(i int) {
			<-start
			_, appendErr := store.Append(context.Background(), AppendRequest{Namespace: "n", Stream: "s", ExpectedSequence: 1, RequestID: []byte(fmt.Sprintf("request-%d", i)), Producer: "test", Records: []InputRecord{{Payload: []byte{byte(i)}}}})
			results <- appendErr
		}(i)
	}
	close(start)
	var successes, conflicts int
	for i := 0; i < 2; i++ {
		appendErr := <-results
		if appendErr == nil {
			successes++
		} else if errors.Is(appendErr, errdefs.ErrSequenceConflict) {
			conflicts++
		} else {
			t.Fatalf("unexpected Append error = %v", appendErr)
		}
	}
	if successes != 1 || conflicts != 1 {
		t.Fatalf("successes = %d, conflicts = %d", successes, conflicts)
	}
}

func TestWaitForAppendDoesNotLoseNotifications(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	woke := make(chan error, 1)
	go func() { woke <- store.WaitForAppend(context.Background(), "n", "s", 0) }()
	request := AppendRequest{Namespace: "n", Stream: "s", RequestID: []byte("r"), Producer: "test", Records: []InputRecord{{Payload: []byte("record")}}}
	if _, err = store.Append(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	select {
	case err = <-woke:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("WaitForAppend did not observe committed Append")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err = store.WaitForAppend(ctx, "n", "s", 0); err != nil {
		t.Fatalf("already-visible Append was missed: %v", err)
	}
}

func TestCloseUnblocksAppendWaiters(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	woke := make(chan error, 1)
	go func() { woke <- store.WaitForAppend(context.Background(), "n", "s", 0) }()
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err = <-woke:
		if !errors.Is(err, errdefs.ErrClosed) {
			t.Fatalf("wait error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not unblock WaitForAppend")
	}
}
func TestRequestHashCanonicalHeaderOrder(t *testing.T) {
	a := AppendRequest{Namespace: "n", Stream: "s", RequestID: []byte("r"), Records: []InputRecord{{Headers: []format.Header{{Key: "z", Value: []byte("1")}, {Key: "a", Value: []byte("2")}}}}}
	b := a
	b.Records = []InputRecord{{Headers: []format.Header{{Key: "a", Value: []byte("2")}, {Key: "z", Value: []byte("1")}}}}
	hashA, err := RequestHash(a)
	if err != nil {
		t.Fatal(err)
	}
	hashB, err := RequestHash(b)
	if err != nil {
		t.Fatal(err)
	}
	if hashA != hashB {
		t.Fatal("stable request hashes differ")
	}
}

func TestCheckpointPublishesSegmentRotatesWALAndRestarts(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	request := AppendRequest{Namespace: "agent", Stream: "events", RequestID: []byte("first"), Producer: "test", Records: []InputRecord{{Payload: []byte("one")}, {Payload: []byte("two")}}}
	if _, err = store.Append(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	manifest, created, err := store.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	if !created || manifest.Header.RecordCount != 3 || len(manifest.SegmentReferences) != 1 {
		t.Fatalf("checkpoint Manifest = %+v, created = %v", manifest, created)
	}
	if _, created, err = store.Checkpoint(); err != nil || created {
		t.Fatalf("empty checkpoint created=%v error=%v", created, err)
	}
	read, err := store.Read("agent", "events", 0, 10, 0)
	if err != nil || len(read.Records) != 2 {
		t.Fatalf("post-checkpoint Read = %+v, %v", read, err)
	}
	second := AppendRequest{Namespace: "agent", Stream: "events", ExpectedSequence: 2, RequestID: []byte("second"), Producer: "test", Records: []InputRecord{{Payload: []byte("three")}}}
	if _, err = store.Append(context.Background(), second); err != nil {
		t.Fatal(err)
	}
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	read, err = store.Read("agent", "events", 0, 10, 0)
	if err != nil || len(read.Records) != 3 || string(read.Records[2].Payload) != "three" {
		t.Fatalf("restart Read = %+v, %v", read, err)
	}
}

func TestCompactSwitchesGenerationBeforeRetiringInputs(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	appendAndCheckpoint := func(expected uint64, requestID, payload string) format.Manifest {
		t.Helper()
		_, appendErr := store.Append(context.Background(), AppendRequest{Namespace: "agent", Stream: "events", ExpectedSequence: expected, RequestID: []byte(requestID), Producer: "test", Records: []InputRecord{{Payload: []byte(payload)}}})
		if appendErr != nil {
			t.Fatal(appendErr)
		}
		manifest, created, checkpointErr := store.Checkpoint()
		if checkpointErr != nil || !created {
			t.Fatalf("Checkpoint created=%v error=%v", created, checkpointErr)
		}
		return manifest
	}
	appendAndCheckpoint(0, "first", "one")
	before := appendAndCheckpoint(1, "second", "two")
	if len(before.SegmentReferences) != 2 {
		t.Fatalf("Segment count before Compact = %d", len(before.SegmentReferences))
	}

	stopReads := make(chan struct{})
	readErrors := make(chan error, 1)
	go func() {
		for {
			select {
			case <-stopReads:
				readErrors <- nil
				return
			default:
				result, readErr := store.Read("agent", "events", 0, 10, 0)
				if readErr != nil || len(result.Records) != 2 {
					readErrors <- fmt.Errorf("concurrent Read records=%d: %w", len(result.Records), readErr)
					return
				}
			}
		}
	}()
	compacted, err := store.Compact(CompactionOptions{MinSegments: 2, MaxInputSegments: 4, MaxInputBytes: 64 << 20})
	close(stopReads)
	if readErr := <-readErrors; readErr != nil {
		t.Fatal(readErr)
	}
	if err != nil {
		t.Fatal(err)
	}
	if !compacted.Created || compacted.InputSegments != 2 || compacted.Manifest.Header.Generation != before.Header.Generation+1 || len(compacted.Manifest.SegmentReferences) != 1 {
		t.Fatalf("Compaction = %+v", compacted)
	}
	result, err := store.Read("agent", "events", 0, 10, 0)
	if err != nil || len(result.Records) != 2 || string(result.Records[1].Payload) != "two" {
		t.Fatalf("post-Compact Read = %+v, %v", result, err)
	}
	for _, reference := range before.SegmentReferences {
		if _, statErr := os.Stat(filepath.Join(dir, reference.LocalPath)); !os.IsNotExist(statErr) {
			t.Fatalf("retired input Segment remains live: %v", statErr)
		}
	}
}

func TestPinnedManifestDefersCompactionRetirement(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	for i, payload := range []string{"one", "two"} {
		_, err = store.Append(context.Background(), AppendRequest{Namespace: "agent", Stream: "events", ExpectedSequence: uint64(i), RequestID: []byte(payload), Producer: "test", Records: []InputRecord{{Payload: []byte(payload)}}})
		if err != nil {
			t.Fatal(err)
		}
		if _, created, checkpointErr := store.Checkpoint(); checkpointErr != nil || !created {
			t.Fatalf("Checkpoint created=%v error=%v", created, checkpointErr)
		}
	}
	pinned, _, release, err := store.CheckpointAndPin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.Compact(CompactionOptions{MinSegments: 2, MaxInputSegments: 4, MaxInputBytes: 64 << 20}); err != nil {
		t.Fatal(err)
	}
	for _, reference := range pinned.SegmentReferences {
		if _, err = os.Stat(filepath.Join(dir, reference.LocalPath)); err != nil {
			t.Fatalf("pinned Segment retired: %v", err)
		}
	}
	release()
	for _, reference := range pinned.SegmentReferences {
		if _, err = os.Stat(filepath.Join(dir, reference.LocalPath)); !os.IsNotExist(err) {
			t.Fatalf("released Segment remains live: %v", err)
		}
	}
}
