package engine

import (
	"context"
	"errors"
	"github.com/akzj/streamd/internal/storage/errdefs"
	"github.com/akzj/streamd/internal/storage/format"
	"testing"
	"time"
)

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
