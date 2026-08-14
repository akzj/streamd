package engine

import (
	"context"
	"github.com/akzj/streamd/internal/storage/format"
	"testing"
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
