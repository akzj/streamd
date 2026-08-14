package memtable

import (
	"crypto/sha256"
	"github.com/akzj/streamd/internal/storage/format"
	"testing"
)

func batch(t *testing.T, startEntry, startSequence, startOffset uint64, count int) []format.WALEntry {
	t.Helper()
	hash := sha256.Sum256([]byte("request"))
	out := make([]format.WALEntry, 0, count)
	previous := uint32(0)
	offset := startOffset
	for i := 0; i < count; i++ {
		frame, err := format.MarshalRecordFrame(format.RecordFrame{EntryID: startEntry + uint64(i), StreamID: 1, Sequence: startSequence + uint64(i), ByteOffset: offset, RecordedAt: int64(10 + i), BatchIndex: uint32(i), BatchCount: uint32(count), RequestHash: hash, RequestID: []byte("r"), Producer: "p", Payload: []byte{byte(i)}})
		if err != nil {
			t.Fatal(err)
		}
		encoded, err := format.MarshalWALEntry(0, previous, frame)
		if err != nil {
			t.Fatal(err)
		}
		entry, err := format.UnmarshalWALEntry(encoded)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, entry)
		previous = entry.CRC32C
		offset += uint64(len(frame))
	}
	return out
}
func TestApplyBatchPublishesAtomically(t *testing.T) {
	table := New(128)
	entries := batch(t, 0, 0, 0, 2)
	if err := table.ApplyBatch(entries); err != nil {
		t.Fatal(err)
	}
	tail, ok := table.Tail(1)
	if !ok || tail.NextSequence != 2 || tail.RecordCount != 2 {
		t.Fatalf("tail %+v %v", tail, ok)
	}
	records, next, err := table.Read(1, 0, 10)
	if err != nil || len(records) != 2 || next != 2 {
		t.Fatalf("read %d %d %v", len(records), next, err)
	}
	bad := batch(t, 2, 3, tail.NextByteOffset, 1)
	if err = table.ApplyBatch(bad); err == nil {
		t.Fatal("gap accepted")
	}
	tail2, _ := table.Tail(1)
	if tail2 != tail {
		t.Fatalf("failed Batch changed tail: %+v", tail2)
	}
}
func TestBatchValidationAndFreeze(t *testing.T) {
	table := New(0)
	entries := batch(t, 0, 0, 0, 2)
	entries[1].BatchIndex = 0
	if err := table.ApplyBatch(entries); err == nil {
		t.Fatal("invalid Batch accepted")
	}
	table.Freeze()
	if err := table.ApplyBatch(batch(t, 0, 0, 0, 1)); err == nil {
		t.Fatal("frozen Table accepted Apply")
	}
}
