package segment

import (
	"crypto/sha256"
	"github.com/akzj/streamd/internal/storage/format"
	"github.com/akzj/streamd/internal/storage/memtable"
	"path/filepath"
	"testing"
)

func TestWriteOpenAndRead(t *testing.T) {
	table := memtable.New(0)
	hash := sha256.Sum256([]byte("r"))
	entries := make([]format.WALEntry, 0, 2)
	offset := uint64(0)
	previous := uint32(0)
	for i := 0; i < 2; i++ {
		frame, err := format.MarshalRecordFrame(format.RecordFrame{EntryID: uint64(i), StreamID: 3, Sequence: uint64(i), ByteOffset: offset, RecordedAt: int64(10 + i), BatchIndex: uint32(i), BatchCount: 2, RequestHash: hash, RequestID: []byte("r"), Producer: "p", Payload: []byte{byte(i)}})
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
		entries = append(entries, entry)
		previous = entry.CRC32C
		offset += uint64(len(frame))
	}
	if err := table.ApplyBatch(entries); err != nil {
		t.Fatal(err)
	}
	var id format.UUID
	id[15] = 1
	path := filepath.Join(t.TempDir(), "segment.seg")
	meta, err := WriteFile(path, id, 100, table.FreezeSnapshot())
	if err != nil {
		t.Fatal(err)
	}
	if meta.Header.RecordCount != 2 {
		t.Fatalf("metadata %+v", meta)
	}
	reader, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	record, err := reader.Read(3, 1)
	if err != nil {
		t.Fatal(err)
	}
	if record.Sequence != 1 || len(record.Payload) != 1 || record.Payload[0] != 1 {
		t.Fatalf("record %+v", record)
	}
}
