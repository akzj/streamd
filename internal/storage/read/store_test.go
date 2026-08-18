package read

import (
	"crypto/sha256"
	"github.com/akzj/streamd/internal/storage/format"
	"github.com/akzj/streamd/internal/storage/memtable"
	"github.com/akzj/streamd/internal/storage/segment"
	"path/filepath"
	"testing"
)

func TestReadInspectResolveAcrossSegment(t *testing.T) {
	build := memtable.New(0)
	hash := sha256.Sum256([]byte("r"))
	var entries []format.WALEntry
	offset := uint64(0)
	previous := uint32(0)
	for i := 0; i < 3; i++ {
		frame, err := format.MarshalRecordFrame(format.RecordFrame{EntryID: uint64(i), StreamID: 1, Sequence: uint64(i), ByteOffset: offset, RecordedAt: int64(10 + i*10), BatchIndex: uint32(i), BatchCount: 3, RequestHash: hash, Producer: "p", Payload: []byte{byte(i)}})
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
	if err := build.ApplyBatch(entries); err != nil {
		t.Fatal(err)
	}
	var id format.UUID
	id[15] = 1
	path := filepath.Join(t.TempDir(), "s.seg")
	meta, err := segment.WriteFile(path, id, 1, build.FreezeSnapshot())
	if err != nil {
		t.Fatal(err)
	}
	active := memtable.New(0)
	d := meta.Directories[0]
	if err = active.SeedTail(1, memtable.Tail{NextSequence: d.FirstSequence + d.RecordCount, NextByteOffset: d.NextByteOffset, LastRecordedAt: d.LastRecordedAt, LastEntryID: d.LastEntryID, RecordCount: d.RecordCount}); err != nil {
		t.Fatal(err)
	}
	reference := format.SegmentReference{Flags: format.SegmentRefHasLocal, SegmentID: meta.Header.SegmentID, FileSize: meta.Footer.FileLength, FirstEntryID: meta.Header.FirstEntryID, LastEntryID: meta.Header.LastEntryID, StreamCount: meta.Header.StreamCount, RecordCount: meta.Header.RecordCount, LocalPath: filepath.Base(path), ContentSHA256: meta.Footer.ContentSHA256}
	descriptor, err := segment.DescribeReference(filepath.Dir(path), reference)
	if err != nil {
		t.Fatal(err)
	}
	store := New(active, nil, filepath.Dir(path), 1, []segment.Descriptor{descriptor}, nil, 1, 1)
	defer store.Close()
	result, err := store.Read(1, 1, 10, 0)
	if err != nil || len(result.Records) != 2 || result.NextSequence != 3 {
		t.Fatalf("result %+v %v", result, err)
	}
	info, err := store.Inspect(1)
	if err != nil || !info.Exists || info.RecordCount != 3 || info.FirstRecordedAt != 10 {
		t.Fatalf("info %+v %v", info, err)
	}
	sequence, actual, found, err := store.ResolveTime(1, 15, AtOrAfter)
	if err != nil || !found || sequence != 1 || actual != 20 {
		t.Fatalf("resolve %d %d %v %v", sequence, actual, found, err)
	}
	sequence, actual, found, err = store.ResolveTime(1, 25, AtOrBefore)
	if err != nil || !found || sequence != 1 || actual != 20 {
		t.Fatalf("resolve before %d %d %v %v", sequence, actual, found, err)
	}
	store.ClearCache()
}
