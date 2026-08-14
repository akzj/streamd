package recovery

import (
	"crypto/sha256"
	"path/filepath"
	"testing"
	"time"

	"github.com/akzj/streamd/internal/storage/format"
	"github.com/akzj/streamd/internal/storage/fsutil"
	"github.com/akzj/streamd/internal/storage/memtable"
	"github.com/akzj/streamd/internal/storage/segment"
)

func TestBootstrapAndReplaySingleSyncWAL(t *testing.T) {
	root, err := fsutil.OpenRoot(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	state, err := Open(root.Path())
	if err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256([]byte("r"))
	frame, err := format.MarshalRecordFrame(format.RecordFrame{EntryID: 0, StreamID: 1, Sequence: 0, RecordedAt: 1, BatchCount: 1, RequestHash: hash, Producer: "p", Payload: []byte("value")})
	if err != nil {
		t.Fatal(err)
	}
	entry, err := format.MarshalWALEntry(0, 0, frame)
	if err != nil {
		t.Fatal(err)
	}
	if err = state.WAL.Append(entry); err != nil {
		t.Fatal(err)
	}
	if err = state.WAL.Sync(); err != nil {
		t.Fatal(err)
	}
	if err = state.Close(); err != nil {
		t.Fatal(err)
	}
	recovered, err := Open(root.Path())
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close()
	tail, ok := recovered.MemTable.Tail(1)
	if !ok || tail.NextSequence != 1 || !recovered.HasApplied || recovered.AppliedEntryID != 0 {
		t.Fatalf("tail %+v applied %d %v", tail, recovered.AppliedEntryID, recovered.HasApplied)
	}
}

func TestRecoveryAcceptsWALOverlappingManifestCheckpoint(t *testing.T) {
	root, err := fsutil.OpenRoot(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	state, err := Open(root.Path())
	if err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256([]byte("r"))
	frame, err := format.MarshalRecordFrame(format.RecordFrame{EntryID: 0, StreamID: 2, Sequence: 0, RecordedAt: 1, BatchCount: 1, RequestHash: hash, Producer: "p"})
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := format.MarshalWALEntry(0, 0, frame)
	if err != nil {
		t.Fatal(err)
	}
	entry, err := format.UnmarshalWALEntry(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if err = state.WAL.Append(encoded); err != nil {
		t.Fatal(err)
	}
	if err = state.WAL.Sync(); err != nil {
		t.Fatal(err)
	}
	table := memtable.New(0)
	if err = table.ApplyBatch([]format.WALEntry{entry}); err != nil {
		t.Fatal(err)
	}
	var segmentID, manifestID format.UUID
	segmentID[15] = 1
	manifestID[15] = 2
	meta, err := segment.WriteFile(filepath.Join(root.Path(), "segments", "s.seg"), segmentID, 1, table.FreezeSnapshot())
	if err != nil {
		t.Fatal(err)
	}
	_, err = state.Manifest.Publish(format.Manifest{Header: format.ManifestHeader{FileID: manifestID, LastEntryID: 0, LastEntryCRC32C: entry.CRC32C, RecordCount: 1}, SegmentReferences: []format.SegmentReference{{Flags: format.SegmentRefHasLocal, SegmentID: segmentID, FileSize: meta.Footer.FileLength, FirstEntryID: 0, LastEntryID: 0, StreamCount: 1, RecordCount: 1, LocalPath: "segments/s.seg", ContentSHA256: meta.Footer.ContentSHA256}}})
	if err != nil {
		t.Fatal(err)
	}
	if err = state.Close(); err != nil {
		t.Fatal(err)
	}
	recovered, err := Open(root.Path())
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close()
	tail, ok := recovered.MemTable.Tail(2)
	if !ok || tail.NextSequence != 1 {
		t.Fatalf("tail %+v %v", tail, ok)
	}
}

func TestRecoveryReplaysSealedAndActiveWALChain(t *testing.T) {
	root, err := fsutil.OpenRoot(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	state, err := Open(root.Path())
	if err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256([]byte("r"))
	makeEntry := func(id, sequence, offset uint64, previous uint32) ([]byte, uint64, uint32) {
		frame, e := format.MarshalRecordFrame(format.RecordFrame{EntryID: id, StreamID: 3, Sequence: sequence, ByteOffset: offset, RecordedAt: int64(id + 1), BatchCount: 1, RequestHash: hash, Producer: "p"})
		if e != nil {
			t.Fatal(e)
		}
		encoded, e := format.MarshalWALEntry(0, previous, frame)
		if e != nil {
			t.Fatal(e)
		}
		entry, e := format.UnmarshalWALEntry(encoded)
		if e != nil {
			t.Fatal(e)
		}
		return encoded, uint64(len(frame)), entry.CRC32C
	}
	first, size, crc := makeEntry(0, 0, 0, 0)
	if err = state.WAL.Append(first); err != nil {
		t.Fatal(err)
	}
	if err = state.WAL.Rotate(0, time.Now()); err != nil {
		t.Fatal(err)
	}
	second, _, _ := makeEntry(1, 1, size, crc)
	if err = state.WAL.Append(second); err != nil {
		t.Fatal(err)
	}
	if err = state.WAL.Sync(); err != nil {
		t.Fatal(err)
	}
	state.Close()
	recovered, err := Open(root.Path())
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close()
	tail, ok := recovered.MemTable.Tail(3)
	if !ok || tail.NextSequence != 2 || recovered.AppliedEntryID != 1 {
		t.Fatalf("tail %+v applied %d", tail, recovered.AppliedEntryID)
	}
}
