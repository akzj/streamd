package lifecycle

import (
	"crypto/sha256"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/akzj/streamd/internal/storage/format"
	"github.com/akzj/streamd/internal/storage/fsutil"
	manifeststore "github.com/akzj/streamd/internal/storage/manifest"
	"github.com/akzj/streamd/internal/storage/memtable"
	"github.com/akzj/streamd/internal/storage/segment"
)

func TestPublishFlushMergeAndDeferredRetirement(t *testing.T) {
	root, err := fsutil.OpenRoot(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	manifests, err := manifeststore.Open(root.Path())
	if err != nil {
		t.Fatal(err)
	}
	manager := New(root.Path(), manifests)

	firstFrame := recordFrame(t, 0, 7, 0, 0, 10, "first")
	first, err := manager.PublishFlush([]memtable.StreamSnapshot{snapshot(t, firstFrame)}, 0, 11)
	if err != nil {
		t.Fatal(err)
	}
	secondFrame := recordFrame(t, 1, 7, 1, uint64(len(firstFrame)), 11, "second")
	second, err := manager.PublishFlush([]memtable.StreamSnapshot{snapshot(t, secondFrame)}, 1, 22)
	if err != nil {
		t.Fatal(err)
	}
	if second.Header.Generation != 1 || second.Header.RecordCount != 2 || len(second.SegmentReferences) != 2 {
		t.Fatalf("second Manifest = %+v", second)
	}

	firstRef := findReference(t, second, first.SegmentReferences[0].SegmentID)
	secondRef := otherReference(t, second, firstRef.SegmentID)
	release := manager.Pin(firstRef.SegmentID)
	merged, err := manager.Merge([]format.UUID{firstRef.SegmentID, secondRef.SegmentID})
	if err != nil {
		t.Fatal(err)
	}
	if merged.Header.Generation != 2 || merged.Header.RecordCount != 2 || len(merged.SegmentReferences) != 1 {
		t.Fatalf("merged Manifest = %+v", merged)
	}
	reader, err := segment.Open(filepath.Join(root.Path(), merged.SegmentReferences[0].LocalPath))
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	for sequence, payload := range []string{"first", "second"} {
		record, readErr := reader.Read(7, uint64(sequence))
		if readErr != nil {
			t.Fatal(readErr)
		}
		if string(record.Payload) != payload {
			t.Fatalf("sequence %d payload = %q", sequence, record.Payload)
		}
	}

	firstPath := filepath.Join(root.Path(), firstRef.LocalPath)
	secondPath := filepath.Join(root.Path(), secondRef.LocalPath)
	if _, err = os.Stat(firstPath); err != nil {
		t.Fatalf("pinned Segment was retired: %v", err)
	}
	if _, err = os.Stat(secondPath); !os.IsNotExist(err) {
		t.Fatalf("unpinned Segment still live: %v", err)
	}
	if got := trashFiles(t, root.Path()); len(got) != 1 {
		t.Fatalf("trash after Merge = %v", got)
	}

	release()
	if _, err = os.Stat(firstPath); !os.IsNotExist(err) {
		t.Fatalf("released Segment still live: %v", err)
	}
	if got := trashFiles(t, root.Path()); len(got) != 2 {
		t.Fatalf("trash after release = %v", got)
	}
}

func TestMergeRejectsDuplicateInputs(t *testing.T) {
	root, err := fsutil.OpenRoot(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	manifests, err := manifeststore.Open(root.Path())
	if err != nil {
		t.Fatal(err)
	}
	manager := New(root.Path(), manifests)
	frame := recordFrame(t, 0, 1, 0, 0, 1, "record")
	published, err := manager.PublishFlush([]memtable.StreamSnapshot{snapshot(t, frame)}, 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	id := published.SegmentReferences[0].SegmentID
	if _, err = manager.Merge([]format.UUID{id, id}); err == nil {
		t.Fatal("duplicate Merge inputs were accepted")
	}
}

func TestCollectTrashHonorsCutoff(t *testing.T) {
	root, err := fsutil.OpenRoot(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	manager := New(root.Path(), nil)
	trash := filepath.Join(root.Path(), "trash")
	oldPath := filepath.Join(trash, "old.trash")
	newPath := filepath.Join(trash, "new.trash")
	if err = os.WriteFile(oldPath, []byte("old"), 0600); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(newPath, []byte("new"), 0600); err != nil {
		t.Fatal(err)
	}
	cutoff := time.Now().Add(-time.Hour)
	oldTime := cutoff.Add(-time.Hour)
	if err = os.Chtimes(oldPath, oldTime, oldTime); err != nil {
		t.Fatal(err)
	}
	if err = manager.CollectTrash(cutoff); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(oldPath); !os.IsNotExist(err) {
		t.Fatalf("old trash remains: %v", err)
	}
	if _, err = os.Stat(newPath); err != nil {
		t.Fatalf("new trash was removed: %v", err)
	}
}

func recordFrame(t *testing.T, entryID, streamID, sequence, offset uint64, recordedAt int64, payload string) []byte {
	t.Helper()
	hash := sha256.Sum256([]byte(payload))
	frame, err := format.MarshalRecordFrame(format.RecordFrame{
		EntryID:     entryID,
		StreamID:    streamID,
		Sequence:    sequence,
		ByteOffset:  offset,
		RecordedAt:  recordedAt,
		BatchCount:  1,
		RequestHash: hash,
		RequestID:   []byte(payload),
		Producer:    "lifecycle-test",
		Payload:     []byte(payload),
	})
	if err != nil {
		t.Fatal(err)
	}
	return frame
}

func snapshot(t *testing.T, frame []byte) memtable.StreamSnapshot {
	t.Helper()
	record, err := format.UnmarshalRecordFrame(frame)
	if err != nil {
		t.Fatal(err)
	}
	return memtable.StreamSnapshot{
		StreamID: record.StreamID,
		Tail: memtable.Tail{
			NextSequence:   record.Sequence + 1,
			NextByteOffset: record.ByteOffset + uint64(len(frame)),
			LastRecordedAt: record.RecordedAt,
			LastEntryID:    record.EntryID,
			RecordCount:    1,
		},
		Frames: [][]byte{frame},
	}
}

func findReference(t *testing.T, manifest format.Manifest, id format.UUID) format.SegmentReference {
	t.Helper()
	for _, reference := range manifest.SegmentReferences {
		if reference.SegmentID == id {
			return reference
		}
	}
	t.Fatalf("Segment %x not found", id)
	return format.SegmentReference{}
}

func otherReference(t *testing.T, manifest format.Manifest, id format.UUID) format.SegmentReference {
	t.Helper()
	for _, reference := range manifest.SegmentReferences {
		if reference.SegmentID != id {
			return reference
		}
	}
	t.Fatalf("no Segment other than %x found", id)
	return format.SegmentReference{}
}

func trashFiles(t *testing.T, root string) []string {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(root, "trash", "*"))
	if err != nil {
		t.Fatal(err)
	}
	return files
}
