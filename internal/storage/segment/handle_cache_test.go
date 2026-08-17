package segment

import (
	"crypto/sha256"
	"errors"
	"path/filepath"
	"testing"

	"github.com/akzj/streamd/internal/storage/format"
	"github.com/akzj/streamd/internal/storage/memtable"
)

func TestHandleCacheBoundsIdleReadersAndPinsInUse(t *testing.T) {
	root := t.TempDir()
	references := make([]format.SegmentReference, 3)
	for i := range references {
		references[i] = writeCacheSegment(t, root, byte(i+1))
	}
	cache := NewHandleCache(root, 1)
	first, releaseFirst, err := cache.Acquire(references[0])
	if err != nil {
		t.Fatal(err)
	}
	if _, releaseSecond, err := cache.Acquire(references[1]); err != nil {
		t.Fatal(err)
	} else {
		releaseSecond()
	}
	if cache.Len() != 1 {
		t.Fatalf("cache length with pinned handle = %d", cache.Len())
	}
	if _, err = first.Read(1, 0); err != nil {
		t.Fatalf("pinned Reader was closed: %v", err)
	}
	releaseFirst()
	if _, releaseThird, err := cache.Acquire(references[2]); err != nil {
		t.Fatal(err)
	} else {
		releaseThird()
	}
	if cache.Len() != 1 {
		t.Fatalf("cache length after eviction = %d", cache.Len())
	}
	if err = cache.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err = cache.Acquire(references[0]); !errors.Is(err, ErrHandleCacheClosed) {
		t.Fatalf("Acquire after Close error = %v", err)
	}
}

func TestDescribeReferenceRejectsManifestMismatch(t *testing.T) {
	root := t.TempDir()
	reference := writeCacheSegment(t, root, 1)
	reference.RecordCount++
	if _, err := DescribeReference(root, reference); err == nil {
		t.Fatal("mismatched Manifest Reference accepted")
	}
}

func writeCacheSegment(t *testing.T, root string, suffix byte) format.SegmentReference {
	t.Helper()
	hash := sha256.Sum256([]byte{suffix})
	frame, err := format.MarshalRecordFrame(format.RecordFrame{EntryID: uint64(suffix), StreamID: 1, Sequence: 0, RecordedAt: int64(suffix), BatchCount: 1, RequestHash: hash, RequestID: []byte{suffix}, Producer: "test", Payload: []byte{suffix}})
	if err != nil {
		t.Fatal(err)
	}
	var id format.UUID
	id[15] = suffix
	name := "segment-" + string(rune('a'+suffix)) + ".seg"
	meta, err := WriteFile(filepath.Join(root, name), id, int64(suffix), []memtable.StreamSnapshot{{StreamID: 1, Frames: [][]byte{frame}}})
	if err != nil {
		t.Fatal(err)
	}
	return format.SegmentReference{Flags: format.SegmentRefHasLocal, SegmentID: id, FileSize: meta.Footer.FileLength, FirstEntryID: meta.Header.FirstEntryID, LastEntryID: meta.Header.LastEntryID, StreamCount: meta.Header.StreamCount, RecordCount: meta.Header.RecordCount, LocalPath: name, ContentSHA256: meta.Footer.ContentSHA256}
}
