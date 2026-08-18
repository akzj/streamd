package wal

import (
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/akzj/streamd/internal/storage/format"
	"github.com/akzj/streamd/internal/storage/fsutil"
)

func TestHistoryReadsAcrossSealedAndActiveWAL(t *testing.T) {
	root, err := fsutil.OpenRoot(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	log, err := Create(root.Path(), 0, 1, time.Unix(1, 0))
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	previous, firstEntries := appendHistoryEntries(t, log, 0, 2, 0, 1)
	if err = log.Sync(); err != nil {
		t.Fatal(err)
	}
	if err = log.Rotate(2, time.Unix(2, 0)); err != nil {
		t.Fatal(err)
	}
	_, secondEntries := appendHistoryEntries(t, log, 2, 2, previous, 2)
	if err = log.Sync(); err != nil {
		t.Fatal(err)
	}
	history, err := OpenHistory(root.Path())
	if err != nil {
		t.Fatal(err)
	}
	earliest, next, present := history.Bounds()
	if !present || earliest != 0 || next != 4 {
		t.Fatalf("bounds = %d..%d, present = %v", earliest, next, present)
	}
	rangeResult, err := history.ReadRange(1, 2, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(rangeResult.Entries) != 2 || rangeResult.NextEntryID != 3 {
		t.Fatalf("range = %+v", rangeResult)
	}
	for i, encoded := range rangeResult.Entries {
		entry, decodeErr := format.UnmarshalWALEntry(encoded)
		if decodeErr != nil || entry.EntryID != uint64(i+1) {
			t.Fatalf("Entry %d = %+v, error = %v", i, entry, decodeErr)
		}
	}
	encoded, entry, err := history.EntryAt(3)
	if err != nil || entry.EntryID != 3 || len(encoded) != len(secondEntries[1]) {
		t.Fatalf("EntryAt = %+v, bytes = %d, error = %v", entry, len(encoded), err)
	}
	checksum, ok, err := history.ChecksumAt(0)
	first, _ := format.UnmarshalWALEntry(firstEntries[0])
	if err != nil || !ok || checksum != first.CRC32C {
		t.Fatalf("checksum = %d, ok = %v, error = %v", checksum, ok, err)
	}
	limited, err := history.ReadRange(1, 10, uint64(len(firstEntries[1])))
	if err != nil || len(limited.Entries) != 1 {
		t.Fatalf("limited range = %+v, error = %v", limited, err)
	}
}

func TestHistoryPinAndRefresh(t *testing.T) {
	root, err := fsutil.OpenRoot(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	log, err := Create(root.Path(), 0, 1, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	previous, _ := appendHistoryEntries(t, log, 0, 2, 0, 1)
	if err = log.Rotate(2, time.Now()); err != nil {
		t.Fatal(err)
	}
	_, _ = appendHistoryEntries(t, log, 2, 2, previous, 2)
	if err = log.Sync(); err != nil {
		t.Fatal(err)
	}
	history, err := OpenHistory(root.Path())
	if err != nil {
		t.Fatal(err)
	}
	files := history.RetainedFiles()
	if len(files) != 2 {
		t.Fatalf("files = %v", files)
	}
	release, err := history.PinRange(1, 2)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range files {
		if !history.Pinned(name) {
			t.Fatalf("%s is not pinned", name)
		}
	}
	release()
	for _, name := range files {
		if history.Pinned(name) {
			t.Fatalf("%s remained pinned", name)
		}
	}
	last, _ := format.UnmarshalWALEntry(historyEntry(t, log, 4, previousFromLog(log), 2))
	if err = log.Sync(); err != nil {
		t.Fatal(err)
	}
	if err = history.Refresh(); err != nil {
		t.Fatal(err)
	}
	_, next, _ := history.Bounds()
	if next != 5 {
		t.Fatalf("next = %d, last = %+v", next, last)
	}
}

func TestHistoryObserveActiveAdvancesWithoutRefresh(t *testing.T) {
	root, err := fsutil.OpenRoot(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	log, err := Create(root.Path(), 0, 1, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	history, err := OpenHistory(root.Path())
	if err != nil {
		t.Fatal(err)
	}
	_, entries := appendHistoryEntries(t, log, 0, 3, 0, 1)
	if err = history.ObserveActive(log); err != nil {
		t.Fatal(err)
	}
	earliest, next, present := history.Bounds()
	if !present || earliest != 0 || next != 3 {
		t.Fatalf("bounds = %d..%d, present = %v", earliest, next, present)
	}
	encoded, entry, err := history.EntryAt(2)
	if err != nil || entry.EntryID != 2 || len(encoded) != len(entries[2]) {
		t.Fatalf("EntryAt = %+v, bytes = %d, error = %v", entry, len(encoded), err)
	}
}

func TestHistoryBoundsAndCorruptionErrors(t *testing.T) {
	root, err := fsutil.OpenRoot(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	log, err := Create(root.Path(), 5, 1, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	previous, _ := appendHistoryEntries(t, log, 5, 1, 0, 1)
	if err = log.Rotate(2, time.Now()); err != nil {
		t.Fatal(err)
	}
	if err = log.Close(); err != nil {
		t.Fatal(err)
	}
	history, err := OpenHistory(root.Path())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = history.ReadRange(4, 1, 0); !errors.Is(err, ErrNotRetained) {
		t.Fatalf("not retained error = %v", err)
	}
	if _, err = history.ReadRange(7, 1, 0); !errors.Is(err, ErrHistoryAhead) {
		t.Fatalf("ahead error = %v", err)
	}
	paths, _ := filepath.Glob(filepath.Join(root.Path(), "wal", "*.log"))
	for _, path := range paths {
		if filepath.Base(path) == history.RetainedFiles()[len(history.RetainedFiles())-1] {
			continue
		}
		file, openErr := os.OpenFile(path, os.O_RDWR, 0)
		if openErr != nil {
			t.Fatal(openErr)
		}
		if _, openErr = file.WriteAt([]byte{0xff}, format.WALFileHeaderLength+8); openErr != nil {
			t.Fatal(openErr)
		}
		file.Close()
		break
	}
	if _, err = OpenHistory(root.Path()); err == nil {
		t.Fatalf("corrupt sealed WAL accepted; previous = %d", previous)
	}
}

func TestHistoryIgnoresUnpublishedHeaderOnlyWAL(t *testing.T) {
	root, err := fsutil.OpenRoot(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	log, err := Create(root.Path(), 0, 1, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	orphanID := format.UUID{15: 9}
	orphan := format.MarshalWALFileHeader(format.WALFileHeader{FileID: orphanID, FirstEntryID: 0, CreatedTerm: 1, CreatedAt: 1})
	if err = os.WriteFile(filepath.Join(root.Path(), "wal", "WAL-ORPHAN.log"), orphan, 0640); err != nil {
		t.Fatal(err)
	}
	history, err := OpenHistory(root.Path())
	if err != nil {
		t.Fatal(err)
	}
	if files := history.RetainedFiles(); len(files) != 1 {
		t.Fatalf("retained files = %v", files)
	}
}

func TestHistoryGCRequiresSnapshotCoverageAndHonorsPins(t *testing.T) {
	root, err := fsutil.OpenRoot(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	log, err := Create(root.Path(), 0, 1, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	previous, _ := appendHistoryEntries(t, log, 0, 2, 0, 1)
	if err = log.Rotate(1, time.Now()); err != nil {
		t.Fatal(err)
	}
	previous, _ = appendHistoryEntries(t, log, 2, 2, previous, 1)
	if err = log.Rotate(1, time.Now()); err != nil {
		t.Fatal(err)
	}
	_, _ = appendHistoryEntries(t, log, 4, 2, previous, 1)
	if err = log.Sync(); err != nil {
		t.Fatal(err)
	}
	history, err := OpenHistory(root.Path())
	if err != nil {
		t.Fatal(err)
	}
	result, err := history.Collect(GCOptions{SegmentedThrough: 3, SnapshotThrough: 3})
	if err != nil || len(result.DeletedFiles) != 0 {
		t.Fatalf("unverified Snapshot GC = %+v, %v", result, err)
	}
	release, err := history.PinRange(0, 0)
	if err != nil {
		t.Fatal(err)
	}
	type collection struct {
		result GCResult
		err    error
	}
	collected := make(chan collection, 1)
	go func() {
		value, collectErr := history.Collect(GCOptions{SegmentedThrough: 3, SnapshotThrough: 3, SnapshotVerified: true})
		collected <- collection{value, collectErr}
	}()
	select {
	case value := <-collected:
		result, err = value.result, value.err
	case <-time.After(time.Second):
		t.Fatal("GC did not complete while a catch-up Pin was held")
	}
	if err != nil || len(result.DeletedFiles) != 0 {
		t.Fatalf("pinned GC = %+v, %v", result, err)
	}
	release()
	result, err = history.Collect(GCOptions{SegmentedThrough: 3, SnapshotThrough: 3, SnapshotVerified: true, ReplicaDurable: HistoryPosition{Present: true, EntryID: 1}})
	if err != nil || len(result.DeletedFiles) != 2 || result.EarliestWAL != 4 || !result.NeedsSnapshot {
		t.Fatalf("GC = %+v, %v", result, err)
	}
	if files := history.RetainedFiles(); len(files) != 1 {
		t.Fatalf("retained files = %v", files)
	}
	if _, statErr := os.Stat(filepath.Join(root.Path(), "wal", result.DeletedFiles[0])); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("deleted file still exists: %v", statErr)
	}
}

func TestHistoryGCReportsRetentionPressure(t *testing.T) {
	root, err := fsutil.OpenRoot(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	log, err := Create(root.Path(), 0, 1, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	_, _ = appendHistoryEntries(t, log, 0, 1, 0, 1)
	if err = log.Sync(); err != nil {
		t.Fatal(err)
	}
	history, err := OpenHistory(root.Path())
	if err != nil {
		t.Fatal(err)
	}
	result, err := history.Collect(GCOptions{MaxRetainedBytes: 1})
	if !errors.Is(err, ErrRetentionPressure) || !result.RetentionPressure || len(result.DeletedFiles) != 0 {
		t.Fatalf("pressure = %+v, %v", result, err)
	}
}

func appendHistoryEntries(t *testing.T, log *Log, first uint64, count int, previous uint32, term uint64) (uint32, [][]byte) {
	t.Helper()
	entries := make([][]byte, 0, count)
	for i := 0; i < count; i++ {
		encoded := makeHistoryEntry(t, first+uint64(i), previous, term)
		entry, err := format.UnmarshalWALEntry(encoded)
		if err != nil {
			t.Fatal(err)
		}
		previous = entry.CRC32C
		entries = append(entries, encoded)
	}
	if err := log.Append(entries...); err != nil {
		t.Fatal(err)
	}
	return previous, entries
}

func historyEntry(t *testing.T, log *Log, entryID uint64, previous uint32, term uint64) []byte {
	t.Helper()
	encoded := makeHistoryEntry(t, entryID, previous, term)
	if err := log.Append(encoded); err != nil {
		t.Fatal(err)
	}
	return encoded
}

func makeHistoryEntry(t *testing.T, entryID uint64, previous uint32, term uint64) []byte {
	t.Helper()
	hash := sha256.Sum256([]byte{byte(entryID)})
	frame, err := format.MarshalRecordFrame(format.RecordFrame{EntryID: entryID, StreamID: 1, Sequence: entryID, RecordedAt: int64(entryID), BatchCount: 1, RequestHash: hash, Producer: "history-test"})
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := format.MarshalWALEntry(term, previous, frame)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func previousFromLog(log *Log) uint32 { return log.PreviousEntryCRC32C() }
