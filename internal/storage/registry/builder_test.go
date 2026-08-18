package registry

import (
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/akzj/streamd/internal/storage/format"
	"github.com/akzj/streamd/internal/storage/memtable"
	"github.com/akzj/streamd/internal/storage/segment"
)

func TestBuildCheckpointFromSegmentsMatchesV1Encoding(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "segments"), 0750); err != nil {
		t.Fatal(err)
	}
	entries := make([]format.RegistryEntry, format.RegistryBlockEntriesV1+1)
	for i := range entries {
		entries[i] = format.RegistryEntry{
			StreamID:       uint64(i + 1),
			CreatedEntryID: uint64(i + 10),
			Namespace:      "agent",
			StreamName:     fmt.Sprintf("stream-%04d", len(entries)-i),
		}
	}
	byteOffset := uint64(0)
	first := writeRegistryFactSegment(t, root, registryID(10), entries[:128], &byteOffset)
	second := writeRegistryFactSegment(t, root, registryID(11), entries[128:], &byteOffset)
	coveredEntryID := entries[len(entries)-1].CreatedEntryID
	reference, err := BuildCheckpointFromSegments(root, registryID(12), coveredEntryID, 55, []segment.Descriptor{second, first})
	if err != nil {
		t.Fatal(err)
	}
	actual, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(reference.Path)))
	if err != nil {
		t.Fatal(err)
	}
	expected, err := format.MarshalRegistrySnapshot(format.RegistrySnapshot{Header: format.RegistrySnapshotHeader{ArtifactID: registryID(12), CoveredEntryID: coveredEntryID, CreatedAt: 55}, Entries: entries})
	if err != nil {
		t.Fatal(err)
	}
	if string(actual) != string(expected) {
		t.Fatal("streamed Registry Snapshot differs from V1 encoding")
	}
	store, err := OpenCheckpoint(root, reference, coveredEntryID, 1)
	if err != nil {
		t.Fatal(err)
	}
	for _, number := range []int{1, len(entries)} {
		name := fmt.Sprintf("stream-%04d", len(entries)-number+1)
		mapping, found, lookupErr := store.Lookup("agent", name)
		if lookupErr != nil || !found || mapping.StreamID != uint64(number) {
			t.Fatalf("Lookup %q = %+v, found=%v, error=%v", name, mapping, found, lookupErr)
		}
	}
	builds, globErr := filepath.Glob(filepath.Join(root, "registry", ".build-*"))
	if globErr != nil || len(builds) != 0 {
		t.Fatalf("temporary Registry builds = %v, %v", builds, globErr)
	}
}

func TestBuildCheckpointFromSegmentsRejectsDuplicateKeyAndCleans(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "segments"), 0750); err != nil {
		t.Fatal(err)
	}
	entries := []format.RegistryEntry{
		{StreamID: 1, CreatedEntryID: 1, Namespace: "agent", StreamName: "same"},
		{StreamID: 2, CreatedEntryID: 2, Namespace: "agent", StreamName: "same"},
	}
	byteOffset := uint64(0)
	descriptor := writeRegistryFactSegment(t, root, registryID(20), entries, &byteOffset)
	if _, err := BuildCheckpointFromSegments(root, registryID(21), 2, 1, []segment.Descriptor{descriptor}); err == nil {
		t.Fatal("Registry Builder accepted duplicate key facts")
	}
	builds, globErr := filepath.Glob(filepath.Join(root, "registry", ".build-*"))
	staging, stagingErr := filepath.Glob(filepath.Join(root, "registry", ".REGISTRY-*.tmp"))
	if globErr != nil || stagingErr != nil || len(builds) != 0 || len(staging) != 0 {
		t.Fatalf("failed Registry files: builds=%v staging=%v errors=%v/%v", builds, staging, globErr, stagingErr)
	}
}

func TestMergeRegistryRunsOrdersEntries(t *testing.T) {
	directory := t.TempDir()
	left := filepath.Join(directory, "left.bin")
	right := filepath.Join(directory, "right.bin")
	output := filepath.Join(directory, "output.bin")
	if err := writeRegistryRun(left, []format.RegistryEntry{{StreamID: 2, Namespace: "a", StreamName: "b"}, {StreamID: 4, Namespace: "c", StreamName: "d"}}); err != nil {
		t.Fatal(err)
	}
	if err := writeRegistryRun(right, []format.RegistryEntry{{StreamID: 1, Namespace: "a", StreamName: "a"}, {StreamID: 3, Namespace: "b", StreamName: "c"}}); err != nil {
		t.Fatal(err)
	}
	if err := mergeRegistryRuns([]string{left, right}, output); err != nil {
		t.Fatal(err)
	}
	var names []string
	if err := scanRegistryRun(output, func(entry format.RegistryEntry) error {
		names = append(names, entry.Namespace+"/"+entry.StreamName)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	want := []string{"a/a", "a/b", "b/c", "c/d"}
	if fmt.Sprint(names) != fmt.Sprint(want) {
		t.Fatalf("merged names = %v, want %v", names, want)
	}
}

func writeRegistryFactSegment(t *testing.T, root string, id format.UUID, entries []format.RegistryEntry, byteOffset *uint64) segment.Descriptor {
	t.Helper()
	frames := make([][]byte, 0, len(entries))
	for _, entry := range entries {
		payload, err := format.MarshalRegistryRecord(format.RegistryRecord{AssignedStreamID: entry.StreamID, Namespace: entry.Namespace, StreamName: entry.StreamName})
		if err != nil {
			t.Fatal(err)
		}
		hash := sha256.Sum256(payload)
		frame, err := format.MarshalRecordFrame(format.RecordFrame{EntryID: entry.CreatedEntryID, StreamID: RegistryStreamID, Sequence: entry.StreamID - 1, ByteOffset: *byteOffset, RecordedAt: int64(entry.StreamID), BatchCount: 1, RequestHash: hash, RequestID: []byte(fmt.Sprintf("registry-%d", entry.StreamID)), Producer: "test", Payload: payload})
		if err != nil {
			t.Fatal(err)
		}
		frames = append(frames, frame)
		*byteOffset += uint64(len(frame))
	}
	name := fmt.Sprintf("REGISTRY-FACT-%02x.seg", id[15])
	path := filepath.Join(root, "segments", name)
	meta, err := segment.WriteFile(path, id, 1, []memtable.StreamSnapshot{{StreamID: RegistryStreamID, Frames: frames}})
	if err != nil {
		t.Fatal(err)
	}
	reference := format.SegmentReference{Flags: format.SegmentRefHasLocal, SegmentID: id, FileSize: meta.Footer.FileLength, FirstEntryID: meta.Header.FirstEntryID, LastEntryID: meta.Header.LastEntryID, StreamCount: meta.Header.StreamCount, RecordCount: meta.Header.RecordCount, LocalPath: filepath.ToSlash(filepath.Join("segments", name)), ContentSHA256: meta.Footer.ContentSHA256}
	descriptor, err := segment.DescribeReference(root, reference)
	if err != nil {
		t.Fatal(err)
	}
	return descriptor
}
