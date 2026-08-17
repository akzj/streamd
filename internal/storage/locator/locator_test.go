package locator

import (
	"crypto/sha256"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/akzj/streamd/internal/storage/format"
	"github.com/akzj/streamd/internal/storage/memtable"
	"github.com/akzj/streamd/internal/storage/segment"
)

func TestBuildOpenAndLookupLocator(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "segments"), 0750); err != nil {
		t.Fatal(err)
	}
	descriptors := []segment.Descriptor{
		writeLocatorSegment(t, root, locatorID(1), 0, 10),
		writeLocatorSegment(t, root, locatorID(2), 1, 20),
	}
	result, err := BuildCheckpoint(root, locatorID(3), locatorID(4), locatorID(5), 7, 9, descriptors)
	if err != nil {
		t.Fatal(err)
	}
	manifest := format.Manifest{Header: format.ManifestHeader{Generation: 7, LastEntryID: 9}, ArtifactReferences: []format.ArtifactReference{{ArtifactType: format.ArtifactTailCatalog, FormatVersion: format.VersionV1, ArtifactID: locatorID(5), FileSize: 1, Path: "catalog/tail", ContentSHA256: sha256.Sum256([]byte("tail"))}, result.Reference}}
	for _, descriptor := range descriptors {
		manifest.SegmentReferences = append(manifest.SegmentReferences, descriptor.Reference)
	}
	store, err := Open(root, manifest, 1)
	if err != nil {
		t.Fatal(err)
	}
	for sequence, want := range []format.UUID{locatorID(1), locatorID(2)} {
		extent, found, lookupErr := store.LookupSequence(1, uint64(sequence))
		if lookupErr != nil || !found || extent.Reference.SegmentID != want {
			t.Fatalf("Sequence %d Extent=%+v found=%v error=%v", sequence, extent, found, lookupErr)
		}
	}
	if _, found, err := store.LookupSequence(1, 2); err != nil || found {
		t.Fatalf("out-of-range found=%v error=%v", found, err)
	}
	if store.CacheLen() != 1 {
		t.Fatalf("Page Cache length = %d", store.CacheLen())
	}
}

func TestLookupTraversesPreviousPagesAndBoundsCache(t *testing.T) {
	root := t.TempDir()
	count := maxExtentsPerPage + 1
	descriptors := make([]segment.Descriptor, 0, count)
	for i := 0; i < count; i++ {
		id := locatorOrdinalID(i + 1)
		descriptors = append(descriptors, segment.Descriptor{
			Reference: format.SegmentReference{SegmentID: id},
			Directories: []format.StreamDirectoryEntry{{
				StreamID: 1, FirstSequence: uint64(i), RecordCount: 1,
				FirstByteOffset: uint64(i), NextByteOffset: uint64(i + 1),
				FirstRecordedAt: int64(i), LastRecordedAt: int64(i),
			}},
		})
	}
	result, err := BuildCheckpoint(root, locatorID(3), locatorID(4), locatorID(5), 7, uint64(count-1), descriptors)
	if err != nil {
		t.Fatal(err)
	}
	manifest := locatorManifest(result, descriptors, uint64(count-1))
	store, err := Open(root, manifest, 1)
	if err != nil {
		t.Fatal(err)
	}
	for _, sequence := range []uint64{uint64(count - 1), 0} {
		extent, found, lookupErr := store.LookupSequence(1, sequence)
		if lookupErr != nil || !found || extent.Reference.SegmentID != locatorOrdinalID(int(sequence)+1) {
			t.Fatalf("Sequence %d Extent=%+v found=%v error=%v", sequence, extent, found, lookupErr)
		}
	}
	if store.CacheLen() != 1 {
		t.Fatalf("Page Cache length = %d", store.CacheLen())
	}
	store.ClearCache()
	if store.CacheLen() != 0 {
		t.Fatalf("cleared Page Cache length = %d", store.CacheLen())
	}
}

func TestLookupRejectsCorruptPageAndUnknownSegment(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "segments"), 0750); err != nil {
		t.Fatal(err)
	}
	descriptors := []segment.Descriptor{
		writeLocatorSegment(t, root, locatorID(1), 0, 10),
		writeLocatorSegment(t, root, locatorID(2), 1, 20),
	}
	result, err := BuildCheckpoint(root, locatorID(3), locatorID(4), locatorID(5), 7, 9, descriptors)
	if err != nil {
		t.Fatal(err)
	}
	manifest := locatorManifest(result, descriptors, 9)
	missing := manifest
	missing.SegmentReferences = missing.SegmentReferences[:1]
	store, err := Open(root, missing, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = store.LookupSequence(1, 1); err == nil {
		t.Fatal("Locator accepted an unknown Segment")
	}
	file, err := os.OpenFile(filepath.Join(root, filepath.FromSlash(result.Pack.Path)), os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = file.WriteAt([]byte{0xff}, int64(format.SegmentSectionAlignment+32)); err != nil {
		file.Close()
		t.Fatal(err)
	}
	if err = file.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = Open(root, manifest, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = store.LookupSequence(1, 0); err == nil {
		t.Fatal("Locator accepted a corrupt Page")
	}
}

func locatorManifest(result BuildResult, descriptors []segment.Descriptor, coveredEntryID uint64) format.Manifest {
	manifest := format.Manifest{Header: format.ManifestHeader{Generation: 7, LastEntryID: coveredEntryID}, ArtifactReferences: []format.ArtifactReference{{ArtifactType: format.ArtifactTailCatalog, FormatVersion: format.VersionV1, ArtifactID: locatorID(5), FileSize: 1, Path: "catalog/tail", ContentSHA256: sha256.Sum256([]byte("tail"))}, result.Reference}}
	for _, descriptor := range descriptors {
		manifest.SegmentReferences = append(manifest.SegmentReferences, descriptor.Reference)
	}
	return manifest
}

func writeLocatorSegment(t *testing.T, root string, id format.UUID, sequence uint64, recordedAt int64) segment.Descriptor {
	t.Helper()
	hash := sha256.Sum256([]byte{byte(sequence)})
	frame, err := format.MarshalRecordFrame(format.RecordFrame{EntryID: sequence, StreamID: 1, Sequence: sequence, ByteOffset: sequence * 136, RecordedAt: recordedAt, BatchCount: 1, RequestHash: hash, RequestID: []byte{byte(sequence)}, Producer: "test", Payload: []byte{byte(sequence)}})
	if err != nil {
		t.Fatal(err)
	}
	// Use the actual previous frame length so Byte Offsets remain continuous.
	if sequence > 0 {
		previous, _ := format.MarshalRecordFrame(format.RecordFrame{EntryID: 0, StreamID: 1, Sequence: 0, RecordedAt: 10, BatchCount: 1, RequestHash: sha256.Sum256([]byte{0}), RequestID: []byte{0}, Producer: "test", Payload: []byte{0}})
		record, _ := format.UnmarshalRecordFrame(frame)
		record.ByteOffset = uint64(len(previous))
		frame, err = format.MarshalRecordFrame(record)
		if err != nil {
			t.Fatal(err)
		}
	}
	path := filepath.Join(root, "segments", "SEG-"+string(rune('a'+id[15]))+".seg")
	meta, err := segment.WriteFile(path, id, recordedAt, []memtable.StreamSnapshot{{StreamID: 1, Frames: [][]byte{frame}}})
	if err != nil {
		t.Fatal(err)
	}
	reference := format.SegmentReference{Flags: format.SegmentRefHasLocal, SegmentID: id, FileSize: meta.Footer.FileLength, FirstEntryID: meta.Header.FirstEntryID, LastEntryID: meta.Header.LastEntryID, StreamCount: meta.Header.StreamCount, RecordCount: meta.Header.RecordCount, LocalPath: filepath.ToSlash(filepath.Join("segments", filepath.Base(path))), ContentSHA256: meta.Footer.ContentSHA256}
	descriptor, err := segment.DescribeReference(root, reference)
	if err != nil {
		t.Fatal(err)
	}
	return descriptor
}

func locatorID(value byte) format.UUID {
	var id format.UUID
	id[15] = value
	return id
}

func locatorOrdinalID(value int) format.UUID {
	var id format.UUID
	binary.BigEndian.PutUint64(id[8:], uint64(value))
	return id
}
