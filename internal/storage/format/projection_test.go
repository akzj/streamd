package format

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"testing"
)

func TestArtifactFooterAndNodeRoundTrip(t *testing.T) {
	content := []byte("projection checkpoint")
	footer, err := NewArtifactFooter(ArtifactTailCatalog, testUUID(1), content)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := MarshalArtifactFooter(footer)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := VerifyArtifact(content, encoded, ArtifactTailCatalog, testUUID(1))
	if err != nil || decoded != footer {
		t.Fatalf("Artifact Footer: %+v %v", decoded, err)
	}
	corrupt := bytes.Clone(encoded)
	corrupt[48] ^= 1
	if _, err := UnmarshalArtifactFooter(corrupt); !errors.Is(err, ErrChecksum) {
		t.Fatalf("Artifact corruption: %v", err)
	}
	node := NodeIdentity{ClusterID: testUUID(1), GroupID: testUUID(2), NodeID: testUUID(3), CreatedAt: 42}
	encoded, err = MarshalNodeIdentity(node)
	if err != nil {
		t.Fatal(err)
	}
	decodedNode, err := UnmarshalNodeIdentity(encoded)
	if err != nil || decodedNode != node {
		t.Fatalf("NODE: %+v %v", decodedNode, err)
	}
}

func TestTailCatalogFormats(t *testing.T) {
	header := TailCatalogHeader{ArtifactID: testUUID(1), SlotCount: 2, CoveredEntryID: 9, ManifestGeneration: 3}
	b, err := MarshalTailCatalogHeader(header)
	if err != nil {
		t.Fatal(err)
	}
	got, err := UnmarshalTailCatalogHeader(b)
	if err != nil || got != header {
		t.Fatalf("Header: %+v %v", got, err)
	}
	position, err := TailSlotPosition(2)
	if err != nil || position != 4096+2*TailSlotLength {
		t.Fatalf("position %d %v", position, err)
	}
	slot := TailSlot{Generation: 4, Present: true, StreamID: 2, NextSequence: 7, NextByteOffset: 900, LastRecordedAt: 100, LastEntryID: 8, AppliedEntryID: 9, LatestSegmentID: testUUID(4), LatestExtentPackID: testUUID(5), LatestPageOrdinal: 3}
	b, err = MarshalTailSlot(slot)
	if err != nil {
		t.Fatal(err)
	}
	gotSlot, err := UnmarshalTailSlot(b)
	if err != nil || gotSlot != slot {
		t.Fatalf("Slot: %+v %v", gotSlot, err)
	}
	putU64(b[120:128], 6)
	if _, err := UnmarshalTailSlot(b); !errors.Is(err, ErrInvalid) {
		t.Fatalf("torn Slot: %v", err)
	}
}

func TestLocatorFormats(t *testing.T) {
	pack := LocatorPackHeader{ArtifactID: testUUID(1), PageCount: 2, CreatedAt: 10, CoveredEntryID: 8}
	b, err := MarshalLocatorPackHeader(pack)
	if err != nil {
		t.Fatal(err)
	}
	gotPack, err := UnmarshalLocatorPackHeader(b)
	if err != nil || gotPack != pack {
		t.Fatalf("Pack: %+v %v", gotPack, err)
	}
	page := ExtentPage{Header: ExtentPageHeader{PageID: testUUID(2), StreamID: 3, FirstSequence: 0, NextSequence: 4, FirstRecordedAt: 10, LastRecordedAt: 30}, Extents: []ExtentEntry{{SegmentID: testUUID(3), FirstSequence: 0, NextSequence: 2, FirstByteOffset: 0, NextByteOffset: 240, FirstRecordedAt: 10, LastRecordedAt: 20, RecordIndexOffset: 8192, StreamDataOffset: 12288}, {SegmentID: testUUID(4), FirstSequence: 2, NextSequence: 4, FirstByteOffset: 240, NextByteOffset: 500, FirstRecordedAt: 20, LastRecordedAt: 30, RecordIndexOffset: 16384, StreamDataOffset: 20480}}}
	b, err = MarshalExtentPage(page)
	if err != nil {
		t.Fatal(err)
	}
	gotPage, err := UnmarshalExtentPage(b)
	if err != nil || len(gotPage.Extents) != 2 || gotPage.Header != page.Header {
		t.Fatalf("Page: %+v %v", gotPage, err)
	}
	corrupt := bytes.Clone(b)
	corrupt[len(corrupt)-8] ^= 1
	if _, err := UnmarshalExtentPage(corrupt); !errors.Is(err, ErrChecksum) {
		t.Fatalf("Page corruption: %v", err)
	}
	ref := LocatorPackReference{PackID: testUUID(1), FileSize: 4096 + 2*LocatorPageLength + ArtifactFooterLength, PageCount: 2, ContentSHA256: sha256.Sum256([]byte("pack")), Path: "locator/pack.loc"}
	b, err = MarshalLocatorPackReference(ref)
	if err != nil {
		t.Fatal(err)
	}
	gotRef, err := UnmarshalLocatorPackReference(b)
	if err != nil || gotRef != ref {
		t.Fatalf("Pack Reference: %+v %v", gotRef, err)
	}
	root := LocatorRootEntry{StreamID: 3, PackID: testUUID(1), PageOrdinal: 1}
	b, err = MarshalLocatorRootEntry(root)
	if err != nil {
		t.Fatal(err)
	}
	gotRoot, err := UnmarshalLocatorRootEntry(b)
	if err != nil || gotRoot != root {
		t.Fatalf("Root: %+v %v", gotRoot, err)
	}
}

func TestProjectionHeadersAndEntries(t *testing.T) {
	locator := LocatorSnapshotHeader{ArtifactID: testUUID(1), ManifestGeneration: 2, CoveredEntryID: 8, TailCatalogArtifactID: testUUID(2), PackCount: 1, RootCount: 1, CreatedAt: 10}
	b, err := MarshalLocatorSnapshotHeader(locator)
	if err != nil {
		t.Fatal(err)
	}
	gotLocator, err := UnmarshalLocatorSnapshotHeader(b)
	if err != nil || gotLocator != locator {
		t.Fatalf("Locator Snapshot: %+v %v", gotLocator, err)
	}
	record := RegistryRecord{AssignedStreamID: 1, Namespace: "agent", StreamName: "events"}
	b, err = MarshalRegistryRecord(record)
	if err != nil {
		t.Fatal(err)
	}
	gotRecord, err := UnmarshalRegistryRecord(b)
	if err != nil || gotRecord != record {
		t.Fatalf("Registry Record: %+v %v", gotRecord, err)
	}
	entry := RegistryEntry{StreamID: 1, CreatedEntryID: 4, Namespace: "agent", StreamName: "events"}
	b, err = MarshalRegistryEntry(entry)
	if err != nil {
		t.Fatal(err)
	}
	gotEntry, err := UnmarshalRegistryEntry(b)
	if err != nil || gotEntry != entry {
		t.Fatalf("Registry Entry: %+v %v", gotEntry, err)
	}
	block := RegistryBlockIndexEntry{EntryCount: 1, EntriesOffset: 200, FirstNamespace: "agent", FirstStreamName: "events"}
	b, err = MarshalRegistryBlockIndexEntry(block)
	if err != nil {
		t.Fatal(err)
	}
	gotBlock, err := UnmarshalRegistryBlockIndexEntry(b)
	if err != nil || gotBlock != block {
		t.Fatalf("Registry Block: %+v %v", gotBlock, err)
	}
	header := RegistrySnapshotHeader{ArtifactID: testUUID(3), CoveredEntryID: 8, EntryCount: 1, BlockCount: 1, BlockIndexOffset: RegistrySnapshotHeaderLength, EntriesOffset: 200, CreatedAt: 10}
	b, err = MarshalRegistrySnapshotHeader(header)
	if err != nil {
		t.Fatal(err)
	}
	gotHeader, err := UnmarshalRegistrySnapshotHeader(b)
	if err != nil || gotHeader != header {
		t.Fatalf("Registry Header: %+v %v", gotHeader, err)
	}
	digest := sha256.Sum256([]byte("manifest"))
	snapshot := SnapshotHeader{Flags: SnapshotFlagEmpty, SnapshotID: testUUID(4), GroupID: testUUID(5), ManifestSHA256: digest, ArtifactCount: 1}
	b, err = MarshalSnapshotHeader(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	gotSnapshot, err := UnmarshalSnapshotHeader(b)
	if err != nil || gotSnapshot != snapshot {
		t.Fatalf("Snapshot Header: %+v %v", gotSnapshot, err)
	}
	artifact := SnapshotArtifact{ArtifactType: ArtifactManifest, FormatVersion: VersionV1, Flags: SegmentRefHasLocal, ArtifactID: testUUID(6), FileSize: 100, LocalName: "manifests/MANIFEST.bin", ContentSHA256: digest}
	b, err = MarshalSnapshotArtifact(artifact)
	if err != nil {
		t.Fatal(err)
	}
	gotArtifact, err := UnmarshalSnapshotArtifact(b)
	if err != nil || gotArtifact != artifact {
		t.Fatalf("Snapshot Artifact: %+v %v", gotArtifact, err)
	}
}

func FuzzUnmarshalRegistryRecord(f *testing.F) {
	seed, err := MarshalRegistryRecord(RegistryRecord{AssignedStreamID: 1, Namespace: "ns", StreamName: "stream"})
	if err != nil {
		f.Fatal(err)
	}
	f.Add(seed)
	f.Add([]byte("registry"))
	f.Fuzz(func(t *testing.T, data []byte) {
		decoded, err := UnmarshalRegistryRecord(data)
		if err != nil {
			return
		}
		encoded, err := MarshalRegistryRecord(decoded)
		if err != nil || !bytes.Equal(data, encoded) {
			t.Fatalf("round trip: %v", err)
		}
	})
}

func TestProjectionFilesRoundTrip(t *testing.T) {
	digest := sha256.Sum256([]byte("artifact"))
	locator := LocatorSnapshot{Header: LocatorSnapshotHeader{ArtifactID: testUUID(1), TailCatalogArtifactID: testUUID(2)}, Packs: []LocatorPackReference{{PackID: testUUID(3), FileSize: 4096 + LocatorPageLength + ArtifactFooterLength, PageCount: 1, ContentSHA256: digest, Path: "locator/pack.loc"}}, Roots: []LocatorRootEntry{{StreamID: 1, PackID: testUUID(3)}}}
	b, err := MarshalLocatorSnapshot(locator)
	if err != nil {
		t.Fatal(err)
	}
	decodedLocator, err := UnmarshalLocatorSnapshot(b)
	if err != nil || len(decodedLocator.Packs) != 1 || len(decodedLocator.Roots) != 1 {
		t.Fatalf("Locator file: %+v %v", decodedLocator, err)
	}
	registry := RegistrySnapshot{Header: RegistrySnapshotHeader{ArtifactID: testUUID(4), CoveredEntryID: 10}, Entries: []RegistryEntry{{StreamID: 2, CreatedEntryID: 2, Namespace: "z", StreamName: "b"}, {StreamID: 1, CreatedEntryID: 1, Namespace: "a", StreamName: "a"}}}
	b, err = MarshalRegistrySnapshot(registry)
	if err != nil {
		t.Fatal(err)
	}
	decodedRegistry, err := UnmarshalRegistrySnapshot(b)
	if err != nil || len(decodedRegistry.Entries) != 2 || decodedRegistry.Entries[0].Namespace != "a" {
		t.Fatalf("Registry file: %+v %v", decodedRegistry, err)
	}
	snapshot := SnapshotManifest{Header: SnapshotHeader{Flags: SnapshotFlagEmpty, SnapshotID: testUUID(5), GroupID: testUUID(6), ManifestSHA256: digest}, Artifacts: []SnapshotArtifact{{ArtifactType: ArtifactManifest, FormatVersion: VersionV1, Flags: SegmentRefHasLocal, ArtifactID: testUUID(7), FileSize: 100, LocalName: "manifests/m.bin", ContentSHA256: digest}}}
	b, err = MarshalSnapshotManifest(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	decodedSnapshot, err := UnmarshalSnapshotManifest(b)
	if err != nil || len(decodedSnapshot.Artifacts) != 1 {
		t.Fatalf("Snapshot file: %+v %v", decodedSnapshot, err)
	}
}
