package format

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"testing"
)

func sampleManifest() Manifest {
	previous := sha256.Sum256([]byte("previous manifest"))
	segmentDigest1 := sha256.Sum256([]byte("segment one"))
	segmentDigest2 := sha256.Sum256([]byte("segment two"))
	artifactDigest1 := sha256.Sum256([]byte("tail catalog"))
	artifactDigest2 := sha256.Sum256([]byte("locator"))
	return Manifest{
		Header: ManifestHeader{
			FileID: testUUID(9), Generation: 1, PreviousGeneration: 0,
			PreviousManifestSHA256: previous, CreatedAt: 1_700_000_000_000_000_000,
			LastEntryID: 10, LastEntryCRC32C: 0x10203040, RecordCount: 3,
		},
		SegmentReferences: []SegmentReference{
			{
				Flags: SegmentRefHasObject, SegmentID: testUUID(2), FileSize: 20480,
				FirstEntryID: 7, LastEntryID: 10, StreamCount: 1, RecordCount: 1,
				ObjectLocation: "s3://bucket/SEG-2.seg", ContentSHA256: segmentDigest2,
			},
			{
				Flags: SegmentRefHasLocal, SegmentID: testUUID(1), FileSize: 20480,
				FirstEntryID: 5, LastEntryID: 9, StreamCount: 2, RecordCount: 2,
				LocalPath: "segments/SEG-1.seg", ContentSHA256: segmentDigest1,
			},
		},
		ArtifactReferences: []ArtifactReference{
			{
				ArtifactType: ArtifactLocatorSnapshot, FormatVersion: VersionV1,
				ArtifactID: testUUID(5), FileSize: 2048, CoveredEntryID: 10,
				Path: "locator/LOC-5.bin", ContentSHA256: artifactDigest2,
			},
			{
				ArtifactType: ArtifactTailCatalog, FormatVersion: VersionV1,
				ArtifactID: testUUID(4), FileSize: 1024, CoveredEntryID: 10,
				Path: "tail/TAIL-4.cat", ContentSHA256: artifactDigest1,
			},
		},
	}
}

func TestManifestGoldenCanonicalRoundTrip(t *testing.T) {
	manifest := sampleManifest()
	encoded, err := MarshalManifest(manifest)
	if err != nil {
		t.Fatal(err)
	}
	assertGolden(t, "manifest_v1.hex", encoded)
	decoded, err := UnmarshalManifest(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Header.SegmentCount != 2 || decoded.Header.ArtifactCount != 2 {
		t.Fatalf("counts were not derived: %+v", decoded.Header)
	}
	if decoded.SegmentReferences[0].SegmentID != testUUID(1) || decoded.ArtifactReferences[0].ArtifactType != ArtifactTailCatalog {
		t.Fatal("references were not canonically sorted")
	}
	reencoded, err := MarshalManifest(decoded)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(encoded, reencoded) {
		t.Fatal("Manifest encoding is not stable after round trip")
	}
}

func TestManifestReferenceRoundTrips(t *testing.T) {
	manifest := sampleManifest()
	segment := manifest.SegmentReferences[0]
	encodedSegment, err := MarshalSegmentReference(segment)
	if err != nil {
		t.Fatal(err)
	}
	decodedSegment, err := UnmarshalSegmentReference(encodedSegment)
	if err != nil || decodedSegment != segment {
		t.Fatalf("Segment Reference round trip: %+v, %v", decodedSegment, err)
	}

	artifact := manifest.ArtifactReferences[0]
	encodedArtifact, err := MarshalArtifactReference(artifact)
	if err != nil {
		t.Fatal(err)
	}
	decodedArtifact, err := UnmarshalArtifactReference(encodedArtifact)
	if err != nil || decodedArtifact != artifact {
		t.Fatalf("Artifact Reference round trip: %+v, %v", decodedArtifact, err)
	}
}

func TestCurrentPointerGoldenAndRoundTrip(t *testing.T) {
	pointer := CurrentPointer{
		Generation: 7, ManifestFileID: testUUID(9),
		ManifestSHA256:   sha256.Sum256([]byte("manifest")),
		ManifestFileName: "MANIFEST-00000000000000000007-id.bin",
	}
	encoded, err := MarshalCurrentPointer(pointer)
	if err != nil {
		t.Fatal(err)
	}
	assertGolden(t, "current_v1.hex", encoded)
	decoded, err := UnmarshalCurrentPointer(encoded)
	if err != nil || decoded != pointer {
		t.Fatalf("CURRENT round trip: %+v, %v", decoded, err)
	}
}

func TestManifestRejectsCorruptionAndNoncanonicalData(t *testing.T) {
	manifest := sampleManifest()
	encoded, err := MarshalManifest(manifest)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("truncated", func(t *testing.T) {
		if _, err := UnmarshalManifest(encoded[:len(encoded)-1]); !errors.Is(err, ErrTruncated) && !errors.Is(err, ErrInvalid) {
			t.Fatalf("got %v", err)
		}
	})
	t.Run("trailing bytes", func(t *testing.T) {
		if _, err := UnmarshalManifest(append(bytes.Clone(encoded), 0)); !errors.Is(err, ErrInvalid) {
			t.Fatalf("got %v", err)
		}
	})
	t.Run("content digest", func(t *testing.T) {
		corrupt := bytes.Clone(encoded)
		footerPosition := len(corrupt) - ManifestFooterLength
		corrupt[footerPosition+48] ^= 1
		putU32(corrupt[footerPosition+80:footerPosition+84], checksum(corrupt[footerPosition:footerPosition+80]))
		if _, err := UnmarshalManifest(corrupt); !errors.Is(err, ErrChecksum) {
			t.Fatalf("got %v", err)
		}
	})
	t.Run("reference length", func(t *testing.T) {
		corrupt := bytes.Clone(encoded)
		putU32(corrupt[ManifestHeaderLength:ManifestHeaderLength+4], 4)
		if _, err := UnmarshalManifest(corrupt); !errors.Is(err, ErrInvalid) {
			t.Fatalf("got %v", err)
		}
	})
	t.Run("reference order", func(t *testing.T) {
		raw := marshalManifestInGivenOrder(t, manifest)
		if _, err := UnmarshalManifest(raw); !errors.Is(err, ErrInvalid) {
			t.Fatalf("got %v", err)
		}
	})
}

func TestManifestRejectsInvalidReferences(t *testing.T) {
	tests := []struct {
		name string
		edit func(*Manifest)
	}{
		{"unknown Segment flag", func(value *Manifest) { value.SegmentReferences[0].Flags |= 0x80 }},
		{"unsafe local path", func(value *Manifest) {
			value.SegmentReferences[1].LocalPath = "segments/../secret"
		}},
		{"flag location mismatch", func(value *Manifest) { value.SegmentReferences[0].ObjectLocation = "" }},
		{"unaligned Segment size", func(value *Manifest) { value.SegmentReferences[0].FileSize++ }},
		{"unknown Artifact type", func(value *Manifest) { value.ArtifactReferences[0].ArtifactType = 99 }},
		{"duplicate Segment ID", func(value *Manifest) {
			value.SegmentReferences[0].SegmentID = value.SegmentReferences[1].SegmentID
		}},
		{"wrong total", func(value *Manifest) { value.Header.RecordCount++ }},
		{"wrong last Entry ID", func(value *Manifest) { value.Header.LastEntryID++ }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			value := sampleManifest()
			test.edit(&value)
			if _, err := MarshalManifest(value); !errors.Is(err, ErrInvalid) {
				t.Fatalf("got %v", err)
			}
		})
	}
}

func TestManifestGenerationAndEmptyRules(t *testing.T) {
	empty := Manifest{Header: ManifestHeader{FileID: testUUID(1)}}
	encoded, err := MarshalManifest(empty)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := UnmarshalManifest(encoded)
	if err != nil || decoded.Header.RecordCount != 0 {
		t.Fatalf("empty Manifest: %+v, %v", decoded, err)
	}

	bad := empty
	bad.Header.Generation = 2
	bad.Header.PreviousGeneration = 0
	bad.Header.PreviousManifestSHA256 = sha256.Sum256([]byte("previous"))
	if _, err := MarshalManifest(bad); !errors.Is(err, ErrInvalid) {
		t.Fatalf("generation chain: %v", err)
	}
}

func TestCurrentPointerRejectsInvalidNameAndCorruption(t *testing.T) {
	pointer := CurrentPointer{
		ManifestFileID: testUUID(1), ManifestSHA256: sha256.Sum256([]byte("manifest")),
		ManifestFileName: "../MANIFEST.bin",
	}
	if _, err := MarshalCurrentPointer(pointer); !errors.Is(err, ErrInvalid) {
		t.Fatalf("unsafe name: %v", err)
	}
	pointer.ManifestFileName = "MANIFEST.bin"
	encoded, err := MarshalCurrentPointer(pointer)
	if err != nil {
		t.Fatal(err)
	}
	encoded[len(encoded)-1] ^= 1
	if _, err := UnmarshalCurrentPointer(encoded); !errors.Is(err, ErrChecksum) {
		t.Fatalf("CRC corruption: %v", err)
	}
}

func FuzzUnmarshalManifest(f *testing.F) {
	seed, err := MarshalManifest(sampleManifest())
	if err != nil {
		f.Fatal(err)
	}
	f.Add(seed)
	f.Add([]byte("manifest"))
	f.Fuzz(func(t *testing.T, data []byte) {
		decoded, err := UnmarshalManifest(data)
		if err != nil {
			return
		}
		reencoded, err := MarshalManifest(decoded)
		if err != nil {
			t.Fatalf("valid decoded Manifest cannot be encoded: %v", err)
		}
		if !bytes.Equal(data, reencoded) {
			t.Fatal("successful Manifest decode did not round trip")
		}
	})
}

func marshalManifestInGivenOrder(t *testing.T, manifest Manifest) []byte {
	t.Helper()
	manifest.Header.SegmentCount = uint64(len(manifest.SegmentReferences))
	manifest.Header.ArtifactCount = uint32(len(manifest.ArtifactReferences))
	header, err := MarshalManifestHeader(manifest.Header)
	if err != nil {
		t.Fatal(err)
	}
	content := header
	for _, reference := range manifest.SegmentReferences {
		encoded, err := MarshalSegmentReference(reference)
		if err != nil {
			t.Fatal(err)
		}
		content = append(content, encoded...)
	}
	for _, reference := range manifest.ArtifactReferences {
		encoded, err := MarshalArtifactReference(reference)
		if err != nil {
			t.Fatal(err)
		}
		content = append(content, encoded...)
	}
	footer, err := MarshalManifestFooter(ManifestFooter{
		FileID: manifest.Header.FileID, Generation: manifest.Header.Generation,
		FileLength: uint64(len(content) + ManifestFooterLength), ContentSHA256: sha256.Sum256(content),
	})
	if err != nil {
		t.Fatal(err)
	}
	return append(content, footer...)
}
