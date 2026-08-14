package format

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"path"
	"slices"
	"strings"
	"unicode/utf8"
)

var (
	manifestMagic       = [8]byte{'S', 'T', 'R', 'M', 'M', 'A', 'N', '1'}
	manifestFooterMagic = [8]byte{'M', 'A', 'N', 'E', 'N', 'D', 'V', '1'}
	currentMagic        = [8]byte{'S', 'T', 'R', 'M', 'C', 'U', 'R', '1'}
)

const (
	SegmentRefHasLocal   uint32 = 1 << 0
	SegmentRefHasObject  uint32 = 1 << 1
	segmentRefKnownFlags        = SegmentRefHasLocal | SegmentRefHasObject

	segmentReferenceFixedLength  = 108
	artifactReferenceFixedLength = 84
	currentFixedLength           = 80
)

// ArtifactType identifies a sealed projection referenced by a Manifest.
type ArtifactType uint16

const (
	ArtifactTailCatalog      ArtifactType = 1
	ArtifactLocatorSnapshot  ArtifactType = 2
	ArtifactRegistrySnapshot ArtifactType = 3
	ArtifactLocatorPack      ArtifactType = 4
	ArtifactSnapshotManifest ArtifactType = 5
	ArtifactManifest         ArtifactType = 6
	ArtifactSegment          ArtifactType = 7
)

// ManifestHeader is the fixed V1 checkpoint description.
type ManifestHeader struct {
	FileID                 UUID
	Generation             uint64
	PreviousGeneration     uint64
	PreviousManifestSHA256 [sha256.Size]byte
	CreatedAt              int64
	LastEntryID            uint64
	LastEntryCRC32C        uint32
	RecordCount            uint64
	SegmentCount           uint64
	ArtifactCount          uint32
}

type SegmentReference struct {
	Flags          uint32
	SegmentID      UUID
	FileSize       uint64
	FirstEntryID   uint64
	LastEntryID    uint64
	StreamCount    uint64
	RecordCount    uint64
	LocalPath      string
	ObjectLocation string
	ContentSHA256  [sha256.Size]byte
}

type ArtifactReference struct {
	ArtifactType   ArtifactType
	FormatVersion  uint16
	Flags          uint32
	ArtifactID     UUID
	FileSize       uint64
	CoveredEntryID uint64
	Path           string
	ContentSHA256  [sha256.Size]byte
}

type ManifestFooter struct {
	FileID        UUID
	Generation    uint64
	FileLength    uint64
	ContentSHA256 [sha256.Size]byte
}

// Manifest is one complete immutable checkpoint file.
type Manifest struct {
	Header             ManifestHeader
	SegmentReferences  []SegmentReference
	ArtifactReferences []ArtifactReference
	Footer             ManifestFooter
}

func MarshalManifestHeader(header ManifestHeader) ([]byte, error) {
	if err := validateManifestHeader(header); err != nil {
		return nil, err
	}
	encoded := make([]byte, ManifestHeaderLength)
	copy(encoded[0:8], manifestMagic[:])
	putU16(encoded[8:10], VersionV1)
	putU16(encoded[10:12], ManifestHeaderLength)
	putU32(encoded[12:16], 0)
	copy(encoded[16:32], header.FileID[:])
	putU64(encoded[32:40], header.Generation)
	putU64(encoded[40:48], header.PreviousGeneration)
	copy(encoded[48:80], header.PreviousManifestSHA256[:])
	putI64(encoded[80:88], header.CreatedAt)
	putU64(encoded[88:96], header.LastEntryID)
	putU32(encoded[96:100], header.LastEntryCRC32C)
	putU64(encoded[104:112], header.RecordCount)
	putU64(encoded[112:120], header.SegmentCount)
	putU32(encoded[120:124], header.ArtifactCount)
	putU32(encoded[132:136], checksum(encoded[:132]))
	return encoded, nil
}

func UnmarshalManifestHeader(encoded []byte) (ManifestHeader, error) {
	var header ManifestHeader
	if len(encoded) < ManifestHeaderLength {
		return header, truncatedf("Manifest header needs %d bytes, got %d", ManifestHeaderLength, len(encoded))
	}
	if len(encoded) != ManifestHeaderLength {
		return header, invalidf("Manifest header has trailing bytes: %d", len(encoded)-ManifestHeaderLength)
	}
	if !bytes.Equal(encoded[0:8], manifestMagic[:]) {
		return header, invalidf("Manifest magic is %q", encoded[0:8])
	}
	if version := getU16(encoded[8:10]); version != VersionV1 {
		return header, unsupportedVersion("Manifest", version)
	}
	if length := getU16(encoded[10:12]); length != ManifestHeaderLength {
		return header, invalidf("Manifest header_length is %d", length)
	}
	if flags := getU32(encoded[12:16]); flags != 0 {
		return header, invalidf("Manifest flags contain unsupported bits: 0x%08x", flags)
	}
	if err := expectZero(encoded[100:104], "Manifest reserved_0"); err != nil {
		return header, err
	}
	if err := expectZero(encoded[124:132], "Manifest reserved fields"); err != nil {
		return header, err
	}
	if stored, actual := getU32(encoded[132:136]), checksum(encoded[:132]); stored != actual {
		return header, checksumf("Manifest header CRC32C is %08x, want %08x", stored, actual)
	}
	copy(header.FileID[:], encoded[16:32])
	header.Generation = getU64(encoded[32:40])
	header.PreviousGeneration = getU64(encoded[40:48])
	copy(header.PreviousManifestSHA256[:], encoded[48:80])
	header.CreatedAt = getI64(encoded[80:88])
	header.LastEntryID = getU64(encoded[88:96])
	header.LastEntryCRC32C = getU32(encoded[96:100])
	header.RecordCount = getU64(encoded[104:112])
	header.SegmentCount = getU64(encoded[112:120])
	header.ArtifactCount = getU32(encoded[120:124])
	if err := validateManifestHeader(header); err != nil {
		return ManifestHeader{}, err
	}
	return header, nil
}

func validateManifestHeader(header ManifestHeader) error {
	if isZeroUUID(header.FileID) {
		return invalidf("Manifest file ID is zero")
	}
	if header.Generation == 0 {
		if header.PreviousGeneration != 0 || !isZeroDigest(header.PreviousManifestSHA256) {
			return invalidf("Manifest generation 0 has a previous Manifest")
		}
	} else {
		if header.PreviousGeneration != header.Generation-1 {
			return invalidf("Manifest previous_generation is %d, want %d", header.PreviousGeneration, header.Generation-1)
		}
		if isZeroDigest(header.PreviousManifestSHA256) {
			return invalidf("Manifest previous SHA-256 is zero")
		}
	}
	if header.RecordCount == 0 {
		if header.LastEntryID != 0 || header.LastEntryCRC32C != 0 || header.SegmentCount != 0 {
			return invalidf("empty Manifest has a non-empty checkpoint")
		}
	} else if header.SegmentCount == 0 {
		return invalidf("non-empty Manifest has no Segment references")
	}
	return nil
}

func MarshalSegmentReference(reference SegmentReference) ([]byte, error) {
	if err := validateSegmentReference(reference); err != nil {
		return nil, err
	}
	length, err := referenceLength(segmentReferenceFixedLength, len(reference.LocalPath), len(reference.ObjectLocation))
	if err != nil {
		return nil, err
	}
	encoded := make([]byte, length)
	putU32(encoded[0:4], uint32(length))
	putU32(encoded[4:8], reference.Flags)
	copy(encoded[8:24], reference.SegmentID[:])
	putU64(encoded[24:32], reference.FileSize)
	putU64(encoded[32:40], reference.FirstEntryID)
	putU64(encoded[40:48], reference.LastEntryID)
	putU64(encoded[48:56], reference.StreamCount)
	putU64(encoded[56:64], reference.RecordCount)
	putU16(encoded[64:66], uint16(len(reference.LocalPath)))
	putU16(encoded[66:68], uint16(len(reference.ObjectLocation)))
	copy(encoded[72:104], reference.ContentSHA256[:])
	position := 104
	position += copy(encoded[position:], reference.LocalPath)
	position += copy(encoded[position:], reference.ObjectLocation)
	putU32(encoded[position:position+4], checksum(encoded[:position]))
	return encoded, nil
}

func UnmarshalSegmentReference(encoded []byte) (SegmentReference, error) {
	var reference SegmentReference
	if len(encoded) < segmentReferenceFixedLength {
		return reference, truncatedf("Segment Reference needs at least %d bytes, got %d", segmentReferenceFixedLength, len(encoded))
	}
	declared := getU32(encoded[0:4])
	if uint64(declared) != uint64(len(encoded)) {
		return reference, invalidf("Segment Reference entry_length is %d, got %d bytes", declared, len(encoded))
	}
	localLength := int(getU16(encoded[64:66]))
	objectLength := int(getU16(encoded[66:68]))
	expected := segmentReferenceFixedLength + localLength + objectLength
	if len(encoded) != expected {
		return reference, invalidf("Segment Reference length is %d, want %d", len(encoded), expected)
	}
	if err := expectZero(encoded[68:72], "Segment Reference reserved"); err != nil {
		return reference, err
	}
	crcPosition := len(encoded) - 4
	if stored, actual := getU32(encoded[crcPosition:]), checksum(encoded[:crcPosition]); stored != actual {
		return reference, checksumf("Segment Reference CRC32C is %08x, want %08x", stored, actual)
	}
	reference.Flags = getU32(encoded[4:8])
	copy(reference.SegmentID[:], encoded[8:24])
	reference.FileSize = getU64(encoded[24:32])
	reference.FirstEntryID = getU64(encoded[32:40])
	reference.LastEntryID = getU64(encoded[40:48])
	reference.StreamCount = getU64(encoded[48:56])
	reference.RecordCount = getU64(encoded[56:64])
	copy(reference.ContentSHA256[:], encoded[72:104])
	position := 104
	reference.LocalPath = string(bytes.Clone(encoded[position : position+localLength]))
	position += localLength
	reference.ObjectLocation = string(bytes.Clone(encoded[position : position+objectLength]))
	if err := validateSegmentReference(reference); err != nil {
		return SegmentReference{}, err
	}
	return reference, nil
}

func validateSegmentReference(reference SegmentReference) error {
	if reference.Flags&^segmentRefKnownFlags != 0 || reference.Flags&segmentRefKnownFlags == 0 {
		return invalidf("Segment Reference flags are invalid: 0x%08x", reference.Flags)
	}
	if isZeroUUID(reference.SegmentID) || reference.FileSize == 0 || reference.StreamCount == 0 || reference.RecordCount == 0 {
		return invalidf("Segment Reference has zero identity, size, or counts")
	}
	if reference.FileSize < 5*SegmentSectionAlignment || reference.FileSize%SegmentSectionAlignment != 0 {
		return invalidf("Segment Reference file_size is not a possible V1 Segment size: %d", reference.FileSize)
	}
	if reference.FirstEntryID > reference.LastEntryID {
		return invalidf("Segment Reference first_entry_id is after last_entry_id")
	}
	if isZeroDigest(reference.ContentSHA256) {
		return invalidf("Segment Reference content SHA-256 is zero")
	}
	if reference.Flags&SegmentRefHasLocal != 0 {
		if err := validateRelativePath(reference.LocalPath, "Segment local path"); err != nil {
			return err
		}
	} else if reference.LocalPath != "" {
		return invalidf("Segment local path exists without HAS_LOCAL")
	}
	if reference.Flags&SegmentRefHasObject != 0 {
		if reference.ObjectLocation == "" || !utf8.ValidString(reference.ObjectLocation) || strings.ContainsRune(reference.ObjectLocation, 0) {
			return invalidf("Segment object location is invalid")
		}
	} else if reference.ObjectLocation != "" {
		return invalidf("Segment object location exists without HAS_OBJECT")
	}
	if len(reference.LocalPath) > int(^uint16(0)) || len(reference.ObjectLocation) > int(^uint16(0)) {
		return fmtTooLarge("Segment Reference location", len(reference.LocalPath)+len(reference.ObjectLocation), ^uint16(0))
	}
	return nil
}

func MarshalArtifactReference(reference ArtifactReference) ([]byte, error) {
	if err := validateArtifactReference(reference); err != nil {
		return nil, err
	}
	length, err := referenceLength(artifactReferenceFixedLength, len(reference.Path))
	if err != nil {
		return nil, err
	}
	encoded := make([]byte, length)
	putU32(encoded[0:4], uint32(length))
	putU16(encoded[4:6], uint16(reference.ArtifactType))
	putU16(encoded[6:8], reference.FormatVersion)
	putU32(encoded[8:12], reference.Flags)
	copy(encoded[12:28], reference.ArtifactID[:])
	putU64(encoded[28:36], reference.FileSize)
	putU64(encoded[36:44], reference.CoveredEntryID)
	putU16(encoded[44:46], uint16(len(reference.Path)))
	copy(encoded[48:80], reference.ContentSHA256[:])
	position := 80 + copy(encoded[80:], reference.Path)
	putU32(encoded[position:position+4], checksum(encoded[:position]))
	return encoded, nil
}

func UnmarshalArtifactReference(encoded []byte) (ArtifactReference, error) {
	var reference ArtifactReference
	if len(encoded) < artifactReferenceFixedLength {
		return reference, truncatedf("Artifact Reference needs at least %d bytes, got %d", artifactReferenceFixedLength, len(encoded))
	}
	declared := getU32(encoded[0:4])
	if uint64(declared) != uint64(len(encoded)) {
		return reference, invalidf("Artifact Reference entry_length is %d, got %d bytes", declared, len(encoded))
	}
	pathLength := int(getU16(encoded[44:46]))
	expected := artifactReferenceFixedLength + pathLength
	if len(encoded) != expected {
		return reference, invalidf("Artifact Reference length is %d, want %d", len(encoded), expected)
	}
	if err := expectZero(encoded[46:48], "Artifact Reference reserved"); err != nil {
		return reference, err
	}
	crcPosition := len(encoded) - 4
	if stored, actual := getU32(encoded[crcPosition:]), checksum(encoded[:crcPosition]); stored != actual {
		return reference, checksumf("Artifact Reference CRC32C is %08x, want %08x", stored, actual)
	}
	reference.ArtifactType = ArtifactType(getU16(encoded[4:6]))
	reference.FormatVersion = getU16(encoded[6:8])
	reference.Flags = getU32(encoded[8:12])
	copy(reference.ArtifactID[:], encoded[12:28])
	reference.FileSize = getU64(encoded[28:36])
	reference.CoveredEntryID = getU64(encoded[36:44])
	copy(reference.ContentSHA256[:], encoded[48:80])
	reference.Path = string(bytes.Clone(encoded[80:crcPosition]))
	if err := validateArtifactReference(reference); err != nil {
		return ArtifactReference{}, err
	}
	return reference, nil
}

func validateArtifactReference(reference ArtifactReference) error {
	if reference.ArtifactType < ArtifactTailCatalog || reference.ArtifactType > ArtifactRegistrySnapshot {
		return invalidf("unknown Artifact type %d", reference.ArtifactType)
	}
	if reference.FormatVersion != VersionV1 || reference.Flags != 0 {
		return invalidf("Artifact Reference version or flags are unsupported")
	}
	if isZeroUUID(reference.ArtifactID) || reference.FileSize == 0 || isZeroDigest(reference.ContentSHA256) {
		return invalidf("Artifact Reference has zero identity, size, or SHA-256")
	}
	if err := validateRelativePath(reference.Path, "Artifact path"); err != nil {
		return err
	}
	if len(reference.Path) > int(^uint16(0)) {
		return fmtTooLarge("Artifact path", len(reference.Path), ^uint16(0))
	}
	return nil
}

func MarshalManifestFooter(footer ManifestFooter) ([]byte, error) {
	if err := validateManifestFooter(footer); err != nil {
		return nil, err
	}
	encoded := make([]byte, ManifestFooterLength)
	copy(encoded[0:8], manifestFooterMagic[:])
	putU16(encoded[8:10], VersionV1)
	putU16(encoded[10:12], ManifestFooterLength)
	putU32(encoded[12:16], 0)
	copy(encoded[16:32], footer.FileID[:])
	putU64(encoded[32:40], footer.Generation)
	putU64(encoded[40:48], footer.FileLength)
	copy(encoded[48:80], footer.ContentSHA256[:])
	putU32(encoded[80:84], checksum(encoded[:80]))
	return encoded, nil
}

func UnmarshalManifestFooter(encoded []byte) (ManifestFooter, error) {
	var footer ManifestFooter
	if len(encoded) < ManifestFooterLength {
		return footer, truncatedf("Manifest footer needs %d bytes, got %d", ManifestFooterLength, len(encoded))
	}
	if len(encoded) != ManifestFooterLength {
		return footer, invalidf("Manifest footer has trailing bytes: %d", len(encoded)-ManifestFooterLength)
	}
	if !bytes.Equal(encoded[0:8], manifestFooterMagic[:]) {
		return footer, invalidf("Manifest footer magic is %q", encoded[0:8])
	}
	if version := getU16(encoded[8:10]); version != VersionV1 {
		return footer, unsupportedVersion("Manifest footer", version)
	}
	if length := getU16(encoded[10:12]); length != ManifestFooterLength {
		return footer, invalidf("Manifest footer_length is %d", length)
	}
	if flags := getU32(encoded[12:16]); flags != 0 {
		return footer, invalidf("Manifest footer flags contain unsupported bits: 0x%08x", flags)
	}
	if err := expectZero(encoded[84:88], "Manifest footer reserved"); err != nil {
		return footer, err
	}
	if stored, actual := getU32(encoded[80:84]), checksum(encoded[:80]); stored != actual {
		return footer, checksumf("Manifest footer CRC32C is %08x, want %08x", stored, actual)
	}
	copy(footer.FileID[:], encoded[16:32])
	footer.Generation = getU64(encoded[32:40])
	footer.FileLength = getU64(encoded[40:48])
	copy(footer.ContentSHA256[:], encoded[48:80])
	if err := validateManifestFooter(footer); err != nil {
		return ManifestFooter{}, err
	}
	return footer, nil
}

func validateManifestFooter(footer ManifestFooter) error {
	if isZeroUUID(footer.FileID) || isZeroDigest(footer.ContentSHA256) {
		return invalidf("Manifest footer has zero identity or SHA-256")
	}
	if footer.FileLength < ManifestHeaderLength+ManifestFooterLength {
		return invalidf("Manifest footer file_length is too small: %d", footer.FileLength)
	}
	return nil
}

// MarshalManifest canonicalizes Reference order and returns a complete file.
func MarshalManifest(manifest Manifest) ([]byte, error) {
	segments := slices.Clone(manifest.SegmentReferences)
	artifacts := slices.Clone(manifest.ArtifactReferences)
	slices.SortFunc(segments, compareSegmentReferences)
	slices.SortFunc(artifacts, compareArtifactReferences)
	if err := validateCanonicalReferenceOrder(segments, artifacts); err != nil {
		return nil, err
	}
	manifest.Header.SegmentCount = uint64(len(segments))
	manifest.Header.ArtifactCount = uint32(len(artifacts))
	if uint64(manifest.Header.ArtifactCount) != uint64(len(artifacts)) {
		return nil, fmtTooLarge("Manifest Artifact count", len(artifacts), ^uint32(0))
	}
	if err := validateManifestReferences(manifest.Header, segments, artifacts); err != nil {
		return nil, err
	}
	header, err := MarshalManifestHeader(manifest.Header)
	if err != nil {
		return nil, err
	}
	content := header
	for i, reference := range segments {
		encoded, err := MarshalSegmentReference(reference)
		if err != nil {
			return nil, fmt.Errorf("Segment Reference %d: %w", i, err)
		}
		content = append(content, encoded...)
	}
	for i, reference := range artifacts {
		encoded, err := MarshalArtifactReference(reference)
		if err != nil {
			return nil, fmt.Errorf("Artifact Reference %d: %w", i, err)
		}
		content = append(content, encoded...)
	}
	fileLength, err := checkedAdd(uint64(len(content)), ManifestFooterLength)
	if err != nil {
		return nil, err
	}
	footer := ManifestFooter{
		FileID:        manifest.Header.FileID,
		Generation:    manifest.Header.Generation,
		FileLength:    fileLength,
		ContentSHA256: sha256.Sum256(content),
	}
	encodedFooter, err := MarshalManifestFooter(footer)
	if err != nil {
		return nil, err
	}
	return append(content, encodedFooter...), nil
}

func UnmarshalManifest(encoded []byte) (Manifest, error) {
	var manifest Manifest
	minimum := ManifestHeaderLength + ManifestFooterLength
	if len(encoded) < minimum {
		return manifest, truncatedf("Manifest needs at least %d bytes, got %d", minimum, len(encoded))
	}
	header, err := UnmarshalManifestHeader(encoded[:ManifestHeaderLength])
	if err != nil {
		return manifest, err
	}
	position := ManifestHeaderLength
	if header.SegmentCount > uint64((len(encoded)-position-ManifestFooterLength)/segmentReferenceFixedLength) {
		return manifest, truncatedf("Manifest Segment Reference count exceeds remaining bytes")
	}
	segmentCapacity, err := checkedInt(header.SegmentCount, "Manifest Segment count")
	if err != nil {
		return manifest, err
	}
	segments := make([]SegmentReference, 0, segmentCapacity)
	for i := uint64(0); i < header.SegmentCount; i++ {
		entry, next, err := nextReference(encoded, position, segmentReferenceFixedLength, "Segment")
		if err != nil {
			return manifest, fmt.Errorf("Segment Reference %d: %w", i, err)
		}
		reference, err := UnmarshalSegmentReference(entry)
		if err != nil {
			return manifest, fmt.Errorf("Segment Reference %d: %w", i, err)
		}
		segments = append(segments, reference)
		position = next
	}
	if uint64(header.ArtifactCount) > uint64((len(encoded)-position-ManifestFooterLength)/artifactReferenceFixedLength) {
		return manifest, truncatedf("Manifest Artifact Reference count exceeds remaining bytes")
	}
	artifacts := make([]ArtifactReference, 0, int(header.ArtifactCount))
	for i := uint32(0); i < header.ArtifactCount; i++ {
		entry, next, err := nextReference(encoded, position, artifactReferenceFixedLength, "Artifact")
		if err != nil {
			return manifest, fmt.Errorf("Artifact Reference %d: %w", i, err)
		}
		reference, err := UnmarshalArtifactReference(entry)
		if err != nil {
			return manifest, fmt.Errorf("Artifact Reference %d: %w", i, err)
		}
		artifacts = append(artifacts, reference)
		position = next
	}
	remaining := len(encoded) - position
	if remaining < ManifestFooterLength {
		return manifest, truncatedf("Manifest Footer needs %d bytes, got %d", ManifestFooterLength, remaining)
	}
	if remaining > ManifestFooterLength {
		return manifest, invalidf("Manifest has %d unexpected bytes before Footer", remaining-ManifestFooterLength)
	}
	footer, err := UnmarshalManifestFooter(encoded[position:])
	if err != nil {
		return manifest, err
	}
	if footer.FileID != header.FileID || footer.Generation != header.Generation || footer.FileLength != uint64(len(encoded)) {
		return manifest, invalidf("Manifest Footer does not match Header or file length")
	}
	if digest := sha256.Sum256(encoded[:position]); digest != footer.ContentSHA256 {
		return manifest, checksumf("Manifest content SHA-256 mismatch")
	}
	if err := validateCanonicalReferenceOrder(segments, artifacts); err != nil {
		return manifest, err
	}
	if err := validateManifestReferences(header, segments, artifacts); err != nil {
		return manifest, err
	}
	manifest.Header = header
	manifest.SegmentReferences = segments
	manifest.ArtifactReferences = artifacts
	manifest.Footer = footer
	return manifest, nil
}

func validateManifestReferences(header ManifestHeader, segments []SegmentReference, artifacts []ArtifactReference) error {
	if uint64(len(segments)) != header.SegmentCount || uint64(len(artifacts)) != uint64(header.ArtifactCount) {
		return invalidf("Manifest Reference counts do not match Header")
	}
	var records uint64
	var highestEntryID uint64
	for i, reference := range segments {
		if err := validateSegmentReference(reference); err != nil {
			return fmt.Errorf("Segment Reference %d: %w", i, err)
		}
		var err error
		records, err = checkedAdd(records, reference.RecordCount)
		if err != nil {
			return invalidf("Manifest Segment record count overflows")
		}
		if i == 0 || reference.LastEntryID > highestEntryID {
			highestEntryID = reference.LastEntryID
		}
	}
	for i, reference := range artifacts {
		if err := validateArtifactReference(reference); err != nil {
			return fmt.Errorf("Artifact Reference %d: %w", i, err)
		}
		if reference.CoveredEntryID > header.LastEntryID {
			return invalidf("Artifact Reference %d covers Entry ID after checkpoint", i)
		}
	}
	if records != header.RecordCount {
		return invalidf("Manifest Segment record count is %d, want %d", records, header.RecordCount)
	}
	if header.RecordCount > 0 && highestEntryID != header.LastEntryID {
		return invalidf("Manifest highest Segment Entry ID is %d, want %d", highestEntryID, header.LastEntryID)
	}
	return nil
}

func validateCanonicalReferenceOrder(segments []SegmentReference, artifacts []ArtifactReference) error {
	for i := 1; i < len(segments); i++ {
		if compareSegmentReferences(segments[i-1], segments[i]) >= 0 {
			return invalidf("Segment References are not strictly sorted at entry %d", i)
		}
	}
	for i := 1; i < len(artifacts); i++ {
		if compareArtifactReferences(artifacts[i-1], artifacts[i]) >= 0 {
			return invalidf("Artifact References are not strictly sorted at entry %d", i)
		}
	}
	return nil
}

func compareSegmentReferences(left, right SegmentReference) int {
	return bytes.Compare(left.SegmentID[:], right.SegmentID[:])
}

func compareArtifactReferences(left, right ArtifactReference) int {
	if left.ArtifactType < right.ArtifactType {
		return -1
	}
	if left.ArtifactType > right.ArtifactType {
		return 1
	}
	return bytes.Compare(left.ArtifactID[:], right.ArtifactID[:])
}

func nextReference(encoded []byte, position, minimum int, kind string) ([]byte, int, error) {
	if position < 0 || position > len(encoded)-4 {
		return nil, position, truncatedf("Manifest %s Reference length is missing", kind)
	}
	length := uint64(getU32(encoded[position : position+4]))
	if length < uint64(minimum) {
		return nil, position, invalidf("Manifest %s Reference length is %d, minimum is %d", kind, length, minimum)
	}
	end64, err := checkedAdd(uint64(position), length)
	if err != nil || end64 > uint64(len(encoded)) {
		return nil, position, truncatedf("Manifest %s Reference exceeds file", kind)
	}
	end, err := checkedInt(end64, "Manifest Reference end")
	if err != nil {
		return nil, position, err
	}
	return encoded[position:end], end, nil
}

func referenceLength(fixed int, variable ...int) (int, error) {
	length := uint64(fixed)
	for _, part := range variable {
		if part < 0 {
			return 0, invalidf("negative Reference length")
		}
		var err error
		length, err = checkedAdd(length, uint64(part))
		if err != nil {
			return 0, err
		}
	}
	if length > uint64(^uint32(0)) {
		return 0, fmtTooLarge("Reference", length, ^uint32(0))
	}
	return checkedInt(length, "Reference length")
}

func validateRelativePath(value, field string) error {
	if value == "" || !utf8.ValidString(value) || strings.ContainsRune(value, 0) || strings.Contains(value, "\\") {
		return invalidf("%s is empty or invalid UTF-8/path syntax", field)
	}
	if strings.HasPrefix(value, "/") || path.Clean(value) != value || value == "." {
		return invalidf("%s is not a canonical relative path", field)
	}
	for _, component := range strings.Split(value, "/") {
		if component == "" || component == "." || component == ".." {
			return invalidf("%s contains an unsafe component", field)
		}
	}
	return nil
}

// CurrentPointer identifies the only published Manifest.
type CurrentPointer struct {
	Generation       uint64
	ManifestFileID   UUID
	ManifestSHA256   [sha256.Size]byte
	ManifestFileName string
}

func MarshalCurrentPointer(pointer CurrentPointer) ([]byte, error) {
	if err := validateCurrentPointer(pointer); err != nil {
		return nil, err
	}
	length := currentFixedLength + len(pointer.ManifestFileName)
	encoded := make([]byte, length)
	copy(encoded[0:8], currentMagic[:])
	putU16(encoded[8:10], VersionV1)
	putU16(encoded[10:12], uint16(length))
	putU32(encoded[12:16], 0)
	putU64(encoded[16:24], pointer.Generation)
	copy(encoded[24:40], pointer.ManifestFileID[:])
	putU16(encoded[40:42], uint16(len(pointer.ManifestFileName)))
	copy(encoded[44:76], pointer.ManifestSHA256[:])
	crcPosition := 76 + copy(encoded[76:], pointer.ManifestFileName)
	putU32(encoded[crcPosition:crcPosition+4], checksum(encoded[:crcPosition]))
	return encoded, nil
}

func UnmarshalCurrentPointer(encoded []byte) (CurrentPointer, error) {
	var pointer CurrentPointer
	if len(encoded) < currentFixedLength {
		return pointer, truncatedf("CURRENT needs at least %d bytes, got %d", currentFixedLength, len(encoded))
	}
	if !bytes.Equal(encoded[0:8], currentMagic[:]) {
		return pointer, invalidf("CURRENT magic is %q", encoded[0:8])
	}
	if version := getU16(encoded[8:10]); version != VersionV1 {
		return pointer, unsupportedVersion("CURRENT", version)
	}
	if length := getU16(encoded[10:12]); int(length) != len(encoded) {
		return pointer, invalidf("CURRENT length is %d, got %d bytes", length, len(encoded))
	}
	if flags := getU32(encoded[12:16]); flags != 0 {
		return pointer, invalidf("CURRENT flags contain unsupported bits: 0x%08x", flags)
	}
	if err := expectZero(encoded[42:44], "CURRENT reserved"); err != nil {
		return pointer, err
	}
	nameLength := int(getU16(encoded[40:42]))
	if currentFixedLength+nameLength != len(encoded) {
		return pointer, invalidf("CURRENT Manifest name length does not match file")
	}
	crcPosition := len(encoded) - 4
	if stored, actual := getU32(encoded[crcPosition:]), checksum(encoded[:crcPosition]); stored != actual {
		return pointer, checksumf("CURRENT CRC32C is %08x, want %08x", stored, actual)
	}
	pointer.Generation = getU64(encoded[16:24])
	copy(pointer.ManifestFileID[:], encoded[24:40])
	copy(pointer.ManifestSHA256[:], encoded[44:76])
	pointer.ManifestFileName = string(bytes.Clone(encoded[76:crcPosition]))
	if err := validateCurrentPointer(pointer); err != nil {
		return CurrentPointer{}, err
	}
	return pointer, nil
}

func validateCurrentPointer(pointer CurrentPointer) error {
	if isZeroUUID(pointer.ManifestFileID) || isZeroDigest(pointer.ManifestSHA256) {
		return invalidf("CURRENT has zero Manifest identity or SHA-256")
	}
	name := pointer.ManifestFileName
	if name == "" || !utf8.ValidString(name) || strings.ContainsRune(name, 0) || strings.ContainsAny(name, "/\\") || name == "." || name == ".." {
		return invalidf("CURRENT Manifest name is not one valid filename")
	}
	if currentFixedLength+len(name) > int(^uint16(0)) || len(name) > int(^uint16(0)) {
		return fmtTooLarge("CURRENT", currentFixedLength+len(name), ^uint16(0))
	}
	return nil
}
