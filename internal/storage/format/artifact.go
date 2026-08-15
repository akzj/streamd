package format

import (
	"bytes"
	"crypto/sha256"
)

var artifactFooterMagic = [8]byte{'A', 'R', 'T', 'E', 'N', 'D', 'V', '1'}

// ArtifactFooter seals a reconstructible immutable projection.
type ArtifactFooter struct {
	ArtifactType  ArtifactType
	ArtifactID    UUID
	FileLength    uint64
	ContentLength uint64
	ContentSHA256 [sha256.Size]byte
}

func NewArtifactFooter(kind ArtifactType, id UUID, content []byte) (ArtifactFooter, error) {
	fileLength, err := checkedAdd(uint64(len(content)), ArtifactFooterLength)
	if err != nil {
		return ArtifactFooter{}, err
	}
	footer := ArtifactFooter{ArtifactType: kind, ArtifactID: id, FileLength: fileLength, ContentLength: uint64(len(content)), ContentSHA256: sha256.Sum256(content)}
	if err := validateArtifactFooter(footer); err != nil {
		return ArtifactFooter{}, err
	}
	return footer, nil
}

func MarshalArtifactFooter(footer ArtifactFooter) ([]byte, error) {
	if err := validateArtifactFooter(footer); err != nil {
		return nil, err
	}
	b := make([]byte, ArtifactFooterLength)
	copy(b[0:8], artifactFooterMagic[:])
	putU16(b[8:10], VersionV1)
	putU16(b[10:12], ArtifactFooterLength)
	putU16(b[12:14], uint16(footer.ArtifactType))
	putU16(b[14:16], 0)
	copy(b[16:32], footer.ArtifactID[:])
	putU64(b[32:40], footer.FileLength)
	putU64(b[40:48], footer.ContentLength)
	copy(b[48:80], footer.ContentSHA256[:])
	putU32(b[80:84], checksum(b[:80]))
	return b, nil
}

func UnmarshalArtifactFooter(b []byte) (ArtifactFooter, error) {
	var footer ArtifactFooter
	if len(b) < ArtifactFooterLength {
		return footer, truncatedf("Artifact footer needs %d bytes, got %d", ArtifactFooterLength, len(b))
	}
	if len(b) != ArtifactFooterLength {
		return footer, invalidf("Artifact footer has trailing bytes")
	}
	if !bytes.Equal(b[:8], artifactFooterMagic[:]) {
		return footer, invalidf("Artifact footer magic is invalid")
	}
	if v := getU16(b[8:10]); v != VersionV1 {
		return footer, unsupportedVersion("Artifact footer", v)
	}
	if getU16(b[10:12]) != ArtifactFooterLength || getU16(b[14:16]) != 0 {
		return footer, invalidf("Artifact footer length or flags are invalid")
	}
	if err := expectZero(b[84:88], "Artifact footer reserved"); err != nil {
		return footer, err
	}
	if stored, actual := getU32(b[80:84]), checksum(b[:80]); stored != actual {
		return footer, checksumf("Artifact footer CRC32C is %08x, want %08x", stored, actual)
	}
	footer.ArtifactType = ArtifactType(getU16(b[12:14]))
	copy(footer.ArtifactID[:], b[16:32])
	footer.FileLength = getU64(b[32:40])
	footer.ContentLength = getU64(b[40:48])
	copy(footer.ContentSHA256[:], b[48:80])
	if err := validateArtifactFooter(footer); err != nil {
		return ArtifactFooter{}, err
	}
	return footer, nil
}

func VerifyArtifact(content, footerBytes []byte, expectedType ArtifactType, expectedID UUID) (ArtifactFooter, error) {
	footer, err := UnmarshalArtifactFooter(footerBytes)
	if err != nil {
		return ArtifactFooter{}, err
	}
	if footer.ArtifactType != expectedType || footer.ArtifactID != expectedID {
		return ArtifactFooter{}, invalidf("Artifact footer identity mismatch")
	}
	if footer.ContentLength != uint64(len(content)) || footer.FileLength != uint64(len(content)+len(footerBytes)) {
		return ArtifactFooter{}, invalidf("Artifact footer length mismatch")
	}
	if sha256.Sum256(content) != footer.ContentSHA256 {
		return ArtifactFooter{}, checksumf("Artifact content SHA-256 mismatch")
	}
	return footer, nil
}

func validateArtifactFooter(footer ArtifactFooter) error {
	if (footer.ArtifactType < ArtifactTailCatalog || footer.ArtifactType > ArtifactSnapshotManifest) && footer.ArtifactType != ArtifactReplicationState {
		return invalidf("Artifact footer type %d is invalid", footer.ArtifactType)
	}
	if isZeroUUID(footer.ArtifactID) || isZeroDigest(footer.ContentSHA256) {
		return invalidf("Artifact footer identity or digest is zero")
	}
	expected, err := checkedAdd(footer.ContentLength, ArtifactFooterLength)
	if err != nil || footer.FileLength != expected {
		return invalidf("Artifact footer file_length is %d, want %d", footer.FileLength, expected)
	}
	return nil
}
