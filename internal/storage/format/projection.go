package format

import (
	"bytes"
	"crypto/sha256"
	"unicode/utf8"
)

var locatorSnapshotMagic = [8]byte{'S', 'T', 'R', 'M', 'L', 'O', 'C', 'S'}
var registryRecordMagic = [8]byte{'S', 'T', 'R', 'M', 'R', 'E', 'G', '1'}
var registrySnapshotMagic = [8]byte{'S', 'T', 'R', 'M', 'R', 'E', 'G', 'R'}
var snapshotMagic = [8]byte{'S', 'T', 'R', 'M', 'S', 'N', 'P', '1'}

const locatorPackReferenceFixedLength = 80
const registryBlockIndexFixedLength = 28
const registryEntryFixedLength = 36
const registryRecordFixedLength = 36
const snapshotArtifactFixedLength = 80
const SnapshotFlagEmpty uint32 = 1

type LocatorSnapshotHeader struct {
	ArtifactID            UUID
	ManifestGeneration    uint64
	CoveredEntryID        uint64
	TailCatalogArtifactID UUID
	PackCount             uint32
	RootCount             uint32
	CreatedAt             int64
}

func MarshalLocatorSnapshotHeader(h LocatorSnapshotHeader) ([]byte, error) {
	if isZeroUUID(h.ArtifactID) || isZeroUUID(h.TailCatalogArtifactID) {
		return nil, invalidf("Locator Snapshot identity is zero")
	}
	b := make([]byte, LocatorSnapshotHeaderLength)
	copy(b[:8], locatorSnapshotMagic[:])
	putU16(b[8:10], VersionV1)
	putU16(b[10:12], LocatorSnapshotHeaderLength)
	copy(b[16:32], h.ArtifactID[:])
	putU64(b[32:40], h.ManifestGeneration)
	putU64(b[40:48], h.CoveredEntryID)
	copy(b[48:64], h.TailCatalogArtifactID[:])
	putU32(b[64:68], h.PackCount)
	putU32(b[68:72], h.RootCount)
	putI64(b[72:80], h.CreatedAt)
	putU32(b[80:84], checksum(b[:80]))
	return b, nil
}
func UnmarshalLocatorSnapshotHeader(b []byte) (LocatorSnapshotHeader, error) {
	var h LocatorSnapshotHeader
	if len(b) < LocatorSnapshotHeaderLength {
		return h, truncatedf("Locator Snapshot header needs %d bytes", LocatorSnapshotHeaderLength)
	}
	if len(b) != LocatorSnapshotHeaderLength {
		return h, invalidf("Locator Snapshot header has trailing bytes")
	}
	if !bytes.Equal(b[:8], locatorSnapshotMagic[:]) {
		return h, invalidf("Locator Snapshot magic is invalid")
	}
	if v := getU16(b[8:10]); v != VersionV1 {
		return h, unsupportedVersion("Locator Snapshot", v)
	}
	if getU16(b[10:12]) != LocatorSnapshotHeaderLength || getU32(b[12:16]) != 0 {
		return h, invalidf("Locator Snapshot fixed fields are invalid")
	}
	if err := expectZero(b[84:88], "Locator Snapshot reserved"); err != nil {
		return h, err
	}
	if stored, actual := getU32(b[80:84]), checksum(b[:80]); stored != actual {
		return h, checksumf("Locator Snapshot header CRC mismatch")
	}
	copy(h.ArtifactID[:], b[16:32])
	h.ManifestGeneration = getU64(b[32:40])
	h.CoveredEntryID = getU64(b[40:48])
	copy(h.TailCatalogArtifactID[:], b[48:64])
	h.PackCount = getU32(b[64:68])
	h.RootCount = getU32(b[68:72])
	h.CreatedAt = getI64(b[72:80])
	if isZeroUUID(h.ArtifactID) || isZeroUUID(h.TailCatalogArtifactID) {
		return LocatorSnapshotHeader{}, invalidf("Locator Snapshot identity is zero")
	}
	return h, nil
}

type LocatorPackReference struct {
	Flags         uint32
	PackID        UUID
	FileSize      uint64
	PageCount     uint64
	ContentSHA256 [sha256.Size]byte
	Path          string
}

func MarshalLocatorPackReference(r LocatorPackReference) ([]byte, error) {
	if err := validateLocatorPackReference(r); err != nil {
		return nil, err
	}
	if r.Flags != 0 || isZeroUUID(r.PackID) || r.FileSize == 0 || r.PageCount == 0 || isZeroDigest(r.ContentSHA256) {
		return nil, invalidf("Locator Pack Reference fields are invalid")
	}
	if err := validateRelativePath(r.Path, "Locator Pack path"); err != nil {
		return nil, err
	}
	if len(r.Path) > int(^uint16(0)) {
		return nil, fmtTooLarge("Locator Pack path", len(r.Path), ^uint16(0))
	}
	n := locatorPackReferenceFixedLength + len(r.Path)
	b := make([]byte, n)
	putU32(b[:4], uint32(n))
	copy(b[8:24], r.PackID[:])
	putU64(b[24:32], r.FileSize)
	putU64(b[32:40], r.PageCount)
	copy(b[40:72], r.ContentSHA256[:])
	putU16(b[72:74], uint16(len(r.Path)))
	copy(b[76:], r.Path)
	putU32(b[n-4:], checksum(b[:n-4]))
	return b, nil
}
func UnmarshalLocatorPackReference(b []byte) (LocatorPackReference, error) {
	var r LocatorPackReference
	if len(b) < locatorPackReferenceFixedLength {
		return r, truncatedf("Locator Pack Reference is truncated")
	}
	n := int(getU32(b[:4]))
	pathLen := int(getU16(b[72:74]))
	if n != len(b) || n != locatorPackReferenceFixedLength+pathLen {
		return r, invalidf("Locator Pack Reference length is invalid")
	}
	if getU32(b[4:8]) != 0 {
		return r, invalidf("Locator Pack Reference flags are invalid")
	}
	if err := expectZero(b[74:76], "Locator Pack Reference reserved"); err != nil {
		return r, err
	}
	if stored, actual := getU32(b[n-4:]), checksum(b[:n-4]); stored != actual {
		return r, checksumf("Locator Pack Reference CRC mismatch")
	}
	copy(r.PackID[:], b[8:24])
	r.FileSize = getU64(b[24:32])
	r.PageCount = getU64(b[32:40])
	copy(r.ContentSHA256[:], b[40:72])
	r.Path = string(bytes.Clone(b[76 : n-4]))
	if isZeroUUID(r.PackID) || r.FileSize == 0 || r.PageCount == 0 || isZeroDigest(r.ContentSHA256) {
		return LocatorPackReference{}, invalidf("Locator Pack Reference fields are invalid")
	}
	if err := validateRelativePath(r.Path, "Locator Pack path"); err != nil {
		return LocatorPackReference{}, err
	}
	if err := validateLocatorPackReference(r); err != nil {
		return LocatorPackReference{}, err
	}
	return r, nil
}

func validateLocatorPackReference(r LocatorPackReference) error {
	pageBytes, err := checkedMul(r.PageCount, LocatorPageLength, "Locator Pack pages")
	if err != nil {
		return err
	}
	want, err := checkedAdd(SegmentSectionAlignment, pageBytes, ArtifactFooterLength)
	if err != nil || r.FileSize != want {
		return invalidf("Locator Pack file_size is %d, want %d", r.FileSize, want)
	}
	return nil
}

type LocatorRootEntry struct {
	StreamID    uint64
	PackID      UUID
	PageOrdinal uint32
}

func MarshalLocatorRootEntry(r LocatorRootEntry) ([]byte, error) {
	if isZeroUUID(r.PackID) {
		return nil, invalidf("Locator Root Pack ID is zero")
	}
	b := make([]byte, LocatorRootEntryLength)
	putU64(b[:8], r.StreamID)
	copy(b[8:24], r.PackID[:])
	putU32(b[24:28], r.PageOrdinal)
	putU32(b[32:36], checksum(b[:32]))
	return b, nil
}
func UnmarshalLocatorRootEntry(b []byte) (LocatorRootEntry, error) {
	var r LocatorRootEntry
	if len(b) < LocatorRootEntryLength {
		return r, truncatedf("Locator Root Entry is truncated")
	}
	if len(b) != LocatorRootEntryLength {
		return r, invalidf("Locator Root Entry has trailing bytes")
	}
	if err := expectZero(b[28:32], "Locator Root reserved_0"); err != nil {
		return r, err
	}
	if err := expectZero(b[36:40], "Locator Root reserved_1"); err != nil {
		return r, err
	}
	if stored, actual := getU32(b[32:36]), checksum(b[:32]); stored != actual {
		return r, checksumf("Locator Root CRC mismatch")
	}
	r.StreamID = getU64(b[:8])
	copy(r.PackID[:], b[8:24])
	r.PageOrdinal = getU32(b[24:28])
	if isZeroUUID(r.PackID) {
		return LocatorRootEntry{}, invalidf("Locator Root Pack ID is zero")
	}
	return r, nil
}

type RegistryRecord struct {
	AssignedStreamID uint64
	Namespace        string
	StreamName       string
}

func MarshalRegistryRecord(r RegistryRecord) ([]byte, error) {
	if err := validateRegistryName(r.AssignedStreamID, r.Namespace, r.StreamName); err != nil {
		return nil, err
	}
	n := registryRecordFixedLength + len(r.Namespace) + len(r.StreamName)
	b := make([]byte, n)
	copy(b[:8], registryRecordMagic[:])
	putU16(b[8:10], VersionV1)
	putU32(b[12:16], uint32(n))
	putU64(b[16:24], r.AssignedStreamID)
	putU16(b[24:26], uint16(len(r.Namespace)))
	putU16(b[26:28], uint16(len(r.StreamName)))
	pos := 32
	pos += copy(b[pos:], r.Namespace)
	pos += copy(b[pos:], r.StreamName)
	putU32(b[pos:], checksum(b[:pos]))
	return b, nil
}
func UnmarshalRegistryRecord(b []byte) (RegistryRecord, error) {
	var r RegistryRecord
	if len(b) < registryRecordFixedLength {
		return r, truncatedf("Registry Record is truncated")
	}
	if !bytes.Equal(b[:8], registryRecordMagic[:]) {
		return r, invalidf("Registry Record magic is invalid")
	}
	if v := getU16(b[8:10]); v != VersionV1 {
		return r, unsupportedVersion("Registry Record", v)
	}
	ns, nm := int(getU16(b[24:26])), int(getU16(b[26:28]))
	if getU16(b[10:12]) != 0 || int(getU32(b[12:16])) != len(b) || len(b) != registryRecordFixedLength+ns+nm {
		return r, invalidf("Registry Record length or flags are invalid")
	}
	if err := expectZero(b[28:32], "Registry Record reserved"); err != nil {
		return r, err
	}
	if stored, actual := getU32(b[len(b)-4:]), checksum(b[:len(b)-4]); stored != actual {
		return r, checksumf("Registry Record CRC mismatch")
	}
	r.AssignedStreamID = getU64(b[16:24])
	r.Namespace = string(bytes.Clone(b[32 : 32+ns]))
	r.StreamName = string(bytes.Clone(b[32+ns : len(b)-4]))
	if err := validateRegistryName(r.AssignedStreamID, r.Namespace, r.StreamName); err != nil {
		return RegistryRecord{}, err
	}
	return r, nil
}

type RegistrySnapshotHeader struct {
	ArtifactID       UUID
	CoveredEntryID   uint64
	EntryCount       uint64
	BlockCount       uint32
	BlockIndexOffset uint64
	EntriesOffset    uint64
	CreatedAt        int64
}

func MarshalRegistrySnapshotHeader(h RegistrySnapshotHeader) ([]byte, error) {
	if isZeroUUID(h.ArtifactID) || h.BlockIndexOffset != RegistrySnapshotHeaderLength || h.EntriesOffset < h.BlockIndexOffset {
		return nil, invalidf("Registry Snapshot Header is invalid")
	}
	if (h.EntryCount == 0) != (h.BlockCount == 0) {
		return nil, invalidf("Registry Snapshot counts are inconsistent")
	}
	b := make([]byte, RegistrySnapshotHeaderLength)
	copy(b[:8], registrySnapshotMagic[:])
	putU16(b[8:10], VersionV1)
	putU16(b[10:12], RegistrySnapshotHeaderLength)
	copy(b[16:32], h.ArtifactID[:])
	putU64(b[32:40], h.CoveredEntryID)
	putU64(b[40:48], h.EntryCount)
	putU32(b[48:52], h.BlockCount)
	putU64(b[56:64], h.BlockIndexOffset)
	putU64(b[64:72], h.EntriesOffset)
	putI64(b[72:80], h.CreatedAt)
	putU32(b[80:84], checksum(b[:80]))
	return b, nil
}
func UnmarshalRegistrySnapshotHeader(b []byte) (RegistrySnapshotHeader, error) {
	var h RegistrySnapshotHeader
	if len(b) < RegistrySnapshotHeaderLength {
		return h, truncatedf("Registry Snapshot Header is truncated")
	}
	if len(b) != RegistrySnapshotHeaderLength {
		return h, invalidf("Registry Snapshot Header has trailing bytes")
	}
	if !bytes.Equal(b[:8], registrySnapshotMagic[:]) {
		return h, invalidf("Registry Snapshot magic is invalid")
	}
	if v := getU16(b[8:10]); v != VersionV1 {
		return h, unsupportedVersion("Registry Snapshot", v)
	}
	if getU16(b[10:12]) != RegistrySnapshotHeaderLength || getU32(b[12:16]) != 0 {
		return h, invalidf("Registry Snapshot fixed fields are invalid")
	}
	if err := expectZero(b[52:56], "Registry Snapshot reserved_0"); err != nil {
		return h, err
	}
	if err := expectZero(b[84:88], "Registry Snapshot reserved_1"); err != nil {
		return h, err
	}
	if stored, actual := getU32(b[80:84]), checksum(b[:80]); stored != actual {
		return h, checksumf("Registry Snapshot Header CRC mismatch")
	}
	copy(h.ArtifactID[:], b[16:32])
	h.CoveredEntryID = getU64(b[32:40])
	h.EntryCount = getU64(b[40:48])
	h.BlockCount = getU32(b[48:52])
	h.BlockIndexOffset = getU64(b[56:64])
	h.EntriesOffset = getU64(b[64:72])
	h.CreatedAt = getI64(b[72:80])
	if _, err := MarshalRegistrySnapshotHeader(h); err != nil {
		return RegistrySnapshotHeader{}, err
	}
	return h, nil
}

type RegistryEntry struct {
	Flags          uint32
	StreamID       uint64
	CreatedEntryID uint64
	Namespace      string
	StreamName     string
}

func MarshalRegistryEntry(e RegistryEntry) ([]byte, error) {
	if e.Flags != 0 {
		return nil, invalidf("Registry Entry flags are invalid")
	}
	if err := validateRegistryName(e.StreamID, e.Namespace, e.StreamName); err != nil {
		return nil, err
	}
	n := registryEntryFixedLength + len(e.Namespace) + len(e.StreamName)
	b := make([]byte, n)
	putU32(b[:4], uint32(n))
	putU64(b[8:16], e.StreamID)
	putU64(b[16:24], e.CreatedEntryID)
	putU16(b[24:26], uint16(len(e.Namespace)))
	putU16(b[26:28], uint16(len(e.StreamName)))
	pos := 32
	pos += copy(b[pos:], e.Namespace)
	pos += copy(b[pos:], e.StreamName)
	putU32(b[pos:], checksum(b[:pos]))
	return b, nil
}
func UnmarshalRegistryEntry(b []byte) (RegistryEntry, error) {
	var e RegistryEntry
	if len(b) < registryEntryFixedLength {
		return e, truncatedf("Registry Entry is truncated")
	}
	ns, nm := int(getU16(b[24:26])), int(getU16(b[26:28]))
	if int(getU32(b[:4])) != len(b) || len(b) != registryEntryFixedLength+ns+nm || getU32(b[4:8]) != 0 {
		return e, invalidf("Registry Entry length or flags are invalid")
	}
	if err := expectZero(b[28:32], "Registry Entry reserved"); err != nil {
		return e, err
	}
	if stored, actual := getU32(b[len(b)-4:]), checksum(b[:len(b)-4]); stored != actual {
		return e, checksumf("Registry Entry CRC mismatch")
	}
	e.StreamID = getU64(b[8:16])
	e.CreatedEntryID = getU64(b[16:24])
	e.Namespace = string(bytes.Clone(b[32 : 32+ns]))
	e.StreamName = string(bytes.Clone(b[32+ns : len(b)-4]))
	if err := validateRegistryName(e.StreamID, e.Namespace, e.StreamName); err != nil {
		return RegistryEntry{}, err
	}
	return e, nil
}

type RegistryBlockIndexEntry struct {
	EntryCount      uint32
	EntriesOffset   uint64
	FirstNamespace  string
	FirstStreamName string
}

func MarshalRegistryBlockIndexEntry(e RegistryBlockIndexEntry) ([]byte, error) {
	if e.EntryCount == 0 {
		return nil, invalidf("Registry Block is empty")
	}
	if err := validateRegistryName(1, e.FirstNamespace, e.FirstStreamName); err != nil {
		return nil, err
	}
	n := registryBlockIndexFixedLength + len(e.FirstNamespace) + len(e.FirstStreamName)
	b := make([]byte, n)
	putU32(b[:4], uint32(n))
	putU32(b[4:8], e.EntryCount)
	putU64(b[8:16], e.EntriesOffset)
	putU16(b[16:18], uint16(len(e.FirstNamespace)))
	putU16(b[18:20], uint16(len(e.FirstStreamName)))
	pos := 24
	pos += copy(b[pos:], e.FirstNamespace)
	pos += copy(b[pos:], e.FirstStreamName)
	putU32(b[pos:], checksum(b[:pos]))
	return b, nil
}
func UnmarshalRegistryBlockIndexEntry(b []byte) (RegistryBlockIndexEntry, error) {
	var e RegistryBlockIndexEntry
	if len(b) < registryBlockIndexFixedLength {
		return e, truncatedf("Registry Block Index is truncated")
	}
	ns, nm := int(getU16(b[16:18])), int(getU16(b[18:20]))
	if int(getU32(b[:4])) != len(b) || len(b) != registryBlockIndexFixedLength+ns+nm {
		return e, invalidf("Registry Block Index length is invalid")
	}
	if err := expectZero(b[20:24], "Registry Block Index reserved"); err != nil {
		return e, err
	}
	if stored, actual := getU32(b[len(b)-4:]), checksum(b[:len(b)-4]); stored != actual {
		return e, checksumf("Registry Block Index CRC mismatch")
	}
	e.EntryCount = getU32(b[4:8])
	e.EntriesOffset = getU64(b[8:16])
	e.FirstNamespace = string(bytes.Clone(b[24 : 24+ns]))
	e.FirstStreamName = string(bytes.Clone(b[24+ns : len(b)-4]))
	if e.EntryCount == 0 {
		return RegistryBlockIndexEntry{}, invalidf("Registry Block is empty")
	}
	if err := validateRegistryName(1, e.FirstNamespace, e.FirstStreamName); err != nil {
		return RegistryBlockIndexEntry{}, err
	}
	return e, nil
}

func validateRegistryName(id uint64, namespace, name string) error {
	if id == 0 {
		return invalidf("assigned Stream ID is zero")
	}
	if namespace == "" || name == "" || !utf8.ValidString(namespace) || !utf8.ValidString(name) {
		return invalidf("Registry name is empty or invalid UTF-8")
	}
	if len(namespace) > int(^uint16(0)) || len(name) > int(^uint16(0)) {
		return fmtTooLarge("Registry name", len(namespace)+len(name), ^uint16(0))
	}
	return nil
}

type SnapshotHeader struct {
	Flags                 uint32
	SnapshotID            UUID
	GroupID               UUID
	Term                  uint64
	CheckpointEntryID     uint64
	CheckpointEntryCRC32C uint32
	ManifestGeneration    uint64
	ManifestSHA256        [sha256.Size]byte
	CreatedAt             int64
	ArtifactCount         uint64
}

func MarshalSnapshotHeader(h SnapshotHeader) ([]byte, error) {
	if err := validateSnapshotHeader(h); err != nil {
		return nil, err
	}
	b := make([]byte, SnapshotHeaderLength)
	copy(b[:8], snapshotMagic[:])
	putU16(b[8:10], VersionV1)
	putU16(b[10:12], SnapshotHeaderLength)
	putU32(b[12:16], h.Flags)
	copy(b[16:32], h.SnapshotID[:])
	copy(b[32:48], h.GroupID[:])
	putU64(b[48:56], h.Term)
	putU64(b[56:64], h.CheckpointEntryID)
	putU32(b[64:68], h.CheckpointEntryCRC32C)
	putU64(b[72:80], h.ManifestGeneration)
	copy(b[80:112], h.ManifestSHA256[:])
	putI64(b[112:120], h.CreatedAt)
	putU64(b[120:128], h.ArtifactCount)
	putU32(b[128:132], checksum(b[:128]))
	return b, nil
}
func UnmarshalSnapshotHeader(b []byte) (SnapshotHeader, error) {
	var h SnapshotHeader
	if len(b) < SnapshotHeaderLength {
		return h, truncatedf("Snapshot Header is truncated")
	}
	if len(b) != SnapshotHeaderLength {
		return h, invalidf("Snapshot Header has trailing bytes")
	}
	if !bytes.Equal(b[:8], snapshotMagic[:]) {
		return h, invalidf("Snapshot magic is invalid")
	}
	if v := getU16(b[8:10]); v != VersionV1 {
		return h, unsupportedVersion("Snapshot", v)
	}
	if getU16(b[10:12]) != SnapshotHeaderLength {
		return h, invalidf("Snapshot Header length is invalid")
	}
	if err := expectZero(b[68:72], "Snapshot reserved_0"); err != nil {
		return h, err
	}
	if err := expectZero(b[132:136], "Snapshot reserved_1"); err != nil {
		return h, err
	}
	if stored, actual := getU32(b[128:132]), checksum(b[:128]); stored != actual {
		return h, checksumf("Snapshot Header CRC mismatch")
	}
	h.Flags = getU32(b[12:16])
	copy(h.SnapshotID[:], b[16:32])
	copy(h.GroupID[:], b[32:48])
	h.Term = getU64(b[48:56])
	h.CheckpointEntryID = getU64(b[56:64])
	h.CheckpointEntryCRC32C = getU32(b[64:68])
	h.ManifestGeneration = getU64(b[72:80])
	copy(h.ManifestSHA256[:], b[80:112])
	h.CreatedAt = getI64(b[112:120])
	h.ArtifactCount = getU64(b[120:128])
	if err := validateSnapshotHeader(h); err != nil {
		return SnapshotHeader{}, err
	}
	return h, nil
}
func validateSnapshotHeader(h SnapshotHeader) error {
	if h.Flags&^SnapshotFlagEmpty != 0 || isZeroUUID(h.SnapshotID) || isZeroUUID(h.GroupID) || isZeroDigest(h.ManifestSHA256) {
		return invalidf("Snapshot Header identity, digest, or flags are invalid")
	}
	if h.Flags&SnapshotFlagEmpty != 0 && (h.CheckpointEntryID != 0 || h.CheckpointEntryCRC32C != 0) {
		return invalidf("empty Snapshot has checkpoint fields")
	}
	return nil
}

type SnapshotArtifact struct {
	ArtifactType   ArtifactType
	FormatVersion  uint16
	Flags          uint32
	ArtifactID     UUID
	FileSize       uint64
	LocalName      string
	ObjectLocation string
	ContentSHA256  [sha256.Size]byte
}

func MarshalSnapshotArtifact(a SnapshotArtifact) ([]byte, error) {
	if err := validateSnapshotArtifact(a); err != nil {
		return nil, err
	}
	n := snapshotArtifactFixedLength + len(a.LocalName) + len(a.ObjectLocation)
	b := make([]byte, n)
	putU32(b[:4], uint32(n))
	putU16(b[4:6], uint16(a.ArtifactType))
	putU16(b[6:8], a.FormatVersion)
	putU32(b[8:12], a.Flags)
	copy(b[12:28], a.ArtifactID[:])
	putU64(b[28:36], a.FileSize)
	putU16(b[36:38], uint16(len(a.LocalName)))
	putU16(b[38:40], uint16(len(a.ObjectLocation)))
	copy(b[44:76], a.ContentSHA256[:])
	pos := 76
	pos += copy(b[pos:], a.LocalName)
	pos += copy(b[pos:], a.ObjectLocation)
	putU32(b[pos:], checksum(b[:pos]))
	return b, nil
}
func UnmarshalSnapshotArtifact(b []byte) (SnapshotArtifact, error) {
	var a SnapshotArtifact
	if len(b) < snapshotArtifactFixedLength {
		return a, truncatedf("Snapshot Artifact is truncated")
	}
	local, object := int(getU16(b[36:38])), int(getU16(b[38:40]))
	if int(getU32(b[:4])) != len(b) || len(b) != snapshotArtifactFixedLength+local+object {
		return a, invalidf("Snapshot Artifact length is invalid")
	}
	if err := expectZero(b[40:44], "Snapshot Artifact reserved"); err != nil {
		return a, err
	}
	if stored, actual := getU32(b[len(b)-4:]), checksum(b[:len(b)-4]); stored != actual {
		return a, checksumf("Snapshot Artifact CRC mismatch")
	}
	a.ArtifactType = ArtifactType(getU16(b[4:6]))
	a.FormatVersion = getU16(b[6:8])
	a.Flags = getU32(b[8:12])
	copy(a.ArtifactID[:], b[12:28])
	a.FileSize = getU64(b[28:36])
	copy(a.ContentSHA256[:], b[44:76])
	a.LocalName = string(bytes.Clone(b[76 : 76+local]))
	a.ObjectLocation = string(bytes.Clone(b[76+local : len(b)-4]))
	if err := validateSnapshotArtifact(a); err != nil {
		return SnapshotArtifact{}, err
	}
	return a, nil
}
func validateSnapshotArtifact(a SnapshotArtifact) error {
	if a.ArtifactType < ArtifactTailCatalog || a.ArtifactType > ArtifactSegment || a.FormatVersion != VersionV1 || a.Flags&^segmentRefKnownFlags != 0 || a.Flags&segmentRefKnownFlags == 0 || isZeroUUID(a.ArtifactID) || a.FileSize == 0 || isZeroDigest(a.ContentSHA256) {
		return invalidf("Snapshot Artifact fields are invalid")
	}
	if a.Flags&SegmentRefHasLocal != 0 {
		if err := validateRelativePath(a.LocalName, "Snapshot local name"); err != nil {
			return err
		}
	} else if a.LocalName != "" {
		return invalidf("Snapshot local name exists without flag")
	}
	if a.Flags&SegmentRefHasObject != 0 {
		if a.ObjectLocation == "" || !utf8.ValidString(a.ObjectLocation) {
			return invalidf("Snapshot object location is invalid")
		}
	} else if a.ObjectLocation != "" {
		return invalidf("Snapshot object location exists without flag")
	}
	if len(a.LocalName) > int(^uint16(0)) || len(a.ObjectLocation) > int(^uint16(0)) {
		return fmtTooLarge("Snapshot Artifact location", len(a.LocalName)+len(a.ObjectLocation), ^uint16(0))
	}
	return nil
}
