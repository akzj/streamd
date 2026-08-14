package format

import "bytes"

var tailCatalogMagic = [8]byte{'S', 'T', 'R', 'M', 'T', 'A', 'I', 'L'}

const TailSlotPresent uint32 = 1

type TailCatalogHeader struct {
	ArtifactID         UUID
	SlotCount          uint64
	CoveredEntryID     uint64
	ManifestGeneration uint64
}

func MarshalTailCatalogHeader(header TailCatalogHeader) ([]byte, error) {
	if isZeroUUID(header.ArtifactID) {
		return nil, invalidf("Tail Catalog artifact ID is zero")
	}
	b := make([]byte, TailCatalogHeaderLength)
	copy(b[:8], tailCatalogMagic[:])
	putU16(b[8:10], VersionV1)
	putU16(b[10:12], TailCatalogHeaderLength)
	copy(b[16:32], header.ArtifactID[:])
	putU32(b[32:36], TailSlotLength)
	putU64(b[40:48], header.SlotCount)
	putU64(b[48:56], header.CoveredEntryID)
	putU64(b[56:64], header.ManifestGeneration)
	putU32(b[64:68], checksum(b[:64]))
	return b, nil
}

func UnmarshalTailCatalogHeader(b []byte) (TailCatalogHeader, error) {
	var header TailCatalogHeader
	if len(b) < TailCatalogHeaderLength {
		return header, truncatedf("Tail Catalog header needs %d bytes, got %d", TailCatalogHeaderLength, len(b))
	}
	if len(b) != TailCatalogHeaderLength {
		return header, invalidf("Tail Catalog header has trailing bytes")
	}
	if !bytes.Equal(b[:8], tailCatalogMagic[:]) {
		return header, invalidf("Tail Catalog magic is invalid")
	}
	if v := getU16(b[8:10]); v != VersionV1 {
		return header, unsupportedVersion("Tail Catalog", v)
	}
	if getU16(b[10:12]) != TailCatalogHeaderLength || getU32(b[12:16]) != 0 || getU32(b[32:36]) != TailSlotLength {
		return header, invalidf("Tail Catalog fixed fields are invalid")
	}
	if err := expectZero(b[36:40], "Tail Catalog reserved_0"); err != nil {
		return header, err
	}
	if err := expectZero(b[68:72], "Tail Catalog reserved_1"); err != nil {
		return header, err
	}
	if stored, actual := getU32(b[64:68]), checksum(b[:64]); stored != actual {
		return header, checksumf("Tail Catalog header CRC32C is %08x, want %08x", stored, actual)
	}
	copy(header.ArtifactID[:], b[16:32])
	header.SlotCount = getU64(b[40:48])
	header.CoveredEntryID = getU64(b[48:56])
	header.ManifestGeneration = getU64(b[56:64])
	if isZeroUUID(header.ArtifactID) {
		return TailCatalogHeader{}, invalidf("Tail Catalog artifact ID is zero")
	}
	return header, nil
}

func TailSlotPosition(streamID uint64) (uint64, error) {
	base, err := alignUp(TailCatalogHeaderLength, SegmentSectionAlignment)
	if err != nil {
		return 0, err
	}
	delta, err := checkedMul(streamID, TailSlotLength, "Tail Slot position")
	if err != nil {
		return 0, err
	}
	return checkedAdd(base, delta)
}

type TailSlot struct {
	Generation         uint64
	Present            bool
	StreamID           uint64
	NextSequence       uint64
	NextByteOffset     uint64
	LastRecordedAt     int64
	LastEntryID        uint64
	AppliedEntryID     uint64
	LatestSegmentID    UUID
	LatestExtentPackID UUID
	LatestPageOrdinal  uint32
}

func MarshalTailSlot(slot TailSlot) ([]byte, error) {
	if slot.Generation&1 != 0 {
		return nil, invalidf("Tail Slot generation is odd")
	}
	if err := validateTailSlot(slot); err != nil {
		return nil, err
	}
	b := make([]byte, TailSlotLength)
	putU64(b[:8], slot.Generation)
	if slot.Present {
		putU32(b[8:12], TailSlotPresent)
	}
	putU64(b[16:24], slot.StreamID)
	putU64(b[24:32], slot.NextSequence)
	putU64(b[32:40], slot.NextByteOffset)
	putI64(b[40:48], slot.LastRecordedAt)
	putU64(b[48:56], slot.LastEntryID)
	putU64(b[56:64], slot.AppliedEntryID)
	copy(b[64:80], slot.LatestSegmentID[:])
	copy(b[80:96], slot.LatestExtentPackID[:])
	putU32(b[96:100], slot.LatestPageOrdinal)
	putU32(b[112:116], checksum(b[8:112]))
	putU64(b[120:128], slot.Generation)
	return b, nil
}

func UnmarshalTailSlot(b []byte) (TailSlot, error) {
	var slot TailSlot
	if len(b) < TailSlotLength {
		return slot, truncatedf("Tail Slot needs %d bytes, got %d", TailSlotLength, len(b))
	}
	if len(b) != TailSlotLength {
		return slot, invalidf("Tail Slot has trailing bytes")
	}
	begin, end := getU64(b[:8]), getU64(b[120:128])
	if begin != end || begin&1 != 0 {
		return slot, invalidf("Tail Slot generation is torn")
	}
	flags := getU32(b[8:12])
	if flags&^TailSlotPresent != 0 {
		return slot, invalidf("Tail Slot flags are invalid")
	}
	if err := expectZero(b[12:16], "Tail Slot reserved_0"); err != nil {
		return slot, err
	}
	if err := expectZero(b[100:112], "Tail Slot reserved_1"); err != nil {
		return slot, err
	}
	if err := expectZero(b[116:120], "Tail Slot reserved_2"); err != nil {
		return slot, err
	}
	if stored, actual := getU32(b[112:116]), checksum(b[8:112]); stored != actual {
		return slot, checksumf("Tail Slot CRC32C is %08x, want %08x", stored, actual)
	}
	slot.Generation = begin
	slot.Present = flags&TailSlotPresent != 0
	slot.StreamID = getU64(b[16:24])
	slot.NextSequence = getU64(b[24:32])
	slot.NextByteOffset = getU64(b[32:40])
	slot.LastRecordedAt = getI64(b[40:48])
	slot.LastEntryID = getU64(b[48:56])
	slot.AppliedEntryID = getU64(b[56:64])
	copy(slot.LatestSegmentID[:], b[64:80])
	copy(slot.LatestExtentPackID[:], b[80:96])
	slot.LatestPageOrdinal = getU32(b[96:100])
	if err := validateTailSlot(slot); err != nil {
		return TailSlot{}, err
	}
	return slot, nil
}

func validateTailSlot(slot TailSlot) error {
	if slot.AppliedEntryID < slot.LastEntryID {
		return invalidf("Tail Slot applied Entry ID precedes last Entry ID")
	}
	if isZeroUUID(slot.LatestExtentPackID) && slot.LatestPageOrdinal != 0 {
		return invalidf("Tail Slot Page Ordinal exists without Pack ID")
	}
	if !slot.Present && (slot.StreamID != 0 || slot.NextSequence != 0 || slot.NextByteOffset != 0 || slot.LastRecordedAt != 0 || slot.LastEntryID != 0 || slot.AppliedEntryID != 0 || !isZeroUUID(slot.LatestSegmentID) || !isZeroUUID(slot.LatestExtentPackID) || slot.LatestPageOrdinal != 0) {
		return invalidf("absent Tail Slot contains data")
	}
	return nil
}
