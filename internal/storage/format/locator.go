package format

import (
	"bytes"
	"fmt"
)

var locatorPackMagic = [8]byte{'S', 'T', 'R', 'M', 'L', 'O', 'C', '1'}
var extentPageMagic = [8]byte{'E', 'X', 'T', 'P', 'A', 'G', 'E', '1'}

const ExtentPageHasPrevious uint32 = 1

type LocatorPackHeader struct {
	ArtifactID     UUID
	PageCount      uint64
	CreatedAt      int64
	CoveredEntryID uint64
}

func MarshalLocatorPackHeader(h LocatorPackHeader) ([]byte, error) {
	if isZeroUUID(h.ArtifactID) {
		return nil, invalidf("Locator Pack artifact ID is zero")
	}
	b := make([]byte, LocatorPackHeaderLength)
	copy(b[:8], locatorPackMagic[:])
	putU16(b[8:10], VersionV1)
	putU16(b[10:12], LocatorPackHeaderLength)
	copy(b[16:32], h.ArtifactID[:])
	putU32(b[32:36], LocatorPageLength)
	putU64(b[40:48], h.PageCount)
	putI64(b[48:56], h.CreatedAt)
	putU64(b[56:64], h.CoveredEntryID)
	putU32(b[64:68], checksum(b[:64]))
	return b, nil
}
func UnmarshalLocatorPackHeader(b []byte) (LocatorPackHeader, error) {
	var h LocatorPackHeader
	if len(b) < LocatorPackHeaderLength {
		return h, truncatedf("Locator Pack header needs %d bytes", LocatorPackHeaderLength)
	}
	if len(b) != LocatorPackHeaderLength {
		return h, invalidf("Locator Pack header has trailing bytes")
	}
	if !bytes.Equal(b[:8], locatorPackMagic[:]) {
		return h, invalidf("Locator Pack magic is invalid")
	}
	if v := getU16(b[8:10]); v != VersionV1 {
		return h, unsupportedVersion("Locator Pack", v)
	}
	if getU16(b[10:12]) != LocatorPackHeaderLength || getU32(b[12:16]) != 0 || getU32(b[32:36]) != LocatorPageLength {
		return h, invalidf("Locator Pack fixed fields are invalid")
	}
	if err := expectZero(b[36:40], "Locator Pack reserved_0"); err != nil {
		return h, err
	}
	if err := expectZero(b[68:72], "Locator Pack reserved_1"); err != nil {
		return h, err
	}
	if stored, actual := getU32(b[64:68]), checksum(b[:64]); stored != actual {
		return h, checksumf("Locator Pack header CRC mismatch")
	}
	copy(h.ArtifactID[:], b[16:32])
	h.PageCount = getU64(b[40:48])
	h.CreatedAt = getI64(b[48:56])
	h.CoveredEntryID = getU64(b[56:64])
	if isZeroUUID(h.ArtifactID) {
		return LocatorPackHeader{}, invalidf("Locator Pack artifact ID is zero")
	}
	return h, nil
}
func LocatorPagePosition(ordinal uint32) (uint64, error) {
	delta, err := checkedMul(uint64(ordinal), LocatorPageLength, "Locator Page position")
	if err != nil {
		return 0, err
	}
	return checkedAdd(SegmentSectionAlignment, delta)
}

type ExtentPageHeader struct {
	Flags               uint32
	PageID              UUID
	StreamID            uint64
	FirstSequence       uint64
	NextSequence        uint64
	FirstRecordedAt     int64
	LastRecordedAt      int64
	PreviousPackID      UUID
	PreviousPageOrdinal uint32
}
type ExtentEntry struct {
	SegmentID         UUID
	FirstSequence     uint64
	NextSequence      uint64
	FirstByteOffset   uint64
	NextByteOffset    uint64
	FirstRecordedAt   int64
	LastRecordedAt    int64
	RecordIndexOffset uint64
	StreamDataOffset  uint64
}
type SkipPointer struct {
	TargetPackID        UUID
	TargetPageOrdinal   uint32
	DistancePages       uint32
	TargetFirstSequence uint64
	TargetFirstTime     int64
}
type ExtentPage struct {
	Header       ExtentPageHeader
	Extents      []ExtentEntry
	SkipPointers []SkipPointer
}

func MarshalExtentEntry(e ExtentEntry) ([]byte, error) {
	if err := validateExtentEntry(e); err != nil {
		return nil, err
	}
	b := make([]byte, ExtentEntryLength)
	copy(b[:16], e.SegmentID[:])
	putU64(b[16:24], e.FirstSequence)
	putU64(b[24:32], e.NextSequence)
	putU64(b[32:40], e.FirstByteOffset)
	putU64(b[40:48], e.NextByteOffset)
	putI64(b[48:56], e.FirstRecordedAt)
	putI64(b[56:64], e.LastRecordedAt)
	putU64(b[64:72], e.RecordIndexOffset)
	putU64(b[72:80], e.StreamDataOffset)
	putU32(b[80:84], checksum(b[:80]))
	return b, nil
}
func UnmarshalExtentEntry(b []byte) (ExtentEntry, error) {
	var e ExtentEntry
	if len(b) < ExtentEntryLength {
		return e, truncatedf("Extent Entry needs %d bytes", ExtentEntryLength)
	}
	if len(b) != ExtentEntryLength {
		return e, invalidf("Extent Entry has trailing bytes")
	}
	if err := expectZero(b[84:88], "Extent Entry reserved"); err != nil {
		return e, err
	}
	if stored, actual := getU32(b[80:84]), checksum(b[:80]); stored != actual {
		return e, checksumf("Extent Entry CRC mismatch")
	}
	copy(e.SegmentID[:], b[:16])
	e.FirstSequence = getU64(b[16:24])
	e.NextSequence = getU64(b[24:32])
	e.FirstByteOffset = getU64(b[32:40])
	e.NextByteOffset = getU64(b[40:48])
	e.FirstRecordedAt = getI64(b[48:56])
	e.LastRecordedAt = getI64(b[56:64])
	e.RecordIndexOffset = getU64(b[64:72])
	e.StreamDataOffset = getU64(b[72:80])
	if err := validateExtentEntry(e); err != nil {
		return ExtentEntry{}, err
	}
	return e, nil
}
func validateExtentEntry(e ExtentEntry) error {
	if isZeroUUID(e.SegmentID) {
		return invalidf("Extent Segment ID is zero")
	}
	if e.FirstSequence >= e.NextSequence || e.FirstByteOffset >= e.NextByteOffset || e.FirstRecordedAt > e.LastRecordedAt {
		return invalidf("Extent range is empty or reversed")
	}
	return nil
}

func MarshalSkipPointer(p SkipPointer) ([]byte, error) {
	if isZeroUUID(p.TargetPackID) || p.DistancePages == 0 {
		return nil, invalidf("Skip Pointer target or distance is zero")
	}
	b := make([]byte, SkipPointerLength)
	copy(b[:16], p.TargetPackID[:])
	putU32(b[16:20], p.TargetPageOrdinal)
	putU32(b[20:24], p.DistancePages)
	putU64(b[24:32], p.TargetFirstSequence)
	putI64(b[32:40], p.TargetFirstTime)
	return b, nil
}
func UnmarshalSkipPointer(b []byte) (SkipPointer, error) {
	var p SkipPointer
	if len(b) < SkipPointerLength {
		return p, truncatedf("Skip Pointer needs %d bytes", SkipPointerLength)
	}
	if len(b) != SkipPointerLength {
		return p, invalidf("Skip Pointer has trailing bytes")
	}
	copy(p.TargetPackID[:], b[:16])
	p.TargetPageOrdinal = getU32(b[16:20])
	p.DistancePages = getU32(b[20:24])
	p.TargetFirstSequence = getU64(b[24:32])
	p.TargetFirstTime = getI64(b[32:40])
	if isZeroUUID(p.TargetPackID) || p.DistancePages == 0 {
		return SkipPointer{}, invalidf("Skip Pointer target or distance is zero")
	}
	return p, nil
}

func MarshalExtentPage(page ExtentPage) ([]byte, error) {
	if len(page.Extents) == 0 {
		return nil, invalidf("Extent Page is empty")
	}
	if len(page.Extents) > int(^uint32(0)) || len(page.SkipPointers) > int(^uint16(0)) {
		return nil, fmtTooLarge("Extent Page entries", len(page.Extents)+len(page.SkipPointers), ^uint16(0))
	}
	bodyLength := len(page.Extents)*ExtentEntryLength + len(page.SkipPointers)*SkipPointerLength
	if ExtentPageHeaderLength+bodyLength > LocatorPageLength-4 {
		return nil, fmtTooLarge("Extent Page body", bodyLength, LocatorPageLength-4-ExtentPageHeaderLength)
	}
	if err := validateExtentPage(page); err != nil {
		return nil, err
	}
	b := make([]byte, LocatorPageLength)
	copy(b[:8], extentPageMagic[:])
	putU16(b[8:10], VersionV1)
	putU16(b[10:12], ExtentPageHeaderLength)
	putU32(b[12:16], page.Header.Flags)
	copy(b[16:32], page.Header.PageID[:])
	putU64(b[32:40], page.Header.StreamID)
	putU64(b[40:48], page.Header.FirstSequence)
	putU64(b[48:56], page.Header.NextSequence)
	putI64(b[56:64], page.Header.FirstRecordedAt)
	putI64(b[64:72], page.Header.LastRecordedAt)
	putU32(b[72:76], uint32(len(page.Extents)))
	putU16(b[76:78], uint16(len(page.SkipPointers)))
	copy(b[80:96], page.Header.PreviousPackID[:])
	putU32(b[96:100], page.Header.PreviousPageOrdinal)
	putU32(b[100:104], uint32(bodyLength))
	putU32(b[108:112], checksum(b[:108]))
	pos := ExtentPageHeaderLength
	for _, e := range page.Extents {
		x, _ := MarshalExtentEntry(e)
		copy(b[pos:], x)
		pos += len(x)
	}
	for _, p := range page.SkipPointers {
		x, _ := MarshalSkipPointer(p)
		copy(b[pos:], x)
		pos += len(x)
	}
	putU32(b[LocatorPageLength-4:], checksum(b[:LocatorPageLength-4]))
	return b, nil
}

func UnmarshalExtentPage(b []byte) (ExtentPage, error) {
	var page ExtentPage
	if len(b) < LocatorPageLength {
		return page, truncatedf("Extent Page needs %d bytes, got %d", LocatorPageLength, len(b))
	}
	if len(b) != LocatorPageLength {
		return page, invalidf("Extent Page has trailing bytes")
	}
	if !bytes.Equal(b[:8], extentPageMagic[:]) {
		return page, invalidf("Extent Page magic is invalid")
	}
	if v := getU16(b[8:10]); v != VersionV1 {
		return page, unsupportedVersion("Extent Page", v)
	}
	if getU16(b[10:12]) != ExtentPageHeaderLength {
		return page, invalidf("Extent Page header length is invalid")
	}
	if err := expectZero(b[78:80], "Extent Page reserved_0"); err != nil {
		return page, err
	}
	if err := expectZero(b[104:108], "Extent Page reserved_1"); err != nil {
		return page, err
	}
	if stored, actual := getU32(b[108:112]), checksum(b[:108]); stored != actual {
		return page, checksumf("Extent Page header CRC mismatch")
	}
	if stored, actual := getU32(b[LocatorPageLength-4:]), checksum(b[:LocatorPageLength-4]); stored != actual {
		return page, checksumf("Extent Page CRC mismatch")
	}
	count := getU32(b[72:76])
	skips := getU16(b[76:78])
	body := uint64(getU32(b[100:104]))
	expected, err := checkedAdd(uint64(count)*ExtentEntryLength, uint64(skips)*SkipPointerLength)
	if err != nil || body != expected || body > LocatorPageLength-4-ExtentPageHeaderLength {
		return page, invalidf("Extent Page body length is invalid")
	}
	bodyEnd := ExtentPageHeaderLength + int(body)
	if err := expectZero(b[bodyEnd:LocatorPageLength-4], "Extent Page padding"); err != nil {
		return page, err
	}
	page.Header.Flags = getU32(b[12:16])
	copy(page.Header.PageID[:], b[16:32])
	page.Header.StreamID = getU64(b[32:40])
	page.Header.FirstSequence = getU64(b[40:48])
	page.Header.NextSequence = getU64(b[48:56])
	page.Header.FirstRecordedAt = getI64(b[56:64])
	page.Header.LastRecordedAt = getI64(b[64:72])
	copy(page.Header.PreviousPackID[:], b[80:96])
	page.Header.PreviousPageOrdinal = getU32(b[96:100])
	pos := ExtentPageHeaderLength
	page.Extents = make([]ExtentEntry, 0, int(count))
	for i := uint32(0); i < count; i++ {
		e, eerr := UnmarshalExtentEntry(b[pos : pos+ExtentEntryLength])
		if eerr != nil {
			return ExtentPage{}, fmt.Errorf("Extent %d: %w", i, eerr)
		}
		page.Extents = append(page.Extents, e)
		pos += ExtentEntryLength
	}
	page.SkipPointers = make([]SkipPointer, 0, int(skips))
	for i := uint16(0); i < skips; i++ {
		p, perr := UnmarshalSkipPointer(b[pos : pos+SkipPointerLength])
		if perr != nil {
			return ExtentPage{}, fmt.Errorf("Skip Pointer %d: %w", i, perr)
		}
		page.SkipPointers = append(page.SkipPointers, p)
		pos += SkipPointerLength
	}
	if err := validateExtentPage(page); err != nil {
		return ExtentPage{}, err
	}
	return page, nil
}

func validateExtentPage(page ExtentPage) error {
	h := page.Header
	if isZeroUUID(h.PageID) {
		return invalidf("Extent Page ID is zero")
	}
	if h.Flags&^ExtentPageHasPrevious != 0 {
		return invalidf("Extent Page flags are invalid")
	}
	if h.Flags&ExtentPageHasPrevious == 0 {
		if !isZeroUUID(h.PreviousPackID) || h.PreviousPageOrdinal != 0 {
			return invalidf("Extent Page previous pointer exists without flag")
		}
	} else if isZeroUUID(h.PreviousPackID) {
		return invalidf("Extent Page previous Pack ID is zero")
	}
	for i, e := range page.Extents {
		if err := validateExtentEntry(e); err != nil {
			return fmt.Errorf("Extent %d: %w", i, err)
		}
		if i > 0 {
			p := page.Extents[i-1]
			if p.NextSequence != e.FirstSequence || p.NextByteOffset != e.FirstByteOffset || p.LastRecordedAt > e.FirstRecordedAt {
				return invalidf("Extent ranges are not continuous at %d", i)
			}
		}
	}
	if len(page.Extents) > 0 {
		first, last := page.Extents[0], page.Extents[len(page.Extents)-1]
		if h.FirstSequence != first.FirstSequence || h.NextSequence != last.NextSequence || h.FirstRecordedAt != first.FirstRecordedAt || h.LastRecordedAt != last.LastRecordedAt {
			return invalidf("Extent Page Header range does not match body")
		}
	}
	for i, p := range page.SkipPointers {
		if isZeroUUID(p.TargetPackID) || p.DistancePages == 0 || p.TargetFirstSequence >= h.FirstSequence || p.TargetFirstTime > h.FirstRecordedAt {
			return invalidf("Skip Pointer %d is not earlier than Page", i)
		}
	}
	return nil
}
