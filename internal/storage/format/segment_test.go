package format

import (
	"bytes"
	"errors"
	"testing"
)

func sampleSegmentLayout(t testing.TB) (SegmentHeader, []StreamDirectoryEntry) {
	t.Helper()
	header, err := NewSegmentHeader(testUUID(1), 1_700_000_000_000_000_000, 5, 10, 2, 3, 400)
	if err != nil {
		t.Fatal(err)
	}
	directories := []StreamDirectoryEntry{
		{
			StreamID: 3, FirstSequence: 20, RecordCount: 2,
			FirstByteOffset: 1000, NextByteOffset: 1250,
			FirstRecordedAt: 100, LastRecordedAt: 110,
			FirstEntryID: 5, LastEntryID: 9,
			RecordIndexOffset: header.IndexOffset, RecordIndexLength: 48,
			StreamDataOffset: header.DataOffset, StreamDataLength: 250,
		},
		{
			StreamID: 8, FirstSequence: 7, RecordCount: 1,
			FirstByteOffset: 9000, NextByteOffset: 9150,
			FirstRecordedAt: 200, LastRecordedAt: 200,
			FirstEntryID: 7, LastEntryID: 10,
			RecordIndexOffset: header.IndexOffset + 48, RecordIndexLength: 24,
			StreamDataOffset: header.DataOffset + 250, StreamDataLength: 150,
		},
	}
	return header, directories
}

func TestSegmentFormatsGoldenAndRoundTrip(t *testing.T) {
	header, directories := sampleSegmentLayout(t)
	headerBytes, err := MarshalSegmentHeader(header)
	if err != nil {
		t.Fatal(err)
	}
	assertGolden(t, "segment_header_v1.hex", headerBytes)
	decodedHeader, err := UnmarshalSegmentHeader(headerBytes)
	if err != nil || decodedHeader != header {
		t.Fatalf("header round trip: %+v, %v", decodedHeader, err)
	}
	headerSection := make([]byte, SegmentSectionAlignment)
	copy(headerSection, headerBytes)
	if _, err := UnmarshalSegmentHeaderSection(headerSection); err != nil {
		t.Fatalf("header section: %v", err)
	}

	directoryBytes, err := MarshalStreamDirectoryEntry(directories[0])
	if err != nil {
		t.Fatal(err)
	}
	assertGolden(t, "stream_directory_v1.hex", directoryBytes)
	decodedDirectory, err := UnmarshalStreamDirectoryEntry(directoryBytes)
	if err != nil || decodedDirectory != directories[0] {
		t.Fatalf("directory round trip: %+v, %v", decodedDirectory, err)
	}

	index := DenseIndexEntry{RelativeByteOffset: 120, RecordedAtDelta: 10, FrameLength: 130, FrameCRC32C: 0x12345678}
	indexBytes, err := MarshalDenseIndexEntry(index)
	if err != nil {
		t.Fatal(err)
	}
	assertGolden(t, "dense_index_v1.hex", indexBytes)
	decodedIndex, err := UnmarshalDenseIndexEntry(indexBytes)
	if err != nil || decodedIndex != index {
		t.Fatalf("index round trip: %+v, %v", decodedIndex, err)
	}

	content := make([]byte, header.FooterOffset)
	copy(content, headerBytes)
	footer, err := NewSegmentFooter(header, content)
	if err != nil {
		t.Fatal(err)
	}
	footerBytes, err := MarshalSegmentFooter(footer)
	if err != nil {
		t.Fatal(err)
	}
	assertGolden(t, "segment_footer_v1.hex", footerBytes)
	footerSection := make([]byte, SegmentFooterSectionLength)
	copy(footerSection, footerBytes)
	decodedFooter, err := VerifySegmentFooter(header, content, footerSection)
	if err != nil || decodedFooter != footer {
		t.Fatalf("footer verification: %+v, %v", decodedFooter, err)
	}

	if err := ValidateSegmentLayout(header, directories); err != nil {
		t.Fatal(err)
	}
	entries := []DenseIndexEntry{
		{RelativeByteOffset: 0, RecordedAtDelta: 0, FrameLength: 120, FrameCRC32C: 1},
		{RelativeByteOffset: 120, RecordedAtDelta: 10, FrameLength: 130, FrameCRC32C: 2},
	}
	if err := ValidateDenseIndex(directories[0], entries); err != nil {
		t.Fatal(err)
	}
}

func TestSegmentRejectsCorruptionAndInvalidLayout(t *testing.T) {
	header, directories := sampleSegmentLayout(t)
	headerBytes, err := MarshalSegmentHeader(header)
	if err != nil {
		t.Fatal(err)
	}
	corrupt := bytes.Clone(headerBytes)
	corrupt[40] ^= 1
	if _, err := UnmarshalSegmentHeader(corrupt); !errors.Is(err, ErrChecksum) {
		t.Fatalf("header corruption: %v", err)
	}
	headerSection := make([]byte, SegmentSectionAlignment)
	copy(headerSection, headerBytes)
	headerSection[SegmentHeaderLength] = 1
	if _, err := UnmarshalSegmentHeaderSection(headerSection); !errors.Is(err, ErrInvalid) {
		t.Fatalf("header padding: %v", err)
	}

	badDirectories := append([]StreamDirectoryEntry(nil), directories...)
	badDirectories[1].StreamID = directories[0].StreamID
	if err := ValidateSegmentLayout(header, badDirectories); !errors.Is(err, ErrInvalid) {
		t.Fatalf("directory ordering: %v", err)
	}

	badIndex := []DenseIndexEntry{
		{RelativeByteOffset: 0, RecordedAtDelta: 0, FrameLength: 120},
		{RelativeByteOffset: 121, RecordedAtDelta: 10, FrameLength: 130},
	}
	if err := ValidateDenseIndex(directories[0], badIndex); !errors.Is(err, ErrInvalid) {
		t.Fatalf("index continuity: %v", err)
	}

	content := make([]byte, header.FooterOffset)
	copy(content, headerBytes)
	footer, err := NewSegmentFooter(header, content)
	if err != nil {
		t.Fatal(err)
	}
	footerBytes, err := MarshalSegmentFooter(footer)
	if err != nil {
		t.Fatal(err)
	}
	section := make([]byte, SegmentFooterSectionLength)
	copy(section, footerBytes)
	section[len(section)-1] = 1
	if _, err := VerifySegmentFooter(header, content, section); !errors.Is(err, ErrInvalid) {
		t.Fatalf("footer padding: %v", err)
	}
	section[len(section)-1] = 0
	content[200] = 1
	if _, err := VerifySegmentFooter(header, content, section); !errors.Is(err, ErrChecksum) {
		t.Fatalf("footer digest: %v", err)
	}
}

func FuzzUnmarshalSegmentHeader(f *testing.F) {
	header, err := NewSegmentHeader(testUUID(1), 1, 1, 2, 1, 2, 240)
	if err != nil {
		f.Fatal(err)
	}
	seed, err := MarshalSegmentHeader(header)
	if err != nil {
		f.Fatal(err)
	}
	f.Add(seed)
	f.Add([]byte("segment"))
	f.Fuzz(func(t *testing.T, data []byte) {
		decoded, err := UnmarshalSegmentHeader(data)
		if err != nil {
			return
		}
		reencoded, err := MarshalSegmentHeader(decoded)
		if err != nil || !bytes.Equal(data, reencoded) {
			t.Fatalf("successful Segment Header decode did not round trip: %v", err)
		}
	})
}

func FuzzUnmarshalStreamDirectoryEntry(f *testing.F) {
	header, directories := sampleSegmentLayout(f)
	_ = header
	seed, err := MarshalStreamDirectoryEntry(directories[0])
	if err != nil {
		f.Fatal(err)
	}
	f.Add(seed)
	f.Add([]byte("directory"))
	f.Fuzz(func(t *testing.T, data []byte) {
		decoded, err := UnmarshalStreamDirectoryEntry(data)
		if err != nil {
			return
		}
		reencoded, err := MarshalStreamDirectoryEntry(decoded)
		if err != nil || !bytes.Equal(data, reencoded) {
			t.Fatalf("successful Directory decode did not round trip: %v", err)
		}
	})
}

func testUUID(last byte) UUID {
	var id UUID
	id[15] = last
	return id
}
