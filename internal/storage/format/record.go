package format

import (
	"bytes"
	"sort"
	"unicode/utf8"
)

var recordMagic = [4]byte{'S', 'R', 'F', '1'}

// Header is one canonical, byte-sorted Record header.
type Header struct {
	Key   string
	Value []byte
}

// RecordFrame contains the logical, immutable fields encoded in a V1 frame.
type RecordFrame struct {
	EntryID     uint64
	StreamID    uint64
	Sequence    uint64
	ByteOffset  uint64
	RecordedAt  int64
	BatchIndex  uint32
	BatchCount  uint32
	RequestHash [32]byte
	RequestID   []byte
	Producer    string
	Headers     []Header
	Payload     []byte
}

// MarshalRecordFrame returns the canonical V1 encoding of frame.
func MarshalRecordFrame(frame RecordFrame) ([]byte, error) {
	headers, headersLength, err := canonicalHeaders(frame.Headers)
	if err != nil {
		return nil, err
	}
	if len(frame.RequestID) > MaxRequestIDLength {
		return nil, fmtTooLarge("request_id", len(frame.RequestID), MaxRequestIDLength)
	}
	if !utf8.ValidString(frame.Producer) {
		return nil, invalidf("producer is not valid UTF-8")
	}
	if len(frame.Producer) > MaxProducerLength {
		return nil, fmtTooLarge("producer", len(frame.Producer), MaxProducerLength)
	}
	if frame.BatchCount == 0 || frame.BatchCount > MaxBatchRecordCount {
		return nil, invalidf("batch_count out of range: %d", frame.BatchCount)
	}
	if frame.BatchIndex >= frame.BatchCount {
		return nil, invalidf("batch_index %d is not less than batch_count %d", frame.BatchIndex, frame.BatchCount)
	}

	frameLength64, err := checkedAdd(
		RecordFixedHeaderLength,
		uint64(len(frame.RequestID)),
		uint64(len(frame.Producer)),
		uint64(headersLength),
		uint64(len(frame.Payload)),
		RecordCRCSize,
	)
	if err != nil {
		return nil, err
	}
	if frameLength64 > MaxFrameLength {
		return nil, fmtTooLarge("frame", frameLength64, MaxFrameLength)
	}
	frameLength, err := checkedInt(frameLength64, "frame_length")
	if err != nil {
		return nil, err
	}

	encoded := make([]byte, frameLength)
	copy(encoded[0:4], recordMagic[:])
	putU16(encoded[4:6], VersionV1)
	putU16(encoded[6:8], 0)
	putU32(encoded[8:12], uint32(frameLength))
	putU16(encoded[12:14], RecordFixedHeaderLength)
	putU16(encoded[14:16], uint16(len(headers)))
	putU32(encoded[16:20], uint32(headersLength))
	putU32(encoded[20:24], uint32(len(frame.Payload)))
	putU16(encoded[24:26], uint16(len(frame.Producer)))
	putU16(encoded[26:28], uint16(len(frame.RequestID)))

	putU64(encoded[32:40], frame.EntryID)
	putU64(encoded[40:48], frame.StreamID)
	putU64(encoded[48:56], frame.Sequence)
	putU64(encoded[56:64], frame.ByteOffset)
	putI64(encoded[64:72], frame.RecordedAt)
	putU32(encoded[72:76], frame.BatchIndex)
	putU32(encoded[76:80], frame.BatchCount)
	copy(encoded[80:112], frame.RequestHash[:])

	position := RecordFixedHeaderLength
	position += copy(encoded[position:], frame.RequestID)
	position += copy(encoded[position:], frame.Producer)
	for _, header := range headers {
		putU16(encoded[position:position+2], uint16(len(header.Key)))
		putU16(encoded[position+2:position+4], 0)
		putU32(encoded[position+4:position+8], uint32(len(header.Value)))
		position += 8
		position += copy(encoded[position:], header.Key)
		position += copy(encoded[position:], header.Value)
	}
	position += copy(encoded[position:], frame.Payload)
	putU32(encoded[position:position+RecordCRCSize], checksum(encoded[:position]))
	return encoded, nil
}

// UnmarshalRecordFrame validates and decodes exactly one V1 Record Frame.
func UnmarshalRecordFrame(encoded []byte) (RecordFrame, error) {
	return decodeRecordFrame(encoded, true)
}

// decodeRecordFrame may return byte fields that alias encoded when cloneVariable is false.
func decodeRecordFrame(encoded []byte, cloneVariable bool) (RecordFrame, error) {
	var frame RecordFrame
	if len(encoded) < RecordFixedHeaderLength+RecordCRCSize {
		return frame, truncatedf("record frame needs at least %d bytes, got %d", RecordFixedHeaderLength+RecordCRCSize, len(encoded))
	}
	if !bytes.Equal(encoded[0:4], recordMagic[:]) {
		return frame, invalidf("record magic is %q", encoded[0:4])
	}
	if version := getU16(encoded[4:6]); version != VersionV1 {
		return frame, unsupportedVersion("record frame", version)
	}
	if flags := getU16(encoded[6:8]); flags != 0 {
		return frame, invalidf("record flags contain unsupported bits: 0x%04x", flags)
	}
	declaredLength := uint64(getU32(encoded[8:12]))
	if declaredLength > MaxFrameLength {
		return frame, fmtTooLarge("frame", declaredLength, MaxFrameLength)
	}
	if declaredLength != uint64(len(encoded)) {
		if declaredLength > uint64(len(encoded)) {
			return frame, truncatedf("record declares %d bytes, got %d", declaredLength, len(encoded))
		}
		return frame, invalidf("record has trailing bytes: declares %d, got %d", declaredLength, len(encoded))
	}
	if fixedLength := getU16(encoded[12:14]); fixedLength != RecordFixedHeaderLength {
		return frame, invalidf("record fixed_header_length is %d", fixedLength)
	}
	if err := expectZero(encoded[28:32], "record reserved"); err != nil {
		return frame, err
	}

	headerCount := uint64(getU16(encoded[14:16]))
	headersLength := uint64(getU32(encoded[16:20]))
	payloadLength := uint64(getU32(encoded[20:24]))
	producerLength := uint64(getU16(encoded[24:26]))
	requestIDLength := uint64(getU16(encoded[26:28]))
	if headerCount > MaxHeaderCount {
		return frame, fmtTooLarge("header_count", headerCount, MaxHeaderCount)
	}
	if headersLength > MaxHeadersLength {
		return frame, fmtTooLarge("headers", headersLength, MaxHeadersLength)
	}
	if producerLength > MaxProducerLength {
		return frame, fmtTooLarge("producer", producerLength, MaxProducerLength)
	}
	if requestIDLength > MaxRequestIDLength {
		return frame, fmtTooLarge("request_id", requestIDLength, MaxRequestIDLength)
	}
	expectedLength, err := checkedAdd(RecordFixedHeaderLength, requestIDLength, producerLength, headersLength, payloadLength, RecordCRCSize)
	if err != nil {
		return frame, err
	}
	if expectedLength != declaredLength {
		return frame, invalidf("record component lengths sum to %d, frame_length is %d", expectedLength, declaredLength)
	}

	storedCRC := getU32(encoded[len(encoded)-RecordCRCSize:])
	actualCRC := checksum(encoded[:len(encoded)-RecordCRCSize])
	if actualCRC != storedCRC {
		return frame, checksumf("record CRC32C is %08x, want %08x", storedCRC, actualCRC)
	}

	frame.EntryID = getU64(encoded[32:40])
	frame.StreamID = getU64(encoded[40:48])
	frame.Sequence = getU64(encoded[48:56])
	frame.ByteOffset = getU64(encoded[56:64])
	frame.RecordedAt = getI64(encoded[64:72])
	frame.BatchIndex = getU32(encoded[72:76])
	frame.BatchCount = getU32(encoded[76:80])
	copy(frame.RequestHash[:], encoded[80:112])
	if frame.BatchCount == 0 || frame.BatchCount > MaxBatchRecordCount || frame.BatchIndex >= frame.BatchCount {
		return RecordFrame{}, invalidf("invalid batch position %d/%d", frame.BatchIndex, frame.BatchCount)
	}

	position := RecordFixedHeaderLength
	requestEnd := position + int(requestIDLength)
	frame.RequestID = cloneOrAlias(encoded[position:requestEnd], cloneVariable)
	position = requestEnd
	producerEnd := position + int(producerLength)
	if !utf8.Valid(encoded[position:producerEnd]) {
		return RecordFrame{}, invalidf("producer is not valid UTF-8")
	}
	frame.Producer = string(encoded[position:producerEnd])
	position = producerEnd

	headersEnd := position + int(headersLength)
	frame.Headers = make([]Header, 0, int(headerCount))
	var previousKey []byte
	for i := uint64(0); i < headerCount; i++ {
		if headersEnd-position < 8 {
			return RecordFrame{}, truncatedf("header %d metadata", i)
		}
		keyLength := uint64(getU16(encoded[position : position+2]))
		flags := getU16(encoded[position+2 : position+4])
		valueLength := uint64(getU32(encoded[position+4 : position+8]))
		position += 8
		if flags != 0 {
			return RecordFrame{}, invalidf("header %d flags contain unsupported bits: 0x%04x", i, flags)
		}
		if keyLength == 0 || keyLength > MaxHeaderKeyLength {
			return RecordFrame{}, invalidf("header %d key length out of range: %d", i, keyLength)
		}
		entryEnd64, err := checkedAdd(uint64(position), keyLength, valueLength)
		if err != nil || entryEnd64 > uint64(headersEnd) {
			return RecordFrame{}, truncatedf("header %d body", i)
		}
		entryEnd := int(entryEnd64)
		keyEnd := position + int(keyLength)
		key := encoded[position:keyEnd]
		if !utf8.Valid(key) {
			return RecordFrame{}, invalidf("header %d key is not valid UTF-8", i)
		}
		if i > 0 && bytes.Compare(previousKey, key) >= 0 {
			return RecordFrame{}, invalidf("header keys are not strictly sorted at %q", key)
		}
		frame.Headers = append(frame.Headers, Header{
			Key:   string(key),
			Value: cloneOrAlias(encoded[keyEnd:entryEnd], cloneVariable),
		})
		previousKey = key
		position = entryEnd
	}
	if position != headersEnd {
		return RecordFrame{}, invalidf("headers consumed %d bytes, declared end is %d", position, headersEnd)
	}
	payloadEnd := headersEnd + int(payloadLength)
	frame.Payload = cloneOrAlias(encoded[headersEnd:payloadEnd], cloneVariable)
	return frame, nil
}

// RecordFrameCRC returns the stored CRC after validating the frame.
func RecordFrameCRC(encoded []byte) (uint32, error) {
	if _, err := decodeRecordFrame(encoded, false); err != nil {
		return 0, err
	}
	return getU32(encoded[len(encoded)-RecordCRCSize:]), nil
}

func canonicalHeaders(input []Header) ([]Header, int, error) {
	if len(input) > MaxHeaderCount {
		return nil, 0, fmtTooLarge("header_count", len(input), MaxHeaderCount)
	}
	headers := make([]Header, len(input))
	for i, header := range input {
		if header.Key == "" {
			return nil, 0, invalidf("header %d key is empty", i)
		}
		if !utf8.ValidString(header.Key) {
			return nil, 0, invalidf("header %d key is not valid UTF-8", i)
		}
		if len(header.Key) > MaxHeaderKeyLength {
			return nil, 0, fmtTooLarge("header key", len(header.Key), MaxHeaderKeyLength)
		}
		headers[i] = Header{Key: header.Key, Value: header.Value}
	}
	sort.Slice(headers, func(i, j int) bool {
		return headers[i].Key < headers[j].Key
	})
	var total uint64
	for i, header := range headers {
		if i > 0 && headers[i-1].Key == header.Key {
			return nil, 0, invalidf("duplicate header key %q", header.Key)
		}
		var err error
		total, err = checkedAdd(total, 8, uint64(len(header.Key)), uint64(len(header.Value)))
		if err != nil {
			return nil, 0, err
		}
		if total > MaxHeadersLength {
			return nil, 0, fmtTooLarge("headers", total, MaxHeadersLength)
		}
	}
	length, err := checkedInt(total, "headers_length")
	return headers, length, err
}

func cloneOrAlias(value []byte, clone bool) []byte {
	if clone {
		return bytes.Clone(value)
	}
	return value
}
