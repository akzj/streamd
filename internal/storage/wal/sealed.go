package wal

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"github.com/akzj/streamd/internal/storage/format"
	"io"
	"os"
)

// ScanSealed verifies a complete immutable WAL without loading the file as one allocation.
func ScanSealed(path string, fn func(format.WALEntry) error) (ScanResult, error) {
	var result ScanResult
	f, err := os.Open(path)
	if err != nil {
		return result, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return result, err
	}
	if info.Size() < format.WALFileHeaderLength+format.WALSealFooterLength {
		return result, fmt.Errorf("sealed WAL is truncated")
	}
	contentEnd := info.Size() - format.WALSealFooterLength
	footerBytes := make([]byte, format.WALSealFooterLength)
	if _, err = f.ReadAt(footerBytes, contentEnd); err != nil {
		return result, err
	}
	footer, err := format.UnmarshalWALSealFooter(footerBytes)
	if err != nil {
		return result, err
	}
	headerBytes := make([]byte, format.WALFileHeaderLength)
	if _, err = f.ReadAt(headerBytes, 0); err != nil {
		return result, err
	}
	result.Header, err = format.UnmarshalWALFileHeader(headerBytes)
	if err != nil {
		return result, err
	}
	if result.Header.FileID != footer.FileID {
		return result, fmt.Errorf("sealed WAL Header/Footer ID mismatch")
	}
	hash := sha256.New()
	if _, err = io.CopyN(hash, io.NewSectionReader(f, 0, contentEnd), contentEnd); err != nil {
		return result, err
	}
	var digest [sha256.Size]byte
	copy(digest[:], hash.Sum(nil))
	if digest != footer.ContentSHA256 {
		return result, fmt.Errorf("sealed WAL content SHA-256 mismatch")
	}
	pos := int64(format.WALFileHeaderLength)
	next := result.Header.FirstEntryID
	var previous uint32
	for pos < contentEnd {
		remaining := contentEnd - pos
		if remaining < format.WALEntryHeaderLength {
			return result, fmt.Errorf("sealed WAL Entry header is truncated")
		}
		head := make([]byte, format.WALEntryHeaderLength)
		if _, err = f.ReadAt(head, pos); err != nil {
			return result, err
		}
		length := uint64(binary.LittleEndian.Uint32(head[8:12]))
		if length < format.WALEntryHeaderLength+format.RecordFixedHeaderLength+format.RecordCRCSize+4 || length > format.WALEntryHeaderLength+format.MaxFrameLength+4 || int64(length) > remaining {
			return result, fmt.Errorf("sealed WAL Entry length is invalid")
		}
		b := make([]byte, int(length))
		if _, err = f.ReadAt(b, pos); err != nil {
			return result, err
		}
		entry, err := format.UnmarshalWALEntry(b)
		if err != nil {
			return result, err
		}
		if result.EntryCount == 0 {
			result.FirstEntryPreviousCRC32C = entry.PreviousEntryCRC32C
		}
		if entry.EntryID != next || (result.EntryCount > 0 && entry.PreviousEntryCRC32C != previous) {
			return result, fmt.Errorf("sealed WAL continuity failure")
		}
		if fn != nil {
			if err = fn(entry); err != nil {
				return result, err
			}
		}
		previous = entry.CRC32C
		result.EntryCount++
		result.LastEntryID = entry.EntryID
		result.LastEntryCRC32C = entry.CRC32C
		pos += int64(length)
	}
	result.LastGoodOffset = pos
	if result.EntryCount != footer.EntryCount || result.LastEntryID != footer.LastEntryID || result.LastEntryCRC32C != footer.LastEntryCRC32C {
		return result, fmt.Errorf("sealed WAL Footer summary mismatch")
	}
	return result, nil
}
