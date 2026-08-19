package wal

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/akzj/streamd/internal/storage/format"
	"github.com/akzj/streamd/internal/storage/fsutil"
)

type ScanResult struct {
	Header                   format.WALFileHeader
	EntryCount               uint64
	LastEntryID              uint64
	LastEntryCRC32C          uint32
	LastGoodOffset           int64
	TruncatedBytes           int64
	FirstEntryPreviousCRC32C uint32
}
type Log struct {
	root                   string
	file                   *os.File
	pointer                format.WALCurrentPointer
	scan                   ScanResult
	expectedPreviousCRC32C uint32
	contentHash            hash.Hash
}

func Create(root string, firstEntryID, term uint64, now time.Time) (*Log, error) {
	return CreateAfter(root, firstEntryID, term, 0, now)
}
func CreateAfter(root string, firstEntryID, term uint64, previousCRC32C uint32, now time.Time) (*Log, error) {
	if firstEntryID == 0 && previousCRC32C != 0 {
		return nil, fmt.Errorf("Entry 0 cannot have a previous CRC")
	}
	var id format.UUID
	if _, err := rand.Read(id[:]); err != nil {
		return nil, err
	}
	name := fmt.Sprintf("WAL-%020d-%x.log", firstEntryID, id)
	header := format.WALFileHeader{FileID: id, FirstEntryID: firstEntryID, CreatedTerm: term, CreatedAt: now.UnixNano()}
	path := filepath.Join(root, "wal", name)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0640)
	if err != nil {
		return nil, err
	}
	ok := false
	defer func() {
		if !ok {
			f.Close()
			os.Remove(path)
		}
	}()
	headerBytes := format.MarshalWALFileHeader(header)
	if err = writeFull(f, headerBytes); err != nil {
		return nil, err
	}
	if err = f.Sync(); err != nil {
		return nil, err
	}
	if err = fsutil.SyncDir(filepath.Join(root, "wal")); err != nil {
		return nil, err
	}
	pointer := format.WALCurrentPointer{FileID: id, FirstEntryID: firstEntryID, FileName: name}
	pb, err := format.MarshalWALCurrentPointer(pointer)
	if err != nil {
		return nil, err
	}
	if err = fsutil.AtomicWrite(root, "WAL-CURRENT", pb, 0640, nil); err != nil {
		return nil, err
	}
	ok = true
	contentHash := sha256.New()
	_, _ = contentHash.Write(headerBytes)
	return &Log{root: root, file: f, pointer: pointer, scan: ScanResult{Header: header, LastGoodOffset: format.WALFileHeaderLength}, expectedPreviousCRC32C: previousCRC32C, contentHash: contentHash}, nil
}

func Open(root string) (*Log, error) {
	return OpenWithPrevious(root, 0)
}
func OpenWithPrevious(root string, previousCRC32C uint32) (*Log, error) {
	pb, err := os.ReadFile(filepath.Join(root, "WAL-CURRENT"))
	if err != nil {
		return nil, err
	}
	pointer, err := format.UnmarshalWALCurrentPointer(pb)
	if err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Join(root, "wal", pointer.FileName), os.O_RDWR, 0)
	if err != nil {
		return nil, err
	}
	scan, contentHash, err := scanActive(f)
	if err != nil {
		f.Close()
		return nil, err
	}
	if scan.Header.FileID != pointer.FileID || scan.Header.FirstEntryID != pointer.FirstEntryID {
		f.Close()
		return nil, fmt.Errorf("WAL-CURRENT does not match active WAL header")
	}
	if scan.TruncatedBytes > 0 {
		if err = f.Truncate(scan.LastGoodOffset); err != nil {
			f.Close()
			return nil, err
		}
		if err = f.Sync(); err != nil {
			f.Close()
			return nil, err
		}
	}
	if _, err = f.Seek(scan.LastGoodOffset, io.SeekStart); err != nil {
		f.Close()
		return nil, err
	}
	if pointer.FirstEntryID == 0 {
		previousCRC32C = 0
	}
	if scan.EntryCount > 0 && scan.FirstEntryPreviousCRC32C != previousCRC32C {
		f.Close()
		return nil, fmt.Errorf("active WAL does not continue previous CRC")
	}
	return &Log{root: root, file: f, pointer: pointer, scan: scan, expectedPreviousCRC32C: previousCRC32C, contentHash: contentHash}, nil
}

func ScanActive(f *os.File) (ScanResult, error) {
	result, _, err := scanActive(f)
	return result, err
}

func scanActive(f *os.File) (ScanResult, hash.Hash, error) {
	var result ScanResult
	contentHash := sha256.New()
	info, err := f.Stat()
	if err != nil {
		return result, nil, err
	}
	if info.Size() < format.WALFileHeaderLength {
		return result, nil, fmt.Errorf("active WAL header is truncated")
	}
	hb := make([]byte, format.WALFileHeaderLength)
	if _, err = f.ReadAt(hb, 0); err != nil {
		return result, nil, err
	}
	result.Header, err = format.UnmarshalWALFileHeader(hb)
	if err != nil {
		return result, nil, err
	}
	_, _ = contentHash.Write(hb)
	pos := int64(format.WALFileHeaderLength)
	result.LastGoodOffset = pos
	next := result.Header.FirstEntryID
	var previous uint32
	for pos < info.Size() {
		remaining := info.Size() - pos
		if remaining < format.WALEntryHeaderLength {
			result.TruncatedBytes = remaining
			break
		}
		head := make([]byte, format.WALEntryHeaderLength)
		if _, err = f.ReadAt(head, pos); err != nil {
			return result, nil, err
		}
		length := uint64(binary.LittleEndian.Uint32(head[8:12]))
		if length < format.WALEntryHeaderLength+format.RecordFixedHeaderLength+format.RecordCRCSize+4 || length > format.WALEntryHeaderLength+format.MaxFrameLength+4 {
			return result, nil, fmt.Errorf("invalid WAL entry length %d at %d", length, pos)
		}
		if int64(length) > remaining {
			result.TruncatedBytes = remaining
			break
		}
		entryBytes := make([]byte, int(length))
		if _, err = f.ReadAt(entryBytes, pos); err != nil {
			return result, nil, err
		}
		entry, err := format.UnmarshalWALEntry(entryBytes)
		if err != nil {
			return result, nil, fmt.Errorf("WAL entry at %d: %w", pos, err)
		}
		if result.EntryCount == 0 {
			result.FirstEntryPreviousCRC32C = entry.PreviousEntryCRC32C
		}
		if entry.EntryID != next || (result.EntryCount > 0 && entry.PreviousEntryCRC32C != previous) {
			return result, nil, fmt.Errorf("WAL continuity failure at Entry %d", entry.EntryID)
		}
		_, _ = contentHash.Write(entryBytes)
		previous = entry.CRC32C
		result.EntryCount++
		result.LastEntryID = entry.EntryID
		result.LastEntryCRC32C = entry.CRC32C
		next++
		pos += int64(length)
		result.LastGoodOffset = pos
	}
	return result, contentHash, nil
}

func (l *Log) Append(encodedEntries ...[]byte) error {
	if l == nil || l.file == nil {
		return os.ErrClosed
	}
	next := l.pointer.FirstEntryID + l.scan.EntryCount
	previous := l.scan.LastEntryCRC32C
	if l.scan.EntryCount == 0 {
		previous = l.expectedPreviousCRC32C
	}
	for i, b := range encodedEntries {
		entry, err := format.UnmarshalWALEntry(b)
		if err != nil {
			return fmt.Errorf("entry %d: %w", i, err)
		}
		if entry.EntryID != next || entry.PreviousEntryCRC32C != previous {
			return fmt.Errorf("entry %d is not continuous", i)
		}
		previous = entry.CRC32C
		next++
	}
	for _, b := range encodedEntries {
		if err := writeFull(l.file, b); err != nil {
			return err
		}
		_, _ = l.contentHash.Write(b)
		entry, _ := format.UnmarshalWALEntry(b)
		l.scan.EntryCount++
		l.scan.LastEntryID = entry.EntryID
		l.scan.LastEntryCRC32C = entry.CRC32C
		l.scan.LastGoodOffset += int64(len(b))
	}
	return nil
}
func (l *Log) Sync() error { return l.file.Sync() }
func (l *Log) Close() error {
	if l == nil || l.file == nil {
		return nil
	}
	err := l.file.Close()
	l.file = nil
	return err
}
func (l *Log) Scan() ScanResult    { return l.scan }
func (l *Log) NextEntryID() uint64 { return l.pointer.FirstEntryID + l.scan.EntryCount }
func (l *Log) PreviousEntryCRC32C() uint32 {
	if l.scan.EntryCount > 0 {
		return l.scan.LastEntryCRC32C
	}
	return l.expectedPreviousCRC32C
}
func (l *Log) Replay(fn func(format.WALEntry) error) error {
	pos := int64(format.WALFileHeaderLength)
	for pos < l.scan.LastGoodOffset {
		head := make([]byte, format.WALEntryHeaderLength)
		if _, err := l.file.ReadAt(head, pos); err != nil {
			return err
		}
		length := int(binary.LittleEndian.Uint32(head[8:12]))
		b := make([]byte, length)
		if _, err := l.file.ReadAt(b, pos); err != nil {
			return err
		}
		entry, err := format.UnmarshalWALEntry(b)
		if err != nil {
			return err
		}
		if err = fn(entry); err != nil {
			return err
		}
		pos += int64(length)
	}
	return nil
}
func (l *Log) Seal() error {
	if l == nil || l.file == nil {
		return os.ErrClosed
	}
	if err := l.file.Sync(); err != nil {
		return err
	}
	var digest [sha256.Size]byte
	copy(digest[:], l.contentHash.Sum(nil))
	footer := format.WALSealFooter{FileID: l.pointer.FileID, EntryCount: l.scan.EntryCount, LastEntryID: l.scan.LastEntryID, LastEntryCRC32C: l.scan.LastEntryCRC32C, ContentSHA256: digest}
	fb, err := format.MarshalWALSealFooter(footer)
	if err != nil {
		return err
	}
	if _, err = l.file.Seek(l.scan.LastGoodOffset, io.SeekStart); err != nil {
		return err
	}
	if err = writeFull(l.file, fb); err != nil {
		return err
	}
	return l.file.Sync()
}

// Rotate seals the current file, persists a new empty WAL, then publishes WAL-CURRENT.
func (l *Log) Rotate(term uint64, now time.Time) error {
	if l == nil || l.file == nil {
		return os.ErrClosed
	}
	oldPath := filepath.Join(l.root, "wal", l.pointer.FileName)
	oldEntryCount := l.scan.EntryCount
	previous := l.scan.LastEntryCRC32C
	if oldEntryCount == 0 {
		previous = l.expectedPreviousCRC32C
	} else if err := l.Seal(); err != nil {
		return err
	}
	first := l.pointer.FirstEntryID + l.scan.EntryCount
	if err := l.file.Close(); err != nil {
		return err
	}
	l.file = nil
	var id format.UUID
	if _, err := rand.Read(id[:]); err != nil {
		return err
	}
	name := fmt.Sprintf("WAL-%020d-%x.log", first, id)
	walDir := filepath.Join(l.root, "wal")
	path := filepath.Join(walDir, name)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0640)
	if err != nil {
		return err
	}
	published := false
	defer func() {
		if !published {
			f.Close()
			_ = os.Remove(path)
		}
	}()
	header := format.WALFileHeader{FileID: id, FirstEntryID: first, CreatedTerm: term, CreatedAt: now.UnixNano()}
	headerBytes := format.MarshalWALFileHeader(header)
	if err = writeFull(f, headerBytes); err != nil {
		return err
	}
	if err = f.Sync(); err != nil {
		return err
	}
	if err = fsutil.SyncDir(walDir); err != nil {
		return err
	}
	pointer := format.WALCurrentPointer{FileID: id, FirstEntryID: first, FileName: name}
	pb, err := format.MarshalWALCurrentPointer(pointer)
	if err != nil {
		return err
	}
	if err = fsutil.AtomicWrite(l.root, "WAL-CURRENT", pb, 0640, nil); err != nil {
		return err
	}
	published = true
	l.file = f
	l.pointer = pointer
	l.scan = ScanResult{Header: header, LastGoodOffset: format.WALFileHeaderLength}
	l.expectedPreviousCRC32C = previous
	l.contentHash = sha256.New()
	_, _ = l.contentHash.Write(headerBytes)
	if oldEntryCount == 0 {
		// The old active WAL was empty and never part of history. Removal is
		// best-effort after the new pointer is durable; crash recovery ignores
		// an unpublished header-only orphan.
		_ = os.Remove(oldPath)
		_ = fsutil.SyncDir(walDir)
	}
	return nil
}
func writeFull(w io.Writer, b []byte) error {
	for len(b) > 0 {
		n, err := w.Write(b)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		b = b[n:]
	}
	return nil
}
