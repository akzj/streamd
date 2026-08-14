package wal

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/akzj/streamd/internal/storage/format"
	"github.com/akzj/streamd/internal/storage/fsutil"
)

type ScanResult struct {
	Header          format.WALFileHeader
	EntryCount      uint64
	LastEntryID     uint64
	LastEntryCRC32C uint32
	LastGoodOffset  int64
	TruncatedBytes  int64
}
type Log struct {
	root                   string
	file                   *os.File
	pointer                format.WALCurrentPointer
	scan                   ScanResult
	expectedPreviousCRC32C uint32
}

func Create(root string, firstEntryID, term uint64, now time.Time) (*Log, error) {
	if firstEntryID != 0 {
		return nil, fmt.Errorf("initial WAL must start at Entry 0")
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
	if _, err = f.Write(format.MarshalWALFileHeader(header)); err != nil {
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
	return &Log{root: root, file: f, pointer: pointer, scan: ScanResult{Header: header, LastGoodOffset: format.WALFileHeaderLength}}, nil
}

func Open(root string) (*Log, error) {
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
	scan, err := ScanActive(f)
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
	if scan.EntryCount == 0 && pointer.FirstEntryID != 0 {
		f.Close()
		return nil, fmt.Errorf("empty rotated WAL requires previous sealed WAL recovery")
	}
	return &Log{root: root, file: f, pointer: pointer, scan: scan}, nil
}

func ScanActive(f *os.File) (ScanResult, error) {
	var result ScanResult
	info, err := f.Stat()
	if err != nil {
		return result, err
	}
	if info.Size() < format.WALFileHeaderLength {
		return result, fmt.Errorf("active WAL header is truncated")
	}
	hb := make([]byte, format.WALFileHeaderLength)
	if _, err = f.ReadAt(hb, 0); err != nil {
		return result, err
	}
	result.Header, err = format.UnmarshalWALFileHeader(hb)
	if err != nil {
		return result, err
	}
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
			return result, err
		}
		length := uint64(binary.LittleEndian.Uint32(head[8:12]))
		if length < format.WALEntryHeaderLength+format.RecordFixedHeaderLength+format.RecordCRCSize+4 || length > format.WALEntryHeaderLength+format.MaxFrameLength+4 {
			return result, fmt.Errorf("invalid WAL entry length %d at %d", length, pos)
		}
		if int64(length) > remaining {
			result.TruncatedBytes = remaining
			break
		}
		entryBytes := make([]byte, int(length))
		if _, err = f.ReadAt(entryBytes, pos); err != nil {
			return result, err
		}
		entry, err := format.UnmarshalWALEntry(entryBytes)
		if err != nil {
			return result, fmt.Errorf("WAL entry at %d: %w", pos, err)
		}
		if entry.EntryID != next || (result.EntryCount > 0 && entry.PreviousEntryCRC32C != previous) {
			return result, fmt.Errorf("WAL continuity failure at Entry %d", entry.EntryID)
		}
		previous = entry.CRC32C
		result.EntryCount++
		result.LastEntryID = entry.EntryID
		result.LastEntryCRC32C = entry.CRC32C
		next++
		pos += int64(length)
		result.LastGoodOffset = pos
	}
	return result, nil
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
func (l *Log) Scan() ScanResult { return l.scan }
func (l *Log) Seal() error {
	if l == nil || l.file == nil {
		return os.ErrClosed
	}
	if err := l.file.Sync(); err != nil {
		return err
	}
	h := sha256.New()
	if _, err := l.file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	if _, err := io.CopyN(h, l.file, l.scan.LastGoodOffset); err != nil {
		return err
	}
	var digest [sha256.Size]byte
	copy(digest[:], h.Sum(nil))
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
	if err := l.Seal(); err != nil {
		return err
	}
	previous := l.scan.LastEntryCRC32C
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
	if err = writeFull(f, format.MarshalWALFileHeader(header)); err != nil {
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
