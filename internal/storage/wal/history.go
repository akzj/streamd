package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sync"

	"github.com/akzj/streamd/internal/storage/format"
)

var (
	ErrNotRetained       = errors.New("WAL Entry is not retained")
	ErrHistoryAhead      = errors.New("WAL Entry is ahead of history")
	ErrEntryTooLarge     = errors.New("WAL Entry exceeds range byte limit")
	ErrRetentionPressure = errors.New("WAL retention exceeds its configured bound")
)

type GCOptions struct {
	SegmentedThrough uint64
	SnapshotThrough  uint64
	SnapshotVerified bool
	ReplicaDurable   HistoryPosition
	MaxRetainedBytes uint64
}

type GCResult struct {
	DeletedFiles      []string
	DeletedBytes      uint64
	RetainedBytes     uint64
	EarliestWAL       uint64
	NeedsSnapshot     bool
	RetentionPressure bool
}

type HistoryPosition struct {
	Present bool
	EntryID uint64
	CRC32C  uint32
}

type HistoryRange struct {
	Entries     [][]byte
	NextEntryID uint64
	Last        HistoryPosition
}

type historyFile struct {
	path       string
	name       string
	first      uint64
	count      uint64
	last       uint64
	lastCRC    uint32
	firstPrev  uint32
	contentEnd int64
	sealed     bool
	size       int64
}

type History struct {
	mu          sync.RWMutex
	root        string
	files       []historyFile
	pins        map[string]uint64
	sealedCache map[string]historyFile
}

func OpenHistory(root string) (*History, error) {
	history := &History{root: root, pins: make(map[string]uint64), sealedCache: make(map[string]historyFile)}
	if err := history.Refresh(); err != nil {
		return nil, err
	}
	return history, nil
}

func (h *History) Refresh() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	pointerBytes, err := os.ReadFile(filepath.Join(h.root, "WAL-CURRENT"))
	if err != nil {
		return err
	}
	pointer, err := format.UnmarshalWALCurrentPointer(pointerBytes)
	if err != nil {
		return err
	}
	paths, err := filepath.Glob(filepath.Join(h.root, "wal", "*.log"))
	if err != nil {
		return err
	}
	var active historyFile
	activeFound := false
	sealed := make([]historyFile, 0, len(paths))
	for _, path := range paths {
		name := filepath.Base(path)
		if name == pointer.FileName {
			active, err = scanHistoryFile(path, false)
			if err != nil {
				// Rotate seals the old active file before publishing the new
				// WAL-CURRENT. A concurrent refresh may observe that durable
				// intermediate state, which is a valid history endpoint.
				active, err = scanHistoryFile(path, true)
				if err != nil {
					return err
				}
			}
			if active.first != pointer.FirstEntryID || activeHeaderID(path) != pointer.FileID {
				return fmt.Errorf("WAL-CURRENT does not match active WAL")
			}
			activeFound = true
			continue
		}
		info, statErr := os.Stat(path)
		if statErr != nil {
			return statErr
		}
		cached, ok := h.sealedCache[path]
		if ok && cached.size == info.Size() {
			sealed = append(sealed, cached)
			continue
		}
		meta, scanErr := scanHistoryFile(path, true)
		if scanErr != nil {
			orphan, activeErr := scanHistoryFile(path, false)
			if activeErr == nil && orphan.count == 0 {
				// A header-only, non-current WAL can be left before the
				// WAL-CURRENT switch or observed during Rotate. It never
				// entered the published history.
				continue
			}
			return scanErr
		}
		h.sealedCache[path] = meta
		sealed = append(sealed, meta)
	}
	if !activeFound {
		return fmt.Errorf("WAL-CURRENT active file is missing")
	}
	chain := []historyFile{active}
	nextFirst := active.first
	nextPrevious := active.firstPrev
	nextHasPrevious := active.count > 0
	for nextFirst > 0 {
		candidate := -1
		for i := range sealed {
			if sealed[i].count > 0 && sealed[i].last != ^uint64(0) && sealed[i].last+1 == nextFirst {
				if candidate >= 0 {
					return fmt.Errorf("multiple sealed WAL files precede Entry %d", nextFirst)
				}
				candidate = i
			}
		}
		if candidate < 0 {
			break
		}
		previous := sealed[candidate]
		if nextHasPrevious && previous.lastCRC != nextPrevious {
			return fmt.Errorf("WAL checksum chain differs before Entry %d", nextFirst)
		}
		chain = append(chain, previous)
		nextFirst = previous.first
		nextPrevious = previous.firstPrev
		nextHasPrevious = true
		sealed = append(sealed[:candidate], sealed[candidate+1:]...)
	}
	slices.Reverse(chain)
	h.files = chain
	return nil
}

func (h *History) Bounds() (earliest, next uint64, present bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return historyBounds(h.files)
}

func (h *History) ChecksumAt(entryID uint64) (uint32, bool, error) {
	encoded, entry, err := h.EntryAt(entryID)
	_ = encoded
	if errors.Is(err, ErrNotRetained) || errors.Is(err, ErrHistoryAhead) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	return entry.CRC32C, true, nil
}

func (h *History) EntryAt(entryID uint64) ([]byte, format.WALEntry, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	file, err := locateHistoryFile(h.files, entryID)
	if err != nil {
		return nil, format.WALEntry{}, err
	}
	encoded, err := readEntryFromFile(file, entryID)
	if err != nil {
		return nil, format.WALEntry{}, err
	}
	entry, err := format.UnmarshalWALEntry(encoded)
	return encoded, entry, err
}

func (h *History) ReadRange(from uint64, maxEntries int, maxBytes uint64) (HistoryRange, error) {
	if maxEntries <= 0 {
		return HistoryRange{}, fmt.Errorf("max Entries must be positive")
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	earliest, next, present := historyBounds(h.files)
	if !present {
		if from == 0 {
			return HistoryRange{NextEntryID: 0}, nil
		}
		return HistoryRange{}, ErrHistoryAhead
	}
	if from < earliest {
		return HistoryRange{}, ErrNotRetained
	}
	if from > next {
		return HistoryRange{}, ErrHistoryAhead
	}
	result := HistoryRange{NextEntryID: from}
	if from == next {
		return result, nil
	}
	for _, file := range h.files {
		if file.count == 0 || result.NextEntryID > file.last || result.NextEntryID < file.first {
			continue
		}
		stop, err := appendFileRange(&result, file, maxEntries, maxBytes)
		if err != nil {
			return HistoryRange{}, err
		}
		if stop || len(result.Entries) == maxEntries {
			break
		}
	}
	return result, nil
}

func (h *History) PinRange(from, through uint64) (func(), error) {
	if through < from {
		return nil, fmt.Errorf("Pin range is reversed")
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	first, err := locateHistoryFile(h.files, from)
	if err != nil {
		return nil, err
	}
	last, err := locateHistoryFile(h.files, through)
	if err != nil {
		return nil, err
	}
	var names []string
	inRange := false
	for _, file := range h.files {
		if file.name == first.name {
			inRange = true
		}
		if inRange {
			h.pins[file.name]++
			names = append(names, file.name)
		}
		if file.name == last.name {
			break
		}
	}
	var once sync.Once
	return func() {
		once.Do(func() {
			h.mu.Lock()
			defer h.mu.Unlock()
			for _, name := range names {
				h.pins[name]--
				if h.pins[name] == 0 {
					delete(h.pins, name)
				}
			}
		})
	}, nil
}

func (h *History) Pinned(fileName string) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.pins[fileName] > 0
}

func (h *History) RetainedFiles() []string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	result := make([]string, len(h.files))
	for i, file := range h.files {
		result[i] = file.name
	}
	return result
}

// Collect removes only a contiguous sealed WAL prefix covered by both local
// Segments and a verified installable Snapshot. A pin stops collection at that
// file so the retained history can never acquire a hole.
func (h *History) Collect(options GCOptions) (GCResult, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	result := GCResult{}
	if len(h.files) == 0 {
		return result, fmt.Errorf("WAL history is empty")
	}
	through := options.SegmentedThrough
	if options.SnapshotThrough < through {
		through = options.SnapshotThrough
	}
	deleteCount := 0
	if options.SnapshotVerified {
		for _, file := range h.files {
			if !file.sealed || file.count == 0 || file.last > through || h.pins[file.name] > 0 {
				break
			}
			if err := os.Remove(file.path); err != nil {
				return result, err
			}
			result.DeletedFiles = append(result.DeletedFiles, file.name)
			result.DeletedBytes += uint64(file.size)
			delete(h.sealedCache, file.path)
			deleteCount++
		}
	}
	if deleteCount > 0 {
		h.files = append([]historyFile(nil), h.files[deleteCount:]...)
		if err := syncWALDir(h.root); err != nil {
			return result, err
		}
	}
	for _, file := range h.files {
		if file.size > 0 {
			result.RetainedBytes += uint64(file.size)
		}
	}
	result.EarliestWAL = h.files[0].first
	if result.EarliestWAL > 0 && (!options.ReplicaDurable.Present || options.ReplicaDurable.EntryID < result.EarliestWAL-1) {
		result.NeedsSnapshot = true
	}
	result.RetentionPressure = options.MaxRetainedBytes > 0 && result.RetainedBytes > options.MaxRetainedBytes
	if result.RetentionPressure {
		return result, ErrRetentionPressure
	}
	return result, nil
}

func syncWALDir(root string) error {
	directory, err := os.Open(filepath.Join(root, "wal"))
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}

func scanHistoryFile(path string, sealed bool) (historyFile, error) {
	var scan ScanResult
	var err error
	if sealed {
		scan, err = ScanSealed(path, nil)
	} else {
		file, openErr := os.Open(path)
		if openErr != nil {
			return historyFile{}, openErr
		}
		scan, err = ScanActive(file)
		file.Close()
	}
	if err != nil {
		return historyFile{}, err
	}
	info, err := os.Stat(path)
	if err != nil {
		return historyFile{}, err
	}
	meta := historyFile{path: path, name: filepath.Base(path), first: scan.Header.FirstEntryID, count: scan.EntryCount, last: scan.LastEntryID, lastCRC: scan.LastEntryCRC32C, firstPrev: scan.FirstEntryPreviousCRC32C, contentEnd: scan.LastGoodOffset, sealed: sealed, size: info.Size()}
	return meta, nil
}

func activeHeaderID(path string) format.UUID {
	file, err := os.Open(path)
	if err != nil {
		return format.UUID{}
	}
	defer file.Close()
	encoded := make([]byte, format.WALFileHeaderLength)
	if _, err = io.ReadFull(file, encoded); err != nil {
		return format.UUID{}
	}
	header, err := format.UnmarshalWALFileHeader(encoded)
	if err != nil {
		return format.UUID{}
	}
	return header.FileID
}

func historyBounds(files []historyFile) (earliest, next uint64, present bool) {
	for _, file := range files {
		if file.count > 0 {
			if !present {
				earliest = file.first
				present = true
			}
			next = file.last + 1
		}
	}
	if !present && len(files) > 0 {
		next = files[len(files)-1].first
	}
	return earliest, next, present
}

func locateHistoryFile(files []historyFile, entryID uint64) (historyFile, error) {
	earliest, next, present := historyBounds(files)
	if !present {
		return historyFile{}, ErrHistoryAhead
	}
	if entryID < earliest {
		return historyFile{}, ErrNotRetained
	}
	if entryID >= next {
		return historyFile{}, ErrHistoryAhead
	}
	for _, file := range files {
		if file.count > 0 && entryID >= file.first && entryID <= file.last {
			return file, nil
		}
	}
	return historyFile{}, fmt.Errorf("WAL history has a gap at Entry %d", entryID)
}

func readEntryFromFile(meta historyFile, entryID uint64) ([]byte, error) {
	file, err := os.Open(meta.path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	position := int64(format.WALFileHeaderLength)
	current := meta.first
	for position < meta.contentEnd {
		encoded, next, readErr := readEncodedEntry(file, position, meta.contentEnd)
		if readErr != nil {
			return nil, readErr
		}
		if current == entryID {
			return encoded, nil
		}
		position = next
		current++
	}
	return nil, fmt.Errorf("Entry %d is missing from %s", entryID, meta.name)
}

func appendFileRange(result *HistoryRange, meta historyFile, maxEntries int, maxBytes uint64) (bool, error) {
	file, err := os.Open(meta.path)
	if err != nil {
		return false, err
	}
	defer file.Close()
	position := int64(format.WALFileHeaderLength)
	current := meta.first
	var used uint64
	for _, encoded := range result.Entries {
		used += uint64(len(encoded))
	}
	for position < meta.contentEnd {
		encoded, next, readErr := readEncodedEntry(file, position, meta.contentEnd)
		if readErr != nil {
			return false, readErr
		}
		position = next
		if current < result.NextEntryID {
			current++
			continue
		}
		if maxBytes > 0 && uint64(len(encoded)) > maxBytes-used {
			if len(result.Entries) == 0 {
				return false, ErrEntryTooLarge
			}
			return true, nil
		}
		entry, decodeErr := format.UnmarshalWALEntry(encoded)
		if decodeErr != nil {
			return false, decodeErr
		}
		result.Entries = append(result.Entries, encoded)
		result.Last = HistoryPosition{Present: true, EntryID: entry.EntryID, CRC32C: entry.CRC32C}
		result.NextEntryID = entry.EntryID + 1
		used += uint64(len(encoded))
		current++
		if len(result.Entries) == maxEntries {
			return true, nil
		}
	}
	return false, nil
}

func readEncodedEntry(file *os.File, position, contentEnd int64) ([]byte, int64, error) {
	if contentEnd-position < format.WALEntryHeaderLength {
		return nil, position, fmt.Errorf("WAL Entry Header is truncated")
	}
	header := make([]byte, format.WALEntryHeaderLength)
	if _, err := file.ReadAt(header, position); err != nil {
		return nil, position, err
	}
	length := uint64(binary.LittleEndian.Uint32(header[8:12]))
	if length < format.WALEntryHeaderLength+format.RecordFixedHeaderLength+format.RecordCRCSize+4 || length > format.WALEntryHeaderLength+format.MaxFrameLength+4 || int64(length) > contentEnd-position {
		return nil, position, fmt.Errorf("WAL Entry length is invalid")
	}
	encoded := make([]byte, int(length))
	if _, err := file.ReadAt(encoded, position); err != nil {
		return nil, position, err
	}
	if _, err := format.UnmarshalWALEntry(encoded); err != nil {
		return nil, position, err
	}
	return encoded, position + int64(length), nil
}
