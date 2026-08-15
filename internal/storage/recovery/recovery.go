package recovery

import (
	"errors"
	"fmt"
	"github.com/akzj/streamd/internal/storage/format"
	manifeststore "github.com/akzj/streamd/internal/storage/manifest"
	"github.com/akzj/streamd/internal/storage/memtable"
	"github.com/akzj/streamd/internal/storage/registry"
	"github.com/akzj/streamd/internal/storage/segment"
	"github.com/akzj/streamd/internal/storage/wal"
	"os"
	"path/filepath"
	"slices"
	"time"
)

type Result struct {
	Manifest       *manifeststore.Store
	WAL            *wal.Log
	MemTable       *memtable.Table
	Registry       *registry.Registry
	Segments       []*segment.Reader
	AppliedEntryID uint64
	HasApplied     bool
}
type extent struct {
	reader    *segment.Reader
	directory format.StreamDirectoryEntry
}

type Options struct {
	ApplyThrough *uint64
}

func Open(root string) (*Result, error) {
	return OpenWithOptions(root, Options{})
}

func OpenWithOptions(root string, options Options) (*Result, error) {
	ms, err := manifeststore.Open(root)
	if err != nil {
		return nil, err
	}
	table := memtable.New(0)
	reg := registry.New()
	result := &Result{Manifest: ms, MemTable: table, Registry: reg}
	current, hasManifest := ms.Current()
	checkpointID := uint64(0)
	checkpointCRC := uint32(0)
	hasCheckpoint := false
	byStream := make(map[uint64][]extent)
	if hasManifest {
		if current.Header.RecordCount > 0 {
			checkpointID = current.Header.LastEntryID
			checkpointCRC = current.Header.LastEntryCRC32C
			hasCheckpoint = true
		}
		for _, ref := range current.SegmentReferences {
			if ref.Flags&format.SegmentRefHasLocal == 0 {
				return nil, fmt.Errorf("Segment %x has no local copy", ref.SegmentID)
			}
			reader, e := segment.Open(filepath.Join(root, ref.LocalPath))
			if e != nil {
				result.Close()
				return nil, e
			}
			if reader.Header.SegmentID != ref.SegmentID {
				reader.Close()
				result.Close()
				return nil, fmt.Errorf("Segment Reference ID mismatch")
			}
			result.Segments = append(result.Segments, reader)
			for _, d := range reader.Directories {
				byStream[d.StreamID] = append(byStream[d.StreamID], extent{reader: reader, directory: d})
			}
		}
	}
	if options.ApplyThrough != nil && hasCheckpoint && *options.ApplyThrough < checkpointID {
		result.Close()
		return nil, fmt.Errorf("committed recovery watermark %d is behind Manifest checkpoint %d", *options.ApplyThrough, checkpointID)
	}
	for streamID, extents := range byStream {
		slices.SortFunc(extents, func(a, b extent) int {
			if a.directory.FirstSequence < b.directory.FirstSequence {
				return -1
			}
			if a.directory.FirstSequence > b.directory.FirstSequence {
				return 1
			}
			return 0
		})
		var tail memtable.Tail
		for i, e := range extents {
			d := e.directory
			if i > 0 && (d.FirstSequence != tail.NextSequence || d.FirstByteOffset != tail.NextByteOffset || d.FirstRecordedAt < tail.LastRecordedAt) {
				result.Close()
				return nil, fmt.Errorf("Stream %d Segment extents are not continuous", streamID)
			}
			tail = memtable.Tail{NextSequence: d.FirstSequence + d.RecordCount, NextByteOffset: d.NextByteOffset, LastRecordedAt: d.LastRecordedAt, LastEntryID: d.LastEntryID, RecordCount: tail.RecordCount + d.RecordCount}
			if streamID == registry.RegistryStreamID {
				for sequence := d.FirstSequence; sequence < d.FirstSequence+d.RecordCount; sequence++ {
					record, e := e.reader.Read(streamID, sequence)
					if e != nil {
						result.Close()
						return nil, e
					}
					if e = reg.ApplyRecord(record.EntryID, record.Payload); e != nil {
						result.Close()
						return nil, e
					}
				}
			}
		}
		if err = table.SeedTail(streamID, tail); err != nil {
			result.Close()
			return nil, err
		}
	}
	first := uint64(0)
	if hasCheckpoint {
		first = checkpointID + 1
	}
	pending := make([]format.WALEntry, 0)
	applyEntry := func(entry format.WALEntry) error {
		if hasCheckpoint && entry.EntryID <= checkpointID {
			if entry.EntryID == checkpointID && entry.CRC32C != checkpointCRC {
				return fmt.Errorf("WAL checkpoint CRC does not match Manifest")
			}
			return nil
		}
		if options.ApplyThrough != nil && entry.EntryID > *options.ApplyThrough {
			return nil
		}
		if len(pending) == 0 && entry.BatchIndex != 0 {
			return fmt.Errorf("WAL replay starts inside Batch")
		}
		pending = append(pending, entry)
		if uint32(len(pending)) < entry.BatchCount {
			return nil
		}
		if uint32(len(pending)) != entry.BatchCount {
			return fmt.Errorf("WAL Batch length is invalid")
		}
		if e := table.ApplyBatch(pending); e != nil {
			return e
		}
		for _, applied := range pending {
			if applied.StreamID == registry.RegistryStreamID {
				if e := reg.ApplyRecord(applied.EntryID, applied.Record.Payload); e != nil {
					return e
				}
			}
		}
		result.AppliedEntryID = entry.EntryID
		result.HasApplied = true
		pending = pending[:0]
		return nil
	}
	pointerBytes, pointerErr := os.ReadFile(filepath.Join(root, "WAL-CURRENT"))
	if errors.Is(pointerErr, os.ErrNotExist) {
		log, createErr := wal.CreateAfter(root, first, 0, checkpointCRC, time.Now())
		if createErr != nil {
			result.Close()
			return nil, createErr
		}
		result.WAL = log
	} else if pointerErr != nil {
		result.Close()
		return nil, pointerErr
	} else {
		pointer, e := format.UnmarshalWALCurrentPointer(pointerBytes)
		if e != nil {
			result.Close()
			return nil, e
		}
		activeFirst := pointer.FirstEntryID
		expectedPrevious := checkpointCRC
		if activeFirst == 0 {
			expectedPrevious = 0
		} else if activeFirst > first {
			type sealedCandidate struct {
				path string
				scan wal.ScanResult
			}
			var candidates []sealedCandidate
			paths, _ := filepath.Glob(filepath.Join(root, "wal", "*.log"))
			for _, path := range paths {
				if filepath.Base(path) == pointer.FileName {
					continue
				}
				scan, e := wal.ScanSealed(path, nil)
				if e != nil {
					result.Close()
					return nil, e
				}
				if scan.LastEntryID >= first && scan.Header.FirstEntryID < activeFirst {
					candidates = append(candidates, sealedCandidate{path, scan})
				}
			}
			slices.SortFunc(candidates, func(a, b sealedCandidate) int {
				if a.scan.Header.FirstEntryID < b.scan.Header.FirstEntryID {
					return -1
				}
				if a.scan.Header.FirstEntryID > b.scan.Header.FirstEntryID {
					return 1
				}
				return 0
			})
			next := first
			previous := checkpointCRC
			for _, candidate := range candidates {
				if candidate.scan.LastEntryID < next {
					continue
				}
				if candidate.scan.Header.FirstEntryID > next {
					result.Close()
					return nil, fmt.Errorf("sealed WAL chain has a gap before Entry %d", next)
				}
				if candidate.scan.Header.FirstEntryID == next && candidate.scan.FirstEntryPreviousCRC32C != previous {
					result.Close()
					return nil, fmt.Errorf("sealed WAL previous CRC mismatch")
				}
				if _, e = wal.ScanSealed(candidate.path, applyEntry); e != nil {
					result.Close()
					return nil, e
				}
				next = candidate.scan.LastEntryID + 1
				previous = candidate.scan.LastEntryCRC32C
				if next == activeFirst {
					break
				}
			}
			if next != activeFirst {
				result.Close()
				return nil, fmt.Errorf("sealed WAL chain ends at %d, active starts at %d", next, activeFirst)
			}
			expectedPrevious = previous
		} else if activeFirst < first && activeFirst != 0 {
			found := false
			paths, _ := filepath.Glob(filepath.Join(root, "wal", "*.log"))
			for _, path := range paths {
				if filepath.Base(path) == pointer.FileName {
					continue
				}
				scan, scanErr := wal.ScanSealed(path, nil)
				if scanErr != nil {
					continue
				}
				if scan.EntryCount > 0 && scan.LastEntryID+1 == activeFirst {
					expectedPrevious = scan.LastEntryCRC32C
					found = true
					break
				}
			}
			if !found {
				result.Close()
				return nil, fmt.Errorf("active WAL overlap requires its preceding sealed chain")
			}
		}
		log, e := wal.OpenWithPrevious(root, expectedPrevious)
		if e != nil {
			result.Close()
			return nil, e
		}
		result.WAL = log
		if e = log.Replay(applyEntry); e != nil {
			result.Close()
			return nil, e
		}
	}
	if result.WAL == nil {
		log, err := wal.CreateAfter(root, first, 0, checkpointCRC, time.Now())
		if err != nil {
			result.Close()
			return nil, err
		}
		result.WAL = log
	}
	if len(pending) != 0 {
		result.Close()
		return nil, fmt.Errorf("active WAL ends with a partial Batch")
	}
	if !result.HasApplied && hasCheckpoint {
		result.AppliedEntryID = checkpointID
		result.HasApplied = true
	}
	return result, nil
}
func (r *Result) Close() error {
	var errs []error
	if r.WAL != nil {
		errs = append(errs, r.WAL.Close())
		r.WAL = nil
	}
	for _, reader := range r.Segments {
		errs = append(errs, reader.Close())
	}
	r.Segments = nil
	return errors.Join(errs...)
}
