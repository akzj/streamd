package read

import (
	"fmt"
	"sort"

	"github.com/akzj/streamd/internal/storage/errdefs"
	"github.com/akzj/streamd/internal/storage/format"
	locatorstore "github.com/akzj/streamd/internal/storage/locator"
	"github.com/akzj/streamd/internal/storage/memtable"
	"github.com/akzj/streamd/internal/storage/segment"
	tailstore "github.com/akzj/streamd/internal/storage/tail"
)

type TimeMode uint8

const (
	AtOrAfter TimeMode = iota + 1
	AtOrBefore
)

type Result struct {
	Records             []format.RecordFrame
	NextSequence        uint64
	CurrentNextSequence uint64
}
type StreamInfo struct {
	Exists          bool
	NextSequence    uint64
	RecordCount     uint64
	FirstRecordedAt int64
	LastRecordedAt  int64
}
type extent struct {
	reference format.SegmentReference
	directory format.StreamDirectoryEntry
}
type Store struct {
	table      *memtable.Table
	tails      *tailstore.Resolver
	root       string
	generation uint64
	segments   []segment.Descriptor
	handles    *segment.HandleCache
	locator    *locatorstore.Store
}

func New(table *memtable.Table, tails *tailstore.Resolver, root string, generation uint64, segments []segment.Descriptor, locator *locatorstore.Store, streamCacheCapacity, handleCapacity int) *Store {
	if tails == nil {
		tails = tailstore.NewResolver(table, nil, root, segments, streamCacheCapacity)
	}
	return &Store{table: table, tails: tails, root: root, generation: generation, segments: append([]segment.Descriptor(nil), segments...), handles: segment.NewHandleCache(root, handleCapacity), locator: locator}
}
func (s *Store) Read(streamID, from uint64, maxRecords int, maxBytes uint64) (Result, error) {
	tail, ok, err := s.tails.Lookup(streamID)
	if err != nil {
		return Result{}, err
	}
	if !ok {
		return Result{}, fmt.Errorf("Stream %d: %w", streamID, errdefs.ErrStreamNotFound)
	}
	result := Result{NextSequence: from, CurrentNextSequence: tail.NextSequence}
	if from > tail.NextSequence {
		return result, &errdefs.SequenceAheadError{Requested: from, CurrentNextSequence: tail.NextSequence}
	}
	if maxRecords <= 0 || from == tail.NextSequence {
		return result, nil
	}
	base, _, active := s.table.ActiveRange(streamID)
	for sequence := from; sequence < tail.NextSequence && len(result.Records) < maxRecords; sequence++ {
		var record format.RecordFrame
		var err error
		if active && sequence >= base {
			records, _, e := s.table.Read(streamID, sequence, 1)
			err = e
			if e == nil && len(records) == 1 {
				record = records[0]
			}
		} else {
			record, err = s.readSegment(streamID, sequence)
		}
		if err != nil {
			return result, err
		}
		encoded, e := format.MarshalRecordFrame(record)
		if e != nil {
			return result, e
		}
		if maxBytes > 0 && uint64(len(encoded)) > maxBytes {
			if len(result.Records) == 0 {
				return result, &errdefs.RecordTooLargeError{Sequence: sequence, RequiredBytes: uint64(len(encoded))}
			}
			break
		}
		result.Records = append(result.Records, record)
		result.NextSequence = sequence + 1
		if maxBytes > 0 {
			maxBytes -= uint64(len(encoded))
		}
	}
	return result, nil
}
func (s *Store) readSegment(streamID, sequence uint64) (format.RecordFrame, error) {
	if s.locator != nil {
		extent, found, err := s.locator.LookupSequence(streamID, sequence)
		if err == nil && found {
			reader, release, acquireErr := s.handles.Acquire(extent.Reference)
			if acquireErr != nil {
				return format.RecordFrame{}, acquireErr
			}
			defer release()
			return reader.Read(streamID, sequence)
		}
		// Locator files are reconstructible projections. A missing or corrupt
		// page must not make immutable Segment data unreadable.
	}
	e, found, err := s.findExtent(streamID, sequence)
	if err != nil {
		return format.RecordFrame{}, err
	}
	if found {
		reader, release, err := s.handles.Acquire(e.reference)
		if err != nil {
			return format.RecordFrame{}, err
		}
		defer release()
		return reader.Read(streamID, sequence)
	}
	return format.RecordFrame{}, fmt.Errorf("Sequence %d is not covered by a Segment", sequence)
}
func (s *Store) Inspect(streamID uint64) (StreamInfo, error) {
	tail, ok, err := s.tails.Lookup(streamID)
	if err != nil {
		return StreamInfo{}, err
	}
	if !ok {
		return StreamInfo{}, nil
	}
	info := StreamInfo{Exists: true, NextSequence: tail.NextSequence, RecordCount: tail.RecordCount, LastRecordedAt: tail.LastRecordedAt}
	if s.locator != nil && tail.NextSequence > 0 {
		extent, found, lookupErr := s.locator.LookupSequence(streamID, 0)
		if lookupErr == nil && found {
			info.FirstRecordedAt = extent.Entry.FirstRecordedAt
			return info, nil
		}
	}
	if first, found, findErr := s.firstExtent(streamID); findErr != nil {
		return StreamInfo{}, findErr
	} else if found {
		info.FirstRecordedAt = first.directory.FirstRecordedAt
	} else if tail.NextSequence > 0 {
		base, _, _ := s.table.ActiveRange(streamID)
		records, _, err := s.table.Read(streamID, base, 1)
		if err != nil {
			return StreamInfo{}, err
		}
		if len(records) > 0 {
			info.FirstRecordedAt = records[0].RecordedAt
		}
	}
	return info, nil
}
func (s *Store) ResolveTime(streamID uint64, target int64, mode TimeMode) (uint64, int64, bool, error) {
	tail, ok, err := s.tails.Lookup(streamID)
	if err != nil {
		return 0, 0, false, err
	}
	if !ok || tail.NextSequence == 0 {
		return 0, 0, false, nil
	}
	low, high := uint64(0), tail.NextSequence
	for low < high {
		mid := low + (high-low)/2
		record, err := s.record(streamID, mid)
		if err != nil {
			return 0, 0, false, err
		}
		if record.RecordedAt < target {
			low = mid + 1
		} else {
			high = mid
		}
	}
	if mode == AtOrAfter {
		if low == tail.NextSequence {
			return 0, 0, false, nil
		}
		record, err := s.record(streamID, low)
		return low, record.RecordedAt, true, err
	}
	if mode != AtOrBefore {
		return 0, 0, false, fmt.Errorf("unknown Time Mode")
	}
	low, high = 0, tail.NextSequence
	for low < high {
		mid := low + (high-low)/2
		record, err := s.record(streamID, mid)
		if err != nil {
			return 0, 0, false, err
		}
		if record.RecordedAt <= target {
			low = mid + 1
		} else {
			high = mid
		}
	}
	if low == 0 {
		return 0, 0, false, nil
	}
	record, err := s.record(streamID, low-1)
	return low - 1, record.RecordedAt, true, err
}
func (s *Store) record(streamID, sequence uint64) (format.RecordFrame, error) {
	base, _, ok := s.table.ActiveRange(streamID)
	if ok && sequence >= base {
		records, _, err := s.table.Read(streamID, sequence, 1)
		if err != nil {
			return format.RecordFrame{}, err
		}
		if len(records) == 1 {
			return records[0], nil
		}
	}
	return s.readSegment(streamID, sequence)
}
func (s *Store) ClearCache() {
	if s.locator != nil {
		s.locator.ClearCache()
	}
}

func (s *Store) findExtent(streamID, sequence uint64) (extent, bool, error) {
	for _, descriptor := range s.segments {
		directories, closeReader, err := s.directories(descriptor)
		if err != nil {
			return extent{}, false, err
		}
		i := sort.Search(len(directories), func(i int) bool { return directories[i].StreamID >= streamID })
		if i < len(directories) && directories[i].StreamID == streamID {
			directory := directories[i]
			if sequence >= directory.FirstSequence && sequence-directory.FirstSequence < directory.RecordCount {
				closeErr := closeReader()
				return extent{reference: descriptor.Reference, directory: directory}, true, closeErr
			}
		}
		if err = closeReader(); err != nil {
			return extent{}, false, err
		}
	}
	return extent{}, false, nil
}

func (s *Store) firstExtent(streamID uint64) (extent, bool, error) {
	var first extent
	found := false
	for _, descriptor := range s.segments {
		directories, closeReader, err := s.directories(descriptor)
		if err != nil {
			return extent{}, false, err
		}
		i := sort.Search(len(directories), func(i int) bool { return directories[i].StreamID >= streamID })
		if i < len(directories) && directories[i].StreamID == streamID && (!found || directories[i].FirstSequence < first.directory.FirstSequence) {
			first = extent{reference: descriptor.Reference, directory: directories[i]}
			found = true
		}
		if err = closeReader(); err != nil {
			return extent{}, false, err
		}
	}
	return first, found, nil
}

func (s *Store) directories(descriptor segment.Descriptor) ([]format.StreamDirectoryEntry, func() error, error) {
	if descriptor.Directories != nil {
		return descriptor.Directories, func() error { return nil }, nil
	}
	reader, err := segment.OpenReference(s.root, descriptor.Reference)
	if err != nil {
		return nil, nil, err
	}
	return reader.Directories, reader.Close, nil
}

func (s *Store) Generation() uint64 { return s.generation }

func (s *Store) Close() error { return s.handles.Close() }
