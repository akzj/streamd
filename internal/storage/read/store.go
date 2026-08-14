package read

import (
	"container/list"
	"fmt"
	"sync"

	"github.com/akzj/streamd/internal/storage/errdefs"
	"github.com/akzj/streamd/internal/storage/format"
	"github.com/akzj/streamd/internal/storage/memtable"
	"github.com/akzj/streamd/internal/storage/segment"
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
	reader    *segment.Reader
	directory format.StreamDirectoryEntry
}
type cacheEntry struct {
	streamID uint64
	extents  []extent
}
type Store struct {
	table    *memtable.Table
	segments []*segment.Reader
	capacity int
	mu       sync.Mutex
	cache    map[uint64]*list.Element
	lru      *list.List
}

func New(table *memtable.Table, segments []*segment.Reader, streamCacheCapacity int) *Store {
	if streamCacheCapacity <= 0 {
		streamCacheCapacity = 1024
	}
	return &Store{table: table, segments: segments, capacity: streamCacheCapacity, cache: make(map[uint64]*list.Element), lru: list.New()}
}
func (s *Store) extents(streamID uint64) []extent {
	s.mu.Lock()
	defer s.mu.Unlock()
	if element := s.cache[streamID]; element != nil {
		s.lru.MoveToFront(element)
		return element.Value.(cacheEntry).extents
	}
	var found []extent
	for _, reader := range s.segments {
		for _, d := range reader.Directories {
			if d.StreamID == streamID {
				found = append(found, extent{reader, d})
				break
			}
		}
	}
	for i := 1; i < len(found); i++ {
		for j := i; j > 0 && found[j].directory.FirstSequence < found[j-1].directory.FirstSequence; j-- {
			found[j], found[j-1] = found[j-1], found[j]
		}
	}
	element := s.lru.PushFront(cacheEntry{streamID, found})
	s.cache[streamID] = element
	if s.lru.Len() > s.capacity {
		old := s.lru.Back()
		delete(s.cache, old.Value.(cacheEntry).streamID)
		s.lru.Remove(old)
	}
	return found
}
func (s *Store) Read(streamID, from uint64, maxRecords int, maxBytes uint64) (Result, error) {
	tail, ok := s.table.Tail(streamID)
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
	base, _, _ := s.table.ActiveRange(streamID)
	for sequence := from; sequence < tail.NextSequence && len(result.Records) < maxRecords; sequence++ {
		var record format.RecordFrame
		var err error
		if sequence >= base {
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
	for _, e := range s.extents(streamID) {
		d := e.directory
		if sequence >= d.FirstSequence && sequence < d.FirstSequence+d.RecordCount {
			return e.reader.Read(streamID, sequence)
		}
	}
	return format.RecordFrame{}, fmt.Errorf("Sequence %d is not covered by a Segment", sequence)
}
func (s *Store) Inspect(streamID uint64) (StreamInfo, error) {
	tail, ok := s.table.Tail(streamID)
	if !ok {
		return StreamInfo{}, nil
	}
	info := StreamInfo{Exists: true, NextSequence: tail.NextSequence, RecordCount: tail.RecordCount, LastRecordedAt: tail.LastRecordedAt}
	extents := s.extents(streamID)
	if len(extents) > 0 {
		info.FirstRecordedAt = extents[0].directory.FirstRecordedAt
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
	tail, ok := s.table.Tail(streamID)
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
	s.mu.Lock()
	s.cache = make(map[uint64]*list.Element)
	s.lru.Init()
	s.mu.Unlock()
}
