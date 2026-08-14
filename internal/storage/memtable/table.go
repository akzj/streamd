package memtable

import (
	"bytes"
	"fmt"
	"slices"
	"sync"

	"github.com/akzj/streamd/internal/storage/format"
)

const DefaultChunkSize = 4 << 20

type Tail struct {
	NextSequence   uint64
	NextByteOffset uint64
	LastRecordedAt int64
	LastEntryID    uint64
	RecordCount    uint64
}
type recordRef struct {
	chunk      int
	start      int
	length     int
	entryID    uint64
	recordedAt int64
}
type streamData struct {
	tail         Tail
	baseSequence uint64
	records      []recordRef
}
type StreamSnapshot struct {
	StreamID uint64
	Tail     Tail
	Frames   [][]byte
}
type Table struct {
	mu        sync.RWMutex
	chunkSize int
	chunks    [][]byte
	streams   map[uint64]*streamData
	frozen    bool
	bytes     uint64
	records   uint64
}

func New(chunkSize int) *Table {
	if chunkSize <= 0 {
		chunkSize = DefaultChunkSize
	}
	return &Table{chunkSize: chunkSize, streams: make(map[uint64]*streamData)}
}
func (t *Table) ValidateBatch(entries []format.WALEntry) error {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.validateBatch(entries)
}
func (t *Table) ApplyBatch(entries []format.WALEntry) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.frozen {
		return fmt.Errorf("MemTable is frozen")
	}
	if err := t.validateBatch(entries); err != nil {
		return err
	}
	stream := t.streams[entries[0].StreamID]
	if stream == nil {
		stream = &streamData{}
		t.streams[entries[0].StreamID] = stream
	}
	previousRecordCount := stream.tail.RecordCount
	refs := make([]recordRef, 0, len(entries))
	for _, entry := range entries {
		chunk, start := t.appendFrame(entry.Frame)
		refs = append(refs, recordRef{chunk: chunk, start: start, length: len(entry.Frame), entryID: entry.EntryID, recordedAt: entry.RecordedAt})
	}
	stream.records = append(stream.records, refs...)
	last := entries[len(entries)-1]
	stream.tail = Tail{NextSequence: last.Sequence + 1, NextByteOffset: last.ByteOffset + uint64(len(last.Frame)), LastRecordedAt: last.RecordedAt, LastEntryID: last.EntryID, RecordCount: previousRecordCount + uint64(len(entries))}
	t.records += uint64(len(entries))
	return nil
}
func (t *Table) validateBatch(entries []format.WALEntry) error {
	if len(entries) == 0 {
		return fmt.Errorf("empty Batch")
	}
	first := entries[0]
	if first.BatchIndex != 0 || int(first.BatchCount) != len(entries) {
		return fmt.Errorf("Batch count or first index is invalid")
	}
	stream := t.streams[first.StreamID]
	var tail Tail
	if stream != nil {
		tail = stream.tail
	}
	if first.Sequence != tail.NextSequence || first.ByteOffset != tail.NextByteOffset {
		return fmt.Errorf("Batch does not continue Stream tail")
	}
	if tail.RecordCount > 0 && first.RecordedAt < tail.LastRecordedAt {
		return fmt.Errorf("recorded_at decreases")
	}
	for i, entry := range entries {
		if entry.StreamID != first.StreamID || entry.BatchCount != first.BatchCount || entry.BatchIndex != uint32(i) {
			return fmt.Errorf("Batch metadata differs at %d", i)
		}
		if !bytes.Equal(entry.Record.RequestID, first.Record.RequestID) || entry.Record.RequestHash != first.Record.RequestHash {
			return fmt.Errorf("Batch request identity differs at %d", i)
		}
		if i > 0 {
			prev := entries[i-1]
			if entry.EntryID != prev.EntryID+1 || entry.Sequence != prev.Sequence+1 || entry.ByteOffset != prev.ByteOffset+uint64(len(prev.Frame)) || entry.RecordedAt < prev.RecordedAt {
				return fmt.Errorf("Batch is not continuous at %d", i)
			}
		}
	}
	return nil
}
func (t *Table) appendFrame(frame []byte) (int, int) {
	if len(frame) > t.chunkSize || len(t.chunks) == 0 || len(t.chunks[len(t.chunks)-1])+len(frame) > cap(t.chunks[len(t.chunks)-1]) {
		capacity := t.chunkSize
		if len(frame) > capacity {
			capacity = len(frame)
		}
		t.chunks = append(t.chunks, make([]byte, 0, capacity))
	}
	i := len(t.chunks) - 1
	start := len(t.chunks[i])
	t.chunks[i] = append(t.chunks[i], frame...)
	t.bytes += uint64(len(frame))
	return i, start
}
func (t *Table) Tail(streamID uint64) (Tail, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	s, ok := t.streams[streamID]
	if !ok {
		return Tail{}, false
	}
	return s.tail, true
}
func (t *Table) SeedTail(streamID uint64, tail Tail) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.frozen {
		return fmt.Errorf("MemTable is frozen")
	}
	if _, exists := t.streams[streamID]; exists {
		return fmt.Errorf("Stream %d already exists", streamID)
	}
	t.streams[streamID] = &streamData{tail: tail, baseSequence: tail.NextSequence}
	return nil
}
func (t *Table) Read(streamID, from uint64, maxRecords int) ([]format.RecordFrame, uint64, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	s := t.streams[streamID]
	if s == nil {
		if from == 0 {
			return nil, 0, nil
		}
		return nil, 0, fmt.Errorf("Sequence %d is ahead of empty Stream", from)
	}
	if from > s.tail.NextSequence {
		return nil, s.tail.NextSequence, fmt.Errorf("Sequence %d is ahead of tail %d", from, s.tail.NextSequence)
	}
	if from == s.tail.NextSequence {
		return nil, from, nil
	}
	if from < s.baseSequence {
		return nil, from, fmt.Errorf("Sequence %d precedes active MemTable base %d", from, s.baseSequence)
	}
	if maxRecords <= 0 {
		return nil, from, nil
	}
	start := from - s.baseSequence
	end := min(uint64(len(s.records)), start+uint64(maxRecords))
	out := make([]format.RecordFrame, 0, end-start)
	for _, ref := range s.records[start:end] {
		frame := t.chunks[ref.chunk][ref.start : ref.start+ref.length]
		record, err := format.UnmarshalRecordFrame(frame)
		if err != nil {
			return nil, from, err
		}
		out = append(out, record)
	}
	return out, s.baseSequence + end, nil
}
func (t *Table) Freeze() { t.mu.Lock(); t.frozen = true; t.mu.Unlock() }
func (t *Table) FreezeSnapshot() []StreamSnapshot {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.frozen = true
	ids := make([]uint64, 0, len(t.streams))
	for id := range t.streams {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	out := make([]StreamSnapshot, 0, len(ids))
	for _, id := range ids {
		s := t.streams[id]
		snap := StreamSnapshot{StreamID: id, Tail: s.tail, Frames: make([][]byte, 0, len(s.records))}
		for _, ref := range s.records {
			snap.Frames = append(snap.Frames, bytes.Clone(t.chunks[ref.chunk][ref.start:ref.start+ref.length]))
		}
		out = append(out, snap)
	}
	return out
}
func (t *Table) Frozen() bool { t.mu.RLock(); defer t.mu.RUnlock(); return t.frozen }
func (t *Table) Stats() (records, bytes uint64) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.records, t.bytes
}
