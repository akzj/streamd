package tail

import (
	"container/list"
	"fmt"
	"math"
	"slices"
	"sync"

	"github.com/akzj/streamd/internal/storage/format"
	"github.com/akzj/streamd/internal/storage/memtable"
	"github.com/akzj/streamd/internal/storage/segment"
)

type cachedTail struct {
	streamID uint64
	tail     memtable.Tail
}

// Resolver composes the active WAL overlay with an immutable Tail Catalog.
// Its cache is disposable and bounded; Segment facts are scanned only when a
// Catalog is unavailable or a requested Slot cannot be trusted.
type Resolver struct {
	table    *memtable.Table
	catalog  *Catalog
	root     string
	segments []segment.Descriptor
	capacity int

	mu    sync.Mutex
	cache map[uint64]*list.Element
	lru   *list.List
}

func NewResolver(table *memtable.Table, catalog *Catalog, root string, segments []segment.Descriptor, capacity int) *Resolver {
	if capacity <= 0 {
		capacity = 1024
	}
	return &Resolver{
		table: table, catalog: catalog, root: root,
		segments: append([]segment.Descriptor(nil), segments...), capacity: capacity,
		cache: make(map[uint64]*list.Element), lru: list.New(),
	}
}

func (r *Resolver) Lookup(streamID uint64) (memtable.Tail, bool, error) {
	if tail, ok := r.table.Tail(streamID); ok {
		return tail, true, nil
	}
	r.mu.Lock()
	if element := r.cache[streamID]; element != nil {
		r.lru.MoveToFront(element)
		tail := element.Value.(cachedTail).tail
		r.mu.Unlock()
		return tail, true, nil
	}
	r.mu.Unlock()

	var tail memtable.Tail
	var found bool
	var err error
	if r.catalog != nil {
		var slot format.TailSlot
		slot, found, err = r.catalog.Lookup(streamID)
		if err == nil && found {
			tail = memtable.Tail{
				NextSequence: slot.NextSequence, NextByteOffset: slot.NextByteOffset,
				LastRecordedAt: slot.LastRecordedAt, LastEntryID: slot.LastEntryID,
				RecordCount: slot.NextSequence,
			}
		}
	}
	if r.catalog == nil || err != nil || !found {
		tail, found, err = r.scanFacts(streamID)
	}
	if err != nil || !found {
		return memtable.Tail{}, found, err
	}
	r.remember(streamID, tail)
	return tail, true, nil
}

// EnsureActive seeds one historical Tail into the active MemTable. It is used
// immediately before WAL replay or Append so MemTable validation still checks
// exact Sequence and Byte Offset continuity without retaining every Stream.
func (r *Resolver) EnsureActive(streamID uint64) (memtable.Tail, bool, error) {
	if tail, ok := r.table.Tail(streamID); ok {
		return tail, true, nil
	}
	tail, found, err := r.Lookup(streamID)
	if err != nil || !found {
		return tail, found, err
	}
	if err = r.table.SeedTail(streamID, tail); err != nil {
		if current, ok := r.table.Tail(streamID); ok {
			return current, true, nil
		}
		return memtable.Tail{}, false, err
	}
	return tail, true, nil
}

func (r *Resolver) remember(streamID uint64, tail memtable.Tail) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if element := r.cache[streamID]; element != nil {
		element.Value = cachedTail{streamID: streamID, tail: tail}
		r.lru.MoveToFront(element)
		return
	}
	element := r.lru.PushFront(cachedTail{streamID: streamID, tail: tail})
	r.cache[streamID] = element
	if r.lru.Len() > r.capacity {
		old := r.lru.Back()
		delete(r.cache, old.Value.(cachedTail).streamID)
		r.lru.Remove(old)
	}
}

func (r *Resolver) scanFacts(streamID uint64) (memtable.Tail, bool, error) {
	ordered := append([]segment.Descriptor(nil), r.segments...)
	slices.SortFunc(ordered, func(a, b segment.Descriptor) int {
		if a.Reference.FirstEntryID < b.Reference.FirstEntryID {
			return -1
		}
		if a.Reference.FirstEntryID > b.Reference.FirstEntryID {
			return 1
		}
		return 0
	})
	var tail memtable.Tail
	found := false
	for _, descriptor := range ordered {
		directories := descriptor.Directories
		var reader *segment.Reader
		if directories == nil {
			var err error
			reader, err = segment.OpenReference(r.root, descriptor.Reference)
			if err != nil {
				return memtable.Tail{}, false, err
			}
			directories = reader.Directories
		}
		index, ok := slices.BinarySearchFunc(directories, streamID, func(directory format.StreamDirectoryEntry, id uint64) int {
			if directory.StreamID < id {
				return -1
			}
			if directory.StreamID > id {
				return 1
			}
			return 0
		})
		if reader != nil {
			if closeErr := reader.Close(); closeErr != nil {
				return memtable.Tail{}, false, closeErr
			}
		}
		if !ok {
			continue
		}
		directory := directories[index]
		if directory.RecordCount > math.MaxUint64-directory.FirstSequence {
			return memtable.Tail{}, false, fmt.Errorf("Stream %d extent Sequence overflows", streamID)
		}
		if found && (directory.FirstSequence != tail.NextSequence || directory.FirstByteOffset != tail.NextByteOffset || directory.FirstRecordedAt < tail.LastRecordedAt) {
			return memtable.Tail{}, false, fmt.Errorf("Stream %d Segment extents are not continuous", streamID)
		}
		found = true
		tail = memtable.Tail{
			NextSequence:   directory.FirstSequence + directory.RecordCount,
			NextByteOffset: directory.NextByteOffset, LastRecordedAt: directory.LastRecordedAt,
			LastEntryID: directory.LastEntryID, RecordCount: tail.RecordCount + directory.RecordCount,
		}
	}
	return tail, found, nil
}

func (r *Resolver) CacheLen() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.lru.Len()
}
