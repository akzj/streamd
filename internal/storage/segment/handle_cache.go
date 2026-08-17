package segment

import (
	"container/list"
	"errors"
	"fmt"
	"sync"

	"github.com/akzj/streamd/internal/storage/format"
)

var ErrHandleCacheClosed = errors.New("Segment Handle Cache is closed")

type handleEntry struct {
	reference format.SegmentReference
	reader    *Reader
	refs      int
	element   *list.Element
}

// HandleCache bounds idle open Segment files. In-use handles may temporarily
// exceed the configured capacity and are closed as soon as their final
// reference is released.
type HandleCache struct {
	mu       sync.Mutex
	root     string
	capacity int
	entries  map[format.UUID]*handleEntry
	lru      *list.List
	closed   bool
}

func NewHandleCache(root string, capacity int) *HandleCache {
	if capacity <= 0 {
		capacity = 64
	}
	return &HandleCache{
		root:     root,
		capacity: capacity,
		entries:  make(map[format.UUID]*handleEntry),
		lru:      list.New(),
	}
}

// Acquire returns a validated Reader and an idempotent release function.
func (c *HandleCache) Acquire(reference format.SegmentReference) (*Reader, func(), error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, nil, ErrHandleCacheClosed
	}
	entry := c.entries[reference.SegmentID]
	if entry == nil {
		reader, err := OpenReference(c.root, reference)
		if err != nil {
			return nil, nil, err
		}
		entry = &handleEntry{reference: reference, reader: reader}
		entry.element = c.lru.PushFront(entry)
		c.entries[reference.SegmentID] = entry
	} else {
		if entry.reference.LocalPath != reference.LocalPath || entry.reference.ContentSHA256 != reference.ContentSHA256 {
			return nil, nil, fmt.Errorf("Segment %x reference changed while cached", reference.SegmentID)
		}
		c.lru.MoveToFront(entry.element)
	}
	entry.refs++
	c.evictLocked()
	var once sync.Once
	return entry.reader, func() {
		once.Do(func() { c.release(entry) })
	}, nil
}

func (c *HandleCache) release(entry *handleEntry) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if entry.refs > 0 {
		entry.refs--
	}
	if c.closed && entry.refs == 0 {
		c.removeLocked(entry)
		return
	}
	c.evictLocked()
}

func (c *HandleCache) evictLocked() {
	for len(c.entries) > c.capacity {
		var candidate *handleEntry
		for element := c.lru.Back(); element != nil; element = element.Prev() {
			entry := element.Value.(*handleEntry)
			if entry.refs == 0 {
				candidate = entry
				break
			}
		}
		if candidate == nil {
			return
		}
		c.removeLocked(candidate)
	}
}

func (c *HandleCache) removeLocked(entry *handleEntry) {
	delete(c.entries, entry.reference.SegmentID)
	c.lru.Remove(entry.element)
	_ = entry.reader.Close()
}

func (c *HandleCache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.entries)
}

// Close prevents new acquisitions. Pinned readers are closed by their final
// release; callers that synchronize Close with reads get a fully synchronous
// shutdown.
func (c *HandleCache) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	var closeErr error
	for element := c.lru.Back(); element != nil; {
		previous := element.Prev()
		entry := element.Value.(*handleEntry)
		if entry.refs == 0 {
			delete(c.entries, entry.reference.SegmentID)
			c.lru.Remove(element)
			closeErr = errors.Join(closeErr, entry.reader.Close())
		}
		element = previous
	}
	return closeErr
}
