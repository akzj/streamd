package commit

import (
	"context"
	"fmt"
	"sync"

	"github.com/akzj/streamd/internal/storage/format"
	"github.com/akzj/streamd/internal/storage/memtable"
)

type DurableLog interface {
	Append(...[]byte) error
	Sync() error
}
type Watermarks struct {
	HasValue     bool
	Appended     uint64
	LocalDurable uint64
	Committed    uint64
	Applied      uint64
}
type Result struct {
	FirstEntryID    uint64
	LastEntryID     uint64
	RecordCount     uint32
	ResultUncertain bool
}
type Committer struct {
	mu         sync.Mutex
	log        DurableLog
	table      *memtable.Table
	watermarks Watermarks
	fatal      error
}

func New(log DurableLog, table *memtable.Table) *Committer { return &Committer{log: log, table: table} }
func (c *Committer) Commit(ctx context.Context, encoded [][]byte) (Result, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.fatal != nil {
		return Result{}, fmt.Errorf("commit core failed: %w", c.fatal)
	}
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	entries := make([]format.WALEntry, len(encoded))
	for i, b := range encoded {
		entry, err := format.UnmarshalWALEntry(b)
		if err != nil {
			return Result{}, fmt.Errorf("Entry %d: %w", i, err)
		}
		entries[i] = entry
	}
	if err := c.table.ValidateBatch(entries); err != nil {
		return Result{}, err
	}
	if err := c.log.Append(encoded...); err != nil {
		return Result{ResultUncertain: true}, err
	}
	last := entries[len(entries)-1].EntryID
	c.watermarks.HasValue = true
	c.watermarks.Appended = last
	if err := c.log.Sync(); err != nil {
		c.fatal = err
		return Result{FirstEntryID: entries[0].EntryID, LastEntryID: last, RecordCount: uint32(len(entries)), ResultUncertain: true}, err
	}
	c.watermarks.LocalDurable = last
	c.watermarks.Committed = last
	if err := c.table.ApplyBatch(entries); err != nil {
		c.fatal = err
		return Result{FirstEntryID: entries[0].EntryID, LastEntryID: last, RecordCount: uint32(len(entries)), ResultUncertain: true}, fmt.Errorf("durable Batch could not Apply: %w", err)
	}
	c.watermarks.Applied = last
	result := Result{FirstEntryID: entries[0].EntryID, LastEntryID: last, RecordCount: uint32(len(entries))}
	if err := ctx.Err(); err != nil {
		result.ResultUncertain = true
		return result, err
	}
	return result, nil
}
func (c *Committer) Watermarks() Watermarks { c.mu.Lock(); defer c.mu.Unlock(); return c.watermarks }
func (c *Committer) FatalError() error      { c.mu.Lock(); defer c.mu.Unlock(); return c.fatal }
