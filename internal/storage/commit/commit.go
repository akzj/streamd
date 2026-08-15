package commit

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/akzj/streamd/internal/storage/format"
	"github.com/akzj/streamd/internal/storage/memtable"
)

type DurableLog interface {
	Append(...[]byte) error
	Sync() error
}

// Replica durably copies an already encoded local WAL group. Replicate must
// return the exact last Entry ID acknowledged by the replica. AdvanceCommit is
// cumulative notification; a failure does not revoke an already established
// two-copy commit and is retried by subsequent groups.
type Replica interface {
	Replicate(context.Context, [][]byte) (uint64, error)
	AdvanceCommit(context.Context, uint64) error
}

type Guard interface {
	CanCommit() error
}

type Options struct {
	MaxDelay       time.Duration
	MaxRequests    int
	MaxBytes       uint64
	QueueCapacity  int
	Replica        Replica
	ReplicaTimeout time.Duration
	Guard          Guard
}

type Watermarks struct {
	HasValue        bool
	HasLocalDurable bool
	HasCommitted    bool
	HasReplicated   bool
	HasApplied      bool
	Appended        uint64
	LocalDurable    uint64
	Committed       uint64
	Replicated      uint64
	Applied         uint64
}

type Result struct {
	FirstEntryID    uint64
	LastEntryID     uint64
	RecordCount     uint32
	ResultUncertain bool
}

type completion struct {
	result Result
	err    error
}

type pendingKind uint8

const (
	pendingCommit pendingKind = iota
	pendingBarrier
	pendingShutdown
)

type pending struct {
	kind       pendingKind
	encoded    [][]byte
	entries    []format.WALEntry
	streamID   uint64
	bytes      uint64
	completion chan completion
}

type Future struct {
	completion <-chan completion
}

func (f *Future) Wait(ctx context.Context) (Result, error) {
	select {
	case completed := <-f.completion:
		return completed.result, completed.err
	default:
	}
	select {
	case completed := <-f.completion:
		return completed.result, completed.err
	case <-ctx.Done():
		return Result{ResultUncertain: true}, ctx.Err()
	}
}

type Committer struct {
	log        DurableLog
	table      *memtable.Table
	options    Options
	queue      chan *pending
	done       chan struct{}
	submitMu   sync.Mutex
	stateMu    sync.Mutex
	closed     bool
	fatal      error
	watermarks Watermarks
	replica    Replica
	guard      Guard
}

func New(log DurableLog, table *memtable.Table) *Committer {
	return NewWithOptions(log, table, Options{})
}

func NewWithOptions(log DurableLog, table *memtable.Table, options Options) *Committer {
	if options.MaxDelay <= 0 {
		options.MaxDelay = 250 * time.Microsecond
	}
	if options.MaxRequests <= 0 {
		options.MaxRequests = 64
	}
	if options.MaxBytes == 0 {
		options.MaxBytes = 4 << 20
	}
	if options.QueueCapacity <= 0 {
		options.QueueCapacity = 1024
	}
	if options.ReplicaTimeout <= 0 {
		options.ReplicaTimeout = 30 * time.Second
	}
	committer := &Committer{log: log, table: table, options: options, queue: make(chan *pending, options.QueueCapacity), done: make(chan struct{}), replica: options.Replica, guard: options.Guard}
	go committer.run()
	return committer
}

// Enqueue takes ownership of encoded and its byte slices.
func (c *Committer) Enqueue(encoded [][]byte) (*Future, error) {
	pending, err := newPending(encoded)
	if err != nil {
		return nil, err
	}
	c.submitMu.Lock()
	defer c.submitMu.Unlock()
	c.stateMu.Lock()
	closed, fatal := c.closed, c.fatal
	c.stateMu.Unlock()
	if fatal != nil {
		return nil, fmt.Errorf("commit core failed: %w", fatal)
	}
	if closed {
		return nil, fmt.Errorf("commit core is closed")
	}
	c.queue <- pending
	return &Future{completion: pending.completion}, nil
}

func (c *Committer) Commit(ctx context.Context, encoded [][]byte) (Result, error) {
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	future, err := c.Enqueue(encoded)
	if err != nil {
		return Result{}, err
	}
	return future.Wait(ctx)
}

func (c *Committer) Barrier(ctx context.Context) error {
	c.submitMu.Lock()
	c.stateMu.Lock()
	closed, fatal := c.closed, c.fatal
	c.stateMu.Unlock()
	if fatal != nil {
		c.submitMu.Unlock()
		return fmt.Errorf("commit core failed: %w", fatal)
	}
	if closed {
		c.submitMu.Unlock()
		return fmt.Errorf("commit core is closed")
	}
	pending := &pending{kind: pendingBarrier, completion: make(chan completion, 1)}
	c.queue <- pending
	c.submitMu.Unlock()
	_, err := (&Future{completion: pending.completion}).Wait(ctx)
	return err
}

func (c *Committer) Close() error {
	c.submitMu.Lock()
	c.stateMu.Lock()
	if c.closed {
		c.stateMu.Unlock()
		c.submitMu.Unlock()
		<-c.done
		return c.FatalError()
	}
	c.closed = true
	c.stateMu.Unlock()
	pending := &pending{kind: pendingShutdown, completion: make(chan completion, 1)}
	c.queue <- pending
	c.submitMu.Unlock()
	<-c.done
	return c.FatalError()
}

func (c *Committer) Watermarks() Watermarks {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	return c.watermarks
}

func (c *Committer) FatalError() error {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	return c.fatal
}

func (c *Committer) run() {
	defer close(c.done)
	var carry *pending
	for {
		first := carry
		carry = nil
		if first == nil {
			first = <-c.queue
		}
		if first.kind != pendingCommit {
			if c.handleControl(first) {
				return
			}
			continue
		}
		group := []*pending{first}
		streams := map[uint64]struct{}{first.streamID: {}}
		groupBytes := first.bytes
		timer := time.NewTimer(c.options.MaxDelay)
	collect:
		for len(group) < c.options.MaxRequests {
			select {
			case candidate := <-c.queue:
				if candidate.kind != pendingCommit {
					carry = candidate
					break collect
				}
				if _, duplicate := streams[candidate.streamID]; duplicate || groupBytes >= c.options.MaxBytes || candidate.bytes > c.options.MaxBytes-groupBytes {
					carry = candidate
					break collect
				}
				group = append(group, candidate)
				streams[candidate.streamID] = struct{}{}
				groupBytes += candidate.bytes
			case <-timer.C:
				break collect
			}
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		c.process(group)
	}
}

func (c *Committer) handleControl(pending *pending) bool {
	c.stateMu.Lock()
	fatal := c.fatal
	c.stateMu.Unlock()
	if fatal != nil {
		pending.completion <- completion{err: fmt.Errorf("commit core failed: %w", fatal)}
	} else {
		pending.completion <- completion{}
	}
	return pending.kind == pendingShutdown
}

func (c *Committer) process(group []*pending) {
	c.stateMu.Lock()
	fatal := c.fatal
	c.stateMu.Unlock()
	if fatal != nil {
		completeGroup(group, false, fmt.Errorf("commit core failed: %w", fatal))
		return
	}
	if c.guard != nil {
		if err := c.guard.CanCommit(); err != nil {
			completeGroup(group, false, fmt.Errorf("commit guard rejected WAL allocation: %w", err))
			return
		}
	}
	var flattened [][]byte
	for _, request := range group {
		if err := c.table.ValidateBatch(request.entries); err != nil {
			c.setFatal(err)
			completeGroup(group, false, err)
			return
		}
		flattened = append(flattened, request.encoded...)
	}
	if err := c.log.Append(flattened...); err != nil {
		c.setFatal(err)
		completeGroup(group, true, err)
		return
	}
	last := group[len(group)-1].entries[len(group[len(group)-1].entries)-1].EntryID
	c.stateMu.Lock()
	c.watermarks.HasValue = true
	c.watermarks.Appended = last
	c.stateMu.Unlock()
	if err := c.log.Sync(); err != nil {
		c.setFatal(err)
		completeGroup(group, true, err)
		return
	}
	c.stateMu.Lock()
	c.watermarks.HasLocalDurable = true
	c.watermarks.LocalDurable = last
	c.stateMu.Unlock()
	if c.guard != nil {
		if err := c.guard.CanCommit(); err != nil {
			wrapped := fmt.Errorf("commit guard expired after local durability: %w", err)
			c.setFatal(wrapped)
			completeGroup(group, true, wrapped)
			return
		}
	}
	if c.replica != nil {
		ctx, cancel := context.WithTimeout(context.Background(), c.options.ReplicaTimeout)
		replicated, err := c.replica.Replicate(ctx, flattened)
		cancel()
		if err != nil {
			wrapped := fmt.Errorf("replicate durable WAL group: %w", err)
			c.setFatal(wrapped)
			completeGroup(group, true, wrapped)
			return
		}
		if replicated != last {
			wrapped := fmt.Errorf("replica acknowledged Entry %d, want %d", replicated, last)
			c.setFatal(wrapped)
			completeGroup(group, true, wrapped)
			return
		}
		c.stateMu.Lock()
		c.watermarks.HasReplicated = true
		c.watermarks.Replicated = last
		c.stateMu.Unlock()
	}
	if c.guard != nil {
		if err := c.guard.CanCommit(); err != nil {
			wrapped := fmt.Errorf("commit guard expired before commit advance: %w", err)
			c.setFatal(wrapped)
			completeGroup(group, true, wrapped)
			return
		}
	}
	c.stateMu.Lock()
	c.watermarks.HasCommitted = true
	c.watermarks.Committed = last
	c.stateMu.Unlock()
	for _, request := range group {
		if err := c.table.ApplyBatch(request.entries); err != nil {
			wrapped := fmt.Errorf("durable Batch could not Apply: %w", err)
			c.setFatal(wrapped)
			completeGroup(group, true, wrapped)
			return
		}
		applied := request.entries[len(request.entries)-1].EntryID
		c.stateMu.Lock()
		c.watermarks.HasApplied = true
		c.watermarks.Applied = applied
		c.stateMu.Unlock()
	}
	if c.replica != nil {
		ctx, cancel := context.WithTimeout(context.Background(), c.options.ReplicaTimeout)
		_ = c.replica.AdvanceCommit(ctx, last)
		cancel()
	}
	for _, request := range group {
		first := request.entries[0].EntryID
		last := request.entries[len(request.entries)-1].EntryID
		request.completion <- completion{result: Result{FirstEntryID: first, LastEntryID: last, RecordCount: uint32(len(request.entries))}}
	}
}

func (c *Committer) setFatal(err error) {
	c.stateMu.Lock()
	if c.fatal == nil {
		c.fatal = err
	}
	c.stateMu.Unlock()
}

func newPending(encoded [][]byte) (*pending, error) {
	if len(encoded) == 0 {
		return nil, fmt.Errorf("empty commit Batch")
	}
	request := &pending{kind: pendingCommit, encoded: make([][]byte, len(encoded)), entries: make([]format.WALEntry, len(encoded)), completion: make(chan completion, 1)}
	for i, data := range encoded {
		entry, err := format.UnmarshalWALEntry(data)
		if err != nil {
			return nil, fmt.Errorf("Entry %d: %w", i, err)
		}
		request.encoded[i] = data
		request.entries[i] = entry
		request.bytes += uint64(len(data))
	}
	request.streamID = request.entries[0].StreamID
	return request, nil
}

func completeGroup(group []*pending, uncertain bool, err error) {
	for _, request := range group {
		first := request.entries[0].EntryID
		last := request.entries[len(request.entries)-1].EntryID
		request.completion <- completion{result: Result{FirstEntryID: first, LastEntryID: last, RecordCount: uint32(len(request.entries)), ResultUncertain: uncertain}, err: err}
	}
}
