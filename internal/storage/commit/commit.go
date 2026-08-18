package commit

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
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
	MaxDelay          time.Duration
	MaxRequests       int
	MaxBytes          uint64
	QueueCapacity     int
	Replica           Replica
	ReplicaTimeout    time.Duration
	Guard             Guard
	InitialWatermarks Watermarks
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

// Stats is a cumulative, lock-free snapshot of the single WAL writer. Queue
// wait is summed per request; stage durations are summed once per commit group.
type Stats struct {
	Groups           uint64
	Requests         uint64
	Entries          uint64
	Bytes            uint64
	LocalSyncs       uint64
	ReplicatedGroups uint64
	MaxGroupRequests uint64
	MaxGroupBytes    uint64
	QueueWaitNanos   uint64
	CollectNanos     uint64
	AppendNanos      uint64
	LocalSyncNanos   uint64
	ReplicateNanos   uint64
	ApplyNanos       uint64
	ProcessNanos     uint64
	QueueDepth       uint64
	QueueCapacity    uint64
}

type counters struct {
	groups           atomic.Uint64
	requests         atomic.Uint64
	entries          atomic.Uint64
	bytes            atomic.Uint64
	localSyncs       atomic.Uint64
	replicatedGroups atomic.Uint64
	maxGroupRequests atomic.Uint64
	maxGroupBytes    atomic.Uint64
	queueWaitNanos   atomic.Uint64
	collectNanos     atomic.Uint64
	appendNanos      atomic.Uint64
	localSyncNanos   atomic.Uint64
	replicateNanos   atomic.Uint64
	applyNanos       atomic.Uint64
	processNanos     atomic.Uint64
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
	enqueuedAt time.Time
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
	stats      counters
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
	committer := &Committer{log: log, table: table, options: options, queue: make(chan *pending, options.QueueCapacity), done: make(chan struct{}), replica: options.Replica, guard: options.Guard, watermarks: options.InitialWatermarks}
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
	pending.enqueuedAt = time.Now()
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

func (c *Committer) Stats() Stats {
	return Stats{
		Groups: c.stats.groups.Load(), Requests: c.stats.requests.Load(), Entries: c.stats.entries.Load(), Bytes: c.stats.bytes.Load(),
		LocalSyncs: c.stats.localSyncs.Load(), ReplicatedGroups: c.stats.replicatedGroups.Load(),
		MaxGroupRequests: c.stats.maxGroupRequests.Load(), MaxGroupBytes: c.stats.maxGroupBytes.Load(),
		QueueWaitNanos: c.stats.queueWaitNanos.Load(), CollectNanos: c.stats.collectNanos.Load(),
		AppendNanos: c.stats.appendNanos.Load(), LocalSyncNanos: c.stats.localSyncNanos.Load(), ReplicateNanos: c.stats.replicateNanos.Load(),
		ApplyNanos: c.stats.applyNanos.Load(), ProcessNanos: c.stats.processNanos.Load(),
		QueueDepth: uint64(len(c.queue)), QueueCapacity: uint64(cap(c.queue)),
	}
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
		collectStarted := time.Now()
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
		c.process(group, time.Since(collectStarted))
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

func (c *Committer) process(group []*pending, collectDuration time.Duration) {
	processStarted := time.Now()
	defer func() { c.stats.processNanos.Add(durationNanos(time.Since(processStarted))) }()
	c.stats.groups.Add(1)
	c.stats.requests.Add(uint64(len(group)))
	c.stats.collectNanos.Add(durationNanos(collectDuration))
	updateMax(&c.stats.maxGroupRequests, uint64(len(group)))
	for _, request := range group {
		c.stats.entries.Add(uint64(len(request.entries)))
		c.stats.bytes.Add(request.bytes)
		if !request.enqueuedAt.IsZero() {
			c.stats.queueWaitNanos.Add(durationNanos(processStarted.Sub(request.enqueuedAt)))
		}
	}
	var groupBytes uint64
	for _, request := range group {
		groupBytes += request.bytes
	}
	updateMax(&c.stats.maxGroupBytes, groupBytes)
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
	appendStarted := time.Now()
	err := c.log.Append(flattened...)
	c.stats.appendNanos.Add(durationNanos(time.Since(appendStarted)))
	if err != nil {
		c.setFatal(err)
		completeGroup(group, true, err)
		return
	}
	last := group[len(group)-1].entries[len(group[len(group)-1].entries)-1].EntryID
	c.stateMu.Lock()
	c.watermarks.HasValue = true
	c.watermarks.Appended = last
	c.stateMu.Unlock()
	syncStarted := time.Now()
	err = c.log.Sync()
	c.stats.localSyncNanos.Add(durationNanos(time.Since(syncStarted)))
	c.stats.localSyncs.Add(1)
	if err != nil {
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
		replicateStarted := time.Now()
		replicated, err := c.replica.Replicate(ctx, flattened)
		c.stats.replicateNanos.Add(durationNanos(time.Since(replicateStarted)))
		c.stats.replicatedGroups.Add(1)
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
	applyStarted := time.Now()
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
	c.stats.applyNanos.Add(durationNanos(time.Since(applyStarted)))
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

func durationNanos(duration time.Duration) uint64 {
	if duration <= 0 {
		return 0
	}
	return uint64(duration)
}

func updateMax(target *atomic.Uint64, value uint64) {
	for current := target.Load(); value > current; current = target.Load() {
		if target.CompareAndSwap(current, value) {
			return
		}
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
