package engine

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/akzj/streamd/internal/storage/commit"
	"github.com/akzj/streamd/internal/storage/errdefs"
	"github.com/akzj/streamd/internal/storage/format"
	"github.com/akzj/streamd/internal/storage/fsutil"
	"github.com/akzj/streamd/internal/storage/identity"
	"github.com/akzj/streamd/internal/storage/lifecycle"
	locatorstore "github.com/akzj/streamd/internal/storage/locator"
	"github.com/akzj/streamd/internal/storage/memtable"
	"github.com/akzj/streamd/internal/storage/projection"
	readstore "github.com/akzj/streamd/internal/storage/read"
	"github.com/akzj/streamd/internal/storage/recovery"
	"github.com/akzj/streamd/internal/storage/registry"
	"github.com/akzj/streamd/internal/storage/replicationstate"
	"github.com/akzj/streamd/internal/storage/segment"
	tailstore "github.com/akzj/streamd/internal/storage/tail"
	"github.com/akzj/streamd/internal/storage/wal"
)

type InputRecord struct {
	Headers []format.Header
	Payload []byte
}
type AppendRequest struct {
	Namespace        string
	Stream           string
	ExpectedSequence uint64
	RequestID        []byte
	Producer         string
	Records          []InputRecord
}
type AppendResult struct {
	FirstSequence   uint64
	NextSequence    uint64
	RecordCount     uint32
	FirstRecordedAt int64
	LastRecordedAt  int64
	FirstEntryID    uint64
	LastEntryID     uint64
	Deduplicated    bool
}
type Health struct {
	Watermarks       commit.Watermarks
	Fatal            error
	WriteUnavailable error
	Role             format.ReplicationRole
	Durability       format.ReplicationDurability
	Term             uint64
	CapacityCritical bool
}

type MaintenanceStats struct {
	MemTableRecords     uint64
	MemTableBytes       uint64
	ActiveWALBytes      uint64
	AppendGates         uint64
	NotificationStreams uint64
}

type WALCollectionEvidence struct {
	SnapshotEntryID  uint64
	SnapshotVerified bool
	MaxRetainedBytes uint64
}

type CompactionOptions struct {
	MinSegments      int
	MaxInputSegments int
	MaxInputBytes    uint64
}

type CompactionResult struct {
	Manifest      format.Manifest
	Created       bool
	InputSegments int
	InputBytes    uint64
}

type ReplicationOptions struct {
	Term           uint64
	Role           format.ReplicationRole
	Durability     format.ReplicationDurability
	Replica        commit.Replica
	Guard          commit.Guard
	ReplicaTimeout time.Duration
	GroupCommit    GroupCommitOptions
	// ExpectedStateID closes the check-to-lock race for offline callers that
	// made a decision from one exact immutable Replication State.
	ExpectedStateID format.UUID
	// RejectUncommittedSuffix is for offline recovery boundaries that must
	// never silently discard durable entries beyond committed.
	RejectUncommittedSuffix bool
}

type GroupCommitOptions struct {
	MaxDelay      time.Duration
	MaxRequests   int
	MaxBytes      uint64
	QueueCapacity int
}
type streamKey struct {
	namespace string
	stream    string
}
type appendGate struct {
	token chan struct{}
	refs  int
}
type notification struct {
	ch      chan struct{}
	waiters int
}

const (
	defaultStreamCacheCapacity   = 1024
	defaultSegmentHandleCapacity = 64
)

type Store struct {
	maintenanceMu    sync.Mutex
	mu               sync.Mutex
	gateMu           sync.Mutex
	viewMu           sync.RWMutex
	fatalMu          sync.RWMutex
	notifyMu         sync.Mutex
	root             *fsutil.Root
	state            *recovery.Result
	lifecycle        *lifecycle.Manager
	committer        *commit.Committer
	reader           *readstore.Store
	now              func() time.Time
	notifications    map[streamKey]*notification
	appendGates      map[streamKey]*appendGate
	closed           bool
	shutdown         bool
	fatal            error
	nextEntryID      uint64
	previousCRC32C   uint32
	lastRecordedAt   int64
	checkpointHook   fsutil.CrashHook
	term             uint64
	role             format.ReplicationRole
	durability       format.ReplicationDurability
	guard            commit.Guard
	commitOptions    commit.Options
	commitStatsMu    sync.Mutex
	commitStats      commit.Stats
	commitArchived   bool
	capacityCritical atomic.Bool
}

func (s *Store) DataRoot() string { return s.root.Path() }

func (s *Store) Read(namespace, name string, from uint64, maxRecords int, maxBytes uint64) (readstore.Result, error) {
	s.viewMu.RLock()
	defer s.viewMu.RUnlock()
	mapping, ok, err := s.state.Registry.Lookup(namespace, name)
	if err != nil {
		return readstore.Result{}, err
	}
	if !ok {
		return readstore.Result{}, errdefs.ErrStreamNotFound
	}
	return s.reader.Read(mapping.StreamID, from, maxRecords, maxBytes)
}
func (s *Store) Inspect(namespace, name string) (readstore.StreamInfo, error) {
	s.viewMu.RLock()
	defer s.viewMu.RUnlock()
	mapping, ok, err := s.state.Registry.Lookup(namespace, name)
	if err != nil {
		return readstore.StreamInfo{}, err
	}
	if !ok {
		return readstore.StreamInfo{}, nil
	}
	return s.reader.Inspect(mapping.StreamID)
}
func (s *Store) ResolveTime(namespace, name string, target int64, mode readstore.TimeMode) (uint64, int64, bool, error) {
	s.viewMu.RLock()
	defer s.viewMu.RUnlock()
	mapping, ok, err := s.state.Registry.Lookup(namespace, name)
	if err != nil {
		return 0, 0, false, err
	}
	if !ok {
		return 0, 0, false, nil
	}
	return s.reader.ResolveTime(mapping.StreamID, target, mode)
}
func (s *Store) Health() Health {
	s.mu.Lock()
	defer s.mu.Unlock()
	return Health{Watermarks: s.committer.Watermarks(), Fatal: errors.Join(s.committer.FatalError(), s.fatalError()), WriteUnavailable: s.writeUnavailable(), Role: s.role, Durability: s.durability, Term: s.term, CapacityCritical: s.capacityCritical.Load()}
}

// SetCapacityCritical is controlled by the node maintenance loop. Critical
// capacity rejects new writes while preserving reads and maintenance access.
func (s *Store) SetCapacityCritical(critical bool) { s.capacityCritical.Store(critical) }

func (s *Store) MaintenanceStats() MaintenanceStats {
	s.mu.Lock()
	records, bytes := s.state.MemTable.Stats()
	walBytes := s.state.WAL.Scan().LastGoodOffset
	s.mu.Unlock()
	if walBytes < 0 {
		walBytes = 0
	}
	s.gateMu.Lock()
	gates := len(s.appendGates)
	s.gateMu.Unlock()
	s.notifyMu.Lock()
	notifications := len(s.notifications)
	s.notifyMu.Unlock()
	return MaintenanceStats{MemTableRecords: records, MemTableBytes: bytes, ActiveWALBytes: uint64(walBytes), AppendGates: uint64(gates), NotificationStreams: uint64(notifications)}
}

// CollectWAL removes only sealed WAL files covered by both the current
// Manifest and one externally verified installable Snapshot. Maintenance is
// serialized with Checkpoint/Compaction and admitted appends are drained before
// the history view is opened.
func (s *Store) CollectWAL(evidence WALCollectionEvidence) (wal.GCResult, error) {
	if !evidence.SnapshotVerified {
		return wal.GCResult{}, fmt.Errorf("WAL collection requires a verified Snapshot")
	}
	s.maintenanceMu.Lock()
	defer s.maintenanceMu.Unlock()
	s.mu.Lock()
	if s.shutdown {
		s.mu.Unlock()
		return wal.GCResult{}, errdefs.ErrClosed
	}
	if err := s.fatalError(); err != nil {
		s.mu.Unlock()
		return wal.GCResult{}, fmt.Errorf("engine failed: %w", err)
	}
	if err := s.committer.Barrier(context.Background()); err != nil {
		s.setFatal(err)
		s.mu.Unlock()
		return wal.GCResult{}, err
	}
	manifest, ok := s.state.Manifest.Current()
	if !ok || manifest.Header.RecordCount == 0 || evidence.SnapshotEntryID > manifest.Header.LastEntryID {
		s.mu.Unlock()
		return wal.GCResult{}, fmt.Errorf("verified Snapshot is not covered by the current Manifest")
	}
	watermarks := s.committer.Watermarks()
	if (s.role == format.ReplicationRolePrimary || s.role == format.ReplicationRoleStandby) && (!watermarks.HasCommitted || evidence.SnapshotEntryID > watermarks.Committed) {
		s.mu.Unlock()
		return wal.GCResult{}, fmt.Errorf("verified Snapshot is not covered by committed state")
	}
	history, err := wal.OpenHistory(s.root.Path())
	if err != nil {
		s.mu.Unlock()
		return wal.GCResult{}, err
	}
	replica := wal.HistoryPosition{Present: watermarks.HasReplicated, EntryID: watermarks.Replicated}
	if s.role == format.ReplicationRoleSingle {
		replica = wal.HistoryPosition{Present: watermarks.HasLocalDurable, EntryID: watermarks.LocalDurable}
	}
	s.mu.Unlock()
	return history.Collect(wal.GCOptions{
		SegmentedThrough: manifest.Header.LastEntryID, SnapshotThrough: evidence.SnapshotEntryID,
		SnapshotVerified: true, ReplicaDurable: replica, MaxRetainedBytes: evidence.MaxRetainedBytes,
	})
}

func (s *Store) CommitStats() commit.Stats {
	s.commitStatsMu.Lock()
	defer s.commitStatsMu.Unlock()
	if s.commitArchived {
		return s.commitStats
	}
	return mergeCommitStats(s.commitStats, s.committer.Stats())
}

// CommitBarrier waits until every Append admitted before this call has left
// the Committer. It does not rotate the WAL, build Segments, or publish a new
// Manifest Generation.
func (s *Store) CommitBarrier(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.shutdown {
		return errdefs.ErrClosed
	}
	if err := s.fatalError(); err != nil {
		return fmt.Errorf("engine failed: %w", err)
	}
	return s.committer.Barrier(ctx)
}

func (s *Store) archiveCommitterLocked() {
	s.commitStatsMu.Lock()
	defer s.commitStatsMu.Unlock()
	if s.commitArchived {
		return
	}
	s.commitStats = mergeCommitStats(s.commitStats, s.committer.Stats())
	s.commitArchived = true
}

func mergeCommitStats(a, b commit.Stats) commit.Stats {
	return commit.Stats{
		Groups: a.Groups + b.Groups, Requests: a.Requests + b.Requests, Entries: a.Entries + b.Entries, Bytes: a.Bytes + b.Bytes,
		LocalSyncs: a.LocalSyncs + b.LocalSyncs, ReplicatedGroups: a.ReplicatedGroups + b.ReplicatedGroups,
		MaxGroupRequests: max(a.MaxGroupRequests, b.MaxGroupRequests), MaxGroupBytes: max(a.MaxGroupBytes, b.MaxGroupBytes),
		QueueWaitNanos: a.QueueWaitNanos + b.QueueWaitNanos, CollectNanos: a.CollectNanos + b.CollectNanos,
		AppendNanos: a.AppendNanos + b.AppendNanos, LocalSyncNanos: a.LocalSyncNanos + b.LocalSyncNanos,
		ReplicateNanos: a.ReplicateNanos + b.ReplicateNanos, ApplyNanos: a.ApplyNanos + b.ApplyNanos,
		ProcessNanos: a.ProcessNanos + b.ProcessNanos, QueueDepth: b.QueueDepth, QueueCapacity: b.QueueCapacity,
	}
}

func Open(path string) (*Store, error) {
	return open(path, nil, nil, GroupCommitOptions{})
}

func OpenWithGroupCommit(path string, options GroupCommitOptions) (*Store, error) {
	return open(path, nil, nil, options)
}

func OpenWithIdentity(path string, node format.NodeIdentity) (*Store, error) {
	return open(path, &node, nil, GroupCommitOptions{})
}

func OpenWithIdentityAndGroupCommit(path string, node format.NodeIdentity, options GroupCommitOptions) (*Store, error) {
	return open(path, &node, nil, options)
}

func OpenReplicated(path string, node format.NodeIdentity, options ReplicationOptions) (*Store, error) {
	if options.Term == 0 || options.Role == 0 || options.Durability == 0 || options.Guard == nil {
		return nil, fmt.Errorf("replicated engine requires Term, Role, durability, and commit guard")
	}
	if options.Role == format.ReplicationRolePrimary && options.Durability == format.ReplicationDurabilityStrict && options.Replica == nil {
		return nil, fmt.Errorf("Strict Primary requires a replica")
	}
	return open(path, &node, &options, options.GroupCommit)
}

func open(path string, node *format.NodeIdentity, replication *ReplicationOptions, groupCommit GroupCommitOptions) (*Store, error) {
	if groupCommit.MaxDelay < 0 || groupCommit.MaxRequests < 0 || groupCommit.QueueCapacity < 0 {
		return nil, fmt.Errorf("group commit options cannot be negative")
	}
	root, err := fsutil.OpenRoot(path)
	if err != nil {
		return nil, err
	}
	if node != nil {
		if _, err = identity.Ensure(root.Path(), *node); err != nil {
			root.Close()
			return nil, err
		}
	}
	if replication == nil {
		modeIdentity := node
		if modeIdentity == nil {
			loaded, loadErr := identity.Load(root.Path())
			if loadErr == nil {
				modeIdentity = &loaded
			} else if !errors.Is(loadErr, os.ErrNotExist) {
				root.Close()
				return nil, fmt.Errorf("load NODE for storage mode: %w", loadErr)
			}
		}
		if modeIdentity != nil {
			stateStore, stateErr := replicationstate.Open(root.Path(), *modeIdentity)
			if stateErr != nil {
				root.Close()
				return nil, stateErr
			}
			if current, ok := stateStore.Current(); ok && current.Header.Role != format.ReplicationRoleSingle {
				root.Close()
				return nil, fmt.Errorf("durable Replication State role %d cannot open as Single", current.Header.Role)
			}
		}
	}
	var checkpoint *format.ReplicationState
	var applyThrough *uint64
	if replication != nil && node != nil {
		stateStore, stateErr := replicationstate.Open(root.Path(), *node)
		if stateErr != nil {
			root.Close()
			return nil, stateErr
		}
		if current, ok := stateStore.Current(); ok {
			if replication.ExpectedStateID != (format.UUID{}) && current.Header.StateID != replication.ExpectedStateID {
				root.Close()
				return nil, fmt.Errorf("durable Replication State changed before engine lock")
			}
			if current.Header.Term != replication.Term || current.Header.Role != replication.Role || current.Header.Durability != replication.Durability {
				root.Close()
				return nil, fmt.Errorf("replicated engine options do not match durable Replication State")
			}
			checkpoint = &current
			if current.Header.Committed.Present {
				committed := current.Header.Committed.EntryID
				applyThrough = &committed
			}
		}
	}
	state, err := recovery.OpenWithOptions(root.Path(), recovery.Options{ApplyThrough: applyThrough})
	if err != nil {
		root.Close()
		return nil, err
	}
	if checkpoint != nil && (replication.Role == format.ReplicationRolePrimary || replication.RejectUncommittedSuffix) && state.WAL.NextEntryID() > 0 && (!checkpoint.Header.Committed.Present || state.WAL.NextEntryID()-1 > checkpoint.Header.Committed.EntryID) {
		state.Close()
		root.Close()
		return nil, fmt.Errorf("Primary has an unresolved durable WAL suffix after its committed watermark")
	}
	if replication != nil && state.WAL.Scan().Header.CreatedTerm != replication.Term {
		if err = state.WAL.Rotate(replication.Term, time.Now()); err != nil {
			state.Close()
			root.Close()
			return nil, fmt.Errorf("rotate WAL for Term %d: %w", replication.Term, err)
		}
	}
	lastRecordedAt := state.LastRecordedAt
	for _, snapshot := range state.MemTable.Snapshot() {
		if snapshot.Tail.RecordCount > 0 && snapshot.Tail.LastRecordedAt > lastRecordedAt {
			lastRecordedAt = snapshot.Tail.LastRecordedAt
		}
	}
	role, durability := format.ReplicationRoleSingle, format.ReplicationDurabilitySingleSync
	var term uint64
	var guard commit.Guard
	commitOptions := commit.Options{MaxDelay: groupCommit.MaxDelay, MaxRequests: groupCommit.MaxRequests, MaxBytes: groupCommit.MaxBytes, QueueCapacity: groupCommit.QueueCapacity}
	if replication != nil {
		term, role, durability, guard = replication.Term, replication.Role, replication.Durability, replication.Guard
		commitOptions.Replica = replication.Replica
		commitOptions.Guard = replication.Guard
		commitOptions.ReplicaTimeout = replication.ReplicaTimeout
		if checkpoint != nil {
			commitOptions.InitialWatermarks = watermarksFromState(checkpoint.Header)
		}
	}
	generation := uint64(0)
	if current, ok := state.Manifest.Current(); ok {
		generation = current.Header.Generation
	}
	return &Store{root: root, state: state, lifecycle: lifecycle.New(root.Path(), state.Manifest), committer: commit.NewWithOptions(state.WAL, state.MemTable, commitOptions), reader: readstore.New(state.MemTable, state.TailResolver, root.Path(), generation, state.Segments, state.Locator, defaultStreamCacheCapacity, defaultSegmentHandleCapacity), now: time.Now, notifications: make(map[streamKey]*notification), appendGates: make(map[streamKey]*appendGate), nextEntryID: state.WAL.NextEntryID(), previousCRC32C: state.WAL.PreviousEntryCRC32C(), lastRecordedAt: lastRecordedAt, term: term, role: role, durability: durability, guard: guard, commitOptions: commitOptions}, nil
}

func watermarksFromState(header format.ReplicationStateHeader) commit.Watermarks {
	return commit.Watermarks{
		HasValue: header.LastAppended.Present, Appended: header.LastAppended.EntryID,
		HasLocalDurable: header.LocalDurable.Present, LocalDurable: header.LocalDurable.EntryID,
		HasReplicated: header.Replicated.Present, Replicated: header.Replicated.EntryID,
		HasCommitted: header.Committed.Present, Committed: header.Committed.EntryID,
		HasApplied: header.Applied.Present, Applied: header.Applied.EntryID,
	}
}
func (s *Store) Close() error {
	s.maintenanceMu.Lock()
	defer s.maintenanceMu.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.shutdown {
		return nil
	}
	s.shutdown = true
	s.closeNotifications()
	commitErr := s.committer.Close()
	s.archiveCommitterLocked()
	s.viewMu.Lock()
	defer s.viewMu.Unlock()
	return errors.Join(commitErr, s.reader.Close(), s.state.Close(), s.root.Close())
}
func (s *Store) Append(ctx context.Context, request AppendRequest) (AppendResult, error) {
	if request.Namespace == "" || request.Stream == "" || len(request.RequestID) == 0 || len(request.RequestID) > format.MaxRequestIDLength || request.Producer == "" || len(request.Records) == 0 || len(request.Records) > format.MaxBatchRecordCount {
		return AppendResult{}, errdefs.ErrInvalidArgument
	}
	if err := s.writeUnavailable(); err != nil {
		return AppendResult{}, err
	}
	hash, err := RequestHash(request)
	if err != nil {
		return AppendResult{}, fmt.Errorf("%w: %v", errdefs.ErrInvalidArgument, err)
	}
	if _, err = format.MarshalRegistryRecord(format.RegistryRecord{AssignedStreamID: 1, Namespace: request.Namespace, StreamName: request.Stream}); err != nil {
		return AppendResult{}, fmt.Errorf("%w: %v", errdefs.ErrInvalidArgument, err)
	}
	if err = validateInputRecords(request, hash); err != nil {
		return AppendResult{}, fmt.Errorf("%w: %v", errdefs.ErrInvalidArgument, err)
	}
	release, err := s.acquireAppendGate(ctx, streamKey{namespace: request.Namespace, stream: request.Stream})
	if err != nil {
		return AppendResult{}, err
	}
	releaseOnReturn := true
	defer func() {
		if releaseOnReturn {
			release()
		}
	}()
	s.mu.Lock()
	if s.shutdown {
		s.mu.Unlock()
		return AppendResult{}, errdefs.ErrClosed
	}
	if err = s.fatalError(); err != nil {
		s.mu.Unlock()
		return AppendResult{}, fmt.Errorf("engine failed: %w", err)
	}
	if err = s.writeUnavailable(); err != nil {
		s.mu.Unlock()
		return AppendResult{}, err
	}
	mapping, exists, err := s.state.Registry.Lookup(request.Namespace, request.Stream)
	if err != nil {
		s.mu.Unlock()
		return AppendResult{}, err
	}
	if !exists {
		proposal, _, err := s.state.Registry.NextAssignment(request.Namespace, request.Stream)
		if err != nil {
			s.mu.Unlock()
			return AppendResult{}, err
		}
		payload, err := format.MarshalRegistryRecord(proposal)
		if err != nil {
			s.mu.Unlock()
			return AppendResult{}, err
		}
		if err = s.commitRegistry(proposal, payload); err != nil {
			s.mu.Unlock()
			return AppendResult{}, err
		}
		mapping, _, err = s.state.Registry.Lookup(request.Namespace, request.Stream)
		if err != nil {
			s.mu.Unlock()
			return AppendResult{}, err
		}
	}
	if err = ctx.Err(); err != nil {
		s.mu.Unlock()
		return AppendResult{}, err
	}
	tail, ok, err := s.state.TailResolver.EnsureActive(mapping.StreamID)
	if err != nil {
		s.mu.Unlock()
		return AppendResult{}, err
	}
	if !ok {
		tail = zeroTail()
	}
	if request.ExpectedSequence < tail.NextSequence {
		result, deduplicateErr := s.deduplicate(mapping.StreamID, request, hash, tail.NextSequence)
		s.mu.Unlock()
		return result, deduplicateErr
	}
	if request.ExpectedSequence > tail.NextSequence {
		s.mu.Unlock()
		return AppendResult{}, &errdefs.SequenceAheadError{Requested: request.ExpectedSequence, CurrentNextSequence: tail.NextSequence}
	}
	entryID := s.nextEntryID
	previous := s.previousCRC32C
	sequence, offset := tail.NextSequence, tail.NextByteOffset
	count := uint64(len(request.Records))
	if count-1 > math.MaxUint64-entryID || count-1 > math.MaxUint64-sequence {
		s.mu.Unlock()
		return AppendResult{}, fmt.Errorf("Append identifiers overflow")
	}
	recordedAt := s.now().UnixNano()
	if recordedAt < s.lastRecordedAt {
		recordedAt = s.lastRecordedAt
	}
	encoded := make([][]byte, 0, len(request.Records))
	firstTime := recordedAt
	for i, input := range request.Records {
		frame, err := format.MarshalRecordFrame(format.RecordFrame{EntryID: entryID + uint64(i), StreamID: mapping.StreamID, Sequence: sequence + uint64(i), ByteOffset: offset, RecordedAt: recordedAt, BatchIndex: uint32(i), BatchCount: uint32(len(request.Records)), RequestHash: hash, RequestID: request.RequestID, Producer: request.Producer, Headers: input.Headers, Payload: input.Payload})
		if err != nil {
			s.mu.Unlock()
			return AppendResult{}, err
		}
		walEntry, err := format.MarshalWALEntry(s.term, previous, frame)
		if err != nil {
			s.mu.Unlock()
			return AppendResult{}, err
		}
		decoded, err := format.UnmarshalWALEntry(walEntry)
		if err != nil {
			s.mu.Unlock()
			return AppendResult{}, err
		}
		encoded = append(encoded, walEntry)
		previous = decoded.CRC32C
		if uint64(len(frame)) > math.MaxUint64-offset {
			s.mu.Unlock()
			return AppendResult{}, fmt.Errorf("Stream Byte Offset overflows")
		}
		offset += uint64(len(frame))
	}
	committer := s.committer
	future, err := committer.Enqueue(encoded)
	if err != nil {
		s.setFatal(err)
		s.mu.Unlock()
		return AppendResult{}, err
	}
	s.nextEntryID = entryID + count
	s.previousCRC32C = previous
	s.lastRecordedAt = recordedAt
	s.mu.Unlock()
	commitResult, err := future.Wait(ctx)
	if err != nil {
		watermarks := committer.Watermarks()
		if watermarks.HasApplied && watermarks.Applied >= commitResult.LastEntryID && commitResult.RecordCount > 0 {
			s.signal(request.Namespace, request.Stream)
		}
		if commitResult.ResultUncertain && commitResult.RecordCount == 0 {
			releaseOnReturn = false
			go s.finishAppend(future, request.Namespace, request.Stream, release)
		}
		return AppendResult{}, &errdefs.WriteError{Err: err, ResultUncertain: commitResult.ResultUncertain}
	}
	s.signal(request.Namespace, request.Stream)
	return AppendResult{FirstSequence: sequence, NextSequence: sequence + uint64(len(encoded)), RecordCount: uint32(len(encoded)), FirstRecordedAt: firstTime, LastRecordedAt: recordedAt, FirstEntryID: commitResult.FirstEntryID, LastEntryID: commitResult.LastEntryID}, nil
}

func (s *Store) Checkpoint() (format.Manifest, bool, error) {
	s.maintenanceMu.Lock()
	defer s.maintenanceMu.Unlock()
	return s.checkpointLocked(nil)
}

// CheckpointReplicated persists an exact committed recovery floor before it
// freezes the matching MemTable prefix. The Manifest is built later outside
// the Engine lock, but cannot cover entries beyond that durable Replication
// State floor, so a crash cannot reopen a Standby or former Primary behind the
// published checkpoint.
func (s *Store) CheckpointReplicated(states *replicationstate.Store) (format.Manifest, bool, error) {
	if states == nil {
		return format.Manifest{}, false, fmt.Errorf("replicated State store is required")
	}
	s.maintenanceMu.Lock()
	defer s.maintenanceMu.Unlock()
	return s.checkpointLocked(states)
}

// CheckpointAndPin publishes a checkpoint and pins every Segment referenced by
// that exact Manifest Generation. The release function must be called after an
// online Snapshot or transfer no longer needs those immutable files.
func (s *Store) CheckpointAndPin() (format.Manifest, bool, func(), error) {
	s.maintenanceMu.Lock()
	defer s.maintenanceMu.Unlock()
	manifest, created, err := s.checkpointLocked(nil)
	if err != nil {
		return format.Manifest{}, false, nil, err
	}
	releases := make([]func(), 0, len(manifest.SegmentReferences))
	for _, reference := range manifest.SegmentReferences {
		releases = append(releases, s.lifecycle.Pin(reference.SegmentID))
	}
	for _, reference := range manifest.ArtifactReferences {
		releases = append(releases, s.lifecycle.Pin(reference.ArtifactID))
	}
	if s.state.Locator != nil {
		for _, reference := range s.state.Locator.PackArtifacts() {
			releases = append(releases, s.lifecycle.Pin(reference.ArtifactID))
		}
	}
	var once sync.Once
	release := func() {
		once.Do(func() {
			for _, unpin := range releases {
				unpin()
			}
		})
	}
	return manifest, created, release, nil
}

func (s *Store) checkpointLocked(states *replicationstate.Store) (format.Manifest, bool, error) {
	s.mu.Lock()
	if s.shutdown {
		s.mu.Unlock()
		return format.Manifest{}, false, errdefs.ErrClosed
	}
	if err := s.fatalError(); err != nil {
		s.mu.Unlock()
		return format.Manifest{}, false, fmt.Errorf("engine failed: %w", err)
	}
	if err := s.committer.Barrier(context.Background()); err != nil {
		s.setFatal(err)
		s.mu.Unlock()
		return format.Manifest{}, false, err
	}
	if states != nil {
		if _, err := s.checkpointReplicationStateLocked(states); err != nil {
			s.mu.Unlock()
			return format.Manifest{}, false, err
		}
	}
	records, _ := s.state.MemTable.Stats()
	if records == 0 {
		current, _ := s.state.Manifest.Current()
		s.mu.Unlock()
		return current, false, nil
	}
	lastEntryID := s.state.WAL.NextEntryID() - 1
	lastCRC := s.state.WAL.PreviousEntryCRC32C()
	s.commitOptions.InitialWatermarks = s.committer.Watermarks()
	if err := s.committer.Close(); err != nil {
		s.archiveCommitterLocked()
		s.setFatal(err)
		s.mu.Unlock()
		return format.Manifest{}, false, err
	}
	s.archiveCommitterLocked()

	frozen := s.state.MemTable
	frozen.Freeze()
	active := memtable.New(0)
	for _, snapshot := range frozen.Tails() {
		if err := active.SeedTail(snapshot.StreamID, snapshot.Tail); err != nil {
			s.setFatal(err)
			s.mu.Unlock()
			return format.Manifest{}, false, err
		}
	}
	if err := s.state.WAL.Rotate(s.term, s.now()); err != nil {
		s.setFatal(err)
		s.mu.Unlock()
		return format.Manifest{}, false, err
	}
	if s.checkpointHook != nil {
		if err := s.checkpointHook("after_wal_rotate"); err != nil {
			s.setFatal(err)
			s.mu.Unlock()
			return format.Manifest{}, false, err
		}
	}
	previous, _ := s.state.Manifest.Current()
	baseDescriptors := append([]segment.Descriptor(nil), s.state.Segments...)
	existing := make(map[format.UUID]bool, len(baseDescriptors))
	for _, descriptor := range baseDescriptors {
		existing[descriptor.Reference.SegmentID] = true
	}
	previousTailCatalog := s.state.TailCatalog
	previousLocator := s.state.Locator
	nextTailResolver := tailstore.NewResolver(active, previousTailCatalog, s.root.Path(), baseDescriptors, defaultStreamCacheCapacity)
	nextCommitter := commit.NewWithOptions(s.state.WAL, active, s.commitOptions)
	nextReader := readstore.New(readstore.LayerTables(active, frozen), nextTailResolver, s.root.Path(), previous.Header.Generation, baseDescriptors, previousLocator, defaultStreamCacheCapacity, defaultSegmentHandleCapacity)
	s.viewMu.Lock()
	previousReader := s.reader
	s.state.MemTable = active
	s.state.TailResolver = nextTailResolver
	s.reader = nextReader
	s.commitStatsMu.Lock()
	s.committer = nextCommitter
	s.commitArchived = false
	s.commitStatsMu.Unlock()
	s.viewMu.Unlock()
	s.mu.Unlock()

	if err := previousReader.Close(); err != nil {
		s.setFatal(err)
		return format.Manifest{}, false, err
	}
	if s.checkpointHook != nil {
		if err := s.checkpointHook("after_memtable_switch"); err != nil {
			s.setFatal(err)
			return format.Manifest{}, false, err
		}
	}

	snapshots := frozen.Snapshot()
	flush := make([]memtable.StreamSnapshot, 0, len(snapshots))
	for _, snapshot := range snapshots {
		if len(snapshot.Frames) > 0 {
			flush = append(flush, snapshot)
		}
	}
	var descriptors []segment.Descriptor
	var projections projection.Build
	published, err := s.lifecycle.PublishFlushWithArtifacts(flush, lastEntryID, lastCRC, func(generation uint64, references []format.SegmentReference, coveredEntryID uint64) ([]format.ArtifactReference, error) {
		descriptors = append([]segment.Descriptor(nil), baseDescriptors...)
		for _, reference := range references {
			if existing[reference.SegmentID] {
				continue
			}
			descriptor, describeErr := segment.DescribeReference(s.root.Path(), reference)
			if describeErr != nil {
				return nil, describeErr
			}
			descriptors = append(descriptors, descriptor)
		}
		if len(descriptors) != len(references) {
			return nil, fmt.Errorf("published Manifest Segment set is inconsistent")
		}
		var buildErr error
		projections, buildErr = projection.BuildReferences(s.root.Path(), generation, coveredEntryID, s.now().UnixNano(), descriptors)
		if buildErr != nil {
			return nil, buildErr
		}
		return []format.ArtifactReference{projections.TailReference, projections.Locator.Reference, projections.RegistryReference}, nil
	})
	if err != nil {
		s.setFatal(err)
		return format.Manifest{}, false, err
	}
	if s.checkpointHook != nil {
		if err = s.checkpointHook("after_manifest_publish"); err != nil {
			s.setFatal(err)
			return format.Manifest{}, false, err
		}
	}
	nextTailCatalog, err := tailstore.OpenCheckpoint(s.root.Path(), projections.TailReference, published.Header.Generation, published.Header.LastEntryID)
	if err != nil {
		s.setFatal(err)
		return format.Manifest{}, false, err
	}
	nextLocator, err := locatorstore.Open(s.root.Path(), published, 256)
	if err != nil {
		nextTailCatalog.Close()
		s.setFatal(err)
		return format.Manifest{}, false, err
	}
	nextRegistryStore, err := registry.OpenCheckpoint(s.root.Path(), projections.RegistryReference, published.Header.LastEntryID, 64)
	if err != nil {
		nextTailCatalog.Close()
		nextLocator.Close()
		s.setFatal(err)
		return format.Manifest{}, false, err
	}
	nextRegistry := registry.NewWithSnapshot(nextRegistryStore)
	fallbackDescriptors := segment.LightDescriptors(descriptors)
	nextRegistry.SetFallback(func() ([]registry.Mapping, error) {
		return registry.RebuildMappings(s.root.Path(), fallbackDescriptors)
	})

	s.mu.Lock()
	if err = s.fatalError(); err != nil {
		s.mu.Unlock()
		nextTailCatalog.Close()
		nextLocator.Close()
		return published, true, fmt.Errorf("engine failed while publishing checkpoint: %w", err)
	}
	// No Append can enter while mu is held. Drain every request admitted into
	// the successor Committer before pruning seed-only historical Tails.
	if err = s.committer.Barrier(context.Background()); err != nil {
		s.setFatal(err)
		s.mu.Unlock()
		nextTailCatalog.Close()
		nextLocator.Close()
		return published, true, err
	}
	for _, mapping := range s.state.Registry.MappingsAfter(published.Header.LastEntryID) {
		if err = nextRegistry.ApplyMapping(mapping); err != nil {
			s.setFatal(err)
			s.mu.Unlock()
			nextTailCatalog.Close()
			nextLocator.Close()
			return published, true, err
		}
	}
	retiredArtifacts := projection.ReplacedArtifacts(previous.ArtifactReferences, previousLocator, projections)
	lightDescriptors := segment.LightDescriptors(descriptors)
	finalTailResolver := tailstore.NewResolver(active, nextTailCatalog, s.root.Path(), lightDescriptors, defaultStreamCacheCapacity)
	finalReader := readstore.New(active, finalTailResolver, s.root.Path(), published.Header.Generation, lightDescriptors, nextLocator, defaultStreamCacheCapacity, defaultSegmentHandleCapacity)
	s.viewMu.Lock()
	oldReader := s.reader
	s.state.Segments = lightDescriptors
	s.state.TailCatalog = nextTailCatalog
	s.state.TailResolver = finalTailResolver
	s.state.Locator = nextLocator
	s.state.Registry = nextRegistry
	s.reader = finalReader
	active.PruneSeeded()
	s.viewMu.Unlock()
	s.mu.Unlock()

	closeErr := oldReader.Close()
	if previousTailCatalog != nil {
		closeErr = errors.Join(closeErr, previousTailCatalog.Close())
	}
	if previousLocator != nil {
		closeErr = errors.Join(closeErr, previousLocator.Close())
	}
	if closeErr != nil {
		s.setFatal(closeErr)
		return published, true, closeErr
	}
	if err = s.lifecycle.RetireArtifacts(retiredArtifacts); err != nil {
		return published, true, err
	}
	if s.checkpointHook != nil {
		if err = s.checkpointHook("after_view_install"); err != nil {
			return published, true, err
		}
	}
	return published, true, nil
}

// Compact replaces a bounded set of adjacent immutable Segments and installs
// the resulting Manifest Generation before retiring the input files. Appends
// continue while the replacement Segment is built; Checkpoint and other
// maintenance operations are serialized.
func (s *Store) Compact(options CompactionOptions) (CompactionResult, error) {
	if options.MinSegments < 2 || options.MaxInputSegments < 2 || options.MaxInputBytes == 0 {
		return CompactionResult{}, fmt.Errorf("invalid Compaction options")
	}
	s.maintenanceMu.Lock()
	defer s.maintenanceMu.Unlock()
	s.mu.Lock()
	if s.shutdown {
		s.mu.Unlock()
		return CompactionResult{}, errdefs.ErrClosed
	}
	if err := s.fatalError(); err != nil {
		s.mu.Unlock()
		return CompactionResult{}, fmt.Errorf("engine failed: %w", err)
	}
	s.mu.Unlock()

	current, ok := s.state.Manifest.Current()
	if !ok || len(current.SegmentReferences) < options.MinSegments {
		return CompactionResult{Manifest: current}, nil
	}
	selected, inputBytes := selectCompactionInputs(current.SegmentReferences, options)
	if len(selected) < 2 {
		return CompactionResult{Manifest: current}, nil
	}
	ids := make([]format.UUID, len(selected))
	for i := range selected {
		ids[i] = selected[i].SegmentID
	}
	s.viewMu.RLock()
	table := s.state.MemTable
	existing := make(map[format.UUID]segment.Descriptor, len(s.state.Segments))
	for _, descriptor := range s.state.Segments {
		existing[descriptor.Reference.SegmentID] = descriptor
	}
	s.viewMu.RUnlock()
	previous := current
	var descriptors []segment.Descriptor
	var projections projection.Build
	published, retained, err := s.lifecycle.PublishMergeWithArtifacts(ids, func(generation uint64, references []format.SegmentReference, coveredEntryID uint64) ([]format.ArtifactReference, error) {
		descriptors = make([]segment.Descriptor, 0, len(references))
		for _, reference := range references {
			if descriptor, found := existing[reference.SegmentID]; found {
				descriptors = append(descriptors, descriptor)
				continue
			}
			descriptor, describeErr := segment.DescribeReference(s.root.Path(), reference)
			if describeErr != nil {
				return nil, describeErr
			}
			descriptors = append(descriptors, descriptor)
		}
		var buildErr error
		projections, buildErr = projection.BuildReferences(s.root.Path(), generation, coveredEntryID, s.now().UnixNano(), descriptors)
		if buildErr != nil {
			return nil, buildErr
		}
		return []format.ArtifactReference{projections.TailReference, projections.Locator.Reference, projections.RegistryReference}, nil
	})
	if err != nil {
		s.setFatal(err)
		return CompactionResult{}, err
	}
	nextTailCatalog, err := tailstore.OpenCheckpoint(s.root.Path(), projections.TailReference, published.Header.Generation, published.Header.LastEntryID)
	if err != nil {
		s.setFatal(err)
		return CompactionResult{}, err
	}
	nextLocator, err := locatorstore.Open(s.root.Path(), published, 256)
	if err != nil {
		nextTailCatalog.Close()
		s.setFatal(err)
		return CompactionResult{}, err
	}
	nextRegistryStore, err := registry.OpenCheckpoint(s.root.Path(), projections.RegistryReference, published.Header.LastEntryID, 64)
	if err != nil {
		nextTailCatalog.Close()
		nextLocator.Close()
		s.setFatal(err)
		return CompactionResult{}, err
	}
	nextRegistry := registry.NewWithSnapshot(nextRegistryStore)
	fallbackDescriptors := segment.LightDescriptors(descriptors)
	nextRegistry.SetFallback(func() ([]registry.Mapping, error) {
		return registry.RebuildMappings(s.root.Path(), fallbackDescriptors)
	})
	s.mu.Lock()
	for _, mapping := range s.state.Registry.MappingsAfter(published.Header.LastEntryID) {
		if err = nextRegistry.ApplyMapping(mapping); err != nil {
			s.mu.Unlock()
			nextTailCatalog.Close()
			nextLocator.Close()
			s.setFatal(err)
			return CompactionResult{}, err
		}
	}
	retiredArtifacts := projection.ReplacedArtifacts(previous.ArtifactReferences, s.state.Locator, projections)
	lightDescriptors := segment.LightDescriptors(descriptors)
	nextTailResolver := tailstore.NewResolver(table, nextTailCatalog, s.root.Path(), lightDescriptors, defaultStreamCacheCapacity)
	nextReader := readstore.New(table, nextTailResolver, s.root.Path(), published.Header.Generation, lightDescriptors, nextLocator, defaultStreamCacheCapacity, defaultSegmentHandleCapacity)
	s.viewMu.Lock()
	oldReader := s.reader
	oldTailCatalog := s.state.TailCatalog
	oldLocator := s.state.Locator
	s.state.Segments = lightDescriptors
	s.state.TailCatalog = nextTailCatalog
	s.state.TailResolver = nextTailResolver
	s.state.Locator = nextLocator
	s.state.Registry = nextRegistry
	s.reader = nextReader
	s.viewMu.Unlock()
	s.mu.Unlock()
	err = oldReader.Close()
	if oldTailCatalog != nil {
		err = errors.Join(err, oldTailCatalog.Close())
	}
	if oldLocator != nil {
		err = errors.Join(err, oldLocator.Close())
	}
	if err != nil {
		s.setFatal(err)
		return CompactionResult{}, err
	}
	if err = s.lifecycle.Retire(retained); err != nil {
		return CompactionResult{}, err
	}
	if err = s.lifecycle.RetireArtifacts(retiredArtifacts); err != nil {
		return CompactionResult{}, err
	}
	return CompactionResult{Manifest: published, Created: true, InputSegments: len(selected), InputBytes: inputBytes}, nil
}

func selectCompactionInputs(references []format.SegmentReference, options CompactionOptions) ([]format.SegmentReference, uint64) {
	ordered := append([]format.SegmentReference(nil), references...)
	slices.SortFunc(ordered, func(a, b format.SegmentReference) int {
		if a.FirstEntryID < b.FirstEntryID {
			return -1
		}
		if a.FirstEntryID > b.FirstEntryID {
			return 1
		}
		if a.LastEntryID < b.LastEntryID {
			return -1
		}
		if a.LastEntryID > b.LastEntryID {
			return 1
		}
		return 0
	})
	for start := 0; start+1 < len(ordered); start++ {
		var bytes uint64
		var selected []format.SegmentReference
		for i := start; i < len(ordered) && len(selected) < options.MaxInputSegments; i++ {
			if len(selected) > 0 {
				previous := selected[len(selected)-1]
				if previous.LastEntryID == math.MaxUint64 || ordered[i].FirstEntryID != previous.LastEntryID+1 {
					break
				}
			}
			if ordered[i].FileSize > options.MaxInputBytes-bytes {
				break
			}
			selected = append(selected, ordered[i])
			bytes += ordered[i].FileSize
		}
		if len(selected) >= 2 {
			return selected, bytes
		}
	}
	return nil, 0
}

// CheckpointReplicationState publishes a crash-recovery lower bound without
// adding metadata fsync to every Group Commit.
func (s *Store) CheckpointReplicationState(states *replicationstate.Store) (format.ReplicationState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if states == nil || s.role == format.ReplicationRoleSingle {
		return format.ReplicationState{}, fmt.Errorf("replicated State store is required")
	}
	if err := s.committer.Barrier(context.Background()); err != nil {
		return format.ReplicationState{}, err
	}
	return s.checkpointReplicationStateLocked(states)
}

func (s *Store) checkpointReplicationStateLocked(states *replicationstate.Store) (format.ReplicationState, error) {
	history, err := wal.OpenHistory(s.root.Path())
	if err != nil {
		return format.ReplicationState{}, err
	}
	watermarks := s.committer.Watermarks()
	current, _ := states.Current()
	position := func(present bool, entryID uint64, fallback format.ReplicationPosition) (format.ReplicationPosition, error) {
		if !present {
			return format.ReplicationPosition{}, nil
		}
		checksum, ok, lookupErr := history.ChecksumAt(entryID)
		if lookupErr != nil {
			return format.ReplicationPosition{}, lookupErr
		}
		if ok {
			return format.ReplicationPosition{Present: true, EntryID: entryID, CRC32C: checksum}, nil
		}
		if fallback.Present && fallback.EntryID == entryID {
			return fallback, nil
		}
		if current.Header.HasInstalledSnapshot && current.Header.InstalledSnapshotEntry.EntryID == entryID {
			return current.Header.InstalledSnapshotEntry, nil
		}
		return format.ReplicationPosition{}, fmt.Errorf("checksum for replication watermark %d is unavailable", entryID)
	}
	last, err := position(watermarks.HasValue, watermarks.Appended, current.Header.LastAppended)
	if err != nil {
		return format.ReplicationState{}, err
	}
	local, err := position(watermarks.HasLocalDurable, watermarks.LocalDurable, current.Header.LocalDurable)
	if err != nil {
		return format.ReplicationState{}, err
	}
	replicated, err := position(watermarks.HasReplicated, watermarks.Replicated, current.Header.Replicated)
	if err != nil {
		return format.ReplicationState{}, err
	}
	committed, err := position(watermarks.HasCommitted, watermarks.Committed, current.Header.Committed)
	if err != nil {
		return format.ReplicationState{}, err
	}
	applied, err := position(watermarks.HasApplied, watermarks.Applied, current.Header.Applied)
	if err != nil {
		return format.ReplicationState{}, err
	}
	return states.Update(s.now(), func(header *format.ReplicationStateHeader) error {
		if header.Term != s.term || header.Role != s.role || header.Durability != s.durability {
			return fmt.Errorf("engine role does not match durable Replication State")
		}
		header.LastAppended = last
		header.LocalDurable = local
		header.Replicated = replicated
		header.Committed = committed
		header.Applied = applied
		if earliest, next, present := history.Bounds(); present {
			header.EarliestWALEntryID = earliest
		} else {
			header.EarliestWALEntryID = next
		}
		return nil
	})
}

func (s *Store) fatalError() error {
	s.fatalMu.RLock()
	defer s.fatalMu.RUnlock()
	return s.fatal
}

func (s *Store) writeUnavailable() error {
	if s.role != format.ReplicationRoleSingle {
		if s.role != format.ReplicationRolePrimary || s.guard == nil {
			return errdefs.ErrNotLeader
		}
		if err := s.guard.CanCommit(); err != nil {
			return fmt.Errorf("%w: %v", errdefs.ErrNotLeader, err)
		}
	}
	if s.capacityCritical.Load() {
		return errdefs.ErrCapacityCritical
	}
	return nil
}

func (s *Store) setFatal(err error) {
	s.fatalMu.Lock()
	if s.fatal == nil {
		s.fatal = err
	}
	s.fatalMu.Unlock()
}

func (s *Store) WaitForAppend(ctx context.Context, namespace, name string, after uint64) error {
	key := streamKey{namespace: namespace, stream: name}
	s.notifyMu.Lock()
	if s.closed {
		s.notifyMu.Unlock()
		return errdefs.ErrClosed
	}
	n := s.notifications[key]
	if n == nil {
		n = &notification{ch: make(chan struct{})}
		s.notifications[key] = n
	}
	n.waiters++
	info, err := s.Inspect(namespace, name)
	if err != nil || (info.Exists && info.NextSequence > after) {
		s.releaseNotificationLocked(key, n)
		s.notifyMu.Unlock()
		return err
	}
	s.notifyMu.Unlock()
	defer func() {
		s.notifyMu.Lock()
		s.releaseNotificationLocked(key, n)
		s.notifyMu.Unlock()
	}()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-n.ch:
		s.notifyMu.Lock()
		closed := s.closed
		s.notifyMu.Unlock()
		if closed {
			return errdefs.ErrClosed
		}
		return nil
	}
}

func (s *Store) releaseNotificationLocked(key streamKey, n *notification) {
	if n.waiters > 0 {
		n.waiters--
	}
	if n.waiters == 0 && s.notifications[key] == n {
		delete(s.notifications, key)
	}
}

func (s *Store) signal(namespace, name string) {
	key := streamKey{namespace: namespace, stream: name}
	s.notifyMu.Lock()
	if !s.closed {
		if n := s.notifications[key]; n != nil {
			close(n.ch)
			delete(s.notifications, key)
		}
	}
	s.notifyMu.Unlock()
}

func (s *Store) closeNotifications() {
	s.notifyMu.Lock()
	if !s.closed {
		s.closed = true
		for key, n := range s.notifications {
			close(n.ch)
			delete(s.notifications, key)
		}
	}
	s.notifyMu.Unlock()
}
func (s *Store) commitRegistry(proposal format.RegistryRecord, payload []byte) error {
	tail, ok, err := s.state.TailResolver.EnsureActive(registry.RegistryStreamID)
	if err != nil {
		return err
	}
	if !ok {
		tail = zeroTail()
	}
	entryID := s.nextEntryID
	recorded := s.now().UnixNano()
	if recorded < s.lastRecordedAt {
		recorded = s.lastRecordedAt
	}
	hash := sha256.Sum256(payload)
	requestID := []byte(fmt.Sprintf("registry/%d", proposal.AssignedStreamID))
	frame, err := format.MarshalRecordFrame(format.RecordFrame{EntryID: entryID, StreamID: registry.RegistryStreamID, Sequence: tail.NextSequence, ByteOffset: tail.NextByteOffset, RecordedAt: recorded, BatchCount: 1, RequestHash: hash, RequestID: requestID, Producer: "streamd/registry", Payload: payload})
	if err != nil {
		return err
	}
	encoded, err := format.MarshalWALEntry(s.term, s.previousCRC32C, frame)
	if err != nil {
		return err
	}
	decoded, err := format.UnmarshalWALEntry(encoded)
	if err != nil {
		return err
	}
	future, err := s.committer.Enqueue([][]byte{encoded})
	if err != nil {
		s.setFatal(err)
		return err
	}
	s.nextEntryID++
	s.previousCRC32C = decoded.CRC32C
	s.lastRecordedAt = recorded
	result, err := future.Wait(context.Background())
	if err != nil {
		return err
	}
	if !result.ResultUncertain && result.LastEntryID == entryID {
		if err = s.state.Registry.ApplyRecord(entryID, payload); err != nil {
			s.setFatal(err)
			return err
		}
	}
	return nil
}

func (s *Store) acquireAppendGate(ctx context.Context, key streamKey) (func(), error) {
	s.gateMu.Lock()
	gate := s.appendGates[key]
	if gate == nil {
		gate = &appendGate{token: make(chan struct{}, 1)}
		s.appendGates[key] = gate
	}
	gate.refs++
	s.gateMu.Unlock()
	select {
	case gate.token <- struct{}{}:
		return func() {
			<-gate.token
			s.releaseAppendGate(key, gate)
		}, nil
	case <-ctx.Done():
		s.releaseAppendGate(key, gate)
		return nil, ctx.Err()
	}
}

func (s *Store) releaseAppendGate(key streamKey, gate *appendGate) {
	s.gateMu.Lock()
	gate.refs--
	if gate.refs == 0 && s.appendGates[key] == gate {
		delete(s.appendGates, key)
	}
	s.gateMu.Unlock()
}

func (s *Store) finishAppend(future *commit.Future, namespace, stream string, release func()) {
	defer release()
	result, err := future.Wait(context.Background())
	if err == nil && result.RecordCount > 0 {
		s.signal(namespace, stream)
	}
}
func (s *Store) deduplicate(streamID uint64, request AppendRequest, hash [32]byte, currentNext uint64) (AppendResult, error) {
	result, err := s.reader.Read(streamID, request.ExpectedSequence, len(request.Records), 0)
	if err != nil {
		return AppendResult{}, err
	}
	if len(result.Records) != len(request.Records) {
		return AppendResult{}, errdefs.ErrSequenceConflict
	}
	first := result.Records[0]
	if first.BatchIndex != 0 || int(first.BatchCount) != len(request.Records) {
		return AppendResult{}, errdefs.ErrSequenceConflict
	}
	for _, record := range result.Records {
		if !bytes.Equal(record.RequestID, request.RequestID) || record.RequestHash != hash || record.BatchCount != first.BatchCount {
			return AppendResult{}, errdefs.ErrSequenceConflict
		}
	}
	last := result.Records[len(result.Records)-1]
	return AppendResult{FirstSequence: first.Sequence, NextSequence: last.Sequence + 1, RecordCount: first.BatchCount, FirstRecordedAt: first.RecordedAt, LastRecordedAt: last.RecordedAt, FirstEntryID: first.EntryID, LastEntryID: last.EntryID, Deduplicated: true}, nil
}
func RequestHash(request AppendRequest) ([32]byte, error) {
	var zero [32]byte
	if request.Namespace == "" || request.Stream == "" || len(request.RequestID) == 0 {
		return zero, fmt.Errorf("invalid Request Hash input")
	}
	h := sha256.New()
	h.Write([]byte("streamd.append.v1"))
	writeLength(h, []byte(request.Namespace))
	writeLength(h, []byte(request.Stream))
	var u64 [8]byte
	binary.LittleEndian.PutUint64(u64[:], request.ExpectedSequence)
	h.Write(u64[:])
	writeLength(h, request.RequestID)
	var u32 [4]byte
	binary.LittleEndian.PutUint32(u32[:], uint32(len(request.Records)))
	h.Write(u32[:])
	for _, record := range request.Records {
		headers := slices.Clone(record.Headers)
		slices.SortFunc(headers, func(a, b format.Header) int { return bytes.Compare([]byte(a.Key), []byte(b.Key)) })
		binary.LittleEndian.PutUint32(u32[:], uint32(len(headers)))
		h.Write(u32[:])
		for i, header := range headers {
			if i > 0 && header.Key == headers[i-1].Key {
				return zero, fmt.Errorf("duplicate Header %q", header.Key)
			}
			writeLength(h, []byte(header.Key))
			writeLength(h, header.Value)
		}
		writeLength(h, record.Payload)
	}
	copy(zero[:], h.Sum(nil))
	return zero, nil
}
func validateInputRecords(request AppendRequest, hash [32]byte) error {
	offset := uint64(0)
	for i, input := range request.Records {
		frame, err := format.MarshalRecordFrame(format.RecordFrame{EntryID: uint64(i), StreamID: 1, Sequence: uint64(i), ByteOffset: offset, BatchIndex: uint32(i), BatchCount: uint32(len(request.Records)), RequestHash: hash, RequestID: request.RequestID, Producer: request.Producer, Headers: input.Headers, Payload: input.Payload})
		if err != nil {
			return err
		}
		offset += uint64(len(frame))
	}
	return nil
}

type byteWriter interface{ Write([]byte) (int, error) }

func writeLength(w byteWriter, value []byte) {
	var length [8]byte
	binary.LittleEndian.PutUint64(length[:], uint64(len(value)))
	w.Write(length[:])
	w.Write(value)
}
func zeroTail() memtable.Tail { return memtable.Tail{} }
