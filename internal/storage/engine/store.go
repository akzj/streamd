package engine

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"slices"
	"sync"
	"time"

	"github.com/akzj/streamd/internal/storage/commit"
	"github.com/akzj/streamd/internal/storage/errdefs"
	"github.com/akzj/streamd/internal/storage/format"
	"github.com/akzj/streamd/internal/storage/fsutil"
	"github.com/akzj/streamd/internal/storage/identity"
	"github.com/akzj/streamd/internal/storage/lifecycle"
	"github.com/akzj/streamd/internal/storage/memtable"
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
}
type streamKey struct {
	namespace string
	stream    string
}

const (
	defaultStreamCacheCapacity   = 1024
	defaultSegmentHandleCapacity = 64
)

type Store struct {
	maintenanceMu  sync.Mutex
	mu             sync.Mutex
	gateMu         sync.Mutex
	viewMu         sync.RWMutex
	fatalMu        sync.RWMutex
	notifyMu       sync.Mutex
	root           *fsutil.Root
	state          *recovery.Result
	lifecycle      *lifecycle.Manager
	committer      *commit.Committer
	reader         *readstore.Store
	now            func() time.Time
	notifications  map[streamKey]chan struct{}
	appendGates    map[streamKey]chan struct{}
	closed         bool
	shutdown       bool
	fatal          error
	nextEntryID    uint64
	previousCRC32C uint32
	lastRecordedAt int64
	checkpointHook fsutil.CrashHook
	term           uint64
	role           format.ReplicationRole
	durability     format.ReplicationDurability
	guard          commit.Guard
	commitOptions  commit.Options
}

func (s *Store) DataRoot() string { return s.root.Path() }

func (s *Store) Read(namespace, name string, from uint64, maxRecords int, maxBytes uint64) (readstore.Result, error) {
	s.viewMu.RLock()
	defer s.viewMu.RUnlock()
	mapping, ok := s.state.Registry.Lookup(namespace, name)
	if !ok {
		return readstore.Result{}, errdefs.ErrStreamNotFound
	}
	return s.reader.Read(mapping.StreamID, from, maxRecords, maxBytes)
}
func (s *Store) Inspect(namespace, name string) (readstore.StreamInfo, error) {
	s.viewMu.RLock()
	defer s.viewMu.RUnlock()
	mapping, ok := s.state.Registry.Lookup(namespace, name)
	if !ok {
		return readstore.StreamInfo{}, nil
	}
	return s.reader.Inspect(mapping.StreamID)
}
func (s *Store) ResolveTime(namespace, name string, target int64, mode readstore.TimeMode) (uint64, int64, bool, error) {
	s.viewMu.RLock()
	defer s.viewMu.RUnlock()
	mapping, ok := s.state.Registry.Lookup(namespace, name)
	if !ok {
		return 0, 0, false, nil
	}
	return s.reader.ResolveTime(mapping.StreamID, target, mode)
}
func (s *Store) Health() Health {
	s.mu.Lock()
	defer s.mu.Unlock()
	return Health{Watermarks: s.committer.Watermarks(), Fatal: errors.Join(s.committer.FatalError(), s.fatalError()), WriteUnavailable: s.writeUnavailable(), Role: s.role, Durability: s.durability, Term: s.term}
}

func Open(path string) (*Store, error) {
	return open(path, nil, nil)
}

func OpenWithIdentity(path string, node format.NodeIdentity) (*Store, error) {
	return open(path, &node, nil)
}

func OpenReplicated(path string, node format.NodeIdentity, options ReplicationOptions) (*Store, error) {
	if options.Term == 0 || options.Role == 0 || options.Durability == 0 || options.Guard == nil {
		return nil, fmt.Errorf("replicated engine requires Term, Role, durability, and commit guard")
	}
	if options.Role == format.ReplicationRolePrimary && options.Durability == format.ReplicationDurabilityStrict && options.Replica == nil {
		return nil, fmt.Errorf("Strict Primary requires a replica")
	}
	return open(path, &node, &options)
}

func open(path string, node *format.NodeIdentity, replication *ReplicationOptions) (*Store, error) {
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
	var checkpoint *format.ReplicationState
	var applyThrough *uint64
	if replication != nil && node != nil {
		stateStore, stateErr := replicationstate.Open(root.Path(), *node)
		if stateErr != nil {
			root.Close()
			return nil, stateErr
		}
		if current, ok := stateStore.Current(); ok {
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
	if checkpoint != nil && replication.Role == format.ReplicationRolePrimary && state.WAL.NextEntryID() > 0 && (!checkpoint.Header.Committed.Present || state.WAL.NextEntryID()-1 > checkpoint.Header.Committed.EntryID) {
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
	var lastRecordedAt int64
	for _, snapshot := range state.MemTable.Snapshot() {
		if snapshot.Tail.RecordCount > 0 && snapshot.Tail.LastRecordedAt > lastRecordedAt {
			lastRecordedAt = snapshot.Tail.LastRecordedAt
		}
	}
	role, durability := format.ReplicationRoleSingle, format.ReplicationDurabilitySingleSync
	var term uint64
	var guard commit.Guard
	var commitOptions commit.Options
	if replication != nil {
		term, role, durability, guard = replication.Term, replication.Role, replication.Durability, replication.Guard
		commitOptions = commit.Options{Replica: replication.Replica, Guard: replication.Guard, ReplicaTimeout: replication.ReplicaTimeout}
		if checkpoint != nil {
			commitOptions.InitialWatermarks = watermarksFromState(checkpoint.Header)
		}
	}
	generation := uint64(0)
	if current, ok := state.Manifest.Current(); ok {
		generation = current.Header.Generation
	}
	return &Store{root: root, state: state, lifecycle: lifecycle.New(root.Path(), state.Manifest), committer: commit.NewWithOptions(state.WAL, state.MemTable, commitOptions), reader: readstore.New(state.MemTable, root.Path(), generation, state.Segments, defaultStreamCacheCapacity, defaultSegmentHandleCapacity), now: time.Now, notifications: make(map[streamKey]chan struct{}), appendGates: make(map[streamKey]chan struct{}), nextEntryID: state.WAL.NextEntryID(), previousCRC32C: state.WAL.PreviousEntryCRC32C(), lastRecordedAt: lastRecordedAt, term: term, role: role, durability: durability, guard: guard, commitOptions: commitOptions}, nil
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
	mapping, exists := s.state.Registry.Lookup(request.Namespace, request.Stream)
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
		mapping, _ = s.state.Registry.Lookup(request.Namespace, request.Stream)
	}
	if err = ctx.Err(); err != nil {
		s.mu.Unlock()
		return AppendResult{}, err
	}
	tail, ok := s.state.MemTable.Tail(mapping.StreamID)
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
	return s.checkpointLocked()
}

// CheckpointAndPin publishes a checkpoint and pins every Segment referenced by
// that exact Manifest Generation. The release function must be called after an
// online Snapshot or transfer no longer needs those immutable files.
func (s *Store) CheckpointAndPin() (format.Manifest, bool, func(), error) {
	s.maintenanceMu.Lock()
	defer s.maintenanceMu.Unlock()
	manifest, created, err := s.checkpointLocked()
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

func (s *Store) checkpointLocked() (format.Manifest, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.shutdown {
		return format.Manifest{}, false, errdefs.ErrClosed
	}
	if err := s.fatalError(); err != nil {
		return format.Manifest{}, false, fmt.Errorf("engine failed: %w", err)
	}
	if err := s.committer.Barrier(context.Background()); err != nil {
		s.setFatal(err)
		return format.Manifest{}, false, err
	}
	records, _ := s.state.MemTable.Stats()
	if records == 0 {
		current, _ := s.state.Manifest.Current()
		return current, false, nil
	}
	snapshots := s.state.MemTable.Snapshot()
	flush := make([]memtable.StreamSnapshot, 0, len(snapshots))
	for _, snapshot := range snapshots {
		if len(snapshot.Frames) > 0 {
			flush = append(flush, snapshot)
		}
	}
	lastEntryID := s.state.WAL.NextEntryID() - 1
	lastCRC := s.state.WAL.PreviousEntryCRC32C()
	s.commitOptions.InitialWatermarks = s.committer.Watermarks()
	if err := s.committer.Close(); err != nil {
		s.setFatal(err)
		return format.Manifest{}, false, err
	}
	if err := s.state.WAL.Rotate(s.term, s.now()); err != nil {
		s.setFatal(err)
		return format.Manifest{}, false, err
	}
	if s.checkpointHook != nil {
		if err := s.checkpointHook("after_wal_rotate"); err != nil {
			s.setFatal(err)
			return format.Manifest{}, false, err
		}
	}
	previous, _ := s.state.Manifest.Current()
	existing := make(map[format.UUID]bool, len(s.state.Segments))
	for _, descriptor := range s.state.Segments {
		existing[descriptor.Reference.SegmentID] = true
	}
	var descriptors []segment.Descriptor
	var tailReference format.ArtifactReference
	published, err := s.lifecycle.PublishFlushWithArtifacts(flush, lastEntryID, lastCRC, func(generation uint64, references []format.SegmentReference, coveredEntryID uint64) ([]format.ArtifactReference, error) {
		descriptors = append([]segment.Descriptor(nil), s.state.Segments...)
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
		tailReference, buildErr = s.buildTailReference(generation, coveredEntryID, descriptors, snapshots)
		if buildErr != nil {
			return nil, buildErr
		}
		return []format.ArtifactReference{tailReference}, nil
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
	nextTailCatalog, err := tailstore.OpenCheckpoint(s.root.Path(), tailReference, published.Header.Generation, published.Header.LastEntryID)
	if err != nil {
		s.setFatal(err)
		return format.Manifest{}, false, err
	}
	retiredArtifacts := replacedTailArtifacts(previous.ArtifactReferences, tailReference)
	newTable := memtable.New(0)
	for _, snapshot := range snapshots {
		if err = newTable.SeedTail(snapshot.StreamID, snapshot.Tail); err != nil {
			s.setFatal(err)
			return format.Manifest{}, false, err
		}
	}
	s.viewMu.Lock()
	oldTable := s.state.MemTable
	oldReader := s.reader
	oldTailCatalog := s.state.TailCatalog
	s.state.MemTable = newTable
	s.state.Segments = descriptors
	s.state.TailCatalog = nextTailCatalog
	s.committer = commit.NewWithOptions(s.state.WAL, newTable, s.commitOptions)
	s.reader = readstore.New(newTable, s.root.Path(), published.Header.Generation, s.state.Segments, defaultStreamCacheCapacity, defaultSegmentHandleCapacity)
	closeErr := oldReader.Close()
	if oldTailCatalog != nil {
		closeErr = errors.Join(closeErr, oldTailCatalog.Close())
	}
	s.viewMu.Unlock()
	if closeErr != nil {
		s.setFatal(closeErr)
		return published, true, closeErr
	}
	oldTable.Freeze()
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
	snapshots := table.Snapshot()
	var descriptors []segment.Descriptor
	var tailReference format.ArtifactReference
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
		tailReference, buildErr = s.buildTailReference(generation, coveredEntryID, descriptors, snapshots)
		if buildErr != nil {
			return nil, buildErr
		}
		return []format.ArtifactReference{tailReference}, nil
	})
	if err != nil {
		s.setFatal(err)
		return CompactionResult{}, err
	}
	nextTailCatalog, err := tailstore.OpenCheckpoint(s.root.Path(), tailReference, published.Header.Generation, published.Header.LastEntryID)
	if err != nil {
		s.setFatal(err)
		return CompactionResult{}, err
	}
	retiredArtifacts := replacedTailArtifacts(previous.ArtifactReferences, tailReference)
	nextReader := readstore.New(table, s.root.Path(), published.Header.Generation, descriptors, defaultStreamCacheCapacity, defaultSegmentHandleCapacity)
	s.viewMu.Lock()
	oldReader := s.reader
	oldTailCatalog := s.state.TailCatalog
	s.state.Segments = descriptors
	s.state.TailCatalog = nextTailCatalog
	s.reader = nextReader
	s.viewMu.Unlock()
	err = oldReader.Close()
	if oldTailCatalog != nil {
		err = errors.Join(err, oldTailCatalog.Close())
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

func (s *Store) buildTailReference(generation, coveredEntryID uint64, descriptors []segment.Descriptor, snapshots []memtable.StreamSnapshot) (format.ArtifactReference, error) {
	type latestExtent struct {
		segmentID    format.UUID
		nextSequence uint64
	}
	latest := make(map[uint64]latestExtent)
	for _, descriptor := range descriptors {
		for _, directory := range descriptor.Directories {
			if directory.RecordCount > math.MaxUint64-directory.FirstSequence {
				return format.ArtifactReference{}, fmt.Errorf("Stream %d extent Sequence overflows", directory.StreamID)
			}
			next := directory.FirstSequence + directory.RecordCount
			if current, ok := latest[directory.StreamID]; !ok || next > current.nextSequence {
				latest[directory.StreamID] = latestExtent{segmentID: descriptor.Reference.SegmentID, nextSequence: next}
			}
		}
	}
	slots := make([]format.TailSlot, 0, len(snapshots))
	for _, snapshot := range snapshots {
		extent, ok := latest[snapshot.StreamID]
		if snapshot.Tail.RecordCount > 0 && (!ok || extent.nextSequence != snapshot.Tail.NextSequence) {
			return format.ArtifactReference{}, fmt.Errorf("Stream %d Tail has no matching latest Segment", snapshot.StreamID)
		}
		slots = append(slots, format.TailSlot{Generation: 2, Present: true, StreamID: snapshot.StreamID, NextSequence: snapshot.Tail.NextSequence, NextByteOffset: snapshot.Tail.NextByteOffset, LastRecordedAt: snapshot.Tail.LastRecordedAt, LastEntryID: snapshot.Tail.LastEntryID, AppliedEntryID: coveredEntryID, LatestSegmentID: extent.segmentID})
	}
	return tailstore.WriteNewCheckpoint(s.root.Path(), generation, coveredEntryID, slots)
}

func replacedTailArtifacts(previous []format.ArtifactReference, replacement format.ArtifactReference) []format.ArtifactReference {
	var retired []format.ArtifactReference
	for _, old := range previous {
		if old.ArtifactType == format.ArtifactTailCatalog && old.ArtifactID != replacement.ArtifactID {
			retired = append(retired, old)
		}
	}
	return retired
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
	ch := s.notifications[key]
	if ch == nil {
		ch = make(chan struct{})
		s.notifications[key] = ch
	}
	info, err := s.Inspect(namespace, name)
	if err != nil || (info.Exists && info.NextSequence > after) {
		s.notifyMu.Unlock()
		return err
	}
	s.notifyMu.Unlock()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-ch:
		s.notifyMu.Lock()
		closed := s.closed
		s.notifyMu.Unlock()
		if closed {
			return errdefs.ErrClosed
		}
		return nil
	}
}

func (s *Store) signal(namespace, name string) {
	key := streamKey{namespace: namespace, stream: name}
	s.notifyMu.Lock()
	if !s.closed {
		if ch := s.notifications[key]; ch != nil {
			close(ch)
		}
		s.notifications[key] = make(chan struct{})
	}
	s.notifyMu.Unlock()
}

func (s *Store) closeNotifications() {
	s.notifyMu.Lock()
	if !s.closed {
		s.closed = true
		for key, ch := range s.notifications {
			close(ch)
			delete(s.notifications, key)
		}
	}
	s.notifyMu.Unlock()
}
func (s *Store) commitRegistry(proposal format.RegistryRecord, payload []byte) error {
	tail, ok := s.state.MemTable.Tail(registry.RegistryStreamID)
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
		gate = make(chan struct{}, 1)
		s.appendGates[key] = gate
	}
	s.gateMu.Unlock()
	select {
	case gate <- struct{}{}:
		return func() { <-gate }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
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
