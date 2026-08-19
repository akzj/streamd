package replication

import (
	"errors"
	"fmt"
	"math"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/akzj/streamd/internal/storage/format"
	"github.com/akzj/streamd/internal/storage/fsutil"
	"github.com/akzj/streamd/internal/storage/identity"
	"github.com/akzj/streamd/internal/storage/lifecycle"
	locatorstore "github.com/akzj/streamd/internal/storage/locator"
	"github.com/akzj/streamd/internal/storage/memtable"
	"github.com/akzj/streamd/internal/storage/projection"
	"github.com/akzj/streamd/internal/storage/recovery"
	"github.com/akzj/streamd/internal/storage/registry"
	"github.com/akzj/streamd/internal/storage/replicationstate"
	"github.com/akzj/streamd/internal/storage/segment"
	tailstore "github.com/akzj/streamd/internal/storage/tail"
	"github.com/akzj/streamd/internal/storage/wal"
)

type StandbyStore struct {
	mu               sync.Mutex
	root             *fsutil.Root
	identity         format.NodeIdentity
	state            *recovery.Result
	states           *replicationstate.Store
	history          *wal.History
	lifecycle        *lifecycle.Manager
	receiver         *Receiver
	now              func() time.Time
	closed           bool
	capacityCritical atomic.Bool
}

type StandbyMaintenanceStats struct {
	MemTableRecords uint64
	MemTableBytes   uint64
	ActiveWALBytes  uint64
}

type StandbyCompactionOptions struct {
	MinSegments      int
	MaxInputSegments int
	MaxInputBytes    uint64
}

type StandbyCompactionResult struct {
	Created       bool
	Generation    uint64
	InputSegments int
	InputBytes    uint64
	LiveSegments  int
}

type standbyLog struct {
	log     *wal.Log
	history *wal.History
}

func (l standbyLog) Append(entries ...[]byte) error {
	if err := l.log.Append(entries...); err != nil {
		return err
	}
	return l.history.ObserveActive(l.log)
}

func (l standbyLog) Sync() error { return l.log.Sync() }

func OpenStandby(path string, node format.NodeIdentity, term uint64, leaderID format.UUID) (*StandbyStore, error) {
	if term == 0 || zeroUUID(leaderID) || leaderID == node.NodeID {
		return nil, protocolError(ErrInvalidState, "Standby Term and external leader are required")
	}
	root, err := fsutil.OpenRoot(path)
	if err != nil {
		return nil, err
	}
	fail := func(err error) (*StandbyStore, error) {
		root.Close()
		return nil, err
	}
	if _, err = identity.Ensure(root.Path(), node); err != nil {
		return fail(err)
	}
	states, err := replicationstate.Open(root.Path(), node)
	if err != nil {
		return fail(err)
	}
	current, hasState := states.Current()
	if hasState {
		if current.Header.Term > term || (current.Header.Role == format.ReplicationRolePrimary && current.Header.Term >= term) {
			return fail(protocolError(ErrTermStale, "durable Standby State cannot open in requested Term"))
		}
		if current.Header.Term == term && (!current.Header.HasLeader || current.Header.LeaderID != leaderID) {
			return fail(protocolError(ErrNotLeader, "durable Standby leader differs in current Term"))
		}
	}
	var committed *uint64
	if hasState && current.Header.Committed.Present {
		value := current.Header.Committed.EntryID
		committed = &value
	}
	recovered, err := recovery.OpenWithOptions(root.Path(), recovery.Options{ApplyThrough: committed})
	if err != nil {
		return fail(err)
	}
	history, err := wal.OpenHistory(root.Path())
	if err != nil {
		recovered.Close()
		return fail(err)
	}
	store := &StandbyStore{root: root, identity: node, state: recovered, states: states, history: history, lifecycle: lifecycle.New(root.Path(), recovered.Manifest), now: time.Now}
	last := Position{}
	if _, next, present := history.Bounds(); present {
		_, entry, lookupErr := history.EntryAt(next - 1)
		if lookupErr != nil {
			recovered.Close()
			return fail(lookupErr)
		}
		last = Position{Valid: true, EntryID: entry.EntryID, CRC32C: entry.CRC32C}
	} else if hasState && current.Header.HasInstalledSnapshot {
		last = fromFormatPosition(current.Header.InstalledSnapshotEntry)
	}
	receiverState := ReceiverState{Term: term, LeaderID: leaderID, LastAppended: last, LocalDurable: last}
	if hasState {
		receiverState.Committed = fromFormatPosition(current.Header.Committed)
		receiverState.Applied = fromFormatPosition(current.Header.Applied)
	}
	lookupChecksum := func(entryID uint64) (uint32, bool) {
		checksum, ok, lookupErr := history.ChecksumAt(entryID)
		if lookupErr == nil && ok {
			return checksum, true
		}
		if hasState && current.Header.HasInstalledSnapshot && current.Header.InstalledSnapshotEntry.EntryID == entryID {
			return current.Header.InstalledSnapshotEntry.CRC32C, true
		}
		return 0, false
	}
	lookupEntry := func(entryID uint64) (format.WALEntry, bool) {
		_, entry, lookupErr := history.EntryAt(entryID)
		return entry, lookupErr == nil
	}
	receiver, err := NewReceiver(standbyLog{log: recovered.WAL, history: history}, ReceiverConfig{
		GroupID: node.GroupID, NodeID: node.NodeID, State: receiverState,
		ChecksumAt: lookupChecksum, EntryAt: lookupEntry,
		ObserveTerm:  store.observeTerm,
		ApplyEntries: store.applyEntries,
		CanAppend: func() error {
			if store.capacityCritical.Load() {
				return fmt.Errorf("storage capacity is critical")
			}
			return nil
		},
	})
	if err != nil {
		recovered.Close()
		return fail(err)
	}
	store.receiver = receiver
	if !hasState || current.Header.Term < term {
		if err = store.Checkpoint(); err != nil {
			store.Close()
			return nil, err
		}
	}
	return store, nil
}

func (s *StandbyStore) Receiver() *Receiver { return s.receiver }

func (s *StandbyStore) SetCapacityCritical(critical bool) { s.capacityCritical.Store(critical) }

func (s *StandbyStore) MaintenanceStats() StandbyMaintenanceStats {
	s.mu.Lock()
	defer s.mu.Unlock()
	records, bytes := s.state.MemTable.Stats()
	walBytes := s.state.WAL.Scan().LastGoodOffset
	if walBytes < 0 {
		walBytes = 0
	}
	return StandbyMaintenanceStats{MemTableRecords: records, MemTableBytes: bytes, ActiveWALBytes: uint64(walBytes)}
}

func (s *StandbyStore) Hello() (ReplicaHello, error) {
	state, err := s.receiver.State()
	if err != nil {
		return ReplicaHello{}, err
	}
	current, _ := s.states.Current()
	return ReplicaHello{GroupID: s.identity.GroupID, NodeID: s.identity.NodeID, KnownTerm: state.Term, InstalledSnapshotID: current.Header.InstalledSnapshotID, Snapshot: fromFormatPosition(current.Header.InstalledSnapshotEntry), LastAppended: state.LastAppended, LocalDurable: state.LocalDurable, Committed: state.Committed, Applied: state.Applied}, nil
}

func (s *StandbyStore) Checkpoint() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return fmt.Errorf("Standby Store is closed")
	}
	return s.checkpointLocked()
}

// Compact replaces a bounded adjacent Segment set while Receiver appends keep
// using the active WAL/MemTable. Only the final projection switch briefly
// enters the Receiver maintenance boundary.
func (s *StandbyStore) Compact(options StandbyCompactionOptions) (StandbyCompactionResult, error) {
	if options.MinSegments < 2 || options.MaxInputSegments < 2 || options.MaxInputBytes == 0 {
		return StandbyCompactionResult{}, fmt.Errorf("invalid Standby Compaction options")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return StandbyCompactionResult{}, fmt.Errorf("Standby Store is closed")
	}
	current, ok := s.state.Manifest.Current()
	if !ok || len(current.SegmentReferences) < options.MinSegments {
		return StandbyCompactionResult{Generation: current.Header.Generation, LiveSegments: len(current.SegmentReferences)}, nil
	}
	selected, inputBytes := selectStandbyCompactionInputs(current.SegmentReferences, options)
	if len(selected) < 2 {
		return StandbyCompactionResult{Generation: current.Header.Generation, LiveSegments: len(current.SegmentReferences)}, nil
	}
	ids := make([]format.UUID, len(selected))
	for i := range selected {
		ids[i] = selected[i].SegmentID
	}
	existing := make(map[format.UUID]segment.Descriptor, len(s.state.Segments))
	for _, descriptor := range s.state.Segments {
		existing[descriptor.Reference.SegmentID] = descriptor
	}
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
		return StandbyCompactionResult{}, s.receiver.fail(err)
	}
	nextTailCatalog, err := tailstore.OpenCheckpoint(s.root.Path(), projections.TailReference, published.Header.Generation, published.Header.LastEntryID)
	if err != nil {
		return StandbyCompactionResult{}, s.receiver.fail(err)
	}
	nextLocator, err := locatorstore.Open(s.root.Path(), published, 256)
	if err != nil {
		nextTailCatalog.Close()
		return StandbyCompactionResult{}, s.receiver.fail(err)
	}
	nextRegistryStore, err := registry.OpenCheckpoint(s.root.Path(), projections.RegistryReference, published.Header.LastEntryID, 64)
	if err != nil {
		nextTailCatalog.Close()
		nextLocator.Close()
		return StandbyCompactionResult{}, s.receiver.fail(err)
	}
	nextRegistry := registry.NewWithSnapshot(nextRegistryStore)
	lightDescriptors := segment.LightDescriptors(descriptors)
	nextRegistry.SetFallback(func() ([]registry.Mapping, error) {
		return registry.RebuildMappings(s.root.Path(), lightDescriptors)
	})
	retiredArtifacts := projection.ReplacedArtifacts(previous.ArtifactReferences, s.state.Locator, projections)
	var oldTailCatalog *tailstore.Catalog
	var oldLocator *locatorstore.Store
	err = s.receiver.maintain(func(ReceiverState) error {
		for _, mapping := range s.state.Registry.MappingsAfter(published.Header.LastEntryID) {
			if applyErr := nextRegistry.ApplyMapping(mapping); applyErr != nil {
				return applyErr
			}
		}
		oldTailCatalog, oldLocator = s.state.TailCatalog, s.state.Locator
		s.state.Segments = lightDescriptors
		s.state.TailCatalog = nextTailCatalog
		s.state.TailResolver = tailstore.NewResolver(s.state.MemTable, nextTailCatalog, s.root.Path(), lightDescriptors, 1024)
		s.state.Locator = nextLocator
		s.state.Registry = nextRegistry
		return nil
	})
	if err != nil {
		nextTailCatalog.Close()
		nextLocator.Close()
		return StandbyCompactionResult{}, s.receiver.fail(err)
	}
	if oldTailCatalog != nil {
		err = oldTailCatalog.Close()
	}
	if oldLocator != nil {
		err = errors.Join(err, oldLocator.Close())
	}
	if err == nil {
		err = s.lifecycle.Retire(retained)
	}
	if err == nil {
		err = s.lifecycle.RetireArtifacts(retiredArtifacts)
	}
	if err != nil {
		return StandbyCompactionResult{}, s.receiver.fail(err)
	}
	return StandbyCompactionResult{Created: true, Generation: published.Header.Generation, InputSegments: len(selected), InputBytes: inputBytes, LiveSegments: len(published.SegmentReferences)}, nil
}

func selectStandbyCompactionInputs(references []format.SegmentReference, options StandbyCompactionOptions) ([]format.SegmentReference, uint64) {
	ordered := append([]format.SegmentReference(nil), references...)
	slices.SortFunc(ordered, func(a, b format.SegmentReference) int {
		if a.FirstEntryID < b.FirstEntryID {
			return -1
		}
		if a.FirstEntryID > b.FirstEntryID {
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

func (s *StandbyStore) checkpointLocked() error {
	type frozenCheckpoint struct {
		table       *memtable.Table
		position    Position
		previous    format.Manifest
		descriptors []segment.Descriptor
	}
	var frozen *frozenCheckpoint
	err := s.receiver.maintain(func(state ReceiverState) error {
		if err := s.checkpointState(state); err != nil {
			return err
		}
		records, _ := s.state.MemTable.Stats()
		if records == 0 {
			return nil
		}
		if !state.Applied.Valid || state.Applied != state.Committed {
			return fmt.Errorf("Standby applied watermark is not a committed checkpoint")
		}
		oldTable := s.state.MemTable
		oldTable.Freeze()
		active := memtable.New(0)
		for _, snapshot := range oldTable.Tails() {
			if err := active.SeedTail(snapshot.StreamID, snapshot.Tail); err != nil {
				return err
			}
		}
		previous, _ := s.state.Manifest.Current()
		frozen = &frozenCheckpoint{table: oldTable, position: state.Applied, previous: previous, descriptors: append([]segment.Descriptor(nil), s.state.Segments...)}
		s.state.MemTable = active
		s.state.TailResolver = tailstore.NewResolver(active, s.state.TailCatalog, s.root.Path(), s.state.Segments, 1024)
		if err := s.state.WAL.Rotate(state.Term, s.now()); err != nil {
			return err
		}
		return s.history.Refresh()
	})
	if err != nil || frozen == nil {
		return err
	}

	snapshots := frozen.table.Snapshot()
	flush := make([]memtable.StreamSnapshot, 0, len(snapshots))
	for _, snapshot := range snapshots {
		if len(snapshot.Frames) > 0 {
			flush = append(flush, snapshot)
		}
	}
	if len(flush) == 0 {
		return s.receiver.fail(fmt.Errorf("Standby checkpoint froze no committed Records"))
	}
	existing := make(map[format.UUID]segment.Descriptor, len(frozen.descriptors))
	for _, descriptor := range frozen.descriptors {
		existing[descriptor.Reference.SegmentID] = descriptor
	}
	var descriptors []segment.Descriptor
	var projections projection.Build
	published, err := s.lifecycle.PublishFlushWithArtifacts(flush, frozen.position.EntryID, frozen.position.CRC32C, func(generation uint64, references []format.SegmentReference, coveredEntryID uint64) ([]format.ArtifactReference, error) {
		descriptors = append([]segment.Descriptor(nil), frozen.descriptors...)
		for _, reference := range references {
			if _, found := existing[reference.SegmentID]; found {
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
		return s.receiver.fail(err)
	}
	nextTailCatalog, err := tailstore.OpenCheckpoint(s.root.Path(), projections.TailReference, published.Header.Generation, published.Header.LastEntryID)
	if err != nil {
		return s.receiver.fail(err)
	}
	nextLocator, err := locatorstore.Open(s.root.Path(), published, 256)
	if err != nil {
		nextTailCatalog.Close()
		return s.receiver.fail(err)
	}
	nextRegistryStore, err := registry.OpenCheckpoint(s.root.Path(), projections.RegistryReference, published.Header.LastEntryID, 64)
	if err != nil {
		nextTailCatalog.Close()
		nextLocator.Close()
		return s.receiver.fail(err)
	}
	nextRegistry := registry.NewWithSnapshot(nextRegistryStore)
	fallbackDescriptors := segment.LightDescriptors(descriptors)
	nextRegistry.SetFallback(func() ([]registry.Mapping, error) {
		return registry.RebuildMappings(s.root.Path(), fallbackDescriptors)
	})
	retiredArtifacts := projection.ReplacedArtifacts(frozen.previous.ArtifactReferences, s.state.Locator, projections)
	var oldTailCatalog *tailstore.Catalog
	var oldLocator *locatorstore.Store
	err = s.receiver.maintain(func(state ReceiverState) error {
		for _, mapping := range s.state.Registry.MappingsAfter(published.Header.LastEntryID) {
			if err := nextRegistry.ApplyMapping(mapping); err != nil {
				return err
			}
		}
		oldTailCatalog, oldLocator = s.state.TailCatalog, s.state.Locator
		s.state.Segments = segment.LightDescriptors(descriptors)
		s.state.TailCatalog = nextTailCatalog
		s.state.TailResolver = tailstore.NewResolver(s.state.MemTable, nextTailCatalog, s.root.Path(), s.state.Segments, 1024)
		s.state.Locator = nextLocator
		s.state.Registry = nextRegistry
		s.state.MemTable.PruneSeeded()
		return nil
	})
	if err != nil {
		nextTailCatalog.Close()
		nextLocator.Close()
		return s.receiver.fail(err)
	}
	if oldTailCatalog != nil {
		err = oldTailCatalog.Close()
	}
	if oldLocator != nil {
		err = errors.Join(err, oldLocator.Close())
	}
	if err != nil {
		return s.receiver.fail(err)
	}
	if err = s.lifecycle.RetireArtifacts(retiredArtifacts); err != nil {
		return s.receiver.fail(err)
	}
	return nil
}

func (s *StandbyStore) checkpointState(state ReceiverState) error {
	_, err := s.states.Update(s.now(), func(header *format.ReplicationStateHeader) error {
		header.Term = state.Term
		header.Role = format.ReplicationRoleStandby
		header.Durability = format.ReplicationDurabilityStrict
		header.HasLeader = true
		header.LeaderID = state.LeaderID
		header.HasLease = false
		header.LeaseExpiresAt = 0
		header.LastAppended = toFormatPosition(state.LastAppended)
		header.LocalDurable = toFormatPosition(state.LocalDurable)
		header.Replicated = format.ReplicationPosition{}
		header.Committed = toFormatPosition(state.Committed)
		header.Applied = toFormatPosition(state.Applied)
		if earliest, _, present := s.history.Bounds(); present {
			header.EarliestWALEntryID = earliest
		} else if state.LocalDurable.Valid && state.LocalDurable.EntryID < math.MaxUint64 {
			header.EarliestWALEntryID = state.LocalDurable.EntryID + 1
		}
		return nil
	})
	return err
}

func (s *StandbyStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	checkpointErr := s.checkpointLocked()
	s.closed = true
	return errors.Join(checkpointErr, s.state.Close(), s.root.Close())
}

func (s *StandbyStore) observeTerm(term uint64, leaderID format.UUID) error {
	_, err := s.states.Update(s.now(), func(header *format.ReplicationStateHeader) error {
		header.Term = term
		header.Role = format.ReplicationRoleStandby
		header.Durability = format.ReplicationDurabilityStrict
		header.HasLeader = true
		header.LeaderID = leaderID
		header.HasLease = false
		header.LeaseExpiresAt = 0
		header.Replicated = format.ReplicationPosition{}
		return nil
	})
	return err
}

func (s *StandbyStore) applyEntries(entries []format.WALEntry) error {
	for len(entries) > 0 {
		count := int(entries[0].BatchCount)
		if count <= 0 || count > len(entries) {
			return fmt.Errorf("Standby Apply received partial Batch")
		}
		batch := entries[:count]
		if _, found, err := s.state.TailResolver.EnsureActive(batch[0].StreamID); err != nil {
			return err
		} else if !found && (batch[0].Sequence != 0 || batch[0].ByteOffset != 0) {
			return fmt.Errorf("Standby WAL Stream %d has no checkpoint Tail", batch[0].StreamID)
		}
		if err := s.state.MemTable.ApplyBatch(batch); err != nil {
			return err
		}
		for _, entry := range batch {
			if entry.StreamID == registry.RegistryStreamID {
				if err := s.state.Registry.ApplyRecord(entry.EntryID, entry.Record.Payload); err != nil {
					return err
				}
			}
		}
		entries = entries[count:]
	}
	return nil
}

func toFormatPosition(position Position) format.ReplicationPosition {
	return format.ReplicationPosition{Present: position.Valid, EntryID: position.EntryID, CRC32C: position.CRC32C}
}
