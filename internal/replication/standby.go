package replication

import (
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/akzj/streamd/internal/storage/format"
	"github.com/akzj/streamd/internal/storage/fsutil"
	"github.com/akzj/streamd/internal/storage/identity"
	"github.com/akzj/streamd/internal/storage/recovery"
	"github.com/akzj/streamd/internal/storage/registry"
	"github.com/akzj/streamd/internal/storage/replicationstate"
	"github.com/akzj/streamd/internal/storage/wal"
)

type StandbyStore struct {
	mu       sync.Mutex
	root     *fsutil.Root
	identity format.NodeIdentity
	state    *recovery.Result
	states   *replicationstate.Store
	history  *wal.History
	receiver *Receiver
	now      func() time.Time
	closed   bool
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
	store := &StandbyStore{root: root, identity: node, state: recovered, states: states, history: history, now: time.Now}
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

func (s *StandbyStore) Hello() (ReplicaHello, error) {
	state, err := s.receiver.State()
	if err != nil {
		return ReplicaHello{}, err
	}
	current, _ := s.states.Current()
	return ReplicaHello{GroupID: s.identity.GroupID, NodeID: s.identity.NodeID, KnownTerm: state.Term, InstalledSnapshotID: current.Header.InstalledSnapshotID, Snapshot: fromFormatPosition(current.Header.InstalledSnapshotEntry), LastAppended: state.LastAppended, LocalDurable: state.LocalDurable, Committed: state.Committed, Applied: state.Applied}, nil
}

func (s *StandbyStore) Checkpoint() error {
	state, err := s.receiver.State()
	if err != nil {
		return err
	}
	_, err = s.states.Update(s.now(), func(header *format.ReplicationStateHeader) error {
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
	s.closed = true
	checkpointErr := s.Checkpoint()
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
