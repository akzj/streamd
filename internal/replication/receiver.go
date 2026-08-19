package replication

import (
	"fmt"
	"math"
	"sync"

	"github.com/akzj/streamd/internal/storage/format"
)

type StandbyLog interface {
	Append(...[]byte) error
	Sync() error
}

// ChecksumLookup and EntryLookup must read the local durable history and must
// observe Entries after StandbyLog.Append returns. Receiver only retains the
// unapplied suffix in memory.
type ChecksumLookup func(entryID uint64) (uint32, bool)
type EntryLookup func(entryID uint64) (format.WALEntry, bool)
type TermObserver func(term uint64, leaderID format.UUID) error
type ApplyThrough func(firstEntryID, lastEntryID uint64) error
type ApplyEntries func([]format.WALEntry) error
type AppendAdmission func() error

type ReceiverState struct {
	Term          uint64
	LeaderID      format.UUID
	LastAppended  Position
	LocalDurable  Position
	Committed     Position
	Applied       Position
	PendingCommit Position
}

type ReceiverConfig struct {
	GroupID      format.UUID
	NodeID       format.UUID
	State        ReceiverState
	ChecksumAt   ChecksumLookup
	EntryAt      EntryLookup
	ObserveTerm  TermObserver
	ApplyThrough ApplyThrough
	ApplyEntries ApplyEntries
	CanAppend    AppendAdmission
}

type Receiver struct {
	mu           sync.Mutex
	groupID      format.UUID
	nodeID       format.UUID
	log          StandbyLog
	checksumAt   ChecksumLookup
	entryAt      EntryLookup
	observeTerm  TermObserver
	applyThrough ApplyThrough
	applyEntries ApplyEntries
	canAppend    AppendAdmission
	checksums    map[uint64]uint32
	entries      map[uint64]format.WALEntry
	state        ReceiverState
	fatal        error
}

type AppendEntries struct {
	GroupID  format.UUID
	Term     uint64
	LeaderID format.UUID
	Previous Position
	Entries  [][]byte
}

type DurabilityBarrier struct {
	GroupID        format.UUID
	Term           uint64
	LeaderID       format.UUID
	ThroughEntryID uint64
}

type DurableAck struct {
	Term    uint64
	Durable Position
}

type CommitAdvance struct {
	GroupID       format.UUID
	Term          uint64
	LeaderID      format.UUID
	CommitEntryID uint64
}

func NewReceiver(log StandbyLog, config ReceiverConfig) (*Receiver, error) {
	if log == nil || zeroUUID(config.GroupID) || zeroUUID(config.NodeID) || zeroUUID(config.State.LeaderID) || config.ChecksumAt == nil || config.EntryAt == nil || config.ObserveTerm == nil || (config.ApplyThrough == nil && config.ApplyEntries == nil) {
		return nil, protocolError(ErrInvalidState, "Standby log, identities, and protocol callbacks are required")
	}
	if config.NodeID == config.State.LeaderID {
		return nil, protocolError(ErrInvalidState, "Standby and leader identities are equal")
	}
	if err := validatePrefix("standby", config.State.LastAppended, config.State.LocalDurable, config.State.Committed, config.State.Applied); err != nil {
		return nil, err
	}
	if config.State.PendingCommit.Valid && config.State.Committed.Valid && config.State.PendingCommit.EntryID < config.State.Committed.EntryID {
		return nil, protocolError(ErrInvalidState, "pending commit is behind committed state")
	}
	for _, position := range []Position{config.State.LastAppended, config.State.LocalDurable, config.State.Committed, config.State.Applied} {
		if position.Valid {
			checksum, ok := config.ChecksumAt(position.EntryID)
			if !ok || checksum != position.CRC32C {
				return nil, protocolError(ErrInvalidState, "Standby state does not match its local log")
			}
		}
	}
	return &Receiver{groupID: config.GroupID, nodeID: config.NodeID, log: log, checksumAt: config.ChecksumAt, entryAt: config.EntryAt, observeTerm: config.ObserveTerm, applyThrough: config.ApplyThrough, applyEntries: config.ApplyEntries, canAppend: config.CanAppend, checksums: make(map[uint64]uint32), entries: make(map[uint64]format.WALEntry), state: config.State}, nil
}

func (r *Receiver) State() (ReceiverState, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.state, r.fatal
}

func (r *Receiver) maintain(operation func(ReceiverState) error) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.fatal != nil {
		return r.fatal
	}
	if operation == nil {
		return protocolError(ErrInvalidState, "Receiver maintenance operation is required")
	}
	if err := operation(r.state); err != nil {
		r.fatal = fmt.Errorf("Standby maintenance failed: %w", err)
		return r.fatal
	}
	return nil
}

func (r *Receiver) fail(err error) error {
	if err == nil {
		return nil
	}
	r.mu.Lock()
	if r.fatal == nil {
		r.fatal = fmt.Errorf("Standby maintenance failed: %w", err)
	}
	fatal := r.fatal
	r.mu.Unlock()
	return fatal
}

func (r *Receiver) Append(message AppendEntries) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.acceptSender(message.GroupID, message.Term, message.LeaderID); err != nil {
		return err
	}
	if len(message.Entries) == 0 {
		return protocolError(ErrInvalidState, "AppendEntries contains no Entries")
	}
	decoded := make([]format.WALEntry, len(message.Entries))
	for i, encoded := range message.Entries {
		entry, err := format.UnmarshalWALEntry(encoded)
		if err != nil {
			return protocolError(ErrInvalidState, fmt.Sprintf("Entry %d is invalid: %v", i, err))
		}
		if entry.Term > message.Term {
			return protocolError(ErrInvalidState, fmt.Sprintf("Entry %d belongs to future Term %d", entry.EntryID, entry.Term))
		}
		if i == 0 {
			if !matchesPrevious(message.Previous, entry) {
				return protocolError(ErrLogGap, "AppendEntries previous position does not connect to its first Entry")
			}
		} else if entry.EntryID != decoded[i-1].EntryID+1 || entry.PreviousEntryCRC32C != decoded[i-1].CRC32C || entry.Term < decoded[i-1].Term {
			return protocolError(ErrLogGap, fmt.Sprintf("Entries are not continuous at Entry %d", entry.EntryID))
		}
		decoded[i] = entry
	}

	first := decoded[0].EntryID
	next, canAppend := nextEntry(r.state.LastAppended)
	if !canAppend {
		return protocolError(ErrLogGap, "local Entry ID space is exhausted")
	}
	if first > next {
		return protocolError(ErrLogGap, fmt.Sprintf("AppendEntries starts at %d, local next Entry is %d", first, next))
	}
	if first == next && message.Previous != r.state.LastAppended {
		return protocolError(ErrLogDiverged, "AppendEntries previous position differs from local tail")
	}

	appendFrom := len(decoded)
	for i, entry := range decoded {
		if entry.EntryID >= next {
			appendFrom = i
			break
		}
		checksum, ok := r.lookupChecksum(entry.EntryID)
		if !ok || checksum != entry.CRC32C {
			return protocolError(ErrLogDiverged, fmt.Sprintf("Entry %d differs from local log", entry.EntryID))
		}
	}
	if appendFrom == len(decoded) {
		return nil
	}
	if decoded[appendFrom].EntryID != next {
		return protocolError(ErrLogGap, "AppendEntries overlap does not connect to local tail")
	}
	if r.canAppend != nil {
		if err := r.canAppend(); err != nil {
			return protocolError(ErrCapacityCritical, "Standby storage capacity is critical")
		}
	}
	if err := r.log.Append(message.Entries[appendFrom:]...); err != nil {
		r.fatal = fmt.Errorf("append Standby WAL: %w", err)
		return r.fatal
	}
	for _, entry := range decoded[appendFrom:] {
		r.checksums[entry.EntryID] = entry.CRC32C
		r.entries[entry.EntryID] = entry
	}
	last := decoded[len(decoded)-1]
	r.state.LastAppended = Position{Valid: true, EntryID: last.EntryID, CRC32C: last.CRC32C}
	return nil
}

func (r *Receiver) Barrier(message DurabilityBarrier) (DurableAck, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.acceptSender(message.GroupID, message.Term, message.LeaderID); err != nil {
		return DurableAck{}, err
	}
	if !r.state.LastAppended.Valid || message.ThroughEntryID > r.state.LastAppended.EntryID {
		return DurableAck{}, protocolError(ErrLogGap, "Durability Barrier exceeds the local log tail")
	}
	if !r.state.LocalDurable.Valid || message.ThroughEntryID > r.state.LocalDurable.EntryID {
		if err := r.log.Sync(); err != nil {
			r.fatal = fmt.Errorf("sync Standby WAL: %w", err)
			return DurableAck{}, r.fatal
		}
		r.state.LocalDurable = r.state.LastAppended
		if err := r.advanceCommitted(); err != nil {
			return DurableAck{}, err
		}
	}
	return DurableAck{Term: r.state.Term, Durable: r.state.LocalDurable}, nil
}

func (r *Receiver) AdvanceCommit(message CommitAdvance) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.acceptSender(message.GroupID, message.Term, message.LeaderID); err != nil {
		return err
	}
	if !r.state.PendingCommit.Valid || message.CommitEntryID > r.state.PendingCommit.EntryID {
		r.state.PendingCommit = Position{Valid: true, EntryID: message.CommitEntryID}
	}
	return r.advanceCommitted()
}

func (r *Receiver) advanceCommitted() error {
	if !r.state.PendingCommit.Valid || !r.state.LocalDurable.Valid {
		return nil
	}
	target := min(r.state.PendingCommit.EntryID, r.state.LocalDurable.EntryID)
	if r.state.Committed.Valid && target <= r.state.Committed.EntryID {
		return nil
	}
	checksum, ok := r.lookupChecksum(target)
	if !ok {
		r.fatal = fmt.Errorf("committed Entry %d checksum is unavailable", target)
		return r.fatal
	}
	entry, ok := r.lookupEntry(target)
	if !ok {
		r.fatal = fmt.Errorf("committed Entry %d metadata is unavailable", target)
		return r.fatal
	}
	if entry.BatchIndex+1 != entry.BatchCount {
		return protocolError(ErrInvalidState, fmt.Sprintf("commit Entry %d splits an Append Batch", target))
	}
	first := uint64(0)
	if r.state.Applied.Valid {
		if r.state.Applied.EntryID == math.MaxUint64 {
			return nil
		}
		first = r.state.Applied.EntryID + 1
	}
	r.state.Committed = Position{Valid: true, EntryID: target, CRC32C: checksum}
	if first <= target {
		var applyErr error
		if r.applyEntries != nil {
			entries := make([]format.WALEntry, 0)
			for entryID := first; entryID <= target; entryID++ {
				entry, found := r.lookupEntry(entryID)
				if !found {
					applyErr = fmt.Errorf("committed Entry %d metadata is unavailable", entryID)
					break
				}
				entries = append(entries, entry)
				if entryID == math.MaxUint64 {
					break
				}
			}
			if applyErr == nil {
				applyErr = r.applyEntries(entries)
			}
		} else {
			applyErr = r.applyThrough(first, target)
		}
		if applyErr != nil {
			r.fatal = fmt.Errorf("apply committed Standby WAL: %w", applyErr)
			return r.fatal
		}
		r.state.Applied = r.state.Committed
		for entryID := range r.checksums {
			if entryID <= target {
				delete(r.checksums, entryID)
				delete(r.entries, entryID)
			}
		}
	}
	return nil
}

func (r *Receiver) acceptSender(groupID format.UUID, term uint64, leaderID format.UUID) error {
	if r.fatal != nil {
		return r.fatal
	}
	if groupID != r.groupID {
		return protocolError(ErrWrongGroup, "replication message belongs to a different group")
	}
	if zeroUUID(leaderID) || leaderID == r.nodeID {
		return protocolError(ErrInvalidState, "replication message leader identity is invalid")
	}
	if term < r.state.Term {
		return protocolError(ErrTermStale, fmt.Sprintf("message Term %d is older than local Term %d", term, r.state.Term))
	}
	if term == r.state.Term {
		if leaderID != r.state.LeaderID {
			return protocolError(ErrNotLeader, "message leader differs within the current Term")
		}
		return nil
	}
	if err := r.observeTerm(term, leaderID); err != nil {
		return fmt.Errorf("persist newer replication Term: %w", err)
	}
	r.state.Term = term
	r.state.LeaderID = leaderID
	r.state.PendingCommit = r.state.Committed
	return nil
}

func (r *Receiver) lookupChecksum(entryID uint64) (uint32, bool) {
	if checksum, ok := r.checksums[entryID]; ok {
		return checksum, true
	}
	return r.checksumAt(entryID)
}

func (r *Receiver) lookupEntry(entryID uint64) (format.WALEntry, bool) {
	if entry, ok := r.entries[entryID]; ok {
		return entry, true
	}
	return r.entryAt(entryID)
}

func matchesPrevious(previous Position, entry format.WALEntry) bool {
	if entry.EntryID == 0 {
		return !previous.Valid && entry.PreviousEntryCRC32C == 0
	}
	return previous.Valid && previous.EntryID == entry.EntryID-1 && previous.CRC32C == entry.PreviousEntryCRC32C
}
