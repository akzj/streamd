// Package replication implements transport-independent primary/standby protocol logic.
package replication

import (
	"errors"
	"fmt"
	"math"

	"github.com/akzj/streamd/internal/storage/format"
)

type PlanMode uint8

const (
	PlanIncremental PlanMode = iota + 1
	PlanSnapshot
)

type ErrorCode string

const (
	ErrInvalidState     ErrorCode = "INVALID_STATE"
	ErrWrongGroup       ErrorCode = "WRONG_GROUP"
	ErrTermStale        ErrorCode = "TERM_STALE"
	ErrNotLeader        ErrorCode = "NOT_LEADER"
	ErrLogGap           ErrorCode = "LOG_GAP"
	ErrLogDiverged      ErrorCode = "LOG_DIVERGED"
	ErrNoRecoverySource ErrorCode = "NO_RECOVERY_SOURCE"
	ErrNeedsSnapshot    ErrorCode = "NEEDS_SNAPSHOT"
	ErrCapacityCritical ErrorCode = "CAPACITY_CRITICAL"
)

type ProtocolError struct {
	Code ErrorCode
	Msg  string
}

func (e *ProtocolError) Error() string { return string(e.Code) + ": " + e.Msg }

func IsCode(err error, code ErrorCode) bool {
	var protocolErr *ProtocolError
	return errors.As(err, &protocolErr) && protocolErr.Code == code
}

// Position represents an Entry ID and its checksum. Valid=false is the empty
// log position; Entry ID zero remains a valid first Entry.
type Position struct {
	Valid   bool
	EntryID uint64
	CRC32C  uint32
}

type ReplicaHello struct {
	GroupID             format.UUID
	NodeID              format.UUID
	KnownTerm           uint64
	InstalledSnapshotID format.UUID
	Snapshot            Position
	LastAppended        Position
	LocalDurable        Position
	Committed           Position
	Applied             Position
}

type InstallableSnapshot struct {
	SnapshotID format.UUID
	Checkpoint Position
}

// PrimaryView is the immutable primary state used for one negotiation.
// ChecksumAt must return the checksum for any retained WAL Entry or installed
// Snapshot checkpoint that can be compared with a Standby prefix.
type PrimaryView struct {
	GroupID      format.UUID
	LeaderID     format.UUID
	Term         uint64
	EarliestWAL  uint64
	LastAppended Position
	LocalDurable Position
	Committed    Position
	Snapshot     *InstallableSnapshot
	ChecksumAt   func(entryID uint64) (uint32, bool)
}

type ReplicationPlan struct {
	Term         uint64
	LeaderID     format.UUID
	Mode         PlanMode
	StartEntryID uint64
	SnapshotID   format.UUID
	Checkpoint   Position
	EarliestWAL  uint64
	Committed    Position
}

func Plan(view PrimaryView, hello ReplicaHello) (ReplicationPlan, error) {
	if zeroUUID(view.GroupID) || zeroUUID(view.LeaderID) || zeroUUID(hello.GroupID) || zeroUUID(hello.NodeID) || view.ChecksumAt == nil {
		return ReplicationPlan{}, protocolError(ErrInvalidState, "replication identities and checksum lookup are required")
	}
	if view.GroupID != hello.GroupID {
		return ReplicationPlan{}, protocolError(ErrWrongGroup, "Standby belongs to a different replication group")
	}
	if hello.NodeID == view.LeaderID {
		return ReplicationPlan{}, protocolError(ErrInvalidState, "Primary and Standby node identities are equal")
	}
	if hello.KnownTerm > view.Term {
		return ReplicationPlan{}, protocolError(ErrTermStale, "Standby knows a newer term")
	}
	if err := validatePrefix("primary", view.LastAppended, view.LocalDurable, view.Committed, Position{}); err != nil {
		return ReplicationPlan{}, err
	}
	if !view.LastAppended.Valid && view.EarliestWAL != 0 {
		return ReplicationPlan{}, protocolError(ErrInvalidState, "empty Primary log has a nonzero earliest WAL Entry")
	}
	if view.LastAppended.Valid && view.LastAppended.EntryID != math.MaxUint64 && view.EarliestWAL > view.LastAppended.EntryID+1 {
		return ReplicationPlan{}, protocolError(ErrInvalidState, "Primary earliest WAL Entry is beyond its log tail")
	}
	if err := validatePrefix("standby", hello.LastAppended, hello.LocalDurable, hello.Committed, hello.Applied); err != nil {
		return ReplicationPlan{}, err
	}
	if err := validateSnapshotState("standby", hello.InstalledSnapshotID, hello.Snapshot, hello.LocalDurable, view.EarliestWAL); err != nil {
		return ReplicationPlan{}, err
	}
	prefixKnown := !hello.LocalDurable.Valid
	if hello.LocalDurable.Valid {
		if !view.LastAppended.Valid || hello.LocalDurable.EntryID > view.LastAppended.EntryID {
			return ReplicationPlan{}, protocolError(ErrLogDiverged, "Standby durable log is ahead of Primary")
		}
		checksum, ok := view.ChecksumAt(hello.LocalDurable.EntryID)
		if ok && checksum != hello.LocalDurable.CRC32C {
			return ReplicationPlan{}, protocolError(ErrLogDiverged, "Standby durable prefix does not match Primary")
		}
		prefixKnown = ok
	}
	if view.Snapshot != nil {
		if err := validateSnapshotState("primary", view.Snapshot.SnapshotID, view.Snapshot.Checkpoint, view.Committed, view.EarliestWAL); err != nil {
			return ReplicationPlan{}, err
		}
		if !prefixKnown && hello.InstalledSnapshotID == view.Snapshot.SnapshotID && hello.LocalDurable == view.Snapshot.Checkpoint {
			prefixKnown = true
		}
	}

	plan := ReplicationPlan{Term: view.Term, LeaderID: view.LeaderID, EarliestWAL: view.EarliestWAL, Committed: view.Committed}
	start, canIncrement := nextEntry(hello.LocalDurable)
	if canIncrement && prefixKnown && start >= view.EarliestWAL {
		plan.Mode = PlanIncremental
		plan.StartEntryID = start
		return plan, nil
	}
	if view.Snapshot == nil || zeroUUID(view.Snapshot.SnapshotID) || !view.Snapshot.Checkpoint.Valid {
		return ReplicationPlan{}, protocolError(ErrNoRecoverySource, "required WAL is not retained and no installable Snapshot exists")
	}
	plan.Mode = PlanSnapshot
	plan.SnapshotID = view.Snapshot.SnapshotID
	plan.Checkpoint = view.Snapshot.Checkpoint
	if view.Snapshot.Checkpoint.EntryID == math.MaxUint64 {
		return ReplicationPlan{}, protocolError(ErrInvalidState, "Snapshot checkpoint cannot be advanced")
	}
	plan.StartEntryID = view.Snapshot.Checkpoint.EntryID + 1
	return plan, nil
}

func validatePrefix(name string, appended, durable, committed, applied Position) error {
	positions := []struct {
		name     string
		position Position
	}{
		{"appended", appended},
		{"durable", durable},
		{"committed", committed},
		{"applied", applied},
	}
	for _, item := range positions {
		if !item.position.Valid && (item.position.EntryID != 0 || item.position.CRC32C != 0) {
			return protocolError(ErrInvalidState, fmt.Sprintf("%s %s empty position contains values", name, item.name))
		}
	}
	for i := 1; i < len(positions); i++ {
		later := positions[i-1]
		earlier := positions[i]
		if earlier.position.Valid && (!later.position.Valid || earlier.position.EntryID > later.position.EntryID) {
			return protocolError(ErrInvalidState, fmt.Sprintf("%s %s position exceeds %s position", name, earlier.name, later.name))
		}
		if earlier.position.Valid && earlier.position.EntryID == later.position.EntryID && earlier.position.CRC32C != later.position.CRC32C {
			return protocolError(ErrInvalidState, fmt.Sprintf("%s positions disagree at Entry %d", name, earlier.position.EntryID))
		}
	}
	return nil
}

func validateSnapshotState(name string, id format.UUID, checkpoint, covered Position, earliestWAL uint64) error {
	if !checkpoint.Valid {
		if !zeroUUID(id) {
			return protocolError(ErrInvalidState, fmt.Sprintf("%s Snapshot identity has no checkpoint", name))
		}
		return nil
	}
	if zeroUUID(id) || !covered.Valid || checkpoint.EntryID > covered.EntryID {
		return protocolError(ErrInvalidState, fmt.Sprintf("%s Snapshot is not covered by its durable or committed position", name))
	}
	if checkpoint.EntryID == covered.EntryID && checkpoint.CRC32C != covered.CRC32C {
		return protocolError(ErrInvalidState, fmt.Sprintf("%s Snapshot checksum disagrees with its covered position", name))
	}
	if name == "primary" {
		if checkpoint.EntryID == math.MaxUint64 || checkpoint.EntryID+1 < earliestWAL {
			return protocolError(ErrInvalidState, "primary Snapshot does not connect to retained WAL")
		}
	}
	return nil
}

func nextEntry(position Position) (uint64, bool) {
	if !position.Valid {
		return 0, true
	}
	if position.EntryID == math.MaxUint64 {
		return 0, false
	}
	return position.EntryID + 1, true
}

func protocolError(code ErrorCode, message string) error {
	return &ProtocolError{Code: code, Msg: message}
}

func zeroUUID(id format.UUID) bool { return id == format.UUID{} }
