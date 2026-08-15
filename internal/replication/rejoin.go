package replication

import "github.com/akzj/streamd/internal/storage/format"

type RejoinMode uint8

const (
	RejoinIncremental RejoinMode = iota + 1
	RejoinSnapshot
)

type RejoinView struct {
	GroupID     format.UUID
	NodeID      format.UUID
	Term        uint64
	EarliestWAL uint64
	LastDurable Position
	Committed   Position
	ChecksumAt  func(uint64) (uint32, bool)
}

type RejoinDecision struct {
	Mode               RejoinMode
	StartEntryID       uint64
	DiscardLocalSuffix bool
}

// ResolveRejoin never guesses between conflicting committed histories. An
// uncommitted divergent suffix takes the conservative Snapshot path; exact
// prefixes use incremental catch-up.
func ResolveRejoin(local, leader RejoinView) (RejoinDecision, error) {
	if zeroUUID(local.GroupID) || local.GroupID != leader.GroupID || local.NodeID == leader.NodeID || local.ChecksumAt == nil || leader.ChecksumAt == nil {
		return RejoinDecision{}, protocolError(ErrInvalidState, "Rejoin views or identities are invalid")
	}
	if local.Term >= leader.Term {
		return RejoinDecision{}, protocolError(ErrTermStale, "rejoining node has not observed a higher leader Term")
	}
	if local.Committed.Valid {
		checksum, ok := leader.ChecksumAt(local.Committed.EntryID)
		if !ok {
			if local.Committed.EntryID < leader.EarliestWAL {
				return RejoinDecision{Mode: RejoinSnapshot, DiscardLocalSuffix: true}, nil
			}
			return RejoinDecision{}, protocolError(ErrLogGap, "leader cannot verify rejoining committed prefix")
		}
		if checksum != local.Committed.CRC32C {
			return RejoinDecision{}, protocolError(ErrLogDiverged, "committed histories conflict")
		}
	}
	if local.Committed.Valid && leader.Committed.Valid && local.Committed.EntryID > leader.Committed.EntryID {
		return RejoinDecision{}, protocolError(ErrLogDiverged, "rejoining node has a committed prefix ahead of leader")
	}
	start := uint64(0)
	if local.LastDurable.Valid {
		checksum, ok := leader.ChecksumAt(local.LastDurable.EntryID)
		if !ok || checksum != local.LastDurable.CRC32C {
			return RejoinDecision{Mode: RejoinSnapshot, DiscardLocalSuffix: true}, nil
		}
		if local.LastDurable.EntryID == ^uint64(0) {
			return RejoinDecision{}, protocolError(ErrInvalidState, "rejoining WAL exhausted Entry ID space")
		}
		start = local.LastDurable.EntryID + 1
	}
	if start < leader.EarliestWAL {
		return RejoinDecision{Mode: RejoinSnapshot, DiscardLocalSuffix: true}, nil
	}
	return RejoinDecision{Mode: RejoinIncremental, StartEntryID: start}, nil
}
