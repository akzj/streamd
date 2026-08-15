package replication

import (
	"fmt"
	"math"
	"time"

	"github.com/akzj/streamd/internal/storage/format"
	"github.com/akzj/streamd/internal/storage/fsutil"
	"github.com/akzj/streamd/internal/storage/identity"
	"github.com/akzj/streamd/internal/storage/recovery"
	"github.com/akzj/streamd/internal/storage/registry"
	"github.com/akzj/streamd/internal/storage/replicationstate"
	"github.com/akzj/streamd/internal/storage/wal"
)

type PromotionGrant struct {
	Term         uint64
	LeaderID     format.UUID
	ExpiresAt    time.Time
	Fenced       bool
	SafetyMargin time.Duration
	Now          func() time.Time
}

type PromotionResult struct {
	PreviousTerm  uint64
	Term          uint64
	Committed     Position
	SuffixEntries uint64
}

// Promote validates every physically recovered durable suffix Entry against
// the committed projection before making it public in a higher fenced Term.
func Promote(dataRoot string, node format.NodeIdentity, grant PromotionGrant) (PromotionResult, error) {
	if grant.Now == nil {
		grant.Now = time.Now
	}
	if grant.Term == 0 || grant.LeaderID != node.NodeID || !grant.Fenced || grant.SafetyMargin <= 0 || !grant.Now().Add(grant.SafetyMargin).Before(grant.ExpiresAt) {
		return PromotionResult{}, protocolError(ErrInvalidState, "Promotion requires a safe fenced Lease for this node")
	}
	root, err := fsutil.OpenRoot(dataRoot)
	if err != nil {
		return PromotionResult{}, err
	}
	defer root.Close()
	loaded, err := identity.Load(root.Path())
	if err != nil || loaded.ClusterID != node.ClusterID || loaded.GroupID != node.GroupID || loaded.NodeID != node.NodeID {
		return PromotionResult{}, protocolError(ErrInvalidState, "Promotion NODE identity does not match data root")
	}
	stateStore, err := replicationstate.Open(root.Path(), node)
	if err != nil {
		return PromotionResult{}, err
	}
	current, ok := stateStore.Current()
	if !ok || grant.Term <= current.Header.Term {
		return PromotionResult{}, protocolError(ErrTermStale, "Promotion Term does not advance a non-Primary durable state")
	}
	var committedLimit *uint64
	if current.Header.Committed.Present {
		value := current.Header.Committed.EntryID
		committedLimit = &value
	}
	recovered, err := recovery.OpenWithOptions(root.Path(), recovery.Options{ApplyThrough: committedLimit})
	if err != nil {
		return PromotionResult{}, err
	}
	defer recovered.Close()
	history, err := wal.OpenHistory(root.Path())
	if err != nil {
		return PromotionResult{}, err
	}
	earliest, next, present := history.Bounds()
	start := uint64(0)
	if current.Header.Committed.Present {
		if current.Header.Committed.EntryID == math.MaxUint64 {
			start = math.MaxUint64
		} else {
			start = current.Header.Committed.EntryID + 1
		}
	}
	result := PromotionResult{PreviousTerm: current.Header.Term, Term: grant.Term, Committed: fromFormatPosition(current.Header.Committed)}
	lastPosition := current.Header.Committed
	if present && start < next {
		if start < earliest {
			return PromotionResult{}, protocolError(ErrNeedsSnapshot, "Promotion suffix begins before retained WAL")
		}
		var pending []format.WALEntry
		for entryID := start; entryID < next; {
			rangeResult, readErr := history.ReadRange(entryID, 256, 16<<20)
			if readErr != nil {
				return PromotionResult{}, readErr
			}
			if len(rangeResult.Entries) == 0 {
				return PromotionResult{}, protocolError(ErrLogGap, "Promotion suffix contains a WAL gap")
			}
			for _, encoded := range rangeResult.Entries {
				entry, decodeErr := format.UnmarshalWALEntry(encoded)
				if decodeErr != nil {
					return PromotionResult{}, decodeErr
				}
				if entry.Term >= grant.Term {
					return PromotionResult{}, protocolError(ErrTermStale, "Promotion suffix contains an unfenced future Term")
				}
				if len(pending) == 0 && entry.BatchIndex != 0 {
					return PromotionResult{}, protocolError(ErrInvalidState, "Promotion suffix begins inside an Append Batch")
				}
				pending = append(pending, entry)
				if uint32(len(pending)) == entry.BatchCount {
					if applyErr := recovered.MemTable.ApplyBatch(pending); applyErr != nil {
						return PromotionResult{}, protocolError(ErrLogDiverged, fmt.Sprintf("Promotion suffix violates Stream tails: %v", applyErr))
					}
					for _, applied := range pending {
						if applied.StreamID == registry.RegistryStreamID {
							if applyErr := recovered.Registry.ApplyRecord(applied.EntryID, applied.Record.Payload); applyErr != nil {
								return PromotionResult{}, protocolError(ErrLogDiverged, fmt.Sprintf("Promotion suffix violates Registry: %v", applyErr))
							}
						}
					}
					pending = pending[:0]
				} else if uint32(len(pending)) > entry.BatchCount {
					return PromotionResult{}, protocolError(ErrInvalidState, "Promotion suffix Batch length is invalid")
				}
				lastPosition = format.ReplicationPosition{Present: true, EntryID: entry.EntryID, CRC32C: entry.CRC32C}
				result.SuffixEntries++
			}
			entryID = rangeResult.NextEntryID
		}
		if len(pending) != 0 {
			return PromotionResult{}, protocolError(ErrInvalidState, "Promotion suffix ends inside an Append Batch")
		}
	}
	_, err = stateStore.Update(grant.Now(), func(header *format.ReplicationStateHeader) error {
		header.Term = grant.Term
		header.Role = format.ReplicationRolePrimary
		header.Durability = format.ReplicationDurabilityStrict
		header.HasLeader = true
		header.LeaderID = node.NodeID
		header.HasLease = true
		header.LeaseExpiresAt = grant.ExpiresAt.UnixNano()
		if lastPosition.Present {
			header.LastAppended = lastPosition
			header.LocalDurable = lastPosition
			header.Replicated = lastPosition
			header.Committed = lastPosition
			header.Applied = lastPosition
		}
		return nil
	})
	if err != nil {
		return PromotionResult{}, err
	}
	result.Committed = fromFormatPosition(lastPosition)
	return result, nil
}

func fromFormatPosition(position format.ReplicationPosition) Position {
	return Position{Valid: position.Present, EntryID: position.EntryID, CRC32C: position.CRC32C}
}
