package replication

import (
	"context"
	"fmt"
	"sync"

	"github.com/akzj/streamd/internal/storage/format"
)

type PrimaryPeer interface {
	Append(context.Context, AppendEntries) error
	Barrier(context.Context, DurabilityBarrier) (DurableAck, error)
	AdvanceCommit(context.Context, CommitAdvance) error
}

type Primary struct {
	mu       sync.Mutex
	groupID  format.UUID
	leaderID format.UUID
	term     uint64
	peer     PrimaryPeer
}

func NewPrimary(groupID, leaderID format.UUID, term uint64, peer PrimaryPeer) (*Primary, error) {
	if zeroUUID(groupID) || zeroUUID(leaderID) || peer == nil {
		return nil, protocolError(ErrInvalidState, "Primary group, leader, and peer are required")
	}
	return &Primary{groupID: groupID, leaderID: leaderID, term: term, peer: peer}, nil
}

// Replicate sends a continuous group and returns only after the Standby has
// durably acknowledged its final Entry.
func (p *Primary) Replicate(ctx context.Context, encoded [][]byte) (uint64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if len(encoded) == 0 {
		return 0, protocolError(ErrInvalidState, "cannot replicate an empty Entry group")
	}
	entries := make([]format.WALEntry, len(encoded))
	for i, data := range encoded {
		entry, err := format.UnmarshalWALEntry(data)
		if err != nil {
			return 0, protocolError(ErrInvalidState, fmt.Sprintf("replicated Entry %d is invalid: %v", i, err))
		}
		if entry.Term > p.term {
			return 0, protocolError(ErrInvalidState, fmt.Sprintf("Entry %d belongs to future Term %d", entry.EntryID, entry.Term))
		}
		if i > 0 && (entry.EntryID != entries[i-1].EntryID+1 || entry.PreviousEntryCRC32C != entries[i-1].CRC32C || entry.Term < entries[i-1].Term) {
			return 0, protocolError(ErrLogGap, "Primary replication group is not continuous")
		}
		entries[i] = entry
	}
	first := entries[0]
	previous := Position{}
	if first.EntryID > 0 {
		previous = Position{Valid: true, EntryID: first.EntryID - 1, CRC32C: first.PreviousEntryCRC32C}
	}
	if err := p.peer.Append(ctx, AppendEntries{GroupID: p.groupID, Term: p.term, LeaderID: p.leaderID, Previous: previous, Entries: encoded}); err != nil {
		return 0, err
	}
	last := entries[len(entries)-1]
	ack, err := p.peer.Barrier(ctx, DurabilityBarrier{GroupID: p.groupID, Term: p.term, LeaderID: p.leaderID, ThroughEntryID: last.EntryID})
	if err != nil {
		return 0, err
	}
	if ack.Term != p.term || !ack.Durable.Valid || ack.Durable.EntryID != last.EntryID || ack.Durable.CRC32C != last.CRC32C {
		return 0, protocolError(ErrLogDiverged, "Standby DurableAck does not match replicated group")
	}
	return last.EntryID, nil
}

func (p *Primary) AdvanceCommit(ctx context.Context, entryID uint64) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	return p.peer.AdvanceCommit(ctx, CommitAdvance{GroupID: p.groupID, Term: p.term, LeaderID: p.leaderID, CommitEntryID: entryID})
}

type ReceiverPeer struct {
	Receiver *Receiver
}

func (p ReceiverPeer) Append(ctx context.Context, message AppendEntries) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if p.Receiver == nil {
		return protocolError(ErrInvalidState, "Standby Receiver is unavailable")
	}
	return p.Receiver.Append(message)
}

func (p ReceiverPeer) Barrier(ctx context.Context, message DurabilityBarrier) (DurableAck, error) {
	if err := ctx.Err(); err != nil {
		return DurableAck{}, err
	}
	if p.Receiver == nil {
		return DurableAck{}, protocolError(ErrInvalidState, "Standby Receiver is unavailable")
	}
	return p.Receiver.Barrier(message)
}

func (p ReceiverPeer) AdvanceCommit(ctx context.Context, message CommitAdvance) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if p.Receiver == nil {
		return protocolError(ErrInvalidState, "Standby Receiver is unavailable")
	}
	return p.Receiver.AdvanceCommit(message)
}
