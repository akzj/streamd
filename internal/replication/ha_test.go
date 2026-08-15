package replication

import (
	"context"
	"errors"
	"testing"

	"github.com/akzj/streamd/internal/storage/format"
)

type ackLossPeer struct {
	ReceiverPeer
	dropBarrier bool
	dropCommit  bool
}

func (p *ackLossPeer) Barrier(ctx context.Context, message DurabilityBarrier) (DurableAck, error) {
	ack, err := p.ReceiverPeer.Barrier(ctx, message)
	if err == nil && p.dropBarrier {
		p.dropBarrier = false
		return DurableAck{}, errors.New("injected DurableAck loss")
	}
	return ack, err
}

func (p *ackLossPeer) AdvanceCommit(ctx context.Context, message CommitAdvance) error {
	if p.dropCommit {
		p.dropCommit = false
		return errors.New("injected CommitAdvance loss")
	}
	return p.ReceiverPeer.AdvanceCommit(ctx, message)
}

func TestHADrillLostAckAndCommitAdvanceAreIdempotent(t *testing.T) {
	log := &primaryTestLog{}
	receiver, err := NewReceiver(log, ReceiverConfig{
		GroupID: uuid(1), NodeID: uuid(3), State: ReceiverState{Term: 7, LeaderID: uuid(2)},
		ChecksumAt: func(entryID uint64) (uint32, bool) { entry, ok := log.entryAt(entryID); return entry.CRC32C, ok },
		EntryAt:    log.entryAt, ObserveTerm: func(uint64, format.UUID) error { return nil }, ApplyThrough: func(uint64, uint64) error { return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	peer := &ackLossPeer{ReceiverPeer: ReceiverPeer{Receiver: receiver}, dropBarrier: true, dropCommit: true}
	primary, err := NewPrimary(uuid(1), uuid(2), 7, peer)
	if err != nil {
		t.Fatal(err)
	}
	entries := encodedEntriesFor(t, 7, "ha-drill", 2, 0, 0)
	if _, err = primary.Replicate(context.Background(), entries); err == nil {
		t.Fatal("lost DurableAck was reported as success")
	}
	last, err := primary.Replicate(context.Background(), entries)
	if err != nil || last != 1 || len(log.encoded) != 2 || log.syncs != 1 {
		t.Fatalf("retry last = %d, entries = %d, syncs = %d, error = %v", last, len(log.encoded), log.syncs, err)
	}
	if err = primary.AdvanceCommit(context.Background(), last); err == nil {
		t.Fatal("lost CommitAdvance was reported as delivered")
	}
	if err = primary.AdvanceCommit(context.Background(), last); err != nil {
		t.Fatal(err)
	}
	state, err := receiver.State()
	if err != nil || !state.Committed.Valid || state.Committed.EntryID != 1 || state.Applied.EntryID != 1 {
		t.Fatalf("receiver State = %+v, error = %v", state, err)
	}
}
