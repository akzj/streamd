package replication

import (
	"context"
	"testing"

	"github.com/akzj/streamd/internal/storage/format"
)

type primaryTestLog struct {
	encoded [][]byte
	syncs   int
}

func (l *primaryTestLog) Append(entries ...[]byte) error {
	l.encoded = append(l.encoded, entries...)
	return nil
}

func (l *primaryTestLog) Sync() error {
	l.syncs++
	return nil
}

func (l *primaryTestLog) entryAt(entryID uint64) (format.WALEntry, bool) {
	for _, encoded := range l.encoded {
		entry, err := format.UnmarshalWALEntry(encoded)
		if err == nil && entry.EntryID == entryID {
			return entry, true
		}
	}
	return format.WALEntry{}, false
}

func TestPrimaryReplicatesDurablyAndAdvancesCommit(t *testing.T) {
	log := &primaryTestLog{}
	var applied [][2]uint64
	receiver, err := NewReceiver(log, ReceiverConfig{
		GroupID: uuid(1),
		NodeID:  uuid(3),
		State:   ReceiverState{Term: 7, LeaderID: uuid(2)},
		ChecksumAt: func(entryID uint64) (uint32, bool) {
			entry, ok := log.entryAt(entryID)
			return entry.CRC32C, ok
		},
		EntryAt:     log.entryAt,
		ObserveTerm: func(uint64, format.UUID) error { return nil },
		ApplyThrough: func(first, last uint64) error {
			applied = append(applied, [2]uint64{first, last})
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	primary, err := NewPrimary(uuid(1), uuid(2), 7, ReceiverPeer{Receiver: receiver})
	if err != nil {
		t.Fatal(err)
	}
	entries := encodedEntriesFor(t, 7, "strict", 2, 0, 0)
	last, err := primary.Replicate(context.Background(), entries)
	if err != nil || last != 1 || log.syncs != 1 {
		t.Fatalf("Replicate last = %d, syncs = %d, error = %v", last, log.syncs, err)
	}
	state, err := receiver.State()
	if err != nil || !state.LocalDurable.Valid || state.Committed.Valid {
		t.Fatalf("durable State = %+v, error = %v", state, err)
	}
	if err = primary.AdvanceCommit(context.Background(), last); err != nil {
		t.Fatal(err)
	}
	state, err = receiver.State()
	if err != nil || state.Committed.EntryID != 1 || state.Applied.EntryID != 1 || len(applied) != 1 {
		t.Fatalf("committed State = %+v, applied = %+v, error = %v", state, applied, err)
	}
}

func TestPrimaryRejectsEntryFromFutureTerm(t *testing.T) {
	primary, err := NewPrimary(uuid(1), uuid(2), 7, ReceiverPeer{})
	if err != nil {
		t.Fatal(err)
	}
	_, err = primary.Replicate(context.Background(), encodedEntriesFor(t, 8, "future", 1, 0, 0))
	if !IsCode(err, ErrInvalidState) {
		t.Fatalf("wrong Term error = %v", err)
	}
}
