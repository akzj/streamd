package replication

import (
	"crypto/sha256"
	"errors"
	"testing"

	"github.com/akzj/streamd/internal/storage/format"
)

type receiverLog struct {
	entries [][]byte
	syncs   int
	syncErr error
}

func (l *receiverLog) Append(entries ...[]byte) error {
	l.entries = append(l.entries, entries...)
	return nil
}

func (l *receiverLog) Sync() error {
	l.syncs++
	return l.syncErr
}

func (l *receiverLog) entryAt(entryID uint64) (format.WALEntry, bool) {
	for _, encoded := range l.entries {
		entry, err := format.UnmarshalWALEntry(encoded)
		if err == nil && entry.EntryID == entryID {
			return entry, true
		}
	}
	return format.WALEntry{}, false
}

func (l *receiverLog) checksumAt(entryID uint64) (uint32, bool) {
	entry, ok := l.entryAt(entryID)
	return entry.CRC32C, ok
}

func TestReceiverAppendIsContinuousAndIdempotent(t *testing.T) {
	log := &receiverLog{}
	receiver := newTestReceiver(t, log, ReceiverState{}, nil)
	entries := encodedEntries(t, 3, 0, 0)
	message := appendMessage(4, Position{}, entries)
	if err := receiver.Append(message); err != nil {
		t.Fatal(err)
	}
	if err := receiver.Append(message); err != nil {
		t.Fatalf("duplicate Append = %v", err)
	}
	state, err := receiver.State()
	if err != nil || len(log.entries) != 3 || !state.LastAppended.Valid || state.LastAppended.EntryID != 2 {
		t.Fatalf("state = %+v, entries = %d, error = %v", state, len(log.entries), err)
	}

	gap := appendMessage(4, position(3, 1), encodedEntries(t, 1, 4, 1))
	if err = receiver.Append(gap); !IsCode(err, ErrLogGap) {
		t.Fatalf("gap error = %v", err)
	}
	diverged := appendMessage(4, Position{}, encodedEntriesFor(t, 4, "different", 1, 0, 0))
	if err = receiver.Append(diverged); !IsCode(err, ErrLogDiverged) {
		t.Fatalf("divergence error = %v", err)
	}
}

func TestReceiverCapacityAdmissionRejectsOnlyNewWALData(t *testing.T) {
	log := &receiverLog{}
	critical := true
	config := testReceiverConfig(ReceiverState{})
	config.ChecksumAt = log.checksumAt
	config.EntryAt = log.entryAt
	config.CanAppend = func() error {
		if critical {
			return errors.New("critical")
		}
		return nil
	}
	receiver, err := NewReceiver(log, config)
	if err != nil {
		t.Fatal(err)
	}
	message := appendMessage(4, Position{}, encodedEntries(t, 1, 0, 0))
	if err = receiver.Append(message); !IsCode(err, ErrCapacityCritical) || len(log.entries) != 0 {
		t.Fatalf("critical Append error=%v entries=%d", err, len(log.entries))
	}
	critical = false
	if err = receiver.Append(message); err != nil {
		t.Fatal(err)
	}
	critical = true
	if err = receiver.Append(message); err != nil {
		t.Fatalf("idempotent Append was rejected at critical capacity: %v", err)
	}
}

func TestReceiverBarrierAndOutOfOrderCommitAdvance(t *testing.T) {
	log := &receiverLog{}
	var applied [][2]uint64
	receiver := newTestReceiver(t, log, ReceiverState{}, func(first, last uint64) error {
		applied = append(applied, [2]uint64{first, last})
		return nil
	})
	entries := encodedEntries(t, 3, 0, 0)
	if err := receiver.Append(appendMessage(4, Position{}, entries)); err != nil {
		t.Fatal(err)
	}
	if err := receiver.AdvanceCommit(commitMessage(4, 2)); err != nil {
		t.Fatal(err)
	}
	state, _ := receiver.State()
	if state.Committed.Valid || len(applied) != 0 {
		t.Fatalf("commit advanced before durability: %+v", state)
	}
	ack, err := receiver.Barrier(barrierMessage(4, 1))
	if err != nil {
		t.Fatal(err)
	}
	if log.syncs != 1 || ack.Durable.EntryID != 2 || len(applied) != 1 || applied[0] != [2]uint64{0, 2} {
		t.Fatalf("ack = %+v, syncs = %d, applied = %+v", ack, log.syncs, applied)
	}
	if _, err = receiver.Barrier(barrierMessage(4, 2)); err != nil || log.syncs != 1 {
		t.Fatalf("idempotent Barrier error = %v, syncs = %d", err, log.syncs)
	}
	if err = receiver.Append(appendMessage(4, Position{}, entries)); err != nil {
		t.Fatalf("durable duplicate Append = %v", err)
	}
}

func TestReceiverRejectsCommitThatSplitsBatch(t *testing.T) {
	receiver := newTestReceiver(t, &receiverLog{}, ReceiverState{}, nil)
	if err := receiver.Append(appendMessage(4, Position{}, encodedEntries(t, 3, 0, 0))); err != nil {
		t.Fatal(err)
	}
	if err := receiver.AdvanceCommit(commitMessage(4, 1)); err != nil {
		t.Fatal(err)
	}
	if _, err := receiver.Barrier(barrierMessage(4, 1)); !IsCode(err, ErrInvalidState) {
		t.Fatalf("split Batch error = %v", err)
	}
	if err := receiver.AdvanceCommit(commitMessage(4, 2)); err != nil {
		t.Fatalf("corrected commit = %v", err)
	}
}

func TestReceiverPersistsNewTermBeforeAcceptingData(t *testing.T) {
	log := &receiverLog{}
	var observed uint64
	config := testReceiverConfig(ReceiverState{})
	config.ChecksumAt = log.checksumAt
	config.EntryAt = log.entryAt
	config.ObserveTerm = func(term uint64, leader format.UUID) error {
		observed = term
		return nil
	}
	receiver, err := NewReceiver(log, config)
	if err != nil {
		t.Fatal(err)
	}
	if err = receiver.Append(appendMessage(5, Position{}, encodedEntriesFor(t, 5, "request", 1, 0, 0))); err != nil {
		t.Fatal(err)
	}
	if observed != 5 {
		t.Fatalf("observed Term = %d", observed)
	}
	if err = receiver.Append(appendMessage(4, Position{}, encodedEntries(t, 1, 0, 0))); !IsCode(err, ErrTermStale) {
		t.Fatalf("stale Term error = %v", err)
	}
}

func TestReceiverAllowsHistoricalEntriesFromAnOlderTerm(t *testing.T) {
	state := ReceiverState{Term: 5, LeaderID: uuid(2)}
	receiver := newTestReceiver(t, &receiverLog{}, state, nil)
	if err := receiver.Append(appendMessage(5, Position{}, encodedEntries(t, 1, 0, 0))); err != nil {
		t.Fatalf("historical Append = %v", err)
	}
}

func TestReceiverDropsUncommittedAdvanceOnNewTerm(t *testing.T) {
	receiver := newTestReceiver(t, &receiverLog{}, ReceiverState{}, nil)
	entries := encodedEntries(t, 3, 0, 0)
	if err := receiver.Append(appendMessage(4, Position{}, entries)); err != nil {
		t.Fatal(err)
	}
	if err := receiver.AdvanceCommit(commitMessage(4, 2)); err != nil {
		t.Fatal(err)
	}
	if err := receiver.Append(appendMessage(5, Position{}, entries)); err != nil {
		t.Fatal(err)
	}
	state, err := receiver.State()
	if err != nil || state.PendingCommit.Valid {
		t.Fatalf("state after new Term = %+v, error = %v", state, err)
	}
}

func TestReceiverSyncFailureFailsClosed(t *testing.T) {
	stop := errors.New("disk stopped")
	log := &receiverLog{syncErr: stop}
	receiver := newTestReceiver(t, log, ReceiverState{}, nil)
	if err := receiver.Append(appendMessage(4, Position{}, encodedEntries(t, 1, 0, 0))); err != nil {
		t.Fatal(err)
	}
	if _, err := receiver.Barrier(barrierMessage(4, 0)); !errors.Is(err, stop) {
		t.Fatalf("Barrier error = %v", err)
	}
	if err := receiver.AdvanceCommit(commitMessage(4, 0)); !errors.Is(err, stop) {
		t.Fatalf("failed Receiver accepted commit: %v", err)
	}
}

func newTestReceiver(t *testing.T, log StandbyLog, state ReceiverState, apply ApplyThrough) *Receiver {
	t.Helper()
	config := testReceiverConfig(state)
	if local, ok := log.(*receiverLog); ok {
		config.ChecksumAt = local.checksumAt
		config.EntryAt = local.entryAt
	}
	if apply != nil {
		config.ApplyThrough = apply
	}
	receiver, err := NewReceiver(log, config)
	if err != nil {
		t.Fatal(err)
	}
	return receiver
}

func testReceiverConfig(state ReceiverState) ReceiverConfig {
	return ReceiverConfig{
		GroupID: uuid(1),
		NodeID:  uuid(3),
		State: func() ReceiverState {
			if state.Term == 0 {
				state.Term = 4
			}
			if state.LeaderID == (format.UUID{}) {
				state.LeaderID = uuid(2)
			}
			return state
		}(),
		ChecksumAt:  func(uint64) (uint32, bool) { return 0, false },
		EntryAt:     func(uint64) (format.WALEntry, bool) { return format.WALEntry{}, false },
		ObserveTerm: func(uint64, format.UUID) error { return nil },
		ApplyThrough: func(uint64, uint64) error {
			return nil
		},
	}
}

func appendMessage(term uint64, previous Position, entries [][]byte) AppendEntries {
	return AppendEntries{GroupID: uuid(1), Term: term, LeaderID: uuid(2), Previous: previous, Entries: entries}
}

func barrierMessage(term, through uint64) DurabilityBarrier {
	return DurabilityBarrier{GroupID: uuid(1), Term: term, LeaderID: uuid(2), ThroughEntryID: through}
}

func commitMessage(term, through uint64) CommitAdvance {
	return CommitAdvance{GroupID: uuid(1), Term: term, LeaderID: uuid(2), CommitEntryID: through}
}

func encodedEntries(t *testing.T, count int, first uint64, previous uint32) [][]byte {
	return encodedEntriesFor(t, 4, "request", count, first, previous)
}

func encodedEntriesFor(t *testing.T, term uint64, request string, count int, first uint64, previous uint32) [][]byte {
	t.Helper()
	entries := make([][]byte, count)
	for i := range entries {
		entryID := first + uint64(i)
		hash := sha256.Sum256([]byte(request))
		frame, err := format.MarshalRecordFrame(format.RecordFrame{EntryID: entryID, StreamID: 1, Sequence: entryID, ByteOffset: uint64(i), BatchIndex: uint32(i), BatchCount: uint32(count), RequestHash: hash, RequestID: []byte(request), Producer: "primary"})
		if err != nil {
			t.Fatal(err)
		}
		entries[i], err = format.MarshalWALEntry(term, previous, frame)
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := format.UnmarshalWALEntry(entries[i])
		if err != nil {
			t.Fatal(err)
		}
		previous = decoded.CRC32C
	}
	return entries
}
