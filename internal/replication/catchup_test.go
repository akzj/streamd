package replication

import (
	"context"
	"testing"

	"github.com/akzj/streamd/internal/storage/format"
	"github.com/akzj/streamd/internal/storage/wal"
)

type catchUpHistory struct {
	entries  [][]byte
	pinned   bool
	released bool
}

func (h *catchUpHistory) PinRange(uint64, uint64) (func(), error) {
	h.pinned = true
	return func() { h.released = true }, nil
}

func (h *catchUpHistory) ReadRange(from uint64, maxEntries int, maxBytes uint64) (wal.HistoryRange, error) {
	result := wal.HistoryRange{NextEntryID: from}
	var used uint64
	for _, encoded := range h.entries {
		entry, err := format.UnmarshalWALEntry(encoded)
		if err != nil {
			return result, err
		}
		if entry.EntryID < from {
			continue
		}
		if len(result.Entries) == maxEntries || used+uint64(len(encoded)) > maxBytes {
			break
		}
		result.Entries = append(result.Entries, encoded)
		used += uint64(len(encoded))
		result.NextEntryID = entry.EntryID + 1
		result.Last = wal.HistoryPosition{Present: true, EntryID: entry.EntryID, CRC32C: entry.CRC32C}
	}
	return result, nil
}

func TestPrimaryCatchUpPinsHistoricalTermsAndAdvancesCommit(t *testing.T) {
	log := &primaryTestLog{}
	receiver, err := NewReceiver(log, ReceiverConfig{
		GroupID: uuid(1), NodeID: uuid(3), State: ReceiverState{Term: 7, LeaderID: uuid(2)},
		ChecksumAt: func(entryID uint64) (uint32, bool) { entry, ok := log.entryAt(entryID); return entry.CRC32C, ok },
		EntryAt:    log.entryAt, ObserveTerm: func(uint64, format.UUID) error { return nil }, ApplyThrough: func(uint64, uint64) error { return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	primary, err := NewPrimary(uuid(1), uuid(2), 7, ReceiverPeer{Receiver: receiver})
	if err != nil {
		t.Fatal(err)
	}
	entries := encodedEntriesFor(t, 6, "historical", 3, 0, 0)
	last, _ := format.UnmarshalWALEntry(entries[2])
	history := &catchUpHistory{entries: entries}
	if err = primary.CatchUp(context.Background(), history, 0, 2, Position{Valid: true, EntryID: 2, CRC32C: last.CRC32C}, 2, 1<<20); err != nil {
		t.Fatal(err)
	}
	state, err := receiver.State()
	if err != nil || !state.Applied.Valid || state.Applied.EntryID != 2 || !history.pinned || !history.released || log.syncs != 2 {
		t.Fatalf("state = %+v, pinned = %v/%v, syncs = %d, error = %v", state, history.pinned, history.released, log.syncs, err)
	}
}
