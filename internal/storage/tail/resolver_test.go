package tail

import (
	"testing"

	"github.com/akzj/streamd/internal/storage/format"
	"github.com/akzj/streamd/internal/storage/memtable"
)

func TestResolverBoundsHistoricalTailCache(t *testing.T) {
	root := t.TempDir()
	slots := make([]format.TailSlot, 0, 8)
	for streamID := uint64(1); streamID <= 8; streamID++ {
		slots = append(slots, format.TailSlot{
			Generation: 2, Present: true, StreamID: streamID,
			NextSequence: streamID, NextByteOffset: streamID * 10,
			LastRecordedAt: int64(streamID), LastEntryID: streamID,
			AppliedEntryID: 8,
		})
	}
	reference, err := WriteNewCheckpoint(root, 1, 8, slots)
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := OpenCheckpoint(root, reference, 1, 8)
	if err != nil {
		t.Fatal(err)
	}
	defer catalog.Close()
	resolver := NewResolver(memtable.New(0), catalog, root, nil, 2)
	for streamID := uint64(1); streamID <= 8; streamID++ {
		tail, found, lookupErr := resolver.Lookup(streamID)
		if lookupErr != nil || !found || tail.NextSequence != streamID {
			t.Fatalf("Lookup(%d) = %+v, %v, %v", streamID, tail, found, lookupErr)
		}
	}
	if resolver.CacheLen() != 2 {
		t.Fatalf("cache length = %d, want 2", resolver.CacheLen())
	}
	if snapshots := resolver.table.Snapshot(); len(snapshots) != 0 {
		t.Fatalf("read-only Tail lookups seeded MemTable: %+v", snapshots)
	}
}
