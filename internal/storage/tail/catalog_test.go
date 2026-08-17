package tail

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/akzj/streamd/internal/storage/format"
)

func TestWriteOpenAndLookupCheckpoint(t *testing.T) {
	root := t.TempDir()
	id := tailID(1)
	reference, err := WriteCheckpoint(root, id, 7, 19, []format.TailSlot{
		{Generation: 2, Present: true, StreamID: 0, NextSequence: 1, NextByteOffset: 20, LastRecordedAt: 10, LastEntryID: 3, AppliedEntryID: 19, LatestSegmentID: tailID(2)},
		{Generation: 4, Present: true, StreamID: 2, NextSequence: 9, NextByteOffset: 99, LastRecordedAt: 11, LastEntryID: 18, AppliedEntryID: 19, LatestSegmentID: tailID(3)},
	})
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := OpenCheckpoint(root, reference, 7, 19)
	if err != nil {
		t.Fatal(err)
	}
	defer catalog.Close()
	if catalog.Header().SlotCount != 3 {
		t.Fatalf("Slot count = %d", catalog.Header().SlotCount)
	}
	if _, found, err := catalog.Lookup(1); err != nil || found {
		t.Fatalf("missing Slot found=%v error=%v", found, err)
	}
	slot, found, err := catalog.Lookup(2)
	if err != nil || !found || slot.NextSequence != 9 {
		t.Fatalf("Slot = %+v, found=%v, error=%v", slot, found, err)
	}
	if _, found, err = catalog.Lookup(3); err != nil || found {
		t.Fatalf("out-of-range Slot found=%v error=%v", found, err)
	}
}

func TestOpenCheckpointRejectsCorruption(t *testing.T) {
	root := t.TempDir()
	reference, err := WriteCheckpoint(root, tailID(1), 1, 2, []format.TailSlot{{Generation: 2, Present: true, StreamID: 1, NextSequence: 1, NextByteOffset: 1, LastEntryID: 1, AppliedEntryID: 2, LatestSegmentID: tailID(2)}})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, filepath.FromSlash(reference.Path))
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = file.WriteAt([]byte{0xff}, 4096+format.TailSlotLength+16); err != nil {
		t.Fatal(err)
	}
	file.Close()
	catalog, err := OpenCheckpoint(root, reference, 1, 2)
	if err != nil {
		t.Fatalf("lazy open rejected Slot-local corruption: %v", err)
	}
	defer catalog.Close()
	if _, _, err = catalog.Lookup(1); err == nil {
		t.Fatal("corrupt Tail Slot accepted")
	}
}

func TestWriteCheckpointRejectsDuplicateAndWatermarkMismatch(t *testing.T) {
	slot := format.TailSlot{Generation: 2, Present: true, StreamID: 1, NextSequence: 1, NextByteOffset: 1, LastEntryID: 1, AppliedEntryID: 2, LatestSegmentID: tailID(2)}
	if _, err := WriteCheckpoint(t.TempDir(), tailID(1), 1, 2, []format.TailSlot{slot, slot}); err == nil {
		t.Fatal("duplicate Tail Slot accepted")
	}
	slot.AppliedEntryID = 3
	if _, err := WriteCheckpoint(t.TempDir(), tailID(1), 1, 2, []format.TailSlot{slot}); err == nil {
		t.Fatal("Tail Slot watermark mismatch accepted")
	}
}

func tailID(value byte) format.UUID {
	var id format.UUID
	id[15] = value
	return id
}
