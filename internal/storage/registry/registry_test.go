package registry

import (
	"fmt"
	"github.com/akzj/streamd/internal/storage/format"
	"os"
	"path/filepath"
	"testing"
)

func TestApplyLookupSnapshotRestore(t *testing.T) {
	r := New()
	proposal, exists, err := r.NextAssignment("agent", "events")
	if err != nil || exists || proposal.AssignedStreamID != 1 {
		t.Fatalf("proposal %+v %v %v", proposal, exists, err)
	}
	payload, err := format.MarshalRegistryRecord(proposal)
	if err != nil {
		t.Fatal(err)
	}
	if err = r.ApplyRecord(3, payload); err != nil {
		t.Fatal(err)
	}
	mapping, ok, err := r.Lookup("agent", "events")
	if err != nil || !ok || mapping.StreamID != 1 {
		t.Fatalf("mapping %+v %v %v", mapping, ok, err)
	}
	var id format.UUID
	id[15] = 1
	snapshot, err := r.Snapshot(id, 3, 10)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := format.MarshalRegistrySnapshot(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := format.UnmarshalRegistrySnapshot(encoded)
	if err != nil {
		t.Fatal(err)
	}
	restored, err := FromSnapshot(decoded)
	if err != nil {
		t.Fatal(err)
	}
	if restored.Count() != 1 {
		t.Fatalf("count %d", restored.Count())
	}
}

func TestSnapshotStoreLookupCacheAndFallback(t *testing.T) {
	root := t.TempDir()
	r := New()
	for i := 1; i <= format.RegistryBlockEntriesV1+1; i++ {
		record := format.RegistryRecord{AssignedStreamID: uint64(i), Namespace: "agent", StreamName: fmt.Sprintf("stream-%04d", i)}
		payload, err := format.MarshalRegistryRecord(record)
		if err != nil {
			t.Fatal(err)
		}
		if err = r.ApplyRecord(uint64(i), payload); err != nil {
			t.Fatal(err)
		}
	}
	id := registryID(2)
	snapshot, err := r.Snapshot(id, uint64(format.RegistryBlockEntriesV1+1), 10)
	if err != nil {
		t.Fatal(err)
	}
	reference, err := WriteCheckpoint(root, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	store, err := OpenCheckpoint(root, reference, reference.CoveredEntryID, 1)
	if err != nil {
		t.Fatal(err)
	}
	for _, number := range []int{1, format.RegistryBlockEntriesV1 + 1} {
		mapping, found, lookupErr := store.Lookup("agent", fmt.Sprintf("stream-%04d", number))
		if lookupErr != nil || !found || mapping.StreamID != uint64(number) {
			t.Fatalf("Lookup %d = %+v, %v, %v", number, mapping, found, lookupErr)
		}
	}
	if store.CacheLen() != 1 {
		t.Fatalf("Registry Block Cache length = %d", store.CacheLen())
	}
	if _, found, lookupErr := store.Lookup("agent", "missing"); lookupErr != nil || found {
		t.Fatalf("missing Lookup found=%v error=%v", found, lookupErr)
	}
	store.ClearCache()
	file, err := os.OpenFile(filepath.Join(root, filepath.FromSlash(reference.Path)), os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = file.WriteAt([]byte{0xff}, int64(store.blocks[0].index.EntriesOffset+8)); err != nil {
		file.Close()
		t.Fatal(err)
	}
	if err = file.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err = store.Lookup("agent", "stream-0001"); err == nil {
		t.Fatal("corrupt Registry Block passed lookup")
	}
	withBase := NewWithSnapshot(store)
	withBase.SetFallback(func() ([]Mapping, error) {
		mappings := make([]Mapping, 0, len(snapshot.Entries))
		for _, entry := range snapshot.Entries {
			mappings = append(mappings, Mapping{Key: Key{Namespace: entry.Namespace, StreamName: entry.StreamName}, StreamID: entry.StreamID, CreatedEntryID: entry.CreatedEntryID})
		}
		return mappings, nil
	})
	mapping, found, err := withBase.Lookup("agent", "stream-0001")
	if err != nil || !found || mapping.StreamID != 1 || withBase.HasSnapshot() {
		t.Fatalf("fallback Lookup = %+v, %v, %v snapshot=%v", mapping, found, err, withBase.HasSnapshot())
	}
}
func TestRejectsBidirectionalConflict(t *testing.T) {
	r := New()
	one, _ := format.MarshalRegistryRecord(format.RegistryRecord{AssignedStreamID: 1, Namespace: "a", StreamName: "x"})
	two, _ := format.MarshalRegistryRecord(format.RegistryRecord{AssignedStreamID: 1, Namespace: "b", StreamName: "y"})
	if err := r.ApplyRecord(1, one); err != nil {
		t.Fatal(err)
	}
	if err := r.ApplyRecord(2, two); err == nil {
		t.Fatal("conflicting Stream ID accepted")
	}
}

func registryID(value byte) format.UUID {
	var id format.UUID
	id[15] = value
	return id
}
