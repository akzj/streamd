package registry

import (
	"github.com/akzj/streamd/internal/storage/format"
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
	mapping, ok := r.Lookup("agent", "events")
	if !ok || mapping.StreamID != 1 {
		t.Fatalf("mapping %+v %v", mapping, ok)
	}
	var id format.UUID
	id[15] = 1
	snapshot := r.Snapshot(id, 10)
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
