package registry

import (
	"fmt"
	"github.com/akzj/streamd/internal/storage/format"
	"sort"
	"sync"
)

const RegistryStreamID uint64 = 0

type Key struct {
	Namespace  string
	StreamName string
}
type Mapping struct {
	Key            Key
	StreamID       uint64
	CreatedEntryID uint64
}
type Registry struct {
	mu             sync.RWMutex
	byKey          map[Key]Mapping
	byID           map[uint64]Mapping
	nextID         uint64
	coveredEntryID uint64
}

func New() *Registry {
	return &Registry{byKey: make(map[Key]Mapping), byID: make(map[uint64]Mapping), nextID: 1}
}
func FromSnapshot(snapshot format.RegistrySnapshot) (*Registry, error) {
	r := New()
	for _, entry := range snapshot.Entries {
		if err := r.apply(Mapping{Key: Key{entry.Namespace, entry.StreamName}, StreamID: entry.StreamID, CreatedEntryID: entry.CreatedEntryID}); err != nil {
			return nil, err
		}
	}
	r.coveredEntryID = snapshot.Header.CoveredEntryID
	return r, nil
}
func (r *Registry) ApplyRecord(entryID uint64, payload []byte) error {
	record, err := format.UnmarshalRegistryRecord(payload)
	if err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if entryID < r.coveredEntryID {
		return fmt.Errorf("Registry Entry %d precedes covered checkpoint %d", entryID, r.coveredEntryID)
	}
	if err = r.apply(Mapping{Key: Key{record.Namespace, record.StreamName}, StreamID: record.AssignedStreamID, CreatedEntryID: entryID}); err != nil {
		return err
	}
	if entryID > r.coveredEntryID {
		r.coveredEntryID = entryID
	}
	return nil
}
func (r *Registry) apply(mapping Mapping) error {
	if existing, ok := r.byKey[mapping.Key]; ok {
		if existing.StreamID != mapping.StreamID || existing.CreatedEntryID != mapping.CreatedEntryID {
			return fmt.Errorf("Registry name maps to conflicting Stream IDs")
		}
		return nil
	}
	if existing, ok := r.byID[mapping.StreamID]; ok {
		if existing.Key != mapping.Key {
			return fmt.Errorf("Registry Stream ID maps to conflicting names")
		}
		return nil
	}
	if mapping.StreamID == 0 {
		return fmt.Errorf("Stream ID 0 is reserved")
	}
	r.byKey[mapping.Key] = mapping
	r.byID[mapping.StreamID] = mapping
	if mapping.StreamID >= r.nextID {
		r.nextID = mapping.StreamID + 1
	}
	return nil
}
func (r *Registry) Lookup(namespace, name string) (Mapping, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	m, ok := r.byKey[Key{namespace, name}]
	return m, ok
}
func (r *Registry) LookupID(id uint64) (Mapping, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	m, ok := r.byID[id]
	return m, ok
}

// NextAssignment returns the next permanent ID. The caller must serialize proposal and WAL commit.
func (r *Registry) NextAssignment(namespace, name string) (format.RegistryRecord, bool, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if existing, ok := r.byKey[Key{namespace, name}]; ok {
		return format.RegistryRecord{AssignedStreamID: existing.StreamID, Namespace: namespace, StreamName: name}, true, nil
	}
	proposal := format.RegistryRecord{AssignedStreamID: r.nextID, Namespace: namespace, StreamName: name}
	if _, err := format.MarshalRegistryRecord(proposal); err != nil {
		return format.RegistryRecord{}, false, err
	}
	return proposal, false, nil
}
func (r *Registry) Snapshot(id format.UUID, createdAt int64) format.RegistrySnapshot {
	r.mu.RLock()
	defer r.mu.RUnlock()
	entries := make([]format.RegistryEntry, 0, len(r.byKey))
	for _, m := range r.byKey {
		entries = append(entries, format.RegistryEntry{StreamID: m.StreamID, CreatedEntryID: m.CreatedEntryID, Namespace: m.Key.Namespace, StreamName: m.Key.StreamName})
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].Namespace != entries[j].Namespace {
			return entries[i].Namespace < entries[j].Namespace
		}
		return entries[i].StreamName < entries[j].StreamName
	})
	return format.RegistrySnapshot{Header: format.RegistrySnapshotHeader{ArtifactID: id, CoveredEntryID: r.coveredEntryID, CreatedAt: createdAt}, Entries: entries}
}
func (r *Registry) AdvanceCovered(entryID uint64) {
	r.mu.Lock()
	if entryID > r.coveredEntryID {
		r.coveredEntryID = entryID
	}
	r.mu.Unlock()
}
func (r *Registry) Count() int { r.mu.RLock(); defer r.mu.RUnlock(); return len(r.byKey) }
