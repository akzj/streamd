package registry

import (
	"fmt"
	"sort"
	"sync"

	"github.com/akzj/streamd/internal/storage/format"
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
	base           *SnapshotStore
	fallbackMu     sync.Mutex
	fallback       func() ([]Mapping, error)
}

func New() *Registry {
	return &Registry{byKey: make(map[Key]Mapping), byID: make(map[uint64]Mapping), nextID: 1}
}
func NewWithSnapshot(base *SnapshotStore) *Registry {
	r := New()
	r.base = base
	r.nextID = base.EntryCount() + 1
	r.coveredEntryID = base.CoveredEntryID()
	return r
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
	if existing, found, lookupErr := r.Lookup(record.Namespace, record.StreamName); lookupErr != nil {
		return lookupErr
	} else if found {
		if existing.StreamID != record.AssignedStreamID || existing.CreatedEntryID != entryID {
			return fmt.Errorf("Registry name maps to conflicting Stream IDs")
		}
		return nil
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
	if r.base != nil && mapping.StreamID < r.nextID {
		return fmt.Errorf("Registry Stream ID conflicts with Snapshot range")
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
func (r *Registry) Lookup(namespace, name string) (Mapping, bool, error) {
	r.mu.RLock()
	m, ok := r.byKey[Key{namespace, name}]
	base := r.base
	r.mu.RUnlock()
	if ok || base == nil {
		return m, ok, nil
	}
	m, ok, err := base.Lookup(namespace, name)
	if err == nil {
		return m, ok, nil
	}
	if rebuildErr := r.rebuild(); rebuildErr != nil {
		return Mapping{}, false, fmt.Errorf("Registry Snapshot lookup failed: %w; fact rebuild failed: %v", err, rebuildErr)
	}
	r.mu.RLock()
	m, ok = r.byKey[Key{namespace, name}]
	r.mu.RUnlock()
	return m, ok, nil
}
func (r *Registry) LookupID(id uint64) (Mapping, bool, error) {
	r.mu.RLock()
	m, ok := r.byID[id]
	base := r.base
	r.mu.RUnlock()
	if ok || base == nil {
		return m, ok, nil
	}
	mappings, err := base.Mappings()
	if err != nil {
		if rebuildErr := r.rebuild(); rebuildErr != nil {
			return Mapping{}, false, fmt.Errorf("Registry Snapshot scan failed: %w; fact rebuild failed: %v", err, rebuildErr)
		}
		r.mu.RLock()
		m, ok = r.byID[id]
		r.mu.RUnlock()
		return m, ok, nil
	}
	for _, mapping := range mappings {
		if mapping.StreamID == id {
			return mapping, true, nil
		}
	}
	return Mapping{}, false, nil
}

// NextAssignment returns the next permanent ID. The caller must serialize proposal and WAL commit.
func (r *Registry) NextAssignment(namespace, name string) (format.RegistryRecord, bool, error) {
	r.mu.RLock()
	if existing, ok := r.byKey[Key{namespace, name}]; ok {
		r.mu.RUnlock()
		return format.RegistryRecord{AssignedStreamID: existing.StreamID, Namespace: namespace, StreamName: name}, true, nil
	}
	base := r.base
	nextID := r.nextID
	r.mu.RUnlock()
	if base != nil {
		existing, ok, err := base.Lookup(namespace, name)
		if err != nil {
			if rebuildErr := r.rebuild(); rebuildErr != nil {
				return format.RegistryRecord{}, false, fmt.Errorf("Registry Snapshot lookup failed: %w; fact rebuild failed: %v", err, rebuildErr)
			}
			return r.NextAssignment(namespace, name)
		}
		if ok {
			return format.RegistryRecord{AssignedStreamID: existing.StreamID, Namespace: namespace, StreamName: name}, true, nil
		}
	}
	proposal := format.RegistryRecord{AssignedStreamID: nextID, Namespace: namespace, StreamName: name}
	if _, err := format.MarshalRegistryRecord(proposal); err != nil {
		return format.RegistryRecord{}, false, err
	}
	return proposal, false, nil
}
func (r *Registry) Snapshot(id format.UUID, coveredEntryID uint64, createdAt int64) (format.RegistrySnapshot, error) {
	r.mu.RLock()
	base := r.base
	overlay := make([]Mapping, 0, len(r.byKey))
	for _, m := range r.byKey {
		overlay = append(overlay, m)
	}
	r.mu.RUnlock()
	mappings := overlay
	if base != nil {
		baseMappings, err := base.Mappings()
		if err != nil {
			if rebuildErr := r.rebuild(); rebuildErr != nil {
				return format.RegistrySnapshot{}, fmt.Errorf("Registry Snapshot scan failed: %w; fact rebuild failed: %v", err, rebuildErr)
			}
			return r.Snapshot(id, coveredEntryID, createdAt)
		}
		mappings = append(baseMappings, overlay...)
	}
	return BuildSnapshot(id, coveredEntryID, createdAt, mappings), nil
}

func BuildSnapshot(id format.UUID, coveredEntryID uint64, createdAt int64, mappings []Mapping) format.RegistrySnapshot {
	entries := make([]format.RegistryEntry, 0, len(mappings))
	for _, m := range mappings {
		entries = append(entries, format.RegistryEntry{StreamID: m.StreamID, CreatedEntryID: m.CreatedEntryID, Namespace: m.Key.Namespace, StreamName: m.Key.StreamName})
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].Namespace != entries[j].Namespace {
			return entries[i].Namespace < entries[j].Namespace
		}
		return entries[i].StreamName < entries[j].StreamName
	})
	return format.RegistrySnapshot{Header: format.RegistrySnapshotHeader{ArtifactID: id, CoveredEntryID: coveredEntryID, CreatedAt: createdAt}, Entries: entries}
}
func (r *Registry) AdvanceCovered(entryID uint64) {
	r.mu.Lock()
	if entryID > r.coveredEntryID {
		r.coveredEntryID = entryID
	}
	r.mu.Unlock()
}
func (r *Registry) Count() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	count := len(r.byKey)
	if r.base != nil {
		count += int(r.base.EntryCount())
	}
	return count
}

func (r *Registry) HasSnapshot() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.base != nil
}

func (r *Registry) MappingsAfter(entryID uint64) []Mapping {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var mappings []Mapping
	for _, mapping := range r.byKey {
		if mapping.CreatedEntryID > entryID {
			mappings = append(mappings, mapping)
		}
	}
	return mappings
}

func (r *Registry) ApplyMapping(mapping Mapping) error {
	payload, err := format.MarshalRegistryRecord(format.RegistryRecord{AssignedStreamID: mapping.StreamID, Namespace: mapping.Key.Namespace, StreamName: mapping.Key.StreamName})
	if err != nil {
		return err
	}
	return r.ApplyRecord(mapping.CreatedEntryID, payload)
}

func (r *Registry) SnapshotReference() (format.ArtifactReference, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.base == nil {
		return format.ArtifactReference{}, false
	}
	return r.base.Reference(), true
}

func (r *Registry) SetFallback(fallback func() ([]Mapping, error)) {
	r.fallbackMu.Lock()
	r.fallback = fallback
	r.fallbackMu.Unlock()
}

func (r *Registry) RebuildFromFacts() error { return r.rebuild() }

func (r *Registry) rebuild() error {
	r.fallbackMu.Lock()
	defer r.fallbackMu.Unlock()
	r.mu.RLock()
	base := r.base
	r.mu.RUnlock()
	if base == nil {
		return nil
	}
	if r.fallback == nil {
		return fmt.Errorf("Registry fact rebuild is unavailable")
	}
	mappings, err := r.fallback()
	if err != nil {
		return err
	}
	rebuilt := New()
	for _, mapping := range mappings {
		if err = rebuilt.apply(mapping); err != nil {
			return err
		}
	}
	r.mu.Lock()
	for _, mapping := range r.byKey {
		if err = rebuilt.apply(mapping); err != nil {
			r.mu.Unlock()
			return err
		}
	}
	r.byKey = rebuilt.byKey
	r.byID = rebuilt.byID
	r.nextID = rebuilt.nextID
	r.base = nil
	r.mu.Unlock()
	return nil
}
