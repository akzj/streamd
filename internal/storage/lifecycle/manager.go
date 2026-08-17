package lifecycle

import (
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"github.com/akzj/streamd/internal/storage/format"
	"github.com/akzj/streamd/internal/storage/fsutil"
	manifeststore "github.com/akzj/streamd/internal/storage/manifest"
	"github.com/akzj/streamd/internal/storage/memtable"
	"github.com/akzj/streamd/internal/storage/segment"
)

type retirement struct {
	source      string
	destination string
	renamed     bool
}

type Manager struct {
	mu        sync.Mutex
	root      string
	manifests *manifeststore.Store
	pins      map[format.UUID]int
	pending   map[format.UUID]*retirement
}

type ArtifactBuilder func(generation uint64, segments []format.SegmentReference, coveredEntryID uint64) ([]format.ArtifactReference, error)

func New(root string, manifests *manifeststore.Store) *Manager {
	return &Manager{
		root:      root,
		manifests: manifests,
		pins:      make(map[format.UUID]int),
		pending:   make(map[format.UUID]*retirement),
	}
}

func (m *Manager) Pin(id format.UUID) func() {
	m.mu.Lock()
	m.pins[id]++
	m.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			m.mu.Lock()
			m.pins[id]--
			if m.pins[id] == 0 {
				delete(m.pins, id)
				if pending := m.pending[id]; pending != nil {
					_ = m.retireLocked(id, pending.source)
				}
			}
			m.mu.Unlock()
		})
	}
}
func (m *Manager) PublishFlush(streams []memtable.StreamSnapshot, lastEntryID uint64, lastCRC uint32) (format.Manifest, error) {
	return m.PublishFlushWithArtifacts(streams, lastEntryID, lastCRC, nil)
}

func (m *Manager) PublishFlushWithArtifacts(streams []memtable.StreamSnapshot, lastEntryID uint64, lastCRC uint32, builder ArtifactBuilder) (format.Manifest, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(streams) == 0 {
		return format.Manifest{}, fmt.Errorf("empty Flush")
	}
	id, err := newID()
	if err != nil {
		return format.Manifest{}, err
	}
	staging := filepath.Join(m.root, "staging", fmt.Sprintf("SEG-%x.tmp", id))
	meta, err := segment.WriteFile(staging, id, time.Now().UnixNano(), streams)
	if err != nil {
		return format.Manifest{}, err
	}
	name := fmt.Sprintf("SEG-%x.seg", id)
	final := filepath.Join(m.root, "segments", name)
	if err = os.Rename(staging, final); err != nil {
		return format.Manifest{}, err
	}
	if err = fsutil.SyncDir(filepath.Join(m.root, "segments")); err != nil {
		return format.Manifest{}, err
	}
	reference := format.SegmentReference{Flags: format.SegmentRefHasLocal, SegmentID: id, FileSize: meta.Footer.FileLength, FirstEntryID: meta.Header.FirstEntryID, LastEntryID: meta.Header.LastEntryID, StreamCount: meta.Header.StreamCount, RecordCount: meta.Header.RecordCount, LocalPath: "segments/" + name, ContentSHA256: meta.Footer.ContentSHA256}
	next, err := m.nextManifest(appendCurrent(m.manifests, reference), lastEntryID, lastCRC)
	if err != nil {
		return format.Manifest{}, err
	}
	if err = applyArtifactBuilder(&next, builder); err != nil {
		return format.Manifest{}, err
	}
	return m.manifests.Publish(next)
}
func (m *Manager) Merge(ids []format.UUID) (format.Manifest, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	published, selected, err := m.publishMergeLocked(ids, nil)
	if err != nil {
		return format.Manifest{}, err
	}
	for _, ref := range selected {
		path := filepath.Join(m.root, ref.LocalPath)
		_ = m.retireLocked(ref.SegmentID, path)
	}
	return published, nil
}

// PublishMerge publishes a replacement Segment while retaining all input
// files. The caller must install the new reader Generation before calling
// Retire, so a reader on the previous Generation can still open its files.
func (m *Manager) PublishMerge(ids []format.UUID) (format.Manifest, []format.SegmentReference, error) {
	return m.PublishMergeWithArtifacts(ids, nil)
}

func (m *Manager) PublishMergeWithArtifacts(ids []format.UUID, builder ArtifactBuilder) (format.Manifest, []format.SegmentReference, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.publishMergeLocked(ids, builder)
}

func (m *Manager) publishMergeLocked(ids []format.UUID, builder ArtifactBuilder) (format.Manifest, []format.SegmentReference, error) {
	if len(ids) < 2 {
		return format.Manifest{}, nil, fmt.Errorf("Merge requires at least two Segments")
	}
	current, ok := m.manifests.Current()
	if !ok {
		return format.Manifest{}, nil, fmt.Errorf("no current Manifest")
	}
	wanted := make(map[format.UUID]bool, len(ids))
	for _, id := range ids {
		if wanted[id] {
			return format.Manifest{}, nil, fmt.Errorf("Merge input contains duplicate Segment %x", id)
		}
		wanted[id] = true
	}
	var selected []format.SegmentReference
	var kept []format.SegmentReference
	for _, ref := range current.SegmentReferences {
		if wanted[ref.SegmentID] {
			selected = append(selected, ref)
		} else {
			kept = append(kept, ref)
		}
	}
	if len(selected) != len(wanted) {
		return format.Manifest{}, nil, fmt.Errorf("Merge input is not fully referenced")
	}
	byStream := make(map[uint64][][]byte)
	for _, ref := range selected {
		if ref.Flags&format.SegmentRefHasLocal == 0 {
			return format.Manifest{}, nil, fmt.Errorf("Merge input Segment %x has no local copy", ref.SegmentID)
		}
		reader, err := segment.Open(filepath.Join(m.root, ref.LocalPath))
		if err != nil {
			return format.Manifest{}, nil, err
		}
		for _, d := range reader.Directories {
			for seq := d.FirstSequence; seq < d.FirstSequence+d.RecordCount; seq++ {
				frame, e := reader.ReadFrame(d.StreamID, seq)
				if e != nil {
					reader.Close()
					return format.Manifest{}, nil, e
				}
				byStream[d.StreamID] = append(byStream[d.StreamID], frame)
			}
		}
		reader.Close()
	}
	streams := make([]memtable.StreamSnapshot, 0, len(byStream))
	for streamID, frames := range byStream {
		slices.SortFunc(frames, func(a, b []byte) int {
			ra, _ := format.UnmarshalRecordFrame(a)
			rb, _ := format.UnmarshalRecordFrame(b)
			if ra.Sequence < rb.Sequence {
				return -1
			}
			if ra.Sequence > rb.Sequence {
				return 1
			}
			return 0
		})
		records := make([]format.RecordFrame, len(frames))
		for i, frame := range frames {
			r, e := format.UnmarshalRecordFrame(frame)
			if e != nil {
				return format.Manifest{}, nil, e
			}
			records[i] = r
			if i > 0 && (r.Sequence != records[i-1].Sequence+1 || r.ByteOffset != records[i-1].ByteOffset+uint64(len(frames[i-1]))) {
				return format.Manifest{}, nil, fmt.Errorf("Merge input has a Stream gap")
			}
		}
		last := records[len(records)-1]
		streams = append(streams, memtable.StreamSnapshot{StreamID: streamID, Tail: memtable.Tail{NextSequence: last.Sequence + 1, NextByteOffset: last.ByteOffset + uint64(len(frames[len(frames)-1])), LastRecordedAt: last.RecordedAt, LastEntryID: last.EntryID, RecordCount: uint64(len(frames))}, Frames: frames})
	}
	slices.SortFunc(streams, func(a, b memtable.StreamSnapshot) int {
		if a.StreamID < b.StreamID {
			return -1
		}
		if a.StreamID > b.StreamID {
			return 1
		}
		return 0
	})
	id, err := newID()
	if err != nil {
		return format.Manifest{}, nil, err
	}
	staging := filepath.Join(m.root, "staging", fmt.Sprintf("SEG-%x.tmp", id))
	meta, err := segment.WriteFile(staging, id, time.Now().UnixNano(), streams)
	if err != nil {
		return format.Manifest{}, nil, err
	}
	name := fmt.Sprintf("SEG-%x.seg", id)
	final := filepath.Join(m.root, "segments", name)
	if err = os.Rename(staging, final); err != nil {
		return format.Manifest{}, nil, err
	}
	if err = fsutil.SyncDir(filepath.Join(m.root, "segments")); err != nil {
		return format.Manifest{}, nil, err
	}
	kept = append(kept, format.SegmentReference{Flags: format.SegmentRefHasLocal, SegmentID: id, FileSize: meta.Footer.FileLength, FirstEntryID: meta.Header.FirstEntryID, LastEntryID: meta.Header.LastEntryID, StreamCount: meta.Header.StreamCount, RecordCount: meta.Header.RecordCount, LocalPath: "segments/" + name, ContentSHA256: meta.Footer.ContentSHA256})
	next, err := m.nextManifest(kept, current.Header.LastEntryID, current.Header.LastEntryCRC32C)
	if err != nil {
		return format.Manifest{}, nil, err
	}
	if err = applyArtifactBuilder(&next, builder); err != nil {
		return format.Manifest{}, nil, err
	}
	published, err := m.manifests.Publish(next)
	if err != nil {
		return format.Manifest{}, nil, err
	}
	return published, selected, nil
}

func applyArtifactBuilder(next *format.Manifest, builder ArtifactBuilder) error {
	if builder == nil {
		return nil
	}
	replacements, err := builder(next.Header.Generation, append([]format.SegmentReference(nil), next.SegmentReferences...), next.Header.LastEntryID)
	if err != nil {
		return err
	}
	types := make(map[format.ArtifactType]bool, len(replacements))
	for _, replacement := range replacements {
		if types[replacement.ArtifactType] {
			return fmt.Errorf("duplicate Artifact replacement type %d", replacement.ArtifactType)
		}
		types[replacement.ArtifactType] = true
	}
	artifacts := make([]format.ArtifactReference, 0, len(next.ArtifactReferences)+len(replacements))
	for _, reference := range next.ArtifactReferences {
		if !types[reference.ArtifactType] {
			artifacts = append(artifacts, reference)
		}
	}
	next.ArtifactReferences = append(artifacts, replacements...)
	return nil
}

func (m *Manager) Retire(references []format.SegmentReference) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	var retireErr error
	for _, ref := range references {
		retireErr = errors.Join(retireErr, m.retireLocked(ref.SegmentID, filepath.Join(m.root, ref.LocalPath)))
	}
	return retireErr
}

func (m *Manager) RetireArtifacts(references []format.ArtifactReference) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	var retireErr error
	for _, ref := range references {
		if m.pending[ref.ArtifactID] == nil {
			m.pending[ref.ArtifactID] = &retirement{
				source:      filepath.Join(m.root, ref.Path),
				destination: filepath.Join(m.root, "trash", fmt.Sprintf("ART-%x-%d.trash", ref.ArtifactID, time.Now().UnixNano())),
			}
		}
		retireErr = errors.Join(retireErr, m.retireLocked(ref.ArtifactID, filepath.Join(m.root, ref.Path)))
	}
	return retireErr
}

func (m *Manager) nextManifest(refs []format.SegmentReference, lastID uint64, lastCRC uint32) (format.Manifest, error) {
	var header format.ManifestHeader
	current, ok := m.manifests.Current()
	id, err := newID()
	if err != nil {
		return format.Manifest{}, err
	}
	header.FileID = id
	header.CreatedAt = time.Now().UnixNano()
	header.LastEntryID = lastID
	header.LastEntryCRC32C = lastCRC
	for _, ref := range refs {
		header.RecordCount += ref.RecordCount
	}
	if ok {
		header.Generation = current.Header.Generation + 1
		header.PreviousGeneration = current.Header.Generation
		header.PreviousManifestSHA256 = current.Footer.ContentSHA256
	}
	var artifacts []format.ArtifactReference
	if ok {
		artifacts = current.ArtifactReferences
	}
	return format.Manifest{Header: header, SegmentReferences: refs, ArtifactReferences: artifacts}, nil
}
func appendCurrent(store *manifeststore.Store, ref format.SegmentReference) []format.SegmentReference {
	current, ok := store.Current()
	if !ok {
		return []format.SegmentReference{ref}
	}
	return append(current.SegmentReferences, ref)
}
func (m *Manager) retireLocked(id format.UUID, source string) error {
	pending := m.pending[id]
	if pending == nil {
		pending = &retirement{
			source:      source,
			destination: filepath.Join(m.root, "trash", fmt.Sprintf("SEG-%x-%d.trash", id, time.Now().UnixNano())),
		}
		m.pending[id] = pending
	}
	if m.pins[id] > 0 {
		return nil
	}
	if !pending.renamed {
		if err := os.Rename(pending.source, pending.destination); err != nil {
			return err
		}
		pending.renamed = true
	}
	if err := fsutil.SyncDir(filepath.Dir(pending.source)); err != nil {
		return err
	}
	if err := fsutil.SyncDir(filepath.Dir(pending.destination)); err != nil {
		return err
	}
	delete(m.pending, id)
	return nil
}

func (m *Manager) CollectTrash(before time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	var retryErr error
	for id, pending := range m.pending {
		if m.pins[id] == 0 {
			retryErr = errors.Join(retryErr, m.retireLocked(id, pending.source))
		}
	}
	entries, err := os.ReadDir(filepath.Join(m.root, "trash"))
	if err != nil {
		return err
	}
	for _, entry := range entries {
		info, e := entry.Info()
		if e != nil {
			return e
		}
		if info.ModTime().Before(before) {
			if e = os.Remove(filepath.Join(m.root, "trash", entry.Name())); e != nil {
				return e
			}
		}
	}
	return errors.Join(retryErr, fsutil.SyncDir(filepath.Join(m.root, "trash")))
}

func newID() (format.UUID, error) {
	var id format.UUID
	_, err := rand.Read(id[:])
	return id, err
}
