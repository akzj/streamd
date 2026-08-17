package registry

import (
	"container/list"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/akzj/streamd/internal/storage/format"
	"github.com/akzj/streamd/internal/storage/fsutil"
)

const defaultBlockCacheCapacity = 64

type registryBlock struct {
	index format.RegistryBlockIndexEntry
	end   uint64
}

type cachedBlock struct {
	ordinal int
	entries []Mapping
}

// SnapshotStore keeps only the sparse block index resident. Registry entries
// are read and validated one bounded block at a time.
type SnapshotStore struct {
	root      string
	reference format.ArtifactReference
	header    format.RegistrySnapshotHeader
	blocks    []registryBlock
	capacity  int
	mu        sync.Mutex
	cache     map[int]*list.Element
	lru       *list.List
}

func FindReference(manifest format.Manifest) (format.ArtifactReference, bool, error) {
	var found format.ArtifactReference
	for _, reference := range manifest.ArtifactReferences {
		if reference.ArtifactType != format.ArtifactRegistrySnapshot {
			continue
		}
		if found.ArtifactID != (format.UUID{}) {
			return format.ArtifactReference{}, false, fmt.Errorf("Manifest contains multiple Registry Snapshots")
		}
		found = reference
	}
	return found, found.ArtifactID != (format.UUID{}), nil
}

func WriteCheckpoint(root string, snapshot format.RegistrySnapshot) (format.ArtifactReference, error) {
	encoded, err := format.MarshalRegistrySnapshot(snapshot)
	if err != nil {
		return format.ArtifactReference{}, err
	}
	verified, err := format.UnmarshalRegistrySnapshot(encoded)
	if err != nil {
		return format.ArtifactReference{}, err
	}
	directory := filepath.Join(root, "registry")
	if err = os.MkdirAll(directory, 0750); err != nil {
		return format.ArtifactReference{}, err
	}
	name := fmt.Sprintf("REGISTRY-%x.reg", verified.Header.ArtifactID)
	if err = fsutil.AtomicWrite(directory, name, encoded, 0640, nil); err != nil {
		return format.ArtifactReference{}, err
	}
	return format.ArtifactReference{
		ArtifactType: format.ArtifactRegistrySnapshot, FormatVersion: format.VersionV1,
		ArtifactID: verified.Header.ArtifactID, FileSize: uint64(len(encoded)),
		CoveredEntryID: verified.Header.CoveredEntryID,
		Path:           filepath.ToSlash(filepath.Join("registry", name)),
		ContentSHA256:  verified.Footer.ContentSHA256,
	}, nil
}

func OpenCheckpoint(root string, reference format.ArtifactReference, coveredEntryID uint64, capacity int) (*SnapshotStore, error) {
	if reference.ArtifactType != format.ArtifactRegistrySnapshot || reference.FormatVersion != format.VersionV1 || reference.CoveredEntryID != coveredEntryID {
		return nil, fmt.Errorf("Registry Snapshot Reference does not match checkpoint")
	}
	file, err := os.Open(filepath.Join(root, filepath.FromSlash(reference.Path)))
	if err != nil {
		return nil, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || uint64(info.Size()) != reference.FileSize {
		return nil, fmt.Errorf("Registry Snapshot size does not match Manifest")
	}
	headerBytes := make([]byte, format.RegistrySnapshotHeaderLength)
	if _, err = file.ReadAt(headerBytes, 0); err != nil {
		return nil, err
	}
	header, err := format.UnmarshalRegistrySnapshotHeader(headerBytes)
	if err != nil || header.ArtifactID != reference.ArtifactID || header.CoveredEntryID != coveredEntryID {
		return nil, fmt.Errorf("Registry Snapshot Header does not match Manifest")
	}
	footerBytes := make([]byte, format.ArtifactFooterLength)
	if _, err = file.ReadAt(footerBytes, info.Size()-format.ArtifactFooterLength); err != nil {
		return nil, err
	}
	footer, err := format.UnmarshalArtifactFooter(footerBytes)
	if err != nil || footer.ArtifactType != format.ArtifactRegistrySnapshot || footer.ArtifactID != reference.ArtifactID || footer.FileLength != reference.FileSize || footer.ContentSHA256 != reference.ContentSHA256 {
		return nil, fmt.Errorf("Registry Snapshot Footer does not match Manifest")
	}
	if header.EntriesOffset > footer.ContentLength || header.EntriesOffset < format.RegistrySnapshotHeaderLength {
		return nil, fmt.Errorf("Registry Snapshot index bounds are invalid")
	}
	indexBytes := make([]byte, header.EntriesOffset-format.RegistrySnapshotHeaderLength)
	n, readErr := file.ReadAt(indexBytes, format.RegistrySnapshotHeaderLength)
	if n != len(indexBytes) || (readErr != nil && readErr != io.EOF) {
		if readErr == nil {
			readErr = io.ErrUnexpectedEOF
		}
		return nil, fmt.Errorf("Registry Snapshot Block Index read failed: %w", readErr)
	}
	if header.BlockCount > 0 && (uint64(header.BlockCount) > header.EntryCount || uint64(header.BlockCount) > uint64(len(indexBytes))/28) {
		return nil, fmt.Errorf("Registry Snapshot Block count is invalid")
	}
	blocks := make([]registryBlock, 0, header.BlockCount)
	position := 0
	var entries uint64
	for i := uint32(0); i < header.BlockCount; i++ {
		if len(indexBytes)-position < 4 {
			return nil, fmt.Errorf("Registry Snapshot Block Index is truncated")
		}
		length := int(binary.LittleEndian.Uint32(indexBytes[position : position+4]))
		if length <= 0 || length > len(indexBytes)-position {
			return nil, fmt.Errorf("Registry Snapshot Block Index length is invalid")
		}
		index, decodeErr := format.UnmarshalRegistryBlockIndexEntry(indexBytes[position : position+length])
		if decodeErr != nil {
			return nil, decodeErr
		}
		if index.EntryCount > format.RegistryBlockEntriesV1 {
			return nil, fmt.Errorf("Registry Snapshot Block exceeds entry limit")
		}
		if i == 0 && index.EntriesOffset != header.EntriesOffset {
			return nil, fmt.Errorf("Registry Snapshot first Block offset is invalid")
		}
		if i > 0 {
			previous := blocks[len(blocks)-1]
			if compareKeys(previous.index.FirstNamespace, previous.index.FirstStreamName, index.FirstNamespace, index.FirstStreamName) >= 0 || index.EntriesOffset <= previous.index.EntriesOffset {
				return nil, fmt.Errorf("Registry Snapshot Block Index is not strictly ordered")
			}
			blocks[len(blocks)-1].end = index.EntriesOffset
		}
		entries += uint64(index.EntryCount)
		blocks = append(blocks, registryBlock{index: index})
		position += length
	}
	if position != len(indexBytes) || entries != header.EntryCount {
		return nil, fmt.Errorf("Registry Snapshot Block Index count is invalid")
	}
	if len(blocks) > 0 {
		blocks[len(blocks)-1].end = footer.ContentLength
		if blocks[len(blocks)-1].end <= blocks[len(blocks)-1].index.EntriesOffset {
			return nil, fmt.Errorf("Registry Snapshot last Block bounds are invalid")
		}
	} else if header.EntriesOffset != footer.ContentLength {
		return nil, fmt.Errorf("empty Registry Snapshot has Entry bytes")
	}
	if capacity <= 0 {
		capacity = defaultBlockCacheCapacity
	}
	return &SnapshotStore{root: root, reference: reference, header: header, blocks: blocks, capacity: capacity, cache: make(map[int]*list.Element), lru: list.New()}, nil
}

func (s *SnapshotStore) Lookup(namespace, name string) (Mapping, bool, error) {
	key := Key{Namespace: namespace, StreamName: name}
	i := sort.Search(len(s.blocks), func(i int) bool {
		block := s.blocks[i].index
		return compareKeys(block.FirstNamespace, block.FirstStreamName, namespace, name) > 0
	}) - 1
	if i < 0 {
		return Mapping{}, false, nil
	}
	entries, err := s.block(i)
	if err != nil {
		return Mapping{}, false, err
	}
	j := sort.Search(len(entries), func(j int) bool {
		return compareKeys(entries[j].Key.Namespace, entries[j].Key.StreamName, namespace, name) >= 0
	})
	if j == len(entries) || entries[j].Key != key {
		return Mapping{}, false, nil
	}
	return entries[j], true, nil
}

func (s *SnapshotStore) Mappings() ([]Mapping, error) {
	out := make([]Mapping, 0, s.header.EntryCount)
	for i := range s.blocks {
		entries, err := s.block(i)
		if err != nil {
			return nil, err
		}
		out = append(out, entries...)
	}
	return out, nil
}

func (s *SnapshotStore) block(ordinal int) ([]Mapping, error) {
	s.mu.Lock()
	if element := s.cache[ordinal]; element != nil {
		s.lru.MoveToFront(element)
		entries := element.Value.(cachedBlock).entries
		s.mu.Unlock()
		return entries, nil
	}
	s.mu.Unlock()
	entries, err := s.readBlock(ordinal)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if element := s.cache[ordinal]; element != nil {
		s.lru.MoveToFront(element)
		return element.Value.(cachedBlock).entries, nil
	}
	element := s.lru.PushFront(cachedBlock{ordinal: ordinal, entries: entries})
	s.cache[ordinal] = element
	if s.lru.Len() > s.capacity {
		old := s.lru.Back()
		delete(s.cache, old.Value.(cachedBlock).ordinal)
		s.lru.Remove(old)
	}
	return entries, nil
}

func (s *SnapshotStore) readBlock(ordinal int) ([]Mapping, error) {
	if ordinal < 0 || ordinal >= len(s.blocks) {
		return nil, fmt.Errorf("Registry Block ordinal is invalid")
	}
	block := s.blocks[ordinal]
	length := block.end - block.index.EntriesOffset
	if length == 0 || length > uint64(format.RegistryBlockEntriesV1)*(2*uint64(^uint16(0))+36) {
		return nil, fmt.Errorf("Registry Block length is invalid")
	}
	encoded := make([]byte, int(length))
	file, err := os.Open(filepath.Join(s.root, filepath.FromSlash(s.reference.Path)))
	if err != nil {
		return nil, err
	}
	n, err := file.ReadAt(encoded, int64(block.index.EntriesOffset))
	closeErr := file.Close()
	if n != len(encoded) || err != nil {
		if err == nil {
			err = io.ErrUnexpectedEOF
		}
		return nil, fmt.Errorf("Registry Block read failed: %w", err)
	}
	if closeErr != nil {
		return nil, closeErr
	}
	entries := make([]Mapping, 0, block.index.EntryCount)
	position := 0
	for i := uint32(0); i < block.index.EntryCount; i++ {
		if len(encoded)-position < 4 {
			return nil, fmt.Errorf("Registry Block is truncated")
		}
		entryLength := int(binary.LittleEndian.Uint32(encoded[position : position+4]))
		if entryLength <= 0 || entryLength > len(encoded)-position {
			return nil, fmt.Errorf("Registry Entry length is invalid")
		}
		entry, decodeErr := format.UnmarshalRegistryEntry(encoded[position : position+entryLength])
		if decodeErr != nil {
			return nil, decodeErr
		}
		mapping := Mapping{Key: Key{Namespace: entry.Namespace, StreamName: entry.StreamName}, StreamID: entry.StreamID, CreatedEntryID: entry.CreatedEntryID}
		if i == 0 && (mapping.Key.Namespace != block.index.FirstNamespace || mapping.Key.StreamName != block.index.FirstStreamName) {
			return nil, fmt.Errorf("Registry Block first Key does not match Index")
		}
		if len(entries) > 0 && compareKeys(entries[len(entries)-1].Key.Namespace, entries[len(entries)-1].Key.StreamName, mapping.Key.Namespace, mapping.Key.StreamName) >= 0 {
			return nil, fmt.Errorf("Registry Block Keys are not strictly ordered")
		}
		entries = append(entries, mapping)
		position += entryLength
	}
	if position != len(encoded) {
		return nil, fmt.Errorf("Registry Block has trailing bytes")
	}
	return entries, nil
}

func compareKeys(aNamespace, aName, bNamespace, bName string) int {
	if aNamespace < bNamespace {
		return -1
	}
	if aNamespace > bNamespace {
		return 1
	}
	if aName < bName {
		return -1
	}
	if aName > bName {
		return 1
	}
	return 0
}

func (s *SnapshotStore) Reference() format.ArtifactReference { return s.reference }
func (s *SnapshotStore) CoveredEntryID() uint64              { return s.header.CoveredEntryID }
func (s *SnapshotStore) EntryCount() uint64                  { return s.header.EntryCount }
func (s *SnapshotStore) CacheLen() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lru.Len()
}
func (s *SnapshotStore) ClearCache() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cache = make(map[int]*list.Element)
	s.lru.Init()
}
