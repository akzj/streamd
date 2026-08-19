package locator

import (
	"bytes"
	"container/list"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/akzj/streamd/internal/storage/format"
)

func VerifyPack(root string, pack format.LocatorPackReference) error {
	file, err := os.Open(filepath.Join(root, filepath.FromSlash(pack.Path)))
	if err != nil {
		return err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return err
	}
	if info.Size() < format.SegmentSectionAlignment+format.ArtifactFooterLength || uint64(info.Size()) != pack.FileSize {
		return fmt.Errorf("Locator Pack size does not match Snapshot")
	}
	headerBytes := make([]byte, format.LocatorPackHeaderLength)
	if _, err = file.ReadAt(headerBytes, 0); err != nil {
		return err
	}
	header, err := format.UnmarshalLocatorPackHeader(headerBytes)
	if err != nil || header.ArtifactID != pack.PackID || header.PageCount != pack.PageCount {
		return fmt.Errorf("Locator Pack Header does not match Snapshot")
	}
	footerBytes := make([]byte, format.ArtifactFooterLength)
	if _, err = file.ReadAt(footerBytes, info.Size()-format.ArtifactFooterLength); err != nil {
		return err
	}
	footer, err := format.UnmarshalArtifactFooter(footerBytes)
	if err != nil || footer.ArtifactType != format.ArtifactLocatorPack || footer.ArtifactID != pack.PackID || footer.FileLength != uint64(info.Size()) || footer.ContentLength != uint64(info.Size())-format.ArtifactFooterLength {
		return fmt.Errorf("Locator Pack Footer does not match Snapshot")
	}
	digest := sha256.New()
	if _, err = io.CopyBuffer(digest, io.NewSectionReader(file, 0, int64(footer.ContentLength)), make([]byte, 256*1024)); err != nil {
		return err
	}
	var contentSHA256 [sha256.Size]byte
	copy(contentSHA256[:], digest.Sum(nil))
	if contentSHA256 != footer.ContentSHA256 || contentSHA256 != pack.ContentSHA256 {
		return fmt.Errorf("Locator Pack digest does not match Snapshot")
	}
	return nil
}

type pageKey struct {
	packID  format.UUID
	ordinal uint32
}

type cachedPage struct {
	key  pageKey
	page format.ExtentPage
}

type Extent struct {
	Reference format.SegmentReference
	Entry     format.ExtentEntry
}

type Store struct {
	root           string
	reference      format.ArtifactReference
	header         format.LocatorSnapshotHeader
	packReferences []format.LocatorPackReference
	packs          map[format.UUID]format.LocatorPackReference
	segments       map[format.UUID]format.SegmentReference
	rootsOffset    int64
	snapshotMu     sync.RWMutex
	snapshotFile   *os.File
	rootCapacity   int
	rootCache      map[uint64]*list.Element
	rootLRU        *list.List
	capacity       int
	mu             sync.Mutex
	cache          map[pageKey]*list.Element
	lru            *list.List
}

type cachedRoot struct {
	streamID uint64
	entry    format.LocatorRootEntry
}

const (
	locatorPackReferenceMinimumLength = 80
	defaultRootCacheCapacity          = 1024
)

func Open(root string, manifest format.Manifest, pageCapacity int) (*Store, error) {
	var reference format.ArtifactReference
	var tailID format.UUID
	for _, artifact := range manifest.ArtifactReferences {
		switch artifact.ArtifactType {
		case format.ArtifactLocatorSnapshot:
			if reference.ArtifactID != (format.UUID{}) {
				return nil, fmt.Errorf("Manifest contains multiple Locator Snapshots")
			}
			reference = artifact
		case format.ArtifactTailCatalog:
			tailID = artifact.ArtifactID
		}
	}
	if reference.ArtifactID == (format.UUID{}) {
		return nil, os.ErrNotExist
	}
	file, err := os.Open(filepath.Join(root, filepath.FromSlash(reference.Path)))
	if err != nil {
		return nil, err
	}
	closeOnError := true
	defer func() {
		if closeOnError {
			_ = file.Close()
		}
	}()
	info, err := file.Stat()
	if err != nil || uint64(info.Size()) != reference.FileSize {
		return nil, fmt.Errorf("Locator Snapshot size does not match Manifest")
	}
	headerBytes := make([]byte, format.LocatorSnapshotHeaderLength)
	if _, err = file.ReadAt(headerBytes, 0); err != nil {
		return nil, err
	}
	header, err := format.UnmarshalLocatorSnapshotHeader(headerBytes)
	if err != nil {
		return nil, err
	}
	footerBytes := make([]byte, format.ArtifactFooterLength)
	if _, err = file.ReadAt(footerBytes, info.Size()-format.ArtifactFooterLength); err != nil {
		return nil, err
	}
	footer, err := format.UnmarshalArtifactFooter(footerBytes)
	if err != nil || footer.ArtifactType != format.ArtifactLocatorSnapshot || footer.ArtifactID != reference.ArtifactID || footer.FileLength != reference.FileSize || footer.ContentSHA256 != reference.ContentSHA256 {
		return nil, fmt.Errorf("Locator Snapshot Footer does not match Manifest")
	}
	if header.ArtifactID != reference.ArtifactID || header.ManifestGeneration != manifest.Header.Generation || header.CoveredEntryID != manifest.Header.LastEntryID || header.TailCatalogArtifactID != tailID {
		return nil, fmt.Errorf("Locator Snapshot does not match Manifest")
	}
	if footer.ContentLength < format.LocatorSnapshotHeaderLength || uint64(header.PackCount) > (footer.ContentLength-format.LocatorSnapshotHeaderLength)/locatorPackReferenceMinimumLength {
		return nil, fmt.Errorf("Locator Pack count exceeds Snapshot bounds")
	}
	position := int64(format.LocatorSnapshotHeaderLength)
	packReferences := make([]format.LocatorPackReference, 0, header.PackCount)
	for i := uint32(0); i < header.PackCount; i++ {
		fixed := make([]byte, locatorPackReferenceMinimumLength)
		if _, err = file.ReadAt(fixed, position); err != nil {
			return nil, fmt.Errorf("Locator Pack Reference %d is truncated: %w", i, err)
		}
		length := uint64(binary.LittleEndian.Uint32(fixed[:4]))
		if uint64(position) > footer.ContentLength || length < locatorPackReferenceMinimumLength || length > uint64(locatorPackReferenceMinimumLength)+uint64(^uint16(0)) || length > footer.ContentLength-uint64(position) {
			return nil, fmt.Errorf("Locator Pack Reference %d length is invalid", i)
		}
		encoded := fixed
		if length > uint64(len(fixed)) {
			encoded = make([]byte, length)
			if _, err = file.ReadAt(encoded, position); err != nil {
				return nil, fmt.Errorf("Locator Pack Reference %d is truncated: %w", i, err)
			}
		}
		pack, decodeErr := format.UnmarshalLocatorPackReference(encoded)
		if decodeErr != nil {
			return nil, decodeErr
		}
		if len(packReferences) > 0 {
			previous := packReferences[len(packReferences)-1]
			if bytes.Compare(previous.PackID[:], pack.PackID[:]) >= 0 {
				return nil, fmt.Errorf("Locator Packs are not strictly sorted")
			}
		}
		packReferences = append(packReferences, pack)
		position += int64(length)
	}
	rootsLength := uint64(header.RootCount) * format.LocatorRootEntryLength
	if uint64(position) > footer.ContentLength || rootsLength != footer.ContentLength-uint64(position) {
		return nil, fmt.Errorf("Locator Root bounds do not match Snapshot")
	}
	if pageCapacity <= 0 {
		pageCapacity = 256
	}
	store := &Store{root: root, reference: reference, header: header, packReferences: packReferences, packs: make(map[format.UUID]format.LocatorPackReference, len(packReferences)), segments: make(map[format.UUID]format.SegmentReference, len(manifest.SegmentReferences)), rootsOffset: position, snapshotFile: file, rootCapacity: defaultRootCacheCapacity, rootCache: make(map[uint64]*list.Element), rootLRU: list.New(), capacity: pageCapacity, cache: make(map[pageKey]*list.Element), lru: list.New()}
	for _, pack := range packReferences {
		store.packs[pack.PackID] = pack
	}
	for _, segment := range manifest.SegmentReferences {
		store.segments[segment.SegmentID] = segment
	}
	closeOnError = false
	return store, nil
}

func (s *Store) LookupSequence(streamID, sequence uint64) (Extent, bool, error) {
	root, found, err := s.lookupRoot(streamID)
	if err != nil || !found {
		return Extent{}, false, err
	}
	pointer := pageKey{packID: root.PackID, ordinal: root.PageOrdinal}
	for {
		page, err := s.page(pointer, streamID)
		if err != nil {
			return Extent{}, false, err
		}
		if sequence >= page.Header.FirstSequence {
			if sequence >= page.Header.NextSequence {
				return Extent{}, false, nil
			}
			index := sort.Search(len(page.Extents), func(i int) bool { return page.Extents[i].NextSequence > sequence })
			if index == len(page.Extents) || sequence < page.Extents[index].FirstSequence {
				return Extent{}, false, fmt.Errorf("Locator Page does not cover Sequence %d", sequence)
			}
			entry := page.Extents[index]
			reference, ok := s.segments[entry.SegmentID]
			if !ok {
				return Extent{}, false, fmt.Errorf("Locator references unknown Segment %x", entry.SegmentID)
			}
			return Extent{Reference: reference, Entry: entry}, true, nil
		}
		if page.Header.Flags&format.ExtentPageHasPrevious == 0 {
			return Extent{}, false, nil
		}
		pointer = pageKey{packID: page.Header.PreviousPackID, ordinal: page.Header.PreviousPageOrdinal}
	}
}

func (s *Store) lookupRoot(streamID uint64) (format.LocatorRootEntry, bool, error) {
	s.snapshotMu.RLock()
	closed := s.snapshotFile == nil
	s.snapshotMu.RUnlock()
	if closed {
		return format.LocatorRootEntry{}, false, os.ErrClosed
	}
	s.mu.Lock()
	if element := s.rootCache[streamID]; element != nil {
		s.rootLRU.MoveToFront(element)
		entry := element.Value.(cachedRoot).entry
		s.mu.Unlock()
		return entry, true, nil
	}
	s.mu.Unlock()
	low, high := uint32(0), s.header.RootCount
	for low < high {
		mid := low + (high-low)/2
		entry, err := s.rootAt(mid)
		if err != nil {
			return format.LocatorRootEntry{}, false, err
		}
		if mid > 0 {
			previous, previousErr := s.rootAtRaw(mid - 1)
			if previousErr != nil {
				return format.LocatorRootEntry{}, false, previousErr
			}
			if previous.StreamID >= entry.StreamID {
				return format.LocatorRootEntry{}, false, fmt.Errorf("Locator Roots are not strictly sorted near ordinal %d", mid)
			}
		}
		if mid+1 < s.header.RootCount {
			next, nextErr := s.rootAtRaw(mid + 1)
			if nextErr != nil {
				return format.LocatorRootEntry{}, false, nextErr
			}
			if entry.StreamID >= next.StreamID {
				return format.LocatorRootEntry{}, false, fmt.Errorf("Locator Roots are not strictly sorted near ordinal %d", mid)
			}
		}
		if entry.StreamID < streamID {
			low = mid + 1
		} else {
			high = mid
		}
	}
	if low == s.header.RootCount {
		return format.LocatorRootEntry{}, false, nil
	}
	entry, err := s.rootAt(low)
	if err != nil {
		return format.LocatorRootEntry{}, false, err
	}
	if entry.StreamID != streamID {
		return format.LocatorRootEntry{}, false, nil
	}
	s.cacheRoot(entry)
	return entry, true, nil
}

func (s *Store) rootAt(ordinal uint32) (format.LocatorRootEntry, error) {
	entry, err := s.rootAtRaw(ordinal)
	if err != nil {
		return format.LocatorRootEntry{}, err
	}
	pack, ok := s.packs[entry.PackID]
	if !ok || uint64(entry.PageOrdinal) >= pack.PageCount {
		return format.LocatorRootEntry{}, fmt.Errorf("Locator Root for Stream %d is outside known Packs", entry.StreamID)
	}
	return entry, nil
}

func (s *Store) rootAtRaw(ordinal uint32) (format.LocatorRootEntry, error) {
	if ordinal >= s.header.RootCount {
		return format.LocatorRootEntry{}, fmt.Errorf("Locator Root ordinal %d is out of range", ordinal)
	}
	encoded := make([]byte, format.LocatorRootEntryLength)
	s.snapshotMu.RLock()
	defer s.snapshotMu.RUnlock()
	if s.snapshotFile == nil {
		return format.LocatorRootEntry{}, os.ErrClosed
	}
	position := s.rootsOffset + int64(ordinal)*format.LocatorRootEntryLength
	n, err := s.snapshotFile.ReadAt(encoded, position)
	if n != len(encoded) || (err != nil && err != io.EOF) {
		if err == nil {
			err = io.ErrUnexpectedEOF
		}
		return format.LocatorRootEntry{}, err
	}
	return format.UnmarshalLocatorRootEntry(encoded)
}

func (s *Store) cacheRoot(entry format.LocatorRootEntry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if element := s.rootCache[entry.StreamID]; element != nil {
		s.rootLRU.MoveToFront(element)
		return
	}
	element := s.rootLRU.PushFront(cachedRoot{streamID: entry.StreamID, entry: entry})
	s.rootCache[entry.StreamID] = element
	if s.rootLRU.Len() > s.rootCapacity {
		old := s.rootLRU.Back()
		delete(s.rootCache, old.Value.(cachedRoot).streamID)
		s.rootLRU.Remove(old)
	}
}

func (s *Store) page(key pageKey, streamID uint64) (format.ExtentPage, error) {
	s.mu.Lock()
	if element := s.cache[key]; element != nil {
		s.lru.MoveToFront(element)
		page := element.Value.(cachedPage).page
		s.mu.Unlock()
		if page.Header.StreamID != streamID {
			return format.ExtentPage{}, fmt.Errorf("Locator Page belongs to Stream %d, not %d", page.Header.StreamID, streamID)
		}
		return page, nil
	}
	s.mu.Unlock()
	page, err := s.readPage(key, streamID)
	if err != nil {
		return format.ExtentPage{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if element := s.cache[key]; element != nil {
		s.lru.MoveToFront(element)
		return element.Value.(cachedPage).page, nil
	}
	element := s.lru.PushFront(cachedPage{key: key, page: page})
	s.cache[key] = element
	if s.lru.Len() > s.capacity {
		old := s.lru.Back()
		delete(s.cache, old.Value.(cachedPage).key)
		s.lru.Remove(old)
	}
	return page, nil
}

func (s *Store) readPage(key pageKey, streamID uint64) (format.ExtentPage, error) {
	pack, ok := s.packs[key.packID]
	if !ok || uint64(key.ordinal) >= pack.PageCount {
		return format.ExtentPage{}, fmt.Errorf("Locator Page pointer is outside known Packs")
	}
	file, err := os.Open(filepath.Join(s.root, filepath.FromSlash(pack.Path)))
	if err != nil {
		return format.ExtentPage{}, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || uint64(info.Size()) != pack.FileSize {
		return format.ExtentPage{}, fmt.Errorf("Locator Pack size does not match Snapshot")
	}
	headerBytes := make([]byte, format.LocatorPackHeaderLength)
	if _, err = file.ReadAt(headerBytes, 0); err != nil {
		return format.ExtentPage{}, err
	}
	header, err := format.UnmarshalLocatorPackHeader(headerBytes)
	if err != nil || header.ArtifactID != pack.PackID || header.PageCount != pack.PageCount || header.CoveredEntryID != s.header.CoveredEntryID {
		return format.ExtentPage{}, fmt.Errorf("Locator Pack Header does not match Snapshot")
	}
	footerBytes := make([]byte, format.ArtifactFooterLength)
	if _, err = file.ReadAt(footerBytes, info.Size()-format.ArtifactFooterLength); err != nil {
		return format.ExtentPage{}, err
	}
	footer, err := format.UnmarshalArtifactFooter(footerBytes)
	if err != nil || footer.ArtifactType != format.ArtifactLocatorPack || footer.ArtifactID != pack.PackID || footer.FileLength != pack.FileSize || footer.ContentSHA256 != pack.ContentSHA256 {
		return format.ExtentPage{}, fmt.Errorf("Locator Pack Footer does not match Snapshot")
	}
	position, err := format.LocatorPagePosition(key.ordinal)
	if err != nil {
		return format.ExtentPage{}, err
	}
	encoded := make([]byte, format.LocatorPageLength)
	if _, err = file.ReadAt(encoded, int64(position)); err != nil {
		return format.ExtentPage{}, err
	}
	page, err := format.UnmarshalExtentPage(encoded)
	if err != nil {
		return format.ExtentPage{}, err
	}
	if page.Header.StreamID != streamID {
		return format.ExtentPage{}, fmt.Errorf("Locator Page belongs to Stream %d, not %d", page.Header.StreamID, streamID)
	}
	return page, nil
}

func (s *Store) PackArtifacts() []format.ArtifactReference {
	out := make([]format.ArtifactReference, 0, len(s.packReferences))
	for _, pack := range s.packReferences {
		out = append(out, format.ArtifactReference{ArtifactType: format.ArtifactLocatorPack, FormatVersion: format.VersionV1, ArtifactID: pack.PackID, FileSize: pack.FileSize, CoveredEntryID: s.header.CoveredEntryID, Path: pack.Path, ContentSHA256: pack.ContentSHA256})
	}
	return out
}

func (s *Store) Reference() format.ArtifactReference { return s.reference }

func (s *Store) CacheLen() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lru.Len()
}

func (s *Store) RootCacheLen() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.rootLRU.Len()
}

func (s *Store) ClearCache() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cache = make(map[pageKey]*list.Element)
	s.lru.Init()
	s.rootCache = make(map[uint64]*list.Element)
	s.rootLRU.Init()
}

func (s *Store) Close() error {
	s.snapshotMu.Lock()
	defer s.snapshotMu.Unlock()
	if s.snapshotFile == nil {
		return nil
	}
	err := s.snapshotFile.Close()
	s.snapshotFile = nil
	return err
}
