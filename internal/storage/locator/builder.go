package locator

import (
	"bufio"
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"

	"github.com/akzj/streamd/internal/storage/format"
	"github.com/akzj/streamd/internal/storage/fsutil"
	"github.com/akzj/streamd/internal/storage/segment"
)

const maxExtentsPerPage = (format.LocatorPageLength - 4 - format.ExtentPageHeaderLength) / format.ExtentEntryLength

type Pointer struct {
	PackID      format.UUID
	PageOrdinal uint32
}

type BuildResult struct {
	Reference format.ArtifactReference
	Pack      format.LocatorPackReference
	Roots     map[uint64]Pointer
}

func NewID() (format.UUID, error) {
	var id format.UUID
	_, err := rand.Read(id[:])
	return id, err
}

func BuildCheckpoint(root string, snapshotID, packID, tailCatalogID format.UUID, manifestGeneration, coveredEntryID uint64, descriptors []segment.Descriptor) (BuildResult, error) {
	byStream := make(map[uint64][]format.ExtentEntry)
	for _, descriptor := range descriptors {
		for _, directory := range descriptor.Directories {
			if directory.RecordCount > math.MaxUint64-directory.FirstSequence {
				return BuildResult{}, fmt.Errorf("Stream %d extent Sequence overflows", directory.StreamID)
			}
			byStream[directory.StreamID] = append(byStream[directory.StreamID], format.ExtentEntry{
				SegmentID: descriptor.Reference.SegmentID, FirstSequence: directory.FirstSequence,
				NextSequence: directory.FirstSequence + directory.RecordCount, FirstByteOffset: directory.FirstByteOffset,
				NextByteOffset: directory.NextByteOffset, FirstRecordedAt: directory.FirstRecordedAt,
				LastRecordedAt: directory.LastRecordedAt, RecordIndexOffset: directory.RecordIndexOffset,
				StreamDataOffset: directory.StreamDataOffset,
			})
		}
	}
	streamIDs := make([]uint64, 0, len(byStream))
	pageCount := uint64(0)
	for streamID, extents := range byStream {
		sort.Slice(extents, func(i, j int) bool { return extents[i].FirstSequence < extents[j].FirstSequence })
		for i := 1; i < len(extents); i++ {
			previous, next := extents[i-1], extents[i]
			if previous.NextSequence != next.FirstSequence || previous.NextByteOffset != next.FirstByteOffset || previous.LastRecordedAt > next.FirstRecordedAt {
				return BuildResult{}, fmt.Errorf("Stream %d Extents are not continuous", streamID)
			}
		}
		byStream[streamID] = extents
		streamIDs = append(streamIDs, streamID)
		pages := (len(extents) + maxExtentsPerPage - 1) / maxExtentsPerPage
		if uint64(pages) > math.MaxUint64-pageCount {
			return BuildResult{}, fmt.Errorf("Locator Page count overflows")
		}
		pageCount += uint64(pages)
	}
	if pageCount == 0 || pageCount > math.MaxUint32 {
		return BuildResult{}, fmt.Errorf("Locator Pack Page count is invalid")
	}
	sort.Slice(streamIDs, func(i, j int) bool { return streamIDs[i] < streamIDs[j] })
	locatorDir := filepath.Join(root, "locator")
	if err := os.MkdirAll(locatorDir, 0750); err != nil {
		return BuildResult{}, err
	}
	packName := fmt.Sprintf("EXTENTS-%x.loc", packID)
	staging := filepath.Join(locatorDir, "."+packName+".tmp")
	final := filepath.Join(locatorDir, packName)
	file, err := os.OpenFile(staging, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0640)
	if err != nil {
		return BuildResult{}, err
	}
	keep := false
	defer func() {
		_ = file.Close()
		if !keep {
			_ = os.Remove(staging)
		}
	}()
	header := format.LocatorPackHeader{ArtifactID: packID, PageCount: pageCount, CreatedAt: 0, CoveredEntryID: coveredEntryID}
	headerBytes, err := format.MarshalLocatorPackHeader(header)
	if err != nil {
		return BuildResult{}, err
	}
	digest := sha256.New()
	writer := bufio.NewWriterSize(io.MultiWriter(file, digest), 256*1024)
	if _, err = writer.Write(headerBytes); err != nil {
		return BuildResult{}, err
	}
	if _, err = writer.Write(make([]byte, format.SegmentSectionAlignment-len(headerBytes))); err != nil {
		return BuildResult{}, err
	}
	roots := make(map[uint64]Pointer, len(streamIDs))
	rootEntries := make([]format.LocatorRootEntry, 0, len(streamIDs))
	ordinal := uint32(0)
	for _, streamID := range streamIDs {
		extents := byStream[streamID]
		var previous uint32
		for start := 0; start < len(extents); start += maxExtentsPerPage {
			end := min(start+maxExtentsPerPage, len(extents))
			pageID, idErr := NewID()
			if idErr != nil {
				return BuildResult{}, idErr
			}
			pageExtents := extents[start:end]
			page := format.ExtentPage{Header: format.ExtentPageHeader{PageID: pageID, StreamID: streamID, FirstSequence: pageExtents[0].FirstSequence, NextSequence: pageExtents[len(pageExtents)-1].NextSequence, FirstRecordedAt: pageExtents[0].FirstRecordedAt, LastRecordedAt: pageExtents[len(pageExtents)-1].LastRecordedAt}, Extents: pageExtents}
			if start > 0 {
				page.Header.Flags = format.ExtentPageHasPrevious
				page.Header.PreviousPackID = packID
				page.Header.PreviousPageOrdinal = previous
			}
			encoded, encodeErr := format.MarshalExtentPage(page)
			if encodeErr != nil {
				return BuildResult{}, encodeErr
			}
			if _, err = writer.Write(encoded); err != nil {
				return BuildResult{}, err
			}
			previous = ordinal
			ordinal++
		}
		pointer := Pointer{PackID: packID, PageOrdinal: previous}
		roots[streamID] = pointer
		rootEntries = append(rootEntries, format.LocatorRootEntry{StreamID: streamID, PackID: packID, PageOrdinal: previous})
	}
	if uint64(ordinal) != pageCount {
		return BuildResult{}, fmt.Errorf("Locator Pack Page count changed while writing")
	}
	if err = writer.Flush(); err != nil {
		return BuildResult{}, err
	}
	contentLength, err := format.LocatorPagePosition(ordinal)
	if err != nil || contentLength > math.MaxUint64-format.ArtifactFooterLength {
		return BuildResult{}, fmt.Errorf("Locator Pack length overflows")
	}
	var contentSHA [sha256.Size]byte
	copy(contentSHA[:], digest.Sum(nil))
	footer := format.ArtifactFooter{ArtifactType: format.ArtifactLocatorPack, ArtifactID: packID, FileLength: contentLength + format.ArtifactFooterLength, ContentLength: contentLength, ContentSHA256: contentSHA}
	footerBytes, err := format.MarshalArtifactFooter(footer)
	if err != nil {
		return BuildResult{}, err
	}
	if _, err = file.Write(footerBytes); err != nil {
		return BuildResult{}, err
	}
	if err = file.Sync(); err != nil {
		return BuildResult{}, err
	}
	if err = file.Close(); err != nil {
		return BuildResult{}, err
	}
	if err = os.Rename(staging, final); err != nil {
		return BuildResult{}, err
	}
	keep = true
	if err = fsutil.SyncDir(locatorDir); err != nil {
		return BuildResult{}, err
	}
	packReference := format.LocatorPackReference{PackID: packID, FileSize: footer.FileLength, PageCount: pageCount, ContentSHA256: contentSHA, Path: filepath.ToSlash(filepath.Join("locator", packName))}
	snapshot := format.LocatorSnapshot{Header: format.LocatorSnapshotHeader{ArtifactID: snapshotID, ManifestGeneration: manifestGeneration, CoveredEntryID: coveredEntryID, TailCatalogArtifactID: tailCatalogID, CreatedAt: 0}, Packs: []format.LocatorPackReference{packReference}, Roots: rootEntries}
	snapshotBytes, err := format.MarshalLocatorSnapshot(snapshot)
	if err != nil {
		return BuildResult{}, err
	}
	verified, err := format.UnmarshalLocatorSnapshot(snapshotBytes)
	if err != nil {
		return BuildResult{}, err
	}
	snapshotName := fmt.Sprintf("LOCATOR-%x.snapshot", snapshotID)
	if err = fsutil.AtomicWrite(locatorDir, snapshotName, snapshotBytes, 0640, nil); err != nil {
		return BuildResult{}, err
	}
	return BuildResult{
		Reference: format.ArtifactReference{ArtifactType: format.ArtifactLocatorSnapshot, FormatVersion: format.VersionV1, ArtifactID: snapshotID, FileSize: uint64(len(snapshotBytes)), CoveredEntryID: coveredEntryID, Path: filepath.ToSlash(filepath.Join("locator", snapshotName)), ContentSHA256: verified.Footer.ContentSHA256},
		Pack:      packReference, Roots: roots,
	}, nil
}
