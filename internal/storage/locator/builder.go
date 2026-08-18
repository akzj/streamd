package locator

import (
	"bufio"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"

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

// BuildCheckpoint external-sorts Segment Directory entries into bounded
// fan-in runs, then emits Locator Pages one page at a time. Memory is bounded
// by one Segment Directory, the merge fan-in, one Page, and the Root map that
// is still shared with the Tail Catalog builder.
func BuildCheckpoint(root string, snapshotID, packID, tailCatalogID format.UUID, manifestGeneration, coveredEntryID uint64, descriptors []segment.Descriptor) (BuildResult, error) {
	locatorDir := filepath.Join(root, "locator")
	if err := os.MkdirAll(locatorDir, 0750); err != nil {
		return BuildResult{}, err
	}
	buildDir, err := os.MkdirTemp(locatorDir, ".build-")
	if err != nil {
		return BuildResult{}, err
	}
	defer os.RemoveAll(buildDir)
	runPath, err := buildExtentRun(root, buildDir, descriptors)
	if err != nil {
		return BuildResult{}, err
	}
	pageCount, rootCount, err := countExtentPages(runPath)
	if err != nil {
		return BuildResult{}, err
	}
	if pageCount == 0 || pageCount > math.MaxUint32 || rootCount > math.MaxUint32 {
		return BuildResult{}, fmt.Errorf("Locator Pack counts are invalid")
	}
	packReference, rootsPath, roots, err := writeLocatorPack(root, buildDir, packID, coveredEntryID, pageCount, rootCount, runPath)
	if err != nil {
		return BuildResult{}, err
	}
	reference, err := writeLocatorSnapshot(root, snapshotID, tailCatalogID, manifestGeneration, coveredEntryID, rootCount, packReference, rootsPath)
	if err != nil {
		return BuildResult{}, err
	}
	return BuildResult{Reference: reference, Pack: packReference, Roots: roots}, nil
}

func countExtentPages(runPath string) (pageCount, rootCount uint64, resultErr error) {
	var previous *extentRecord
	var streamExtents uint64
	finish := func() error {
		if previous == nil {
			return nil
		}
		pages := (streamExtents + maxExtentsPerPage - 1) / maxExtentsPerPage
		if pages > math.MaxUint64-pageCount {
			return fmt.Errorf("Locator Page count overflows")
		}
		pageCount += pages
		rootCount++
		return nil
	}
	err := scanExtentRun(runPath, func(record extentRecord) error {
		if record.directory.RecordCount > math.MaxUint64-record.directory.FirstSequence {
			return fmt.Errorf("Stream %d extent Sequence overflows", record.directory.StreamID)
		}
		if previous == nil || previous.directory.StreamID != record.directory.StreamID {
			if err := finish(); err != nil {
				return err
			}
			streamExtents = 1
		} else {
			if err := validateExtentContinuity(*previous, record); err != nil {
				return err
			}
			streamExtents++
		}
		copy := record
		previous = &copy
		return nil
	})
	if err != nil {
		return 0, 0, err
	}
	if err = finish(); err != nil {
		return 0, 0, err
	}
	return pageCount, rootCount, nil
}

func validateExtentContinuity(previous, next extentRecord) error {
	p, n := previous.directory, next.directory
	if p.NextByteOffset != n.FirstByteOffset || p.LastRecordedAt > n.FirstRecordedAt || p.RecordCount > math.MaxUint64-p.FirstSequence || p.FirstSequence+p.RecordCount != n.FirstSequence {
		return fmt.Errorf("Stream %d Extents are not continuous", n.StreamID)
	}
	return nil
}

func extentEntry(record extentRecord) format.ExtentEntry {
	directory := record.directory
	return format.ExtentEntry{
		SegmentID: record.segmentID, FirstSequence: directory.FirstSequence,
		NextSequence: directory.FirstSequence + directory.RecordCount, FirstByteOffset: directory.FirstByteOffset,
		NextByteOffset: directory.NextByteOffset, FirstRecordedAt: directory.FirstRecordedAt,
		LastRecordedAt: directory.LastRecordedAt, RecordIndexOffset: directory.RecordIndexOffset,
		StreamDataOffset: directory.StreamDataOffset,
	}
}

func writeLocatorPack(root, buildDir string, packID format.UUID, coveredEntryID, pageCount, rootCount uint64, runPath string) (reference format.LocatorPackReference, rootsPath string, roots map[uint64]Pointer, resultErr error) {
	packName := fmt.Sprintf("EXTENTS-%x.loc", packID)
	locatorDir := filepath.Join(root, "locator")
	staging := filepath.Join(locatorDir, "."+packName+".tmp")
	final := filepath.Join(locatorDir, packName)
	file, err := os.OpenFile(staging, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0640)
	if err != nil {
		return reference, "", nil, err
	}
	keep := false
	fileClosed := false
	defer func() {
		if !fileClosed {
			resultErr = errors.Join(resultErr, file.Close())
		}
		if !keep {
			_ = os.Remove(staging)
		}
	}()
	headerBytes, err := format.MarshalLocatorPackHeader(format.LocatorPackHeader{ArtifactID: packID, PageCount: pageCount, CreatedAt: 0, CoveredEntryID: coveredEntryID})
	if err != nil {
		return reference, "", nil, err
	}
	digest := sha256.New()
	writer := bufio.NewWriterSize(io.MultiWriter(file, digest), 256*1024)
	if _, err = writer.Write(headerBytes); err == nil {
		_, err = writer.Write(make([]byte, format.SegmentSectionAlignment-len(headerBytes)))
	}
	if err != nil {
		return reference, "", nil, err
	}
	rootsPath = filepath.Join(buildDir, "roots.bin")
	rootFile, err := os.OpenFile(rootsPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return reference, "", nil, err
	}
	rootWriter := bufio.NewWriterSize(rootFile, 128*1024)
	rootClosed := false
	defer func() {
		if !rootClosed {
			resultErr = errors.Join(resultErr, rootWriter.Flush(), rootFile.Close())
		}
	}()
	roots = make(map[uint64]Pointer, rootCount)
	pageExtents := make([]format.ExtentEntry, 0, maxExtentsPerPage)
	var streamID uint64
	var hasStream bool
	var previousPage uint32
	var ordinal uint32
	var streamPages uint32
	flushPage := func() error {
		if len(pageExtents) == 0 {
			return nil
		}
		pageID, err := NewID()
		if err != nil {
			return err
		}
		page := format.ExtentPage{Header: format.ExtentPageHeader{PageID: pageID, StreamID: streamID, FirstSequence: pageExtents[0].FirstSequence, NextSequence: pageExtents[len(pageExtents)-1].NextSequence, FirstRecordedAt: pageExtents[0].FirstRecordedAt, LastRecordedAt: pageExtents[len(pageExtents)-1].LastRecordedAt}, Extents: pageExtents}
		if streamPages > 0 {
			page.Header.Flags = format.ExtentPageHasPrevious
			page.Header.PreviousPackID = packID
			page.Header.PreviousPageOrdinal = previousPage
		}
		encoded, err := format.MarshalExtentPage(page)
		if err != nil {
			return err
		}
		if _, err = writer.Write(encoded); err != nil {
			return err
		}
		previousPage = ordinal
		ordinal++
		streamPages++
		pageExtents = pageExtents[:0]
		return nil
	}
	finishStream := func() error {
		if !hasStream {
			return nil
		}
		if err := flushPage(); err != nil {
			return err
		}
		pointer := Pointer{PackID: packID, PageOrdinal: previousPage}
		roots[streamID] = pointer
		encoded, err := format.MarshalLocatorRootEntry(format.LocatorRootEntry{StreamID: streamID, PackID: packID, PageOrdinal: previousPage})
		if err == nil {
			_, err = rootWriter.Write(encoded)
		}
		return err
	}
	err = scanExtentRun(runPath, func(record extentRecord) error {
		if !hasStream || streamID != record.directory.StreamID {
			if err := finishStream(); err != nil {
				return err
			}
			streamID = record.directory.StreamID
			hasStream = true
			streamPages = 0
		}
		pageExtents = append(pageExtents, extentEntry(record))
		if len(pageExtents) == maxExtentsPerPage {
			return flushPage()
		}
		return nil
	})
	if err == nil {
		err = finishStream()
	}
	if err != nil {
		return reference, "", nil, err
	}
	if uint64(ordinal) != pageCount || uint64(len(roots)) != rootCount {
		return reference, "", nil, fmt.Errorf("Locator counts changed while writing")
	}
	if err = rootWriter.Flush(); err != nil {
		return reference, "", nil, err
	}
	if err = rootFile.Close(); err != nil {
		return reference, "", nil, err
	}
	rootClosed = true
	if err = writer.Flush(); err != nil {
		return reference, "", nil, err
	}
	contentLength, err := format.LocatorPagePosition(ordinal)
	if err != nil || contentLength > math.MaxUint64-format.ArtifactFooterLength {
		return reference, "", nil, fmt.Errorf("Locator Pack length overflows")
	}
	var contentSHA [sha256.Size]byte
	copy(contentSHA[:], digest.Sum(nil))
	footer := format.ArtifactFooter{ArtifactType: format.ArtifactLocatorPack, ArtifactID: packID, FileLength: contentLength + format.ArtifactFooterLength, ContentLength: contentLength, ContentSHA256: contentSHA}
	footerBytes, err := format.MarshalArtifactFooter(footer)
	if err == nil {
		_, err = file.Write(footerBytes)
	}
	if err == nil {
		err = file.Sync()
	}
	if err == nil {
		err = file.Close()
		fileClosed = err == nil
	}
	if err == nil {
		err = os.Rename(staging, final)
	}
	if err != nil {
		return reference, "", nil, err
	}
	keep = true
	if err = fsutil.SyncDir(locatorDir); err != nil {
		return reference, "", nil, err
	}
	reference = format.LocatorPackReference{PackID: packID, FileSize: footer.FileLength, PageCount: pageCount, ContentSHA256: contentSHA, Path: filepath.ToSlash(filepath.Join("locator", packName))}
	return reference, rootsPath, roots, nil
}

func writeLocatorSnapshot(root string, snapshotID, tailCatalogID format.UUID, manifestGeneration, coveredEntryID, rootCount uint64, pack format.LocatorPackReference, rootsPath string) (reference format.ArtifactReference, resultErr error) {
	if rootCount > math.MaxUint32 {
		return reference, fmt.Errorf("Locator Root count overflows")
	}
	headerBytes, err := format.MarshalLocatorSnapshotHeader(format.LocatorSnapshotHeader{ArtifactID: snapshotID, ManifestGeneration: manifestGeneration, CoveredEntryID: coveredEntryID, TailCatalogArtifactID: tailCatalogID, PackCount: 1, RootCount: uint32(rootCount), CreatedAt: 0})
	if err != nil {
		return reference, err
	}
	packBytes, err := format.MarshalLocatorPackReference(pack)
	if err != nil {
		return reference, err
	}
	locatorDir := filepath.Join(root, "locator")
	name := fmt.Sprintf("LOCATOR-%x.snapshot", snapshotID)
	staging := filepath.Join(locatorDir, "."+name+".tmp")
	final := filepath.Join(locatorDir, name)
	file, err := os.OpenFile(staging, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0640)
	if err != nil {
		return reference, err
	}
	keep := false
	fileClosed := false
	defer func() {
		if !fileClosed {
			resultErr = errors.Join(resultErr, file.Close())
		}
		if !keep {
			_ = os.Remove(staging)
		}
	}()
	digest := sha256.New()
	writer := bufio.NewWriterSize(io.MultiWriter(file, digest), 128*1024)
	if _, err = writer.Write(headerBytes); err == nil {
		_, err = writer.Write(packBytes)
	}
	rootFile, openErr := os.Open(rootsPath)
	if err == nil {
		err = openErr
	}
	if rootFile != nil {
		defer rootFile.Close()
	}
	var copied int64
	if err == nil {
		copied, err = io.Copy(writer, rootFile)
		if err == nil && uint64(copied) != rootCount*format.LocatorRootEntryLength {
			err = fmt.Errorf("Locator Root run length changed while writing Snapshot")
		}
	}
	if err == nil {
		err = writer.Flush()
	}
	if err != nil {
		return reference, err
	}
	contentLength := uint64(len(headerBytes)+len(packBytes)) + rootCount*format.LocatorRootEntryLength
	var contentSHA [sha256.Size]byte
	copy(contentSHA[:], digest.Sum(nil))
	footer := format.ArtifactFooter{ArtifactType: format.ArtifactLocatorSnapshot, ArtifactID: snapshotID, FileLength: contentLength + format.ArtifactFooterLength, ContentLength: contentLength, ContentSHA256: contentSHA}
	footerBytes, err := format.MarshalArtifactFooter(footer)
	if err == nil {
		_, err = file.Write(footerBytes)
	}
	if err == nil {
		err = file.Sync()
	}
	if err == nil {
		err = file.Close()
		fileClosed = err == nil
	}
	if err == nil {
		err = os.Rename(staging, final)
	}
	if err != nil {
		return reference, err
	}
	keep = true
	if err = fsutil.SyncDir(locatorDir); err != nil {
		return reference, err
	}
	return format.ArtifactReference{ArtifactType: format.ArtifactLocatorSnapshot, FormatVersion: format.VersionV1, ArtifactID: snapshotID, FileSize: footer.FileLength, CoveredEntryID: coveredEntryID, Path: filepath.ToSlash(filepath.Join("locator", name)), ContentSHA256: contentSHA}, nil
}
