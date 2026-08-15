package scrub

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"

	"github.com/akzj/streamd/internal/storage/format"
	"github.com/akzj/streamd/internal/storage/fsutil"
	"github.com/akzj/streamd/internal/storage/identity"
	manifeststore "github.com/akzj/streamd/internal/storage/manifest"
	"github.com/akzj/streamd/internal/storage/segment"
	"github.com/akzj/streamd/internal/storage/wal"
)

type Report struct {
	ManifestGeneration uint64
	Segments           uint64
	Records            uint64
	Streams            uint64
	Artifacts          uint64
	SealedWALs         uint64
	ActiveWALEntries   uint64
}

type extent struct {
	directory format.StreamDirectoryEntry
}

func DataRoot(path string) (Report, error) {
	root, err := fsutil.LockExistingRoot(path)
	if err != nil {
		return Report{}, err
	}
	defer root.Close()
	if _, err = identity.Load(root.Path()); err != nil {
		return Report{}, fmt.Errorf("NODE: %w", err)
	}
	manifests, err := manifeststore.Open(root.Path())
	if err != nil {
		return Report{}, err
	}
	current, hasManifest := manifests.Current()
	report := Report{}
	byStream := make(map[uint64][]extent)
	if hasManifest {
		report.ManifestGeneration = current.Header.Generation
		for _, reference := range current.SegmentReferences {
			if reference.Flags&format.SegmentRefHasLocal == 0 {
				return report, fmt.Errorf("Segment %x has no local copy", reference.SegmentID)
			}
			metadata, scrubErr := segment.ScrubFile(filepath.Join(root.Path(), reference.LocalPath))
			if scrubErr != nil {
				return report, fmt.Errorf("Segment %x: %w", reference.SegmentID, scrubErr)
			}
			if metadata.Header.SegmentID != reference.SegmentID || metadata.Footer.FileLength != reference.FileSize || metadata.Header.FirstEntryID != reference.FirstEntryID || metadata.Header.LastEntryID != reference.LastEntryID || metadata.Header.StreamCount != reference.StreamCount || metadata.Header.RecordCount != reference.RecordCount || metadata.Footer.ContentSHA256 != reference.ContentSHA256 {
				return report, fmt.Errorf("Segment %x does not match Manifest Reference", reference.SegmentID)
			}
			report.Segments++
			report.Records += metadata.Header.RecordCount
			for _, directory := range metadata.Directories {
				byStream[directory.StreamID] = append(byStream[directory.StreamID], extent{directory: directory})
			}
		}
		for _, reference := range current.ArtifactReferences {
			if err = scrubArtifact(root.Path(), reference); err != nil {
				return report, err
			}
			report.Artifacts++
		}
	}
	for streamID, extents := range byStream {
		slices.SortFunc(extents, func(a, b extent) int {
			if a.directory.FirstSequence < b.directory.FirstSequence {
				return -1
			}
			if a.directory.FirstSequence > b.directory.FirstSequence {
				return 1
			}
			return 0
		})
		for i := 1; i < len(extents); i++ {
			previous, next := extents[i-1].directory, extents[i].directory
			if next.FirstSequence != previous.FirstSequence+previous.RecordCount || next.FirstByteOffset != previous.NextByteOffset || next.FirstRecordedAt < previous.LastRecordedAt {
				return report, fmt.Errorf("Stream %d Segment extents are not continuous", streamID)
			}
		}
	}
	report.Streams = uint64(len(byStream))
	if err = scrubWAL(root.Path(), current, hasManifest, &report); err != nil {
		return report, err
	}
	return report, nil
}

func scrubArtifact(root string, reference format.ArtifactReference) error {
	data, err := os.ReadFile(filepath.Join(root, reference.Path))
	if err != nil {
		return err
	}
	if uint64(len(data)) != reference.FileSize || len(data) < format.ArtifactFooterLength {
		return fmt.Errorf("Artifact %x size mismatch", reference.ArtifactID)
	}
	footer, err := format.VerifyArtifact(data[:len(data)-format.ArtifactFooterLength], data[len(data)-format.ArtifactFooterLength:], reference.ArtifactType, reference.ArtifactID)
	if err != nil {
		return err
	}
	if footer.ContentSHA256 != reference.ContentSHA256 {
		return fmt.Errorf("Artifact %x digest mismatch", reference.ArtifactID)
	}
	return nil
}

type walFile struct {
	active bool
	scan   wal.ScanResult
}

func scrubWAL(root string, manifest format.Manifest, hasManifest bool, report *Report) error {
	pointerBytes, err := os.ReadFile(filepath.Join(root, "WAL-CURRENT"))
	if err != nil {
		return err
	}
	pointer, err := format.UnmarshalWALCurrentPointer(pointerBytes)
	if err != nil {
		return err
	}
	paths, err := filepath.Glob(filepath.Join(root, "wal", "*.log"))
	if err != nil {
		return err
	}
	files := make([]walFile, 0, len(paths))
	for _, path := range paths {
		if filepath.Base(path) == pointer.FileName {
			file, openErr := os.Open(path)
			if openErr != nil {
				return openErr
			}
			scan, scanErr := wal.ScanActive(file)
			file.Close()
			if scanErr != nil {
				return scanErr
			}
			if scan.TruncatedBytes != 0 || scan.Header.FileID != pointer.FileID || scan.Header.FirstEntryID != pointer.FirstEntryID {
				return fmt.Errorf("active WAL is truncated or does not match WAL-CURRENT")
			}
			report.ActiveWALEntries = scan.EntryCount
			files = append(files, walFile{active: true, scan: scan})
			continue
		}
		scan, scanErr := wal.ScanSealed(path, nil)
		if scanErr != nil {
			file, openErr := os.Open(path)
			if openErr != nil {
				return openErr
			}
			orphan, activeErr := wal.ScanActive(file)
			file.Close()
			if activeErr == nil && orphan.EntryCount == 0 {
				continue
			}
			return scanErr
		}
		report.SealedWALs++
		files = append(files, walFile{scan: scan})
	}
	if len(files) == 0 {
		return fmt.Errorf("WAL-CURRENT references no WAL file")
	}
	slices.SortFunc(files, func(a, b walFile) int {
		if a.scan.Header.FirstEntryID < b.scan.Header.FirstEntryID {
			return -1
		}
		if a.scan.Header.FirstEntryID > b.scan.Header.FirstEntryID {
			return 1
		}
		return 0
	})
	foundActive := false
	checkpointVerified := !hasManifest || manifest.Header.RecordCount == 0
	for i, file := range files {
		if file.active {
			foundActive = true
		}
		if hasManifest && manifest.Header.RecordCount > 0 && file.scan.EntryCount > 0 && file.scan.LastEntryID == manifest.Header.LastEntryID && file.scan.LastEntryCRC32C == manifest.Header.LastEntryCRC32C {
			checkpointVerified = true
		}
		if i == 0 {
			continue
		}
		previous := files[i-1].scan
		if previous.EntryCount > 0 && file.scan.Header.FirstEntryID != previous.LastEntryID+1 {
			return fmt.Errorf("retained WAL files are not continuous")
		}
		if previous.EntryCount > 0 && file.scan.EntryCount > 0 && file.scan.FirstEntryPreviousCRC32C != previous.LastEntryCRC32C {
			return fmt.Errorf("retained WAL CRC chain is not continuous")
		}
	}
	if !foundActive {
		return fmt.Errorf("WAL-CURRENT target is missing")
	}
	if !checkpointVerified {
		return fmt.Errorf("Manifest checkpoint is not verifiable from retained WAL")
	}
	return nil
}
