package registry

import (
	"bufio"
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

type registryLayout struct {
	entryCount    uint64
	blockCount    uint32
	entriesOffset uint64
	contentLength uint64
}

// BuildCheckpointFromSegments reconstructs the Registry projection from its
// authoritative Stream using bounded external runs and streaming file output.
func BuildCheckpointFromSegments(root string, artifactID format.UUID, coveredEntryID uint64, createdAt int64, descriptors []segment.Descriptor) (format.ArtifactReference, error) {
	directory := filepath.Join(root, "registry")
	if err := os.MkdirAll(directory, 0750); err != nil {
		return format.ArtifactReference{}, err
	}
	buildDir, err := os.MkdirTemp(directory, ".build-")
	if err != nil {
		return format.ArtifactReference{}, err
	}
	defer os.RemoveAll(buildDir)
	runPath, err := buildRegistryRun(root, buildDir, descriptors)
	if err != nil {
		return format.ArtifactReference{}, err
	}
	layout, err := inspectRegistryRun(runPath, coveredEntryID)
	if err != nil {
		return format.ArtifactReference{}, err
	}
	return writeRegistrySnapshot(root, artifactID, coveredEntryID, createdAt, layout, runPath)
}

func inspectRegistryRun(runPath string, coveredEntryID uint64) (registryLayout, error) {
	var layout registryLayout
	var indexLength uint64
	var entryBytes uint64
	var previous *format.RegistryEntry
	err := scanRegistryRun(runPath, func(entry format.RegistryEntry) error {
		if entry.CreatedEntryID > coveredEntryID {
			return fmt.Errorf("Registry Entry is after checkpoint")
		}
		if previous != nil && compareEntries(*previous, entry) >= 0 {
			return fmt.Errorf("Registry keys are duplicate or unsorted")
		}
		encoded, err := format.MarshalRegistryEntry(entry)
		if err != nil {
			return err
		}
		if layout.entryCount%format.RegistryBlockEntriesV1 == 0 {
			indexEntry, indexErr := format.MarshalRegistryBlockIndexEntry(format.RegistryBlockIndexEntry{EntryCount: 1, EntriesOffset: 1, FirstNamespace: entry.Namespace, FirstStreamName: entry.StreamName})
			if indexErr != nil || uint64(len(indexEntry)) > math.MaxUint64-indexLength {
				return fmt.Errorf("Registry Block Index length overflows")
			}
			indexLength += uint64(len(indexEntry))
		}
		if uint64(len(encoded)) > math.MaxUint64-entryBytes {
			return fmt.Errorf("Registry Entry bytes overflow")
		}
		entryBytes += uint64(len(encoded))
		if layout.entryCount == math.MaxUint64 {
			return fmt.Errorf("Registry Entry count overflows")
		}
		layout.entryCount++
		copy := entry
		previous = &copy
		return nil
	})
	if err != nil {
		return registryLayout{}, err
	}
	var blocks uint64
	if layout.entryCount > 0 {
		blocks = (layout.entryCount-1)/format.RegistryBlockEntriesV1 + 1
	}
	if blocks > math.MaxUint32 || indexLength > math.MaxUint64-format.RegistrySnapshotHeaderLength {
		return registryLayout{}, fmt.Errorf("Registry Snapshot counts overflow")
	}
	layout.blockCount = uint32(blocks)
	layout.entriesOffset = format.RegistrySnapshotHeaderLength + indexLength
	if entryBytes > math.MaxUint64-layout.entriesOffset || layout.entriesOffset+entryBytes > math.MaxInt64-format.ArtifactFooterLength {
		return registryLayout{}, fmt.Errorf("Registry Snapshot file length overflows")
	}
	layout.contentLength = layout.entriesOffset + entryBytes
	return layout, nil
}

func writeRegistrySnapshot(root string, artifactID format.UUID, coveredEntryID uint64, createdAt int64, layout registryLayout, runPath string) (reference format.ArtifactReference, resultErr error) {
	header := format.RegistrySnapshotHeader{ArtifactID: artifactID, CoveredEntryID: coveredEntryID, EntryCount: layout.entryCount, BlockCount: layout.blockCount, BlockIndexOffset: format.RegistrySnapshotHeaderLength, EntriesOffset: layout.entriesOffset, CreatedAt: createdAt}
	headerBytes, err := format.MarshalRegistrySnapshotHeader(header)
	if err != nil {
		return reference, err
	}
	directory := filepath.Join(root, "registry")
	name := fmt.Sprintf("REGISTRY-%x.reg", artifactID)
	staging := filepath.Join(directory, "."+name+".tmp")
	final := filepath.Join(directory, name)
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
	writer := bufio.NewWriterSize(io.MultiWriter(file, digest), 256*1024)
	if _, err = writer.Write(headerBytes); err != nil {
		return reference, err
	}
	var ordinal uint64
	entryOffset := layout.entriesOffset
	err = scanRegistryRun(runPath, func(entry format.RegistryEntry) error {
		encoded, encodeErr := format.MarshalRegistryEntry(entry)
		if encodeErr != nil {
			return encodeErr
		}
		if ordinal%format.RegistryBlockEntriesV1 == 0 {
			remaining := layout.entryCount - ordinal
			count := min(remaining, uint64(format.RegistryBlockEntriesV1))
			indexBytes, indexErr := format.MarshalRegistryBlockIndexEntry(format.RegistryBlockIndexEntry{EntryCount: uint32(count), EntriesOffset: entryOffset, FirstNamespace: entry.Namespace, FirstStreamName: entry.StreamName})
			if indexErr != nil {
				return indexErr
			}
			if _, indexErr = writer.Write(indexBytes); indexErr != nil {
				return indexErr
			}
		}
		entryOffset += uint64(len(encoded))
		ordinal++
		return nil
	})
	if err != nil {
		return reference, err
	}
	if ordinal != layout.entryCount || entryOffset != layout.contentLength {
		return reference, fmt.Errorf("Registry layout changed while writing Index")
	}
	ordinal = 0
	err = scanRegistryRun(runPath, func(entry format.RegistryEntry) error {
		encoded, encodeErr := format.MarshalRegistryEntry(entry)
		if encodeErr == nil {
			_, encodeErr = writer.Write(encoded)
		}
		ordinal++
		return encodeErr
	})
	if err != nil {
		return reference, err
	}
	if ordinal != layout.entryCount {
		return reference, fmt.Errorf("Registry Entry count changed while writing")
	}
	if err = writer.Flush(); err != nil {
		return reference, err
	}
	var contentSHA [sha256.Size]byte
	copy(contentSHA[:], digest.Sum(nil))
	footer := format.ArtifactFooter{ArtifactType: format.ArtifactRegistrySnapshot, ArtifactID: artifactID, FileLength: layout.contentLength + format.ArtifactFooterLength, ContentLength: layout.contentLength, ContentSHA256: contentSHA}
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
	if err = fsutil.SyncDir(directory); err != nil {
		return reference, err
	}
	return format.ArtifactReference{ArtifactType: format.ArtifactRegistrySnapshot, FormatVersion: format.VersionV1, ArtifactID: artifactID, FileSize: footer.FileLength, CoveredEntryID: coveredEntryID, Path: filepath.ToSlash(filepath.Join("registry", name)), ContentSHA256: contentSHA}, nil
}
