package snapshot

import (
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/akzj/streamd/internal/storage/engine"
	"github.com/akzj/streamd/internal/storage/format"
	"github.com/akzj/streamd/internal/storage/fsutil"
	"github.com/akzj/streamd/internal/storage/identity"
	"github.com/akzj/streamd/internal/storage/replicationstate"
	"github.com/akzj/streamd/internal/storage/segment"
)

type Result struct {
	Path               string
	SnapshotID         format.UUID
	GroupID            format.UUID
	Term               uint64
	ManifestGeneration uint64
	CheckpointEntryID  uint64
	CheckpointCRC32C   uint32
	Artifacts          uint64
}

func Create(dataRoot, destination string) (result Result, err error) {
	dataAbs, err := filepath.Abs(dataRoot)
	if err != nil {
		return result, err
	}
	node, err := identity.Load(dataAbs)
	if err != nil {
		return result, fmt.Errorf("NODE: %w", err)
	}
	term := uint64(0)
	states, err := replicationstate.Open(dataAbs, node)
	if err != nil {
		return result, fmt.Errorf("Replication State: %w", err)
	}
	if current, ok := states.Current(); ok {
		term = current.Header.Term
	}
	store, err := engine.Open(dataAbs)
	if err != nil {
		return result, err
	}
	defer func() { err = errors.Join(err, store.Close()) }()
	return createOnline(store, destination, term)
}

// CreateOnline takes a short engine checkpoint and then copies only immutable
// artifacts from that exact Manifest. Appends may continue while the files are
// copied because no later mutable CURRENT file is used as Snapshot input.
func CreateOnline(store *engine.Store, destination string) (result Result, err error) {
	if store == nil {
		return result, fmt.Errorf("Snapshot engine is required")
	}
	return createOnline(store, destination, store.Health().Term)
}

func createOnline(store *engine.Store, destination string, term uint64) (result Result, err error) {
	if store == nil {
		return result, fmt.Errorf("Snapshot engine is required")
	}
	dataAbs := store.DataRoot()
	destination, err = filepath.Abs(destination)
	if err != nil {
		return result, err
	}
	if inside(dataAbs, destination) && filepath.Dir(destination) != filepath.Join(dataAbs, "snapshots") {
		return result, fmt.Errorf("Snapshot destination cannot be inside the data root")
	}
	if _, err = os.Stat(destination); !errors.Is(err, os.ErrNotExist) {
		if err == nil {
			return result, fmt.Errorf("Snapshot destination already exists")
		}
		return result, err
	}
	manifest, _, releaseManifest, err := store.CheckpointAndPin()
	if err != nil {
		return result, err
	}
	defer releaseManifest()
	node, err := identity.Load(dataAbs)
	if err != nil {
		return result, fmt.Errorf("NODE: %w", err)
	}
	manifestFileName := fmt.Sprintf("MANIFEST-%020d-%x.bin", manifest.Header.Generation, manifest.Header.FileID)
	manifestBytes, err := os.ReadFile(filepath.Join(dataAbs, "manifests", manifestFileName))
	if err != nil {
		return result, err
	}
	verifiedManifest, err := format.UnmarshalManifest(manifestBytes)
	if err != nil {
		return result, err
	}
	if verifiedManifest.Header.FileID != manifest.Header.FileID || verifiedManifest.Footer.ContentSHA256 != manifest.Footer.ContentSHA256 {
		return result, fmt.Errorf("checkpoint Manifest changed while creating Snapshot")
	}
	if manifest.Header.RecordCount == 0 {
		return result, fmt.Errorf("empty data roots do not have an installable V1 Manifest")
	}
	parent := filepath.Dir(destination)
	staging, err := os.MkdirTemp(parent, ".streamd-snapshot-")
	if err != nil {
		return result, err
	}
	published := false
	defer func() {
		if !published {
			_ = os.RemoveAll(staging)
		}
	}()
	for _, directory := range []string{"manifests", "segments"} {
		if err = os.Mkdir(filepath.Join(staging, directory), 0750); err != nil {
			return result, err
		}
	}
	pointerBytes, err := format.MarshalCurrentPointer(format.CurrentPointer{Generation: manifest.Header.Generation, ManifestFileID: manifest.Header.FileID, ManifestSHA256: manifest.Footer.ContentSHA256, ManifestFileName: manifestFileName})
	if err != nil {
		return result, err
	}
	if err = fsutil.AtomicWrite(staging, "CURRENT", pointerBytes, 0640, nil); err != nil {
		return result, err
	}
	manifestName := filepath.ToSlash(filepath.Join("manifests", manifestFileName))
	if err = copyFile(filepath.Join(staging, filepath.FromSlash(manifestName)), filepath.Join(dataAbs, filepath.FromSlash(manifestName))); err != nil {
		return result, err
	}
	artifacts := []format.SnapshotArtifact{{ArtifactType: format.ArtifactManifest, FormatVersion: format.VersionV1, Flags: format.SegmentRefHasLocal, ArtifactID: manifest.Header.FileID, FileSize: uint64(len(manifestBytes)), LocalName: manifestName, ContentSHA256: manifest.Footer.ContentSHA256}}
	for _, reference := range manifest.SegmentReferences {
		if reference.Flags&format.SegmentRefHasLocal == 0 {
			return result, fmt.Errorf("Segment %x has no local copy", reference.SegmentID)
		}
		if _, err = segment.ScrubFile(filepath.Join(dataAbs, reference.LocalPath)); err != nil {
			return result, err
		}
		if err = os.MkdirAll(filepath.Dir(filepath.Join(staging, filepath.FromSlash(reference.LocalPath))), 0750); err != nil {
			return result, err
		}
		if err = copyFile(filepath.Join(staging, filepath.FromSlash(reference.LocalPath)), filepath.Join(dataAbs, filepath.FromSlash(reference.LocalPath))); err != nil {
			return result, err
		}
		artifacts = append(artifacts, format.SnapshotArtifact{ArtifactType: format.ArtifactSegment, FormatVersion: format.VersionV1, Flags: format.SegmentRefHasLocal, ArtifactID: reference.SegmentID, FileSize: reference.FileSize, LocalName: reference.LocalPath, ContentSHA256: reference.ContentSHA256})
	}
	snapshotID, err := newID()
	if err != nil {
		return result, err
	}
	snapshotBytes, err := format.MarshalSnapshotManifest(format.SnapshotManifest{Header: format.SnapshotHeader{SnapshotID: snapshotID, GroupID: node.GroupID, Term: term, CheckpointEntryID: manifest.Header.LastEntryID, CheckpointEntryCRC32C: manifest.Header.LastEntryCRC32C, ManifestGeneration: manifest.Header.Generation, ManifestSHA256: manifest.Footer.ContentSHA256, CreatedAt: time.Now().UnixNano()}, Artifacts: artifacts})
	if err != nil {
		return result, err
	}
	snapshotName := fmt.Sprintf("SNAPSHOT-%x.bin", snapshotID)
	if err = fsutil.AtomicWrite(staging, snapshotName, snapshotBytes, 0640, nil); err != nil {
		return result, err
	}
	if err = fsutil.SyncDir(filepath.Join(staging, "manifests")); err != nil {
		return result, err
	}
	if err = fsutil.SyncDir(filepath.Join(staging, "segments")); err != nil {
		return result, err
	}
	if err = os.Rename(staging, destination); err != nil {
		return result, err
	}
	published = true
	if err = fsutil.SyncDir(parent); err != nil {
		return result, err
	}
	return Result{Path: destination, SnapshotID: snapshotID, GroupID: node.GroupID, Term: term, ManifestGeneration: manifest.Header.Generation, CheckpointEntryID: manifest.Header.LastEntryID, CheckpointCRC32C: manifest.Header.LastEntryCRC32C, Artifacts: uint64(len(artifacts))}, nil
}

func Verify(path string) (Result, error) {
	paths, err := filepath.Glob(filepath.Join(path, "SNAPSHOT-*.bin"))
	if err != nil || len(paths) != 1 {
		return Result{}, fmt.Errorf("Snapshot directory must contain exactly one Snapshot Manifest")
	}
	data, err := os.ReadFile(paths[0])
	if err != nil {
		return Result{}, err
	}
	snapshot, err := format.UnmarshalSnapshotManifest(data)
	if err != nil {
		return Result{}, err
	}
	segments := make(map[format.UUID]format.SnapshotArtifact)
	var manifest format.Manifest
	foundManifest := false
	for _, artifact := range snapshot.Artifacts {
		artifactPath := filepath.Join(path, filepath.FromSlash(artifact.LocalName))
		info, statErr := os.Stat(artifactPath)
		if statErr != nil || uint64(info.Size()) != artifact.FileSize {
			return Result{}, fmt.Errorf("Snapshot Artifact %x is missing or has wrong size", artifact.ArtifactID)
		}
		switch artifact.ArtifactType {
		case format.ArtifactManifest:
			encoded, readErr := os.ReadFile(artifactPath)
			if readErr != nil {
				return Result{}, readErr
			}
			manifest, err = format.UnmarshalManifest(encoded)
			if err != nil || manifest.Header.FileID != artifact.ArtifactID || manifest.Footer.ContentSHA256 != artifact.ContentSHA256 {
				return Result{}, fmt.Errorf("Snapshot Manifest Artifact is invalid")
			}
			foundManifest = true
		case format.ArtifactSegment:
			metadata, scrubErr := segment.ScrubFile(artifactPath)
			if scrubErr != nil || metadata.Header.SegmentID != artifact.ArtifactID || metadata.Footer.ContentSHA256 != artifact.ContentSHA256 {
				return Result{}, fmt.Errorf("Snapshot Segment %x is invalid", artifact.ArtifactID)
			}
			segments[artifact.ArtifactID] = artifact
		default:
			return Result{}, fmt.Errorf("unsupported Snapshot Artifact type %d", artifact.ArtifactType)
		}
	}
	if !foundManifest || manifest.Header.Generation != snapshot.Header.ManifestGeneration || manifest.Footer.ContentSHA256 != snapshot.Header.ManifestSHA256 || manifest.Header.LastEntryID != snapshot.Header.CheckpointEntryID || manifest.Header.LastEntryCRC32C != snapshot.Header.CheckpointEntryCRC32C || len(segments) != len(manifest.SegmentReferences) {
		return Result{}, fmt.Errorf("Snapshot does not match its Manifest checkpoint")
	}
	pointerBytes, err := os.ReadFile(filepath.Join(path, "CURRENT"))
	if err != nil {
		return Result{}, err
	}
	pointer, err := format.UnmarshalCurrentPointer(pointerBytes)
	if err != nil || pointer.Generation != manifest.Header.Generation || pointer.ManifestFileID != manifest.Header.FileID || pointer.ManifestSHA256 != manifest.Footer.ContentSHA256 {
		return Result{}, fmt.Errorf("Snapshot CURRENT does not match Manifest")
	}
	for _, reference := range manifest.SegmentReferences {
		artifact, ok := segments[reference.SegmentID]
		if !ok || artifact.FileSize != reference.FileSize || artifact.ContentSHA256 != reference.ContentSHA256 {
			return Result{}, fmt.Errorf("Snapshot is missing Segment %x", reference.SegmentID)
		}
	}
	return Result{Path: path, SnapshotID: snapshot.Header.SnapshotID, GroupID: snapshot.Header.GroupID, Term: snapshot.Header.Term, ManifestGeneration: snapshot.Header.ManifestGeneration, CheckpointEntryID: snapshot.Header.CheckpointEntryID, CheckpointCRC32C: snapshot.Header.CheckpointEntryCRC32C, Artifacts: snapshot.Header.ArtifactCount}, nil
}

func copyFile(destination, source string) error {
	in, err := os.Open(source)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(destination, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0640)
	if err != nil {
		return err
	}
	ok := false
	defer func() {
		out.Close()
		if !ok {
			_ = os.Remove(destination)
		}
	}()
	if _, err = io.Copy(out, in); err != nil {
		return err
	}
	if err = out.Sync(); err != nil {
		return err
	}
	if err = out.Close(); err != nil {
		return err
	}
	ok = true
	return nil
}

func inside(root, path string) bool {
	relative, err := filepath.Rel(root, path)
	return err == nil && relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator))
}

func newID() (format.UUID, error) {
	var id format.UUID
	_, err := rand.Read(id[:])
	return id, err
}
