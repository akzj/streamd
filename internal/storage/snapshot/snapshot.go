package snapshot

import (
	"context"
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
	locatorstore "github.com/akzj/streamd/internal/storage/locator"
	"github.com/akzj/streamd/internal/storage/replicationstate"
	"github.com/akzj/streamd/internal/storage/segment"
)

type offlinePrimaryGuard struct{}

func (offlinePrimaryGuard) CanCommit() error { return nil }

type offlinePrimaryReplica struct{}

func (offlinePrimaryReplica) Replicate(context.Context, [][]byte) (uint64, error) {
	return 0, fmt.Errorf("offline Primary Snapshot source cannot append")
}

func (offlinePrimaryReplica) AdvanceCommit(context.Context, uint64) error { return nil }

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
	states, err := replicationstate.Open(dataAbs, node)
	if err != nil {
		return result, fmt.Errorf("Replication State: %w", err)
	}
	if current, ok := states.Current(); ok {
		if current.Header.Role != format.ReplicationRoleSingle {
			return result, fmt.Errorf("offline Snapshot creation is unavailable for replicated data roots; create it from a running Strict Primary")
		}
	}
	store, err := engine.Open(dataAbs)
	if err != nil {
		return result, err
	}
	defer func() { err = errors.Join(err, store.Close()) }()
	return createOnline(store, destination, 0)
}

// CreatePrimaryOffline creates a Snapshot from a stopped Strict Primary. A
// clean shutdown releases leadership and persists RECOVERING, so that state is
// accepted only when its hash-linked immediate predecessor is a Strict Primary
// in the same Term. OpenReplicated revalidates the state under the data-root
// lock and rejects any WAL suffix beyond committed.
func CreatePrimaryOffline(dataRoot, destination string) (result Result, err error) {
	dataAbs, err := filepath.Abs(dataRoot)
	if err != nil {
		return result, err
	}
	node, err := identity.Load(dataAbs)
	if err != nil {
		return result, fmt.Errorf("NODE: %w", err)
	}
	states, err := replicationstate.Open(dataAbs, node)
	if err != nil {
		return result, fmt.Errorf("Replication State: %w", err)
	}
	current, ok := states.Current()
	if !ok || current.Header.Durability != format.ReplicationDurabilityStrict || current.Header.Term == 0 {
		return result, fmt.Errorf("offline Primary Snapshot requires durable Strict PRIMARY provenance")
	}
	options := engine.ReplicationOptions{Term: current.Header.Term, Role: current.Header.Role, Durability: format.ReplicationDurabilityStrict, Guard: offlinePrimaryGuard{}, ExpectedStateID: current.Header.StateID, RejectUncommittedSuffix: true}
	switch current.Header.Role {
	case format.ReplicationRolePrimary:
		options.Replica = offlinePrimaryReplica{}
	case format.ReplicationRoleRecovering:
		previous, found, previousErr := states.Previous()
		if previousErr != nil {
			return result, fmt.Errorf("Primary provenance: %w", previousErr)
		}
		if !found || previous.Header.Role != format.ReplicationRolePrimary || previous.Header.Durability != format.ReplicationDurabilityStrict || previous.Header.Term != current.Header.Term {
			return result, fmt.Errorf("offline Primary Snapshot requires RECOVERING state to directly continue a Strict PRIMARY in the same Term")
		}
	default:
		return result, fmt.Errorf("offline Primary Snapshot requires durable Strict PRIMARY provenance")
	}
	store, err := engine.OpenReplicated(dataAbs, node, options)
	if err != nil {
		return result, err
	}
	defer func() { err = errors.Join(err, store.Close()) }()
	return create(store, destination, current.Header.Term, true)
}

// CreateOnline takes a short engine checkpoint and then copies only immutable
// artifacts from that exact Manifest. Appends may continue while the files are
// copied because no later mutable CURRENT file is used as Snapshot input.
func CreateOnline(store *engine.Store, destination string) (result Result, err error) {
	if store == nil {
		return result, fmt.Errorf("Snapshot engine is required")
	}
	health := store.Health()
	if err = validateSource(health, false); err != nil {
		return result, err
	}
	return createOnline(store, destination, health.Term)
}

func createOnline(store *engine.Store, destination string, term uint64) (result Result, err error) {
	return create(store, destination, term, false)
}

func create(store *engine.Store, destination string, term uint64, allowReleasedPrimary bool) (result Result, err error) {
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
	if manifest.Header.RecordCount == 0 {
		return result, fmt.Errorf("empty data roots do not have an installable V1 Manifest")
	}
	health := store.Health()
	if err = validateSource(health, allowReleasedPrimary); err != nil {
		return result, err
	}
	if health.Term != term {
		return result, fmt.Errorf("Snapshot source Term changed from %d to %d", term, health.Term)
	}
	if (health.Role == format.ReplicationRolePrimary || allowReleasedPrimary && health.Role == format.ReplicationRoleRecovering) && (!health.Watermarks.HasCommitted || manifest.Header.LastEntryID > health.Watermarks.Committed) {
		return result, fmt.Errorf("Snapshot checkpoint Entry %d is not covered by committed watermark", manifest.Header.LastEntryID)
	}
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
	for _, directory := range []string{"manifests", "segments", "catalog", "locator", "registry"} {
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
	for _, reference := range manifest.ArtifactReferences {
		source := filepath.Join(dataAbs, filepath.FromSlash(reference.Path))
		data, readErr := os.ReadFile(source)
		if readErr != nil {
			return result, readErr
		}
		if uint64(len(data)) != reference.FileSize || len(data) < format.ArtifactFooterLength {
			return result, fmt.Errorf("Artifact %x size mismatch", reference.ArtifactID)
		}
		footer, verifyErr := format.VerifyArtifact(data[:len(data)-format.ArtifactFooterLength], data[len(data)-format.ArtifactFooterLength:], reference.ArtifactType, reference.ArtifactID)
		if verifyErr != nil {
			return result, fmt.Errorf("Artifact %x verification failed: %w", reference.ArtifactID, verifyErr)
		}
		if footer.ContentSHA256 != reference.ContentSHA256 {
			return result, fmt.Errorf("Artifact %x digest mismatch", reference.ArtifactID)
		}
		if err = os.MkdirAll(filepath.Dir(filepath.Join(staging, filepath.FromSlash(reference.Path))), 0750); err != nil {
			return result, err
		}
		if err = copyFile(filepath.Join(staging, filepath.FromSlash(reference.Path)), source); err != nil {
			return result, err
		}
		artifacts = append(artifacts, format.SnapshotArtifact{ArtifactType: reference.ArtifactType, FormatVersion: format.VersionV1, Flags: format.SegmentRefHasLocal, ArtifactID: reference.ArtifactID, FileSize: reference.FileSize, LocalName: reference.Path, ContentSHA256: reference.ContentSHA256})
		if reference.ArtifactType == format.ArtifactLocatorSnapshot {
			locatorSnapshot, decodeErr := format.UnmarshalLocatorSnapshot(data)
			if decodeErr != nil {
				return result, decodeErr
			}
			for _, pack := range locatorSnapshot.Packs {
				if err = locatorstore.VerifyPack(dataAbs, pack); err != nil {
					return result, err
				}
				if err = os.MkdirAll(filepath.Dir(filepath.Join(staging, filepath.FromSlash(pack.Path))), 0750); err != nil {
					return result, err
				}
				if err = copyFile(filepath.Join(staging, filepath.FromSlash(pack.Path)), filepath.Join(dataAbs, filepath.FromSlash(pack.Path))); err != nil {
					return result, err
				}
				artifacts = append(artifacts, format.SnapshotArtifact{ArtifactType: format.ArtifactLocatorPack, FormatVersion: format.VersionV1, Flags: format.SegmentRefHasLocal, ArtifactID: pack.PackID, FileSize: pack.FileSize, LocalName: pack.Path, ContentSHA256: pack.ContentSHA256})
			}
		}
	}
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
	if err = fsutil.SyncDir(filepath.Join(staging, "catalog")); err != nil {
		return result, err
	}
	if err = fsutil.SyncDir(filepath.Join(staging, "locator")); err != nil {
		return result, err
	}
	if err = fsutil.SyncDir(filepath.Join(staging, "registry")); err != nil {
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

func validateSource(health engine.Health, allowReleasedPrimary bool) error {
	if health.Fatal != nil {
		return fmt.Errorf("Snapshot source is failed: %w", health.Fatal)
	}
	switch health.Role {
	case format.ReplicationRoleSingle:
		if health.Durability != format.ReplicationDurabilitySingleSync {
			return fmt.Errorf("Single Snapshot source has invalid durability %d", health.Durability)
		}
	case format.ReplicationRolePrimary:
		if health.Durability != format.ReplicationDurabilityStrict {
			return fmt.Errorf("replicated Snapshot source is not a Strict Primary")
		}
		if health.WriteUnavailable != nil {
			return fmt.Errorf("Strict Primary is not safely writable: %w", health.WriteUnavailable)
		}
	case format.ReplicationRoleRecovering:
		if !allowReleasedPrimary || health.Durability != format.ReplicationDurabilityStrict {
			return fmt.Errorf("Snapshot source role %d cannot create a Snapshot", health.Role)
		}
	default:
		return fmt.Errorf("Snapshot source role %d cannot create a Snapshot", health.Role)
	}
	return nil
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
	artifacts := make(map[format.UUID]format.SnapshotArtifact)
	locatorSnapshots := make([]format.LocatorSnapshot, 0, 1)
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
		case format.ArtifactTailCatalog, format.ArtifactLocatorSnapshot, format.ArtifactRegistrySnapshot, format.ArtifactLocatorPack:
			encoded, readErr := os.ReadFile(artifactPath)
			if readErr != nil || len(encoded) < format.ArtifactFooterLength {
				return Result{}, fmt.Errorf("Snapshot projection %x is invalid", artifact.ArtifactID)
			}
			footer, verifyErr := format.VerifyArtifact(encoded[:len(encoded)-format.ArtifactFooterLength], encoded[len(encoded)-format.ArtifactFooterLength:], artifact.ArtifactType, artifact.ArtifactID)
			if verifyErr != nil || footer.ContentSHA256 != artifact.ContentSHA256 {
				return Result{}, fmt.Errorf("Snapshot projection %x is invalid", artifact.ArtifactID)
			}
			if artifact.ArtifactType == format.ArtifactLocatorSnapshot {
				locatorSnapshot, decodeErr := format.UnmarshalLocatorSnapshot(encoded)
				if decodeErr != nil {
					return Result{}, fmt.Errorf("Snapshot Locator Snapshot is invalid: %w", decodeErr)
				}
				locatorSnapshots = append(locatorSnapshots, locatorSnapshot)
			} else if artifact.ArtifactType == format.ArtifactRegistrySnapshot {
				if _, decodeErr := format.UnmarshalRegistrySnapshot(encoded); decodeErr != nil {
					return Result{}, fmt.Errorf("Snapshot Registry Snapshot is invalid: %w", decodeErr)
				}
			}
		default:
			return Result{}, fmt.Errorf("unsupported Snapshot Artifact type %d", artifact.ArtifactType)
		}
		if _, duplicate := artifacts[artifact.ArtifactID]; duplicate {
			return Result{}, fmt.Errorf("Snapshot contains duplicate Artifact %x", artifact.ArtifactID)
		}
		artifacts[artifact.ArtifactID] = artifact
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
	for _, reference := range manifest.ArtifactReferences {
		artifact, ok := artifacts[reference.ArtifactID]
		if !ok || artifact.ArtifactType != reference.ArtifactType || artifact.FileSize != reference.FileSize || artifact.LocalName != reference.Path || artifact.ContentSHA256 != reference.ContentSHA256 {
			return Result{}, fmt.Errorf("Snapshot is missing Artifact %x", reference.ArtifactID)
		}
	}
	for _, locatorSnapshot := range locatorSnapshots {
		for _, pack := range locatorSnapshot.Packs {
			artifact, ok := artifacts[pack.PackID]
			if !ok || artifact.ArtifactType != format.ArtifactLocatorPack || artifact.FileSize != pack.FileSize || artifact.LocalName != pack.Path || artifact.ContentSHA256 != pack.ContentSHA256 {
				return Result{}, fmt.Errorf("Snapshot is missing Locator Pack %x", pack.PackID)
			}
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
