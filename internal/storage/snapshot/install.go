package snapshot

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/akzj/streamd/internal/storage/format"
	"github.com/akzj/streamd/internal/storage/fsutil"
	"github.com/akzj/streamd/internal/storage/identity"
	"github.com/akzj/streamd/internal/storage/replicationstate"
	"github.com/akzj/streamd/internal/storage/segment"
	"github.com/akzj/streamd/internal/storage/wal"
)

const installJournalName = "SNAPSHOT-INSTALL.json"

type InstallOptions struct {
	Term     uint64
	LeaderID format.UUID
	Now      func() time.Time
	Hook     fsutil.CrashHook
}

type installJournal struct {
	Version               uint32 `json:"version"`
	SnapshotID            string `json:"snapshot_id"`
	GroupID               string `json:"group_id"`
	Term                  uint64 `json:"term"`
	LeaderID              string `json:"leader_id"`
	CheckpointEntryID     uint64 `json:"checkpoint_entry_id"`
	CheckpointEntryCRC32C uint32 `json:"checkpoint_entry_crc32c"`
	StageDir              string `json:"stage_dir"`
}

func Install(dataRoot, source string, options InstallOptions) (Result, error) {
	if options.Term == 0 || options.LeaderID == (format.UUID{}) {
		return Result{}, fmt.Errorf("Snapshot install requires Coordinator Term and Leader ID")
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	verified, err := Verify(source)
	if err != nil {
		return Result{}, err
	}
	snapshotManifest, err := readSnapshotManifest(source)
	if err != nil {
		return Result{}, err
	}
	if options.Term < snapshotManifest.Header.Term {
		return Result{}, fmt.Errorf("install Term %d is older than Snapshot Term %d", options.Term, snapshotManifest.Header.Term)
	}
	root, err := fsutil.OpenRoot(dataRoot)
	if err != nil {
		return Result{}, err
	}
	defer root.Close()
	node, err := identity.Load(root.Path())
	if err != nil {
		return Result{}, fmt.Errorf("target NODE: %w", err)
	}
	if node.GroupID != snapshotManifest.Header.GroupID || options.LeaderID == node.NodeID {
		return Result{}, fmt.Errorf("Snapshot group or install leader does not match target NODE")
	}
	if _, err = os.Stat(filepath.Join(root.Path(), installJournalName)); !errors.Is(err, os.ErrNotExist) {
		if err == nil {
			return Result{}, fmt.Errorf("another Snapshot install is already pending")
		}
		return Result{}, err
	}
	stateStore, err := replicationstate.Open(root.Path(), node)
	if err != nil {
		return Result{}, err
	}
	if current, ok := stateStore.Current(); ok {
		if current.Header.Term > options.Term {
			return Result{}, fmt.Errorf("target knows newer Term %d", current.Header.Term)
		}
		if current.Header.Committed.Present && current.Header.Committed.EntryID > snapshotManifest.Header.CheckpointEntryID {
			return Result{}, fmt.Errorf("Snapshot checkpoint would regress committed state")
		}
	}
	stageName := "snapshot-install-" + hex.EncodeToString(snapshotManifest.Header.SnapshotID[:])
	stage := filepath.Join(root.Path(), "staging", stageName)
	if err = os.Mkdir(stage, 0750); err != nil {
		return Result{}, err
	}
	staged := false
	defer func() {
		if !staged {
			_ = os.RemoveAll(stage)
		}
	}()
	for _, artifact := range snapshotManifest.Artifacts {
		if !validInstallArtifact(artifact) {
			return Result{}, fmt.Errorf("Snapshot Artifact path is not installable: %q", artifact.LocalName)
		}
		destination := filepath.Join(stage, filepath.FromSlash(artifact.LocalName))
		if err = os.MkdirAll(filepath.Dir(destination), 0750); err != nil {
			return Result{}, err
		}
		if err = copyFile(destination, filepath.Join(source, filepath.FromSlash(artifact.LocalName))); err != nil {
			return Result{}, err
		}
		if err = fsutil.SyncDir(filepath.Dir(destination)); err != nil {
			return Result{}, err
		}
	}
	for _, name := range []string{"CURRENT", snapshotManifestName(snapshotManifest.Header.SnapshotID)} {
		if err = copyFile(filepath.Join(stage, name), filepath.Join(source, name)); err != nil {
			return Result{}, err
		}
	}
	if err = fsutil.SyncDir(stage); err != nil {
		return Result{}, err
	}
	journal := installJournal{Version: 1, SnapshotID: hex.EncodeToString(snapshotManifest.Header.SnapshotID[:]), GroupID: hex.EncodeToString(snapshotManifest.Header.GroupID[:]), Term: options.Term, LeaderID: hex.EncodeToString(options.LeaderID[:]), CheckpointEntryID: snapshotManifest.Header.CheckpointEntryID, CheckpointEntryCRC32C: snapshotManifest.Header.CheckpointEntryCRC32C, StageDir: stageName}
	journalBytes, err := json.Marshal(journal)
	if err != nil {
		return Result{}, err
	}
	if err = fsutil.AtomicWrite(root.Path(), installJournalName, journalBytes, 0640, nil); err != nil {
		return Result{}, err
	}
	staged = true
	if options.Hook != nil {
		if err = options.Hook("after_install_journal"); err != nil {
			return Result{}, err
		}
	}
	if err = resumeInstallLocked(root.Path(), node, options.Now, options.Hook); err != nil {
		return Result{}, err
	}
	return verified, nil
}

// ResumeInstall completes a durable Snapshot install intent. It must run before
// storage recovery and before any network listener is opened.
func ResumeInstall(dataRoot string, hook fsutil.CrashHook) (bool, error) {
	root, err := fsutil.OpenRoot(dataRoot)
	if err != nil {
		return false, err
	}
	defer root.Close()
	if _, err = os.Stat(filepath.Join(root.Path(), installJournalName)); errors.Is(err, os.ErrNotExist) {
		return false, nil
	} else if err != nil {
		return false, err
	}
	node, err := identity.Load(root.Path())
	if err != nil {
		return false, err
	}
	if err = resumeInstallLocked(root.Path(), node, time.Now, hook); err != nil {
		return true, err
	}
	return true, nil
}

func resumeInstallLocked(root string, node format.NodeIdentity, now func() time.Time, hook fsutil.CrashHook) error {
	journal, err := readInstallJournal(filepath.Join(root, installJournalName))
	if err != nil {
		return err
	}
	groupID, err := parseJournalUUID(journal.GroupID)
	if err != nil || groupID != node.GroupID {
		return fmt.Errorf("Snapshot install journal group does not match NODE")
	}
	leaderID, err := parseJournalUUID(journal.LeaderID)
	if err != nil || leaderID == node.NodeID {
		return fmt.Errorf("Snapshot install journal Leader is invalid")
	}
	snapshotID, err := parseJournalUUID(journal.SnapshotID)
	if err != nil || journal.StageDir != "snapshot-install-"+journal.SnapshotID {
		return fmt.Errorf("Snapshot install journal staging identity is invalid")
	}
	stage := filepath.Join(root, "staging", journal.StageDir)
	verified, err := Verify(stage)
	if err != nil || verified.SnapshotID != snapshotID || verified.CheckpointEntryID != journal.CheckpointEntryID {
		return fmt.Errorf("staged Snapshot is invalid: %w", err)
	}
	snapshotManifest, err := readSnapshotManifest(stage)
	if err != nil || snapshotManifest.Header.CheckpointEntryCRC32C != journal.CheckpointEntryCRC32C {
		return fmt.Errorf("staged Snapshot checkpoint is invalid")
	}
	for _, artifact := range snapshotManifest.Artifacts {
		source := filepath.Join(stage, filepath.FromSlash(artifact.LocalName))
		target := filepath.Join(root, filepath.FromSlash(artifact.LocalName))
		if err = publishArtifact(source, target, artifact); err != nil {
			return err
		}
	}
	if hook != nil {
		if err = hook("after_install_artifacts"); err != nil {
			return err
		}
	}
	if journal.CheckpointEntryID == math.MaxUint64 {
		return fmt.Errorf("Snapshot checkpoint exhausts Entry ID space")
	}
	newWAL, err := wal.CreateAfter(root, journal.CheckpointEntryID+1, journal.Term, journal.CheckpointEntryCRC32C, now())
	if err != nil {
		return err
	}
	if err = newWAL.Close(); err != nil {
		return err
	}
	if hook != nil {
		if err = hook("after_install_wal"); err != nil {
			return err
		}
	}
	currentBytes, err := os.ReadFile(filepath.Join(stage, "CURRENT"))
	if err != nil {
		return err
	}
	if err = fsutil.AtomicWrite(root, "CURRENT", currentBytes, 0640, nil); err != nil {
		return err
	}
	if hook != nil {
		if err = hook("after_install_current"); err != nil {
			return err
		}
	}
	stateStore, err := replicationstate.Open(root, node)
	if err != nil {
		return err
	}
	position := format.ReplicationPosition{Present: true, EntryID: journal.CheckpointEntryID, CRC32C: journal.CheckpointEntryCRC32C}
	_, err = stateStore.Update(now(), func(header *format.ReplicationStateHeader) error {
		header.Term = journal.Term
		header.Role = format.ReplicationRoleStandby
		header.Durability = format.ReplicationDurabilityStrict
		header.HasLeader = true
		header.LeaderID = leaderID
		header.HasLease = false
		header.LeaseExpiresAt = 0
		header.LastAppended = position
		header.LocalDurable = position
		header.Replicated = format.ReplicationPosition{}
		header.Committed = position
		header.Applied = position
		header.EarliestWALEntryID = journal.CheckpointEntryID + 1
		header.HasInstalledSnapshot = true
		header.InstalledSnapshotID = snapshotID
		header.InstalledSnapshotEntry = position
		return nil
	})
	if err != nil {
		return err
	}
	if hook != nil {
		if err = hook("after_install_state"); err != nil {
			return err
		}
	}
	if err = os.Remove(filepath.Join(root, installJournalName)); err != nil {
		return err
	}
	if err = fsutil.SyncDir(root); err != nil {
		return err
	}
	if err = os.RemoveAll(stage); err != nil {
		return err
	}
	return fsutil.SyncDir(filepath.Join(root, "staging"))
}

func readInstallJournal(path string) (installJournal, error) {
	file, err := os.Open(path)
	if err != nil {
		return installJournal{}, err
	}
	defer file.Close()
	decoder := json.NewDecoder(io.LimitReader(file, 4096))
	decoder.DisallowUnknownFields()
	var journal installJournal
	if err = decoder.Decode(&journal); err != nil {
		return installJournal{}, err
	}
	if decoder.Decode(&struct{}{}) != io.EOF || journal.Version != 1 || journal.Term == 0 {
		return installJournal{}, fmt.Errorf("Snapshot install journal is invalid")
	}
	return journal, nil
}

func publishArtifact(source, target string, artifact format.SnapshotArtifact) error {
	if _, err := os.Stat(target); err == nil {
		return verifyInstalledArtifact(target, artifact)
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(target), 0750); err != nil {
		return err
	}
	if err := copyFile(target, source); err != nil {
		return err
	}
	return fsutil.SyncDir(filepath.Dir(target))
}

func verifyInstalledArtifact(path string, artifact format.SnapshotArtifact) error {
	info, err := os.Stat(path)
	if err != nil || uint64(info.Size()) != artifact.FileSize {
		return fmt.Errorf("existing Snapshot Artifact conflicts with install target")
	}
	switch artifact.ArtifactType {
	case format.ArtifactManifest:
		encoded, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		manifest, decodeErr := format.UnmarshalManifest(encoded)
		if decodeErr != nil || manifest.Header.FileID != artifact.ArtifactID || manifest.Footer.ContentSHA256 != artifact.ContentSHA256 {
			return fmt.Errorf("existing Manifest conflicts with Snapshot")
		}
	case format.ArtifactSegment:
		metadata, scrubErr := segment.ScrubFile(path)
		if scrubErr != nil || metadata.Header.SegmentID != artifact.ArtifactID || metadata.Footer.ContentSHA256 != artifact.ContentSHA256 {
			return fmt.Errorf("existing Segment conflicts with Snapshot")
		}
	case format.ArtifactTailCatalog, format.ArtifactLocatorSnapshot, format.ArtifactRegistrySnapshot, format.ArtifactLocatorPack:
		encoded, readErr := os.ReadFile(path)
		if readErr != nil || len(encoded) < format.ArtifactFooterLength {
			return fmt.Errorf("existing projection conflicts with Snapshot")
		}
		footer, verifyErr := format.VerifyArtifact(encoded[:len(encoded)-format.ArtifactFooterLength], encoded[len(encoded)-format.ArtifactFooterLength:], artifact.ArtifactType, artifact.ArtifactID)
		if verifyErr != nil || footer.ContentSHA256 != artifact.ContentSHA256 {
			return fmt.Errorf("existing projection conflicts with Snapshot")
		}
	default:
		return fmt.Errorf("unsupported install Artifact %d", artifact.ArtifactType)
	}
	return nil
}

func validInstallArtifact(artifact format.SnapshotArtifact) bool {
	clean := filepath.ToSlash(filepath.Clean(filepath.FromSlash(artifact.LocalName)))
	if clean != artifact.LocalName || strings.Contains(clean, "..") {
		return false
	}
	prefix := ""
	switch artifact.ArtifactType {
	case format.ArtifactManifest:
		prefix = "manifests/"
	case format.ArtifactSegment:
		prefix = "segments/"
	case format.ArtifactTailCatalog:
		prefix = "catalog/"
	case format.ArtifactLocatorSnapshot, format.ArtifactLocatorPack:
		prefix = "locator/"
	case format.ArtifactRegistrySnapshot:
		prefix = "registry/"
	}
	return prefix != "" && strings.HasPrefix(clean, prefix) && len(clean) > len(prefix)
}

func readSnapshotManifest(path string) (format.SnapshotManifest, error) {
	paths, err := filepath.Glob(filepath.Join(path, "SNAPSHOT-*.bin"))
	if err != nil || len(paths) != 1 {
		return format.SnapshotManifest{}, fmt.Errorf("Snapshot directory must contain one Manifest")
	}
	encoded, err := os.ReadFile(paths[0])
	if err != nil {
		return format.SnapshotManifest{}, err
	}
	return format.UnmarshalSnapshotManifest(encoded)
}

func snapshotManifestName(id format.UUID) string { return fmt.Sprintf("SNAPSHOT-%x.bin", id) }

func parseJournalUUID(value string) (format.UUID, error) {
	var id format.UUID
	if len(value) != 32 || strings.ToLower(value) != value {
		return id, fmt.Errorf("UUID is not 32 lowercase hexadecimal digits")
	}
	decoded, err := hex.DecodeString(value)
	if err != nil {
		return id, err
	}
	copy(id[:], decoded)
	if bytes.Equal(id[:], make([]byte, len(id))) {
		return id, fmt.Errorf("UUID is zero")
	}
	return id, nil
}
