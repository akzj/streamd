package snapshot

import (
	"errors"
	"os"
	"path/filepath"

	"github.com/akzj/streamd/internal/storage/format"
)

// FindAvailable returns a fully verified, transferable Snapshot package that
// matches durable replication metadata. Installed Snapshot metadata alone is
// a local recovery checkpoint; it does not prove that a package still exists
// under data/snapshots for another replica to install.
func FindAvailable(dataRoot string, snapshotID, groupID format.UUID, checkpointEntryID uint64, checkpointCRC32C uint32) (Result, bool, error) {
	root := filepath.Join(dataRoot, "snapshots")
	entries, err := os.ReadDir(root)
	if errors.Is(err, os.ErrNotExist) {
		return Result{}, false, nil
	}
	if err != nil {
		return Result{}, false, err
	}
	manifestName := snapshotManifestName(snapshotID)
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		path := filepath.Join(root, entry.Name())
		if _, err = os.Stat(filepath.Join(path, manifestName)); errors.Is(err, os.ErrNotExist) {
			continue
		} else if err != nil {
			return Result{}, false, err
		}
		verified, verifyErr := Verify(path)
		if verifyErr != nil {
			continue
		}
		if verified.SnapshotID == snapshotID && verified.GroupID == groupID && verified.CheckpointEntryID == checkpointEntryID && verified.CheckpointCRC32C == checkpointCRC32C {
			return verified, true, nil
		}
	}
	return Result{}, false, nil
}
