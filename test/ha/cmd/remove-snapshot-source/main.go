package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/akzj/streamd/internal/storage/fsutil"
	"github.com/akzj/streamd/internal/storage/identity"
	"github.com/akzj/streamd/internal/storage/replicationstate"
	"github.com/akzj/streamd/internal/storage/snapshot"
)

func main() {
	dataRoot := flag.String("data", "", "stopped disposable Primary data directory")
	snapshotPath := flag.String("snapshot", "", "Snapshot package to remove")
	flag.Parse()
	if *dataRoot == "" || *snapshotPath == "" {
		flag.Usage()
		os.Exit(2)
	}
	if err := remove(*dataRoot, *snapshotPath); err != nil {
		fmt.Fprintln(os.Stderr, "remove-snapshot-source:", err)
		os.Exit(1)
	}
}

func remove(dataRoot, snapshotPath string) error {
	root, err := fsutil.LockExistingRoot(dataRoot)
	if err != nil {
		return err
	}
	defer root.Close()

	absSnapshot, err := filepath.Abs(snapshotPath)
	if err != nil {
		return err
	}
	snapshotsRoot := filepath.Join(root.Path(), "snapshots")
	if filepath.Dir(absSnapshot) != snapshotsRoot || filepath.Base(absSnapshot) == "." {
		return fmt.Errorf("Snapshot must be a direct child of the data snapshots directory")
	}
	verified, err := snapshot.Verify(absSnapshot)
	if err != nil {
		return err
	}
	node, err := identity.Load(root.Path())
	if err != nil {
		return err
	}
	states, err := replicationstate.Open(root.Path(), node)
	if err != nil {
		return err
	}
	current, ok := states.Current()
	if !ok || !current.Header.HasInstalledSnapshot || current.Header.InstalledSnapshotID != verified.SnapshotID || current.Header.InstalledSnapshotEntry.EntryID != verified.CheckpointEntryID || current.Header.InstalledSnapshotEntry.CRC32C != verified.CheckpointCRC32C {
		return fmt.Errorf("Snapshot package does not match the durable installed Snapshot")
	}
	if err = os.RemoveAll(absSnapshot); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return fsutil.SyncDir(snapshotsRoot)
}
