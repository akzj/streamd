package main

import (
	"crypto/sha256"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/akzj/streamd/internal/storage/format"
	"github.com/akzj/streamd/internal/storage/fsutil"
	"github.com/akzj/streamd/internal/storage/identity"
	"github.com/akzj/streamd/internal/storage/recovery"
	"github.com/akzj/streamd/internal/storage/replicationstate"
)

type result struct {
	EntryID uint64 `json:"entry_id"`
	CRC32C  uint32 `json:"crc32c"`
}

func main() {
	rootPath := flag.String("data", "", "stopped disposable Standby data directory")
	flag.Parse()
	if *rootPath == "" {
		flag.Usage()
		os.Exit(2)
	}
	value, err := inject(*rootPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "inject-divergence:", err)
		os.Exit(1)
	}
	if err = json.NewEncoder(os.Stdout).Encode(value); err != nil {
		fmt.Fprintln(os.Stderr, "inject-divergence:", err)
		os.Exit(1)
	}
}

func inject(path string) (result, error) {
	root, err := fsutil.LockExistingRoot(path)
	if err != nil {
		return result{}, err
	}
	defer root.Close()

	node, err := identity.Load(root.Path())
	if err != nil {
		return result{}, err
	}
	states, err := replicationstate.Open(root.Path(), node)
	if err != nil {
		return result{}, err
	}
	current, ok := states.Current()
	if !ok || current.Header.Role != format.ReplicationRoleStandby || current.Header.Term == 0 {
		return result{}, fmt.Errorf("fault injection requires a durable stopped Standby")
	}
	if !current.Header.LocalDurable.Present || !current.Header.Committed.Present || current.Header.LocalDurable != current.Header.Committed {
		return result{}, fmt.Errorf("fault injection requires a fully committed Standby tail")
	}
	committed := current.Header.Committed.EntryID
	recovered, err := recovery.OpenWithOptions(root.Path(), recovery.Options{ApplyThrough: &committed})
	if err != nil {
		return result{}, err
	}
	defer recovered.Close()

	entryID := recovered.WAL.NextEntryID()
	if entryID != current.Header.LocalDurable.EntryID+1 || recovered.WAL.PreviousEntryCRC32C() != current.Header.LocalDurable.CRC32C {
		return result{}, fmt.Errorf("physical WAL tail does not match durable Standby state")
	}
	requestHash := sha256.Sum256([]byte("streamd-ha-log-divergence"))
	frame, err := format.MarshalRecordFrame(format.RecordFrame{
		EntryID: entryID, StreamID: 1 << 63, RecordedAt: time.Now().UnixNano(),
		BatchCount: 1, RequestHash: requestHash, RequestID: []byte("ha-divergent-suffix"),
		Producer: "ha-fault-injector", Payload: []byte("must-never-be-applied"),
	})
	if err != nil {
		return result{}, err
	}
	encoded, err := format.MarshalWALEntry(current.Header.Term, recovered.WAL.PreviousEntryCRC32C(), frame)
	if err != nil {
		return result{}, err
	}
	entry, err := format.UnmarshalWALEntry(encoded)
	if err != nil {
		return result{}, err
	}
	if err = recovered.WAL.Append(encoded); err != nil {
		return result{}, err
	}
	if err = recovered.WAL.Sync(); err != nil {
		return result{}, err
	}
	return result{EntryID: entry.EntryID, CRC32C: entry.CRC32C}, nil
}
