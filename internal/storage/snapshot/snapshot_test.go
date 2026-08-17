package snapshot

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/akzj/streamd/internal/storage/engine"
	"github.com/akzj/streamd/internal/storage/format"
	"github.com/akzj/streamd/internal/storage/replicationstate"
	"github.com/akzj/streamd/internal/storage/scrub"
	"github.com/akzj/streamd/internal/storage/wal"
)

var errInstallCrash = errors.New("install crash")

type snapshotGuard struct {
	err error
}

func (g *snapshotGuard) CanCommit() error { return g.err }

type snapshotReplica struct {
	err error
}

func (r *snapshotReplica) Replicate(_ context.Context, encoded [][]byte) (uint64, error) {
	if r.err != nil {
		return 0, r.err
	}
	entry, err := format.UnmarshalWALEntry(encoded[len(encoded)-1])
	return entry.EntryID, err
}

func (*snapshotReplica) AdvanceCommit(context.Context, uint64) error { return nil }

func TestCreateVerifyAndScrubSnapshot(t *testing.T) {
	data := filepath.Join(t.TempDir(), "data")
	node := format.NodeIdentity{ClusterID: snapshotID(1), GroupID: snapshotID(2), NodeID: snapshotID(3), CreatedAt: 1}
	store, err := engine.OpenWithIdentity(data, node)
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.Append(context.Background(), engine.AppendRequest{Namespace: "n", Stream: "s", RequestID: []byte("r"), Producer: "test", Records: []engine.InputRecord{{Payload: []byte("record")}}})
	if err != nil {
		t.Fatal(err)
	}
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
	destination := filepath.Join(t.TempDir(), "snapshot")
	created, err := Create(data, destination)
	if err != nil {
		t.Fatal(err)
	}
	verified, err := Verify(destination)
	if err != nil {
		t.Fatal(err)
	}
	if verified.SnapshotID != created.SnapshotID || verified.Artifacts != created.Artifacts || verified.Artifacts != 6 {
		t.Fatalf("created = %+v, verified = %+v", created, verified)
	}
	report, err := scrub.DataRoot(data)
	if err != nil {
		t.Fatal(err)
	}
	if report.Segments != 1 || report.Records != 2 || report.Artifacts != 4 {
		t.Fatalf("scrub report = %+v", report)
	}
	packs, err := filepath.Glob(filepath.Join(destination, "locator", "*.loc"))
	if err != nil || len(packs) != 1 {
		t.Fatalf("Locator Packs = %v, %v", packs, err)
	}
	pack, err := os.OpenFile(packs[0], os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	original := []byte{0}
	if _, err = pack.ReadAt(original, int64(format.SegmentSectionAlignment+32)); err != nil {
		pack.Close()
		t.Fatal(err)
	}
	corrupt := []byte{original[0] ^ 0xff}
	if _, err = pack.WriteAt(corrupt, int64(format.SegmentSectionAlignment+32)); err != nil {
		pack.Close()
		t.Fatal(err)
	}
	if _, err = Verify(destination); err == nil {
		pack.Close()
		t.Fatal("corrupt Locator Pack passed verification")
	}
	if _, err = pack.WriteAt(original, int64(format.SegmentSectionAlignment+32)); err != nil {
		pack.Close()
		t.Fatal(err)
	}
	if err = pack.Close(); err != nil {
		t.Fatal(err)
	}
	segments, err := filepath.Glob(filepath.Join(destination, "segments", "*.seg"))
	if err != nil || len(segments) != 1 {
		t.Fatalf("segments = %v, %v", segments, err)
	}
	file, err := os.OpenFile(segments[0], os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = file.WriteAt([]byte{0xff}, 1); err != nil {
		t.Fatal(err)
	}
	file.Close()
	if _, err = Verify(destination); err == nil {
		t.Fatal("corrupt Snapshot passed verification")
	}
}

func TestOfflineCreateRejectsReplicatedDataRoot(t *testing.T) {
	data := filepath.Join(t.TempDir(), "data")
	node := format.NodeIdentity{ClusterID: snapshotID(1), GroupID: snapshotID(2), NodeID: snapshotID(3), CreatedAt: 1}
	store, err := engine.OpenWithIdentity(data, node)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.Append(context.Background(), engine.AppendRequest{Namespace: "n", Stream: "s", RequestID: []byte("r"), Producer: "test", Records: []engine.InputRecord{{Payload: []byte("record")}}}); err != nil {
		t.Fatal(err)
	}
	manifest, _, err := store.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
	states, err := replicationstate.Open(data, node)
	if err != nil {
		t.Fatal(err)
	}
	position := format.ReplicationPosition{Present: true, EntryID: manifest.Header.LastEntryID, CRC32C: manifest.Header.LastEntryCRC32C}
	_, err = states.Update(time.Now(), func(header *format.ReplicationStateHeader) error {
		header.Term = 7
		header.Role = format.ReplicationRolePrimary
		header.Durability = format.ReplicationDurabilityStrict
		header.HasLeader = true
		header.LeaderID = node.NodeID
		header.HasLease = true
		header.LeaseExpiresAt = time.Now().Add(time.Minute).UnixNano()
		header.LastAppended = position
		header.LocalDurable = position
		header.Replicated = position
		header.Committed = position
		header.Applied = position
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	destination := filepath.Join(t.TempDir(), "snapshot")
	if _, err = Create(data, destination); err == nil || !strings.Contains(err.Error(), "running Strict Primary") {
		t.Fatalf("offline replicated Snapshot error = %v", err)
	}
	if _, statErr := os.Stat(destination); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("rejected Snapshot destination exists: %v", statErr)
	}
	primarySnapshot := filepath.Join(t.TempDir(), "primary-snapshot")
	created, err := CreatePrimaryOffline(data, primarySnapshot)
	if err != nil {
		t.Fatal(err)
	}
	if created.Term != 7 || created.CheckpointEntryID != manifest.Header.LastEntryID {
		t.Fatalf("offline Primary Snapshot = %+v", created)
	}
	if _, err = states.Update(time.Now(), func(header *format.ReplicationStateHeader) error {
		header.Role = format.ReplicationRoleRecovering
		header.HasLeader = false
		header.LeaderID = format.UUID{}
		header.HasLease = false
		header.LeaseExpiresAt = 0
		header.Replicated = format.ReplicationPosition{}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err = CreatePrimaryOffline(data, filepath.Join(t.TempDir(), "released-primary-snapshot")); err != nil {
		t.Fatalf("cleanly released Primary Snapshot: %v", err)
	}

	log, err := wal.OpenWithPrevious(data, manifest.Header.LastEntryCRC32C)
	if err != nil {
		t.Fatal(err)
	}
	entryID := log.NextEntryID()
	frame, err := format.MarshalRecordFrame(format.RecordFrame{EntryID: entryID, StreamID: 99, BatchCount: 1, RequestID: []byte("suffix"), Producer: "test", Payload: []byte("uncommitted")})
	if err != nil {
		log.Close()
		t.Fatal(err)
	}
	encoded, err := format.MarshalWALEntry(7, log.PreviousEntryCRC32C(), frame)
	if err != nil {
		log.Close()
		t.Fatal(err)
	}
	if err = log.Append(encoded); err == nil {
		err = log.Sync()
	}
	if closeErr := log.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		t.Fatal(err)
	}
	if _, err = CreatePrimaryOffline(data, filepath.Join(t.TempDir(), "unsafe-primary-snapshot")); err == nil || !strings.Contains(err.Error(), "unresolved durable WAL suffix") {
		t.Fatalf("unresolved Primary suffix Snapshot error = %v", err)
	}
}

func TestOfflinePrimaryRejectsRecoveringFormerStandby(t *testing.T) {
	data := t.TempDir()
	node := format.NodeIdentity{ClusterID: snapshotID(1), GroupID: snapshotID(2), NodeID: snapshotID(3), CreatedAt: 1}
	store, err := engine.OpenWithIdentity(data, node)
	if err != nil {
		t.Fatal(err)
	}
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
	states, err := replicationstate.Open(data, node)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = states.Update(time.Now(), func(header *format.ReplicationStateHeader) error {
		header.Term = 7
		header.Role = format.ReplicationRoleStandby
		header.Durability = format.ReplicationDurabilityStrict
		header.HasLeader = true
		header.LeaderID = snapshotID(4)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err = states.Update(time.Now(), func(header *format.ReplicationStateHeader) error {
		header.Role = format.ReplicationRoleRecovering
		header.HasLeader = false
		header.LeaderID = format.UUID{}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err = CreatePrimaryOffline(data, filepath.Join(t.TempDir(), "snapshot")); err == nil || !strings.Contains(err.Error(), "directly continue a Strict PRIMARY") {
		t.Fatalf("former Standby Snapshot error = %v", err)
	}
}

func TestCreateOnlineStrictPrimaryRequiresCommittedWritableSource(t *testing.T) {
	data := filepath.Join(t.TempDir(), "data")
	node := format.NodeIdentity{ClusterID: snapshotID(1), GroupID: snapshotID(2), NodeID: snapshotID(3), CreatedAt: 1}
	guard := &snapshotGuard{}
	replica := &snapshotReplica{}
	store, err := engine.OpenReplicated(data, node, engine.ReplicationOptions{Term: 7, Role: format.ReplicationRolePrimary, Durability: format.ReplicationDurabilityStrict, Replica: replica, Guard: guard})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, err = store.Append(context.Background(), engine.AppendRequest{Namespace: "n", Stream: "s", RequestID: []byte("r"), Producer: "test", Records: []engine.InputRecord{{Payload: []byte("record")}}}); err != nil {
		t.Fatal(err)
	}
	created, err := CreateOnline(store, filepath.Join(t.TempDir(), "snapshot"))
	if err != nil {
		t.Fatal(err)
	}
	health := store.Health()
	if created.Term != 7 || !health.Watermarks.HasCommitted || created.CheckpointEntryID > health.Watermarks.Committed {
		t.Fatalf("Snapshot = %+v, Health = %+v", created, health)
	}

	guard.err = errors.New("lease expired")
	destination := filepath.Join(t.TempDir(), "unsafe-snapshot")
	if _, err = CreateOnline(store, destination); err == nil || !strings.Contains(err.Error(), "not safely writable") {
		t.Fatalf("unsafe Primary Snapshot error = %v", err)
	}
	if _, statErr := os.Stat(destination); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("unsafe Snapshot destination exists: %v", statErr)
	}
}

func TestCreateOnlineRejectsUncertainStrictSuffix(t *testing.T) {
	data := filepath.Join(t.TempDir(), "data")
	node := format.NodeIdentity{ClusterID: snapshotID(1), GroupID: snapshotID(2), NodeID: snapshotID(3), CreatedAt: 1}
	guard := &snapshotGuard{}
	replica := &snapshotReplica{}
	store, err := engine.OpenReplicated(data, node, engine.ReplicationOptions{Term: 7, Role: format.ReplicationRolePrimary, Durability: format.ReplicationDurabilityStrict, Replica: replica, Guard: guard})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.Append(context.Background(), engine.AppendRequest{Namespace: "n", Stream: "s", RequestID: []byte("committed"), Producer: "test", Records: []engine.InputRecord{{Payload: []byte("record")}}}); err != nil {
		t.Fatal(err)
	}
	replica.err = errors.New("standby unavailable")
	_, appendErr := store.Append(context.Background(), engine.AppendRequest{Namespace: "n", Stream: "s", ExpectedSequence: 1, RequestID: []byte("uncertain"), Producer: "test", Records: []engine.InputRecord{{Payload: []byte("uncertain")}}})
	if appendErr == nil {
		t.Fatal("partitioned Strict Append succeeded")
	}
	destination := filepath.Join(t.TempDir(), "snapshot")
	if _, err = CreateOnline(store, destination); err == nil || !strings.Contains(err.Error(), "source is failed") {
		t.Fatalf("uncertain suffix Snapshot error = %v", err)
	}
	if _, statErr := os.Stat(destination); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("uncertain Snapshot destination exists: %v", statErr)
	}
	if closeErr := store.Close(); closeErr == nil {
		t.Fatal("failed Strict commit was absent from Close error")
	}
}

func TestInstallAndCrashResumeAreAtomic(t *testing.T) {
	base := t.TempDir()
	source := filepath.Join(base, "source")
	sourceNode := format.NodeIdentity{ClusterID: snapshotID(1), GroupID: snapshotID(2), NodeID: snapshotID(3), CreatedAt: 1}
	store, err := engine.OpenWithIdentity(source, sourceNode)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.Append(context.Background(), engine.AppendRequest{Namespace: "n", Stream: "s", RequestID: []byte("source"), Producer: "test", Records: []engine.InputRecord{{Payload: []byte("record")}}}); err != nil {
		t.Fatal(err)
	}
	snapshotPath := filepath.Join(base, "snapshot")
	if _, err = CreateOnline(store, snapshotPath); err != nil {
		t.Fatal(err)
	}
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
	for _, crashPoint := range []string{"", "after_install_journal", "after_install_artifacts", "after_install_wal", "after_install_current", "after_install_state"} {
		t.Run(crashPoint, func(t *testing.T) {
			target := filepath.Join(base, "target-"+crashPoint)
			targetNode := format.NodeIdentity{ClusterID: sourceNode.ClusterID, GroupID: sourceNode.GroupID, NodeID: snapshotID(4), CreatedAt: 2}
			targetStore, openErr := engine.OpenWithIdentity(target, targetNode)
			if openErr != nil {
				t.Fatal(openErr)
			}
			if closeErr := targetStore.Close(); closeErr != nil {
				t.Fatal(closeErr)
			}
			hook := func(point string) error {
				if point == crashPoint {
					return errInstallCrash
				}
				return nil
			}
			_, installErr := Install(target, snapshotPath, InstallOptions{Term: 7, LeaderID: sourceNode.NodeID, Hook: hook})
			if crashPoint == "" {
				if installErr != nil {
					t.Fatal(installErr)
				}
			} else {
				if !errors.Is(installErr, errInstallCrash) {
					t.Fatalf("Install error = %v", installErr)
				}
				resumed, resumeErr := ResumeInstall(target, nil)
				if resumeErr != nil || !resumed {
					t.Fatalf("Resume = %v, %v", resumed, resumeErr)
				}
			}
			resumed, resumeErr := ResumeInstall(target, nil)
			if resumeErr != nil || resumed {
				t.Fatalf("completed Resume = %v, %v", resumed, resumeErr)
			}
			reopened, openErr := engine.OpenReplicated(target, targetNode, engine.ReplicationOptions{Term: 7, Role: format.ReplicationRoleStandby, Durability: format.ReplicationDurabilityStrict, Guard: &snapshotGuard{}})
			if openErr != nil {
				t.Fatal(openErr)
			}
			defer reopened.Close()
			read, readErr := reopened.Read("n", "s", 0, 10, 0)
			if readErr != nil || len(read.Records) != 1 || string(read.Records[0].Payload) != "record" {
				t.Fatalf("Read = %+v, %v", read, readErr)
			}
		})
	}
}

func snapshotID(value byte) format.UUID {
	var id format.UUID
	id[15] = value
	return id
}
