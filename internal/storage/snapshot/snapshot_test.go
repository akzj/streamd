package snapshot

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/akzj/streamd/internal/storage/engine"
	"github.com/akzj/streamd/internal/storage/format"
	"github.com/akzj/streamd/internal/storage/replicationstate"
	"github.com/akzj/streamd/internal/storage/scrub"
)

var errInstallCrash = errors.New("install crash")

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

func TestOfflineCreatePreservesDurableReplicationTerm(t *testing.T) {
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
	created, err := Create(data, filepath.Join(t.TempDir(), "snapshot"))
	if err != nil {
		t.Fatal(err)
	}
	if created.Term != 7 {
		t.Fatalf("Snapshot Term = %d, want 7", created.Term)
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
			reopened, openErr := engine.OpenWithIdentity(target, targetNode)
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
