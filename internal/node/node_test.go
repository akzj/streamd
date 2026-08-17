package node

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/akzj/streamd/internal/access"
	"github.com/akzj/streamd/internal/diagnostics"
	"github.com/akzj/streamd/internal/service"
	"github.com/akzj/streamd/internal/storage/engine"
	"github.com/akzj/streamd/internal/storage/format"
	"github.com/akzj/streamd/internal/storage/replicationstate"
	"github.com/akzj/streamd/internal/storage/snapshot"
	"github.com/prometheus/client_golang/prometheus"
)

func TestAdminHealthAndDrainReadiness(t *testing.T) {
	store, err := engine.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	authorizer := access.AuthorizeFunc(func(context.Context, string, string, access.Operation) (access.Principal, error) {
		return access.Principal{Tenant: "test", Service: "node"}, nil
	})
	streamService, err := service.New(store, authorizer)
	if err != nil {
		t.Fatal(err)
	}
	registry := prometheus.NewRegistry()
	admin := adminServer(streamService, registry)
	for path, want := range map[string]int{"/livez": http.StatusOK, "/readyz": http.StatusOK, "/diagnostics": http.StatusOK, "/metrics": http.StatusOK} {
		request := httptest.NewRequest(http.MethodGet, path, nil)
		response := httptest.NewRecorder()
		admin.Handler.ServeHTTP(response, request)
		if response.Code != want {
			t.Fatalf("%s status = %d", path, response.Code)
		}
	}
	streamService.BeginDrain()
	response := httptest.NewRecorder()
	admin.Handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("draining readiness = %d", response.Code)
	}
	var snapshot diagnostics.Snapshot
	if err = json.Unmarshal(response.Body.Bytes(), &snapshot); err != nil {
		t.Fatal(err)
	}
	if snapshot.SchemaVersion != "v1" || snapshot.Ready || snapshot.WriteReady || snapshot.Status != diagnostics.StatusReadyRead || len(snapshot.Reasons) != 1 || snapshot.Reasons[0].Code != diagnostics.ReasonServerDraining {
		t.Fatalf("draining diagnostics = %+v", snapshot)
	}
	response = httptest.NewRecorder()
	admin.Handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/diagnostics", nil))
	if response.Code != http.StatusOK || response.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("diagnostics response = %d, cache %q", response.Code, response.Header().Get("Cache-Control"))
	}
	response = httptest.NewRecorder()
	admin.Handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/readyz", nil))
	if response.Code != http.StatusMethodNotAllowed || response.Header().Get("Allow") != "GET, HEAD" {
		t.Fatalf("POST readiness = %d, allow %q", response.Code, response.Header().Get("Allow"))
	}
}

func TestResumePendingSnapshotInstallBeforeNodeRecovery(t *testing.T) {
	base := t.TempDir()
	source := filepath.Join(base, "source")
	sourceNode := format.NodeIdentity{ClusterID: nodeTestID(1), GroupID: nodeTestID(2), NodeID: nodeTestID(3), CreatedAt: 1}
	store, err := engine.OpenWithIdentity(source, sourceNode)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.Append(context.Background(), engine.AppendRequest{Namespace: "n", Stream: "s", RequestID: []byte("r"), Producer: "test", Records: []engine.InputRecord{{Payload: []byte("record")}}}); err != nil {
		store.Close()
		t.Fatal(err)
	}
	snapshotPath := filepath.Join(base, "snapshot")
	if _, err = snapshot.CreateOnline(store, snapshotPath); err != nil {
		store.Close()
		t.Fatal(err)
	}
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}

	target := filepath.Join(base, "target")
	targetNode := format.NodeIdentity{ClusterID: sourceNode.ClusterID, GroupID: sourceNode.GroupID, NodeID: nodeTestID(4), CreatedAt: 2}
	targetStore, err := engine.OpenWithIdentity(target, targetNode)
	if err != nil {
		t.Fatal(err)
	}
	if err = targetStore.Close(); err != nil {
		t.Fatal(err)
	}
	injected := errors.New("stop after install journal")
	_, err = snapshot.Install(target, snapshotPath, snapshot.InstallOptions{Term: 7, LeaderID: sourceNode.NodeID, Hook: func(point string) error {
		if point == "after_install_journal" {
			return injected
		}
		return nil
	}})
	if !errors.Is(err, injected) {
		t.Fatalf("Install error = %v", err)
	}
	if _, err = os.Stat(filepath.Join(target, "SNAPSHOT-INSTALL.json")); err != nil {
		t.Fatalf("pending install journal: %v", err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	if err = resumePendingSnapshotInstall(target, logger); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(filepath.Join(target, "SNAPSHOT-INSTALL.json")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("completed install journal still exists: %v", err)
	}
	states, err := replicationstate.Open(target, targetNode)
	if err != nil {
		t.Fatal(err)
	}
	current, ok := states.Current()
	if !ok || current.Header.Role != format.ReplicationRoleStandby || !current.Header.HasInstalledSnapshot {
		t.Fatalf("resumed Replication State = %+v, ok = %v", current.Header, ok)
	}
}

func nodeTestID(value byte) format.UUID {
	var id format.UUID
	id[15] = value
	return id
}
