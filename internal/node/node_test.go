package node

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/akzj/streamd/internal/access"
	"github.com/akzj/streamd/internal/diagnostics"
	"github.com/akzj/streamd/internal/service"
	"github.com/akzj/streamd/internal/storage/engine"
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
