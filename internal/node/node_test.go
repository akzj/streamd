package node

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/akzj/streamd/internal/access"
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
	for path, want := range map[string]int{"/livez": http.StatusOK, "/readyz": http.StatusOK, "/metrics": http.StatusOK} {
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
}
