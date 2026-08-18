package node

import (
	"context"
	"errors"
	"net"
	"net/http"

	"github.com/akzj/streamd/internal/diagnostics"
	"github.com/akzj/streamd/internal/observe"
	"github.com/prometheus/client_golang/prometheus"
)

// adminRuntime owns one Admin listener and metrics registry across replicated
// startup, recovery-blocked, and ready role states. Public gRPC remains a
// separate lifecycle and is opened only after the role is safe to serve.
type adminRuntime struct {
	provider *diagnostics.SwitchableProvider
	registry *prometheus.Registry
	listener net.Listener
	server   *http.Server
	serveErr chan error
}

func startAdminRuntime(config Config, initial diagnostics.Provider) (*adminRuntime, error) {
	provider, err := diagnostics.NewSwitchableProvider(initial)
	if err != nil {
		return nil, err
	}
	registry := prometheus.NewRegistry()
	registry.MustRegister(prometheus.NewGoCollector(), prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))
	metrics, err := observe.NewNodeMetrics(config.DataDirectory, provider)
	if err != nil {
		return nil, err
	}
	registry.MustRegister(metrics)
	listener, err := net.Listen("tcp", config.AdminAddress)
	if err != nil {
		return nil, err
	}
	server := adminServer(provider, registry)
	runtime := &adminRuntime{provider: provider, registry: registry, listener: listener, server: server, serveErr: make(chan error, 1)}
	go func() { runtime.serveErr <- server.Serve(listener) }()
	return runtime, nil
}

func (r *adminRuntime) setProvider(provider diagnostics.Provider) error {
	return r.provider.Set(provider)
}

func (r *adminRuntime) monitor(ctx context.Context, cancel context.CancelFunc) <-chan error {
	result := make(chan error, 1)
	go func() {
		select {
		case err := <-r.serveErr:
			if errors.Is(err, http.ErrServerClosed) {
				err = nil
			}
			if err != nil {
				cancel()
			}
			result <- err
		case <-ctx.Done():
			result <- nil
		}
	}()
	return result
}

func (r *adminRuntime) shutdown(config Config) error {
	timeout, _ := config.shutdownDuration()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	err := r.server.Shutdown(ctx)
	_ = r.listener.Close()
	return err
}
