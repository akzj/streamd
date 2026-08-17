package node

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	streamdv1 "github.com/akzj/streamd/api/streamd/v1"
	"github.com/akzj/streamd/internal/access"
	"github.com/akzj/streamd/internal/diagnostics"
	"github.com/akzj/streamd/internal/observe"
	"github.com/akzj/streamd/internal/service"
	"github.com/akzj/streamd/internal/storage/engine"
	"github.com/akzj/streamd/internal/storage/snapshot"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
)

func Run(ctx context.Context, config Config, logger *slog.Logger) error {
	if err := config.Validate(); err != nil {
		return err
	}
	if logger == nil {
		logger = slog.Default()
	}
	if config.Replication.Role != "" && config.Replication.Role != "single" {
		return runReplicated(ctx, config, logger)
	}
	credentials, err := config.serverCredentials()
	if err != nil {
		return fmt.Errorf("load mTLS credentials: %w", err)
	}
	nodeIdentity, _ := config.nodeIdentity()
	if resumed, resumeErr := snapshot.ResumeInstall(config.DataDirectory, nil); resumeErr != nil {
		return fmt.Errorf("resume Snapshot install: %w", resumeErr)
	} else if resumed {
		logger.Info("resumed Snapshot install before recovery")
	}
	store, err := engine.OpenWithIdentity(config.DataDirectory, nodeIdentity)
	if err != nil {
		return fmt.Errorf("open storage engine: %w", err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = store.Close()
		}
	}()
	authorizer := access.Controller{
		Authenticator: access.MTLSAuthenticator{PrincipalsByURI: config.PrincipalsByURI},
		Policy:        access.StaticPolicy{Rules: config.Authorization},
	}
	sendTimeout, _ := config.subscribeSendDuration()
	streamService, err := service.NewWithOptions(store, authorizer, service.Options{SubscribeSendTimeout: sendTimeout, Limits: config.Limits})
	if err != nil {
		return err
	}

	registry := prometheus.NewRegistry()
	registry.MustRegister(prometheus.NewGoCollector(), prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))
	nodeMetrics, err := observe.NewNodeMetrics(config.DataDirectory, streamService)
	if err != nil {
		return err
	}
	registry.MustRegister(nodeMetrics)
	rpcMetrics := observe.NewRPCMetrics(registry)
	grpcServer := grpc.NewServer(
		grpc.Creds(credentials),
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		grpc.ChainUnaryInterceptor(rpcMetrics.UnaryInterceptor()),
		grpc.ChainStreamInterceptor(rpcMetrics.StreamInterceptor()),
	)
	streamdv1.RegisterStreamServiceServer(grpcServer, streamService)

	grpcListener, err := net.Listen("tcp", config.ListenAddress)
	if err != nil {
		return err
	}
	defer grpcListener.Close()
	adminListener, err := net.Listen("tcp", config.AdminAddress)
	if err != nil {
		return err
	}
	defer adminListener.Close()
	admin := adminServer(streamService, registry)
	serveErrors := make(chan error, 2)
	go func() { serveErrors <- grpcServer.Serve(grpcListener) }()
	go func() { serveErrors <- admin.Serve(adminListener) }()
	checkpointCtx, stopCheckpoints := context.WithCancel(context.Background())
	checkpointDone := make(chan struct{})
	checkpointInterval, _ := config.checkpointDuration()
	go func() {
		defer close(checkpointDone)
		ticker := time.NewTicker(checkpointInterval)
		defer ticker.Stop()
		for {
			select {
			case <-checkpointCtx.Done():
				return
			case <-ticker.C:
				manifest, created, checkpointErr := store.Checkpoint()
				if checkpointErr != nil {
					logger.Error("checkpoint failed", "error", checkpointErr)
				} else if created {
					logger.Info("checkpoint published", "generation", manifest.Header.Generation, "entry_id", manifest.Header.LastEntryID)
				}
			}
		}
	}()
	logger.Info("streamd started", "grpc_address", grpcListener.Addr().String(), "admin_address", adminListener.Addr().String())

	var serveErr error
	select {
	case <-ctx.Done():
	case serveErr = <-serveErrors:
		if errors.Is(serveErr, grpc.ErrServerStopped) || errors.Is(serveErr, http.ErrServerClosed) {
			serveErr = nil
		}
	}
	stopCheckpoints()
	<-checkpointDone
	streamService.BeginDrain()
	shutdownTimeout, _ := config.shutdownDuration()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	_ = admin.Shutdown(shutdownCtx)
	stopped := make(chan struct{})
	go func() {
		grpcServer.GracefulStop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-shutdownCtx.Done():
		grpcServer.Stop()
		<-stopped
	}
	closeErr := store.Close()
	closed = true
	logger.Info("streamd stopped")
	return errors.Join(serveErr, closeErr)
}

func adminServer(provider diagnostics.Provider, registry *prometheus.Registry) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/livez", func(response http.ResponseWriter, request *http.Request) {
		if !readOnlyMethod(response, request) {
			return
		}
		response.Header().Set("Content-Type", "text/plain; charset=utf-8")
		response.WriteHeader(http.StatusOK)
		if request.Method != http.MethodHead {
			_, _ = response.Write([]byte("ok\n"))
		}
	})
	mux.HandleFunc("/readyz", diagnosticHandler(provider, true))
	mux.HandleFunc("/diagnostics", diagnosticHandler(provider, false))
	mux.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
	return &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 30 * time.Second}
}

func diagnosticHandler(provider diagnostics.Provider, readiness bool) http.HandlerFunc {
	return func(response http.ResponseWriter, request *http.Request) {
		if !readOnlyMethod(response, request) {
			return
		}
		snapshot := provider.Snapshot()
		response.Header().Set("Content-Type", "application/json")
		response.Header().Set("Cache-Control", "no-store")
		status := http.StatusOK
		if readiness && !snapshot.Ready {
			status = http.StatusServiceUnavailable
		}
		response.WriteHeader(status)
		if request.Method != http.MethodHead {
			_ = json.NewEncoder(response).Encode(snapshot)
		}
	}
}

func readOnlyMethod(response http.ResponseWriter, request *http.Request) bool {
	if request.Method == http.MethodGet || request.Method == http.MethodHead {
		return true
	}
	response.Header().Set("Allow", "GET, HEAD")
	http.Error(response, "method not allowed", http.StatusMethodNotAllowed)
	return false
}
