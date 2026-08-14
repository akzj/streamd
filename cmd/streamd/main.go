package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/akzj/streamd/internal/node"
	"github.com/akzj/streamd/internal/observe"
)

func main() {
	configPath := flag.String("config", "", "path to streamd JSON configuration")
	flag.Parse()
	if *configPath == "" {
		fmt.Fprintln(os.Stderr, "streamd: -config is required")
		os.Exit(2)
	}
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	config, err := node.LoadConfig(*configPath)
	if err != nil {
		logger.Error("configuration failed", "error", err)
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	shutdownTracing, err := observe.StartTracing(ctx, observe.TracingConfig{OTLPEndpoint: config.OTLPTraceEndpoint, ServiceName: "streamd"})
	if err != nil {
		logger.Error("tracing initialization failed", "error", err)
		os.Exit(1)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = shutdownTracing(shutdownCtx)
	}()
	if err = node.Run(ctx, config, logger); err != nil {
		logger.Error("streamd stopped with error", "error", err)
		os.Exit(1)
	}
}
