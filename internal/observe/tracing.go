package observe

import (
	"context"
	"crypto/tls"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"google.golang.org/grpc/credentials"
)

type TracingConfig struct {
	OTLPEndpoint string
	ServiceName  string
}

func StartTracing(ctx context.Context, config TracingConfig) (func(context.Context) error, error) {
	if config.OTLPEndpoint == "" {
		return func(context.Context) error { return nil }, nil
	}
	if config.ServiceName == "" {
		return nil, fmt.Errorf("trace service name is required")
	}
	exporter, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(config.OTLPEndpoint),
		otlptracegrpc.WithTLSCredentials(credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12})),
	)
	if err != nil {
		return nil, err
	}
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(resource.NewSchemaless(attribute.String("service.name", config.ServiceName))),
	)
	otel.SetTracerProvider(provider)
	return provider.Shutdown, nil
}
