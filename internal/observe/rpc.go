package observe

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

type RPCMetrics struct {
	requests *prometheus.CounterVec
	duration *prometheus.HistogramVec
	active   *prometheus.GaugeVec
}

func NewRPCMetrics(registerer prometheus.Registerer) *RPCMetrics {
	m := &RPCMetrics{
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "streamd_rpc_requests_total", Help: "Completed streamd RPCs."}, []string{"method", "code"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "streamd_rpc_duration_seconds", Help: "streamd RPC latency.", Buckets: prometheus.DefBuckets}, []string{"method"}),
		active:   prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "streamd_rpc_active", Help: "Active streamd RPCs."}, []string{"method"}),
	}
	registerer.MustRegister(m.requests, m.duration, m.active)
	return m
}

func (m *RPCMetrics) UnaryInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (response any, err error) {
		started := time.Now()
		m.active.WithLabelValues(info.FullMethod).Inc()
		defer func() {
			m.active.WithLabelValues(info.FullMethod).Dec()
			m.duration.WithLabelValues(info.FullMethod).Observe(time.Since(started).Seconds())
			m.requests.WithLabelValues(info.FullMethod, status.Code(err).String()).Inc()
		}()
		return handler(ctx, request)
	}
}

func (m *RPCMetrics) StreamInterceptor() grpc.StreamServerInterceptor {
	return func(server any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) (err error) {
		started := time.Now()
		m.active.WithLabelValues(info.FullMethod).Inc()
		defer func() {
			m.active.WithLabelValues(info.FullMethod).Dec()
			m.duration.WithLabelValues(info.FullMethod).Observe(time.Since(started).Seconds())
			m.requests.WithLabelValues(info.FullMethod, status.Code(err).String()).Inc()
		}()
		return handler(server, stream)
	}
}
