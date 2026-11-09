package metrics

import (
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics holds the Prometheus collectors for proxy throughput, latency,
// active backend connections, and backend health state.
type Metrics struct {
	registry          *prometheus.Registry
	requestsTotal     *prometheus.CounterVec
	requestDuration   *prometheus.HistogramVec
	activeConnections *prometheus.GaugeVec
	backendHealthy    *prometheus.GaugeVec
}

// New creates and registers all proxy Prometheus collectors on an isolated registry.
func New() *Metrics {
	reg := prometheus.NewRegistry()

	requestsTotal := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "proxy_requests_total",
			Help: "Total number of HTTP/gRPC requests handled by the reverse proxy.",
		},
		[]string{"route", "backend", "status_code"},
	)

	requestDuration := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "proxy_request_duration_seconds",
			Help:    "Upstream request duration in seconds.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"route", "backend"},
	)

	activeConnections := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "proxy_active_connections",
			Help: "Current number of active in-flight connections per backend.",
		},
		[]string{"backend"},
	)

	backendHealthy := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "proxy_backend_healthy",
			Help: "Current health status of each backend (1 = healthy, 0 = unhealthy).",
		},
		[]string{"backend"},
	)

	reg.MustRegister(requestsTotal, requestDuration, activeConnections, backendHealthy)

	return &Metrics{
		registry:          reg,
		requestsTotal:     requestsTotal,
		requestDuration:   requestDuration,
		activeConnections: activeConnections,
		backendHealthy:    backendHealthy,
	}
}

// RecordRequest records request throughput and latency for a completed request.
func (m *Metrics) RecordRequest(route, backend string, statusCode int, duration time.Duration) {
	if m == nil {
		return
	}
	m.requestsTotal.WithLabelValues(route, backend, strconv.Itoa(statusCode)).Inc()
	m.requestDuration.WithLabelValues(route, backend).Observe(duration.Seconds())
}

// SetActiveConnections sets the current active connection gauge for a backend.
func (m *Metrics) SetActiveConnections(backend string, active int64) {
	if m == nil {
		return
	}
	m.activeConnections.WithLabelValues(backend).Set(float64(active))
}

// SetBackendHealthy updates the health gauge (1 or 0) for a backend.
func (m *Metrics) SetBackendHealthy(backend string, healthy bool) {
	if m == nil {
		return
	}
	if healthy {
		m.backendHealthy.WithLabelValues(backend).Set(1)
	} else {
		m.backendHealthy.WithLabelValues(backend).Set(0)
	}
}

// Handler returns an http.Handler serving the Prometheus /metrics exposition endpoint.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}
