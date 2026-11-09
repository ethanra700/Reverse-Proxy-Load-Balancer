package health

import (
	"context"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/ethanra700/Reverse-Proxy-Load-Balancer/internal/balancer"
	"github.com/ethanra700/Reverse-Proxy-Load-Balancer/internal/config"
	"github.com/ethanra700/Reverse-Proxy-Load-Balancer/internal/metrics"
)

// Checker performs periodic active HTTP health checks against all configured backends.
type Checker struct {
	interval    time.Duration
	timeout     time.Duration
	path        string
	client      *http.Client
	getBackends func() []*balancer.Backend
	metrics     *metrics.Metrics
}

// NewChecker creates an active health checker configured from HealthCheckConfig.
func NewChecker(cfg config.HealthCheckConfig, getBackends func() []*balancer.Backend, m *metrics.Metrics) *Checker {
	dialer := &net.Dialer{
		Timeout:   cfg.Timeout,
		KeepAlive: 30 * time.Second,
	}
	transport := &http.Transport{
		DialContext:         dialer.DialContext,
		MaxIdleConnsPerHost: 4,
		IdleConnTimeout:     30 * time.Second,
	}
	c := &Checker{
		interval:    cfg.Interval,
		timeout:     cfg.Timeout,
		path:        cfg.Path,
		client:      &http.Client{Transport: transport},
		getBackends: getBackends,
		metrics:     m,
	}
	c.SyncMetrics()
	return c
}

// SyncMetrics initializes the health gauge for all currently configured backends.
func (c *Checker) SyncMetrics() {
	if c.metrics == nil {
		return
	}
	for _, b := range c.getBackends() {
		c.metrics.SetBackendHealthy(b.URL.Host, b.IsHealthy())
	}
}

// Run starts the ticker loop and blocks until ctx is canceled.
func (c *Checker) Run(ctx context.Context) {
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.CheckAll(ctx)
		}
	}
}

// CheckAll probes every backend returned by getBackends concurrently.
func (c *Checker) CheckAll(ctx context.Context) {
	backends := c.getBackends()
	var wg sync.WaitGroup
	for _, b := range backends {
		wg.Add(1)
		go func(backend *balancer.Backend) {
			defer wg.Done()
			c.CheckBackend(ctx, backend)
		}(b)
	}
	wg.Wait()
}

// CheckBackend sends a single GET request to the backend's health path and updates its status.
func (c *Checker) CheckBackend(ctx context.Context, b *balancer.Backend) bool {
	reqCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	healthURL := strings.TrimRight(b.URL.String(), "/") + c.path
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, healthURL, nil)
	if err != nil {
		b.SetHealthy(false)
		c.metrics.SetBackendHealthy(b.URL.Host, false)
		return false
	}

	resp, err := c.client.Do(req)
	healthy := false
	if err == nil {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		healthy = resp.StatusCode >= 200 && resp.StatusCode < 300
	}

	prev := b.IsHealthy()
	b.SetHealthy(healthy)
	c.metrics.SetBackendHealthy(b.URL.Host, healthy)
	if prev != healthy {
		if healthy {
			log.Printf("health check [%s]: recovered (healthy=true)", b.URL.Host)
		} else {
			log.Printf("health check [%s]: marked unhealthy (healthy=false)", b.URL.Host)
		}
	}
	return healthy
}

// RecordPassiveFailure updates a backend's consecutive failure count and logs if tripped unhealthy.
func RecordPassiveFailure(b *balancer.Backend, threshold int32, m *metrics.Metrics) bool {
	if b.RecordFailure(threshold) {
		m.SetBackendHealthy(b.URL.Host, false)
		log.Printf("passive health [%s]: tripped unhealthy after %d consecutive failures", b.URL.Host, threshold)
		return true
	}
	return false
}
