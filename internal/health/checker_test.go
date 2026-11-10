package health

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethanra700/Reverse-Proxy-Load-Balancer/internal/balancer"
	"github.com/ethanra700/Reverse-Proxy-Load-Balancer/internal/config"
	"github.com/ethanra700/Reverse-Proxy-Load-Balancer/internal/metrics"
)

func TestActiveHealthCheckTransitionsAndRecovery(t *testing.T) {
	var ok atomic.Bool
	ok.Store(true)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/health" {
			http.NotFound(w, r)
			return
		}
		if ok.Load() {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	u, _ := url.Parse(srv.URL)
	backend := balancer.NewBackend(u)
	m := metrics.New()

	checker := NewChecker(config.HealthCheckConfig{
		Interval:           25 * time.Millisecond,
		Timeout:            500 * time.Millisecond,
		Path:               "/health",
		UnhealthyThreshold: 3,
	}, func() []*balancer.Backend {
		return []*balancer.Backend{backend}
	}, m)

	// Healthy check keeps backend healthy.
	if !checker.CheckBackend(context.Background(), backend) || !backend.IsHealthy() {
		t.Fatal("expected backend healthy when /health returns 200")
	}

	// Failing /health transitions backend to unhealthy.
	ok.Store(false)
	if checker.CheckBackend(context.Background(), backend) || backend.IsHealthy() {
		t.Fatal("expected backend unhealthy when /health returns 503")
	}

	// Recovery transitions backend back to healthy and clears passive failures.
	ok.Store(true)
	if !checker.CheckBackend(context.Background(), backend) || !backend.IsHealthy() {
		t.Fatal("expected backend to recover when /health returns 200")
	}
}

func TestPassiveFailureTrippedThenRecoveredByActiveChecker(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	u, _ := url.Parse(srv.URL)
	backend := balancer.NewBackend(u)
	m := metrics.New()

	// Trip unhealthy passively after 3 consecutive failures.
	RecordPassiveFailure(backend, 3, m)
	RecordPassiveFailure(backend, 3, m)
	if !backend.IsHealthy() {
		t.Fatal("expected backend still healthy after 2 failures")
	}
	if !RecordPassiveFailure(backend, 3, m) || backend.IsHealthy() {
		t.Fatal("expected backend unhealthy after 3rd consecutive failure")
	}

	checker := NewChecker(config.HealthCheckConfig{
		Interval:           20 * time.Millisecond,
		Timeout:            500 * time.Millisecond,
		Path:               "/health",
		UnhealthyThreshold: 3,
	}, func() []*balancer.Backend {
		return []*balancer.Backend{backend}
	}, m)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		checker.Run(ctx)
		close(done)
	}()

	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		if backend.IsHealthy() && backend.ConsecutiveFailures() == 0 {
			break
		}
		time.Sleep(15 * time.Millisecond)
	}
	cancel()
	<-done

	if !backend.IsHealthy() || backend.ConsecutiveFailures() != 0 {
		t.Fatalf("expected active health check to recover backend (healthy=%v, failures=%d)",
			backend.IsHealthy(), backend.ConsecutiveFailures())
	}
}
