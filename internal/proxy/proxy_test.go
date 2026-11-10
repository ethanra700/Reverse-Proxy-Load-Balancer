package proxy_test

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	echopb "github.com/ethanra700/Reverse-Proxy-Load-Balancer/cmd/dummy-grpc/proto"
	"github.com/ethanra700/Reverse-Proxy-Load-Balancer/internal/config"
	"github.com/ethanra700/Reverse-Proxy-Load-Balancer/internal/metrics"
	"github.com/ethanra700/Reverse-Proxy-Load-Balancer/internal/proxy"
	"github.com/ethanra700/Reverse-Proxy-Load-Balancer/internal/router"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func buildTestProxy(t *testing.T, yamlCfg string) (*proxy.Proxy, *router.DynamicRouter, *metrics.Metrics) {
	t.Helper()
	cfg, err := config.Parse([]byte(yamlCfg))
	if err != nil {
		t.Fatalf("config.Parse: %v", err)
	}
	rt, err := router.New(cfg)
	if err != nil {
		t.Fatalf("router.New: %v", err)
	}
	dyn := router.NewDynamic(rt)
	m := metrics.New()
	p := proxy.New(dyn, cfg.Transport, cfg.HealthCheck.UnhealthyThreshold, m)
	return p, dyn, m
}

func TestProxyRetryOnFailedBackend(t *testing.T) {
	// Start a backend and close it immediately to simulate a dead upstream (connection refused).
	dead := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	deadURL := dead.URL
	dead.Close()

	alive := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Backend-ID", "alive-1")
		_, _ = w.Write([]byte("ok-from-alive"))
	}))
	defer alive.Close()

	yamlCfg := fmt.Sprintf(`
pools:
  http:
    backends:
      - %q
      - %q
routes:
  - prefix: "/"
    pool: "http"
`, deadURL, alive.URL)

	p, _, m := buildTestProxy(t, yamlCfg)
	defer p.CloseIdleConnections()

	proxySrv := httptest.NewServer(p)
	defer proxySrv.Close()

	// Multiple GET requests must all succeed: any hitting the dead backend retry seamlessly onto alive.
	for i := 0; i < 6; i++ {
		resp, err := http.Get(proxySrv.URL + "/hello")
		if err != nil {
			t.Fatalf("request %d failed: %v", i, err)
		}
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Fatalf("request %d: expected 200 OK after failover retry, got %d", i, resp.StatusCode)
		}
		if string(body) != "ok-from-alive" {
			t.Fatalf("request %d: unexpected body %q", i, string(body))
		}
	}

	// Verify Prometheus /metrics exposition contains expected proxy metrics.
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	m.Handler().ServeHTTP(rec, req)
	metricsBody := rec.Body.String()
	for _, want := range []string{
		"proxy_requests_total",
		"proxy_request_duration_seconds",
		"proxy_active_connections",
	} {
		if !strings.Contains(metricsBody, want) {
			t.Fatalf("expected /metrics output to contain %q", want)
		}
	}
}

func TestProxyHotConfigReloadUnderTraffic(t *testing.T) {
	backendA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("backend-a"))
	}))
	defer backendA.Close()

	backendB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("backend-b"))
	}))
	defer backendB.Close()

	cfgAYAML := fmt.Sprintf(`
pools:
  p:
    backends: [%q]
routes:
  - prefix: "/"
    pool: "p"
`, backendA.URL)
	cfgBYAML := fmt.Sprintf(`
pools:
  p:
    backends: [%q]
routes:
  - prefix: "/"
    pool: "p"
`, backendB.URL)

	p, dyn, _ := buildTestProxy(t, cfgAYAML)
	defer p.CloseIdleConnections()
	proxySrv := httptest.NewServer(p)
	defer proxySrv.Close()

	// Verify initial route goes to backend-a.
	resp, err := http.Get(proxySrv.URL + "/check")
	if err != nil {
		t.Fatalf("initial request: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if string(body) != "backend-a" {
		t.Fatalf("expected backend-a before reload, got %q", string(body))
	}

	cfgB, err := config.Parse([]byte(cfgBYAML))
	if err != nil {
		t.Fatalf("parse cfgB: %v", err)
	}
	rtB, err := router.New(cfgB)
	if err != nil {
		t.Fatalf("router.New(cfgB): %v", err)
	}

	// Swap router concurrently with in-flight HTTP traffic.
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				r, err := http.Get(proxySrv.URL + "/check")
				if err != nil {
					t.Errorf("concurrent request failed: %v", err)
					return
				}
				b, _ := io.ReadAll(r.Body)
				_ = r.Body.Close()
				if r.StatusCode != http.StatusOK || (string(b) != "backend-a" && string(b) != "backend-b") {
					t.Errorf("unexpected response: status=%d body=%q", r.StatusCode, string(b))
					return
				}
			}
		}()
	}

	time.Sleep(10 * time.Millisecond)
	dyn.Swap(rtB)
	wg.Wait()

	// After swap, all traffic must hit backend-b.
	resp, err = http.Get(proxySrv.URL + "/check")
	if err != nil {
		t.Fatalf("post-reload request: %v", err)
	}
	body, _ = io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if string(body) != "backend-b" {
		t.Fatalf("expected backend-b after reload, got %q", string(body))
	}
}

type testEchoServer struct {
	echopb.UnimplementedEchoServiceServer
	id string
}

func (s *testEchoServer) Ping(_ context.Context, req *echopb.PingRequest) (*echopb.PongResponse, error) {
	return &echopb.PongResponse{
		BackendId: s.id,
		Message:   req.GetMessage(),
	}, nil
}

func startTestGRPCBackend(t *testing.T, id string) (string, func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen gRPC backend %s: %v", id, err)
	}

	grpcSrv := grpc.NewServer()
	echopb.RegisterEchoServiceServer(grpcSrv, &testEchoServer{id: id})

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.ProtoMajor == 2 && strings.HasPrefix(r.Header.Get("Content-Type"), "application/grpc") {
			grpcSrv.ServeHTTP(w, r)
			return
		}
		if r.URL.Path == "/health" {
			w.WriteHeader(http.StatusOK)
			return
		}
		http.NotFound(w, r)
	})

	srv := &http.Server{
		Handler:   handler,
		Protocols: proxy.NewServerProtocols(),
	}
	go func() {
		_ = srv.Serve(ln)
	}()

	cleanup := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
		grpcSrv.Stop()
	}
	return "http://" + ln.Addr().String(), cleanup
}

func TestProxyGRPCPerRPCLoadBalancingOverH2C(t *testing.T) {
	url1, stop1 := startTestGRPCBackend(t, "grpc-1")
	defer stop1()
	url2, stop2 := startTestGRPCBackend(t, "grpc-2")
	defer stop2()
	url3, stop3 := startTestGRPCBackend(t, "grpc-3")
	defer stop3()

	yamlCfg := fmt.Sprintf(`
pools:
  grpc-backends:
    protocol: "h2c"
    backends:
      - %q
      - %q
      - %q
routes:
  - prefix: "/echo.EchoService"
    pool: "grpc-backends"
`, url1, url2, url3)

	p, _, _ := buildTestProxy(t, yamlCfg)
	defer p.CloseIdleConnections()

	proxyLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen proxy: %v", err)
	}
	proxySrv := &http.Server{
		Handler:   p,
		Protocols: proxy.NewServerProtocols(),
	}
	go func() {
		_ = proxySrv.Serve(proxyLn)
	}()
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = proxySrv.Shutdown(ctx)
	}()

	// Single multiplexed HTTP/2 gRPC client connection to the proxy.
	conn, err := grpc.NewClient(proxyLn.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	defer func() { _ = conn.Close() }()

	client := echopb.NewEchoServiceClient(conn)
	counts := make(map[string]int)

	for i := 0; i < 18; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		msg := fmt.Sprintf("ping-%d", i)
		resp, err := client.Ping(ctx, &echopb.PingRequest{Message: msg})
		cancel()
		if err != nil {
			t.Fatalf("Ping %d failed over h2c proxy: %v", i, err)
		}
		if resp.GetMessage() != msg {
			t.Fatalf("Ping %d: expected message %q, got %q", i, msg, resp.GetMessage())
		}
		counts[resp.GetBackendId()]++
	}

	for _, id := range []string{"grpc-1", "grpc-2", "grpc-3"} {
		if counts[id] == 0 {
			t.Fatalf("expected per-RPC load balancing to reach %s, got distribution %v", id, counts)
		}
	}
}
