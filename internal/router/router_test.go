package router

import (
	"sync"
	"testing"

	"github.com/ethanra700/Reverse-Proxy-Load-Balancer/internal/config"
)

func TestRouterLongestPrefixMatching(t *testing.T) {
	cfg, err := config.Parse([]byte(`
pools:
  root:
    backends: ["http://root:8080"]
  api:
    backends: ["http://api:8081"]
  api-v2:
    backends: ["http://api-v2:8082"]
  grpc:
    protocol: h2c
    backends: ["http://grpc:9001"]
routes:
  - prefix: "/"
    pool: "root"
  - prefix: "/api"
    pool: "api"
  - prefix: "/api/v2"
    pool: "api-v2"
  - prefix: "/echo.EchoService"
    pool: "grpc"
`))
	if err != nil {
		t.Fatalf("config.Parse: %v", err)
	}

	rt, err := New(cfg)
	if err != nil {
		t.Fatalf("router.New: %v", err)
	}

	tests := []struct {
		path     string
		wantPool string
	}{
		{path: "/", wantPool: "root"},
		{path: "/healthz", wantPool: "root"},
		{path: "/api", wantPool: "api"},
		{path: "/api/users", wantPool: "api"},
		{path: "/api/v2", wantPool: "api-v2"},
		{path: "/api/v2/items/42", wantPool: "api-v2"},
		{path: "/apiv2/users", wantPool: "root"}, // segment boundary check
		{path: "/echo.EchoService/Ping", wantPool: "grpc"},
	}

	for _, tc := range tests {
		route, ok := rt.Match(tc.path)
		if !ok {
			t.Fatalf("path %q: expected match, got none", tc.path)
		}
		if route.Pool.Name != tc.wantPool {
			t.Fatalf("path %q: expected pool %q, got %q", tc.path, tc.wantPool, route.Pool.Name)
		}
	}
}

func TestDynamicRouterAtomicSwapConcurrency(t *testing.T) {
	cfgA, _ := config.Parse([]byte(`
pools:
  a:
    backends: ["http://a:8081"]
routes:
  - prefix: "/"
    pool: "a"
`))
	cfgB, _ := config.Parse([]byte(`
pools:
  b:
    backends: ["http://b:8082"]
routes:
  - prefix: "/"
    pool: "b"
`))
	rtA, _ := New(cfgA)
	rtB, _ := New(cfgB)
	dyn := NewDynamic(rtA)

	var wg sync.WaitGroup
	// Concurrent readers.
	for i := 0; i < 24; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 500; j++ {
				route, ok := dyn.Match("/items")
				if !ok || (route.Pool.Name != "a" && route.Pool.Name != "b") {
					t.Errorf("unexpected route match: ok=%v route=%+v", ok, route)
					return
				}
				_ = dyn.AllBackends()
			}
		}()
	}

	// Concurrent hot swaps.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for j := 0; j < 200; j++ {
			if j%2 == 0 {
				dyn.Swap(rtB)
			} else {
				dyn.Swap(rtA)
			}
		}
	}()

	wg.Wait()
}
