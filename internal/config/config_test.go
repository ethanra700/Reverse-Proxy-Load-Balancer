package config

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestParseAppliesDefaultsAndValidates(t *testing.T) {
	raw := []byte(`
pools:
  api:
    backends:
      - "http://localhost:8081"
routes:
  - prefix: "/"
    pool: "api"
`)
	cfg, err := Parse(raw)
	if err != nil {
		t.Fatalf("Parse() unexpected error: %v", err)
	}
	if cfg.ListenAddr != ":8080" {
		t.Fatalf("expected default listen_addr :8080, got %q", cfg.ListenAddr)
	}
	if cfg.HealthCheck.Interval != 5*time.Second {
		t.Fatalf("expected default health_check.interval 5s, got %s", cfg.HealthCheck.Interval)
	}
	if cfg.HealthCheck.UnhealthyThreshold != 3 {
		t.Fatalf("expected default unhealthy_threshold 3, got %d", cfg.HealthCheck.UnhealthyThreshold)
	}
	if cfg.Pools["api"].Protocol != "http" {
		t.Fatalf("expected default pool protocol http, got %q", cfg.Pools["api"].Protocol)
	}
}

func TestParseValidationErrors(t *testing.T) {
	cases := []struct {
		name string
		yaml string
	}{
		{
			name: "missing pools",
			yaml: "routes:\n  - prefix: /\n    pool: missing\n",
		},
		{
			name: "unknown route pool",
			yaml: "pools:\n  a:\n    backends: [http://localhost:8081]\nroutes:\n  - prefix: /\n    pool: b\n",
		},
		{
			name: "invalid backend url",
			yaml: "pools:\n  a:\n    backends: [ftp://bad]\nroutes:\n  - prefix: /\n    pool: a\n",
		},
		{
			name: "unsupported protocol",
			yaml: "pools:\n  a:\n    protocol: quic\n    backends: [http://localhost:8081]\nroutes:\n  - prefix: /\n    pool: a\n",
		},
		{
			name: "duplicate route prefix",
			yaml: "pools:\n  a:\n    backends: [http://localhost:8081]\nroutes:\n  - prefix: /api\n    pool: a\n  - prefix: /api\n    pool: a\n",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := Parse([]byte(tc.yaml)); err == nil {
				t.Fatalf("expected validation error for %s, got nil", tc.name)
			}
		})
	}
}

func TestWatchReloadsValidAndIgnoresInvalid(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.yaml")

	initial := "pools:\n  a:\n    backends: [http://localhost:8081]\nroutes:\n  - prefix: /\n    pool: a\n"
	if err := os.WriteFile(cfgPath, []byte(initial), 0o600); err != nil {
		t.Fatalf("write initial config: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reloaded := make(chan *Config, 2)
	go Watch(ctx, cfgPath, 20*time.Millisecond, func(c *Config) {
		reloaded <- c
	})

	// Write an invalid config first; watcher must ignore it.
	time.Sleep(40 * time.Millisecond)
	if err := os.WriteFile(cfgPath, []byte("pools: {}\nroutes: []\n"), 0o600); err != nil {
		t.Fatalf("write invalid config: %v", err)
	}
	future := time.Now().Add(2 * time.Second)
	_ = os.Chtimes(cfgPath, future, future)

	select {
	case cfg := <-reloaded:
		t.Fatalf("expected invalid config to be ignored, got reload: %+v", cfg)
	case <-time.After(80 * time.Millisecond):
	}

	// Write a valid updated config and ensure onReload fires.
	updated := "pools:\n  b:\n    backends: [http://localhost:8082]\nroutes:\n  - prefix: /\n    pool: b\n"
	if err := os.WriteFile(cfgPath, []byte(updated), 0o600); err != nil {
		t.Fatalf("write updated config: %v", err)
	}
	future2 := future.Add(2 * time.Second)
	_ = os.Chtimes(cfgPath, future2, future2)

	select {
	case cfg := <-reloaded:
		if _, ok := cfg.Pools["b"]; !ok {
			t.Fatalf("expected pool 'b' in reloaded config, got %+v", cfg.Pools)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for hot config reload")
	}
}
