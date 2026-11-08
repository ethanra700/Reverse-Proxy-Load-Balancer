package config

import (
	"context"
	"fmt"
	"log"
	"net/url"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Config defines the top-level proxy configuration loaded from config.yaml.
type Config struct {
	ListenAddr  string                `yaml:"listen_addr"`
	HealthCheck HealthCheckConfig     `yaml:"health_check"`
	Transport   TransportConfig       `yaml:"transport"`
	Pools       map[string]PoolConfig `yaml:"pools"`
	Routes      []RouteConfig         `yaml:"routes"`
}

// HealthCheckConfig configures active and passive backend health checks.
type HealthCheckConfig struct {
	Interval           time.Duration `yaml:"interval"`
	Timeout            time.Duration `yaml:"timeout"`
	Path               string        `yaml:"path"`
	UnhealthyThreshold int32         `yaml:"unhealthy_threshold"`
}

// TransportConfig tunes the upstream HTTP connection pool and timeouts.
type TransportConfig struct {
	MaxIdleConns          int           `yaml:"max_idle_conns"`
	MaxIdleConnsPerHost   int           `yaml:"max_idle_conns_per_host"`
	IdleConnTimeout       time.Duration `yaml:"idle_conn_timeout"`
	DialTimeout           time.Duration `yaml:"dial_timeout"`
	ResponseHeaderTimeout time.Duration `yaml:"response_header_timeout"`
}

// PoolConfig represents a named pool of upstream backend URLs.
type PoolConfig struct {
	Protocol string   `yaml:"protocol"`
	Backends []string `yaml:"backends"`
}

// RouteConfig maps a URL path prefix to a named backend pool.
type RouteConfig struct {
	Prefix string `yaml:"prefix"`
	Pool   string `yaml:"pool"`
}

// Load reads, parses, and validates a YAML configuration file from disk.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config %q: %w", path, err)
	}
	return Parse(data)
}

// Parse unmarshals YAML bytes into a Config, applies defaults, and validates it.
func Parse(data []byte) (*Config, error) {
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse yaml: %w", err)
	}
	cfg.applyDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func (c *Config) applyDefaults() {
	if c.ListenAddr == "" {
		c.ListenAddr = ":8080"
	}
	if c.HealthCheck.Interval == 0 {
		c.HealthCheck.Interval = 5 * time.Second
	}
	if c.HealthCheck.Timeout == 0 {
		c.HealthCheck.Timeout = 2 * time.Second
	}
	if c.HealthCheck.Path == "" {
		c.HealthCheck.Path = "/health"
	}
	if c.HealthCheck.UnhealthyThreshold == 0 {
		c.HealthCheck.UnhealthyThreshold = 3
	}
	if c.Transport.MaxIdleConns == 0 {
		c.Transport.MaxIdleConns = 256
	}
	if c.Transport.MaxIdleConnsPerHost == 0 {
		c.Transport.MaxIdleConnsPerHost = 64
	}
	if c.Transport.IdleConnTimeout == 0 {
		c.Transport.IdleConnTimeout = 90 * time.Second
	}
	if c.Transport.DialTimeout == 0 {
		c.Transport.DialTimeout = 5 * time.Second
	}
	if c.Transport.ResponseHeaderTimeout == 0 {
		c.Transport.ResponseHeaderTimeout = 10 * time.Second
	}
	for name, pool := range c.Pools {
		if pool.Protocol == "" {
			pool.Protocol = "http"
			c.Pools[name] = pool
		}
	}
}

// Validate checks configuration invariants and returns a descriptive error if invalid.
func (c *Config) Validate() error {
	if strings.TrimSpace(c.ListenAddr) == "" {
		return fmt.Errorf("listen_addr must not be empty")
	}
	if c.HealthCheck.Interval <= 0 {
		return fmt.Errorf("health_check.interval must be positive")
	}
	if c.HealthCheck.Timeout <= 0 {
		return fmt.Errorf("health_check.timeout must be positive")
	}
	if !strings.HasPrefix(c.HealthCheck.Path, "/") {
		return fmt.Errorf("health_check.path must start with '/'")
	}
	if c.HealthCheck.UnhealthyThreshold < 1 {
		return fmt.Errorf("health_check.unhealthy_threshold must be >= 1")
	}
	if len(c.Pools) == 0 {
		return fmt.Errorf("at least one backend pool must be configured")
	}
	for name, pool := range c.Pools {
		if pool.Protocol != "http" && pool.Protocol != "h2c" {
			return fmt.Errorf("pool %q: unsupported protocol %q (expected http or h2c)", name, pool.Protocol)
		}
		if len(pool.Backends) == 0 {
			return fmt.Errorf("pool %q must define at least one backend", name)
		}
		for _, raw := range pool.Backends {
			u, err := url.Parse(raw)
			if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
				return fmt.Errorf("pool %q: invalid backend URL %q", name, raw)
			}
		}
	}
	if len(c.Routes) == 0 {
		return fmt.Errorf("at least one route must be configured")
	}
	seenPrefixes := make(map[string]struct{}, len(c.Routes))
	for _, route := range c.Routes {
		if !strings.HasPrefix(route.Prefix, "/") {
			return fmt.Errorf("route prefix %q must start with '/'", route.Prefix)
		}
		if _, dup := seenPrefixes[route.Prefix]; dup {
			return fmt.Errorf("duplicate route prefix %q", route.Prefix)
		}
		seenPrefixes[route.Prefix] = struct{}{}
		if _, ok := c.Pools[route.Pool]; !ok {
			return fmt.Errorf("route %q references unknown pool %q", route.Prefix, route.Pool)
		}
	}
	return nil
}

// Watch polls path using os.Stat ModTime at interval and invokes onReload whenever
// the file changes and parses cleanly. Invalid configs are logged and ignored.
func Watch(ctx context.Context, path string, interval time.Duration, onReload func(*Config)) {
	if interval <= 0 {
		interval = 2 * time.Second
	}

	var lastMod time.Time
	if info, err := os.Stat(path); err == nil {
		lastMod = info.ModTime()
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			info, err := os.Stat(path)
			if err != nil {
				log.Printf("config watch: stat %q failed: %v", path, err)
				continue
			}
			if info.ModTime().Equal(lastMod) {
				continue
			}
			lastMod = info.ModTime()

			cfg, err := Load(path)
			if err != nil {
				log.Printf("config watch: ignoring invalid config %q: %v", path, err)
				continue
			}
			onReload(cfg)
		}
	}
}
