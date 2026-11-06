package router

import (
	"fmt"
	"net/url"
	"sort"
	"strings"

	"github.com/ethanra700/Reverse-Proxy-Load-Balancer/internal/config"
)

// Route associates a URL path prefix with a backend pool.
type Route struct {
	Prefix   string
	PoolName string
	Protocol string
	Backends []*url.URL
}

// Router performs longest-prefix path matching against configured routes.
type Router struct {
	routes []*Route
}

// New builds a Router from a validated configuration, ordering routes longest-prefix first.
func New(cfg *config.Config) (*Router, error) {
	routes := make([]*Route, 0, len(cfg.Routes))
	for _, rc := range cfg.Routes {
		poolCfg, ok := cfg.Pools[rc.Pool]
		if !ok {
			return nil, fmt.Errorf("route %q references unknown pool %q", rc.Prefix, rc.Pool)
		}
		backends := make([]*url.URL, 0, len(poolCfg.Backends))
		for _, raw := range poolCfg.Backends {
			u, err := url.Parse(raw)
			if err != nil {
				return nil, fmt.Errorf("parse backend %q: %w", raw, err)
			}
			backends = append(backends, u)
		}
		routes = append(routes, &Route{
			Prefix:   rc.Prefix,
			PoolName: rc.Pool,
			Protocol: poolCfg.Protocol,
			Backends: backends,
		})
	}

	sort.Slice(routes, func(i, j int) bool {
		return len(routes[i].Prefix) > len(routes[j].Prefix)
	})

	return &Router{routes: routes}, nil
}

// Match returns the route with the longest matching path prefix for path.
func (r *Router) Match(path string) (*Route, bool) {
	for _, route := range r.routes {
		if matchesPrefix(path, route.Prefix) {
			return route, true
		}
	}
	return nil, false
}

func matchesPrefix(path, prefix string) bool {
	if prefix == "/" {
		return strings.HasPrefix(path, "/")
	}
	if strings.HasSuffix(prefix, "/") {
		return strings.HasPrefix(path, prefix)
	}
	return path == prefix || strings.HasPrefix(path, prefix+"/")
}
