package router

import (
	"fmt"
	"net/url"
	"sort"
	"strings"
	"sync/atomic"

	"github.com/ethanra700/Reverse-Proxy-Load-Balancer/internal/balancer"
	"github.com/ethanra700/Reverse-Proxy-Load-Balancer/internal/config"
)

// Route associates a URL path prefix with a load-balanced backend pool.
type Route struct {
	Prefix string
	Pool   *balancer.Pool
}

// Router performs longest-prefix path matching against configured routes.
type Router struct {
	routes []*Route
	pools  map[string]*balancer.Pool
}

// New builds a Router from a validated configuration, ordering routes longest-prefix first.
func New(cfg *config.Config) (*Router, error) {
	pools := make(map[string]*balancer.Pool, len(cfg.Pools))
	for name, poolCfg := range cfg.Pools {
		urls := make([]*url.URL, 0, len(poolCfg.Backends))
		for _, raw := range poolCfg.Backends {
			u, err := url.Parse(raw)
			if err != nil {
				return nil, fmt.Errorf("pool %q: parse backend %q: %w", name, raw, err)
			}
			urls = append(urls, u)
		}
		pools[name] = balancer.NewPool(name, poolCfg.Protocol, urls)
	}

	routes := make([]*Route, 0, len(cfg.Routes))
	for _, rc := range cfg.Routes {
		pool, ok := pools[rc.Pool]
		if !ok {
			return nil, fmt.Errorf("route %q references unknown pool %q", rc.Prefix, rc.Pool)
		}
		routes = append(routes, &Route{
			Prefix: rc.Prefix,
			Pool:   pool,
		})
	}

	sort.Slice(routes, func(i, j int) bool {
		return len(routes[i].Prefix) > len(routes[j].Prefix)
	})

	return &Router{
		routes: routes,
		pools:  pools,
	}, nil
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

// AllBackends returns a flat list of every backend across all configured pools.
func (r *Router) AllBackends() []*balancer.Backend {
	var all []*balancer.Backend
	for _, pool := range r.pools {
		all = append(all, pool.Backends()...)
	}
	return all
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

// DynamicRouter wraps a Router inside an atomic.Pointer for lock-free zero-downtime hot reloads.
type DynamicRouter struct {
	current atomic.Pointer[Router]
}

// NewDynamic creates a DynamicRouter initialized with the given Router.
func NewDynamic(initial *Router) *DynamicRouter {
	d := &DynamicRouter{}
	d.current.Store(initial)
	return d
}

// Current returns the active Router snapshot.
func (d *DynamicRouter) Current() *Router {
	return d.current.Load()
}

// Swap atomically replaces the active Router without interrupting in-flight requests.
func (d *DynamicRouter) Swap(next *Router) {
	d.current.Store(next)
}

// Match delegates longest-prefix matching to the currently active Router snapshot.
func (d *DynamicRouter) Match(path string) (*Route, bool) {
	rt := d.current.Load()
	if rt == nil {
		return nil, false
	}
	return rt.Match(path)
}

// AllBackends returns all backends from the currently active Router snapshot.
func (d *DynamicRouter) AllBackends() []*balancer.Backend {
	rt := d.current.Load()
	if rt == nil {
		return nil
	}
	return rt.AllBackends()
}
