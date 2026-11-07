package balancer

import (
	"math"
	"net/url"
	"sync/atomic"
)

// Backend represents a single upstream server tracked with lock-free atomic state.
type Backend struct {
	URL                 *url.URL
	active              atomic.Int64
	healthy             atomic.Bool
	consecutiveFailures atomic.Int32
}

// NewBackend creates a Backend initialized as healthy with zero active connections.
func NewBackend(u *url.URL) *Backend {
	b := &Backend{URL: u}
	b.healthy.Store(true)
	return b
}

// ActiveConnections returns the current number of in-flight requests to this backend.
func (b *Backend) ActiveConnections() int64 {
	return b.active.Load()
}

// IncActive increments the in-flight request counter and returns the new count.
func (b *Backend) IncActive() int64 {
	return b.active.Add(1)
}

// DecActive decrements the in-flight request counter and returns the new count.
func (b *Backend) DecActive() int64 {
	return b.active.Add(-1)
}

// IsHealthy reports whether the backend is currently eligible for traffic.
func (b *Backend) IsHealthy() bool {
	return b.healthy.Load()
}

// SetHealthy updates the backend health status atomically.
func (b *Backend) SetHealthy(ok bool) {
	b.healthy.Store(ok)
	if ok {
		b.consecutiveFailures.Store(0)
	}
}

// ConsecutiveFailures returns the current passive consecutive failure count.
func (b *Backend) ConsecutiveFailures() int32 {
	return b.consecutiveFailures.Load()
}

// RecordSuccess resets the passive consecutive failure counter.
func (b *Backend) RecordSuccess() {
	b.consecutiveFailures.Store(0)
}

// RecordFailure increments consecutive failures and marks the backend unhealthy
// once threshold is reached. Returns true if the backend was tripped unhealthy.
func (b *Backend) RecordFailure(threshold int32) bool {
	if threshold < 1 {
		threshold = 1
	}
	failures := b.consecutiveFailures.Add(1)
	if failures >= threshold {
		b.healthy.Store(false)
		return true
	}
	return false
}

// String returns the backend's URL string.
func (b *Backend) String() string {
	return b.URL.String()
}

// Pool manages a set of backends using lock-free Least-Connections selection.
type Pool struct {
	Name     string
	Protocol string
	backends []*Backend
	rr       atomic.Uint64
}

// NewPool constructs a backend pool from parsed upstream URLs.
func NewPool(name, protocol string, urls []*url.URL) *Pool {
	backends := make([]*Backend, len(urls))
	for i, u := range urls {
		backends[i] = NewBackend(u)
	}
	return &Pool{
		Name:     name,
		Protocol: protocol,
		backends: backends,
	}
}

// Backends returns all backends registered in this pool.
func (p *Pool) Backends() []*Backend {
	return p.backends
}

// Next selects the healthy backend with the fewest active connections.
// Any backend listed in exclude is skipped. Returns nil if no healthy backend is available.
func (p *Pool) Next(exclude ...*Backend) *Backend {
	n := len(p.backends)
	if n == 0 {
		return nil
	}

	// Rotate the starting scan index so equal connection counts distribute evenly.
	start := int((p.rr.Add(1) - 1) % uint64(n))

	var best *Backend
	bestConns := int64(math.MaxInt64)

	for i := 0; i < n; i++ {
		cand := p.backends[(start+i)%n]
		if !cand.IsHealthy() || isExcluded(cand, exclude) {
			continue
		}
		conns := cand.ActiveConnections()
		if conns < bestConns {
			best = cand
			bestConns = conns
			if conns == 0 {
				return best
			}
		}
	}
	return best
}

func isExcluded(target *Backend, exclude []*Backend) bool {
	for _, b := range exclude {
		if b == target {
			return true
		}
	}
	return false
}
