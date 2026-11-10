package balancer

import (
	"net/url"
	"sync"
	"testing"
)

func mustParseURLs(t *testing.T, raw ...string) []*url.URL {
	t.Helper()
	urls := make([]*url.URL, len(raw))
	for i, s := range raw {
		u, err := url.Parse(s)
		if err != nil {
			t.Fatalf("url.Parse(%q): %v", s, err)
		}
		urls[i] = u
	}
	return urls
}

func TestPoolLeastConnectionsSelection(t *testing.T) {
	pool := NewPool("http", "http", mustParseURLs(t,
		"http://b1:8081",
		"http://b2:8082",
		"http://b3:8083",
	))
	backends := pool.Backends()

	// Make b1 and b3 busy; b2 has the fewest active connections.
	backends[0].IncActive()
	backends[0].IncActive()
	backends[1].IncActive()
	backends[2].IncActive()
	backends[2].IncActive()
	backends[2].IncActive()

	got := pool.Next()
	if got != backends[1] {
		t.Fatalf("expected b2 (1 active), got %s (%d active)", got, got.ActiveConnections())
	}

	// Excluding b2 should select b1 (2 active) over b3 (3 active).
	got = pool.Next(backends[1])
	if got != backends[0] {
		t.Fatalf("expected b1 (2 active), got %s (%d active)", got, got.ActiveConnections())
	}

	// Marking all unhealthy yields nil.
	for _, b := range backends {
		b.SetHealthy(false)
	}
	if got := pool.Next(); got != nil {
		t.Fatalf("expected nil when all backends unhealthy, got %s", got)
	}
}

func TestPoolTieBreakDistributesEvenly(t *testing.T) {
	pool := NewPool("http", "http", mustParseURLs(t,
		"http://b1:8081",
		"http://b2:8082",
		"http://b3:8083",
	))

	seen := make(map[string]int)
	for i := 0; i < 9; i++ {
		b := pool.Next()
		seen[b.URL.Host]++
	}
	for _, host := range []string{"b1:8081", "b2:8082", "b3:8083"} {
		if seen[host] != 3 {
			t.Fatalf("expected 3 hits for %s, got %d (all=%v)", host, seen[host], seen)
		}
	}
}

func TestBackendPassiveFailureThresholdAndRecovery(t *testing.T) {
	u, _ := url.Parse("http://b1:8081")
	b := NewBackend(u)

	if tripped := b.RecordFailure(3); tripped || !b.IsHealthy() {
		t.Fatal("1st failure should not trip backend unhealthy")
	}
	if tripped := b.RecordFailure(3); tripped || !b.IsHealthy() {
		t.Fatal("2nd failure should not trip backend unhealthy")
	}
	// A success resets consecutive failures.
	b.RecordSuccess()
	if b.ConsecutiveFailures() != 0 {
		t.Fatalf("expected 0 consecutive failures after success, got %d", b.ConsecutiveFailures())
	}

	// Three consecutive failures trip unhealthy.
	b.RecordFailure(3)
	b.RecordFailure(3)
	if tripped := b.RecordFailure(3); !tripped || b.IsHealthy() {
		t.Fatal("3rd consecutive failure should trip backend unhealthy")
	}

	// Active check recovery resets healthy flag and consecutive failures.
	b.SetHealthy(true)
	if !b.IsHealthy() || b.ConsecutiveFailures() != 0 {
		t.Fatal("expected backend healthy with 0 failures after recovery")
	}
}

func TestPoolConcurrentAccessRaceFree(t *testing.T) {
	pool := NewPool("http", "http", mustParseURLs(t,
		"http://b1:8081",
		"http://b2:8082",
		"http://b3:8083",
	))

	var wg sync.WaitGroup
	for g := 0; g < 32; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				if b := pool.Next(); b != nil {
					b.IncActive()
					if i%17 == 0 {
						b.RecordFailure(3)
					} else {
						b.RecordSuccess()
					}
					b.DecActive()
				}
				if i%25 == 0 {
					pool.Backends()[id%3].SetHealthy(true)
				}
			}
		}(g)
	}
	wg.Wait()

	for _, b := range pool.Backends() {
		if b.ActiveConnections() != 0 {
			t.Fatalf("expected 0 active connections on %s, got %d", b, b.ActiveConnections())
		}
	}
}
