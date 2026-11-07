package proxy

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"syscall"
	"time"

	"github.com/ethanra700/Reverse-Proxy-Load-Balancer/internal/balancer"
	"github.com/ethanra700/Reverse-Proxy-Load-Balancer/internal/config"
	"github.com/ethanra700/Reverse-Proxy-Load-Balancer/internal/health"
	"github.com/ethanra700/Reverse-Proxy-Load-Balancer/internal/router"
)

const maxAttempts = 2

// NewTransport constructs an http.Transport tuned for persistent upstream connection pooling.
func NewTransport(cfg config.TransportConfig) *http.Transport {
	dialer := &net.Dialer{
		Timeout:   cfg.DialTimeout,
		KeepAlive: 30 * time.Second,
	}
	return &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           dialer.DialContext,
		MaxIdleConns:          cfg.MaxIdleConns,
		MaxIdleConnsPerHost:   cfg.MaxIdleConnsPerHost,
		IdleConnTimeout:       cfg.IdleConnTimeout,
		ResponseHeaderTimeout: cfg.ResponseHeaderTimeout,
		ExpectContinueTimeout: 1 * time.Second,
	}
}

// Proxy routes incoming requests and load-balances them across healthy backends.
type Proxy struct {
	router             *router.Router
	transport          *http.Transport
	unhealthyThreshold int32
}

// New creates a load-balanced L7 reverse proxy using a pooled upstream transport.
func New(r *router.Router, transportCfg config.TransportConfig, unhealthyThreshold int32) *Proxy {
	if unhealthyThreshold < 1 {
		unhealthyThreshold = 3
	}
	return &Proxy{
		router:             r,
		transport:          NewTransport(transportCfg),
		unhealthyThreshold: unhealthyThreshold,
	}
}

// ServeHTTP matches the request path, selects the least-loaded healthy backend,
// and retries once on retryable upstream failures.
func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	route, ok := p.router.Match(r.URL.Path)
	if !ok {
		http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
		return
	}

	tried := make([]*balancer.Backend, 0, maxAttempts)
	for attempt := 0; attempt < maxAttempts; attempt++ {
		backend := route.Pool.Next(tried...)
		if backend == nil {
			if attempt == 0 {
				http.Error(w, "no healthy upstream backends", http.StatusServiceUnavailable)
			} else {
				http.Error(w, http.StatusText(http.StatusBadGateway), http.StatusBadGateway)
			}
			return
		}
		tried = append(tried, backend)

		if !p.forwardAttempt(w, r, backend, route.Pool, attempt, tried) {
			return
		}
		if r.GetBody != nil {
			body, err := r.GetBody()
			if err != nil {
				http.Error(w, http.StatusText(http.StatusBadGateway), http.StatusBadGateway)
				return
			}
			r.Body = body
		}
	}
}

func (p *Proxy) forwardAttempt(
	w http.ResponseWriter,
	r *http.Request,
	backend *balancer.Backend,
	pool *balancer.Pool,
	attempt int,
	tried []*balancer.Backend,
) (shouldRetry bool) {
	backend.IncActive()
	defer backend.DecActive()

	rp := &httputil.ReverseProxy{
		Transport: p.transport,
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetURL(backend.URL)
			pr.SetXForwarded()
		},
		ModifyResponse: func(resp *http.Response) error {
			if resp.StatusCode == http.StatusBadGateway ||
				resp.StatusCode == http.StatusServiceUnavailable ||
				resp.StatusCode == http.StatusGatewayTimeout {
				health.RecordPassiveFailure(backend, p.unhealthyThreshold)
			} else {
				backend.RecordSuccess()
			}
			return nil
		},
		ErrorHandler: func(rw http.ResponseWriter, req *http.Request, err error) {
			if req.Context().Err() == nil {
				health.RecordPassiveFailure(backend, p.unhealthyThreshold)
			}
			if attempt == 0 && canRetry(req, err) && pool.Next(tried...) != nil {
				log.Printf("retrying [%s %s] after upstream error on %s: %v", req.Method, req.URL.Path, backend.URL.Host, err)
				shouldRetry = true
				return
			}
			log.Printf("proxy error [%s %s -> %s]: %v", req.Method, req.URL.Path, backend.URL.Host, err)
			http.Error(rw, http.StatusText(http.StatusBadGateway), http.StatusBadGateway)
		},
	}
	rp.ServeHTTP(w, r)
	return shouldRetry
}

func canRetry(r *http.Request, err error) bool {
	if r.Context().Err() != nil {
		return false
	}
	if !isIdempotent(r.Method) && !isDialError(err) {
		return false
	}
	if r.Body != nil && r.Body != http.NoBody && r.ContentLength != 0 && r.GetBody == nil {
		return false
	}
	return true
}

func isIdempotent(method string) bool {
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodOptions, http.MethodPut, http.MethodDelete:
		return true
	default:
		return false
	}
}

func isDialError(err error) bool {
	if errors.Is(err, syscall.ECONNREFUSED) {
		return true
	}
	var opErr *net.OpError
	return errors.As(err, &opErr) && opErr.Op == "dial"
}
