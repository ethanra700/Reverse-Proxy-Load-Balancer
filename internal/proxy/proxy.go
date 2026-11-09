package proxy

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/ethanra700/Reverse-Proxy-Load-Balancer/internal/balancer"
	"github.com/ethanra700/Reverse-Proxy-Load-Balancer/internal/config"
	"github.com/ethanra700/Reverse-Proxy-Load-Balancer/internal/health"
	"github.com/ethanra700/Reverse-Proxy-Load-Balancer/internal/metrics"
	"github.com/ethanra700/Reverse-Proxy-Load-Balancer/internal/router"
)

const maxAttempts = 2

// NewServerProtocols returns an http.Protocols configuration enabling both
// HTTP/1.1 and cleartext HTTP/2 (h2c) on the frontend http.Server.
func NewServerProtocols() *http.Protocols {
	var p http.Protocols
	p.SetHTTP1(true)
	p.SetUnencryptedHTTP2(true)
	return &p
}

// NewTransport constructs an HTTP/1.1 http.Transport tuned for persistent upstream connection pooling.
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

// NewH2CTransport constructs an http.Transport configured for cleartext HTTP/2 (h2c)
// upstream forwarding, preserving HTTP/2 streams and trailers for gRPC traffic.
func NewH2CTransport(cfg config.TransportConfig) *http.Transport {
	tr := NewTransport(cfg)
	var protocols http.Protocols
	protocols.SetUnencryptedHTTP2(true)
	tr.Protocols = &protocols
	return tr
}

// Proxy routes incoming HTTP/1.1, HTTP/2 (h2c), and gRPC requests across healthy backends.
type Proxy struct {
	router             *router.DynamicRouter
	http1Transport     *http.Transport
	h2cTransport       *http.Transport
	metrics            *metrics.Metrics
	unhealthyThreshold atomic.Int32
}

// New creates a load-balanced L7 reverse proxy with HTTP/1.1 and h2c upstream pools.
func New(r *router.DynamicRouter, transportCfg config.TransportConfig, unhealthyThreshold int32, m *metrics.Metrics) *Proxy {
	if unhealthyThreshold < 1 {
		unhealthyThreshold = 3
	}
	p := &Proxy{
		router:         r,
		http1Transport: NewTransport(transportCfg),
		h2cTransport:   NewH2CTransport(transportCfg),
		metrics:        m,
	}
	p.unhealthyThreshold.Store(unhealthyThreshold)
	return p
}

// SetUnhealthyThreshold updates the passive failure threshold atomically on config reload.
func (p *Proxy) SetUnhealthyThreshold(threshold int32) {
	if threshold < 1 {
		threshold = 3
	}
	p.unhealthyThreshold.Store(threshold)
}

// CloseIdleConnections closes any idle connections held in the upstream transports.
func (p *Proxy) CloseIdleConnections() {
	p.http1Transport.CloseIdleConnections()
	p.h2cTransport.CloseIdleConnections()
}

// ServeHTTP matches the request path, selects the least-loaded healthy backend,
// and retries once on retryable upstream failures.
func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	route, ok := p.router.Match(r.URL.Path)
	if !ok {
		http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
		p.metrics.RecordRequest("unmatched", "none", http.StatusNotFound, time.Since(start))
		return
	}

	tried := make([]*balancer.Backend, 0, maxAttempts)
	for attempt := 0; attempt < maxAttempts; attempt++ {
		backend := route.Pool.Next(tried...)
		if backend == nil {
			status := http.StatusBadGateway
			msg := http.StatusText(http.StatusBadGateway)
			if attempt == 0 {
				status = http.StatusServiceUnavailable
				msg = "no healthy upstream backends"
			}
			http.Error(w, msg, status)
			p.metrics.RecordRequest(route.Prefix, "none", status, time.Since(start))
			return
		}
		tried = append(tried, backend)

		status, shouldRetry := p.forwardAttempt(w, r, backend, route.Pool, attempt, tried)
		if !shouldRetry {
			p.metrics.RecordRequest(route.Prefix, backend.URL.Host, status, time.Since(start))
			return
		}
		if r.GetBody != nil {
			body, err := r.GetBody()
			if err != nil {
				http.Error(w, http.StatusText(http.StatusBadGateway), http.StatusBadGateway)
				p.metrics.RecordRequest(route.Prefix, backend.URL.Host, http.StatusBadGateway, time.Since(start))
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
) (statusCode int, shouldRetry bool) {
	active := backend.IncActive()
	p.metrics.SetActiveConnections(backend.URL.Host, active)
	defer func() {
		remaining := backend.DecActive()
		p.metrics.SetActiveConnections(backend.URL.Host, remaining)
	}()

	transport := p.http1Transport
	var flushInterval time.Duration
	if pool.Protocol == "h2c" || r.ProtoMajor == 2 {
		transport = p.h2cTransport
		flushInterval = -1
	}

	rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
	rp := &httputil.ReverseProxy{
		Transport:     transport,
		FlushInterval: flushInterval,
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetURL(backend.URL)
			pr.SetXForwarded()
		},
		ModifyResponse: func(resp *http.Response) error {
			if resp.StatusCode == http.StatusBadGateway ||
				resp.StatusCode == http.StatusServiceUnavailable ||
				resp.StatusCode == http.StatusGatewayTimeout {
				health.RecordPassiveFailure(backend, p.unhealthyThreshold.Load(), p.metrics)
			} else {
				backend.RecordSuccess()
			}
			return nil
		},
		ErrorHandler: func(rw http.ResponseWriter, req *http.Request, err error) {
			if req.Context().Err() == nil {
				health.RecordPassiveFailure(backend, p.unhealthyThreshold.Load(), p.metrics)
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
	rp.ServeHTTP(rec, r)
	return rec.status, shouldRetry
}

type statusRecorder struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

func (s *statusRecorder) WriteHeader(code int) {
	if !s.wroteHeader {
		s.status = code
		s.wroteHeader = true
	}
	s.ResponseWriter.WriteHeader(code)
}

func (s *statusRecorder) Flush() {
	if flusher, ok := s.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

func (s *statusRecorder) Unwrap() http.ResponseWriter {
	return s.ResponseWriter
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
