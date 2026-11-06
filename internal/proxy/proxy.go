package proxy

import (
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"time"

	"github.com/ethanra700/Reverse-Proxy-Load-Balancer/internal/config"
	"github.com/ethanra700/Reverse-Proxy-Load-Balancer/internal/router"
)

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

// Proxy routes incoming requests via path-prefix matching and forwards them upstream.
type Proxy struct {
	router    *router.Router
	transport *http.Transport
}

// New creates a path-routed L7 reverse proxy using a pooled upstream transport.
func New(r *router.Router, transportCfg config.TransportConfig) *Proxy {
	return &Proxy{
		router:    r,
		transport: NewTransport(transportCfg),
	}
}

// ServeHTTP matches the request path to a route and forwards it to the matched pool.
func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	route, ok := p.router.Match(r.URL.Path)
	if !ok || len(route.Backends) == 0 {
		http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
		return
	}

	target := route.Backends[0]
	rp := &httputil.ReverseProxy{
		Transport: p.transport,
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetURL(target)
			pr.SetXForwarded()
		},
		ErrorHandler: func(rw http.ResponseWriter, req *http.Request, err error) {
			log.Printf("proxy error [%s %s -> %s]: %v", req.Method, req.URL.Path, target.Host, err)
			http.Error(rw, http.StatusText(http.StatusBadGateway), http.StatusBadGateway)
		},
	}
	rp.ServeHTTP(w, r)
}
