package proxy

import (
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"time"

	"github.com/ethanra700/Reverse-Proxy-Load-Balancer/internal/config"
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

// Proxy forwards incoming HTTP requests to an upstream backend using a pooled transport.
type Proxy struct {
	transport *http.Transport
	target    *url.URL
}

// New creates a core L7 reverse proxy targeting the given upstream URL.
func New(target *url.URL, transportCfg config.TransportConfig) *Proxy {
	return &Proxy{
		transport: NewTransport(transportCfg),
		target:    target,
	}
}

// ServeHTTP forwards the incoming request to the upstream target.
func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	rp := &httputil.ReverseProxy{
		Transport: p.transport,
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetURL(p.target)
			pr.SetXForwarded()
		},
		ErrorHandler: func(rw http.ResponseWriter, req *http.Request, err error) {
			log.Printf("proxy error [%s %s -> %s]: %v", req.Method, req.URL.Path, p.target.Host, err)
			http.Error(rw, http.StatusText(http.StatusBadGateway), http.StatusBadGateway)
		},
	}
	rp.ServeHTTP(w, r)
}
