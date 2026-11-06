package main

import (
	"flag"
	"log"
	"net/http"
	"net/url"
	"time"

	"github.com/ethanra700/Reverse-Proxy-Load-Balancer/internal/config"
	"github.com/ethanra700/Reverse-Proxy-Load-Balancer/internal/proxy"
)

func main() {
	configPath := flag.String("config", "config.yaml", "path to YAML configuration file")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	firstPool := cfg.Pools[cfg.Routes[0].Pool]
	target, err := url.Parse(firstPool.Backends[0])
	if err != nil {
		log.Fatalf("invalid backend URL: %v", err)
	}

	handler := proxy.New(target, cfg.Transport)
	srv := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
	}

	log.Printf("l7-reverse-proxy listening on %s -> %s", cfg.ListenAddr, target)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("server error: %v", err)
	}
}
