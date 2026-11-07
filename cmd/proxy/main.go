package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"time"

	"github.com/ethanra700/Reverse-Proxy-Load-Balancer/internal/config"
	"github.com/ethanra700/Reverse-Proxy-Load-Balancer/internal/health"
	"github.com/ethanra700/Reverse-Proxy-Load-Balancer/internal/proxy"
	"github.com/ethanra700/Reverse-Proxy-Load-Balancer/internal/router"
)

func main() {
	configPath := flag.String("config", "config.yaml", "path to YAML configuration file")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	rt, err := router.New(cfg)
	if err != nil {
		log.Fatalf("failed to build router: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	checker := health.NewChecker(cfg.HealthCheck, rt.AllBackends)
	go checker.Run(ctx)

	handler := proxy.New(rt, cfg.Transport, cfg.HealthCheck.UnhealthyThreshold)
	srv := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
	}

	log.Printf("l7-reverse-proxy listening on %s (%d routes)", cfg.ListenAddr, len(cfg.Routes))
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("server error: %v", err)
	}
}
