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
	watchInterval := flag.Duration("watch-interval", 2*time.Second, "config file polling interval for hot reload")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	rt, err := router.New(cfg)
	if err != nil {
		log.Fatalf("failed to build router: %v", err)
	}
	dynRouter := router.NewDynamic(rt)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	checker := health.NewChecker(cfg.HealthCheck, dynRouter.AllBackends)
	go checker.Run(ctx)

	handler := proxy.New(dynRouter, cfg.Transport, cfg.HealthCheck.UnhealthyThreshold)

	applyConfig := func(nextCfg *config.Config) {
		nextRT, err := router.New(nextCfg)
		if err != nil {
			log.Printf("hot reload failed: %v", err)
			return
		}
		dynRouter.Swap(nextRT)
		handler.SetUnhealthyThreshold(nextCfg.HealthCheck.UnhealthyThreshold)
		log.Printf("hot reloaded config: pools=%d routes=%d", len(nextCfg.Pools), len(nextCfg.Routes))
	}
	go config.Watch(ctx, *configPath, *watchInterval, applyConfig)

	srv := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           handler,
		Protocols:         proxy.NewServerProtocols(),
		ReadHeaderTimeout: 10 * time.Second,
	}

	log.Printf("l7-reverse-proxy listening on %s (%d routes)", cfg.ListenAddr, len(cfg.Routes))
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("server error: %v", err)
	}
}
