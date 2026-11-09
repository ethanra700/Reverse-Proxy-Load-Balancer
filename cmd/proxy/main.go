package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/ethanra700/Reverse-Proxy-Load-Balancer/internal/config"
	"github.com/ethanra700/Reverse-Proxy-Load-Balancer/internal/health"
	"github.com/ethanra700/Reverse-Proxy-Load-Balancer/internal/proxy"
	"github.com/ethanra700/Reverse-Proxy-Load-Balancer/internal/router"
)

func main() {
	configPath := flag.String("config", "config.yaml", "path to YAML configuration file")
	watchInterval := flag.Duration("watch-interval", 2*time.Second, "config file polling interval for hot reload")
	shutdownTimeout := flag.Duration("shutdown-timeout", 15*time.Second, "graceful shutdown drain timeout")
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

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	checker := health.NewChecker(cfg.HealthCheck, dynRouter.AllBackends)
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

	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()
		checker.Run(ctx)
	}()
	go func() {
		defer wg.Done()
		config.Watch(ctx, *configPath, *watchInterval, applyConfig)
	}()
	go func() {
		defer wg.Done()
		reloadCh := make(chan os.Signal, 1)
		notifyReload(reloadCh)
		defer signal.Stop(reloadCh)

		for {
			select {
			case <-ctx.Done():
				return
			case <-reloadCh:
				log.Printf("received SIGHUP, reloading %s", *configPath)
				nextCfg, err := config.Load(*configPath)
				if err != nil {
					log.Printf("SIGHUP reload failed: %v", err)
					continue
				}
				applyConfig(nextCfg)
			}
		}
	}()

	srv := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           handler,
		Protocols:         proxy.NewServerProtocols(),
		ReadHeaderTimeout: 10 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		log.Printf("l7-reverse-proxy listening on %s (%d routes)", cfg.ListenAddr, len(cfg.Routes))
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
		close(errCh)
	}()

	select {
	case err := <-errCh:
		if err != nil {
			log.Fatalf("server error: %v", err)
		}
	case <-ctx.Done():
		log.Printf("shutdown signal received, draining active requests (timeout=%s)...", *shutdownTimeout)
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), *shutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("graceful shutdown error: %v", err)
	}

	stop()
	wg.Wait()
	handler.CloseIdleConnections()
	log.Printf("l7-reverse-proxy stopped cleanly")
}
