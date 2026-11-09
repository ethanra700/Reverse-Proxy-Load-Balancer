package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/ethanra700/Reverse-Proxy-Load-Balancer/internal/config"
	"github.com/ethanra700/Reverse-Proxy-Load-Balancer/internal/health"
	"github.com/ethanra700/Reverse-Proxy-Load-Balancer/internal/metrics"
	"github.com/ethanra700/Reverse-Proxy-Load-Balancer/internal/proxy"
	"github.com/ethanra700/Reverse-Proxy-Load-Balancer/internal/router"
)

func main() {
	configPath := flag.String("config", "config.yaml", "path to YAML configuration file")
	watchInterval := flag.Duration("watch-interval", 2*time.Second, "config file polling interval for hot reload")
	shutdownTimeout := flag.Duration("shutdown-timeout", 15*time.Second, "graceful shutdown drain timeout")
	pprofAddr := flag.String("pprof-addr", "", "optional internal address for pprof debug endpoints (e.g. localhost:6060)")
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
	promMetrics := metrics.New()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	checker := health.NewChecker(cfg.HealthCheck, dynRouter.AllBackends, promMetrics)
	proxyHandler := proxy.New(dynRouter, cfg.Transport, cfg.HealthCheck.UnhealthyThreshold, promMetrics)

	applyConfig := func(nextCfg *config.Config) {
		nextRT, err := router.New(nextCfg)
		if err != nil {
			log.Printf("hot reload failed: %v", err)
			return
		}
		dynRouter.Swap(nextRT)
		proxyHandler.SetUnhealthyThreshold(nextCfg.HealthCheck.UnhealthyThreshold)
		checker.SyncMetrics()
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

	metricsHandler := promMetrics.Handler()
	topHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/metrics" {
			metricsHandler.ServeHTTP(w, r)
			return
		}
		proxyHandler.ServeHTTP(w, r)
	})

	srv := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           topHandler,
		Protocols:         proxy.NewServerProtocols(),
		ReadHeaderTimeout: 10 * time.Second,
	}

	var debugSrv *http.Server
	if *pprofAddr != "" {
		debugMux := http.NewServeMux()
		debugMux.HandleFunc("/debug/pprof/", pprof.Index)
		debugMux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		debugMux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		debugMux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		debugMux.HandleFunc("/debug/pprof/trace", pprof.Trace)
		debugSrv = &http.Server{
			Addr:              *pprofAddr,
			Handler:           debugMux,
			ReadHeaderTimeout: 5 * time.Second,
		}
		go func() {
			log.Printf("pprof debug server listening on %s", *pprofAddr)
			if err := debugSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Printf("pprof server error: %v", err)
			}
		}()
	}

	errCh := make(chan error, 1)
	go func() {
		log.Printf("l7-reverse-proxy listening on %s (%d routes, /metrics enabled)", cfg.ListenAddr, len(cfg.Routes))
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
	if debugSrv != nil {
		_ = debugSrv.Shutdown(shutdownCtx)
	}

	stop()
	wg.Wait()
	proxyHandler.CloseIdleConnections()
	log.Printf("l7-reverse-proxy stopped cleanly")
}
