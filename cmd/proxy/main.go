package main

import (
	"flag"
	"log"

	"github.com/ethanra700/Reverse-Proxy-Load-Balancer/internal/config"
)

func main() {
	configPath := flag.String("config", "config.yaml", "path to YAML configuration file")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}
	log.Printf("loaded config: listen=%s pools=%d routes=%d", cfg.ListenAddr, len(cfg.Pools), len(cfg.Routes))
}
