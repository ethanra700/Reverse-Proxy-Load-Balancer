package main

import (
	"flag"
	"log"
)

func main() {
	configPath := flag.String("config", "config.yaml", "path to YAML configuration file")
	flag.Parse()

	log.Printf("l7-reverse-proxy starting with config %s", *configPath)
}
