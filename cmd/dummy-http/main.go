package main

import (
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"os"
	"time"
)

func main() {
	id := flag.String("id", envOrDefault("BACKEND_ID", "http-1"), "backend instance identifier")
	addr := flag.String("addr", envOrDefault("LISTEN_ADDR", ":8081"), "listen address")
	flag.Parse()

	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Backend-ID", *id)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Backend-ID", *id)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"backend_id": *id,
			"method":     r.Method,
			"path":       r.URL.Path,
			"proto":      r.Proto,
		})
	})

	srv := &http.Server{
		Addr:              *addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	log.Printf("dummy-http [%s] listening on %s", *id, *addr)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("dummy-http [%s] error: %v", *id, err)
	}
}

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
