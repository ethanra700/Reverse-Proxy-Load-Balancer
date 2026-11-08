package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	echopb "github.com/ethanra700/Reverse-Proxy-Load-Balancer/cmd/dummy-grpc/proto"
	"google.golang.org/grpc"
)

type echoServer struct {
	echopb.UnimplementedEchoServiceServer
	id string
}

func (s *echoServer) Ping(_ context.Context, req *echopb.PingRequest) (*echopb.PongResponse, error) {
	return &echopb.PongResponse{
		BackendId: s.id,
		Message:   req.GetMessage(),
	}, nil
}

func main() {
	id := flag.String("id", envOrDefault("BACKEND_ID", "grpc-1"), "backend instance identifier")
	addr := flag.String("addr", envOrDefault("LISTEN_ADDR", ":9001"), "listen address")
	flag.Parse()

	grpcServer := grpc.NewServer()
	echopb.RegisterEchoServiceServer(grpcServer, &echoServer{id: *id})

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.ProtoMajor == 2 && strings.HasPrefix(r.Header.Get("Content-Type"), "application/grpc") {
			grpcServer.ServeHTTP(w, r)
			return
		}
		if r.URL.Path == "/health" {
			w.Header().Set("X-Backend-ID", *id)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok\n"))
			return
		}
		http.NotFound(w, r)
	})

	var protocols http.Protocols
	protocols.SetHTTP1(true)
	protocols.SetUnencryptedHTTP2(true)

	srv := &http.Server{
		Addr:              *addr,
		Handler:           handler,
		Protocols:         &protocols,
		ReadHeaderTimeout: 10 * time.Second,
	}

	log.Printf("dummy-grpc [%s] listening (h2c + /health) on %s", *id, *addr)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("dummy-grpc [%s] error: %v", *id, err)
	}
}

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
