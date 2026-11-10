# L7 Reverse Proxy & Load Balancer

A concurrent Layer-7 HTTP/1.1, HTTP/2 (`h2c`), and gRPC reverse proxy and load balancer written in Go.

## Features

- **L7 Reverse Proxy & Upstream Connection Pooling**: Built on Go's `net/http` and `httputil.ReverseProxy` with tuned `http.Transport` connection pools (`MaxIdleConns`, `MaxIdleConnsPerHost`, `IdleConnTimeout`, dial and response header timeouts).
- **HTTP/1.1, Cleartext HTTP/2 (`h2c`), & Native gRPC Forwarding**: Uses Go's `http.Protocols` (`SetUnencryptedHTTP2(true)`) on both frontend and upstream transports. The proxy itself does not import gRPC or parse Protobuf—it forwards gRPC natively over `h2c` while preserving HTTP/2 streams and trailers (`Grpc-Status`), enabling per-RPC load balancing over a single multiplexed client connection.
- **Longest-Prefix Path Routing**: YAML-configured path prefixes (e.g., `/api`, `/echo.EchoService`, `/grpc`, `/`) mapped to named backend pools with segment-boundary matching.
- **Lock-Free Least-Connections Load Balancing**: Tracks active in-flight requests per backend using `atomic.Int64` and routes each request to the healthy backend with the fewest active connections (with atomic round-robin tie-breaking).
- **Active & Passive Health Checks + Automatic Retries**:
  - **Active**: Background `time.Ticker` probes each backend's `/health` endpoint concurrently and updates an `atomic.Bool` health flag.
  - **Passive**: Tracks consecutive upstream failures with `atomic.Int32` and trips a backend unhealthy after 3 consecutive failures until active checks recover it.
  - **Retries**: Automatically retries failed idempotent requests (`GET`, `HEAD`, `OPTIONS`, `PUT`, `DELETE`) and dial/connection-refused failures once against the next healthy backend for seamless failover.
- **Zero-Downtime Hot Config Reloads**: Active router and pool snapshots live inside an `atomic.Pointer[Router]`, updated atomically via `os.Stat` `ModTime` file watching (cross-platform, including Windows) and `SIGHUP` on Linux/Docker.
- **Graceful Shutdown**: Handles `SIGINT`/`SIGTERM` via `server.Shutdown(ctx)` to drain in-flight requests, stop background health-check and watcher goroutines, and close idle upstream connections.
- **Observability**: Exposes Prometheus metrics at `/metrics` and optional `net/http/pprof` profiling on an isolated internal debug server (`-pprof-addr`).

## Architecture

```text
                       +-----------------------------------------+
  HTTP/1.1 / h2c       |             L7 Reverse Proxy            |
  & gRPC Clients  ---> |  /metrics (Prometheus)                  |
                       |  DynamicRouter (atomic.Pointer[Router]) |
                       |    -> Longest-Prefix Route Match        |
                       |    -> Lock-Free Least-Connections Pool  |
                       |    -> Pooled HTTP/1.1 & h2c Transports  |
                       |    -> Retry & Passive Failure Tracker   |
                       +------+---------------------------+------+
                              |                           |
            Active /health    | HTTP/1.1                  | HTTP/2 (h2c)
            Ticker Checks     v                           v
                    +-------------------+       +-------------------+
                    |   http-backends   |       |   grpc-backends   |
                    | http-1, 2, 3      |       | grpc-1, 2, 3      |
                    +-------------------+       +-------------------+
```

## Project Structure

```text
├── cmd/
│   ├── proxy/                  # Proxy entrypoint, signal handling, hot reload, graceful shutdown
│   ├── dummy-http/             # Lightweight HTTP test backend (/health + JSON echo)
│   └── dummy-grpc/             # Minimal echo.proto + gRPC/h2c test backend with /health
├── internal/
│   ├── config/                 # YAML config schema, validation, and ModTime file watcher
│   ├── router/                 # Longest-prefix path router + atomic.Pointer DynamicRouter
│   ├── balancer/               # Lock-free Backend atomics and Least-Connections Pool
│   ├── health/                 # Active ticker health checker and passive failure tracking
│   ├── proxy/                  # httputil.ReverseProxy, HTTP/1.1 & h2c transports, retries
│   └── metrics/                # Prometheus counters, histograms, and gauges
├── config.yaml                 # Local development configuration
├── config.docker.yaml          # Docker Compose configuration
├── Dockerfile                  # Multi-stage build for proxy, dummy-http, and dummy-grpc
└── docker-compose.yml          # Spins up 3 HTTP backends, 3 gRPC backends, and the proxy
```

## Configuration (`config.yaml`)

```yaml
listen_addr: ":8080"

health_check:
  interval: 5s
  timeout: 2s
  path: "/health"
  unhealthy_threshold: 3

transport:
  max_idle_conns: 256
  max_idle_conns_per_host: 64
  idle_conn_timeout: 90s
  dial_timeout: 5s
  response_header_timeout: 10s

pools:
  http-backends:
    backends:
      - "http://localhost:8081"
      - "http://localhost:8082"
      - "http://localhost:8083"
  grpc-backends:
    protocol: "h2c"
    backends:
      - "http://localhost:9001"
      - "http://localhost:9002"
      - "http://localhost:9003"

routes:
  - prefix: "/api"
    pool: "http-backends"
  - prefix: "/echo.EchoService"
    pool: "grpc-backends"
  - prefix: "/grpc"
    pool: "grpc-backends"
  - prefix: "/"
    pool: "http-backends"
```

### Hot Reloading

- **File Watcher**: Edit and save `config.yaml` while the proxy is running; invalid configs are logged and rejected without disrupting live traffic.
- **SIGHUP (Linux / Docker)**: Send `kill -HUP <pid>` (or `docker compose kill -s HUP proxy`) to trigger an immediate atomic reload.

## Prometheus Metrics & Profiling

Scrape `http://localhost:8080/metrics` to view:

| Metric | Type | Labels | Description |
| --- | --- | --- | --- |
| `proxy_requests_total` | Counter | `route`, `backend`, `status_code` | Total HTTP/gRPC requests handled |
| `proxy_request_duration_seconds` | Histogram | `route`, `backend` | End-to-end upstream request latency |
| `proxy_active_connections` | Gauge | `backend` | In-flight connections per backend |
| `proxy_backend_healthy` | Gauge | `backend` | Backend health state (`1` = healthy, `0` = unhealthy) |

To enable Go `pprof` profiling on an internal-only debug server:

```bash
go run ./cmd/proxy -config config.yaml -pprof-addr localhost:6060
```

## Running Locally

### Option 1: Docker Compose (Recommended)

Spins up 3 `dummy-http` backends, 3 `dummy-grpc` backends, and the proxy on port `8080`:

```bash
docker compose up --build
```

Verify load balancing and metrics:

```bash
# HTTP load balancing across http-1, http-2, http-3
for i in $(seq 1 6); do curl -s http://localhost:8080/api/hello; done

# Prometheus metrics
curl -s http://localhost:8080/metrics | grep proxy_

# Simulate live failover (requests continue succeeding without drops)
docker compose stop http-1
curl -s http://localhost:8080/api/hello
```

### Option 2: Direct with Go

In separate terminals, start the test backends and proxy:

```bash
go run ./cmd/dummy-http -id http-1 -addr :8081
go run ./cmd/dummy-http -id http-2 -addr :8082
go run ./cmd/dummy-http -id http-3 -addr :8083

go run ./cmd/dummy-grpc -id grpc-1 -addr :9001
go run ./cmd/dummy-grpc -id grpc-2 -addr :9002
go run ./cmd/dummy-grpc -id grpc-3 -addr :9003

go run ./cmd/proxy -config config.yaml
```

## Testing

Run the full unit, failover, gRPC (`h2c`), hot-reload, and concurrency test suite with the race detector enabled:

```bash
go test -race -v ./...
```
