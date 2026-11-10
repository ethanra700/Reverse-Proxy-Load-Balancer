FROM golang:1.26-alpine AS builder

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/proxy ./cmd/proxy && \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/dummy-http ./cmd/dummy-http && \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/dummy-grpc ./cmd/dummy-grpc

FROM alpine:3.21

RUN apk add --no-cache ca-certificates

COPY --from=builder /out/proxy /usr/local/bin/proxy
COPY --from=builder /out/dummy-http /usr/local/bin/dummy-http
COPY --from=builder /out/dummy-grpc /usr/local/bin/dummy-grpc
COPY config.yaml /etc/proxy/config.yaml

EXPOSE 8080

ENTRYPOINT ["/usr/local/bin/proxy"]
CMD ["-config", "/etc/proxy/config.yaml"]
