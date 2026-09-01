# Build stage runs on the native GitHub runner and cross-compiles for each
# requested image platform, avoiding slow QEMU execution of the Go toolchain.
FROM --platform=$BUILDPLATFORM golang:1.26-alpine AS builder

RUN apk add --no-cache git

WORKDIR /build

COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG VERSION=dev
ARG BUILD_TIME=unknown
ARG TARGETOS
ARG TARGETARCH

RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -ldflags "-s -w \
    -X main.version=${VERSION} \
    -X main.buildTime=${BUILD_TIME}" \
    -tags "with_quic with_utls with_wireguard with_clash_api" \
    -o xboard-node ./cmd/xboard-node

FROM caddy:2.10.2-alpine@sha256:4c6e91c6ed0e2fa03efd5b44747b625fec79bc9cd06ac5235a779726618e530d AS caddy

# Runtime stage — sing-box & xray-core are embedded as Go libraries.
# The same immutable image also supplies Caddy for the staging TLS/cover edge.
FROM alpine:3.20

RUN apk add --no-cache ca-certificates tzdata

COPY --from=builder /build/xboard-node /usr/local/bin/xboard-node
COPY --from=caddy /usr/bin/caddy /usr/bin/caddy

RUN mkdir -p /etc/xboard-node

WORKDIR /etc/xboard-node

# Config can be provided via file mount OR environment variables.
# Env var mode (no config file needed):
#   docker run -d --network=host \
#     -e apiHost=https://panel.example.com \
#     -e apiKey=YOUR_TOKEN \
#     -e nodeID=1 \
#     ghcr.io/cedar2025/xboard-node:latest
#
# Supported env vars:
#   apiHost  / API_HOST    → panel URL
#   apiKey   / API_KEY     → server token
#   nodeID   / NODE_ID     → node ID
#   nodeType / NODE_TYPE   → node type (optional)
#   kernel   / KERNEL_TYPE → singbox (default) or xray
#   domain   / DOMAIN      → TLS domain (enables auto_tls)
#   certFile / CERT_FILE   → TLS cert path
#   keyFile  / KEY_FILE    → TLS key path
#   logLevel / LOG_LEVEL   → log level

ENTRYPOINT ["xboard-node"]
CMD ["-c", "/etc/xboard-node/config.yml"]
