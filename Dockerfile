# syntax=docker/dockerfile:1.7

# ---------- build ----------
FROM golang:1.27-alpine AS builder
WORKDIR /src

ARG VERSION=dev
ENV CGO_ENABLED=0 GOFLAGS=-buildvcs=false

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

# gen/ is committed, so no buf/protoc is needed inside the image build.
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go build -trimpath -ldflags="-s -w -X main.version=${VERSION}" -o /out/server ./cmd/server

# ---------- runtime ----------
# distroless/static: no shell, no package manager, runs as uid 65532 (nonroot).
# NOTE: no HEALTHCHECK -- distroless has no shell. Use a gRPC probe against
# grpc.health.v1.Health/Check (k8s: `grpc: { port: 50051 }`) or grpc_health_probe.
FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=builder /out/server /server

ENV GRPC_ADDR=:50051 LOG_FORMAT=json
EXPOSE 50051
USER nonroot:nonroot

ENTRYPOINT ["/server"]
