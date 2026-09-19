# syntax=docker/dockerfile:1.7

# ---------- build ----------
FROM golang:1.27-alpine AS builder
WORKDIR /src

ARG VERSION=dev
ENV CGO_ENABLED=0 GOFLAGS=-buildvcs=false

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

# gen/ is committed and the Lua scripts are embedded, so no buf/protoc is
# needed inside the image build.
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go build -trimpath -ldflags="-s -w -X main.version=${VERSION}" -o /out/ratelimitd ./cmd/ratelimitd

# ---------- runtime ----------
# distroless/static: no shell, no package manager, runs as uid 65532 (nonroot).
# NOTE: no HEALTHCHECK -- distroless has no shell. Use a gRPC probe against
# grpc.health.v1.Health/Check (k8s: `grpc: { port: 50053 }`) or GET /readyz on
# the admin port.
FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=builder /out/ratelimitd /ratelimitd

ENV GRPC_ADDR=:50053 HTTP_ADDR=:8093 ADMIN_ADDR=:9130 LOG_FORMAT=json
EXPOSE 50053 8093 9130
USER nonroot:nonroot

ENTRYPOINT ["/ratelimitd"]
