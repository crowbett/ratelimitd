# ratelimitd

Minimal, production-shaped gRPC service in Go. Protos are managed with
[buf](https://buf.build) (v2 config, local plugins, no network), generated code is
committed, and the interceptors are hand-written (`log/slog`, no middleware framework).

```
proto/echo/v1/echo.proto      EchoService: unary Echo + server-streaming EchoStream
gen/echo/v1/*.pb.go           protoc-gen-go + protoc-gen-go-grpc output (COMMITTED; `task gen` refreshes)
cmd/server/main.go            grpc.Server: interceptor chain, health, reflection, keepalive, graceful stop
cmd/client/main.go            smoke-test client: health check + Echo + EchoStream
internal/interceptor/         request-id (x-request-id metadata), logging, panic recovery (unary + stream)
internal/server/              EchoService implementation + bufconn table-driven tests
buf.yaml / buf.gen.yaml       buf v2: STANDARD lint, FILE breaking, local plugins, paths=source_relative
Taskfile.yml                  task gen|lint|lint:breaking|test|run|client|build|docker:build
.golangci.yml                 golangci-lint v2 (gen/ excluded)
Dockerfile                    multi-stage -> distroless static, non-root, EXPOSE 50051
.github/workflows/ci.yml      buf lint + breaking-vs-main + generate-is-clean, go test (race, linux+windows), lint, vuln, docker
```

## Quick start

```powershell
go run ./cmd/server                          # listens on :50051, text logs: $env:LOG_FORMAT="text"
go run ./cmd/client -msg hello -count 3      # in another shell
```

Expected client output:

```
health: SERVING
echo: "hello" (request_id=cfaf968049c28e155eabcf6f)
stream[1]: "hello"
stream[2]: "hello"
stream[3]: "hello"
```

With [Task](https://taskfile.dev):

```powershell
task gen            # buf generate  (proto/ -> gen/)
task lint           # buf lint + golangci-lint
task lint:breaking  # buf breaking --against ".git#branch=main"
task test           # go test -race (needs a C compiler on Windows: WinLibs gcc is fine)
task run            # server on :50051
task client         # smoke test against localhost:50051  (task client -- -msg hi -count 5)
task build          # bin/server(.exe)
```

## Calling it with grpcurl

Reflection is enabled, so no `.proto` files are needed on the client side.
`grpcurl` is not installed by default:

```powershell
go install github.com/fullstorydev/grpcurl/cmd/grpcurl@latest
```

```powershell
grpcurl -plaintext localhost:50051 list
grpcurl -plaintext localhost:50051 describe echo.v1.EchoService
grpcurl -plaintext -d '{"message":"hi"}' localhost:50051 echo.v1.EchoService/Echo
grpcurl -plaintext -d '{"message":"hi","count":3}' localhost:50051 echo.v1.EchoService/EchoStream
grpcurl -plaintext -H 'x-request-id: my-trace-1' -d '{"message":"hi"}' localhost:50051 echo.v1.EchoService/Echo
grpcurl -plaintext -d '{"service":"echo.v1.EchoService"}' localhost:50051 grpc.health.v1.Health/Check
```

(On Windows PowerShell 5.1 use double quotes around the JSON and escape inner quotes:
`-d "{\"message\":\"hi\"}"`.)

## Configuration

| Var                | Default  | Notes                                     |
| ------------------ | -------- | ----------------------------------------- |
| `GRPC_ADDR`        | `:50051` | listen address (client: dial address)     |
| `LOG_LEVEL`        | `info`   | debug / info / warn / error               |
| `LOG_FORMAT`       | `json`   | json / text                               |
| `SHUTDOWN_TIMEOUT` | `10s`    | GracefulStop window before forced Stop    |

## Interceptor chain

Order in `grpc.ChainUnaryInterceptor` / `ChainStreamInterceptor` is outermost first:

1. `Recover` - turns a handler panic into `codes.Internal` and logs the stack.
2. `RequestID` - reads `x-request-id` from incoming metadata (or generates one), stores it
   in the context (`interceptor.RequestIDFrom(ctx)`) and echoes it as a response header.
3. `Logging` - one `slog` line per RPC with method, kind, code, duration, request_id.

There are no third-party middleware dependencies; add auth/rate-limit interceptors in
`internal/interceptor/` following the same shape.

## Health and shutdown

- `grpc.health.v1.Health/Check` is registered; `echo.v1.EchoService` reports `SERVING`.
  Kubernetes: `livenessProbe: { grpc: { port: 50051 } }`.
- On SIGINT/SIGTERM the server flips health to `NOT_SERVING`, then `GracefulStop()`s with a
  `SHUTDOWN_TIMEOUT` deadline before forcing `Stop()`.

## Adding an RPC

1. Edit `proto/echo/v1/echo.proto` (or add `proto/<svc>/v1/<svc>.proto`).
2. `task gen` (or `buf generate`), commit the `gen/` changes.
3. Implement the method on `EchoServer` in `internal/server/echo.go`.
4. Add a table case in `internal/server/echo_test.go` (bufconn, real interceptor chain).
5. `task lint` - `buf lint` enforces STANDARD naming rules; CI runs `buf breaking` against
   `main` so wire-incompatible edits fail the PR.

## Generated code policy

`gen/` is committed so `go build ./...` and the Dockerfile work without buf or protoc. CI
re-runs `buf generate` and fails if the result differs. The plugin versions are pinned in
`.github/workflows/ci.yml` and stamped at the top of each `*.pb.go`; bump both when you
upgrade locally.

## Tooling

```powershell
go install github.com/bufbuild/buf/cmd/buf@latest
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
go install github.com/fullstorydev/grpcurl/cmd/grpcurl@latest
go install github.com/go-task/task/v3/cmd/task@latest
go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest
```
