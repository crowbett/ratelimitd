---
name: add-rpc
description: "Add an RPC to THIS repo's gRPC service end to end: rpc + messages in proto/echo/v1/echo.proto, `task gen` (never hand-edit gen/), handler method on EchoServer in internal/server/echo.go, bufconn table test in echo_test.go, a flag + call in cmd/client/main.go, then buf lint / vet / race test / golangci-lint. Use when the user says 'add an RPC', 'new method on the service', 'add a <Verb> rpc', or 'expose <thing> over gRPC'."
argument-hint: "<RpcName> [unary|server-stream] [\"one-line purpose\"]"
arguments: [rpc, kind]
shell: powershell
allowed-tools: PowerShell(go build *) PowerShell(go vet *) PowerShell(go test *) PowerShell(go generate *) PowerShell(go version) PowerShell(buf *) PowerShell(protoc *) PowerShell(golangci-lint run *) PowerShell(Select-String *) Bash(go build *) Bash(go vet *) Bash(go test *) Bash(go generate *) Bash(buf *) Bash(protoc *)
---

# add-rpc

RPC: `$rpc`  Kind: `$kind` (default unary)  Raw: `$ARGUMENTS`

## Current service surface
!`Select-String -Path proto\echo\v1\echo.proto -Pattern '^\s*rpc\s'; Select-String -Path internal\server\echo.go -Pattern '^func \(s \*EchoServer\)'; Select-String -Path internal\server\echo_test.go -Pattern '^func Test'; Select-String -Path cmd\client\main.go -Pattern 'flag\.\w+\("'; go version`

## Files (do not invent others)
| What | Where |
|---|---|
| `rpc <Name>(<Name>Request) returns (<Name>Response)` + messages | `proto/echo/v1/echo.proto` |
| generated stubs | `gen/echo/v1/*.pb.go` - written ONLY by `task gen` (hookify blocks manual edits) |
| handler `func (s *EchoServer) <Name>(...)` | `internal/server/echo.go` |
| table test `Test<Name>` | `internal/server/echo_test.go` (uses `newTestClient`) |
| client flag + call | `cmd/client/main.go` |
| usage line | `README.md` "Quick start" / grpcurl block |

## Procedure
1. Read `proto/echo/v1/echo.proto`, `internal/server/echo.go` (see `Echo` / `EchoStream`) and `echo_test.go` (`newTestClient`) before writing.
2. **Proto**: add the `rpc` to `service EchoService` and its `<Name>Request` / `<Name>Response` messages.
   Field names `snake_case`, comment every field, never reuse or renumber an existing tag. Server-stream:
   `returns (stream <Name>Response)`. Run `buf lint` now; STANDARD rules reject `Get`/`List` without matching types, etc.
3. **Generate**: `task gen` (`buf generate`). Then `git status --porcelain gen/` must list only `echo.pb.go` / `echo_grpc.pb.go`.
   If build errors point into `gen/`, fix the proto or the plugin pins in `.github/workflows/ci.yml`; never edit `gen/`.
4. **Handler** in `internal/server/echo.go`, same shape as `Echo`:
   ```go
   func (s *EchoServer) <Name>(ctx context.Context, req *echov1.<Name>Request) (*echov1.<Name>Response, error) {
       if req.Get<Field>() == "" {
           return nil, status.Error(codes.InvalidArgument, "<field> must not be empty")
       }
       return &echov1.<Name>Response{...}, nil
   }
   ```
   Rules: validate first (`InvalidArgument`), errors are `status.Error(codes.X, ...)`, request id via
   `interceptor.RequestIDFrom(ctx)`. Streams: bound the count, check `stream.Context().Err()` before every `Send`,
   wrap send errors with `%w`. Logic that grows past ~40 lines moves to `internal/<domain>` and the handler calls it.
5. **Test**: add `Test<Name>` to `echo_test.go` using `newTestClient(t)`, `t.Parallel()` on the test and each subtest,
   rows for: ok, each validation failure (`wantCode: codes.InvalidArgument`), and for streams a cancel case
   (see `TestEchoStreamCancel`). Assert on `status.Code(err)`, never on error strings.
6. **Client**: add a `flag.*` for the new input in `cmd/client/main.go` and a call in `run` that prints one line per result.
7. **Docs**: add a `grpcurl ... echo.v1.EchoService/<Name>` line to README.
8. **Verify** (report exactly what ran):
   ```powershell
   buf lint; task gen; git status --porcelain gen/
   go build ./...; go vet ./...; go test -race -count=1 ./...
   golangci-lint run ./...
   ```
   Then, if a server is running: `go run ./cmd/client -h` shows the new flag.

## Done when
`buf lint` clean, `gen/` regenerated and committed alongside the proto, `Test<Name>` passes under `-race`, vet + lint clean,
client flag + README line present. Report: files touched, the proto diff (rpc + messages), and the status codes the RPC returns.
