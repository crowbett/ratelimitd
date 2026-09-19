---
name: add-rpc
description: "Add an RPC to THIS repo's gRPC services end to end: rpc + messages in proto/ratelimit/v1/ratelimit.proto, `task gen` (never hand-edit gen/), handler method on CheckServer (internal/server/check.go) or AdminServer (internal/server/admin.go), bufconn table test in internal/server, a subcommand in cmd/ratelimitctl, then buf lint / vet / race test / golangci-lint. Use when the user says 'add an RPC', 'new method on the service', 'add a <Verb> rpc', or 'expose <thing> over gRPC'."
argument-hint: "<RpcName> [ratelimit|admin] [\"one-line purpose\"]"
arguments: [rpc, service]
shell: powershell
allowed-tools: PowerShell(go build *) PowerShell(go vet *) PowerShell(go test *) PowerShell(go generate *) PowerShell(go version) PowerShell(buf *) PowerShell(protoc *) PowerShell(golangci-lint run *) PowerShell(Select-String *) Bash(go build *) Bash(go vet *) Bash(go test *) Bash(go generate *) Bash(buf *) Bash(protoc *)
---

# add-rpc

RPC: `$rpc`  Service: `$service` (default ratelimit = RateLimitService; admin = AdminService)  Raw: `$ARGUMENTS`

## Current service surface
!`Select-String -Path proto\ratelimit\v1\ratelimit.proto -Pattern '^\s*rpc\s'; Select-String -Path internal\server\check.go,internal\server\admin.go -Pattern '^func \(s \*(Check|Admin)Server\)'; Select-String -Path internal\server\*_test.go -Pattern '^func Test' -ErrorAction SilentlyContinue; go version`

## Files (do not invent others)
| What | Where |
|---|---|
| `rpc <Name>(<Name>Request) returns (<Name>Response)` + messages | `proto/ratelimit/v1/ratelimit.proto` (STANDARD lint: unique `<Name>Request`/`<Name>Response`, no shared or Empty responses) |
| generated stubs | `gen/ratelimit/v1/*.pb.go` - written ONLY by `task gen` (hookify blocks manual edits) |
| handler `func (s *CheckServer) <Name>(...)` / `(s *AdminServer)` | `internal/server/check.go` / `internal/server/admin.go` |
| proto <-> domain conversion | `internal/server/convert.go`; errors via `server.ToStatus` (`internal/server/errors.go`) |
| table test `Test<Name>` | `internal/server/<file>_test.go` (bufconn, production interceptor chain) |
| CLI subcommand | `cmd/ratelimitctl/` |
| usage line | `README.md` |

## Procedure
1. Read the proto, the target server file and an existing `_test.go` before writing. Signatures in `docs/PLAN.md` are frozen; an RPC that needs a new port means amending the plan first.
2. **Proto**: add the `rpc` and its messages. Field names `snake_case`, comment every field, never reuse or renumber a tag. Run `buf lint` now.
3. **Generate**: `task gen`. `git status --porcelain gen/` must list only `ratelimit.pb.go` / `ratelimit_grpc.pb.go`. Never edit `gen/`.
4. **Handler**: validate first (`codes.InvalidArgument`), call the domain (`limit.Store` / `policy.Registry`), convert with `convert.go`, return errors through `ToStatus`. A denied Check is `CODE_OVER_LIMIT` with an OK status. Logic past ~40 lines moves into `internal/<domain>`.
5. **Test**: bufconn table test, `t.Parallel()`, rows for ok + each validation failure; assert on `status.Code(err)`, never on strings. Metrics assertions go through `admin.Snapshot.Counters()`.
6. **CLI**: a subcommand in `cmd/ratelimitctl` that prints one line per result. **Docs**: a README line.
7. **Verify** (report exactly what ran):
   ```powershell
   buf lint; task gen; git status --porcelain gen/
   go build ./...; go vet ./...; go test -race -count=1 ./...
   golangci-lint run ./...
   ```

## Done when
`buf lint` clean, `gen/` regenerated and committed with the proto, `Test<Name>` passes under `-race`, vet + lint clean, CLI subcommand + README line present. Report: files touched, the proto diff, and the status codes the RPC returns.
