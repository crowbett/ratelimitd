# ratelimitd

Distributed rate limiter: GCRA / token bucket / sliding window, per-tenant policies, Redis Lua atomic path with a breaker-guarded local fallback, gRPC + HTTP JSON data plane, admin API, Prometheus text + OTel, and a public client library (`pkg/ratelimit`). Module `github.com/crowbett/ratelimitd`, Go 1.27. Global conventions from `~/.claude/CLAUDE.md` apply.

## Where things are
- `docs/PLAN.md`: design record (ADR-0001); ports and signatures are frozen there, read before touching one.
- `docs/PACKAGES.md`: what every package does. `docs/CONVENTIONS.md`: full text of the rules below.

## Invariants
- Ports are frozen: redis 6380, gRPC :50053, HTTP :8093, admin gRPC 127.0.0.1:50054 (AdminService only, never on the data plane), admin 127.0.0.1:9130.
- Integer microseconds everywhere that crosses into an algorithm or Lua script (`int64` unix us); never nanoseconds, never floats.
- The caller's clock is authoritative: `now` is a parameter of every `Step`/`Apply`/Lua call; nothing reads `time.Now` or Redis `TIME` inside a decision. Skew refused at startup above `MAX_CLOCK_SKEW`.
- Single pass, buffered writes: a subject's keys share hash tag `{t=<tenant>|s=<subject>}`, go in ONE script call, and writes happen only if every key allowed. A denial never mutates state. The Go `Step` is the reference; `TestDifferential`, `TestDifferentialMulti`, `TestDifferentialLease` must agree on both FCALL and EVALSHA paths. Lua `eval` stays pure.
- Shared policies (`shared: true`) never join the subject's call; `internal/store/lease` takes units from a leased per-node pool first and refunds on subject denial. Never over-admits.
- Signatures in `docs/PLAN.md` "Interfaces and key types" are the contract; change the plan first and every caller in the same commit.
- No goroutine per request or key; only watcher, prober, sweeper, skew sampler (stopped in teardown stage 2).
- A denied Check is `CODE_OVER_LIMIT` with OK gRPC status; only `pkg/ratelimit` maps it to `ErrDenied`/429. Errors leave gRPC handlers via `server.ToStatus`.
- Metrics come from one `admin.Snapshot`; `tenant`/`policy` are labels, the subject never is.
- Schema first: edit `proto/` then `task gen`; never edit `gen/` (committed, generated).
- Redis-needing tests go behind `//go:build integration` and `TEST_REDIS_ADDR`.
- go-redis needs `MaxRetries: -1` AND `DialerRetries: 1`; the breaker owns retries.
- Every number in README.md comes from `results/bench/<ts>.md` or a pasted `-benchmem` line; never type one by hand.
- Windows: no SIGHUP (reload = mtime polling + admin API); stop signal is Ctrl-Break; the three `teardown N/3` lines appear in order and exit is 0.

## Commands (Taskfile.yml)
- `task redis:up` | `task run` | `task build` | `task client -- health|check|policy|reset|stats` | `task bench` | `task bench:matrix` | `task bench:micro`
- `task gen` | `task lint` | `task lint:breaking` | `task fmt`
- `task test` (`-race`, needs gcc) | `task test:fast` | `task test:integration` (`TEST_REDIS_ADDR=127.0.0.1:6380`) | `task check`

## Definition of done
`buf lint`, `buf generate` leaving `git status --porcelain gen/` empty, `go build ./...`, `go vet ./...`, `golangci-lint run ./...` (0 issues), `go test -race -count=1 ./...` (or `task test:fast`, say which), `task test:integration` with the stack up (differential Lua test, 8-goroutine one-key bound, testproxy partition test), and the bench: `ratelimitctl bench --rps 60000 --keys 10000 --duration 30s` sustaining >= 50k decisions/s, p99 < 5 ms on the Redis path, allowed ratio within 1 % of expected, zero errors, winning row in `README.md`.
