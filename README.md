# ratelimitd

Distributed rate limiter in Go: GCRA, token bucket and sliding window over integer microseconds, per-tenant
policies from a hot-reloaded `policies.yaml`, one atomic Redis Lua round trip per subject (dry-run then commit
across every matching policy), and a breaker-guarded local fallback when Redis is down. gRPC + HTTP JSON data
plane, admin API, Prometheus text + OTel, and a public client library with `net/http` middleware and gRPC
interceptors. Design: [docs/PLAN.md](docs/PLAN.md).

**Status: step 1 of the plan (scaffold, proto, frozen contract stubs).** Nothing limits yet.

```powershell
task redis:up                                 # redis:7-alpine on 127.0.0.1:6380
task run                                      # gRPC :50053, HTTP :8093, admin 127.0.0.1:9130
task client -- health                         # health: SERVING
task test ; task lint                         # -race + buf lint + golangci-lint
task test:integration                         # -tags integration, TEST_REDIS_ADDR=127.0.0.1:6380
```

| Surface | Where |
| --- | --- |
| Proto | `proto/ratelimit/v1/ratelimit.proto` -> `gen/ratelimit/v1` (committed) |
| Daemon / CLI | `cmd/ratelimitd`, `cmd/ratelimitctl` |
| Domain, algorithms, policy | `internal/limit`, `internal/algo/*`, `internal/policy` |
| Backends | `internal/store/{local,redisx,fallback}`, `internal/testproxy` |
| Edge | `internal/server`, `internal/httpapi`, `internal/admin`, `internal/obs` |
| Client library | `pkg/ratelimit` |

Bench target (step 12): >= 50k decisions/s single node, p99 < 5 ms on the Redis path. Result row: _pending_.
