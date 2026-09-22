# ratelimitd

Distributed rate limiter in Go: GCRA, token bucket and sliding window as pure integer-microsecond state
machines, per-tenant policies from a hot-reloaded YAML file, **one atomic Redis Lua round trip per subject**
(every matching policy dry-run, then committed only if all allow), and a breaker-guarded local fallback when
Redis is down. gRPC + HTTP JSON data plane, admin API, hand-written Prometheus text + OTel, and a public client
library (`pkg/ratelimit`) with `net/http` middleware and gRPC interceptors. Design record: [docs/PLAN.md](docs/PLAN.md).

```powershell
task redis:up                                   # redis:7-alpine on 127.0.0.1:6380
task run                                        # gRPC :50053, HTTP :8093, admin gRPC 127.0.0.1:50054, admin 127.0.0.1:9130, POLICY_FILE=policies.example.yaml
task client -- check -tenant acme -subject k1   # ALLOW policy=per_ip limit=60 remaining=59 ... backend=redis
task client -- check -tenant acme -subject k2 -route /v1/search/bulk -n 3
#   ALLOW policy=search limit=20 remaining=10   (bulk costs 10 of a 20-token bucket)
#   ALLOW policy=search limit=20 remaining=0
#   DENY  policy=search limit=20 remaining=0 retry_after=996.697ms   (exit 3)
curl -X POST 127.0.0.1:8093/v1/check -d '{"tenant":"acme","subject":"k1"}'
task client -- policy list -tenant acme         # AdminService, on the loopback control plane (127.0.0.1:50054)
#   {"allowed":true,"limit":60,"remaining":56,"retry_after_ms":0,"reset_after_ms":77780,"policy":"per_ip","backend":"redis","degraded":false}
```

## How a decision is made

```
gRPC Check / HTTP POST /v1/check
  -> validate (tenant/subject 1..256 printable ASCII, <= 16 descriptors, cost 1..10000)
  -> policy.Registry.Snapshot() (atomic.Pointer, lock-free) .Resolve(tenant, subject, descriptors, route, method)
       deny list  -> Decision{Allowed:false, Policy:"deny"}      allow list -> immediate allow
       else every enabled matching policy (tenant, then global) -> limit.Req{Key, Params, Cost, Policy}
  -> fallback.Store.Apply(reqs, now)
       breaker closed: redisx -> ONE EVALSHA, KEYS = one per policy (same hash slot), commit=0 pass over all,
                       commit=1 pass only if every key allowed          (a denial never mutates state)
       breaker open:   local.Scale(divisor).Apply   |  fail_open  |  fail_closed        (FALLBACK_MODE)
  -> limit.Strictest(decisions): deny wins; among equals min Remaining, then max RetryAfter
  -> CheckResponse{code, limit, remaining, retry_after, reset_after, policy, backend, degraded}
       a denial is CODE_OVER_LIMIT with an OK gRPC status; only pkg/ratelimit maps it to ErrDenied / 429
```

- **Integer microseconds everywhere.** Lua numbers are doubles (exact to 2^53); unix nanos are not exact, unix
  micros are until 2255. `now`, TAT, emission interval and micro-token counts cross the wire as integers.
- **The caller's clock is authoritative**: `now` is an argument of every `Step`, `Apply` and script call. Skew vs
  Redis `TIME` is sampled every 10 s (`rl_clock_skew_seconds`), warned above 250 ms, refused at startup above
  `MAX_CLOCK_SKEW` (2 s). A slow clock under-admits (fail-safe); a fast one over-admits by at most `skew * rate`.
- **The Go `Step` is the reference model.** The Lua implements the same arithmetic and a differential test
  (`-tags integration`) drives 12k random ops through both, asserting identical decisions *and* stored state,
  including at `now = 2^52` and after a 30-day idle gap.
- **Keys** are `rl:{t=<tenant>|s=<tag>}:p=<policy>[:extras]` where the tag is the first `{subject}` or
  descriptor placeholder of the template. `Validate` rejects any two policies that can match one request but
  hash to different tags, so a call never CROSSSLOTs at runtime. Consequence: a truly global shared counter
  cannot share every subject's slot; `global_ingress` in the example is a per-subject ceiling.

## Algorithms (`internal/algo`, `BenchmarkStep`, 0 allocs)

| algorithm | state | Step | semantics |
|---|---|---:|---|
| `gcra` (default) | 1 int64: theoretical arrival time | 5.0 ns | tolerance = burst × emission; exact `Retry-After`; smooth pacing |
| `token_bucket` | {micro-tokens, last_us} | 19.6 ns | lazy integer refill, elapsed clamped to time-to-full (< 2^53) |
| `sliding_window` | {prev, cur, window idx} | 13.9 ns | weighted two-window approximation; a behind-clock is pinned to the stored window |
| `sliding_window_exact` | timestamp ring / ZSET | 343 ns | O(requests) memory; tests and small limits only |

Property tests per algorithm: `allowed <= burst + rate*elapsed`, state never rewinds, a denied call never mutates
state, `cost n == n × cost 1` (GCRA, bucket); `FuzzStep` against a float64 model.

## Policies (`policies.example.yaml`, hot-reloaded)

```yaml
version: 1
defaults: { algorithm: gcra, on_backend_error: local, cost: 1 }
tenants:
  acme:
    default: api
    allow: ["10.0.0.0/8"]           # short-circuit, never touches a backend
    deny:  ["203.0.113.7"]
    policies:
      - { name: api,    algorithm: gcra,           rate: 1000, per: 1s, burst: 200, key: "{tenant}:{subject}" }
      - { name: search, algorithm: token_bucket,   rate: 10,   per: 1s, burst: 20,  key: "{tenant}:{subject}:{route}",
          match: { routes: ["/v1/search*"] }, cost: { "/v1/search/bulk*": 10 } }
      - { name: per_ip, algorithm: sliding_window, rate: 60,   per: 1m, burst: 60,  key: "{tenant}:{subject}:{ip}" }
global:
  - { name: global_ingress, algorithm: gcra, rate: 200000, per: 1s, burst: 20000, key: "{subject}" }
```

Strict parse (unknown keys rejected), `Validate` (rates, bursts, names `[a-z0-9._-]{1,32}`, template placeholders,
hash-tag rule, glob syntax), atomic swap only when valid. Reload is an mtime+size poll (`POLICY_POLL`, 2 s) then a
content hash - Windows has no SIGHUP. `AdminService.UpsertPolicy/DeletePolicy` rewrite the file (temp + rename) and
the watcher no-ops on its own write. Globs use `path.Match` syntax with `*` spanning `/`, so `/v1/search*` covers
`/v1/search/bulk`. Request `cost` multiplies the policy's route cost.

## Fallback and the partition test

`internal/store/fallback` wraps redisx + local behind a breaker: a sliding sample of `BREAKER_MIN` (20) calls,
open when failures/samples >= `BREAKER_RATIO` (0.5) - a call slower than `REDIS_TIMEOUT` (50 ms) counts as a
failure - stays open `BREAKER_OPEN_FOR` (5 s), then a single-flight half-open probe every `BREAKER_PROBE` (500 ms)
closes it after 3 consecutive successes. A failed primary call is answered from the degraded path immediately,
before the breaker trips. Modes: `local` (default: each node enforces `limit/FALLBACK_DIVISOR`, so N nodes
over-admit by at most N/divisor), `fail_open` (metered, not default), `fail_closed`.

`internal/testproxy` sits between the daemon and Redis and `Blackhole()`s: accepted connections are parked, no
RST, no FIN (the `docker pause` case). Measured in `TestPartition`: breaker opened after 3 failed calls in 152 ms;
977 calls answered as `degraded` over the 1.0 s gap with 109 allowed against a local bound of 110; closed 53 ms
after `Restore()`. `TestNoscriptRecovery`: `SCRIPT FLUSH` mid-run costs exactly one reload, zero errors.

## Client library (`pkg/ratelimit`)

```go
c, _ := ratelimit.Dial(ctx, "localhost:50053")                       // or ratelimit.Embedded(set) - no daemon
h := ratelimit.Middleware(c, func(r *http.Request) ratelimit.Request {
    return ratelimit.Request{Tenant: "acme", Subject: r.Header.Get("X-API-Key"), Route: r.URL.Path, Method: r.Method}
}, ratelimit.WithOnError(ratelimit.FailOpen))(mux)
// every response: RateLimit-Limit / RateLimit-Remaining / RateLimit-Reset (s, ceil)
// denial: 429 application/problem+json + Retry-After (s, ceil, min 1)
srv := grpc.NewServer(grpc.UnaryInterceptor(ratelimit.UnaryInterceptor(c, keyFunc)))   // ResourceExhausted + trailers
```

`Embedded` runs `Resolve -> local.Apply -> Strictest` in-process (with `WithClock` for tests): the same policy
file, no Redis, per-process limits.

## Listeners

| port | env | default bind | serves |
|---|---|---|---|
| 50053 | `GRPC_ADDR` | `:50053` | data plane: `RateLimitService` (Check, BatchCheck), gRPC health, reflection |
| 8093 | `HTTP_ADDR` | `:8093` | data plane: `POST /v1/check` |
| 50054 | `ADMIN_GRPC_ADDR` | `127.0.0.1:50054` | control plane: `AdminService` (policy CRUD, ResetKey, Stats) behind `ADMIN_TOKEN`, gRPC health, reflection |
| 9130 | `ADMIN_ADDR` | `127.0.0.1:9130` | `/metrics`, `/admin/stats`, `/healthz`, `/readyz` |

The AdminService is never registered on `GRPC_ADDR`: an admin RPC there is `Unimplemented`, token or not
(`TestPlaneSeparation`). `ratelimitctl` sends `check`/`bench` to `-addr` and `policy`/`reset`/`stats` to `-admin-addr`
(env `ADMIN_GRPC_ADDR`); `health -admin` probes the control plane.

## Configuration (env, all validated at once)

| env | default | meaning |
|---|---|---|
| `GRPC_ADDR` / `HTTP_ADDR` / `ADMIN_ADDR` | `:50053` / `:8093` / `127.0.0.1:9130` | listeners |
| `ADMIN_GRPC_ADDR` | `127.0.0.1:50054` | control-plane gRPC listener (AdminService only); must differ from `GRPC_ADDR` |
| `ADMIN_TOKEN` | unset | bearer token the AdminService RPCs require on `ADMIN_GRPC_ADDR` (defence in depth on top of the loopback bind); `ratelimitctl -token` / env `ADMIN_TOKEN` sends it. Unset = unauthenticated control plane, logged at startup (a warning when `ADMIN_GRPC_ADDR` is not loopback) |
| `REDIS_ADDR` / `REDIS_POOL_SIZE` / `REDIS_CLUSTER` / `REDIS_TIMEOUT` | `127.0.0.1:6380` / `64` / `false` / `50ms` | primary; timeout = read, write, dial and the breaker's latency budget |
| `POLICY_FILE` / `POLICY_POLL` | `policies.yaml` / `2s` | hot reload |
| `FALLBACK_MODE` / `FALLBACK_DIVISOR` | `local` / `1` | degraded behaviour, per-node quota split |
| `BREAKER_RATIO` / `BREAKER_MIN` / `BREAKER_OPEN_FOR` / `BREAKER_PROBE` | `0.5` / `20` / `5s` / `500ms` | breaker |
| `LOCAL_SHARDS` / `LOCAL_IDLE_TTL` / `LOCAL_MAX_KEYS` | `256` / `10m` / `1000000` | local backend + sweeper |
| `MAX_CLOCK_SKEW` / `TRUST_CLIENT_CLOCK` | `2s` / `false` | clock rules |
| `SHUTDOWN_TIMEOUT` / `OTEL_ENABLED` / `OTEL_EXPORTER_OTLP_ENDPOINT` / `LOG_LEVEL` / `LOG_FORMAT` | `10s` / `false` / `http://localhost:4317` / `info` / `json` | lifecycle + obs |

## Observability

`/metrics` on the admin listener (hand-written text, ~28 µs / 17 allocs per render; series capped at 4096):
`rl_decisions_total{tenant,policy,allowed,backend}`, `rl_decision_latency_seconds{backend}` (50 µs..1 s buckets),
`rl_backend_errors_total{backend,kind}`, `rl_fallback_active{backend}`, `rl_fallback_seconds_total`,
`rl_redis_script_loads_total`, `rl_policy_version_info{version}`, `rl_policy_reload_errors_total`,
`rl_clock_skew_seconds`, `rl_local_keys`, `rl_local_evicted_total`, `rl_build_info`. The OTel instruments read
the same `admin.Snapshot`. `/admin/stats` (JSON incl. breaker state), `/healthz`, `/readyz` (503 only when Redis
is down *and* `FALLBACK_MODE=fail_closed`), `AdminService.Stats`. Dashboard: `deploy/grafana/ratelimitd.json`.
Excerpt after the smoke above:

```
rl_decisions_total{tenant="acme",policy="per_ip",allowed="true",backend="redis"} 4
rl_decisions_total{tenant="acme",policy="search",allowed="false",backend="redis"} 1
rl_decisions_total{tenant="acme",policy="search",allowed="true",backend="redis"} 2
rl_decision_latency_seconds_count{backend="redis"} 7
rl_policy_version_info{version="99871c43cc3f2ccd"} 1
```

## Benchmarks

Machine: AMD Ryzen 7 3700X (8c/16t), Windows 11, Go 1.27.0; Redis 7 in Docker Desktop (WSL2) on 127.0.0.1:6380.
`scripts/bench.ps1` (`task bench:matrix`) builds, starts the daemon per row, runs `ratelimitctl bench` and writes
`results/bench/<ts>.md`; every number below is copied from `results/bench/20260919-121852.md` and
`results/bench/20260919-122230.md`. Load: open-loop pacer, 60k decisions/s offered, 24 workers, batch 8 subjects
per tick (gRPC `BatchCheck`, pipelined into one Redis round trip), 30 s after 5 s warmup; missed ticks are
reported, never hidden. Policy `bench/load` gcra 100000/s burst 10000 (one key per call) unless noted.

| row | proto | backend | keys | achieved/s | p50 | p99 | max | allowed (expected) | errors | missed ticks | gate |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---|
| redis, 1 policy | grpc | redis | 10000 | **43,574** | 4.30 ms | 6.83 ms | 12.19 ms | 1.0000 (1.0000) | 0 | 61,585 | ✖ 50k / 5 ms |
| redis, 2 policies (`+ global_ingress`, dry-run + commit) | grpc | redis | 10000 | 25,971 | 7.34 ms | 10.25 ms | 16.18 ms | 1.0000 (1.0000) | 0 | 127,615 | ✖ |
| redis, hot key | grpc | redis | 1 | 45,402 | 4.22 ms | 7.15 ms | 15.65 ms | 1.0000 (1.0000) | 0 | 54,739 | ✖ |
| redis, deny path (`benchdeny/strict` 20000/s, 1 key) | grpc | redis | 1 | **50,468** | 3.74 ms | 6.30 ms | 15.01 ms | **0.3963 (0.3963)** | 0 | 35,744 | ✖ p99 |
| redis, HTTP JSON (8 sequential POSTs per tick) | http | redis | 10000 | 11,977 | 15.76 ms | 22.54 ms | 42.29 ms | 1.0000 (1.0000) | 0 | 180,094 | ✖ |
| local (Redis down, `FALLBACK_MODE=local`) | grpc | degraded | 10000 | **60,000** | 0.51 ms | 1.15 ms | 3.72 ms | 1.0000 (1.0000) | 0 | 0 | ✔ |

Reading it honestly:

- **The 50k/s, p99 < 5 ms target is met on the local path (60k/s offered = 60k/s achieved, p99 1.15 ms, 0
  missed ticks) and not on the Redis path (43.6k/s, p99 6.8 ms).** The ceiling is Redis itself as deployed here:
  `redis-benchmark` *inside the container*, pipelined, executes this script (one gcra key, commit) at
  **47,393 calls/s** and 32,446/s unpipelined - a Lua EVALSHA costs ~20 µs of single-threaded Redis time on this
  WSL2 VM, against 862k/s for a plain `GET`. The daemon reaches 92 % of that in-container ceiling through Docker
  Desktop's NAT; the missing 15 % to 50k is not on the Go side (see local).
- The two-policy row halves throughput because the all-or-nothing rule runs every algorithm twice (dry-run, then
  commit). A single-pass variant that buffers the writes is the obvious next step (`dispatch.lua`); single-key
  calls already skip the dry-run.
- **Correctness under load is exact**: the deny row's allowed ratio is 0.3963 measured vs 0.3963 from the closed form
  `min(offered, rate·t)/offered`, at 50k decisions/s on one key. Denials are cheaper than allows (no `SET`).
- There is no hot-key penalty at this scale (45.4k/s on one key vs 43.6k on 10k): Redis is single-threaded either
  way and the Go shard lock is ~75 ns.
- HTTP has no batch endpoint, so its row is 8 sequential POSTs per tick on 24 connections; per-request latency is
  ~2 ms (15.76 / 8). It measures the JSON adapter, not the limiter.
- Two things found and fixed on the way: `BatchCheck` was one Redis round trip *per subject* (11k/s, p50 11 ms)
  before the `limit.BatchStore` pipelining port; and go-redis >= 9.16 retries a failed **dial** 5 × 100 ms on top
  of `MaxRetries`, which stalled the request that happened to be the half-open probe for 400 ms with Redis down
  (`DialerRetries: 1` now).

Micro (`task bench:micro`): `gcra.Step` 5.0 ns, `local.Apply` 74-79 ns / 1 alloc under `RunParallel` with 10k keys,
`policy.Resolve` 731 ns / 2 allocs for a 3-policy tenant, `admin` `RecordDecision` 81-103 ns / 0 allocs.

## Teardown

Ctrl-Break (Windows) / SIGTERM: **1/3** health -> NOT_SERVING, gRPC `GracefulStop` + both HTTP servers drained
within `SHUTDOWN_TIMEOUT` (else forced); **2/3** cancel the background context and wait 5 s for the policy watcher,
breaker prober, local sweeper, skew sampler and registry mirror; **3/3** close the Redis pool, flush OTel (5 s).
Verified on Ctrl-Break: three lines in order, 3 ms, exit 0.

## Testing and definition of done

```powershell
task test                    # go test -race -count=1 ./...   (unit tests use fakes; no Redis)
task test:integration        # -tags integration, TEST_REDIS_ADDR=127.0.0.1:6380: differential Lua test, one-key bound,
                             # all-or-nothing, NOSCRIPT recovery, ApplyMany, partition through testproxy
task lint                    # buf lint + golangci-lint (0 issues)
go test -run '^$' -fuzz '^FuzzParse$' -fuzztime 60s ./internal/policy/   # 18 Fuzz* targets: every parser/decoder (see docs/PLAN.md "Hardening")
task bench:matrix            # scripts/bench.ps1 -> results/bench/<ts>.md
```

CI (`.github/workflows/ci.yml`): build + vet + `-race` on ubuntu and windows, lint, `buf generate` diff, and the
integration job against a `redis:7-alpine` service container.

## Not done / follow-ups

- Redis path short of the 50k/s target by 13 % on this box; single-pass Lua for multi-policy calls and a
  Redis outside Docker Desktop are the two levers (rueidis is the third, kept behind the `Store` port).
- A shared global counter needs its own slot (and its own call); today `global` policies are per-subject ceilings.
- `ResetKey` addresses `rl:{t|s}:p=<policy>` exactly; templates with extras (`{route}`, `{ip}`) leave those variants
  until they expire.
- No per-tenant auth on the data plane (v1 trusts `tenant`; bind to loopback or front with an authenticating proxy).
  The AdminService has its own listener (`ADMIN_GRPC_ADDR`, loopback by default) and is plaintext gRPC: bind it
  wider only with `ADMIN_TOKEN` set, and behind TLS if it leaves the host.
- Not exercised live: OTel export against a running collector, Redis Cluster mode.
