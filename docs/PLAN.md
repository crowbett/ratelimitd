# ADR-0001: P12 "ratelimitd" - distributed rate limiter (GCRA/token-bucket/sliding-window, Redis Lua + local fallback)
Status: Accepted (implemented 2026-09-19; lane deviations at the end) | Date: 2026-09-19 | Module: `github.com/crowbett/ratelimitd` | Go 1.27

## Context
Scaffold is `~/dev/_templates/go-grpc` (buf v2 `proto/` -> committed `gen/`, `internal/interceptor` Recover -> RequestID -> Logging, bufconn table tests, health + reflection, Taskfile, strict `.golangci.yml`, CI diffing `buf generate`). Step 1 deletes the `echo` surface exactly as P5/P9 did.
Reuse from `~/dev/go/jobrun` (P9): `cmd/jobrund` staged teardown, `internal/config` env-only `Load()`/`Validate()` with no globals, `internal/admin` hand-written Prometheus text exposer + `/admin/stats` + `Snapshot` atomics feeding both OTel and `/metrics`, the CI integration job with a service container. From `~/dev/go/aigw/internal/gate`: the "never start a goroutine, pool the per-call token, atomics for stats" style that the local backend copies. From `~/dev/go/perf-lab`: bench harness conventions (open-loop pacing, warmup, p50/p99 from a sorted sample, one result row committed to README).
Requirement (ROADMAP P12): GCRA + token bucket + sliding window, per-tenant policies, Redis Lua atomic path with local fallback when Redis is down, gRPC + HTTP middleware library, Prometheus, **50k decisions/s single node**, correctness under partition.
Ports must not collide: orders pg 5433/nats 4223, sysmon 9100, aigw 8090, edgecache 8080, jobrun pg 5434/gRPC 50052/admin 9110. ratelimitd takes **redis 6380** (its own `compose.yaml`, so it coexists with docker-stack's 6379), **gRPC :50053**, **HTTP JSON :8093**, **admin 127.0.0.1:9130**, and (amended 2026-09-22, see "Control-plane listener") **admin gRPC 127.0.0.1:50054**.
New deps beyond the scaffold: **`github.com/redis/go-redis/v9` only**, plus the same OTel set as jobrun. No Prometheus client (the hand-written exposer is house style and zero-alloc), no fsnotify, no circuit-breaker library, no mocking framework, no toxiproxy.

## Decision
1. **Algorithms are pure, float-free state machines** in `internal/algo/*`, each a `Step(state, Params, cost, now) (state, Decision)` over `int64` microseconds. They allocate nothing, do no I/O, and know nothing about Redis or keys. The Go `Step` is the **reference model**; the Lua implements the same arithmetic on the same integer units, and a differential test drives both from the same random operation sequence and asserts identical states and decisions. That is the whole correctness story for the distributed path.
2. **Microseconds, never nanoseconds, never floats.** Redis Lua numbers are IEEE doubles: integers are exact only to 2^53 (9.0e15). Unix nanos (1.8e18) silently lose precision; unix micros (1.8e15) are exact until the year 2255. Go and Lua both carry `now`, TAT, emission interval and token counts as integer microseconds / micro-tokens.
3. **The caller's clock is authoritative, not Redis `TIME`.** `now` is passed in as an argument, which keeps the script deterministic, testable and one round trip cheaper. Skew is fail-safe in the strict direction for GCRA and token bucket (a node whose clock is behind sees a larger `tat - now`, i.e. less capacity, and never over-admits); a node whose clock is *ahead* over-admits by at most `skew * rate`. Every node samples Redis `TIME` every 10 s, exports `rl_clock_skew_seconds`, warns above 250 ms and refuses to start above `MAX_CLOCK_SKEW` (2 s).
4. **One script, one SHA, all-or-nothing multi-policy.** A request can match several policies and the answer is "all must allow, report the strictest Decision". Committing policy A's token and then denying on policy B leaks quota, so the script does a **dry-run pass over every key, then commits only if every key allowed**. That is only possible when all of a subject's keys live in one slot, hence the hash tag: `rl:{t=<tenant>|s=<subject>}:p=<policy>`. Algorithm sources stay one file each (`gcra.lua`, `tokenbucket.lua`, `slidingwindow.lua`) and are concatenated with `dispatch.lua` at build time into the single script that is loaded.
5. **`go-redis/v9`, not `rueidis`.** rueidis is genuinely faster (auto-pipelining, RESP3), but the entire Redis contact area here is ~200 lines behind the `Store` port: one `redis.Script` (EVALSHA with automatic NOSCRIPT -> EVAL reload), `Pipeline` for batch, `TIME`, `PING`. go-redis gives cluster support, context cancellation, and a pool we tune explicitly; its client-side-caching-free model is irrelevant to a read-modify-write workload. If the bench misses 50k/s on the Redis path, swapping in rueidis is a new `Store` implementation, not a refactor - that is recorded here as the escape hatch.
6. **Degradation is a policy decision, not an accident.** The `fallback` store wraps primary+local behind a breaker with three configurable modes (`fail_open`, `fail_closed`, `local`), default **`local`** with a `degraded_divisor` (default = expected node count, 1 on this box): while degraded, each node enforces `limit/divisor` locally, so N nodes over-admit by at most a factor of N/divisor instead of unbounded. Fail-open is available and loudly metered; it is not the default because a rate limiter that silently stops limiting is worse than one that 429s.

## Package tree
```
cmd/ratelimitd/main.go        daemon wiring only: config -> obs -> stores -> policy -> grpc + admin grpc + http + admin -> staged teardown
cmd/ratelimitctl/main.go      CLI: check | policy get|list|upsert | reset | bench   (flag pkg, gRPC client)
proto/ratelimit/v1/*.proto    RateLimitService (Check/BatchCheck) + AdminService (Get/List/Upsert/DeletePolicy, Reset)
gen/ratelimit/v1/             GENERATED, committed, never hand-edited
pkg/ratelimit/                PUBLIC client library: Client iface, gRPC client, embedded local client,
                              net/http Middleware + RateLimit-* headers, grpc unary/stream interceptors
internal/limit/               DOMAIN, no I/O: Decision, Params, Algorithm, Limiter/Store ports, sentinels, key building
internal/algo/gcra/           pure GCRA: State int64 (TAT us), Step, reference for the Lua
internal/algo/tokenbucket/    pure token bucket: State{Tokens int64 micro-tokens, Last int64 us}, lazy refill
internal/algo/slidingwindow/  weighted two-window approximation (default) + exact ring-of-timestamps variant
internal/store/local/         sharded map backend: 256 shards, per-shard mutex, idle-key GC sweeper
internal/store/redisx/        go-redis backend: embedded Lua (embed.FS), script load, key namespace, TTLs, TIME probe
internal/store/fallback/      breaker (error ratio + latency budget), degraded modes, half-open prober, metrics
internal/policy/              policy model, YAML load + validate, key templates, matcher, allow/deny lists,
                              Registry (atomic.Pointer snapshot) + mtime-poll Watcher + Upsert
internal/server/              gRPC adapters: check.go, admin.go, convert.go, errors.go + bufconn tests
internal/httpapi/             POST /v1/check JSON adapter (same validation path as gRPC, shared converters)
internal/admin/               Snapshot atomics, /metrics text exposer, /admin/stats, /healthz, /readyz
internal/obs/                 OTel trace+metric providers; observable instruments read the same Snapshot
internal/bench/               load harness: open-loop pacer, per-worker histogram, expected-vs-actual allowed ratio
internal/config/              Load()/Validate(); every knob in the env table
internal/testproxy/           TCP proxy used by tests: Pause/Drop/Delay between client and Redis (no toxiproxy)
compose.yaml                  redis:7-alpine on host 6380, healthchecked     deploy/grafana/ratelimitd.json
policies.example.yaml         the config schema below                        docs/PLAN.md (this file)
```
Imports: `cmd` -> everything; `server`/`httpapi`/`store/*`/`policy`/`admin` -> `limit`; `store/*` -> `algo/*` -> `limit`; `limit` -> stdlib only. `pkg/ratelimit` -> `gen/` + `limit` + `algo/*` + `store/local` (legal: our own module may import its own `internal/`, and third parties importing `pkg/ratelimit` compile fine).

## Interfaces and key types
```go
// ---- internal/limit (domain) ----
type Algorithm uint8 // GCRA, TokenBucket, SlidingWindow, SlidingWindowExact
type Params struct { // normalised at policy load; all integer microseconds
    Algo       Algorithm
    Rate       int64         // permitted events per Period
    Period     time.Duration // e.g. time.Second, time.Minute
    Burst      int64         // capacity; GCRA dvt = Burst*EmissionUS, bucket cap = Burst
    EmissionUS int64         // Period/Rate in us, precomputed; 0 is invalid
}
type Decision struct {
    Allowed                bool
    Remaining              int
    RetryAfter, ResetAfter time.Duration
    Limit                  int
    Policy                 string // which policy produced it (the strictest, when several matched)
    Backend                string // "redis" | "local" | "degraded"
}
func Strictest(ds []Decision) Decision // deny wins; among equals, min Remaining then max RetryAfter
type Limiter interface {
    Allow(ctx context.Context, key string, n int, now time.Time) (Decision, error)
}
// Store is the backend port consumed by the service. Keys carry their own Params because a
// single call may span several policies for one subject (all-or-nothing, see Decision 4).
type Req struct{ Key string; Params Params; Cost int; Policy string }
type Store interface {
    Apply(ctx context.Context, reqs []Req, now time.Time) ([]Decision, error) // len(out) == len(reqs)
    Peek(ctx context.Context, reqs []Req, now time.Time) ([]Decision, error)  // no state mutation
    Reset(ctx context.Context, keys []string) error
    Name() string
}
var ( ErrUnknownTenant = errors.New("unknown tenant"); ErrNoPolicy = errors.New("no policy matched")
      ErrInvalidParams = errors.New("invalid policy params"); ErrBackendDown = errors.New("backend unavailable")
      ErrDenied        = errors.New("rate limited") ) // pkg/ratelimit returns this; errors.Is at call sites
// ---- internal/algo/gcra (pure; tokenbucket and slidingwindow mirror this shape) ----
type State int64 // theoretical arrival time, unix microseconds; 0 = empty
func Step(s State, p limit.Params, cost int, nowUS int64) (State, limit.Decision) // no alloc, no time.Time math
func TTL(s State, p limit.Params, nowUS int64) time.Duration
// ---- internal/store/local ----
type Backend struct{ /* shards [256]shard; each: sync.Mutex + map[string]entry; atomics for stats */ }
func New(cfg Config) *Backend            // Config{Shards int; IdleTTL, SweepEvery time.Duration; MaxKeys int}
func (b *Backend) Run(ctx context.Context) error // ONE goroutine: the idle-key sweeper; returns on ctx
func (b *Backend) Apply(ctx context.Context, reqs []limit.Req, now time.Time) ([]limit.Decision, error)
func (b *Backend) Scale(div int64) *Backend      // degraded view: Params.Rate/div, Burst/div, min 1
// ---- internal/store/redisx ----
type Config struct{ Addr, Username, Password string; DB, PoolSize, MinIdle int
    DialTimeout, ReadTimeout, WriteTimeout time.Duration; Cluster bool; KeyPrefix string; TTLSlack time.Duration }
func New(cfg Config, log *slog.Logger, st *admin.Snapshot) (*Backend, error)
func (b *Backend) Apply(...)  // one EVALSHA for all reqs of one subject; Pipeline across subjects in BatchCheck
func (b *Backend) Ping(ctx context.Context) error
func (b *Backend) Skew(ctx context.Context, now time.Time) (time.Duration, error) // Redis TIME vs local clock
// ---- internal/store/fallback ----
type Mode uint8 // ModeLocal (default), ModeFailOpen, ModeFailClosed
type Config struct{ Mode Mode; DegradedDivisor int64
    FailureRatio float64; MinSamples int; LatencyBudget time.Duration // over budget counts as a failure
    OpenFor, ProbeEvery time.Duration; HalfOpenProbes int }
func New(primary, local limit.Store, cfg Config, log *slog.Logger, st *admin.Snapshot) *Store
func (s *Store) Run(ctx context.Context) error // ONE goroutine: half-open prober while open
func (s *Store) Apply(...)                     // primary; on error/timeout -> record -> degraded path
func (s *Store) State() (open bool, since time.Time, mode Mode)
// ---- internal/policy ----
type Policy struct { Name string; Algorithm string; Rate int64; Per time.Duration; Burst int64
    Key     string            // template: "{tenant}:{apikey}" / "{ip}" / "{route}:{method}"
    Match   Match             // optional: Routes, Methods, Descriptors (all must match)
    Cost    map[string]int    // route glob -> cost, default 1
    Enabled bool }
type Tenant struct{ ID string; Policies []Policy; Default string; Allow, Deny []string /* CIDR or subject glob */ }
type Set struct{ Tenants map[string]Tenant; Global []Policy; Version string /* content hash */; LoadedAt time.Time }
func Parse(r io.Reader) (Set, error)                 // strict YAML: unknown fields are an error
func (s Set) Validate() error                        // rate>0, burst>=1, per>=1ms, template refs resolve
type Registry struct{ /* atomic.Pointer[Set] - readers are lock-free on the hot path */ }
func NewRegistry(initial Set, log *slog.Logger) *Registry
func (r *Registry) Snapshot() *Set
func (r *Registry) Upsert(ctx context.Context, tenant string, p Policy) (*Set, error) // admin API; rewrites the file
func (r *Registry) Watch(ctx context.Context, path string, every time.Duration) error // ONE goroutine: mtime+size poll
// Resolve is the hot path: returns the Reqs to send, or a short-circuit Decision for allow/deny lists.
func (r *Registry) Resolve(tenant, subject string, d []Descriptor, route, method string) ([]limit.Req, *limit.Decision, error)
type Descriptor struct{ Key, Value string } // Envoy RLS shape
// ---- pkg/ratelimit (public) ----
type Client interface{ Check(ctx context.Context, req Request) (Result, error); Close() error }
type Request struct{ Tenant, Subject, Route, Method string; Descriptors []Descriptor; Cost int }
type Result struct{ Allowed bool; Limit, Remaining int; RetryAfter, ResetAfter time.Duration; Policy string }
func Dial(ctx context.Context, target string, opts ...Option) (Client, error) // gRPC, keepalive, retry on UNAVAILABLE
func Embedded(set policy.Set, opts ...Option) (Client, error)                 // no daemon: local backend in-process
type KeyFunc func(*http.Request) Request
func Middleware(c Client, kf KeyFunc, opts ...MWOption) func(http.Handler) http.Handler
func UnaryInterceptor(c Client, kf func(context.Context, string) Request) grpc.UnaryServerInterceptor
// MWOptions: WithOnError(FailOpen|FailClosed), WithDenyHandler(http.Handler), WithHeaders(bool), WithCost(func)
```

## Lua: `gcra.lua` (the one that matters)
```lua
-- KEYS[1] state key. ARGV: 1 now_us, 2 emission_us, 3 burst, 4 cost, 5 ttl_slack_ms, 6 commit(0|1)
-- Returns {allowed, remaining, retry_us, reset_us, limit} - all integers, no floats crossing the wire.
local now, T, cap, n = tonumber(ARGV[1]), tonumber(ARGV[2]), tonumber(ARGV[3]), tonumber(ARGV[4])
local dvt = cap * T                               -- delay variation tolerance = full capacity
local tat = tonumber(redis.call('GET', KEYS[1])) or now
if tat < now then tat = now end                   -- a bucket idle past full is just full
local newtat = tat + n * T
local diff = now - (newtat - dvt)                 -- >= 0 means the cell fits inside the tolerance
if diff < 0 then                                  -- denied: state is NOT advanced (no partial spend)
  return {0, math.floor((dvt - (tat - now)) / T), -diff, tat - now, cap}
end
if ARGV[6] == '1' then
  redis.call('SET', KEYS[1], newtat, 'PX', math.ceil((newtat - now) / 1000) + tonumber(ARGV[5]))
end
return {1, math.floor((dvt - (newtat - now)) / T), 0, newtat - now, cap}
```
`dispatch.lua` loops KEYS with `commit=0` first; if every `result[1]` is 1 it re-runs with `commit=1`, else it returns the dry-run results untouched - the all-or-nothing rule from Decision 4. `tokenbucket.lua` keeps a 2-field hash (`t` micro-tokens, `l` last_us), refills `min(cap, t + (now-l)*rate_per_sec)` in integer micro-tokens and clamps elapsed to time-to-full so the product stays far under 2^53. `slidingwindow.lua` keeps two counters (`prev`, `cur`) plus the window index and denies when `prev*(1 - elapsed/window) + cur + n > limit`; the exact variant (`ZREMRANGEBYSCORE` + `ZCARD` + `ZADD` of `now_us:rand`) ships behind `algorithm: sliding_window_exact` with its O(requests) memory documented - tests and small-limit policies only.

## Proto sketch
```proto
package ratelimit.v1;
service RateLimitService {
  rpc Check(CheckRequest) returns (CheckResponse);                 // one subject, N descriptors
  rpc BatchCheck(BatchCheckRequest) returns (BatchCheckResponse);  // N subjects, pipelined
}
service AdminService {
  rpc GetPolicy(GetPolicyRequest) returns (PolicyMsg);  rpc ListPolicies(ListPoliciesRequest) returns (ListPoliciesResponse);
  rpc UpsertPolicy(UpsertPolicyRequest) returns (PolicyMsg); rpc DeletePolicy(DeletePolicyRequest) returns (google.protobuf.Empty);
  rpc ResetKey(ResetKeyRequest) returns (google.protobuf.Empty); rpc Stats(StatsRequest) returns (StatsResponse);
}
message Descriptor { string key = 1; string value = 2; }           // Envoy RLS-compatible shape
message CheckRequest { string tenant = 1; string subject = 2; repeated Descriptor descriptors = 3;
                       string route = 4; string method = 5; uint32 cost = 6; google.protobuf.Timestamp now = 7; }
message CheckResponse { Code code = 1; uint32 limit = 2; uint32 remaining = 3;
                        google.protobuf.Duration retry_after = 4; google.protobuf.Duration reset_after = 5;
                        string policy = 6; string backend = 7; bool degraded = 8; }
enum Code { CODE_UNSPECIFIED = 0; CODE_OK = 1; CODE_OVER_LIMIT = 2; }
```
`now` is optional: set by trusted callers (tests, bench) and ignored unless `TRUST_CLIENT_CLOCK=true`. A denied Check is **`CODE_OVER_LIMIT` with an `OK` gRPC status**, never `RESOURCE_EXHAUSTED` - the RPC succeeded; only `pkg/ratelimit` maps it to `ErrDenied`/429.

## Config schema and env
```yaml
# policies.yaml - hot-reloaded; strict parse, unknown keys rejected, swapped atomically only if Validate passes
version: 1
defaults: { algorithm: gcra, on_backend_error: local, cost: 1 }
tenants:
  acme:
    default: api
    allow: ["10.0.0.0/8"]           # short-circuit allow, never touches a backend
    deny:  ["203.0.113.7"]
    policies:
      - { name: api,     algorithm: gcra,           rate: 1000, per: 1s, burst: 200, key: "{tenant}:{apikey}" }
      - { name: search,  algorithm: token_bucket,   rate: 10,   per: 1s, burst: 20,  key: "{tenant}:{apikey}:{route}",
          match: { routes: ["/v1/search*"] }, cost: { "/v1/search/bulk*": 10 } }
      - { name: per_ip,  algorithm: sliding_window, rate: 60,   per: 1m, burst: 60,  key: "{ip}" }
global:
  - { name: global_ingress, algorithm: gcra, rate: 50000, per: 1s, burst: 5000, key: "global" }
```
| env | default | meaning |
|---|---|---|
| `GRPC_ADDR` / `HTTP_ADDR` / `ADMIN_ADDR` | `:50053` / `:8093` / `127.0.0.1:9130` | listeners |
| `ADMIN_GRPC_ADDR` | `127.0.0.1:50054` | control-plane gRPC listener: AdminService only, never on `GRPC_ADDR` (must differ from it) |
| `ADMIN_TOKEN` | unset | bearer token the AdminService RPCs (policy CRUD, ResetKey, Stats) require on `ADMIN_GRPC_ADDR` (`authorization: Bearer <token>`, constant-time compare, >= 16 bytes); unset = unauthenticated, logged at startup (warning when `ADMIN_GRPC_ADDR` is not loopback) |
| `REDIS_ADDR` / `REDIS_POOL_SIZE` / `REDIS_CLUSTER` | `127.0.0.1:6380` / `64` / `false` | primary backend |
| `REDIS_TIMEOUT` | `50ms` | read+write timeout; also the fallback latency budget floor |
| `POLICY_FILE` / `POLICY_POLL` | `policies.yaml` / `2s` | hot reload (mtime+size, then content hash) |
| `FALLBACK_MODE` / `FALLBACK_DIVISOR` | `local` / `1` | degraded behaviour and per-node quota split |
| `BREAKER_RATIO` / `BREAKER_MIN` / `BREAKER_OPEN_FOR` / `BREAKER_PROBE` | `0.5` / `20` / `5s` / `500ms` | breaker |
| `LOCAL_SHARDS` / `LOCAL_IDLE_TTL` / `LOCAL_MAX_KEYS` | `256` / `10m` / `1000000` | local backend + GC |
| `MAX_CLOCK_SKEW` / `TRUST_CLIENT_CLOCK` | `2s` / `false` | Decision 3 |
| `SHUTDOWN_TIMEOUT` / `OTEL_ENABLED` / `LOG_LEVEL` | `10s` / `false` / `info` | lifecycle + obs |

## Data flow
1. HTTP `POST /v1/check` or gRPC `Check` -> interceptors (Recover, RequestID, Logging, OTel) -> validate: tenant/subject 1..256 printable ASCII, <= 16 descriptors, cost 1..10000.
2. `policy.Registry.Snapshot()` (lock-free `atomic.Pointer` load) -> `Resolve`: deny list -> immediate `Decision{Allowed:false, Policy:"deny"}`; allow list -> immediate allow; else every matching policy (tenant + global) renders its key template into `rl:{t=<tenant>|s=<subject>}:p=<policy>` and yields a `limit.Req` with precomputed `Params`.
3. `store.Apply(ctx, reqs, now)` - `fallback` first tries `redisx`: one EVALSHA carrying every req for the subject (same hash slot), dry-run then commit. On `NOSCRIPT` go-redis reloads and retries once; `rl_redis_script_loads_total` counts it.
4. Redis error, timeout or over-budget latency -> breaker records a failure; once open, `local.Scale(divisor).Apply` answers (or fail-open/closed per mode), `Decision.Backend="degraded"`, `rl_fallback_active{backend="redis"}=1`.
5. `limit.Strictest(decisions)` -> adapter maps to `CheckResponse` / JSON; `internal/admin.Snapshot` counters bump once per decision (tenant+policy+allowed+backend labels only - never the subject).
6. `pkg/ratelimit.Middleware` sets `RateLimit-Limit`, `RateLimit-Remaining`, `RateLimit-Reset` on every response and `Retry-After` (seconds, rounded up, min 1) plus 429 on denial; on client error it applies `WithOnError` (default fail-open, logged).
7. Admin `UpsertPolicy` -> validate -> rewrite `policies.yaml` atomically (temp + rename) -> registry swap -> the watcher sees its own write and no-ops on the content hash.

## Concurrency and teardown
| goroutine | started by | stopped by | on failure |
|---|---|---|---|
| gRPC `Serve` (data plane) | `run()` | stage 1 `GracefulStop` (budget `SHUTDOWN_TIMEOUT`, else hard stop) | errCh -> run returns |
| gRPC `Serve` (control plane, AdminService) | `run()` | stage 1 `GracefulStop`, concurrently with the data plane on the same budget | errCh |
| HTTP `/v1` server | `run()` | stage 1 graceful stop | errCh |
| admin server (`/metrics`, `/admin/stats`, `/healthz`, `/readyz`) | `run()` | stage 1 | errCh |
| policy watcher (1) | `Registry.Watch` | stage 2 ctx cancel | log, keep the last good Set, bump `rl_policy_reload_errors_total` |
| breaker prober (1) | `fallback.Run` | stage 2 | stays open, retries next `ProbeEvery` |
| local idle-key sweeper (1) | `local.Run` | stage 2 | log, continue |
| clock-skew sampler (1) | `run()` (10 s ticker) | stage 2 | log, export the last value |
| OTel batch exporters | `obs.Setup` | stage 3 `otelStop(5s)` | logged, non-fatal |
No goroutine is started per request or per key: request handling is entirely on the caller's goroutine, like `aigw/internal/gate`. Staged teardown (jobrun's ladder, each stage its own `context.WithTimeout`, never the signal ctx): **1** stop accepting (both gRPC servers + both HTTP) -> **2** cancel the background ctx and `errgroup.Wait` the four tickers -> **3** close the Redis pool, flush OTel. Ctrl-Break on Windows logs all three in order and exits 0. `/readyz` is 503 until policies are loaded **and** the first Redis `PING` has either succeeded or the breaker has settled into a degraded mode - never before the limiter can actually answer.

## Load, correctness and the partition test
- **Micro**: `go test -bench . -benchmem ./internal/algo/...` - target **< 100 ns/op, 0 allocs/op** per `Step`; `./internal/store/local` under `b.RunParallel` with 10k keys - target < 300 ns/op and no shard convoy (report allocs and the p99 from a sample run).
- **Macro**: `ratelimitctl bench --rps 60000 --keys 10000 --duration 30s --proto grpc|http --backend redis|local`. Open-loop pacer (perf-lab style: a per-worker ticker with a missed-tick counter, so a slow server shows as backlog instead of silently closing the loop), warmup 5 s excluded, per-worker latency samples merged and sorted once at the end. Reports achieved decisions/s, p50/p99/max, allowed ratio **vs the closed-form expectation** `min(offered, rate*duration + burst) / offered`, error counts and degraded time. Pass: **>= 50k decisions/s sustained 30 s, p99 < 5 ms on the Redis path, allowed ratio within 1 % of expected, zero errors.** Knobs to sweep: `REDIS_POOL_SIZE` (32/64/128) x batch size (1/8/32) x `GOMAXPROCS`. The winning row goes in `README.md`.
- **Concurrency correctness** (`-tags integration`, Redis up): 8 goroutines hammer one key for 10 s at 5x the limit; assert `allowed <= rate*elapsed + burst` **and** `allowed >= rate*elapsed*0.95` - a limiter that denies everything must fail too, not just an over-admitting one. Repeated per algorithm and at cost > 1.
- **Partition**: `internal/testproxy` sits between the client and Redis on a loopback port; `Blackhole()` drops in-flight and new connections without RST, which is the nasty case (`docker pause` semantics) that killing the container does not reproduce. Assert: the breaker opens within `BREAKER_MIN` failures and **< 500 ms**, decisions keep flowing with `Backend=="degraded"`, over-admission during the gap is bounded by the local limits (measured, asserted), `rl_fallback_active` goes 1 then back to 0, and after `Restore()` the prober closes the breaker within `2*ProbeEvery`. A second case recycles a real Redis container to prove the `NOSCRIPT` reload path (empty script cache) costs one retry, not an outage.

## Observability
`internal/admin.Snapshot` atomics feed both the OTel meter and the hand-written text exposer, so the two cannot disagree (jobrun's rule): `rl_decisions_total{tenant,policy,allowed,backend}`, `rl_decision_latency_seconds` (histogram, explicit buckets 50us..1s), `rl_backend_errors_total{backend,kind}`, `rl_fallback_active{backend}` (gauge 0/1) + `rl_fallback_seconds_total`, `rl_redis_script_loads_total`, `rl_policy_version_info{version}`, `rl_policy_reload_errors_total`, `rl_clock_skew_seconds`, `rl_local_keys` / `rl_local_evicted_total`. Spans `ratelimit.check` (attrs: tenant, policy count, backend, degraded, allowed) with the Redis call as a child; `pkg/ratelimit` propagates the caller's trace. `deploy/grafana/ratelimitd.json`: decisions/s by outcome, denial ratio by tenant, p99 latency by backend, breaker state timeline, top tenants by denials.

## Alternatives considered
- **Redis `INCR` + `EXPIRE` fixed window**: two commands, a boundary burst of 2x, and no `Retry-After` worth reporting. GCRA gives smooth pacing with one key holding one integer.
- **`redis-cell` module**: excellent, but it is an ops dependency (custom image, no managed Redis) and it hides the algorithm we want to own and property-test.
- **Redis `TIME` inside the script**: removes clock skew but makes the script non-deterministic, unshareable with the Go reference model, and adds a call per evaluation. Rejected; skew is measured and bounded instead (Decision 3).
- **Two-phase peek-then-commit across separate round trips** for multi-policy: two RTTs and a race window. The dry-run/commit loop inside one script gets atomicity for free.
- **rueidis**: see Decision 5. Kept reachable as a second `Store`.
- **CRDT / gossip between nodes instead of Redis**: correct eventual accounting without a single point of failure, but the interesting part (converging counters under partition) is a whole project, and the ROADMAP asks for the Redis Lua path. The `Store` port is where it would land.
- **prometheus/client_golang**: three transitive deps for a text format we already emit by hand in sysmon and jobrun.

## Implementation plan
Every step ends with `task fmt; go build ./...; go vet ./...` green plus the check named.
1. **Strip echo, stand up infra.** Delete `proto/echo`, `gen/echo`, `internal/server/echo*.go`; rename `cmd/server` -> `cmd/ratelimitd`, `cmd/client` -> `cmd/ratelimitctl`; add `compose.yaml` (redis:7-alpine, host 6380, healthcheck), `.env.example`, `policies.example.yaml`. -> `docker compose up -d --wait; docker compose ps`.
2. **Proto + domain.** `proto/ratelimit/v1/*.proto`, `internal/limit/{decision,params,ports,errors,key}.go`. -> `buf lint; task gen`, commit `gen/`; `go test ./internal/limit/...` (key templating, `Strictest` table).
3. **Algorithms.** `internal/algo/{gcra,tokenbucket,slidingwindow}` + table tests, benchmarks, and **property tests**: for random op sequences under a monotone clock, `allowed <= burst + rate*elapsed`; state never rewinds; a denied call never mutates state; cost n equals n calls of cost 1 for GCRA/bucket. `FuzzStep` compares each algorithm against a naive reference model. -> `go test -race ./internal/algo/...`; `go test -bench . -benchmem` meets < 100 ns / 0 allocs.
4. **Policy.** `internal/policy/{model,parse,validate,template,match,registry,watch}.go` + tests: strict YAML rejection, template resolution, glob costs, allow/deny precedence, atomic swap under concurrent readers, mtime poll debounce. -> `go test -race ./internal/policy/...`.
5. **Local backend.** `internal/store/local/{backend,shard,sweep,scale}.go` + tests: shard distribution, idle GC, `MaxKeys` eviction, `Scale` arithmetic, `-race` under 8 writers on one key. -> `go test -race ./internal/store/local/...`.
6. **Redis backend + Lua.** `internal/store/redisx/{backend,script,keys,time}.go`, `lua/{dispatch,gcra,tokenbucket,slidingwindow}.lua` via `embed.FS`. **The differential test is the gate**: the same random op sequence through `algo.Step` and through the script must agree exactly. -> `go test -race -tags integration ./internal/store/redisx/...` with the stack up, plus the 8-goroutine one-key bound.
7. **Fallback + breaker.** `internal/store/fallback/{store,breaker,probe}.go` + unit tests with a scripted failing `limit.Store` (open threshold, half-open single flight, mode matrix) and the `internal/testproxy` blackhole integration test. -> `go test -race ./internal/store/fallback/...` and `-tags integration`.
8. **gRPC + HTTP adapters.** `internal/server/*`, `internal/httpapi/*` + bufconn table tests per RPC through the production interceptor chain, and a JSON golden test proving both surfaces return identical decisions. -> `go test -race ./internal/server/... ./internal/httpapi/...`.
9. **Admin, metrics, obs.** `internal/admin/*` (`/metrics` golden file, `/admin/stats`, `/healthz`, `/readyz` gated on policies+backend), `internal/obs/*`, admin RPCs incl. `UpsertPolicy` atomic file rewrite. -> `go test -race ./internal/admin/...`; scrape `/metrics` by hand.
10. **Client library + middleware.** `pkg/ratelimit/{client,grpc,embedded,middleware,interceptor,options}.go` + tests: header values and rounding, 429 body, fail-open/fail-closed on client error, embedded mode without a daemon, interceptor on a bufconn server. -> `go test -race ./pkg/ratelimit/...`.
11. **Wiring + CLI + bench.** `internal/config/*` (+ a Validate table test), `cmd/ratelimitd/main.go` (staged teardown), `cmd/ratelimitctl/*` (check/policy/reset/bench), `internal/bench/*`. -> `task run`; `ratelimitctl check --tenant acme --subject k1` denies at the expected point; Ctrl-Break logs three stages, exit 0.
12. **Numbers, CI, docs.** Run the bench matrix, land the partition + correctness tests in CI (redis service container), `deploy/grafana/ratelimitd.json`, `Taskfile.yml` (`redis:up`, `test:integration`, `bench`), `README.md` with the measured row, this file at `docs/PLAN.md`. -> `task check`, `go test -race -count=1 ./...`, `golangci-lint run ./...` green; the 50k/s criterion met or the gap explained in README.

## Fanout: 4 workstreams + integrator, exclusive file ownership
| WS | Owns (exclusive) | Depends on | Steps |
|---|---|---|---|
| A - algorithms & policy | `internal/limit/**`, `internal/algo/**`, `internal/policy/**` | none (this doc freezes the ports) | 2 (domain), 3, 4 |
| B - backends | `internal/store/**`, `lua/**`, `internal/testproxy/**` | A(3) signatures | 5, 6, 7 |
| C - service edge | `proto/**`, `gen/**`, `internal/server/**`, `internal/httpapi/**`, `internal/admin/**`, `internal/obs/**`, `internal/interceptor/**`, `deploy/**` | A(2) | 2 (proto), 8, 9 |
| D - client & load | `pkg/ratelimit/**`, `internal/bench/**` | C's committed `gen/` | 10, 11 (bench part) |
| INT - integrator | `cmd/**`, `internal/config/**`, `compose.yaml`, `go.mod`, `go.sum`, `Taskfile.yml`, `.github/**`, `README.md`, `CLAUDE.md`, `docs/**`, echo deletions | all | 1, 11, 12 |
**INT lands step 1 alone** (echo stripped, compose up, module renamed), then **C commits `proto/` + `gen/` before anything else starts** (D cannot compile without it), then **A, B, C, D run in parallel** against the signatures above - changing one means amending this file and every fake. The integrator owns all wiring, resolves the `go mod tidy` conflicts (only `go-redis` + OTel should appear), runs the bench matrix and writes the numbers into the README. One Conventional commit per workstream (`feat(algo):`, `feat(store):`, `feat(server):`, `feat(client):`, `feat(cmd):`).

## Risks
- **Lua numeric precision.** Doubles are exact to 2^53; nanoseconds are not. Everything crossing into Lua is integer microseconds, token counts are integer micro-tokens with elapsed clamped to time-to-full, and the differential test in step 6 runs vectors at `now = 2^52` and after a 30-day idle gap specifically to catch a precision regression. Any new script field gets the same treatment.
- **Clock skew between nodes.** See Decision 3: fail-safe for a slow clock, over-admits `skew * rate` for a fast one. Measured against Redis `TIME`, exported, warned at 250 ms, startup refused above 2 s. Sliding-window's two-window arithmetic is the most skew-sensitive (a skewed node can straddle a boundary early) - documented, and part of why GCRA is the default.
- **Hot keys.** One `global_ingress` key at 50k/s is a single Redis slot and a single Go shard. Mitigations in order: raise `burst` so the key tolerates batching; shard the key with an opt-in `{n}` suffix and divide the limit (`key: "global#{shard}"`); or accept the local-only path for global policies. The bench must include a 1-key run so the honest hot-key ceiling is published next to the 10k-key number.
- **Cluster hash tags.** The all-or-nothing script requires every key in a call to share a slot; the tag is `{t=<tenant>|s=<subject>}`. A policy whose template drops the subject (e.g. `{tenant}` only) breaks that invariant - `policy.Validate` **rejects at load time** any tenant whose matching policies do not share one tag, rather than failing at runtime with `CROSSSLOT`.
- **Fail-open vs fail-closed default.** Defaulting to `local` (not fail-open) turns a Redis outage into per-node limiting, i.e. over-admission bounded by node count rather than an unlimited door. That is the right default for abuse control; an availability-first deployment sets `FALLBACK_MODE=fail_open` and gets `rl_fallback_active` on the dashboard with an alert rule.
- **Script cache invalidation.** A Redis restart or `SCRIPT FLUSH` empties the cache; `redis.Script.Run` recovers via `NOSCRIPT` -> `EVAL`, but 50k/s all reloading at once is a thundering herd. The backend wraps the reload in a dependency-free singleflight (mutex + generation counter) and counts `rl_redis_script_loads_total`; a spike there is the signal.
- **Label cardinality.** `tenant` and `policy` are bounded by the config file; `subject` never becomes a label. The admin API must reject policy names outside `[a-z0-9._-]{1,32}` so a metric name cannot be injected.
- **Windows has no SIGHUP.** Reload is mtime polling + the admin API; `SIGHUP` is registered only in a `//go:build !windows` file. No plan step may depend on `kill -HUP` on this box.
- **`-race` needs a C compiler on Windows.** If gcc is absent, run `task test:fast` and **say so explicitly** rather than claiming a race-clean run.

## Scaffold deviations (step 1, integrator)
- **Proto names bent to `buf lint` STANDARD.** `RPC_RESPONSE_STANDARD_NAME` / `RPC_REQUEST_RESPONSE_UNIQUE` forbid the sketch's shared `PolicyMsg` and `google.protobuf.Empty` returns, so every RPC has its own `<Rpc>Request`/`<Rpc>Response` (`GetPolicyResponse{policy}`, `UpsertPolicyResponse{policy, version}`, `DeletePolicyResponse{version}`, `ResetKeyResponse{removed}`). `Policy`, `Match`, `TenantPolicies` and an `Algorithm` enum (numbered like `limit.Algorithm`) were added for the admin RPCs; `Descriptor` keeps the Envoy name. `CheckResponse` fields are the RateLimit-* header values verbatim (mapping documented on the message).
- **`limit` additions:** `AlgorithmUnspecified = 0` (so the enum maps 1:1 to the proto and a zero `Params` is invalid), `Algorithm.String`/`ParseAlgorithm` (YAML spellings), `NewParams(algo, rate, period, burst)` computing `EmissionUS`, `Backend*` name constants, `HashTag`. `Strictest`, `Key` and `NewParams` are implemented, not stubbed (fully specified, ten lines each); WS A owns their tests.
- **`policy`:** the YAML schema types, strict `Parse` (yaml.v3 `KnownFields`), `Load` and the numeric/name half of `Validate` are real. Two rules are still WS A's: key-template placeholder resolution and the shared-hash-tag invariant. `Policy.Enabled` is `*bool` (nil = true) so YAML omission does not disable a policy; `Registry.Delete` was added for `DeletePolicy`; `policy.Write` renders YAML for the file rewrite.
- **Extra deps:** `golang.org/x/sync` (errgroup for the teardown ladder), `go.opentelemetry.io/otel/{metric,trace}` pinned explicitly; `gopkg.in/yaml.v3` for the policy file (stdlib has no YAML; the plan's "go-redis only" meant Redis clients). No `go mod tidy` yet, so the requires read `// indirect` until integration.
- **`internal/server` constructor shapes** (`NewCheckServer(store, reg, snap, log, Config)`, `NewAdminServer(reg, store, snap, log)`, `ToStatus`, `DecisionToProto`) and `httpapi.New(...)` are the integrator's proposal - WS C may change them with `cmd/ratelimitd`. The HTTP JSON body uses integer milliseconds for the two durations.
- **`admin.Snapshot` method set** (`RecordDecision`, `RecordBackendError`, `SetFallbackActive`, `AddScriptLoad`, `SetPolicyVersion`, `AddPolicyReloadError`, `SetClockSkew`, `SetLocalKeys`, `AddLocalEvicted`, `Counters`) is fixed now because WS B's constructors take `*admin.Snapshot`; the unlabelled atomics work, the labelled series are C's.
- **Teardown is three logged stages** (`teardown N/3`), not jobrun's six: stop accepting -> cancel background + wait -> close Redis + flush OTel. Compose Redis runs with `appendonly yes` so a container recycle keeps state but flushes the script cache (the NOSCRIPT test).
- **Taskfile** uses `redis:up`/`redis:down` (not `db:*`); `bench` is a placeholder that calls `ratelimitctl bench` until step 11.

## Open questions for the owner
1. Default algorithm: GCRA (smooth, exact `Retry-After`, one integer of state) vs token bucket (more intuitive burst semantics). The plan defaults to GCRA via `defaults.algorithm`; confirm.
2. Should `Check` be reserving (the Envoy RLS model, quota consumed on ask), or should there be an explicit `Report`/refund RPC so a caller can return unused cost after a failed upstream call? v1 assumes consume-on-ask, with `ResetKey` as the only escape.
3. Multi-tenant auth on the gRPC surface: v1 trusts the caller's `tenant` field and binds to the host only. Is a per-tenant API-key interceptor in scope for v1, or a P16 hardening item?
4. Should `pkg/ratelimit` be its own module so external users do not pull the daemon's `go-redis` dependency? Cheap now (a second `go.mod` under `pkg/ratelimit`), painful later.

## Lane deviations (recorded at integration, 2026-09-19; all committed and green)
- **A** key layout `rl:{t=<tenant>|s=<TAG>}:p=<policy>[:extras]`: TAG is the first `{subject}`/descriptor placeholder,
  other placeholders are appended as extras, `{tenant}` is a separator only. `Validate` rejects co-matching policies
  with different tags (so `policies.example.yaml` moved to `{subject}`-tagged keys and `global_ingress` is a
  per-subject ceiling). Globs: `path.Match` syntax with `*` spanning `/`. `Registry.ReloadErrors()/Reloads()` added;
  `cmd/ratelimitd` mirrors them into the Snapshot.
- **B** ARGV is 9 per key (`algo, now_us, emission_us, burst, cost, ttl_slack_ms, rate, period_us, nonce`) + a trailing
  commit flag; `common.lua` is a fifth file (first in the concatenation). `local.Sweep(now)` exported, sweeper only
  deletes an idle key whose state has also expired. `redisx.ApplyMany` pipelines one EVALSHA per subject; the
  integrator added the optional `limit.BatchStore` port, `fallback.ApplyMany` and made `server.DecideBatch` use it
  (8 round trips -> 1). Single-key calls skip the dry-run pass in `dispatch.lua`. go-redis `DialerRetries: 1`.
- **C** `NewCheckServer`/`httpapi.New` take a `server.Resolver` and `NewAdminServer` a `server.PolicyRegistry`
  interface (`*policy.Registry` satisfies both). `ErrNoPolicy` -> `FAILED_PRECONDITION` / HTTP 422. Request cost
  multiplies the policy's route cost. `admin.Deps.Breaker`, `AdminServer.SetBuildInfo` added.
- **D** `Result` gained `Backend`/`Degraded`; `WithClock`, `WithDialOptions`, `WithMWLogger`, `DeniedError`, `Problem`
  added. `bench.Config` gained `Batch`, `Timeout`, `Route`, `Method`; `ExpectedAllowedRatio` takes rate per second.
- **INT** `/readyz` is 503 only when Redis is down and `FALLBACK_MODE=fail_closed` (the fallback store answers every
  failed primary call from the degraded path before the breaker trips). Bench matrix: `policies.bench.yaml`
  (one policy) for the headline rows, the example file for the two-policy row; measured 43.6k/s on the Redis path
  vs a 47.4k/s in-container `redis-benchmark` ceiling for the script, 60k/s on the local path.

## Hardening (2026-09-19)
P16 pass on `main` (baseline `37a7f2a`). Gate at the end: `go build`, `go vet`, `golangci-lint` (0 issues, gosec on),
`go test -race -count=3 ./...`, `-tags integration` against redis:7 on 6380, `go test ./...` inside `golang:1.27` (Linux).
- **Vulns.** `govulncheck ./...`: no vulnerabilities, reachable or not. `go mod tidy` was overdue (direct deps marked
  indirect, `otelgrpc` unused, `kr/text` missing from go.sum); no version changed.
- **Security fixes.** (1) The AdminService (policy CRUD, ResetKey, Stats) was registered on the data-plane listener
  with no auth although its comment claimed the admin listener: new `ADMIN_TOKEN` (>= 16 bytes) gates
  `/ratelimit.v1.AdminService/` behind `authorization: Bearer <token>`, SHA-256 + `subtle.ConstantTimeCompare`, never
  logged or echoed; unset = startup warning (`TestAdminAuth`). (2) `limit.NewParams` bounds every product an algorithm
  forms below `MaxExact = 2^53` (gcra burst*emission, bucket burst*1e6 and emission*1e6, sliding window 2*rate*period):
  past it the Lua doubles diverged silently from the Go reference. (3) `globMatch` matched bytes where `path.Match`
  (the documented syntax) matches runes; found by `FuzzGlobMatch`, minimized input under `testdata/fuzz/`.
  Accepted as is: HTTP body cap 64 KiB + `DisallowUnknownFields` + single-object check; gRPC default 4 MiB receive cap
  with BatchCheck <= 64, descriptors <= 16, strings <= 256 printable ASCII (fuzzed); metric labels only from
  `NameRE` names or constants, escaped and capped at 4096 series (fuzzed); `ReadHeaderTimeout` on both `http.Server`s;
  policy file written 0600 via `CreateTemp` + rename; no outbound URL takes user input (OTel endpoint is operator env);
  `ToStatus` maps unknown errors to `internal error`. Data-plane `tenant` is still trusted (loopback or a fronting
  proxy; README "Not done").
- **Fuzz** (60 s each, execs): policy `FuzzParse` 2.8M (YAML -> Validate -> compile -> Resolve -> Write -> Parse fixed
  point), `FuzzTemplate` 2.9M, `FuzzGlobMatch` 7.0M (vs `path.Match`; 1 crasher, fixed), `FuzzListRule` 6.0M,
  `FuzzResolve` 4.4M (key shape, one hash tag per call); redisx `FuzzPackUnpack` 1.9M; server `FuzzInputValidate`
  0.8M, `FuzzPolicyRoundTrip` 1.1M, `FuzzDecisionToProto` 2.3M; httpapi `FuzzServeCheck` 2.1M, `FuzzToInput` 0.9M;
  admin `FuzzMetricsLabels` 0.8M, `FuzzWriteEscaped` 3.5M; limit `FuzzNewParams` 11.3M, `FuzzStrictest` 6.6M;
  existing `FuzzStep` x3 (gcra 9.4M, tokenbucket 9.9M, slidingwindow 6.5M). Seed corpora live in the tests.
- **Coverage** (unit, no Redis; before -> after): limit 100 -> 100, algo/gcra 97.7, tokenbucket 98.1, slidingwindow
  94.5 (unchanged), policy 89.9 -> 92.3, store/local 99.4, store/redisx 23.8 -> 84.4 (closed-port transport tests;
  the success paths stay behind `-tags integration`), store/fallback 82.4 -> 96.9 (`ApplyMany`, the BatchCheck path,
  was at 0 %), server 90.2 -> 89.8 (auth.go added), httpapi 90.1, admin 98.0 -> 98.3. Non-core: bench 91.6, config
  98.2, obs 52.5 (needs a collector), interceptor 0 (exercised through server/httpapi harnesses, no own tests).
- **Flakes found by `-count=3`.** The bench timing tests asserted the host's idle CPU: they now assert the pacer's
  contract (offered = sent + missed*batch, offered within 2 % of rps, never over-driven) unconditionally and the
  achieved-within-10 % bound only when a no-op round trip stayed under 20 ms at p99. The sweep then exposed a real
  pacer bug: ticks dropped across a window edge were lost (offered 2-5 % short under starvation); fixed in
  `loop`/`scheduledIn`. Achieved/latency/ratio columns in the README are unaffected; the missed-ticks column can differ
  by at most one tick per worker per edge, so the matrix was not re-run.
- **CI** green at baseline and after the push (see the run link in the pass report); `.gitattributes` marks golden files,
  fuzz corpora and Lua sources byte-exact. **goreleaser**: not applicable (ratelimitd is not on the CLI release list).

## Control-plane listener (2026-09-22)
The hardening pass gated the AdminService with `ADMIN_TOKEN` but left it on the data-plane listener, so policy CRUD,
ResetKey and Stats were one leaked or unset token away from every data-plane caller. Decision: the AdminService moves
to its own gRPC server on **`ADMIN_GRPC_ADDR`, default `127.0.0.1:50054`** (next free port after gRPC 50053; nothing
else on the box uses it). `ADMIN_TOKEN` stays on the control plane as defence in depth; `config.Validate` rejects
`ADMIN_GRPC_ADDR == GRPC_ADDR`, and an unset token is an info line on loopback, a warning otherwise.
- `cmd/ratelimitd`: `newDataPlane` (RateLimitService + health + reflection, no admin gate needed) and
  `newControlPlane` (AdminService behind `UnaryAdminAuth`/`StreamAdminAuth` + health + reflection) share one
  interceptor chain (`serverOptions`). Each plane has its own health server naming only its own service. Teardown
  stage 1 flips both health servers, then `GracefulStop`s both gRPC servers concurrently on the one
  `SHUTDOWN_TIMEOUT` budget (hard `Stop` of both on expiry); stages 2 and 3 are unchanged.
- `ratelimitctl`: `-admin-addr` (env `ADMIN_GRPC_ADDR`, default `127.0.0.1:50054`) for `policy`/`reset`/`stats`;
  the token is only sent there. `health -admin` probes the control plane. Breaking for anyone who pointed `-addr` at
  the daemon for admin commands.
- Container: `ADMIN_GRPC_ADDR=:50054` (like `ADMIN_ADDR=:9130`, loopback inside a container is unreachable) and
  `EXPOSE 50054`; publish it only with `ADMIN_TOKEN` set. `task docker:run` still publishes 50053/8093 only.
- Tests: `cmd/ratelimitd` `TestPlaneSeparation` builds both planes with the production constructors on real
  loopback listeners and pins: every admin RPC on the data plane is `Unimplemented` with or without a valid token;
  the control plane refuses without the token and serves with it; `Check` is `Unimplemented` on the control plane;
  each health server knows only its own service. The `internal/server` bufconn harness mirrors the split.
