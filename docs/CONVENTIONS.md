# Conventions specific to this repo

Moved verbatim from CLAUDE.md. See [../CLAUDE.md](../CLAUDE.md) for the short invariant list.

- Ports are frozen: redis 6380, gRPC :50053, HTTP :8093, admin gRPC 127.0.0.1:50054 (AdminService only - never register it
  on the data plane), admin 127.0.0.1:9130 (jobrun has 5434/50052/9110, sysmon 9100, aigw 8090, edgecache 8080).
- **Integer microseconds, never nanoseconds, never floats.** Everything that crosses into an algorithm or a Lua script -
  `now`, TAT, emission interval, token counts (micro-tokens) - is an `int64` of unix microseconds. Lua doubles are exact
  to 2^53; nanos are not. `Params.EmissionUS` is precomputed once at policy load.
- **The caller's clock is authoritative.** `now` is a parameter of every `Step`, `Apply` and Lua call; nothing reads
  `time.Now` or Redis `TIME` inside a decision. Skew is measured (`Skew`, `rl_clock_skew_seconds`), warned at 250 ms and
  refused at startup above `MAX_CLOCK_SKEW`.
- **Single pass, buffered writes.** A subject's keys share the hash tag `{t=<tenant>|s=<subject>}` and go to Redis in ONE
  script call: every key is read once and evaluated against its pre-call state, and the writes happen only if every key
  allowed. A denial never mutates state, in Go or in Lua. The Go `Step` is the reference; `TestDifferential`,
  `TestDifferentialMulti` (random policy sets) and `TestDifferentialLease` must show identical states and decisions on
  both the FCALL and the EVALSHA path. Each `lua/<algo>.lua` is a read/eval/write table; `eval` stays pure.
- **Shared policies never join the subject's call.** `shared: true` keys (`rl:{g=<policy>[|t=<tenant>]}`) live in their
  own slot; `internal/store/lease` takes their units from a per-node pool leased from Redis (`LEASE_SPAN`/`LEASE_TTL`)
  before the subject call and refunds them if the subject call denies. It never over-admits; see PLAN "Shared global counter".
- **Frozen contracts.** The signatures in `docs/PLAN.md` "Interfaces and key types" are the contract between workstreams
  A (limit/algo/policy), B (store/*, testproxy), C (proto/server/httpapi/admin/obs) and D (pkg/ratelimit, bench). Every
  stub body says `stub: <pkg>.<Func>` and its header comment names the owner. Changing a signature means amending the
  plan first and every caller in the same commit. All lanes have landed (no `stub:` bodies remain); the per-lane
  deviations are recorded in `docs/PLAN.md` "Lane deviations". `limit.BatchStore` is the optional pipelining port.
- No goroutine per request or per key; the four background goroutines are the watcher, the prober, the sweeper and the
  skew sampler, each started by `Run(ctx)`/`Watch(ctx, ...)` and stopped in teardown stage 2.
- A denied Check is `CODE_OVER_LIMIT` with an OK gRPC status; only `pkg/ratelimit` maps it to `ErrDenied` / 429.
- Metrics come from one `admin.Snapshot`; `/metrics` text and the OTel instruments must read the same values. `tenant`
  and `policy` are labels (names validated by `policy.NameRE`), the subject never is.
- Schema first: edit `proto/` -> `task gen`; never edit `gen/`. Errors leave gRPC handlers via `server.ToStatus`.
- Unit tests use fakes; anything that needs Redis goes behind `//go:build integration` and `TEST_REDIS_ADDR`.
- **Every number in README.md comes from `results/bench/<ts>.md` (scripts/bench.ps1) or a pasted `-benchmem` line.** Never
  type a figure by hand; rerun the matrix on a quiet box, commit the results file, copy rows from it.
- go-redis: `MaxRetries: -1` AND `DialerRetries: 1` - both, or a timeout re-runs the script / a down Redis stalls
  the probe request 400 ms. The breaker owns retries.
- Windows has no SIGHUP: reload is mtime polling + the admin API. The stop signal is Ctrl-Break; the three `teardown N/3`
  lines must appear in order and the process exits 0.

