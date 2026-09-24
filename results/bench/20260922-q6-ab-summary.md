# Q6 A/B summary (loaded box)

Generated from the `*-ab-{before,after}-r{1..6}.md` and `*-shared-r{1..6}.md` files next to this one by
an aggregation script: medians of achieved decisions/s. before = 23a2938 (dry-run + commit, EVALSHA),
after = single-pass dispatcher as a Redis Function + lease layer. Runs interleaved ABAB, 6 rounds,
15 s after 3 s warmup, 60k/s offered, 10k keys, batch 8, other agents loading the box.

| row | before achieved/s (6 runs) | before median | after achieved/s (6 runs) | after median | delta | p99 before | p99 after |
|---|---|---:|---|---:|---:|---|---|
| grpc-redis | 47205, 41744, 44230, 43609, 44652, 44223 | 44226 | 41470, 45341, 47874, 48069, 47003, 43514 | 46172 | +4.4% | 5.31ms, 6.58ms, 4.98ms, 5.32ms, 4.86ms, 5.28ms | 7.02ms, 5.86ms, 4.76ms, 4.62ms, 5.05ms, 6.16ms |
| grpc-redis-2pol | 24900, 23109, 22583, 22459, 22250, 21471 | 22521 | 26969, 28822, 29157, 28280, 28333, 26338 | 28306 | +25.7% | 7.54ms, 8.07ms, 8.68ms, 9.98ms, 10.45ms, 10.07ms | 8.10ms, 7.07ms, 6.91ms, 8.95ms, 8.12ms, 9.72ms |
| grpc-redis-4pol | 31157*, 13343, 13053, 13317, 13434, 12755 | 13330 | 16356, 16440, 16933, 16433, 16444, 15503 | 16436 | +23.3% | 13.14ms, 13.10ms, 14.16ms, 13.42ms, 13.08ms, 14.60ms | 11.29ms, 11.15ms, 10.77ms, 11.14ms, 11.20ms, 13.43ms |

`*` = run had degraded decisions (breaker opened under load); kept in the median, not dropped.

Shared counter cost (after binary only, 1 policy vs 1 policy + a shared per-tenant counter, interleaved per round):

| row | achieved/s (6 runs) | median | p99 |
|---|---|---:|---|
| grpc-redis | 46811, 41473, 42763, 44955, 47437, 48211 | 45883 | 5.22ms, 6.17ms, 6.58ms, 5.68ms, 4.79ms, 4.51ms |
| grpc-redis-shared | 42732, 38190, 43365, 41871, 43387, 43497 | 43048 (-6.2%) | 6.44ms, 8.01ms, 5.56ms, 6.45ms, 5.85ms, 5.57ms |

## Go micro A/B (BenchmarkApplyPolicies, -tags integration)

before/after test binaries run alternately, 6 rounds, 2026-09-24 (loaded box). `redis-us/call` is Redis-side
script time per call (INFO commandstats).

```
goos: windows
goarch: amd64
pkg: github.com/crowbett/ratelimitd/internal/store/redisx
cpu: AMD Ryzen 7 3700X 8-Core Processor             
                        │  before.txt   │              after.txt              │
                        │    sec/op     │    sec/op     vs base               │
ApplyPolicies/gcra1-16     53.86µ ± 31%   49.27µ ± 14%        ~ (p=0.093 n=6)
ApplyPolicies/gcra2-16     67.95µ ± 20%   60.33µ ± 31%  -11.22% (p=0.041 n=6)
ApplyPolicies/gcra4-16     95.77µ ± 33%   85.70µ ±  7%  -10.51% (p=0.009 n=6)
ApplyPolicies/mixed4-16   104.96µ ± 39%   99.26µ ± 26%        ~ (p=0.240 n=6)
geomean                    77.88µ         70.91µ         -8.95%

                        │  before.txt   │              after.txt               │
                        │ redis-us/call │ redis-us/call  vs base               │
ApplyPolicies/gcra1-16      21.12 ± 22%     18.52 ± 11%  -12.33% (p=0.004 n=6)
ApplyPolicies/gcra2-16      34.95 ± 13%     28.21 ± 24%  -19.27% (p=0.015 n=6)
ApplyPolicies/gcra4-16      56.92 ± 27%     47.28 ±  6%  -16.93% (p=0.002 n=6)
ApplyPolicies/mixed4-16     65.19 ± 33%     57.57 ± 17%  -11.68% (p=0.026 n=6)
geomean                     40.68           34.54        -15.11%
```
