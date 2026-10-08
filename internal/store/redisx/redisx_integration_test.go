//go:build integration

package redisx

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/crowbett/ratelimitd/internal/admin"
	"github.com/crowbett/ratelimitd/internal/algo/gcra"
	"github.com/crowbett/ratelimitd/internal/algo/slidingwindow"
	"github.com/crowbett/ratelimitd/internal/algo/tokenbucket"
	"github.com/crowbett/ratelimitd/internal/limit"
)

// newBackend connects to TEST_REDIS_ADDR (skips when unset) under a unique
// key prefix that is wiped on cleanup. TTLSlack is an hour so synthetic
// clocks never race real expiry; the 1 s read timeout keeps a loaded box
// (parallel builds, -race) from turning a slow reply into a false divergence.
func newBackend(tb testing.TB, snap *admin.Snapshot) *Backend {
	tb.Helper()
	addr := os.Getenv("TEST_REDIS_ADDR")
	if addr == "" {
		tb.Skip("TEST_REDIS_ADDR not set")
	}
	prefix := fmt.Sprintf("test:%d:%d:", os.Getpid(), rand.Int64())
	b, err := New(Config{Addr: addr, PoolSize: 16, KeyPrefix: prefix, TTLSlack: time.Hour, ReadTimeout: time.Second}, nil, snap)
	if err != nil {
		tb.Fatalf("New: %v", err)
	}
	tb.Cleanup(func() {
		ctx := context.Background()
		if keys, err := b.rdb.Keys(ctx, prefix+"*").Result(); err == nil && len(keys) > 0 {
			_ = b.rdb.Del(ctx, keys...).Err()
		}
		_ = b.Close()
	})
	if err := b.Ping(context.Background()); err != nil {
		tb.Fatalf("Ping: %v", err)
	}
	return b
}

func mustParams(tb testing.TB, algo limit.Algorithm, rate int64, period time.Duration, burst int64) limit.Params {
	tb.Helper()
	p, err := limit.NewParams(algo, rate, period, burst)
	if err != nil {
		tb.Fatal(err)
	}
	return p
}

// goKey is the Go-side state of one key in the differential test.
type goKey struct {
	req limit.Req
	g   gcra.State
	tb  tokenbucket.State
	sw  slidingwindow.State
	sx  slidingwindow.ExactState
}

func (k *goKey) step(cost int, nowUS int64) (goKey, limit.Decision) {
	next := *k
	var d limit.Decision
	switch k.req.Params.Algo {
	case limit.GCRA:
		next.g, d = gcra.Step(k.g, k.req.Params, cost, nowUS)
	case limit.TokenBucket:
		next.tb, d = tokenbucket.Step(k.tb, k.req.Params, cost, nowUS)
	case limit.SlidingWindow:
		next.sw, d = slidingwindow.Step(k.sw, k.req.Params, cost, nowUS)
	case limit.SlidingWindowExact:
		next.sx, d = slidingwindow.StepExact(k.sx, k.req.Params, cost, nowUS)
	case limit.AlgorithmUnspecified:
	}
	d.Policy, d.Backend = k.req.Policy, limit.BackendRedis
	return next, d
}

// redisState renders the stored state of k as a string so it can be compared
// with the Go state rendered the same way.
func (k *goKey) redisState(tb testing.TB, b *Backend) string {
	tb.Helper()
	ctx := context.Background()
	key := b.cfg.KeyPrefix + k.req.Key
	switch k.req.Params.Algo {
	case limit.GCRA:
		v, err := b.rdb.Get(ctx, key).Result()
		if errors.Is(err, redis.Nil) {
			return "empty"
		}
		mustRead(tb, err)
		return "tat=" + v
	case limit.TokenBucket:
		v, err := b.rdb.HMGet(ctx, key, "t", "l").Result()
		mustRead(tb, err)
		if v[0] == nil {
			return "empty"
		}
		return fmt.Sprintf("t=%v l=%v", v[0], v[1])
	case limit.SlidingWindow:
		v, err := b.rdb.HMGet(ctx, key, "p", "c", "i").Result()
		mustRead(tb, err)
		if v[0] == nil {
			return "empty"
		}
		return fmt.Sprintf("p=%v c=%v i=%v", v[0], v[1], v[2])
	default:
		zs, err := b.rdb.ZRangeWithScores(ctx, key, 0, -1).Result()
		mustRead(tb, err)
		if len(zs) == 0 {
			return "empty"
		}
		stamps := make([]int64, len(zs))
		for i, z := range zs {
			stamps[i] = int64(z.Score)
		}
		return fmt.Sprintf("stamps=%v", stamps)
	}
}

// mustRead fails the test on a read error: only a missing key is "empty",
// a timeout must never pass for a state divergence (or hide one).
func mustRead(tb testing.TB, err error) {
	tb.Helper()
	if err != nil {
		tb.Fatalf("reading state: %v", err)
	}
}

func (k *goKey) goState() string {
	switch k.req.Params.Algo {
	case limit.GCRA:
		if k.g == 0 {
			return "empty"
		}
		return "tat=" + strconv.FormatInt(int64(k.g), 10)
	case limit.TokenBucket:
		if k.tb == (tokenbucket.State{}) {
			return "empty"
		}
		return fmt.Sprintf("t=%d l=%d", k.tb.Tokens, k.tb.Last)
	case limit.SlidingWindow:
		if k.sw == (slidingwindow.State{}) {
			return "empty"
		}
		return fmt.Sprintf("p=%d c=%d i=%d", k.sw.Prev, k.sw.Cur, k.sw.Index)
	default:
		if len(k.sx.Stamps) == 0 {
			return "empty"
		}
		return fmt.Sprintf("stamps=%v", k.sx.Stamps)
	}
}

// TestDifferential is THE gate: the same random operation sequence through
// the Go Step and through the Lua script must yield identical decisions and
// identical stored state after every operation.
func TestDifferential(t *testing.T) {
	seed := uint64(time.Now().UnixNano())
	if s := os.Getenv("TEST_SEED"); s != "" {
		seed, _ = strconv.ParseUint(s, 10, 64)
	}
	t.Logf("seed %d (TEST_SEED to replay)", seed)

	runs := []struct {
		name  string
		start int64
		ops   int
	}{
		{"realtime", time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC).UnixMicro(), 10_000},
		{"2^52", 1 << 52, 2_000},
	}
	for _, run := range runs {
		t.Run(run.name, func(t *testing.T) {
			b := newBackend(t, nil)
			rng := rand.New(rand.NewPCG(seed, uint64(run.start)))
			tag := limit.HashTag("diff", run.name)
			keys := []*goKey{
				{req: limit.Req{Key: limit.KeyPrefix + tag + ":p=g1", Params: mustParams(t, limit.GCRA, 100, time.Second, 20), Policy: "g1"}},
				{req: limit.Req{Key: limit.KeyPrefix + tag + ":p=g2", Params: mustParams(t, limit.GCRA, 7, 3*time.Second, 3), Policy: "g2"}},
				{req: limit.Req{Key: limit.KeyPrefix + tag + ":p=tb1", Params: mustParams(t, limit.TokenBucket, 50, time.Second, 10), Policy: "tb1"}},
				{req: limit.Req{Key: limit.KeyPrefix + tag + ":p=tb2", Params: mustParams(t, limit.TokenBucket, 3, 700*time.Millisecond, 4), Policy: "tb2"}},
				{req: limit.Req{Key: limit.KeyPrefix + tag + ":p=sw1", Params: mustParams(t, limit.SlidingWindow, 30, 2*time.Second, 30), Policy: "sw1"}},
				{req: limit.Req{Key: limit.KeyPrefix + tag + ":p=sw2", Params: mustParams(t, limit.SlidingWindow, 5, 333*time.Millisecond, 5), Policy: "sw2"}},
				{req: limit.Req{Key: limit.KeyPrefix + tag + ":p=sx1", Params: mustParams(t, limit.SlidingWindowExact, 8, time.Second, 8), Policy: "sx1"}},
			}
			now := run.start
			gapAt := run.ops / 3
			ctx := context.Background()
			for op := range run.ops {
				switch {
				case op == gapAt:
					now += 30 * 24 * int64(time.Hour/time.Microsecond) // the 30-day idle gap
				case rng.IntN(100) < 40:
					now += rng.Int64N(100)
				case rng.IntN(100) < 70:
					now += 1_000 + rng.Int64N(1_000_000)
				default:
					now += 1_000_000 + rng.Int64N(60_000_000)
				}
				picked := []*goKey{keys[rng.IntN(len(keys))]}
				if rng.IntN(10) == 0 { // multi-key, all-or-nothing
					other := keys[rng.IntN(len(keys))]
					if other != picked[0] {
						picked = append(picked, other)
					}
				}
				reqs := make([]limit.Req, len(picked))
				nexts := make([]goKey, len(picked))
				wants := make([]limit.Decision, len(picked))
				all := true
				for i, k := range picked {
					reqs[i] = k.req
					reqs[i].Cost = 1 + rng.IntN(5)
					nexts[i], wants[i] = k.step(reqs[i].Cost, now)
					all = all && wants[i].Allowed
				}
				got, err := b.Apply(ctx, reqs, time.UnixMicro(now))
				if err != nil {
					t.Fatalf("op %d: Apply: %v", op, err)
				}
				for i, k := range picked {
					if got[i] != wants[i] {
						t.Fatalf("op %d key %s cost %d now %d: lua %+v != go %+v (go state before: %s)",
							op, k.req.Policy, reqs[i].Cost, now, got[i], wants[i], k.goState())
					}
					if all {
						*k = nexts[i]
					}
					if gs, rs := k.goState(), k.redisState(t, b); gs != rs {
						t.Fatalf("op %d key %s now %d: state diverged: go %s, redis %s", op, k.req.Policy, now, gs, rs)
					}
				}
			}
		})
	}
}

// TestOneKeyBound: 8 goroutines hammer one key for the test duration at 5x
// the limit; the admitted count must stay inside
// [rate*elapsed*0.95, rate*elapsed + limit] where limit is the capacity the
// decisions report (Burst for GCRA and the bucket, Rate for the windows).
// Repeated per algorithm and at cost > 1. The rate divides 1e6 so
// EmissionUS is exact and the bound is not off by the rounding.
func TestOneKeyBound(t *testing.T) {
	t.Parallel()
	dur := 10 * time.Second
	if testing.Short() {
		dur = 2 * time.Second
	}
	cases := []struct {
		algo limit.Algorithm
		cost int
	}{
		{limit.GCRA, 1},
		{limit.GCRA, 3},
		{limit.TokenBucket, 1},
		{limit.TokenBucket, 3},
		{limit.SlidingWindow, 1},
		{limit.SlidingWindow, 3},
	}
	for _, c := range cases {
		t.Run(fmt.Sprintf("%s/cost%d", c.algo, c.cost), func(t *testing.T) {
			t.Parallel()
			b := newBackend(t, nil)
			const (
				rate    = 500
				burst   = 50
				writers = 8
			)
			p := mustParams(t, c.algo, rate, time.Second, burst)
			req := limit.Req{Key: limit.Key("bound", c.algo.String(), strconv.Itoa(c.cost)), Params: p, Cost: c.cost, Policy: "bound"}
			var (
				allowed  atomic.Int64
				errs     atomic.Int64
				lim      atomic.Int64
				first    atomic.Int64
				last     atomic.Int64
				wg       sync.WaitGroup
				interval = time.Duration(float64(time.Second) * writers * float64(c.cost) / (5 * rate))
			)
			first.Store(1<<62 - 1)
			ctx := context.Background()
			deadline := time.Now().Add(dur)
			for range writers {
				wg.Go(func() {
					for time.Now().Before(deadline) {
						start := time.Now()
						ds, err := b.Apply(ctx, []limit.Req{req}, start)
						us := start.UnixMicro()
						for {
							f := first.Load()
							if us >= f || first.CompareAndSwap(f, us) {
								break
							}
						}
						for {
							l := last.Load()
							if us <= l || last.CompareAndSwap(l, us) {
								break
							}
						}
						switch {
						case err != nil:
							errs.Add(1)
						case ds[0].Allowed:
							allowed.Add(int64(c.cost))
							lim.Store(int64(ds[0].Limit))
						}
						if rest := interval - time.Since(start); rest > 0 {
							time.Sleep(rest)
						}
					}
				})
			}
			wg.Wait()
			elapsed := float64(last.Load()-first.Load()) / 1e6
			got := float64(allowed.Load())
			lo, hi := rate*elapsed*0.95, rate*elapsed+float64(lim.Load())
			t.Logf("elapsed %.2fs allowed %v bounds [%.0f, %.0f] errors %d", elapsed, got, lo, hi, errs.Load())
			if errs.Load() != 0 {
				t.Fatalf("%d errors", errs.Load())
			}
			if got > hi {
				t.Fatalf("allowed %v > rate*elapsed+limit %.1f", got, hi)
			}
			if got < lo {
				t.Fatalf("allowed %v < 0.95*rate*elapsed %.1f", got, lo)
			}
		})
	}
}

func TestAllOrNothing(t *testing.T) {
	b := newBackend(t, nil)
	ctx := context.Background()
	now := time.Now()
	tag := limit.HashTag("aon", "x")
	wide := limit.Req{Key: limit.KeyPrefix + tag + ":p=wide", Params: mustParams(t, limit.GCRA, 100, time.Second, 100), Cost: 1, Policy: "wide"}
	narrow := limit.Req{Key: limit.KeyPrefix + tag + ":p=narrow", Params: mustParams(t, limit.TokenBucket, 1, time.Second, 1), Cost: 1, Policy: "narrow"}

	ds, err := b.Apply(ctx, []limit.Req{wide, narrow}, now)
	if err != nil || !ds[0].Allowed || !ds[1].Allowed {
		t.Fatalf("first: %+v %v", ds, err)
	}
	ds, err = b.Apply(ctx, []limit.Req{wide, narrow}, now)
	if err != nil || !ds[0].Allowed || ds[1].Allowed || ds[0].Remaining != 98 {
		t.Fatalf("second: %+v %v", ds, err)
	}
	// wide was not spent by the denied call: Peek still shows 99 available.
	ds, err = b.Peek(ctx, []limit.Req{wide}, now)
	if err != nil || ds[0].Remaining != 98 || !ds[0].Allowed {
		t.Fatalf("peek: %+v %v", ds, err)
	}
	ds, err = b.Apply(ctx, []limit.Req{wide}, now)
	if err != nil || ds[0].Remaining != 98 {
		t.Fatalf("wide alone: %+v %v", ds, err)
	}
	if err := b.Reset(ctx, []string{wide.Key, narrow.Key, "missing"}); err != nil {
		t.Fatal(err)
	}
	ds, err = b.Peek(ctx, []limit.Req{wide, narrow}, now)
	if err != nil || ds[0].Remaining != 99 || ds[1].Remaining != 0 || !ds[1].Allowed {
		t.Fatalf("after reset: %+v %v", ds, err)
	}
}

// TestNoscriptRecovery: SCRIPT FLUSH under load costs exactly one reload
// and zero errors.
func TestNoscriptRecovery(t *testing.T) {
	snap := admin.NewSnapshot()
	b := newBackend(t, snap)
	if b.ScriptLoads() != 1 || snap.Counters().ScriptLoads != 1 {
		t.Fatalf("loads after New = %d / %d, want 1", b.ScriptLoads(), snap.Counters().ScriptLoads)
	}
	ctx := context.Background()
	p := mustParams(t, limit.GCRA, 1_000_000, time.Second, 1000)
	var (
		errs atomic.Int64
		wg   sync.WaitGroup
	)
	stop := make(chan struct{})
	for g := range 8 {
		wg.Go(func() {
			req := limit.Req{Key: limit.Key("noscript", strconv.Itoa(g), "p"), Params: p, Cost: 1, Policy: "p"}
			for {
				select {
				case <-stop:
					return
				default:
				}
				if _, err := b.Apply(ctx, []limit.Req{req}, time.Now()); err != nil {
					errs.Add(1)
					t.Log(err)
				}
			}
		})
	}
	time.Sleep(200 * time.Millisecond)
	if err := flushCode(ctx, b); err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)
	close(stop)
	wg.Wait()
	if errs.Load() != 0 {
		t.Fatalf("%d errors across the flush", errs.Load())
	}
	if b.ScriptLoads() != 2 || snap.Counters().ScriptLoads != 2 {
		t.Fatalf("loads = %d / %d, want exactly 2 (New + one reload)", b.ScriptLoads(), snap.Counters().ScriptLoads)
	}
	if c := snap.Counters(); c.BackendErrors != 0 {
		t.Fatalf("backend errors = %d", c.BackendErrors)
	}
}

func TestApplyMany(t *testing.T) {
	b := newBackend(t, nil)
	ctx := context.Background()
	p := mustParams(t, limit.GCRA, 10, time.Second, 2)
	groups := [][]limit.Req{
		{{Key: limit.Key("many", "a", "p"), Params: p, Cost: 1, Policy: "p"}},
		{{Key: limit.Key("many", "b", "p"), Params: p, Cost: 3, Policy: "p"}},
		{},
		{{Key: limit.Key("many", "c", "p"), Params: limit.Params{Algo: limit.GCRA}, Cost: 1, Policy: "bad"}},
	}
	out, errs, err := b.ApplyMany(ctx, groups, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if errs[0] != nil || !out[0][0].Allowed || out[0][0].Remaining != 1 {
		t.Fatalf("group 0: %+v %v", out[0], errs[0])
	}
	if errs[1] != nil || out[1][0].Allowed {
		t.Fatalf("group 1: %+v %v", out[1], errs[1])
	}
	if errs[2] != nil || len(out[2]) != 0 {
		t.Fatalf("group 2: %+v %v", out[2], errs[2])
	}
	if !errors.Is(errs[3], limit.ErrInvalidParams) || out[3] != nil {
		t.Fatalf("group 3: %+v %v", out[3], errs[3])
	}
	// Flush then ApplyMany: the pipeline path reloads too.
	if err := flushCode(ctx, b); err != nil {
		t.Fatal(err)
	}
	out, errs, err = b.ApplyMany(ctx, groups[:1], time.Now())
	if err != nil || errs[0] != nil || out[0][0].Remaining != 0 {
		t.Fatalf("after flush: %+v %v %v", out, errs, err)
	}
	if b.ScriptLoads() != 2 {
		t.Fatalf("loads = %d, want 2", b.ScriptLoads())
	}
}

func TestSkewAndErrors(t *testing.T) {
	b := newBackend(t, nil)
	ctx := context.Background()
	skew, err := b.Skew(ctx, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if skew < -time.Second || skew > time.Second {
		t.Fatalf("skew %v against a local container", skew)
	}
	t.Logf("skew %v", skew)

	// Wrong key type: an error reply, not ErrBackendDown.
	p := mustParams(t, limit.TokenBucket, 10, time.Second, 10)
	key := limit.Key("err", "wrongtype", "p")
	if err := b.rdb.Set(ctx, b.cfg.KeyPrefix+key, "x", 0).Err(); err != nil {
		t.Fatal(err)
	}
	_, err = b.Apply(ctx, []limit.Req{{Key: key, Params: p, Cost: 1, Policy: "p"}}, time.Now())
	if err == nil || errors.Is(err, limit.ErrBackendDown) {
		t.Fatalf("WRONGTYPE err = %v", err)
	}

	// A dead endpoint: ErrBackendDown within the timeout.
	snap := admin.NewSnapshot()
	dead, _ := New(Config{Addr: "127.0.0.1:1", ReadTimeout: 50 * time.Millisecond, DialTimeout: 100 * time.Millisecond}, nil, snap)
	defer dead.Close()
	start := time.Now()
	_, err = dead.Apply(ctx, []limit.Req{{Key: key, Params: p, Cost: 1, Policy: "p"}}, time.Now())
	if !errors.Is(err, limit.ErrBackendDown) {
		t.Fatalf("dead endpoint err = %v", err)
	}
	if el := time.Since(start); el > time.Second {
		t.Fatalf("dead endpoint took %v", el)
	}
	if err := dead.Ping(ctx); !errors.Is(err, limit.ErrBackendDown) {
		t.Fatalf("Ping = %v", err)
	}
	if c := snap.Counters(); c.BackendErrors < 2 {
		t.Fatalf("BackendErrors = %d", c.BackendErrors)
	}
}

// randomPolicy draws valid Params for algo: rates, periods and bursts spread
// over four orders of magnitude so refill, rollover and clamp paths all run.
func randomPolicy(tb testing.TB, rng *rand.Rand, algo limit.Algorithm) limit.Params {
	tb.Helper()
	for {
		rate := 1 + rng.Int64N(2000)
		period := time.Duration(1+rng.Int64N(10_000)) * time.Millisecond
		burst := 1 + rng.Int64N(300)
		if algo == limit.SlidingWindow || algo == limit.SlidingWindowExact {
			burst = rate
			if algo == limit.SlidingWindowExact {
				rate = 1 + rng.Int64N(20)
				burst = rate
			}
		}
		if p, err := limit.NewParams(algo, rate, period, burst); err == nil {
			return p
		}
	}
}

// TestDifferentialMulti is the multi-policy gate for the single-pass
// dispatcher: random policy SETS (1-6 keys per call over a pool of randomly
// drawn policies of every algorithm) through the Go reference and through
// one EVALSHA. Decisions must match per key on every call, the call commits
// all-or-nothing (Go commits only when every reference Step allowed), and
// the stored state of every key in the call plus one bystander must equal the
// Go state after every op. Peeks are mixed in and must never mutate.
func TestDifferentialMulti(t *testing.T) {
	seed := uint64(time.Now().UnixNano())
	if s := os.Getenv("TEST_SEED"); s != "" {
		seed, _ = strconv.ParseUint(s, 10, 64)
	}
	t.Logf("seed %d (TEST_SEED to replay)", seed)
	runs := []struct {
		name  string
		start int64
		ops   int
	}{
		{"realtime", time.Date(2026, 9, 22, 12, 0, 0, 0, time.UTC).UnixMicro(), 12_000},
		{"2^52", 1 << 52, 3_000},
	}
	algos := []limit.Algorithm{limit.GCRA, limit.TokenBucket, limit.SlidingWindow, limit.SlidingWindowExact}
	for _, run := range runs {
		for _, mode := range []string{"fcall", "evalsha"} {
			t.Run(run.name+"/"+mode, func(t *testing.T) {
				b := newBackend(t, nil)
				if mode == "evalsha" {
					b.fn.Store(false) // the Redis 6 path: same chunk, EVAL form
				}
				diffMulti(t, b, seed, run.start, run.ops, algos)
			})
		}
	}
}

func diffMulti(t *testing.T, b *Backend, seed uint64, start int64, ops int, algos []limit.Algorithm) {
	t.Helper()
	rng := rand.New(rand.NewPCG(seed, uint64(start)^0x9e3779b97f4a7c15))
	tag := limit.HashTag("multi", strconv.FormatInt(start, 10))
	pool := make([]*goKey, 12)
	for i := range pool {
		a := algos[rng.IntN(len(algos))]
		if i < len(algos) {
			a = algos[i] // every algorithm at least once
		}
		name := fmt.Sprintf("p%d", i)
		pool[i] = &goKey{req: limit.Req{Key: limit.KeyPrefix + tag + ":p=" + name, Params: randomPolicy(t, rng, a), Policy: name}}
	}
	now := start
	ctx := context.Background()
	var calls, multi, denied, peeks int
	for op := range ops {
		switch r := rng.IntN(100); {
		case op == ops/2:
			now += 30 * 24 * int64(time.Hour/time.Microsecond)
		case r < 40:
			now += rng.Int64N(200)
		case r < 85:
			now += 1_000 + rng.Int64N(500_000)
		default:
			now += 500_000 + rng.Int64N(20_000_000)
		}
		n := 1 + rng.IntN(6)
		perm := rng.Perm(len(pool))[:n]
		picked := make([]*goKey, n, n+1)
		reqs := make([]limit.Req, n)
		nexts := make([]goKey, n)
		wants := make([]limit.Decision, n)
		all := true
		for i, pi := range perm {
			picked[i] = pool[pi]
			reqs[i] = pool[pi].req
			reqs[i].Cost = 1 + rng.IntN(4)
			nexts[i], wants[i] = pool[pi].step(reqs[i].Cost, now)
			all = all && wants[i].Allowed
		}
		peek := rng.IntN(20) == 0
		var (
			got []limit.Decision
			err error
		)
		if peek {
			got, err = b.Peek(ctx, reqs, time.UnixMicro(now))
			peeks++
		} else {
			got, err = b.Apply(ctx, reqs, time.UnixMicro(now))
		}
		if err != nil {
			t.Fatalf("op %d: %v", op, err)
		}
		calls++
		if n > 1 {
			multi++
		}
		if !all {
			denied++
		}
		for i, k := range picked {
			if got[i] != wants[i] {
				t.Fatalf("op %d (n=%d peek=%v) key %s %s cost %d now %d: lua %+v != go %+v (go state before: %s)",
					op, n, peek, k.req.Policy, k.req.Params.Algo, reqs[i].Cost, now, got[i], wants[i], k.goState())
			}
		}
		if all && !peek {
			for i, k := range picked {
				*k = nexts[i]
			}
		}
		picked = append(picked, pool[rng.IntN(len(pool))]) // plus one bystander for the state check
		for _, k := range picked {
			if gs, rs := k.goState(), k.redisState(t, b); gs != rs {
				t.Fatalf("op %d (n=%d all=%v peek=%v) key %s now %d: state diverged: go %s, redis %s",
					op, n, all, peek, k.req.Policy, now, gs, rs)
			}
		}
	}
	t.Logf("%d calls: %d multi-policy, %d denied (no key committed), %d peeks", calls, multi, denied, peeks)
	if multi < calls/2 || denied == 0 || denied == calls {
		t.Fatalf("degenerate run: calls %d multi %d denied %d", calls, multi, denied)
	}
}

// flushCode empties both code caches (FUNCTION FLUSH + SCRIPT FLUSH), which
// is what a Redis restart without persistence or an operator flush does.
func flushCode(ctx context.Context, b *Backend) error {
	if err := b.rdb.FunctionFlush(ctx).Err(); err != nil {
		return err
	}
	return b.rdb.ScriptFlush(ctx).Err()
}

// TestDifferentialLease: dispatch.lua mode 'L' against limit.Grant over the
// Go reference Steps, on both code paths: random (lo, want) leases mixed with
// ordinary one-key Applies on the same keys; decision, granted count and
// stored state must match after every op.
func TestDifferentialLease(t *testing.T) {
	seed := uint64(time.Now().UnixNano())
	if s := os.Getenv("TEST_SEED"); s != "" {
		seed, _ = strconv.ParseUint(s, 10, 64)
	}
	t.Logf("seed %d (TEST_SEED to replay)", seed)
	for _, mode := range []string{"fcall", "evalsha"} {
		t.Run(mode, func(t *testing.T) {
			b := newBackend(t, nil)
			if mode == "evalsha" {
				b.fn.Store(false)
			}
			rng := rand.New(rand.NewPCG(seed, 7))
			algos := []limit.Algorithm{limit.GCRA, limit.TokenBucket, limit.SlidingWindow}
			pool := make([]*goKey, 6)
			for i := range pool {
				name := fmt.Sprintf("g%d", i)
				pool[i] = &goKey{req: limit.Req{Key: limit.KeyPrefix + "{g=" + name + "}:p=" + name, Params: randomPolicy(t, rng, algos[i%3]), Policy: name, Shared: true}}
			}
			now := time.Date(2026, 9, 22, 12, 0, 0, 0, time.UTC).UnixMicro()
			ctx := context.Background()
			var grants, partial, denied int
			for op := range 6_000 {
				if rng.IntN(3) == 0 {
					now += rng.Int64N(2_000_000)
				} else {
					now += rng.Int64N(2_000)
				}
				k := pool[rng.IntN(len(pool))]
				lo := 1 + rng.IntN(4)
				if rng.IntN(4) == 0 { // an ordinary Apply on the same key
					next, want := k.step(lo, now)
					req := k.req
					req.Cost = lo
					got, err := b.Apply(ctx, []limit.Req{req}, time.UnixMicro(now))
					if err != nil || got[0] != want {
						t.Fatalf("op %d apply: %+v %v, want %+v", op, got, err, want)
					}
					if want.Allowed {
						*k = next
					}
				} else {
					wantN := lo + rng.IntN(300)
					next, wantD, wantG := limit.Grant(func(c int) (goKey, limit.Decision) { return k.step(c, now) }, lo, wantN)
					req := k.req
					req.Cost = lo
					gotD, gotG, err := b.Lease(ctx, req, wantN, time.UnixMicro(now))
					if err != nil {
						t.Fatalf("op %d lease: %v", op, err)
					}
					if gotD != wantD || gotG != wantG {
						t.Fatalf("op %d lease %s lo %d want %d now %d: lua %+v granted %d != go %+v granted %d (state before %s)",
							op, k.req.Params.Algo, lo, wantN, now, gotD, gotG, wantD, wantG, k.goState())
					}
					switch {
					case wantG == 0:
						denied++
					case wantG < wantN:
						partial++
					default:
						grants++
					}
					if wantG > 0 {
						*k = next
					}
				}
				if gs, rs := k.goState(), k.redisState(t, b); gs != rs {
					t.Fatalf("op %d key %s: state diverged: go %s, redis %s", op, k.req.Policy, gs, rs)
				}
			}
			t.Logf("leases: %d full, %d partial, %d denied", grants, partial, denied)
			if grants == 0 || partial == 0 || denied == 0 {
				t.Fatal("degenerate run: every lease branch must be exercised")
			}
		})
	}
	b := newBackend(t, nil)
	x := limit.Req{Key: "rl:{g=x}:p=x", Params: mustParams(t, limit.SlidingWindowExact, 5, time.Second, 5), Cost: 1, Policy: "x"}
	if _, _, err := b.Lease(context.Background(), x, 3, time.Now()); !errors.Is(err, limit.ErrInvalidParams) {
		t.Fatalf("exact lease err = %v", err)
	}
}

// TestResetVariants: Reset of a policy key also UNLINKs its templated
// variants via SCAN MATCH, including keys whose subject holds glob syntax,
// and leaves a policy that merely shares the name prefix alone.
func TestResetVariants(t *testing.T) {
	b := newBackend(t, admin.NewSnapshot())
	ctx := context.Background()
	now := time.Unix(1_700_000_000, 0)
	p := mustParams(t, limit.GCRA, 100, time.Second, 100)
	key := limit.Key("acme", "u*[1]?", "api")
	keep := limit.Key("acme", "u*[1]?", "api2") + ":r=/a"
	other := limit.Key("acme", "u1", "api") + ":r=/a" // a literal-subject sibling a raw glob would also match
	reqs := []limit.Req{}
	for _, k := range []string{key, key + ":r=/a", key + ":r=/b:ip=10.0.0.1", keep, other} {
		reqs = append(reqs, limit.Req{Key: k, Params: p, Cost: 5, Policy: "api"})
	}
	for _, r := range reqs {
		if _, err := b.Apply(ctx, []limit.Req{r}, now); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.Reset(ctx, []string{key}); err != nil {
		t.Fatal(err)
	}
	fresh := limit.Req{Key: limit.Key("acme", "never-used", "api"), Params: p, Cost: 5, Policy: "api"}
	ds, err := b.Peek(ctx, append(reqs, fresh), now)
	if err != nil {
		t.Fatal(err)
	}
	clean := ds[len(reqs)].Remaining
	for i, r := range reqs {
		reset := i < 3
		if got := ds[i].Remaining; (got == clean) != reset {
			t.Errorf("%s: remaining %d, a never-used key has %d; want reset=%v", r.Key, got, clean, reset)
		}
	}
}
