package local

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/crowbett/ratelimitd/internal/limit"
)

var t0 = time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC)

func params(tb testing.TB, algo limit.Algorithm, rate int64, period time.Duration, burst int64) limit.Params {
	tb.Helper()
	p, err := limit.NewParams(algo, rate, period, burst)
	if err != nil {
		tb.Fatalf("NewParams: %v", err)
	}
	return p
}

func req(key string, p limit.Params) limit.Req {
	return limit.Req{Key: key, Params: p, Cost: 1, Policy: "p"}
}

func apply1(tb testing.TB, b *Backend, r limit.Req, now time.Time) limit.Decision {
	tb.Helper()
	ds, err := b.Apply(context.Background(), []limit.Req{r}, now)
	if err != nil {
		tb.Fatalf("Apply: %v", err)
	}
	if len(ds) != 1 {
		tb.Fatalf("Apply returned %d decisions, want 1", len(ds))
	}
	return ds[0]
}

func TestNewDefaults(t *testing.T) {
	t.Parallel()
	cfg := New(Config{}).Config()
	if cfg.Shards != 256 || cfg.IdleTTL != 10*time.Minute || cfg.SweepEvery != time.Minute || cfg.MaxKeys != 1_000_000 {
		t.Fatalf("defaults = %+v", cfg)
	}
	if got := New(Config{Shards: 100}).Config().Shards; got != 128 {
		t.Fatalf("Shards 100 rounded to %d, want 128", got)
	}
	if got := New(Config{Shards: 1 << 20}).Config().Shards; got != maxShards {
		t.Fatalf("Shards 2^20 capped to %d, want %d", got, maxShards)
	}
}

func TestShardDistribution(t *testing.T) {
	t.Parallel()
	b := New(Config{Shards: 256})
	const keys = 20_000
	for i := range keys {
		idx := b.core.index(limit.Key("acme", fmt.Sprintf("subject-%d", i), "api"))
		b.core.shards[idx].m[strconv.Itoa(i)] = entry{}
	}
	mean := keys / 256
	lo, hi := mean, mean
	for i := range b.core.shards {
		n := len(b.core.shards[i].m)
		lo, hi = min(lo, n), max(hi, n)
	}
	if lo < mean/2 || hi > mean*2 {
		t.Fatalf("shard load min %d max %d, mean %d: distribution too skewed", lo, hi, mean)
	}
}

func TestApplyDecisions(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	for _, algo := range []limit.Algorithm{limit.GCRA, limit.TokenBucket, limit.SlidingWindow, limit.SlidingWindowExact} {
		t.Run(algo.String(), func(t *testing.T) {
			t.Parallel()
			b := New(Config{Shards: 4})
			p := params(t, algo, 10, time.Second, 10)
			r := req("k", p)
			// A fresh key admits exactly its capacity (10 for every algorithm
			// here: burst 10, and rate 10 for the window variants).
			allowed := 0
			for range 20 {
				if apply1(t, b, r, t0).Allowed {
					allowed++
				}
			}
			if allowed != 10 {
				t.Fatalf("fresh key admitted %d, want 10", allowed)
			}
			d := apply1(t, b, r, t0)
			if d.Allowed || d.RetryAfter <= 0 || d.Backend != limit.BackendLocal || d.Policy != "p" || d.Limit != 10 {
				t.Fatalf("denied decision = %+v", d)
			}
			// One second later the capacity is back (window variants roll
			// over gradually; a full period later at least one is admitted).
			if d := apply1(t, b, r, t0.Add(2*time.Second)); !d.Allowed {
				t.Fatalf("after 2s: %+v", d)
			}
			if st := b.Stats(); st.Keys != 1 {
				t.Fatalf("Keys = %d, want 1", st.Keys)
			}
			if err := b.Reset(ctx, []string{"k", "missing"}); err != nil {
				t.Fatal(err)
			}
			if st := b.Stats(); st.Keys != 0 {
				t.Fatalf("Keys after Reset = %d, want 0", st.Keys)
			}
		})
	}
}

func TestPeekDoesNotMutate(t *testing.T) {
	t.Parallel()
	b := New(Config{})
	r := req("k", params(t, limit.GCRA, 10, time.Second, 3))
	for range 3 {
		ds, err := b.Peek(context.Background(), []limit.Req{r}, t0)
		if err != nil || !ds[0].Allowed || ds[0].Remaining != 2 {
			t.Fatalf("Peek = %+v, %v", ds, err)
		}
	}
	if st := b.Stats(); st.Keys != 0 || st.Applied != 0 {
		t.Fatalf("Peek touched state: %+v", st)
	}
}

func TestInvalidParams(t *testing.T) {
	t.Parallel()
	b := New(Config{})
	_, err := b.Apply(context.Background(), []limit.Req{{Key: "k", Params: limit.Params{Algo: limit.GCRA}}}, t0)
	if !errors.Is(err, limit.ErrInvalidParams) {
		t.Fatalf("err = %v, want ErrInvalidParams", err)
	}
	ds, err := b.Apply(context.Background(), nil, t0)
	if err != nil || len(ds) != 0 {
		t.Fatalf("empty Apply = %v, %v", ds, err)
	}
}

func TestAllOrNothing(t *testing.T) {
	t.Parallel()
	b := New(Config{})
	wide := req("rl:{t=a|s=x}:p=wide", params(t, limit.GCRA, 100, time.Second, 100))
	narrow := req("rl:{t=a|s=x}:p=narrow", params(t, limit.TokenBucket, 1, time.Second, 1))
	ctx := context.Background()

	ds, err := b.Apply(ctx, []limit.Req{wide, narrow}, t0)
	if err != nil || !ds[0].Allowed || !ds[1].Allowed {
		t.Fatalf("first: %+v %v", ds, err)
	}
	ds, err = b.Apply(ctx, []limit.Req{wide, narrow}, t0)
	if err != nil {
		t.Fatal(err)
	}
	if !ds[0].Allowed || ds[1].Allowed {
		t.Fatalf("second: wide %+v narrow %+v", ds[0], ds[1])
	}
	if ds[0].Remaining != 98 {
		t.Fatalf("dry-run view of wide: Remaining %d, want 98", ds[0].Remaining)
	}
	// The denial left wide untouched: it still has 99 (one spent by the
	// first call), so a lone Apply reports 98 remaining after spending one.
	if d := apply1(t, b, wide, t0); d.Remaining != 98 {
		t.Fatalf("wide after denied multi-key call: Remaining %d, want 98", d.Remaining)
	}
	if st := b.Stats(); st.Keys != 2 {
		t.Fatalf("Keys = %d, want 2", st.Keys)
	}
}

func TestManyKeysOneCall(t *testing.T) {
	t.Parallel()
	b := New(Config{Shards: 8})
	p := params(t, limit.GCRA, 100, time.Second, 5)
	reqs := make([]limit.Req, 20) // > stackReqs, spans every shard
	for i := range reqs {
		reqs[i] = req(fmt.Sprintf("k%d", i), p)
	}
	ds, err := b.Apply(context.Background(), reqs, t0)
	if err != nil || len(ds) != 20 {
		t.Fatalf("Apply: %d %v", len(ds), err)
	}
	for i, d := range ds {
		if !d.Allowed || d.Remaining != 4 {
			t.Fatalf("ds[%d] = %+v", i, d)
		}
	}
	if st := b.Stats(); st.Keys != 20 {
		t.Fatalf("Keys = %d, want 20", st.Keys)
	}
}

func TestScale(t *testing.T) {
	t.Parallel()
	b := New(Config{})
	p := params(t, limit.GCRA, 100, time.Second, 10)
	tests := []struct {
		div      int64
		wantCap  int
		wantName string
	}{
		{1, 10, limit.BackendLocal},
		{4, 2, limit.BackendDegraded},
		{1000, 1, limit.BackendDegraded},
	}
	for _, tt := range tests {
		t.Run(strconv.FormatInt(tt.div, 10), func(t *testing.T) {
			t.Parallel()
			v := b.Scale(tt.div)
			if v.Name() != tt.wantName || v.Divisor() != max(tt.div, 1) {
				t.Fatalf("Name %q Divisor %d", v.Name(), v.Divisor())
			}
			r := req(fmt.Sprintf("scale-%d", tt.div), p)
			allowed := 0
			for range 20 {
				d := apply1(t, v, r, t0)
				if d.Backend != tt.wantName || d.Limit != tt.wantCap {
					t.Fatalf("decision = %+v", d)
				}
				if d.Allowed {
					allowed++
				}
			}
			if allowed != tt.wantCap {
				t.Fatalf("admitted %d, want %d", allowed, tt.wantCap)
			}
		})
	}
	if b.Scale(0) != b || b.Scale(1) != b {
		t.Fatal("Scale(<=1) must return the primary")
	}
	// The views share state: a key spent through the scaled view is spent
	// for the primary too (checked once the parallel subtests are done).
	t.Cleanup(func() {
		if st := b.Stats(); st.Keys != 3 {
			t.Errorf("Keys = %d, want 3", st.Keys)
		}
	})
}

func TestSweepIdle(t *testing.T) {
	t.Parallel()
	b := New(Config{Shards: 16, IdleTTL: 10 * time.Minute})
	p := params(t, limit.GCRA, 100, time.Second, 10)
	for i := range 100 {
		apply1(t, b, req(fmt.Sprintf("k%d", i), p), t0)
	}
	// A slow policy whose block outlives IdleTTL must survive the sweep.
	slow := req("slow", params(t, limit.GCRA, 1, time.Hour, 1))
	apply1(t, b, slow, t0)

	b.Sweep(t0.Add(5 * time.Minute))
	if st := b.Stats(); st.Keys != 101 || st.Swept != 0 {
		t.Fatalf("half IdleTTL: %+v", st)
	}
	b.Sweep(t0.Add(10*time.Minute + time.Second))
	st := b.Stats()
	if st.Keys != 1 || st.Swept != 100 || st.Evicted != 0 {
		t.Fatalf("past IdleTTL: %+v", st)
	}
	if d := apply1(t, b, slow, t0.Add(11*time.Minute)); d.Allowed {
		t.Fatalf("still-blocked key was swept: %+v", d)
	}
	b.Sweep(t0.Add(2 * time.Hour))
	if st := b.Stats(); st.Keys != 0 {
		t.Fatalf("after 2h: %+v", st)
	}
}

func TestSweepMaxKeys(t *testing.T) {
	t.Parallel()
	b := New(Config{Shards: 4, MaxKeys: 64, IdleTTL: time.Hour})
	p := params(t, limit.GCRA, 100, time.Second, 10)
	for i := range 200 {
		apply1(t, b, req(fmt.Sprintf("k%d", i), p), t0.Add(time.Duration(i)*time.Second))
	}
	b.Sweep(t0.Add(200 * time.Second))
	st := b.Stats()
	if st.Keys > 64 || st.Evicted != 200-st.Keys || st.Swept != 0 {
		t.Fatalf("after eviction: %+v", st)
	}
	// Survivors are the newest per shard: the oldest key of every shard is gone.
	seen := make(map[int]bool)
	for i := range 200 {
		k := fmt.Sprintf("k%d", i)
		sh := b.core.shardOf(k)
		_, ok := sh.m[k]
		idx := b.core.index(k)
		if !seen[idx] {
			seen[idx] = true
			if ok {
				t.Fatalf("oldest key %s in shard %d survived", k, idx)
			}
		}
	}
	k199 := "k199"
	if _, ok := b.core.shardOf(k199).m[k199]; !ok {
		t.Fatal("newest key was evicted")
	}
}

func TestRunSweeps(t *testing.T) {
	t.Parallel()
	b := New(Config{Shards: 4, IdleTTL: time.Minute, SweepEvery: 5 * time.Millisecond})
	p := params(t, limit.GCRA, 100, time.Second, 10)
	apply1(t, b, req("old", p), t0)
	apply1(t, b, req("new", p), t0.Add(2*time.Minute)) // advances the sweeper clock
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- b.Run(ctx) }()
	deadline := time.Now().Add(2 * time.Second)
	for b.Stats().Swept == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run returned %v", err)
	}
	if st := b.Stats(); st.Swept != 1 || st.Keys != 1 {
		t.Fatalf("sweeper: %+v", st)
	}
}

func TestConcurrentWritersOneKey(t *testing.T) {
	t.Parallel()
	const (
		rate    = 1000
		burst   = 100
		writers = 8
		perG    = 20_000
		tickUS  = 10 // virtual microseconds per op => 1.6 s of virtual time
	)
	b := New(Config{})
	p := params(t, limit.GCRA, rate, time.Second, burst)
	var (
		tick    atomic.Int64
		allowed atomic.Int64
		wg      sync.WaitGroup
	)
	for range writers {
		wg.Go(func() {
			r := req("hot", p)
			for range perG {
				now := t0.Add(time.Duration(tick.Add(1)*tickUS) * time.Microsecond)
				if apply1(t, b, r, now).Allowed {
					allowed.Add(1)
				}
			}
		})
	}
	wg.Wait()
	elapsed := float64(tick.Load()*tickUS) / 1e6
	got := float64(allowed.Load())
	if got > rate*elapsed+burst {
		t.Fatalf("allowed %v > rate*elapsed+burst = %v", got, rate*elapsed+burst)
	}
	if got < rate*elapsed*0.95 {
		t.Fatalf("allowed %v < 0.95*rate*elapsed = %v", got, rate*elapsed*0.95)
	}
}

func BenchmarkApply(b *testing.B) {
	const keys = 10_000
	be := New(Config{})
	p, _ := limit.NewParams(limit.GCRA, 100_000, time.Second, 1000)
	ks := make([]string, keys)
	for i := range ks {
		ks[i] = limit.Key("bench", fmt.Sprintf("s%d", i), "api")
	}
	var next atomic.Int64
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		ctx := context.Background()
		i := int(next.Add(7919)) % keys
		reqs := []limit.Req{{Key: ks[i], Params: p, Cost: 1, Policy: "api"}}
		nowUS := t0.UnixMicro()
		for pb.Next() {
			i++
			if i == keys {
				i = 0
			}
			reqs[0].Key = ks[i]
			nowUS += 3
			if _, err := be.Apply(ctx, reqs, time.UnixMicro(nowUS)); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func TestLease(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 9, 22, 12, 0, 0, 0, time.UTC)
	ctx := context.Background()
	tests := []struct {
		name    string
		algo    limit.Algorithm
		lo      int
		wants   []int // successive leases at the same instant
		granted []int
	}{
		{"gcra full then partial then denied", limit.GCRA, 1, []int{6, 6, 6}, []int{6, 4, 0}},
		{"token bucket", limit.TokenBucket, 2, []int{7, 7, 7}, []int{7, 3, 0}},
		{"sliding window", limit.SlidingWindow, 1, []int{9, 9}, []int{9, 1}},
		{"lo above what is left denies", limit.GCRA, 5, []int{8, 5}, []int{8, 0}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := New(Config{})
			r := limit.Req{Key: "rl:{g=x}:p=x", Params: params(t, tt.algo, 10, time.Second, 10), Cost: tt.lo, Policy: "x", Shared: true}
			for i, w := range tt.wants {
				d, got, err := b.Lease(ctx, r, w, now)
				if err != nil || got != tt.granted[i] || d.Allowed != (got > 0) || d.Backend != limit.BackendLocal {
					t.Fatalf("lease %d: %+v got %d err %v, want %d", i, d, got, err, tt.granted[i])
				}
			}
		})
	}
	x := limit.Req{Key: "k", Params: params(t, limit.SlidingWindowExact, 10, time.Second, 10), Cost: 1, Policy: "x"}
	if _, _, err := New(Config{}).Lease(ctx, x, 3, now); !errors.Is(err, limit.ErrInvalidParams) {
		t.Fatalf("exact lease err = %v", err)
	}
	if d, got, _ := New(Config{}).Scale(2).Lease(ctx, limit.Req{Key: "s", Params: params(t, limit.GCRA, 10, time.Second, 10), Cost: 1, Policy: "x"}, 10, now); got != 5 || d.Backend != limit.BackendDegraded {
		t.Fatalf("scaled lease: %+v got %d, want 5 of burst 10/2", d, got)
	}
}

// TestResetClearsTemplatedVariants: resetting a policy key also drops its
// {route}/{ip} variants (key + ":" + extras, on other shards), and nothing of
// a policy whose name merely starts the same.
func TestResetClearsTemplatedVariants(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	b := New(Config{Shards: 8})
	p := params(t, limit.GCRA, 10, time.Second, 10)
	key := limit.Key("acme", "u1", "api")
	keep := []string{limit.Key("acme", "u1", "api2"), limit.Key("acme", "u2", "api") + ":r=/a"}
	all := append([]string{key, key + ":r=/a", key + ":r=/b:ip=10.0.0.1"}, keep...)
	for _, k := range all {
		apply1(t, b, req(k, p), t0)
	}
	if st := b.Stats(); st.Keys != int64(len(all)) {
		t.Fatalf("Keys = %d, want %d", st.Keys, len(all))
	}
	if err := b.Reset(ctx, []string{key}); err != nil {
		t.Fatal(err)
	}
	if st := b.Stats(); st.Keys != int64(len(keep)) {
		t.Fatalf("Keys after Reset = %d, want %d (the untouched ones)", st.Keys, len(keep))
	}
	for _, k := range keep {
		if d := apply1(t, b, req(k, p), t0); d.Remaining != 8 {
			t.Fatalf("%s lost its state: remaining %d, want 8", k, d.Remaining)
		}
	}
}
