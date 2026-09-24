package lease

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
	"github.com/crowbett/ratelimitd/internal/store/local"
)

// fakeInner records what reaches the subject store and answers from a
// local backend (or with a programmed error).
type fakeInner struct {
	*local.Backend
	mu    sync.Mutex
	calls [][]limit.Req
	err   error
}

func (f *fakeInner) Apply(ctx context.Context, reqs []limit.Req, now time.Time) ([]limit.Decision, error) {
	f.mu.Lock()
	f.calls = append(f.calls, append([]limit.Req(nil), reqs...))
	err := f.err
	f.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return f.Backend.Apply(ctx, reqs, now)
}

func (f *fakeInner) nCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

// countingSource wraps a Source, counting calls and optionally failing.
type countingSource struct {
	src   Source
	calls atomic.Int64
	err   error
}

func (c *countingSource) Lease(ctx context.Context, req limit.Req, want int, now time.Time) (limit.Decision, int, error) {
	c.calls.Add(1)
	if c.err != nil {
		return limit.Decision{}, 0, c.err
	}
	return c.src.Lease(ctx, req, want, now)
}

func params(tb testing.TB, algo limit.Algorithm, rate int64, per time.Duration, burst int64) limit.Params {
	tb.Helper()
	p, err := limit.NewParams(algo, rate, per, burst)
	if err != nil {
		tb.Fatal(err)
	}
	return p
}

func sharedReq(tb testing.TB, rate, burst int64) limit.Req {
	tb.Helper()
	return limit.Req{Key: "rl:{g=ingress}:p=ingress", Params: params(tb, limit.GCRA, rate, time.Second, burst), Cost: 1, Policy: "ingress", Shared: true}
}

func subjectReq(tb testing.TB, subject string, rate, burst int64) limit.Req {
	tb.Helper()
	return limit.Req{Key: limit.Key("t", subject, "api"), Params: params(tb, limit.GCRA, rate, time.Second, burst), Cost: 1, Policy: "api"}
}

type rig struct {
	s        *Store
	inner    *fakeInner
	primary  *countingSource
	degraded *countingSource
	open     atomic.Bool
}

func newRig(tb testing.TB, cfg Config) *rig {
	tb.Helper()
	r := &rig{
		inner:    &fakeInner{Backend: local.New(local.Config{})},
		primary:  &countingSource{src: local.New(local.Config{})}, // stands in for Redis: the shared key's home
		degraded: &countingSource{src: local.New(local.Config{}).Scale(2)},
	}
	r.s = New(r.inner, r.primary, r.degraded, r.open.Load, cfg)
	return r
}

func TestChunk(t *testing.T) {
	t.Parallel()
	s := New(nil, nil, nil, nil, Config{Span: 10 * time.Millisecond})
	tests := []struct {
		name string
		p    limit.Params
		cost int
		want int
	}{
		{"200k/s -> 2000 per 10ms", params(t, limit.GCRA, 200_000, time.Second, 20_000), 1, 2000},
		{"capped by burst", params(t, limit.GCRA, 200_000, time.Second, 500), 1, 500},
		{"slow policy still leases one", params(t, limit.TokenBucket, 10, time.Second, 10), 1, 1},
		{"cost above chunk wins", params(t, limit.GCRA, 1000, time.Second, 100), 25, 25},
		{"sliding window capacity is rate", params(t, limit.SlidingWindow, 100, 100*time.Millisecond, 100), 1, 10},
		{"per minute", params(t, limit.GCRA, 60_000, time.Minute, 1000), 1, 10},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := s.chunk(tt.p, tt.cost); got != tt.want {
				t.Fatalf("chunk = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestApply(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 9, 22, 12, 0, 0, 0, time.UTC)
	ctx := context.Background()

	t.Run("no shared req passes straight through", func(t *testing.T) {
		t.Parallel()
		r := newRig(t, Config{})
		ds, err := r.s.Apply(ctx, []limit.Req{subjectReq(t, "a", 10, 10)}, now)
		if err != nil || !ds[0].Allowed || r.inner.nCalls() != 1 || r.primary.calls.Load() != 0 {
			t.Fatalf("ds %+v err %v calls %d leases %d", ds, err, r.inner.nCalls(), r.primary.calls.Load())
		}
	})

	t.Run("shared reqs never reach inner and one lease serves many calls", func(t *testing.T) {
		t.Parallel()
		r := newRig(t, Config{Span: 10 * time.Millisecond})
		sh := sharedReq(t, 100_000, 10_000) // chunk = 1000
		for i := range 500 {
			ds, err := r.s.Apply(ctx, []limit.Req{subjectReq(t, strconv.Itoa(i), 10, 10), sh}, now)
			if err != nil || !ds[0].Allowed || !ds[1].Allowed {
				t.Fatalf("call %d: %+v %v", i, ds, err)
			}
			if ds[1].Remaining != 1000-(i+1) || ds[1].Policy != "ingress" {
				t.Fatalf("call %d shared decision %+v", i, ds[1])
			}
		}
		if r.primary.calls.Load() != 1 {
			t.Fatalf("leases = %d, want 1 for 500 units of a 1000-unit chunk", r.primary.calls.Load())
		}
		for _, c := range r.inner.calls {
			if len(c) != 1 || c[0].Shared {
				t.Fatalf("inner saw %+v", c)
			}
		}
	})

	t.Run("shared denial skips inner and consumes nothing", func(t *testing.T) {
		t.Parallel()
		r := newRig(t, Config{})
		sh := sharedReq(t, 1, 1) // one unit, then empty for a second
		sub := subjectReq(t, "s", 10, 10)
		if ds, err := r.s.Apply(ctx, []limit.Req{sub, sh}, now); err != nil || !ds[1].Allowed {
			t.Fatalf("first: %+v %v", ds, err)
		}
		calls := r.inner.nCalls()
		ds, err := r.s.Apply(ctx, []limit.Req{sub, sh}, now)
		if err != nil || ds[1].Allowed || ds[1].RetryAfter <= 0 {
			t.Fatalf("second: %+v %v", ds, err)
		}
		if ds[0].Backend != BackendSkipped || !ds[0].Allowed || r.inner.nCalls() != calls {
			t.Fatalf("subject req was evaluated: %+v, inner calls %d -> %d", ds[0], calls, r.inner.nCalls())
		}
		if st := limit.Strictest(ds); st.Allowed || st.Policy != "ingress" {
			t.Fatalf("Strictest = %+v", st)
		}
		// The subject key was spent once (the first call), not by the denied one:
		// a Peek of one more unit reports 10-2 = 8 remaining.
		if d, _ := r.inner.Peek(ctx, []limit.Req{sub}, now); d[0].Remaining != 8 {
			t.Fatalf("subject remaining = %d, want 8", d[0].Remaining)
		}
	})

	t.Run("a denied refill backs off instead of asking per request", func(t *testing.T) {
		t.Parallel()
		r := newRig(t, Config{Span: 10 * time.Millisecond})
		sh := sharedReq(t, 1000, 1) // one unit, then one per ms
		if ds, _ := r.s.Apply(ctx, []limit.Req{sh}, now); !ds[0].Allowed {
			t.Fatalf("first: %+v", ds)
		}
		for i := range 100 { // 100 asks inside the backoff: one denied lease, 99 local denials
			ds, err := r.s.Apply(ctx, []limit.Req{sh}, now.Add(time.Duration(i)*50*time.Microsecond))
			if err != nil || ds[0].Allowed || ds[0].RetryAfter <= 0 {
				t.Fatalf("ask %d: %+v %v", i, ds, err)
			}
		}
		if n := r.primary.calls.Load(); n != 2 {
			t.Fatalf("lease calls = %d, want 2 (grant + one denial)", n)
		}
		// After max(RetryAfter, Span) = 10ms the budget accrued meanwhile is
		// leased in one call (capped by burst 1 here).
		ds, _ := r.s.Apply(ctx, []limit.Req{sh}, now.Add(10*time.Millisecond))
		if !ds[0].Allowed || r.primary.calls.Load() != 3 {
			t.Fatalf("after backoff: %+v calls %d", ds, r.primary.calls.Load())
		}
	})

	t.Run("subject denial refunds the shared take", func(t *testing.T) {
		t.Parallel()
		r := newRig(t, Config{Span: 10 * time.Millisecond})
		sh := sharedReq(t, 100_000, 10_000)
		sub := subjectReq(t, "s", 1, 1)
		if _, err := r.s.Apply(ctx, []limit.Req{sub, sh}, now); err != nil {
			t.Fatal(err)
		}
		ds, err := r.s.Apply(ctx, []limit.Req{sub, sh}, now)
		if err != nil || ds[0].Allowed || !ds[1].Allowed {
			t.Fatalf("second: %+v %v", ds, err)
		}
		// 1000 leased, one kept by the first call, the second's refunded.
		ds, _ = r.s.Apply(ctx, []limit.Req{sh}, now)
		if ds[0].Remaining != 1000-2 || r.s.Stats().Refunds != 1 {
			t.Fatalf("after refund %+v, stats %+v", ds[0], r.s.Stats())
		}
	})

	t.Run("inner error refunds and propagates", func(t *testing.T) {
		t.Parallel()
		r := newRig(t, Config{Span: 10 * time.Millisecond})
		sh := sharedReq(t, 100_000, 10_000)
		r.inner.err = errors.New("boom")
		if _, err := r.s.Apply(ctx, []limit.Req{subjectReq(t, "s", 10, 10), sh}, now); err == nil {
			t.Fatal("inner error swallowed")
		}
		r.inner.err = nil
		ds, _ := r.s.Apply(ctx, []limit.Req{sh}, now)
		if ds[0].Remaining != 999 {
			t.Fatalf("remaining %d, want 999 (the failed call's unit refunded)", ds[0].Remaining)
		}
	})

	t.Run("leased units expire after TTL", func(t *testing.T) {
		t.Parallel()
		r := newRig(t, Config{Span: 10 * time.Millisecond, TTL: 50 * time.Millisecond})
		sh := sharedReq(t, 100_000, 10_000)
		_, _ = r.s.Apply(ctx, []limit.Req{sh}, now)
		_, _ = r.s.Apply(ctx, []limit.Req{sh}, now.Add(49*time.Millisecond))
		if r.primary.calls.Load() != 1 {
			t.Fatalf("leases = %d before TTL", r.primary.calls.Load())
		}
		_, _ = r.s.Apply(ctx, []limit.Req{sh}, now.Add(50*time.Millisecond))
		if r.primary.calls.Load() != 2 {
			t.Fatalf("leases = %d after TTL, want a refill", r.primary.calls.Load())
		}
	})
}

func TestDegraded(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 9, 22, 12, 0, 0, 0, time.UTC)
	ctx := context.Background()
	tests := []struct {
		name         string
		open         bool
		primaryErr   error
		wantErr      bool
		wantPrimary  int64
		wantDegraded int64
	}{
		{name: "breaker open: degraded only", open: true, wantDegraded: 1},
		{name: "primary down: degraded", primaryErr: fmt.Errorf("%w: redis timeout", limit.ErrBackendDown), wantPrimary: 1, wantDegraded: 1},
		{name: "script error propagates", primaryErr: errors.New("WRONGTYPE"), wantErr: true, wantPrimary: 1},
		{name: "healthy: primary", wantPrimary: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			r := newRig(t, Config{})
			r.open.Store(tt.open)
			r.primary.err = tt.primaryErr
			ds, err := r.s.Apply(ctx, []limit.Req{sharedReq(t, 1000, 100)}, now)
			if (err != nil) != tt.wantErr {
				t.Fatalf("err = %v", err)
			}
			if r.primary.calls.Load() != tt.wantPrimary || r.degraded.calls.Load() != tt.wantDegraded {
				t.Fatalf("primary %d degraded %d", r.primary.calls.Load(), r.degraded.calls.Load())
			}
			if err == nil && tt.wantDegraded > 0 && (ds[0].Backend != limit.BackendDegraded || !ds[0].Allowed) {
				t.Fatalf("decision %+v", ds[0])
			}
		})
	}
	t.Run("fail open and fail closed sources", func(t *testing.T) {
		t.Parallel()
		sh := sharedReq(t, 1000, 100)
		open := New(&fakeInner{Backend: local.New(local.Config{})}, nil, FailOpen{}, func() bool { return true }, Config{})
		if ds, err := open.Apply(ctx, []limit.Req{sh}, now); err != nil || !ds[0].Allowed {
			t.Fatalf("fail open: %+v %v", ds, err)
		}
		closed := New(&fakeInner{Backend: local.New(local.Config{})}, nil, FailClosed{RetryAfter: time.Second}, func() bool { return true }, Config{})
		if ds, err := closed.Apply(ctx, []limit.Req{sh}, now); err != nil || ds[0].Allowed || ds[0].RetryAfter != time.Second {
			t.Fatalf("fail closed: %+v %v", ds, err)
		}
	})
}

func TestApplyMany(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 9, 22, 12, 0, 0, 0, time.UTC)
	ctx := context.Background()
	r := newRig(t, Config{Span: time.Millisecond})
	sh := sharedReq(t, 1000, 3) // chunk = 1: every take is its own lease; 3 units then empty
	groups := [][]limit.Req{
		{subjectReq(t, "plain", 10, 10)},    // no shared: straight to inner
		{subjectReq(t, "a", 10, 10), sh},    // allowed
		{sh},                                // shared only
		{subjectReq(t, "denied", 1, 1), sh}, // takes the last unit... after the next line spends
		{subjectReq(t, "late", 10, 10), sh}, // shared empty: skipped
		{subjectReq(t, "bad", 10, 10), {Key: "rl:{g=x}:p=x", Params: limit.Params{Algo: limit.SlidingWindowExact, EmissionUS: 1, Rate: 1, Burst: 1, Period: time.Second}, Cost: 1, Policy: "x", Shared: true}},
	}
	// Pre-spend "denied"'s subject key so its group denies on the subject side.
	if _, err := r.inner.Backend.Apply(ctx, []limit.Req{groups[3][0]}, now); err != nil {
		t.Fatal(err)
	}
	out, errs, err := r.s.ApplyMany(ctx, groups, now)
	if err != nil {
		t.Fatal(err)
	}
	if errs[0] != nil || !out[0][0].Allowed {
		t.Fatalf("group 0: %+v %v", out[0], errs[0])
	}
	if errs[1] != nil || !out[1][0].Allowed || !out[1][1].Allowed {
		t.Fatalf("group 1: %+v %v", out[1], errs[1])
	}
	if errs[2] != nil || !out[2][0].Allowed {
		t.Fatalf("group 2: %+v %v", out[2], errs[2])
	}
	if errs[3] != nil || out[3][0].Allowed || !out[3][1].Allowed {
		t.Fatalf("group 3: %+v %v", out[3], errs[3])
	}
	// Group 3's shared unit was refunded to the pool, but chunk=1 leases are
	// spent by the time group 4 asks: gcra burst 3 is exhausted by groups 1-3.
	if errs[4] != nil || out[4][1].Allowed || out[4][0].Backend != BackendSkipped {
		t.Fatalf("group 4: %+v %v", out[4], errs[4])
	}
	if errs[5] == nil || !errors.Is(errs[5], limit.ErrInvalidParams) {
		t.Fatalf("group 5: %+v %v", out[5], errs[5])
	}
}

// TestBoundConcurrent is the correctness claim under concurrency: 4 nodes
// (4 Stores) x 8 goroutines hammer one shared policy at ~4x its rate through
// one budget source (standing in for Redis). Admitted units must stay under
// the algorithm's bound from an empty key, rate*elapsed + burst, and above
// rate*elapsed*0.9 minus what leasing can strand (one chunk per node per
// TTL window).
func TestBoundConcurrent(t *testing.T) {
	t.Parallel()
	dur := 2 * time.Second
	if testing.Short() {
		dur = time.Second
	}
	const (
		rate    = 20_000
		burst   = 2_000
		nodes   = 4
		workers = 8
	)
	home := local.New(local.Config{})
	sh := sharedReq(t, rate, burst)
	stores := make([]*Store, nodes)
	for i := range stores {
		stores[i] = New(&fakeInner{Backend: local.New(local.Config{})}, home, FailClosed{}, nil, Config{Span: 5 * time.Millisecond, TTL: 50 * time.Millisecond})
	}
	var (
		admitted atomic.Int64
		wg       sync.WaitGroup
	)
	ctx := context.Background()
	start := time.Now()
	deadline := start.Add(dur)
	interval := time.Duration(float64(time.Second) * nodes * workers / (4 * rate))
	for n := range nodes {
		for w := range workers {
			wg.Go(func() {
				sub := subjectReq(t, fmt.Sprintf("n%dw%d", n, w), 1_000_000, 1_000_000)
				next := time.Now()
				for now := time.Now(); now.Before(deadline); now = time.Now() {
					ds, err := stores[n].Apply(ctx, []limit.Req{sub, sh}, now)
					if err != nil {
						t.Error(err)
						return
					}
					if limit.Strictest(ds).Allowed {
						admitted.Add(1)
					}
					if next = next.Add(interval); time.Until(next) > 0 {
						time.Sleep(time.Until(next))
					}
				}
			})
		}
	}
	wg.Wait()
	elapsed := time.Since(start).Seconds()
	got := float64(admitted.Load())
	hi := rate*elapsed + burst
	chunk := float64(stores[0].chunk(sh.Params, 1))
	lo := rate*elapsed*0.9 - nodes*chunk
	t.Logf("elapsed %.2fs admitted %.0f bound [%.0f, %.0f] (chunk %.0f)", elapsed, got, lo, hi, chunk)
	if got > hi {
		t.Fatalf("admitted %.0f > rate*elapsed + burst %.0f", got, hi)
	}
	if got < lo {
		t.Fatalf("admitted %.0f < %.0f: leasing stranded too much budget", got, lo)
	}
}

func BenchmarkApplyShared(b *testing.B) {
	ctx := context.Background()
	for _, tc := range []struct {
		name   string
		shared bool
	}{{"subject_only", false}, {"subject+shared", true}} {
		b.Run(tc.name, func(b *testing.B) {
			s := New(&fakeInner{Backend: local.New(local.Config{})}, local.New(local.Config{}), FailClosed{}, nil, Config{})
			sh := sharedReq(b, 1_000_000_000, 1_000_000)
			subs := make([]limit.Req, 1024)
			for i := range subs {
				subs[i] = subjectReq(b, strconv.Itoa(i), 1_000_000, 1_000_000)
			}
			var i atomic.Int64
			b.ReportAllocs()
			b.RunParallel(func(pb *testing.PB) {
				reqs := make([]limit.Req, 0, 2)
				for pb.Next() {
					reqs = append(reqs[:0], subs[i.Add(1)%1024])
					if tc.shared {
						reqs = append(reqs, sh)
					}
					if _, err := s.Apply(ctx, reqs, time.Now()); err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}
