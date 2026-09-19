package slidingwindow

import (
	"context"
	"errors"
	"math"
	"math/rand/v2"
	"slices"
	"testing"
	"time"

	"github.com/crowbett/ratelimitd/internal/limit"
)

func params(tb testing.TB, algo limit.Algorithm, rate int64, period time.Duration) limit.Params {
	tb.Helper()
	p, err := limit.NewParams(algo, rate, period, rate)
	if err != nil {
		tb.Fatalf("NewParams: %v", err)
	}
	return p
}

func TestStepTable(t *testing.T) {
	t.Parallel()
	// 10 per second: window 1_000_000 us.
	p := params(t, limit.SlidingWindow, 10, time.Second)
	const w = int64(1_000_000)
	const idx = int64(1000)
	base := idx * w
	tests := []struct {
		name  string
		state State
		p     limit.Params
		cost  int
		now   int64
		want  limit.Decision
		wantS State
	}{
		{
			name: "empty window first request", state: State{}, p: p, cost: 1, now: base,
			want:  limit.Decision{Allowed: true, Remaining: 9, ResetAfter: 2 * time.Second, Limit: 10},
			wantS: State{Prev: 0, Cur: 1, Index: idx},
		},
		{
			name: "fill current window", state: State{Cur: 7, Index: idx}, p: p, cost: 3, now: base + 500_000,
			want:  limit.Decision{Allowed: true, Remaining: 0, ResetAfter: 1500 * time.Millisecond, Limit: 10},
			wantS: State{Prev: 0, Cur: 10, Index: idx},
		},
		{
			name: "over the limit within the window", state: State{Cur: 10, Index: idx}, p: p, cost: 1, now: base + 500_000,
			want: limit.Decision{
				Allowed: false, Remaining: 0, RetryAfter: 500 * time.Millisecond,
				ResetAfter: 1500 * time.Millisecond, Limit: 10,
			},
			wantS: State{Cur: 10, Index: idx},
		},
		{
			name: "rollover weights the previous window", state: State{Cur: 10, Index: idx}, p: p, cost: 1, now: base + w + 250_000,
			// prev*(1-0.25) = 7.5 + 1 = 8.5 <= 10 -> allowed, remaining floor(10 - 7.5 - 1) = 1
			want:  limit.Decision{Allowed: true, Remaining: 1, ResetAfter: 1750 * time.Millisecond, Limit: 10},
			wantS: State{Prev: 10, Cur: 1, Index: idx + 1},
		},
		{
			name: "rollover denial with decay retry", state: State{Prev: 10, Cur: 1, Index: idx + 1}, p: p, cost: 2, now: base + w + 250_000,
			// 7.5 + 1 + 2 = 10.5 > 10; excess 0.5 event -> prev decays 10 per s -> 50ms
			want: limit.Decision{
				Allowed: false, Remaining: 1, RetryAfter: 50 * time.Millisecond,
				ResetAfter: 1750 * time.Millisecond, Limit: 10,
			},
			wantS: State{Prev: 10, Cur: 1, Index: idx + 1},
		},
		{
			name: "two windows idle empties both", state: State{Prev: 10, Cur: 10, Index: idx}, p: p, cost: 4, now: base + 2*w,
			want:  limit.Decision{Allowed: true, Remaining: 6, ResetAfter: 2 * time.Second, Limit: 10},
			wantS: State{Prev: 0, Cur: 4, Index: idx + 2},
		},
		{
			name: "clock behind is pinned to the window start", state: State{Prev: 4, Cur: 2, Index: idx}, p: p, cost: 1, now: base - 300_000,
			// elapsed pinned to 0: 4 + 2 + 1 = 7 -> remaining 3
			want:  limit.Decision{Allowed: true, Remaining: 3, ResetAfter: 2 * time.Second, Limit: 10},
			wantS: State{Prev: 4, Cur: 3, Index: idx},
		},
		{
			name: "cost above limit denies until boundary", state: State{}, p: p, cost: 11, now: base + 100_000,
			want: limit.Decision{
				Allowed: false, Remaining: 10, RetryAfter: 900 * time.Millisecond, Limit: 10,
			},
			wantS: State{},
		},
		{
			name: "cost zero is a peek", state: State{Prev: 4, Cur: 2, Index: idx}, p: p, cost: 0, now: base + 500_000,
			want:  limit.Decision{Allowed: true, Remaining: 6, ResetAfter: 1500 * time.Millisecond, Limit: 10},
			wantS: State{Prev: 4, Cur: 2, Index: idx},
		},
		{
			name: "invalid params deny", state: State{Cur: 1, Index: idx}, p: limit.Params{}, cost: 1, now: base,
			want: limit.Decision{}, wantS: State{Cur: 1, Index: idx},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			gotS, got := Step(tc.state, tc.p, tc.cost, tc.now)
			if got != tc.want {
				t.Errorf("decision = %+v, want %+v", got, tc.want)
			}
			if gotS != tc.wantS {
				t.Errorf("state = %+v, want %+v", gotS, tc.wantS)
			}
		})
	}
}

func TestStepExactTable(t *testing.T) {
	t.Parallel()
	p := params(t, limit.SlidingWindowExact, 3, time.Second)
	const now = int64(5_000_000)
	tests := []struct {
		name  string
		state ExactState
		cost  int
		now   int64
		want  limit.Decision
		wantS []int64
	}{
		{
			name: "empty admits", state: ExactState{}, cost: 2, now: now,
			want:  limit.Decision{Allowed: true, Remaining: 1, ResetAfter: time.Second, Limit: 3},
			wantS: []int64{now, now},
		},
		{
			name: "full denies with oldest expiry", state: ExactState{Stamps: []int64{now - 800_000, now - 300_000, now - 100_000}}, cost: 1, now: now,
			want: limit.Decision{
				Allowed: false, Remaining: 0, RetryAfter: 200 * time.Millisecond, ResetAfter: 900 * time.Millisecond, Limit: 3,
			},
			wantS: []int64{now - 800_000, now - 300_000, now - 100_000},
		},
		{
			name: "expired stamps are dropped on admission", state: ExactState{Stamps: []int64{now - 1_000_000, now - 999_999, now - 100_000}}, cost: 1, now: now,
			want:  limit.Decision{Allowed: true, Remaining: 0, ResetAfter: time.Second, Limit: 3},
			wantS: []int64{now - 999_999, now - 100_000, now},
		},
		{
			name: "cost two needs two expiries", state: ExactState{Stamps: []int64{now - 800_000, now - 300_000, now - 100_000}}, cost: 2, now: now,
			want: limit.Decision{
				Allowed: false, Remaining: 0, RetryAfter: 700 * time.Millisecond, ResetAfter: 900 * time.Millisecond, Limit: 3,
			},
			wantS: []int64{now - 800_000, now - 300_000, now - 100_000},
		},
		{
			name: "cost above rate reports a full window", state: ExactState{}, cost: 4, now: now,
			want:  limit.Decision{Allowed: false, Remaining: 3, RetryAfter: time.Second, Limit: 3},
			wantS: nil,
		},
		{
			name: "cost zero is a peek", state: ExactState{Stamps: []int64{now - 500_000}}, cost: 0, now: now,
			want:  limit.Decision{Allowed: true, Remaining: 2, ResetAfter: 500 * time.Millisecond, Limit: 3},
			wantS: []int64{now - 500_000},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			gotS, got := StepExact(tc.state, p, tc.cost, tc.now)
			if got != tc.want {
				t.Errorf("decision = %+v, want %+v", got, tc.want)
			}
			if !slices.Equal(gotS.Stamps, tc.wantS) {
				t.Errorf("stamps = %v, want %v", gotS.Stamps, tc.wantS)
			}
		})
	}
}

func TestTTL(t *testing.T) {
	t.Parallel()
	p := params(t, limit.SlidingWindow, 10, time.Second)
	if got := TTL(State{}, p, 5_000_000); got != 0 {
		t.Errorf("zero TTL = %s", got)
	}
	if got := TTL(State{Cur: 1, Index: 1000}, p, 1000_250_000); got != 1750*time.Millisecond {
		t.Errorf("TTL = %s, want 1.75s", got)
	}
	if got := TTL(State{Cur: 1, Index: 1000}, p, 1003_000_000); got != 0 {
		t.Errorf("stale TTL = %s, want 0", got)
	}
	if got := TTLExact(ExactState{}, p, 1); got != 0 {
		t.Errorf("empty exact TTL = %s", got)
	}
	if got := TTLExact(ExactState{Stamps: []int64{100, 400_000}}, p, 500_000); got != 900*time.Millisecond {
		t.Errorf("exact TTL = %s, want 900ms", got)
	}
}

func TestProperties(t *testing.T) {
	t.Parallel()
	for seed := uint64(1); seed <= 40; seed++ {
		t.Run("", func(t *testing.T) {
			t.Parallel()
			rng := rand.New(rand.NewPCG(seed, seed*15485863))
			rate := int64(rng.IntN(50) + 1)
			period := time.Duration(rng.IntN(2000)+1) * time.Millisecond
			p := params(t, limit.SlidingWindow, rate, period)
			pe := params(t, limit.SlidingWindowExact, rate, period)
			w := period.Microseconds()
			start := int64(rng.IntN(1<<40)) + 1
			now := start
			var s State
			var e ExactState
			allowed, allowedExact := int64(0), int64(0)
			for i := 0; i < 2000; i++ {
				now += int64(rng.IntN(int(w/rate*3) + 1))
				cost := rng.IntN(3) + 1
				next, d := Step(s, p, cost, now)
				if d.Allowed {
					allowed += int64(cost)
					if next.Index < s.Index {
						t.Fatalf("Index rewound: %d -> %d", s.Index, next.Index)
					}
				} else if next != s {
					t.Fatalf("denied call mutated state: %+v -> %+v", s, next)
				}
				nextE, de := StepExact(e, pe, cost, now)
				if de.Allowed {
					allowedExact += int64(cost)
				} else if !slices.Equal(nextE.Stamps, e.Stamps) {
					t.Fatalf("denied exact call mutated state")
				}
				// Two windows of rate is the approximation's worst case (the
				// weighting starts at full prev), plus one for rounding.
				elapsed := now - start
				bound := 2*rate + rate*elapsed/w + 1
				if allowed > bound {
					t.Fatalf("approx over-admission: %d > %d at step %d", allowed, bound, i)
				}
				if allowedExact > rate+rate*elapsed/w+1 {
					t.Fatalf("exact over-admission: %d > %d at step %d", allowedExact, rate+rate*elapsed/w+1, i)
				}
				if int64(len(nextE.Stamps)) > rate {
					t.Fatalf("exact state holds %d > rate %d stamps", len(nextE.Stamps), rate)
				}
				s, e = next, nextE
			}
		})
	}
}

// TestExactWindowBound: the exact variant never admits more than Rate in any
// trailing window, checked against every admitted stamp.
func TestExactWindowBound(t *testing.T) {
	t.Parallel()
	rng := rand.New(rand.NewPCG(7, 9))
	p := params(t, limit.SlidingWindowExact, 5, 100*time.Millisecond)
	var e ExactState
	var all []int64
	now := int64(1_000_000)
	for i := 0; i < 3000; i++ {
		now += int64(rng.IntN(15_000))
		var d limit.Decision
		e, d = StepExact(e, p, 1, now)
		if d.Allowed {
			all = append(all, now)
		}
	}
	w := p.Period.Microseconds()
	for i := range all {
		n := 0
		for j := i; j < len(all) && all[j]-all[i] < w; j++ {
			n++
		}
		if n > 5 {
			t.Fatalf("%d admissions inside one window starting at %d", n, all[i])
		}
	}
}

func TestPrecision(t *testing.T) {
	t.Parallel()
	p := params(t, limit.SlidingWindow, 50_000, time.Second)
	const big = int64(1) << 52
	s, d := Step(State{}, p, 50_000, big)
	if !d.Allowed || d.Remaining != 0 {
		t.Fatalf("at 2^52: %+v", d)
	}
	_, d = Step(s, p, 1, big+1)
	if d.Allowed {
		t.Fatalf("2^52 overflow: %+v", d)
	}
	gap := big + 30*24*time.Hour.Microseconds()
	s2, d := Step(s, p, 50_000, gap)
	if !d.Allowed || d.Remaining != 0 || s2.Prev != 0 {
		t.Fatalf("after 30d gap: %+v state %+v", d, s2)
	}
	for _, v := range []int64{s2.Index * p.Period.Microseconds(), p.Rate * p.Period.Microseconds(), gap} {
		if v >= 1<<53 {
			t.Fatalf("value %d exceeds 2^53", v)
		}
	}
	e, d := StepExact(ExactState{}, params(t, limit.SlidingWindowExact, 3, time.Second), 3, gap)
	if !d.Allowed || len(e.Stamps) != 3 || e.Stamps[2] != gap {
		t.Fatalf("exact at gap: %+v %v", d, e.Stamps)
	}
}

// FuzzStep compares Step with a float64 reference of the weighted formula.
func FuzzStep(f *testing.F) {
	f.Add(int64(0), int64(0), int64(0), int64(10), int64(1000), 1, int64(1_000_000))
	f.Add(int64(10), int64(3), int64(1000), int64(10), int64(1000), 2, int64(1000_250_000))
	f.Add(int64(0), int64(0), int64(0), int64(50_000), int64(1000), 10, int64(1)<<52)
	f.Fuzz(func(t *testing.T, prev, cur, index, rate, periodMS int64, cost int, now int64) {
		if rate <= 0 || rate > 1_000_000 || periodMS <= 0 || periodMS > 86_400_000 {
			t.Skip()
		}
		if cost < 0 || cost > 10_000 || now < 0 || now > 1<<52 || prev < 0 || cur < 0 || index < 0 {
			t.Skip()
		}
		if prev > rate || cur > rate || index > now/(periodMS*1000)+1 {
			t.Skip()
		}
		p, err := limit.NewParams(limit.SlidingWindow, rate, time.Duration(periodMS)*time.Millisecond, rate)
		if err != nil {
			t.Skip()
		}
		s := State{Prev: prev, Cur: cur, Index: index}
		next, got := Step(s, p, cost, now)

		w := float64(p.Period.Microseconds())
		idx := now / p.Period.Microseconds()
		elapsed := float64(now - idx*p.Period.Microseconds())
		fp, fc := 0.0, 0.0
		switch {
		case idx == index:
			fp, fc = float64(prev), float64(cur)
		case idx == index+1:
			fp = float64(cur)
		case idx < index:
			fp, fc, elapsed = float64(prev), float64(cur), 0
		}
		used := fp*(1-elapsed/w) + fc
		slack := float64(rate) - used - float64(cost)
		if math.Abs(slack) >= 1 && got.Allowed != (slack >= 0) {
			t.Fatalf("allowed=%v, reference slack %.3f", got.Allowed, slack)
		}
		refRemaining := float64(rate) - used
		if got.Allowed {
			refRemaining -= float64(cost)
		}
		if math.Abs(float64(got.Remaining)-math.Max(refRemaining, 0)) > 1 {
			t.Fatalf("remaining=%d, reference %.3f", got.Remaining, refRemaining)
		}
		if !got.Allowed && next != s {
			t.Fatal("denied call mutated state")
		}
		if next.Index < s.Index {
			t.Fatal("Index rewound")
		}
	})
}

func TestLimiter(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	now := time.UnixMicro(1_000_000_000)
	for _, algo := range []limit.Algorithm{limit.SlidingWindow, limit.SlidingWindowExact} {
		t.Run(algo.String(), func(t *testing.T) {
			t.Parallel()
			p := params(t, algo, 2, time.Second)
			l := NewLimiter(p)
			if l.Params() != p {
				t.Fatal("Params mismatch")
			}
			for i := 0; i < 2; i++ {
				d, err := l.Allow(ctx, "k", 1, now)
				if err != nil || !d.Allowed || d.Backend != limit.BackendLocal {
					t.Fatalf("call %d: %+v %v", i, d, err)
				}
			}
			d, err := l.Allow(ctx, "k", 1, now)
			if err != nil || d.Allowed {
				t.Fatalf("third: %+v %v", d, err)
			}
			if d, err = l.Allow(ctx, "k", 1, now.Add(2*time.Second)); err != nil || !d.Allowed {
				t.Fatalf("after two windows: %+v %v", d, err)
			}
			if l.Len() != 1 {
				t.Fatalf("Len = %d, want 1", l.Len())
			}
			l.Reset("k")
			if d, err = l.Allow(ctx, "k", 2, now); err != nil || !d.Allowed {
				t.Fatalf("after reset: %+v %v", d, err)
			}
			if _, err = l.Allow(ctx, "k", 0, now); !errors.Is(err, limit.ErrInvalidParams) {
				t.Fatalf("cost 0 err = %v", err)
			}
		})
	}
	bad := NewLimiter(limit.Params{Algo: limit.GCRA, Rate: 1, Period: time.Second, Burst: 1, EmissionUS: 1})
	if _, err := bad.Allow(ctx, "k", 1, now); !errors.Is(err, limit.ErrInvalidParams) {
		t.Fatalf("wrong algo err = %v", err)
	}
}

func BenchmarkStep(b *testing.B) {
	p, _ := limit.NewParams(limit.SlidingWindow, 1000, time.Second, 1000)
	var s State
	now := int64(1_000_000_000)
	var sink limit.Decision
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		now += 500
		s, sink = Step(s, p, 1, now)
	}
	_ = sink
}

func BenchmarkStepExact(b *testing.B) {
	p, _ := limit.NewParams(limit.SlidingWindowExact, 100, time.Second, 100)
	var s ExactState
	now := int64(1_000_000_000)
	var sink limit.Decision
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		now += 5000
		s, sink = StepExact(s, p, 1, now)
	}
	_ = sink
}
