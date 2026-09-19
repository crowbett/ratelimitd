package tokenbucket

import (
	"context"
	"errors"
	"math"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/crowbett/ratelimitd/internal/limit"
)

func params(tb testing.TB, rate int64, period time.Duration, burst int64) limit.Params {
	tb.Helper()
	p, err := limit.NewParams(limit.TokenBucket, rate, period, burst)
	if err != nil {
		tb.Fatalf("NewParams: %v", err)
	}
	return p
}

func TestStepTable(t *testing.T) {
	t.Parallel()
	// 10 per second, burst 5: one token per 100ms, cap 5_000_000 micro-tokens.
	p := params(t, 10, time.Second, 5)
	const now = int64(1_000_000_000)
	full := State{Tokens: 5 * MicroTokens, Last: now}
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
			name: "zero state is full", state: State{}, p: p, cost: 1, now: now,
			want:  limit.Decision{Allowed: true, Remaining: 4, ResetAfter: 100 * time.Millisecond, Limit: 5},
			wantS: State{Tokens: 4 * MicroTokens, Last: now},
		},
		{
			name: "take the whole burst", state: full, p: p, cost: 5, now: now,
			want:  limit.Decision{Allowed: true, Remaining: 0, ResetAfter: 500 * time.Millisecond, Limit: 5},
			wantS: State{Tokens: 0, Last: now},
		},
		{
			name: "empty denies with retry", state: State{Tokens: 0, Last: now}, p: p, cost: 1, now: now,
			want: limit.Decision{
				Allowed: false, Remaining: 0, RetryAfter: 100 * time.Millisecond,
				ResetAfter: 500 * time.Millisecond, Limit: 5,
			},
			wantS: State{Tokens: 0, Last: now},
		},
		{
			name: "half token refilled after 50ms is not enough", state: State{Tokens: 0, Last: now}, p: p, cost: 1, now: now + 50_000,
			want: limit.Decision{
				Allowed: false, Remaining: 0, RetryAfter: 50 * time.Millisecond,
				ResetAfter: 450 * time.Millisecond, Limit: 5,
			},
			wantS: State{Tokens: 0, Last: now},
		},
		{
			name: "one token after 100ms", state: State{Tokens: 0, Last: now}, p: p, cost: 1, now: now + 100_000,
			want:  limit.Decision{Allowed: true, Remaining: 0, ResetAfter: 500 * time.Millisecond, Limit: 5},
			wantS: State{Tokens: 0, Last: now + 100_000},
		},
		{
			name: "fractional refill carries", state: State{Tokens: 0, Last: now}, p: p, cost: 1, now: now + 150_000,
			want:  limit.Decision{Allowed: true, Remaining: 0, ResetAfter: 450 * time.Millisecond, Limit: 5},
			wantS: State{Tokens: MicroTokens / 2, Last: now + 150_000},
		},
		{
			name: "refill clamps at capacity", state: State{Tokens: 0, Last: now}, p: p, cost: 2, now: now + 10_000_000,
			want:  limit.Decision{Allowed: true, Remaining: 3, ResetAfter: 200 * time.Millisecond, Limit: 5},
			wantS: State{Tokens: 3 * MicroTokens, Last: now + 10_000_000},
		},
		{
			name: "clock behind sees no refill and keeps Last", state: State{Tokens: MicroTokens, Last: now}, p: p, cost: 1, now: now - 5_000,
			want:  limit.Decision{Allowed: true, Remaining: 0, ResetAfter: 500 * time.Millisecond, Limit: 5},
			wantS: State{Tokens: 0, Last: now},
		},
		{
			name: "cost above burst never fits", state: full, p: p, cost: 6, now: now,
			want: limit.Decision{
				Allowed: false, Remaining: 5, RetryAfter: 100 * time.Millisecond, ResetAfter: 0, Limit: 5,
			},
			wantS: full,
		},
		{
			name: "cost zero is a peek", state: State{Tokens: 2 * MicroTokens, Last: now}, p: p, cost: 0, now: now + 100_000,
			want:  limit.Decision{Allowed: true, Remaining: 3, ResetAfter: 200 * time.Millisecond, Limit: 5},
			wantS: State{Tokens: 2 * MicroTokens, Last: now},
		},
		{
			name: "invalid params deny", state: full, p: limit.Params{}, cost: 1, now: now,
			want: limit.Decision{}, wantS: full,
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

func TestTTL(t *testing.T) {
	t.Parallel()
	p := params(t, 10, time.Second, 5)
	const now = int64(1_000_000_000)
	tests := []struct {
		name string
		s    State
		now  int64
		want time.Duration
	}{
		{"zero state", State{}, now, 0},
		{"empty bucket", State{Tokens: 0, Last: now}, now, 500 * time.Millisecond},
		{"partially refilled", State{Tokens: 0, Last: now}, now + 120_000, 380 * time.Millisecond},
		{"full", State{Tokens: 5 * MicroTokens, Last: now}, now, 0},
		{"idle past full", State{Tokens: 0, Last: now}, now + 3_000_000, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := TTL(tc.s, p, tc.now); got != tc.want {
				t.Errorf("TTL = %s, want %s", got, tc.want)
			}
		})
	}
}

func TestProperties(t *testing.T) {
	t.Parallel()
	for seed := uint64(1); seed <= 40; seed++ {
		t.Run("", func(t *testing.T) {
			t.Parallel()
			rng := rand.New(rand.NewPCG(seed, seed*104729))
			rate := int64(rng.IntN(50) + 1)
			period := time.Duration(rng.IntN(2000)+1) * time.Millisecond
			burst := int64(rng.IntN(20) + 1)
			p := params(t, rate, period, burst)
			start := int64(rng.IntN(1<<40)) + 1
			now := start
			var s State
			allowed := int64(0)
			for i := 0; i < 2000; i++ {
				now += int64(rng.IntN(int(p.EmissionUS*3) + 1))
				cost := rng.IntN(3) + 1
				next, d := Step(s, p, cost, now)
				if d.Allowed {
					allowed += int64(cost)
					if next.Last < s.Last {
						t.Fatalf("Last rewound: %d -> %d", s.Last, next.Last)
					}
				} else if next != s {
					t.Fatalf("denied call mutated state: %+v -> %+v", s, next)
				}
				bound := burst + (now-start)/p.EmissionUS + 1
				if allowed > bound {
					t.Fatalf("over-admission: allowed %d > bound %d at step %d", allowed, bound, i)
				}
				s = next
			}
		})
	}
}

func TestCostEquivalence(t *testing.T) {
	t.Parallel()
	rng := rand.New(rand.NewPCG(11, 13))
	for i := 0; i < 500; i++ {
		p := params(t, int64(rng.IntN(100)+1), time.Duration(rng.IntN(999)+1)*time.Millisecond, int64(rng.IntN(30)+1))
		now := int64(rng.IntN(1<<40)) + 1
		s := State{Tokens: int64(rng.IntN(int(p.Burst*MicroTokens) + 1)), Last: now - int64(rng.IntN(int(p.EmissionUS*4)))}
		n := rng.IntN(int(p.Burst)+2) + 1
		bulkS, bulk := Step(s, p, n, now)
		single := s
		ok := true
		var last limit.Decision
		for j := 0; j < n; j++ {
			var d limit.Decision
			single, d = Step(single, p, 1, now)
			last = d
			if !d.Allowed {
				ok = false
				break
			}
		}
		if bulk.Allowed != ok {
			t.Fatalf("cost %d allowed=%v but singles allowed=%v (p=%+v s=%+v now=%d)", n, bulk.Allowed, ok, p, s, now)
		}
		if ok && (bulkS != single || bulk.Remaining != last.Remaining || bulk.ResetAfter != last.ResetAfter) {
			t.Fatalf("cost %d state %+v dec %+v; singles state %+v dec %+v", n, bulkS, bulk, single, last)
		}
	}
}

func TestPrecision(t *testing.T) {
	t.Parallel()
	p := params(t, 50_000, time.Second, 5_000) // T = 20us
	const big = int64(1) << 52
	s, d := Step(State{}, p, 5_000, big)
	if !d.Allowed || d.Remaining != 0 || s != (State{Tokens: 0, Last: big}) {
		t.Fatalf("at 2^52: %+v state %+v", d, s)
	}
	_, d = Step(s, p, 1, big+19)
	if d.Allowed || d.RetryAfter != time.Microsecond {
		t.Fatalf("19us later: %+v", d)
	}
	s2, d := Step(s, p, 1, big+20)
	if !d.Allowed || s2.Tokens != 0 || s2.Last != big+20 {
		t.Fatalf("20us later: %+v state %+v", d, s2)
	}
	gap := big + 30*24*time.Hour.Microseconds()
	s3, d := Step(s, p, 5_000, gap)
	if !d.Allowed || d.Remaining != 0 || s3 != (State{Tokens: 0, Last: gap}) {
		t.Fatalf("after 30d gap: %+v state %+v", d, s3)
	}
	for _, v := range []int64{s3.Last, s3.Tokens, gap, p.Burst * MicroTokens} {
		if v >= 1<<53 {
			t.Fatalf("value %d exceeds 2^53", v)
		}
	}
}

// FuzzStep compares Step with a float64 reference model, tolerating one token.
func FuzzStep(f *testing.F) {
	f.Add(int64(0), int64(0), int64(10), int64(1000), int64(5), 1, int64(1_000_000))
	f.Add(int64(2_500_000), int64(900_000), int64(1000), int64(1000), int64(200), 3, int64(1_000_000))
	f.Add(int64(0), int64(1)<<52, int64(50_000), int64(1000), int64(5000), 10, int64(1)<<52+7)
	f.Fuzz(func(t *testing.T, tokens, last, rate, periodMS, burst int64, cost int, now int64) {
		if rate <= 0 || rate > 1_000_000 || periodMS <= 0 || periodMS > 86_400_000 || burst <= 0 || burst > 1_000_000 {
			t.Skip()
		}
		if cost < 0 || cost > 10_000 || now < 0 || now > 1<<52 || last < 0 || last > 1<<52 {
			t.Skip()
		}
		if tokens < 0 || tokens > burst*MicroTokens {
			t.Skip()
		}
		p, err := limit.NewParams(limit.TokenBucket, rate, time.Duration(periodMS)*time.Millisecond, burst)
		if err != nil {
			t.Skip()
		}
		s := State{Tokens: tokens, Last: last}
		next, got := Step(s, p, cost, now)

		capacity := float64(burst)
		ref := capacity
		if last != 0 {
			ref = float64(tokens) / float64(MicroTokens)
			if now > last {
				ref = math.Min(capacity, ref+float64(now-last)/float64(p.EmissionUS))
			}
		}
		slack := ref - float64(cost)
		if math.Abs(slack) >= 1 {
			if got.Allowed != (slack >= 0) {
				t.Fatalf("allowed=%v, reference tokens %.3f cost %d", got.Allowed, ref, cost)
			}
		}
		refRemaining := ref
		if got.Allowed {
			refRemaining = ref - float64(cost)
		}
		if math.Abs(float64(got.Remaining)-math.Max(refRemaining, 0)) > 1 {
			t.Fatalf("remaining=%d, reference %.3f", got.Remaining, refRemaining)
		}
		if !got.Allowed && next != s {
			t.Fatal("denied call mutated state")
		}
		if next.Last < s.Last {
			t.Fatalf("Last rewound %d -> %d", s.Last, next.Last)
		}
	})
}

func TestLimiter(t *testing.T) {
	t.Parallel()
	p := params(t, 10, time.Second, 2)
	l := NewLimiter(p)
	if l.Params() != p {
		t.Fatal("Params mismatch")
	}
	now := time.UnixMicro(1_000_000_000)
	ctx := context.Background()
	for i := 0; i < 2; i++ {
		d, err := l.Allow(ctx, "k", 1, now)
		if err != nil || !d.Allowed || d.Backend != limit.BackendLocal {
			t.Fatalf("call %d: %+v %v", i, d, err)
		}
	}
	d, err := l.Allow(ctx, "k", 1, now)
	if err != nil || d.Allowed || d.RetryAfter != 100*time.Millisecond {
		t.Fatalf("third: %+v %v", d, err)
	}
	if d, err = l.Allow(ctx, "k", 1, now.Add(100*time.Millisecond)); err != nil || !d.Allowed {
		t.Fatalf("after refill: %+v %v", d, err)
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
	bad := NewLimiter(limit.Params{Algo: limit.GCRA, EmissionUS: 1, Burst: 1})
	if _, err = bad.Allow(ctx, "k", 1, now); !errors.Is(err, limit.ErrInvalidParams) {
		t.Fatalf("wrong algo err = %v", err)
	}
}

func BenchmarkStep(b *testing.B) {
	p, _ := limit.NewParams(limit.TokenBucket, 1000, time.Second, 200)
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
