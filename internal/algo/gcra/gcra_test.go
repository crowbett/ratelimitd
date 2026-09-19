package gcra

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
	p, err := limit.NewParams(limit.GCRA, rate, period, burst)
	if err != nil {
		tb.Fatalf("NewParams: %v", err)
	}
	return p
}

func TestStepTable(t *testing.T) {
	t.Parallel()
	// 10 per second, burst 5: T = 100ms, dvt = 500ms.
	p := params(t, 10, time.Second, 5)
	const now = int64(1_000_000_000) // 1000 s
	tests := []struct {
		name  string
		state State
		cost  int
		now   int64
		want  limit.Decision
		wantS State
	}{
		{
			name: "empty bucket first cell", state: 0, cost: 1, now: now,
			want:  limit.Decision{Allowed: true, Remaining: 4, ResetAfter: 100 * time.Millisecond, Limit: 5},
			wantS: State(now + 100_000),
		},
		{
			name: "idle past full clamps to now", state: State(now - 10_000_000), cost: 1, now: now,
			want:  limit.Decision{Allowed: true, Remaining: 4, ResetAfter: 100 * time.Millisecond, Limit: 5},
			wantS: State(now + 100_000),
		},
		{
			name: "burst of five fits", state: 0, cost: 5, now: now,
			want:  limit.Decision{Allowed: true, Remaining: 0, ResetAfter: 500 * time.Millisecond, Limit: 5},
			wantS: State(now + 500_000),
		},
		{
			name: "sixth cell denied, state untouched", state: State(now + 500_000), cost: 1, now: now,
			want: limit.Decision{
				Allowed: false, Remaining: 0, RetryAfter: 100 * time.Millisecond,
				ResetAfter: 500 * time.Millisecond, Limit: 5,
			},
			wantS: State(now + 500_000),
		},
		{
			name: "cost above burst never fits", state: 0, cost: 6, now: now,
			want: limit.Decision{
				Allowed: false, Remaining: 5, RetryAfter: 100 * time.Millisecond, ResetAfter: 0, Limit: 5,
			},
			wantS: 0,
		},
		{
			name: "half drained after 250ms", state: State(now + 500_000), cost: 1, now: now + 250_000,
			want:  limit.Decision{Allowed: true, Remaining: 1, ResetAfter: 350 * time.Millisecond, Limit: 5},
			wantS: State(now + 600_000),
		},
		{
			name: "cost two denied with exact retry", state: State(now + 500_000), cost: 2, now: now + 50_000,
			want: limit.Decision{
				Allowed: false, Remaining: 0, RetryAfter: 150 * time.Millisecond,
				ResetAfter: 450 * time.Millisecond, Limit: 5,
			},
			wantS: State(now + 500_000),
		},
		{
			name: "cost zero is a peek", state: State(now + 300_000), cost: 0, now: now,
			want:  limit.Decision{Allowed: true, Remaining: 2, ResetAfter: 300 * time.Millisecond, Limit: 5},
			wantS: State(now + 300_000),
		},
		{
			name: "invalid params deny", state: State(now), cost: 1, now: now,
			want: limit.Decision{}, wantS: State(now),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			pp := p
			if tc.name == "invalid params deny" {
				pp = limit.Params{}
			}
			gotS, got := Step(tc.state, pp, tc.cost, tc.now)
			if got != tc.want {
				t.Errorf("decision = %+v, want %+v", got, tc.want)
			}
			if gotS != tc.wantS {
				t.Errorf("state = %d, want %d", gotS, tc.wantS)
			}
		})
	}
}

func TestTTL(t *testing.T) {
	t.Parallel()
	p := params(t, 10, time.Second, 5)
	if got := TTL(0, p, 1000); got != 0 {
		t.Errorf("empty TTL = %s, want 0", got)
	}
	if got := TTL(State(1_500_000), p, 1_000_000); got != 500*time.Millisecond {
		t.Errorf("TTL = %s, want 500ms", got)
	}
	if got := TTL(State(500_000), p, 1_000_000); got != 0 {
		t.Errorf("stale TTL = %s, want 0", got)
	}
}

// TestProperties drives random op sequences under a monotone clock.
func TestProperties(t *testing.T) {
	t.Parallel()
	for seed := uint64(1); seed <= 40; seed++ {
		t.Run("", func(t *testing.T) {
			t.Parallel()
			rng := rand.New(rand.NewPCG(seed, seed*7919))
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
					if next < s {
						t.Fatalf("state rewound: %d -> %d", s, next)
					}
				} else if next != s {
					t.Fatalf("denied call mutated state: %d -> %d", s, next)
				}
				elapsed := now - start
				bound := burst + elapsed/p.EmissionUS + 1
				if allowed > bound {
					t.Fatalf("over-admission: allowed %d > bound %d at step %d", allowed, bound, i)
				}
				s = next
			}
		})
	}
}

// TestCostEquivalence: cost n from state s equals n calls of cost 1.
func TestCostEquivalence(t *testing.T) {
	t.Parallel()
	rng := rand.New(rand.NewPCG(3, 5))
	for i := 0; i < 500; i++ {
		p := params(t, int64(rng.IntN(100)+1), time.Duration(rng.IntN(999)+1)*time.Millisecond, int64(rng.IntN(30)+1))
		now := int64(rng.IntN(1 << 40))
		s := State(now + int64(rng.IntN(int(p.Burst*p.EmissionUS)+1)))
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
			t.Fatalf("cost %d allowed=%v but singles allowed=%v (p=%+v s=%d now=%d)", n, bulk.Allowed, ok, p, s, now)
		}
		if ok && (bulkS != single || bulk.Remaining != last.Remaining || bulk.ResetAfter != last.ResetAfter) {
			t.Fatalf("cost %d state %d dec %+v; singles state %d dec %+v", n, bulkS, bulk, single, last)
		}
	}
}

// TestPrecision pins the vectors the Lua differential test replays: a clock at
// 2^52 us and a 30-day idle gap. Every integer must stay below 2^53.
func TestPrecision(t *testing.T) {
	t.Parallel()
	p := params(t, 50_000, time.Second, 5_000) // T = 20us, dvt = 100ms
	const big = int64(1) << 52
	s, d := Step(0, p, 5_000, big)
	if !d.Allowed || d.Remaining != 0 || int64(s) != big+100_000 {
		t.Fatalf("at 2^52: %+v state %d", d, s)
	}
	if int64(s) >= 1<<53 {
		t.Fatalf("state %d exceeds 2^53", s)
	}
	_, d = Step(s, p, 1, big)
	if d.Allowed || d.RetryAfter != 20*time.Microsecond {
		t.Fatalf("2^52 overflow cell: %+v", d)
	}
	gap := big + 30*24*time.Hour.Microseconds()
	s2, d := Step(s, p, 5_000, gap)
	if !d.Allowed || d.Remaining != 0 || int64(s2) != gap+100_000 {
		t.Fatalf("after 30d gap: %+v state %d", d, s2)
	}
	if int64(s2) >= 1<<53 {
		t.Fatalf("state %d exceeds 2^53", s2)
	}
}

// FuzzStep compares Step with a float64 reference model, tolerating one event
// of disagreement at the boundary.
func FuzzStep(f *testing.F) {
	f.Add(int64(0), int64(10), int64(1000), int64(5), 1, int64(1_000_000))
	f.Add(int64(1_500_000), int64(1000), int64(1000), int64(200), 3, int64(1_000_000))
	f.Add(int64(1)<<52, int64(50_000), int64(1000), int64(5000), 10, int64(1)<<52)
	f.Fuzz(func(t *testing.T, state, rate, periodMS, burst int64, cost int, now int64) {
		if rate <= 0 || rate > 1_000_000 || periodMS <= 0 || periodMS > 86_400_000 || burst <= 0 || burst > 1_000_000 {
			t.Skip()
		}
		if cost < 0 || cost > 10_000 || now < 0 || now > 1<<52 || state < 0 || state > 1<<52 {
			t.Skip()
		}
		p, err := limit.NewParams(limit.GCRA, rate, time.Duration(periodMS)*time.Millisecond, burst)
		if err != nil {
			t.Skip()
		}
		next, got := Step(State(state), p, cost, now)

		// float reference
		ft := float64(p.EmissionUS)
		dvt := float64(burst) * ft
		tat := math.Max(float64(state), float64(now))
		newtat := tat + float64(cost)*ft
		slack := (float64(now) - (newtat - dvt)) / ft // events of headroom
		if math.Abs(slack) >= 1 {
			wantAllowed := slack >= 0
			if got.Allowed != wantAllowed {
				t.Fatalf("allowed=%v, reference %v (slack %.3f events)", got.Allowed, wantAllowed, slack)
			}
		}
		var refRemaining float64
		if got.Allowed {
			refRemaining = (dvt - (newtat - float64(now))) / ft
		} else {
			refRemaining = (dvt - (tat - float64(now))) / ft
		}
		if math.Abs(float64(got.Remaining)-math.Max(refRemaining, 0)) > 1 {
			t.Fatalf("remaining=%d, reference %.3f", got.Remaining, refRemaining)
		}
		if got.Allowed && cost > 0 && next < State(state) {
			t.Fatalf("state rewound %d -> %d", state, next)
		}
		if !got.Allowed && next != State(state) {
			t.Fatalf("denied call mutated state")
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
	if d, err = l.Allow(ctx, "other", 1, now); err != nil || !d.Allowed {
		t.Fatalf("other key: %+v %v", d, err)
	}
	if l.Len() != 2 {
		t.Fatalf("Len = %d, want 2", l.Len())
	}
	l.Reset("k")
	if d, err = l.Allow(ctx, "k", 1, now); err != nil || !d.Allowed {
		t.Fatalf("after reset: %+v %v", d, err)
	}
	if _, err = l.Allow(ctx, "k", 0, now); !errors.Is(err, limit.ErrInvalidParams) {
		t.Fatalf("cost 0 err = %v", err)
	}
	bad := NewLimiter(limit.Params{Algo: limit.TokenBucket})
	if _, err = bad.Allow(ctx, "k", 1, now); !errors.Is(err, limit.ErrInvalidParams) {
		t.Fatalf("wrong algo err = %v", err)
	}
}

func BenchmarkStep(b *testing.B) {
	p, _ := limit.NewParams(limit.GCRA, 1000, time.Second, 200)
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
