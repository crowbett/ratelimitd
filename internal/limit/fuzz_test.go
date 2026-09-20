package limit

import (
	"errors"
	"testing"
	"time"
)

// FuzzNewParams: any (algo, rate, period, burst) -> NewParams never panics;
// a rejection wraps ErrInvalidParams and returns zero Params; an accepted
// set has EmissionUS = period/rate >= 1 and every product an algorithm
// forms stays below MaxExact, so the Lua double arithmetic is exact.
func FuzzNewParams(f *testing.F) {
	f.Add(uint8(1), int64(1000), int64(1_000_000_000), int64(200))
	f.Add(uint8(2), int64(10), int64(1_000_000_000), int64(20))
	f.Add(uint8(3), int64(60), int64(60_000_000_000), int64(60))
	f.Add(uint8(4), int64(3), int64(1_000_000_000), int64(3))
	f.Add(uint8(0), int64(0), int64(0), int64(0))
	f.Add(uint8(1), int64(1), int64(1<<62), int64(1<<62))
	f.Add(uint8(2), int64(1), int64(86_400_000_000_000), int64(1))
	f.Add(uint8(3), int64(1_000_000), int64(86_400_000_000_000), int64(1_000_000))
	f.Add(uint8(1), int64(-1), int64(-1<<63), int64(1))
	f.Add(uint8(9), int64(1), int64(1_000_000_000), int64(1))
	f.Fuzz(func(t *testing.T, algo uint8, rate, periodNS, burst int64) {
		p, err := NewParams(Algorithm(algo), rate, time.Duration(periodNS), burst)
		if err != nil {
			if !errors.Is(err, ErrInvalidParams) {
				t.Fatalf("error %v does not wrap ErrInvalidParams", err)
			}
			if p != (Params{}) {
				t.Fatalf("error path returned %+v", p)
			}
			return
		}
		if p.Algo == AlgorithmUnspecified || p.Algo > SlidingWindowExact || p.Rate <= 0 || p.Burst < 1 || p.Period < time.Millisecond {
			t.Fatalf("accepted %+v", p)
		}
		period := p.Period.Microseconds()
		if p.EmissionUS != period/p.Rate || p.EmissionUS < 1 {
			t.Fatalf("emission %d for %d per %s", p.EmissionUS, p.Rate, p.Period)
		}
		mulExact := func(what string, a, b int64) {
			t.Helper()
			if a > MaxExact/b { // a*b >= 2^53 (or overflows int64)
				t.Fatalf("%s: %d * %d leaves 2^53 for %+v", what, a, b, p)
			}
		}
		switch p.Algo {
		case GCRA:
			mulExact("gcra burst*emission", p.Burst, p.EmissionUS)
		case TokenBucket:
			mulExact("token bucket burst*1e6", p.Burst, microTokens)
			mulExact("token bucket emission*1e6", p.EmissionUS, microTokens)
		case SlidingWindow, SlidingWindowExact:
			mulExact("sliding window 2*rate*period", 2*p.Rate, period)
		case AlgorithmUnspecified:
			t.Fatal("unreachable")
		}
	})
}

// FuzzStrictest: the fold never panics, returns one of its inputs, and the
// winner is a deny when any input denies.
func FuzzStrictest(f *testing.F) {
	f.Add(true, 5, int64(0), false, 0, int64(250), true, 1, int64(0))
	f.Add(true, 1, int64(0), true, 1, int64(0), true, 1, int64(0))
	f.Add(false, -1, int64(-1), false, 0, int64(1<<62), false, 0, int64(1<<62))
	f.Fuzz(func(t *testing.T, a1 bool, r1 int, w1 int64, a2 bool, r2 int, w2 int64, a3 bool, r3 int, w3 int64) {
		ds := []Decision{
			{Allowed: a1, Remaining: r1, RetryAfter: time.Duration(w1), Policy: "a"},
			{Allowed: a2, Remaining: r2, RetryAfter: time.Duration(w2), Policy: "b"},
			{Allowed: a3, Remaining: r3, RetryAfter: time.Duration(w3), Policy: "c"},
		}
		got := Strictest(ds)
		found := false
		anyDeny := false
		for _, d := range ds {
			if d == got {
				found = true
			}
			if !d.Allowed {
				anyDeny = true
			}
		}
		if !found {
			t.Fatalf("Strictest returned %+v, not one of %+v", got, ds)
		}
		if anyDeny && got.Allowed {
			t.Fatalf("Strictest allowed %+v with a deny in %+v", got, ds)
		}
		for _, d := range ds {
			if d.Allowed == got.Allowed && (d.Remaining < got.Remaining || (d.Remaining == got.Remaining && d.RetryAfter > got.RetryAfter)) {
				t.Fatalf("%+v is stricter than the winner %+v", d, got)
			}
		}
		if Strictest(nil) != (Decision{}) || Strictest(ds[:1]) != ds[0] {
			t.Fatal("empty or single input not handled")
		}
	})
}
