package main

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/crowbett/ratelimitd/internal/algo/gcra"
	"github.com/crowbett/ratelimitd/internal/algo/slidingwindow"
	"github.com/crowbett/ratelimitd/internal/algo/tokenbucket"
	"github.com/crowbett/ratelimitd/internal/limit"
)

func base() RaceParams {
	return RaceParams{Pattern: "steady", Seed: 7, Rate: 50, PeriodMS: 1000, Burst: 20, DurationMS: 30_000, Buckets: 60}
}

func TestRaceMatchesLimiters(t *testing.T) {
	t.Parallel()
	for _, pattern := range []string{"steady", "burst", "ramp", "attack"} {
		t.Run(pattern, func(t *testing.T) {
			t.Parallel()
			p := base()
			p.Pattern = pattern
			got, err := Race(p)
			if err != nil {
				t.Fatal(err)
			}
			events, err := traffic(p.Pattern, p.Seed, float64(p.Rate)/1e6, p.Burst, p.DurationMS*1000)
			if err != nil {
				t.Fatal(err)
			}
			for i, a := range algos {
				lp, err := limit.NewParams(a, p.Rate, time.Second, p.Burst)
				if err != nil {
					t.Fatal(err)
				}
				var l limit.Limiter
				switch a {
				case limit.GCRA:
					l = gcra.NewLimiter(lp)
				case limit.TokenBucket:
					l = tokenbucket.NewLimiter(lp)
				default:
					l = slidingwindow.NewLimiter(lp)
				}
				want := make([]int, p.Buckets)
				for _, ev := range events {
					d, err := l.Allow(context.Background(), "k", 1, time.UnixMicro(epochUS+ev))
					if err != nil {
						t.Fatal(err)
					}
					if d.Allowed {
						want[int(ev*int64(p.Buckets)/(p.DurationMS*1000))]++
					}
				}
				s := got.Series[i]
				if s.Algo != a.String() || !reflect.DeepEqual(s.Allowed, want) {
					t.Errorf("%s: bridge allowed %v, Limiter allowed %v", a, s.Allowed, want)
				}
				if s.TotalAllowed+s.TotalDenied != len(events) {
					t.Errorf("%s: %d allowed + %d denied != %d offered", a, s.TotalAllowed, s.TotalDenied, len(events))
				}
				if s.TotalDenied == 0 {
					t.Errorf("%s: %s traffic over the limit denied nothing", a, pattern)
				}
			}
		})
	}
}

// TestGCRAMatchesTokenBucket backs the demo page's claim: with the same rate
// and burst the two are one limiter in different state, and when the rate
// divides a second evenly the bucket's micro-token refill is exact, so every
// decision matches. Other rates may differ by integer rounding.
func TestGCRAMatchesTokenBucket(t *testing.T) {
	t.Parallel()
	for _, pattern := range []string{"steady", "burst", "ramp", "attack"} {
		for _, rate := range []int64{1, 8, 50, 125, 1000} {
			for _, burst := range []int64{1, 20, 500} {
				p := RaceParams{Pattern: pattern, Seed: uint64(rate * burst), Rate: rate, PeriodMS: 1000, Burst: burst, DurationMS: 30_000, Buckets: 60}
				r, err := Race(p)
				if err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(r.Series[0].Allowed, r.Series[1].Allowed) {
					t.Errorf("%+v: gcra %v, token bucket %v", p, r.Series[0].Allowed, r.Series[1].Allowed)
				}
			}
		}
	}
}

func TestRaceBounds(t *testing.T) {
	t.Parallel()
	p := base()
	p.Pattern = "attack"
	r, err := Race(p)
	if err != nil {
		t.Fatal(err)
	}
	periods := int(p.DurationMS / p.PeriodMS)
	for _, s := range r.Series {
		limitTotal := int(p.Rate) * periods
		switch s.Algo {
		case "gcra", "token_bucket":
			limitTotal += int(p.Burst)
		default:
			limitTotal += int(p.Rate) // the window straddling the start
		}
		if s.TotalAllowed > limitTotal {
			t.Errorf("%s allowed %d over %d periods, above its bound %d", s.Algo, s.TotalAllowed, periods, limitTotal)
		}
	}
	if r.EmissionUS != 20_000 {
		t.Errorf("emission = %d us, want 20000 (1 s / 50)", r.EmissionUS)
	}
}

// TestRaceUIMaximums keeps the site's input limits (limit/s up to 1000,
// burst up to 2000, 30 s runs) inside maxEvents for every pattern and seed.
func TestRaceUIMaximums(t *testing.T) {
	t.Parallel()
	for _, pattern := range []string{"steady", "burst", "ramp", "attack"} {
		for seed := range uint64(20) {
			p := RaceParams{Pattern: pattern, Seed: seed, Rate: 1000, PeriodMS: 1000, Burst: 2000, DurationMS: 30_000, Buckets: 60}
			if _, err := Race(p); err != nil {
				t.Errorf("%s seed %d: %v", pattern, seed, err)
			}
		}
	}
}

func TestRaceDeterministic(t *testing.T) {
	t.Parallel()
	a, err := Race(base())
	if err != nil {
		t.Fatal(err)
	}
	b, err := Race(base())
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(a, b) {
		t.Error("same seed gave different results")
	}
	p := base()
	p.Seed++
	c, err := Race(p)
	if err != nil {
		t.Fatal(err)
	}
	if reflect.DeepEqual(a.Offered, c.Offered) {
		t.Error("a different seed gave the same traffic")
	}
}

func TestHandleErrors(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name, method string
		mutate       func(*RaceParams)
		want         string
	}{
		{"method", "nope", nil, `unknown method "nope"`},
		{"pattern", "race", func(p *RaceParams) { p.Pattern = "flood" }, "unknown pattern"},
		{"rate", "race", func(p *RaceParams) { p.Rate = 0 }, "rate must be > 0"},
		{"burst", "race", func(p *RaceParams) { p.Burst = 0 }, "burst must be >= 1"},
		{"duration", "race", func(p *RaceParams) { p.DurationMS = 500 }, "durationMs"},
		{"buckets", "race", func(p *RaceParams) { p.Buckets = 1 }, "buckets"},
		{"period", "race", func(p *RaceParams) { p.PeriodMS = 9_300_000_000_000 }, "periodMs"},
		{"too many", "race", func(p *RaceParams) { p.Rate, p.DurationMS = 100_000, 120_000 }, "over 120000 requests"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			p := base()
			if c.mutate != nil {
				c.mutate(&p)
			}
			raw, err := json.Marshal(p)
			if err != nil {
				t.Fatal(err)
			}
			got, ok := Handle(c.method, raw).(map[string]string)
			if !ok || !strings.Contains(got["error"], c.want) {
				t.Errorf("got %v, want an error containing %q", got, c.want)
			}
		})
	}
	if _, ok := Handle("race", []byte(`{`)).(map[string]string); !ok {
		t.Error("bad JSON did not return an error")
	}
}

func TestRaceResultHasNoErrorKey(t *testing.T) {
	t.Parallel()
	raw, err := json.Marshal(Handle("race", mustJSON(t, base())))
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	if _, bad := m["error"]; bad {
		t.Errorf("success result has an error key: %s", raw)
	}
	if len(m["series"]) == 0 {
		t.Errorf("no series in %s", raw)
	}
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
