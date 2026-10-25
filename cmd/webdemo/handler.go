// Command webdemo is ratelimitd's in-browser demo bridge, compiled to
// js/wasm by crowbett.github.io: it drives the real gcra, tokenbucket and
// slidingwindow Step functions over one seeded traffic pattern so the three
// can be compared side by side. Contract:
// https://github.com/crowbett/crowbett.github.io/blob/main/docs/demo-contract.md
package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"time"

	"github.com/crowbett/ratelimitd/internal/algo/gcra"
	"github.com/crowbett/ratelimitd/internal/algo/slidingwindow"
	"github.com/crowbett/ratelimitd/internal/algo/tokenbucket"
	"github.com/crowbett/ratelimitd/internal/limit"
)

const (
	// epochUS is where the simulated clock starts; any fixed instant works.
	epochUS = 1_700_000_000_000_000
	// maxEvents bounds one call to well under the 50 ms budget in a browser.
	maxEvents = 200_000
)

// RaceParams is one race: the same limit (Rate per PeriodMS, Burst) for all
// three algorithms, fed DurationMS of Pattern traffic seeded by Seed and
// summarised into Buckets equal time slices.
type RaceParams struct {
	Pattern    string `json:"pattern"`
	Seed       uint64 `json:"seed"`
	Rate       int64  `json:"rate"`
	PeriodMS   int64  `json:"periodMs"`
	Burst      int64  `json:"burst"`
	DurationMS int64  `json:"durationMs"`
	Buckets    int    `json:"buckets"`
}

// Series is one algorithm's outcome per bucket.
type Series struct {
	Algo         string `json:"algo"`
	Allowed      []int  `json:"allowed"`
	Denied       []int  `json:"denied"`
	TotalAllowed int    `json:"totalAllowed"`
	TotalDenied  int    `json:"totalDenied"`
}

// RaceResult is the offered traffic per bucket and each algorithm's series.
// EmissionUS is limit.NewParams' Period/Rate, for drawing the limit line.
type RaceResult struct {
	Pattern    string   `json:"pattern"`
	BucketUS   float64  `json:"bucketUs"`
	EmissionUS int64    `json:"emissionUs"`
	Offered    []int    `json:"offered"`
	Series     []Series `json:"series"`
}

// Handle answers one bridge call. Errors come back as {"error": "..."}.
func Handle(method string, params []byte) any {
	switch method {
	case "race":
		var p RaceParams
		if err := json.Unmarshal(params, &p); err != nil {
			return errorResult(fmt.Errorf("params: %w", err))
		}
		r, err := Race(p)
		if err != nil {
			return errorResult(err)
		}
		return r
	default:
		return errorResult(fmt.Errorf("unknown method %q", method))
	}
}

func errorResult(err error) map[string]string {
	return map[string]string{"error": err.Error()}
}

var algos = []limit.Algorithm{limit.GCRA, limit.TokenBucket, limit.SlidingWindow}

// Race runs p through all three algorithms. Each algorithm holds one key's
// state and, like the in-process Limiters, keeps the new state only when a
// request is allowed.
func Race(p RaceParams) (RaceResult, error) {
	if p.DurationMS < 1000 || p.DurationMS > 120_000 {
		return RaceResult{}, fmt.Errorf("durationMs must be 1000..120000, got %d", p.DurationMS)
	}
	if p.Buckets < 10 || p.Buckets > 240 {
		return RaceResult{}, fmt.Errorf("buckets must be 10..240, got %d", p.Buckets)
	}
	period := time.Duration(p.PeriodMS) * time.Millisecond
	params := make([]limit.Params, len(algos))
	for i, a := range algos {
		lp, err := limit.NewParams(a, p.Rate, period, p.Burst)
		if err != nil {
			return RaceResult{}, err
		}
		params[i] = lp
	}
	durUS := p.DurationMS * 1000
	events, err := traffic(p.Pattern, p.Seed, float64(p.Rate)/float64(period.Microseconds()), p.Burst, durUS)
	if err != nil {
		return RaceResult{}, err
	}

	bucket := func(t int64) int { return int(t * int64(p.Buckets) / durUS) }
	res := RaceResult{
		Pattern:    p.Pattern,
		BucketUS:   float64(durUS) / float64(p.Buckets),
		EmissionUS: params[0].EmissionUS,
		Offered:    make([]int, p.Buckets),
	}
	for _, t := range events {
		res.Offered[bucket(t)]++
	}
	for _, lp := range params {
		s := Series{Algo: lp.Algo.String(), Allowed: make([]int, p.Buckets), Denied: make([]int, p.Buckets)}
		step := stepper(lp)
		for _, t := range events {
			if step(epochUS + t) {
				s.Allowed[bucket(t)]++
				s.TotalAllowed++
			} else {
				s.Denied[bucket(t)]++
				s.TotalDenied++
			}
		}
		res.Series = append(res.Series, s)
	}
	return res, nil
}

// stepper returns a one-key limiter over the algorithm's pure Step.
func stepper(p limit.Params) func(nowUS int64) bool {
	switch p.Algo {
	case limit.GCRA:
		var s gcra.State
		return func(nowUS int64) bool {
			next, d := gcra.Step(s, p, 1, nowUS)
			if d.Allowed {
				s = next
			}
			return d.Allowed
		}
	case limit.TokenBucket:
		var s tokenbucket.State
		return func(nowUS int64) bool {
			next, d := tokenbucket.Step(s, p, 1, nowUS)
			if d.Allowed {
				s = next
			}
			return d.Allowed
		}
	default:
		var s slidingwindow.State
		return func(nowUS int64) bool {
			next, d := slidingwindow.Step(s, p, 1, nowUS)
			if d.Allowed {
				s = next
			}
			return d.Allowed
		}
	}
}

// traffic returns sorted request times in microseconds from 0 to durUS.
// Shapes are offered load relative to the limit L (requests per us):
//
//	steady: Poisson at 1.2 L
//	burst:  Poisson at 0.5 L plus four spikes of 2*burst requests, each within 100 ms
//	ramp:   Poisson rising linearly from 0 to 3 L
//	attack: Poisson at 0.5 L plus 10 L during the middle third
func traffic(pattern string, seed uint64, limitPerUS float64, burst, durUS int64) ([]int64, error) {
	var level func(t float64) float64
	var peak float64
	d := float64(durUS)
	switch pattern {
	case "steady":
		level, peak = func(float64) float64 { return 1.2 }, 1.2
	case "burst":
		level, peak = func(float64) float64 { return 0.5 }, 0.5
	case "ramp":
		level, peak = func(t float64) float64 { return 3 * t / d }, 3
	case "attack":
		level = func(t float64) float64 {
			if t >= d/3 && t < 2*d/3 {
				return 10.5
			}
			return 0.5
		}
		peak = 10.5
	default:
		return nil, fmt.Errorf("unknown pattern %q (want steady, burst, ramp or attack)", pattern)
	}
	r := rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
	tooMany := errors.New("that traffic is over 200,000 requests; lower the rate or the duration")
	var out []int64
	// Thinning: candidates at the peak rate, each kept with level(t)/peak.
	for t := r.ExpFloat64() / (peak * limitPerUS); t < d; t += r.ExpFloat64() / (peak * limitPerUS) {
		if r.Float64()*peak < level(t) {
			if len(out) == maxEvents {
				return nil, tooMany
			}
			out = append(out, int64(t))
		}
	}
	if pattern == "burst" {
		width := min(100_000, durUS/20)
		for k := range int64(4) {
			start := (2*k + 1) * durUS / 8
			for range 2 * burst {
				if len(out) == maxEvents {
					return nil, tooMany
				}
				out = append(out, start+r.Int64N(width))
			}
		}
		slices.Sort(out)
	}
	return out, nil
}
