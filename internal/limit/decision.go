// Package limit is the rate-limiting domain: algorithms, parameters,
// decisions, the Limiter and Store ports, sentinel errors and key building.
// It imports the standard library only and does no I/O.
//
// Owner: workstream A (algorithms & policy). Signatures are frozen in
// docs/PLAN.md "Interfaces and key types"; change the plan before changing them.
package limit

import (
	"errors"
	"fmt"
	"time"
)

// Algorithm selects the limiter state machine. The numbering matches
// ratelimit.v1.Algorithm so conversions are a plain cast.
type Algorithm uint8

// Algorithm values.
const (
	AlgorithmUnspecified Algorithm = iota
	GCRA
	TokenBucket
	SlidingWindow
	SlidingWindowExact
)

var algorithmNames = [...]string{"unspecified", "gcra", "token_bucket", "sliding_window", "sliding_window_exact"}

// String returns the policies.yaml spelling: gcra, token_bucket, sliding_window, sliding_window_exact.
func (a Algorithm) String() string {
	if int(a) < len(algorithmNames) {
		return algorithmNames[a]
	}
	return fmt.Sprintf("algorithm(%d)", uint8(a))
}

// ParseAlgorithm maps the policies.yaml spelling to an Algorithm.
func ParseAlgorithm(s string) (Algorithm, error) {
	for i := 1; i < len(algorithmNames); i++ {
		if algorithmNames[i] == s {
			return Algorithm(i), nil
		}
	}
	return AlgorithmUnspecified, fmt.Errorf("%w: unknown algorithm %q", ErrInvalidParams, s)
}

// Params is a policy normalised at load time. Every duration crossing into an
// algorithm or a Lua script is integer microseconds (docs/PLAN.md, Decision 2).
type Params struct {
	Algo   Algorithm
	Rate   int64         // permitted events per Period
	Period time.Duration // e.g. time.Second, time.Minute
	Burst  int64         // capacity; GCRA dvt = Burst*EmissionUS, bucket cap = Burst
	// EmissionUS is Period/Rate in microseconds, precomputed; 0 is invalid.
	EmissionUS int64
}

// MaxExact is the largest product an algorithm may form: Lua numbers are
// IEEE doubles, exact only below 2^53, and the Go Step is the reference the
// Lua must match bit for bit (docs/PLAN.md, Decision 2).
const MaxExact int64 = 1 << 53

// microTokens mirrors tokenbucket.MicroTokens (the domain package cannot
// import the algorithm): the bucket scales tokens and emission by it.
const microTokens int64 = 1_000_000

// NewParams normalises a policy's numbers into Params, computing EmissionUS.
// It rejects rate <= 0, period < 1ms, burst < 1, an unspecified algorithm, an
// emission interval that rounds to 0us (more than one event per microsecond)
// and any combination whose algorithm would form a product at or above
// MaxExact: burst * emission (GCRA tolerance), burst * 1e6 micro-tokens and
// emission * 1e6 (token bucket refill), rate * period_us * 2 (sliding
// windows, the factor 2 covering the pending cost). Above them the Lua
// arithmetic silently loses digits and diverges from the Go reference.
func NewParams(algo Algorithm, rate int64, period time.Duration, burst int64) (Params, error) {
	var errs []error
	if algo == AlgorithmUnspecified || algo > SlidingWindowExact {
		errs = append(errs, fmt.Errorf("%w: algorithm %s", ErrInvalidParams, algo))
	}
	if rate <= 0 {
		errs = append(errs, fmt.Errorf("%w: rate must be > 0, got %d", ErrInvalidParams, rate))
	}
	if period < time.Millisecond {
		errs = append(errs, fmt.Errorf("%w: period must be >= 1ms, got %s", ErrInvalidParams, period))
	}
	if burst < 1 {
		errs = append(errs, fmt.Errorf("%w: burst must be >= 1, got %d", ErrInvalidParams, burst))
	}
	if len(errs) > 0 {
		return Params{}, errors.Join(errs...)
	}
	periodUS := period.Microseconds()
	emission := periodUS / rate
	if emission < 1 {
		return Params{}, fmt.Errorf("%w: rate %d per %s exceeds one event per microsecond", ErrInvalidParams, rate, period)
	}
	switch algo {
	case GCRA:
		if burst > MaxExact/emission {
			return Params{}, fmt.Errorf("%w: burst %d * emission %dus exceeds 2^53 (Lua exactness); lower burst or raise rate", ErrInvalidParams, burst, emission)
		}
	case TokenBucket:
		if burst > MaxExact/microTokens {
			return Params{}, fmt.Errorf("%w: token_bucket burst %d exceeds %d (2^53 micro-tokens)", ErrInvalidParams, burst, MaxExact/microTokens)
		}
		if emission > MaxExact/microTokens {
			return Params{}, fmt.Errorf("%w: token_bucket emission interval %dus exceeds %dus (2^53 micro-token refill); use gcra for rates below one event per 2.5h", ErrInvalidParams, emission, MaxExact/microTokens)
		}
	case SlidingWindow, SlidingWindowExact:
		if rate > MaxExact/(2*periodUS) {
			return Params{}, fmt.Errorf("%w: %s rate %d * period %s exceeds 2^52us (Lua exactness); shorten the period", ErrInvalidParams, algo, rate, period)
		}
	case AlgorithmUnspecified:
		// unreachable: rejected above
	}
	return Params{Algo: algo, Rate: rate, Period: period, Burst: burst, EmissionUS: emission}, nil
}

// Decision is the answer for one key (or the strictest across several).
type Decision struct {
	Allowed    bool
	Remaining  int
	RetryAfter time.Duration // 0 when Allowed
	ResetAfter time.Duration // time until the state is back to full capacity
	Limit      int           // the burst capacity the decision was made against
	Policy     string        // which policy produced it (the strictest, when several matched)
	Backend    string        // "redis" | "local" | "degraded"
}

// Backend names carried in Decision.Backend and metric labels.
const (
	BackendRedis    = "redis"
	BackendLocal    = "local"
	BackendDegraded = "degraded"
)

// Strictest folds several per-policy decisions into one: a deny wins over any
// allow; among equals the smaller Remaining wins, then the larger RetryAfter.
// An empty input yields the zero Decision (Allowed == false, Policy == "").
func Strictest(ds []Decision) Decision {
	if len(ds) == 0 {
		return Decision{}
	}
	out := ds[0]
	for _, d := range ds[1:] {
		if stricter(d, out) {
			out = d
		}
	}
	return out
}

func stricter(a, b Decision) bool {
	if a.Allowed != b.Allowed {
		return !a.Allowed
	}
	if a.Remaining != b.Remaining {
		return a.Remaining < b.Remaining
	}
	return a.RetryAfter > b.RetryAfter
}
