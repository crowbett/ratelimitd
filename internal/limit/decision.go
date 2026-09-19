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

// NewParams normalises a policy's numbers into Params, computing EmissionUS.
// It rejects rate <= 0, period < 1ms, burst < 1, an unspecified algorithm and
// an emission interval that rounds to 0us (more than one event per microsecond).
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
	emission := period.Microseconds() / rate
	if emission < 1 {
		return Params{}, fmt.Errorf("%w: rate %d per %s exceeds one event per microsecond", ErrInvalidParams, rate, period)
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
