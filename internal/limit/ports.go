package limit

import (
	"context"
	"time"
)

// Limiter is the single-key port: one policy, one key, n units at time now.
// The caller's clock is authoritative (docs/PLAN.md, Decision 3): now is a
// parameter, never read inside.
type Limiter interface {
	Allow(ctx context.Context, key string, n int, now time.Time) (Decision, error)
}

// Req is one key to evaluate. Keys carry their own Params because a single
// Apply may span several policies for one subject (all-or-nothing, Decision 4).
type Req struct {
	Key    string // already rendered: rl:{t=<tenant>|s=<subject>}:p=<policy>
	Params Params
	Cost   int    // >= 1
	Policy string // policy name, copied into Decision.Policy
}

// Store is the backend port consumed by the service. Every implementation
// (local, redisx, fallback) evaluates all reqs of one call atomically:
// dry-run every key, commit only if every key allowed.
type Store interface {
	// Apply evaluates and commits. len(out) == len(reqs) on nil error; out[i]
	// answers reqs[i]. When any key denies, no key is mutated and every
	// Decision reports the dry-run view.
	Apply(ctx context.Context, reqs []Req, now time.Time) ([]Decision, error)
	// Peek is Apply without state mutation (dry-run only).
	Peek(ctx context.Context, reqs []Req, now time.Time) ([]Decision, error)
	// Reset deletes the state behind keys; missing keys are not an error.
	Reset(ctx context.Context, keys []string) error
	// Name is the label reported in Decision.Backend and metrics.
	Name() string
}
