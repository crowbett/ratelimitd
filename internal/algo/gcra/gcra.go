// Package gcra is the pure Generic Cell Rate Algorithm: one int64 of state
// (the theoretical arrival time in unix microseconds), no allocation, no I/O,
// no time.Time math. Step is the reference model for lua/gcra.lua; the
// differential test in internal/store/redisx drives both from the same random
// operation sequence and asserts identical states and decisions.
//
// Owner: workstream A (algorithms & policy).
package gcra

import (
	"context"
	"errors"
	"time"

	"github.com/crowbett/ratelimitd/internal/limit"
)

// State is the theoretical arrival time (TAT) in unix microseconds; 0 = empty.
type State int64

// Step evaluates cost units at nowUS against p and returns the new state and
// the decision. A denied call never advances the state (no partial spend).
// Arithmetic: dvt = Burst*EmissionUS; tat = max(s, now); newtat = tat + cost*T;
// allowed iff now - (newtat - dvt) >= 0. Remaining, RetryAfter and ResetAfter
// come from the same integers; nothing is a float.
//
// STUB: returns s unchanged and the zero Decision.
func Step(s State, _ limit.Params, _ int, _ int64) (State, limit.Decision) {
	return s, limit.Decision{}
}

// TTL is how long the state stays meaningful: (tat - now) plus nothing; a key
// idle past that is indistinguishable from a full bucket and can be expired.
//
// STUB: returns 0.
func TTL(_ State, _ limit.Params, _ int64) time.Duration { return 0 }

// Limiter is an in-process, single-policy limit.Limiter over Step, keyed by
// string. It is the building block of the local store and of Embedded mode.
type Limiter struct {
	params limit.Params
}

// NewLimiter returns a Limiter for p (p.Algo must be limit.GCRA).
func NewLimiter(p limit.Params) *Limiter { return &Limiter{params: p} }

// Params returns the policy the limiter enforces.
func (l *Limiter) Params() limit.Params { return l.params }

// Allow implements limit.Limiter. STUB.
func (l *Limiter) Allow(_ context.Context, _ string, _ int, _ time.Time) (limit.Decision, error) {
	return limit.Decision{}, errors.New("stub: gcra.Limiter.Allow")
}
