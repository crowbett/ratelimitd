// Package slidingwindow holds two pure limiters: the weighted two-window
// approximation (default, three int64s of state) and the exact
// ring-of-timestamps variant (O(requests) memory; tests and small-limit
// policies only). Both are reference models for lua/slidingwindow.lua.
//
// The approximation denies when prev*(1 - elapsed/window) + cur + n > limit,
// computed as integer microseconds: prev*(window - elapsed) + (cur+n)*window
// > limit*window. It is the most clock-skew-sensitive algorithm (a skewed
// node can straddle a boundary early), which is part of why GCRA is the
// default.
//
// Owner: workstream A (algorithms & policy).
package slidingwindow

import (
	"context"
	"errors"
	"time"

	"github.com/crowbett/ratelimitd/internal/limit"
)

// State is the two-window approximation: Prev and Cur are the counts of the
// previous and current window, Index is the current window number
// (nowUS / period_us). The zero State is empty.
type State struct {
	Prev  int64
	Cur   int64
	Index int64
}

// Step evaluates cost units at nowUS against p (Rate per Period is the
// limit; Burst is ignored beyond Burst >= Rate validation). A denied call
// never mutates the counters.
//
// STUB: returns s unchanged and the zero Decision.
func Step(s State, _ limit.Params, _ int, _ int64) (State, limit.Decision) {
	return s, limit.Decision{}
}

// TTL is two periods from the start of the current window; after that both
// counters are irrelevant.
//
// STUB: returns 0.
func TTL(_ State, _ limit.Params, _ int64) time.Duration { return 0 }

// ExactState is the exact variant: every admitted timestamp (unix
// microseconds) inside the trailing window, oldest first.
type ExactState struct {
	Stamps []int64
}

// StepExact drops stamps older than nowUS - period, then admits cost units
// iff len(stamps)+cost <= Rate, appending cost copies of nowUS. It is the
// only Step that allocates.
//
// STUB: returns s unchanged and the zero Decision.
func StepExact(s ExactState, _ limit.Params, _ int, _ int64) (ExactState, limit.Decision) {
	return s, limit.Decision{}
}

// TTLExact is one period after the newest stamp.
//
// STUB: returns 0.
func TTLExact(_ ExactState, _ limit.Params, _ int64) time.Duration { return 0 }

// Limiter is an in-process, single-policy limit.Limiter that picks Step or
// StepExact from p.Algo (limit.SlidingWindow or limit.SlidingWindowExact).
type Limiter struct {
	params limit.Params
}

// NewLimiter returns a Limiter for p.
func NewLimiter(p limit.Params) *Limiter { return &Limiter{params: p} }

// Params returns the policy the limiter enforces.
func (l *Limiter) Params() limit.Params { return l.params }

// Allow implements limit.Limiter. STUB.
func (l *Limiter) Allow(_ context.Context, _ string, _ int, _ time.Time) (limit.Decision, error) {
	return limit.Decision{}, errors.New("stub: slidingwindow.Limiter.Allow")
}
