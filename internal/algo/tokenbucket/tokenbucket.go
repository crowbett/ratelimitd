// Package tokenbucket is the pure token bucket with lazy refill: two int64s of
// state (micro-tokens and the last refill instant in unix microseconds), no
// allocation, no I/O, no floats. Step is the reference model for
// lua/tokenbucket.lua (same integer arithmetic, elapsed clamped to the
// time-to-full so products stay far below 2^53).
//
// Owner: workstream A (algorithms & policy).
package tokenbucket

import (
	"context"
	"errors"
	"time"

	"github.com/crowbett/ratelimitd/internal/limit"
)

// MicroTokens is the scale of State.Tokens: one token = 1_000_000 micro-tokens.
const MicroTokens int64 = 1_000_000

// State is the bucket: Tokens in micro-tokens, Last is the refill instant in
// unix microseconds. The zero State is "never seen" and refills to full.
type State struct {
	Tokens int64
	Last   int64
}

// Step refills min(cap, tokens + elapsed*rate) in integer micro-tokens, then
// takes cost tokens if they fit. A denied call refills but never spends.
//
// STUB: returns s unchanged and the zero Decision.
func Step(s State, _ limit.Params, _ int, _ int64) (State, limit.Decision) {
	return s, limit.Decision{}
}

// TTL is the time until the bucket is full again; an idle key past that can
// be expired because a missing key means a full bucket.
//
// STUB: returns 0.
func TTL(_ State, _ limit.Params, _ int64) time.Duration { return 0 }

// Limiter is an in-process, single-policy limit.Limiter over Step.
type Limiter struct {
	params limit.Params
}

// NewLimiter returns a Limiter for p (p.Algo must be limit.TokenBucket).
func NewLimiter(p limit.Params) *Limiter { return &Limiter{params: p} }

// Params returns the policy the limiter enforces.
func (l *Limiter) Params() limit.Params { return l.params }

// Allow implements limit.Limiter. STUB.
func (l *Limiter) Allow(_ context.Context, _ string, _ int, _ time.Time) (limit.Decision, error) {
	return limit.Decision{}, errors.New("stub: tokenbucket.Limiter.Allow")
}
