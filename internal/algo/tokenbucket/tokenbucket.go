// Package tokenbucket is the pure token bucket with lazy refill: two int64s of
// state (micro-tokens and the last refill instant in unix microseconds), no
// allocation, no I/O, no floats. Step is the reference model for
// lua/tokenbucket.lua (same integer arithmetic, elapsed clamped to the
// time-to-full so products stay far below 2^53).
//
// Refill arithmetic, all integer: one token is earned every EmissionUS
// microseconds. whole = elapsed / EmissionUS (if whole >= Burst the bucket is
// full, which is the elapsed clamp); frac = (elapsed % EmissionUS) *
// MicroTokens / EmissionUS. The largest intermediate is EmissionUS *
// MicroTokens, below 2^53 for any emission interval up to ~9000 s.
//
// Owner: workstream A (algorithms & policy).
package tokenbucket

import (
	"context"
	"fmt"
	"sync"
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
// takes cost tokens if they fit. A denied call reports the refilled view but
// returns s unchanged, so a dry run never writes (docs/PLAN.md, Decision 4).
// Invalid params deny without touching the state; cost <= 0 is a peek.
func Step(s State, p limit.Params, cost int, nowUS int64) (State, limit.Decision) {
	if p.EmissionUS <= 0 || p.Burst <= 0 {
		return s, limit.Decision{}
	}
	capacity := p.Burst * MicroTokens
	tokens, last := refill(s, p, nowUS, capacity)
	need := int64(cost) * MicroTokens
	if need < 0 {
		need = 0
	}
	if tokens < need {
		return s, limit.Decision{
			Allowed:    false,
			Remaining:  int(tokens / MicroTokens),
			RetryAfter: timeFor(need-tokens, p.EmissionUS),
			ResetAfter: timeFor(capacity-tokens, p.EmissionUS),
			Limit:      int(p.Burst),
		}
	}
	if need == 0 {
		return s, limit.Decision{
			Allowed:    true,
			Remaining:  int(tokens / MicroTokens),
			ResetAfter: timeFor(capacity-tokens, p.EmissionUS),
			Limit:      int(p.Burst),
		}
	}
	tokens -= need
	return State{Tokens: tokens, Last: last}, limit.Decision{
		Allowed:    true,
		Remaining:  int(tokens / MicroTokens),
		ResetAfter: timeFor(capacity-tokens, p.EmissionUS),
		Limit:      int(p.Burst),
	}
}

// refill returns the bucket as seen at nowUS without spending. Last never
// moves backwards: a call from a clock that is behind sees no refill.
func refill(s State, p limit.Params, nowUS, capacity int64) (tokens, last int64) {
	if s.Last == 0 {
		return capacity, nowUS
	}
	if nowUS <= s.Last {
		return min(s.Tokens, capacity), s.Last
	}
	elapsed := nowUS - s.Last
	whole := elapsed / p.EmissionUS
	if whole >= p.Burst {
		return capacity, nowUS
	}
	tokens = s.Tokens + whole*MicroTokens + (elapsed%p.EmissionUS)*MicroTokens/p.EmissionUS
	return min(tokens, capacity), nowUS
}

// timeFor is the time to earn mt micro-tokens, rounded up, without forming
// mt*EmissionUS in one product.
func timeFor(mt, emissionUS int64) time.Duration {
	if mt <= 0 {
		return 0
	}
	whole := mt / MicroTokens
	rem := mt % MicroTokens
	us := whole*emissionUS + (rem*emissionUS+MicroTokens-1)/MicroTokens
	return time.Duration(us) * time.Microsecond
}

// TTL is the time until the bucket is full again; an idle key past that can
// be expired because a missing key means a full bucket.
func TTL(s State, p limit.Params, nowUS int64) time.Duration {
	if p.EmissionUS <= 0 || p.Burst <= 0 || s.Last == 0 {
		return 0
	}
	capacity := p.Burst * MicroTokens
	tokens, _ := refill(s, p, nowUS, capacity)
	return timeFor(capacity-tokens, p.EmissionUS)
}

// Limiter is an in-process, single-policy limit.Limiter over Step; per-key
// state lives in one map under one mutex (not the hot path).
type Limiter struct {
	params limit.Params
	mu     sync.Mutex
	keys   map[string]State
}

// NewLimiter returns a Limiter for p (p.Algo must be limit.TokenBucket).
func NewLimiter(p limit.Params) *Limiter {
	return &Limiter{params: p, keys: make(map[string]State)}
}

// Params returns the policy the limiter enforces.
func (l *Limiter) Params() limit.Params { return l.params }

// Len reports how many keys hold state.
func (l *Limiter) Len() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.keys)
}

// Reset drops the state behind key; a missing key is not an error.
func (l *Limiter) Reset(key string) {
	l.mu.Lock()
	delete(l.keys, key)
	l.mu.Unlock()
}

// Allow implements limit.Limiter: n units for key at now (the caller's clock).
// Decision.Backend is limit.BackendLocal; Policy is left empty.
func (l *Limiter) Allow(_ context.Context, key string, n int, now time.Time) (limit.Decision, error) {
	if n < 1 {
		return limit.Decision{}, fmt.Errorf("%w: cost must be >= 1, got %d", limit.ErrInvalidParams, n)
	}
	if l.params.Algo != limit.TokenBucket || l.params.EmissionUS <= 0 || l.params.Burst <= 0 {
		return limit.Decision{}, fmt.Errorf("%w: tokenbucket limiter needs a valid token_bucket policy", limit.ErrInvalidParams)
	}
	nowUS := now.UnixMicro()
	l.mu.Lock()
	next, d := Step(l.keys[key], l.params, n, nowUS)
	if d.Allowed {
		l.keys[key] = next
	}
	l.mu.Unlock()
	d.Backend = limit.BackendLocal
	return d, nil
}
