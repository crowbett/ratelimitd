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
	"fmt"
	"sync"
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
// Remaining is clamped at 0 (the Lua sketch's math.floor would go negative
// only when Burst shrinks under a live key). Invalid params (EmissionUS or
// Burst <= 0) deny without touching the state; cost <= 0 is a peek.
func Step(s State, p limit.Params, cost int, nowUS int64) (State, limit.Decision) {
	t := p.EmissionUS
	if t <= 0 || p.Burst <= 0 {
		return s, limit.Decision{}
	}
	dvt := p.Burst * t
	tat := int64(s)
	if tat < nowUS {
		tat = nowUS // an idle bucket past full is just full
	}
	n := int64(cost)
	if n < 0 {
		n = 0
	}
	newtat := tat + n*t
	diff := nowUS - (newtat - dvt)
	if diff < 0 {
		return s, limit.Decision{
			Allowed:    false,
			Remaining:  remaining(dvt-(tat-nowUS), t),
			RetryAfter: time.Duration(-diff) * time.Microsecond,
			ResetAfter: time.Duration(tat-nowUS) * time.Microsecond,
			Limit:      int(p.Burst),
		}
	}
	if n == 0 {
		return s, limit.Decision{
			Allowed:    true,
			Remaining:  remaining(dvt-(tat-nowUS), t),
			ResetAfter: time.Duration(tat-nowUS) * time.Microsecond,
			Limit:      int(p.Burst),
		}
	}
	return State(newtat), limit.Decision{
		Allowed:    true,
		Remaining:  remaining(dvt-(newtat-nowUS), t),
		ResetAfter: time.Duration(newtat-nowUS) * time.Microsecond,
		Limit:      int(p.Burst),
	}
}

func remaining(slackUS, t int64) int {
	if slackUS <= 0 {
		return 0
	}
	return int(slackUS / t)
}

// TTL is how long the state stays meaningful: (tat - now) plus nothing; a key
// idle past that is indistinguishable from a full bucket and can be expired.
func TTL(s State, _ limit.Params, nowUS int64) time.Duration {
	if int64(s) <= nowUS {
		return 0
	}
	return time.Duration(int64(s)-nowUS) * time.Microsecond
}

// Limiter is an in-process, single-policy limit.Limiter over Step, keyed by
// string. It is the building block of the local store and of Embedded mode;
// per-key state lives in one map under one mutex (not the hot path).
type Limiter struct {
	params limit.Params
	mu     sync.Mutex
	keys   map[string]State
}

// NewLimiter returns a Limiter for p (p.Algo must be limit.GCRA).
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
	if l.params.Algo != limit.GCRA || l.params.EmissionUS <= 0 || l.params.Burst <= 0 {
		return limit.Decision{}, fmt.Errorf("%w: gcra limiter needs a valid GCRA policy", limit.ErrInvalidParams)
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
