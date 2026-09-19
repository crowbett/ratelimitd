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
	"fmt"
	"sort"
	"sync"
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
// never mutates the counters. Index never moves backwards: a call from a
// clock that is behind is pinned to the start of the stored window, which is
// the strict direction. Invalid params deny without touching the state;
// cost <= 0 is a peek.
func Step(s State, p limit.Params, cost int, nowUS int64) (State, limit.Decision) {
	w := p.Period.Microseconds()
	if w <= 0 || p.Rate <= 0 {
		return s, limit.Decision{}
	}
	idx := nowUS / w
	elapsed := nowUS - idx*w
	var prev, cur int64
	switch {
	case idx == s.Index:
		prev, cur = s.Prev, s.Cur
	case idx == s.Index+1:
		prev, cur = s.Cur, 0
	case idx > s.Index+1:
		prev, cur = 0, 0
	default: // clock behind the stored window
		idx, elapsed = s.Index, 0
		prev, cur = s.Prev, s.Cur
	}
	n := int64(cost)
	if n < 0 {
		n = 0
	}
	avail := p.Rate*w - prev*(w-elapsed) - cur*w // scaled by w
	d := limit.Decision{Limit: int(p.Rate)}
	if n*w > avail {
		d.Remaining = remaining(avail, w)
		d.RetryAfter = retryAfter(n*w-avail, prev, w-elapsed)
		d.ResetAfter = resetAfter(prev, cur, w, elapsed)
		return s, d
	}
	d.Allowed = true
	if n == 0 {
		d.Remaining = remaining(avail, w)
		d.ResetAfter = resetAfter(prev, cur, w, elapsed)
		return s, d
	}
	cur += n
	d.Remaining = remaining(avail-n*w, w)
	d.ResetAfter = resetAfter(prev, cur, w, elapsed)
	return State{Prev: prev, Cur: cur, Index: idx}, d
}

func remaining(availScaled, w int64) int {
	if availScaled <= 0 {
		return 0
	}
	return int(availScaled / w)
}

// retryAfter is the time until the weighted previous count has decayed by
// excess/w events, capped at the window boundary (after which cur rolls over
// and the caller re-asks).
func retryAfter(excessScaled, prev, toBoundary int64) time.Duration {
	if prev > 0 {
		r := (excessScaled + prev - 1) / prev
		if r < toBoundary {
			if r < 1 {
				r = 1
			}
			return time.Duration(r) * time.Microsecond
		}
	}
	return time.Duration(toBoundary) * time.Microsecond
}

// resetAfter is the time until both counters have left the trailing window.
func resetAfter(prev, cur, w, elapsed int64) time.Duration {
	switch {
	case cur > 0:
		return time.Duration(2*w-elapsed) * time.Microsecond
	case prev > 0:
		return time.Duration(w-elapsed) * time.Microsecond
	}
	return 0
}

// TTL is two periods from the start of the current window; after that both
// counters are irrelevant.
func TTL(s State, p limit.Params, nowUS int64) time.Duration {
	w := p.Period.Microseconds()
	if w <= 0 || (s.Prev == 0 && s.Cur == 0) {
		return 0
	}
	end := (s.Index+2)*w - nowUS
	if end <= 0 {
		return 0
	}
	return time.Duration(end) * time.Microsecond
}

// ExactState is the exact variant: every admitted timestamp (unix
// microseconds) inside the trailing window, oldest first.
type ExactState struct {
	Stamps []int64
}

// StepExact drops stamps older than nowUS - period, then admits cost units
// iff len(stamps)+cost <= Rate, appending cost copies of nowUS. It is the
// only Step that allocates: an admitted call returns a fresh slice, a denied
// call returns s untouched (the pruning happens on the next admission).
func StepExact(s ExactState, p limit.Params, cost int, nowUS int64) (ExactState, limit.Decision) {
	w := p.Period.Microseconds()
	if w <= 0 || p.Rate <= 0 {
		return s, limit.Decision{}
	}
	cut := nowUS - w // a stamp is live while stamp > now - w
	first := sort.Search(len(s.Stamps), func(i int) bool { return s.Stamps[i] > cut })
	kept := s.Stamps[first:]
	live := int64(len(kept))
	n := int64(cost)
	if n < 0 {
		n = 0
	}
	d := limit.Decision{Limit: int(p.Rate)}
	if live > 0 {
		d.ResetAfter = time.Duration(kept[live-1]+w-nowUS) * time.Microsecond
	}
	if live+n > p.Rate {
		d.Remaining = int(max(p.Rate-live, 0))
		k := live + n - p.Rate // stamps that must expire first
		if k > live {
			d.RetryAfter = time.Duration(w) * time.Microsecond
		} else {
			d.RetryAfter = time.Duration(max(kept[k-1]+w-nowUS, 1)) * time.Microsecond
		}
		return s, d
	}
	d.Allowed = true
	if n == 0 {
		d.Remaining = int(p.Rate - live)
		return s, d
	}
	stamp := nowUS
	if live > 0 && kept[live-1] > stamp {
		stamp = kept[live-1] // clock behind: never break the ordering
	}
	out := make([]int64, live+n)
	copy(out, kept)
	for i := live; i < live+n; i++ {
		out[i] = stamp
	}
	d.Remaining = int(p.Rate - live - n)
	d.ResetAfter = time.Duration(stamp+w-nowUS) * time.Microsecond
	return ExactState{Stamps: out}, d
}

// TTLExact is one period after the newest stamp.
func TTLExact(s ExactState, p limit.Params, nowUS int64) time.Duration {
	w := p.Period.Microseconds()
	if w <= 0 || len(s.Stamps) == 0 {
		return 0
	}
	end := s.Stamps[len(s.Stamps)-1] + w - nowUS
	if end <= 0 {
		return 0
	}
	return time.Duration(end) * time.Microsecond
}

// Limiter is an in-process, single-policy limit.Limiter that picks Step or
// StepExact from p.Algo (limit.SlidingWindow or limit.SlidingWindowExact).
// Per-key state lives in one map under one mutex (not the hot path).
type Limiter struct {
	params limit.Params
	mu     sync.Mutex
	approx map[string]State
	exact  map[string]ExactState
}

// NewLimiter returns a Limiter for p.
func NewLimiter(p limit.Params) *Limiter {
	l := &Limiter{params: p}
	if p.Algo == limit.SlidingWindowExact {
		l.exact = make(map[string]ExactState)
	} else {
		l.approx = make(map[string]State)
	}
	return l
}

// Params returns the policy the limiter enforces.
func (l *Limiter) Params() limit.Params { return l.params }

// Len reports how many keys hold state.
func (l *Limiter) Len() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.approx) + len(l.exact)
}

// Reset drops the state behind key; a missing key is not an error.
func (l *Limiter) Reset(key string) {
	l.mu.Lock()
	delete(l.approx, key)
	delete(l.exact, key)
	l.mu.Unlock()
}

// Allow implements limit.Limiter: n units for key at now (the caller's clock).
// Decision.Backend is limit.BackendLocal; Policy is left empty.
func (l *Limiter) Allow(_ context.Context, key string, n int, now time.Time) (limit.Decision, error) {
	if n < 1 {
		return limit.Decision{}, fmt.Errorf("%w: cost must be >= 1, got %d", limit.ErrInvalidParams, n)
	}
	if l.params.Period.Microseconds() <= 0 || l.params.Rate <= 0 {
		return limit.Decision{}, fmt.Errorf("%w: slidingwindow limiter needs a valid policy", limit.ErrInvalidParams)
	}
	nowUS := now.UnixMicro()
	var d limit.Decision
	l.mu.Lock()
	switch l.params.Algo {
	case limit.SlidingWindowExact:
		var next ExactState
		next, d = StepExact(l.exact[key], l.params, n, nowUS)
		if d.Allowed {
			l.exact[key] = next
		}
	case limit.SlidingWindow:
		var next State
		next, d = Step(l.approx[key], l.params, n, nowUS)
		if d.Allowed {
			l.approx[key] = next
		}
	default:
		l.mu.Unlock()
		return limit.Decision{}, fmt.Errorf("%w: slidingwindow limiter cannot run %s", limit.ErrInvalidParams, l.params.Algo)
	}
	l.mu.Unlock()
	d.Backend = limit.BackendLocal
	return d, nil
}
