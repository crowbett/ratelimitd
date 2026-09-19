package local

import (
	"time"

	"github.com/crowbett/ratelimitd/internal/algo/gcra"
	"github.com/crowbett/ratelimitd/internal/algo/slidingwindow"
	"github.com/crowbett/ratelimitd/internal/algo/tokenbucket"
	"github.com/crowbett/ratelimitd/internal/limit"
)

// entry is the per-key state: a small tagged union over the algorithm
// states plus the last-touched instant for the sweeper.
//
//	gcra:            a = TAT
//	token bucket:    a = micro-tokens, b = last refill
//	sliding window:  a = prev, b = cur, c = index
//	exact:           stamps
type entry struct {
	algo     limit.Algorithm
	a, b, c  int64
	stamps   []int64
	lastSeen int64 // unix microseconds of the last commit
	expires  int64 // unix microseconds after which the state equals an empty key
}

// step evaluates cost at nowUS against p from the state in e (an entry whose
// algo differs from p.Algo is treated as empty: the policy changed algorithm)
// and returns the would-be new entry and the decision. It never mutates e.
func step(e entry, p limit.Params, cost int, nowUS int64) (entry, limit.Decision) {
	if e.algo != p.Algo {
		e = entry{algo: p.Algo}
	}
	var d limit.Decision
	switch p.Algo {
	case limit.GCRA:
		var s gcra.State
		s, d = gcra.Step(gcra.State(e.a), p, cost, nowUS)
		e.a = int64(s)
	case limit.TokenBucket:
		var s tokenbucket.State
		s, d = tokenbucket.Step(tokenbucket.State{Tokens: e.a, Last: e.b}, p, cost, nowUS)
		e.a, e.b = s.Tokens, s.Last
	case limit.SlidingWindow:
		var s slidingwindow.State
		s, d = slidingwindow.Step(slidingwindow.State{Prev: e.a, Cur: e.b, Index: e.c}, p, cost, nowUS)
		e.a, e.b, e.c = s.Prev, s.Cur, s.Index
	case limit.SlidingWindowExact:
		var s slidingwindow.ExactState
		s, d = slidingwindow.StepExact(slidingwindow.ExactState{Stamps: e.stamps}, p, cost, nowUS)
		e.stamps = s.Stamps
	case limit.AlgorithmUnspecified:
	}
	return e, d
}

// ttl is how long e stays meaningful after nowUS; <= 0 means the state is
// indistinguishable from an empty key.
func ttl(e entry, p limit.Params, nowUS int64) time.Duration {
	switch e.algo {
	case limit.GCRA:
		return gcra.TTL(gcra.State(e.a), p, nowUS)
	case limit.TokenBucket:
		return tokenbucket.TTL(tokenbucket.State{Tokens: e.a, Last: e.b}, p, nowUS)
	case limit.SlidingWindow:
		return slidingwindow.TTL(slidingwindow.State{Prev: e.a, Cur: e.b, Index: e.c}, p, nowUS)
	case limit.SlidingWindowExact:
		return slidingwindow.TTLExact(slidingwindow.ExactState{Stamps: e.stamps}, p, nowUS)
	case limit.AlgorithmUnspecified:
	}
	return 0
}
