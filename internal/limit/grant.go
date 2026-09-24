package limit

// Grant is the lease rule every backend implements identically (the local
// backend in Go, lua/dispatch.lua mode 'L' in Redis): hand out as many units
// of one key as fit, between lo and want, in ONE step of the algorithm.
//
// step evaluates cost units against the key's current state and returns the
// would-be state and the decision (it must not mutate anything). Grant tries
// want; if that denies and the denial reports Remaining >= lo it takes exactly
// Remaining (which fits: for every algorithm here Remaining is the largest
// cost that would allow); otherwise it reports the denial evaluated at lo, so
// RetryAfter answers "when can the caller get its minimum". granted is 0 on a
// denial, and next must only be stored when granted > 0.
func Grant[S any](step func(cost int) (S, Decision), lo, want int) (next S, d Decision, granted int) {
	want = max(want, lo)
	if next, d = step(want); d.Allowed {
		return next, d, want
	}
	granted = lo
	if d.Remaining >= lo {
		granted = d.Remaining
	}
	if next, d = step(granted); !d.Allowed {
		return next, d, 0
	}
	return next, d, granted
}
