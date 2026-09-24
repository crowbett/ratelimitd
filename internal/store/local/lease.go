package local

import (
	"context"
	"fmt"
	"time"

	"github.com/crowbett/ratelimitd/internal/limit"
)

// Lease grants between req.Cost and want units of req's policy in one step
// (limit.Grant) and commits the grant. It is the degraded budget source of
// internal/store/lease while Redis is unavailable: on a Scale view the policy
// is divided by the divisor like every other degraded key. The decision's
// Backend is this view's Name.
func (b *Backend) Lease(_ context.Context, req limit.Req, want int, now time.Time) (limit.Decision, int, error) {
	if req.Params.EmissionUS <= 0 || req.Params.Algo == limit.AlgorithmUnspecified || req.Params.Algo == limit.SlidingWindowExact {
		return limit.Decision{}, 0, fmt.Errorf("%w: lease on %s (%s)", limit.ErrInvalidParams, req.Params.Algo, req.Policy)
	}
	nowUS := now.UnixMicro()
	b.core.observe(nowUS)
	p := b.params(req.Params)
	sh := b.core.shardOf(req.Key)
	sh.mu.Lock()
	old, had := sh.m[req.Key]
	ne, d, got := limit.Grant(func(cost int) (entry, limit.Decision) { return step(old, p, cost, nowUS) }, max(req.Cost, 1), want)
	if got > 0 {
		b.core.store(sh, req.Key, ne, p, nowUS, had)
	}
	sh.mu.Unlock()
	d.Policy, d.Backend = req.Policy, b.Name()
	return d, got, nil
}
