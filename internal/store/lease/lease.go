// Package lease serves shared policies (limit.Req.Shared: one counter across
// every subject) from budget that each node leases in chunks from Redis,
// while every other req of the call goes to the wrapped subject store in one
// script call as before.
//
// Why a lease and not a key in the subject's call: a subject's keys share the
// hash tag {t=<tenant>|s=<subject>} so one script can evaluate them
// all-or-nothing; a counter shared by every subject cannot live in every
// subject's slot. Hitting the shared key on every request would also make it
// the hottest key in the cluster (one slot, one Redis thread). Instead a node
// takes `chunk` units from the shared key in ONE call (dispatch.lua mode 'L',
// the limit.Grant rule), spends them locally under a mutex, and asks again
// when they run out. Leased units expire after TTL, so a node that goes quiet
// cannot sit on budget and spend it in a burst later.
//
// The bound (docs/PLAN.md "Shared global counter"): every admitted unit was
// first consumed from the shared key's algorithm in Redis, so over any
// interval [t, t+W] a node can only spend units leased in [t-TTL, t+W]:
// admitted <= rate*(W + TTL)/period + burst across ALL nodes, and exactly the
// algorithm's own bound (rate*W + burst) measured from an empty key. Nothing
// over-admits; the price is under-admission of at most one unspent chunk per
// node per TTL, and a cost-sized remainder dropped at each refill.
//
// Saturation: a denied refill makes the pool deny locally for
// max(RetryAfter, Span), so a saturated shared key costs each node at most
// one Redis call per Span for denials plus one per granted chunk, instead
// of one per request. This only ever denies more (never over-admits); the
// algorithm keeps accruing budget meanwhile, up to its burst, so with
// rate*Span <= burst nothing is lost, the next ask takes the accrued chunk.
//
// All-or-nothing is kept: shared units are taken first; if one is denied the
// subject store is not called at all (its reqs are reported as not evaluated,
// see Apply) and the taken units are returned; if the subject call denies or
// fails, the taken units are returned to their pool. No goroutine is started:
// refills run on the caller's goroutine, one at a time per pool.
package lease

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/crowbett/ratelimitd/internal/limit"
)

// Source hands out budget for one shared key: between req.Cost and want units
// in one step (limit.Grant), returning the decision and the granted count
// (0 on a denial). redisx.Backend is the primary source, a local.Backend
// Scale view the degraded one.
type Source interface {
	Lease(ctx context.Context, req limit.Req, want int, now time.Time) (limit.Decision, int, error)
}

// Config tunes the lease size and lifetime.
type Config struct {
	// Span sizes a lease: rate*Span/period units (at least the request's
	// cost, at most the policy's capacity). Default 10ms: at 200k/s that is
	// 2000 units, so a saturated node asks Redis ~100 times a second per key.
	Span time.Duration
	// TTL is how long leased units stay spendable. Default 100ms. It is the
	// W+TTL term of the bound and the longest a quiet node can strand units.
	TTL time.Duration
}

// Stats are the counters behind the lease path.
type Stats struct {
	Leases         int64 // refills answered by the primary source
	DegradedLeases int64 // refills answered by the degraded source (breaker open or primary down)
	Denied         int64 // refills that granted nothing
	Refunds        int64 // takes returned because the rest of the call denied or failed
}

// Store wraps the subject store. It implements limit.Store and, when inner
// does, limit.BatchStore.
type Store struct {
	inner    limit.Store
	primary  Source
	degraded Source
	open     func() bool
	cfg      Config

	mu    sync.RWMutex
	pools map[string]*pool

	leases, degradedLeases, denied, refunds atomic.Int64
}

// pool is one shared key's local budget.
type pool struct {
	mu      sync.Mutex
	tokens  int            // units leased and not yet spent
	expires int64          // unix us after which tokens are void
	gen     uint64         // bumped on every refill: a refund only returns to the lease it came from
	last    limit.Decision // the last refill's reply: Limit, ResetAfter, Backend
	// backoff: after a denied refill the pool answers denials locally until
	// this instant (unix us) instead of asking Redis on every request.
	backoff int64
}

// ticket is one take, for a refund.
type ticket struct {
	p   *pool
	gen uint64
	n   int
}

// New wraps inner. primary is the Redis source; degraded answers while open()
// reports the breaker open or when primary fails with limit.ErrBackendDown
// (a local.Backend Scale view in FALLBACK_MODE=local, FailOpen or FailClosed
// otherwise). open may be nil.
func New(inner limit.Store, primary, degraded Source, open func() bool, cfg Config) *Store {
	if cfg.Span <= 0 {
		cfg.Span = 10 * time.Millisecond
	}
	if cfg.TTL <= 0 {
		cfg.TTL = 100 * time.Millisecond
	}
	if open == nil {
		open = func() bool { return false }
	}
	return &Store{inner: inner, primary: primary, degraded: degraded, open: open, cfg: cfg, pools: make(map[string]*pool)}
}

// Stats reads the counters.
func (s *Store) Stats() Stats {
	return Stats{Leases: s.leases.Load(), DegradedLeases: s.degradedLeases.Load(), Denied: s.denied.Load(), Refunds: s.refunds.Load()}
}

// Name implements limit.Store: the subject store's name.
func (s *Store) Name() string { return s.inner.Name() }

// Apply implements limit.Store. Shared reqs are served from their pools,
// the rest go to inner in one call. When a shared req denies, inner is not
// called: the subject reqs' Decisions are Allowed with Backend "skipped"
// (not evaluated, nothing consumed), so limit.Strictest always reports the
// shared denial, and the request cost no Redis round trip at all.
func (s *Store) Apply(ctx context.Context, reqs []limit.Req, now time.Time) ([]limit.Decision, error) {
	nShared := countShared(reqs)
	if nShared == 0 {
		return s.inner.Apply(ctx, reqs, now)
	}
	out := make([]limit.Decision, len(reqs))
	tickets, ok, err := s.takeAll(ctx, reqs, out, now)
	if err != nil {
		return nil, err
	}
	rest, idx := subjectReqs(reqs, nShared)
	if !ok {
		skip(reqs, idx, out)
		return out, nil
	}
	if len(rest) == 0 {
		return out, nil
	}
	ds, err := s.inner.Apply(ctx, rest, now)
	if err != nil {
		s.refund(tickets, now)
		return nil, err
	}
	s.merge(ds, idx, out, tickets, now)
	return out, nil
}

// ApplyMany implements limit.BatchStore: shared takes per group first, then
// one inner.ApplyMany (or one Apply per group when inner cannot batch) for
// the groups whose shared reqs all allowed.
func (s *Store) ApplyMany(ctx context.Context, groups [][]limit.Req, now time.Time) (out [][]limit.Decision, errs []error, err error) {
	out = make([][]limit.Decision, len(groups))
	errs = make([]error, len(groups))
	type pending struct {
		group   int
		idx     []int
		tickets []ticket
	}
	var (
		live    [][]limit.Req
		waiting []pending
	)
	for g, reqs := range groups {
		nShared := countShared(reqs)
		if nShared == 0 {
			live = append(live, reqs)
			waiting = append(waiting, pending{group: g})
			continue
		}
		ds := make([]limit.Decision, len(reqs))
		tickets, ok, e := s.takeAll(ctx, reqs, ds, now)
		if e != nil {
			errs[g] = e
			continue
		}
		rest, idx := subjectReqs(reqs, nShared)
		out[g] = ds
		if !ok {
			skip(reqs, idx, ds)
			continue
		}
		if len(rest) == 0 {
			continue
		}
		live = append(live, rest)
		waiting = append(waiting, pending{group: g, idx: idx, tickets: tickets})
	}
	if len(live) == 0 {
		return out, errs, nil
	}
	var (
		dss   [][]limit.Decision
		lerrs []error
	)
	if bs, ok := s.inner.(limit.BatchStore); ok {
		dss, lerrs, err = bs.ApplyMany(ctx, live, now)
		if err != nil {
			for _, w := range waiting {
				s.refund(w.tickets, now)
			}
			return nil, nil, err
		}
	} else {
		dss, lerrs = make([][]limit.Decision, len(live)), make([]error, len(live))
		for i, reqs := range live {
			dss[i], lerrs[i] = s.inner.Apply(ctx, reqs, now)
		}
	}
	for i, w := range waiting {
		if lerrs[i] != nil {
			s.refund(w.tickets, now)
			out[w.group], errs[w.group] = nil, lerrs[i]
			continue
		}
		if w.idx == nil { // no shared reqs: inner's answer as is
			out[w.group] = dss[i]
			continue
		}
		s.merge(dss[i], w.idx, out[w.group], w.tickets, now)
	}
	return out, errs, nil
}

// Peek implements limit.Store without leasing: subject reqs through inner in
// one call, each shared req through inner on its own (one key, one slot).
func (s *Store) Peek(ctx context.Context, reqs []limit.Req, now time.Time) ([]limit.Decision, error) {
	nShared := countShared(reqs)
	if nShared == 0 {
		return s.inner.Peek(ctx, reqs, now)
	}
	out := make([]limit.Decision, len(reqs))
	for i := range reqs {
		if !reqs[i].Shared {
			continue
		}
		ds, err := s.inner.Peek(ctx, reqs[i:i+1], now)
		if err != nil {
			return nil, err
		}
		out[i] = ds[0]
	}
	rest, idx := subjectReqs(reqs, nShared)
	if len(rest) > 0 {
		ds, err := s.inner.Peek(ctx, rest, now)
		if err != nil {
			return nil, err
		}
		for j, i := range idx {
			out[i] = ds[j]
		}
	}
	return out, nil
}

// Reset implements limit.Store: inner's keys, plus any local pool of them
// or of their templated variants.
func (s *Store) Reset(ctx context.Context, keys []string) error {
	s.mu.Lock()
	for k := range s.pools {
		for _, key := range keys {
			if k == key || limit.IsVariant(k, key) {
				delete(s.pools, k)
				break
			}
		}
	}
	s.mu.Unlock()
	return s.inner.Reset(ctx, keys)
}

// takeAll takes every shared req of one call in order, writing each decision
// into out. ok is false when one denied (the earlier takes are refunded).
func (s *Store) takeAll(ctx context.Context, reqs []limit.Req, out []limit.Decision, now time.Time) (tickets []ticket, ok bool, err error) {
	for i := range reqs {
		if !reqs[i].Shared {
			continue
		}
		d, t, e := s.take(ctx, &reqs[i], now)
		if e != nil {
			s.refund(tickets, now)
			return nil, false, e
		}
		out[i] = d
		if !d.Allowed {
			s.refund(tickets, now)
			return nil, false, nil
		}
		tickets = append(tickets, t)
	}
	return tickets, true, nil
}

// take spends req.Cost units from the key's pool, refilling it first when it
// is short or expired.
func (s *Store) take(ctx context.Context, r *limit.Req, now time.Time) (limit.Decision, ticket, error) {
	p := s.pool(r.Key)
	cost := max(r.Cost, 1)
	nowUS := now.UnixMicro()
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.expires <= nowUS {
		p.tokens = 0
	}
	if p.tokens < cost && nowUS < p.backoff {
		// Saturated: the last refill was denied and its retry (or Span) has
		// not elapsed. Deny without a round trip; this is what keeps a
		// saturated shared key from costing one Redis call per request.
		d := p.last
		d.Allowed, d.Remaining = false, 0
		d.RetryAfter = time.Duration(p.backoff-nowUS) * time.Microsecond
		return d, ticket{}, nil
	}
	if p.tokens < cost {
		// Refill. A remainder below cost is dropped rather than carried into
		// the new lease: carrying it would stretch its life past TTL.
		d, got, err := s.lease(ctx, r, s.chunk(r.Params, cost), now)
		if err != nil {
			return limit.Decision{}, ticket{}, err
		}
		p.gen++
		p.tokens, p.expires, p.last = 0, 0, d
		if got < cost { // 0 by limit.Grant; < cost only from a misbehaving source
			s.denied.Add(1)
			d.Allowed = false
			// Back off for the retry time, and at least one Span, so the
			// next ask finds a chunk's worth accrued rather than one unit.
			p.backoff = nowUS + max(d.RetryAfter, s.cfg.Span).Microseconds()
			return d, ticket{}, nil
		}
		p.tokens, p.expires = got, nowUS+s.cfg.TTL.Microseconds()
	}
	p.tokens -= cost
	d := p.last
	d.Allowed, d.RetryAfter = true, 0
	d.Remaining = p.tokens // what this node can still admit without asking
	return d, ticket{p: p, gen: p.gen, n: cost}, nil
}

// lease asks the primary source, or the degraded one while the breaker is
// open or when the primary reports it is down.
func (s *Store) lease(ctx context.Context, r *limit.Req, want int, now time.Time) (limit.Decision, int, error) {
	if !s.open() {
		d, got, err := s.primary.Lease(ctx, *r, want, now)
		if err == nil {
			s.leases.Add(1)
			return d, got, nil
		}
		if !errors.Is(err, limit.ErrBackendDown) {
			return limit.Decision{}, 0, fmt.Errorf("lease %s: %w", r.Policy, err)
		}
	}
	d, got, err := s.degraded.Lease(ctx, *r, want, now)
	if err != nil {
		return limit.Decision{}, 0, fmt.Errorf("lease %s (degraded): %w", r.Policy, err)
	}
	s.degradedLeases.Add(1)
	d.Backend = limit.BackendDegraded
	return d, got, nil
}

// chunk is the lease size for p: rate*Span/period units, at least cost, at
// most the policy's capacity (a bigger ask could never be granted whole).
func (s *Store) chunk(p limit.Params, cost int) int {
	n := int64(1)
	if per := p.Period.Microseconds(); per > 0 {
		n = max(p.Rate*s.cfg.Span.Microseconds()/per, 1)
	}
	capacity := p.Burst
	if p.Algo == limit.SlidingWindow || p.Algo == limit.SlidingWindowExact {
		capacity = p.Rate
	}
	n = min(n, capacity)
	return max(int(n), cost)
}

// refund returns takes to their pools, unless the pool has been refilled or
// has expired since (the units then belonged to a lease that is gone).
func (s *Store) refund(tickets []ticket, now time.Time) {
	nowUS := now.UnixMicro()
	for _, t := range tickets {
		t.p.mu.Lock()
		if t.p.gen == t.gen && t.p.expires > nowUS {
			t.p.tokens += t.n
		}
		t.p.mu.Unlock()
		s.refunds.Add(1)
	}
}

// merge copies the subject decisions into out and refunds the shared takes
// when the subject call denied.
func (s *Store) merge(ds []limit.Decision, idx []int, out []limit.Decision, tickets []ticket, now time.Time) {
	all := true
	for j, i := range idx {
		out[i] = ds[j]
		all = all && ds[j].Allowed
	}
	if !all {
		s.refund(tickets, now)
	}
}

func (s *Store) pool(key string) *pool {
	s.mu.RLock()
	p := s.pools[key]
	s.mu.RUnlock()
	if p != nil {
		return p
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if p = s.pools[key]; p == nil {
		p = &pool{}
		s.pools[key] = p
	}
	return p
}

func countShared(reqs []limit.Req) int {
	n := 0
	for i := range reqs {
		if reqs[i].Shared {
			n++
		}
	}
	return n
}

// subjectReqs returns the non-shared reqs and their indices in reqs.
func subjectReqs(reqs []limit.Req, nShared int) ([]limit.Req, []int) {
	rest := make([]limit.Req, 0, len(reqs)-nShared)
	idx := make([]int, 0, len(reqs)-nShared)
	for i := range reqs {
		if !reqs[i].Shared {
			rest = append(rest, reqs[i])
			idx = append(idx, i)
		}
	}
	return rest, idx
}

// skip marks the subject reqs of a call a shared policy denied.
func skip(reqs []limit.Req, idx []int, out []limit.Decision) {
	for _, i := range idx {
		out[i] = limit.Decision{Allowed: true, Policy: reqs[i].Policy, Backend: BackendSkipped}
	}
}

// BackendSkipped labels a subject Decision that was not evaluated because a
// shared policy of the same call denied first.
const BackendSkipped = "skipped"

// FailOpen is the degraded source for FALLBACK_MODE=fail_open: every lease
// is granted in full without counting.
type FailOpen struct{}

// Lease implements Source.
func (FailOpen) Lease(_ context.Context, req limit.Req, want int, _ time.Time) (limit.Decision, int, error) {
	return limit.Decision{Allowed: true, Limit: int(req.Params.Burst), Remaining: int(req.Params.Burst), Policy: req.Policy, Backend: limit.BackendDegraded}, want, nil
}

// FailClosed is the degraded source for FALLBACK_MODE=fail_closed: nothing
// is granted; RetryAfter tells the caller when to ask again.
type FailClosed struct{ RetryAfter time.Duration }

// Lease implements Source.
func (f FailClosed) Lease(_ context.Context, req limit.Req, _ int, _ time.Time) (limit.Decision, int, error) {
	return limit.Decision{Limit: int(req.Params.Burst), RetryAfter: f.RetryAfter, Policy: req.Policy, Backend: limit.BackendDegraded}, 0, nil
}
