// Package local is the in-process backend: 256 shards, each a mutex and a
// map[string]entry, evaluated with the pure algo.Step functions. It never
// starts a goroutine per request; Run is the ONE idle-key sweeper. Stats are
// atomics (the aigw/internal/gate style).
//
// Owner: workstream B (backends).
package local

import (
	"context"
	"fmt"
	"math/bits"
	"sync"
	"sync/atomic"
	"time"

	"github.com/crowbett/ratelimitd/internal/limit"
)

// Config tunes the backend. Zero values take the defaults below.
type Config struct {
	Shards     int           // LOCAL_SHARDS, default 256 (power of two)
	IdleTTL    time.Duration // LOCAL_IDLE_TTL, default 10m: a key untouched this long is swept
	SweepEvery time.Duration // default IdleTTL/10
	MaxKeys    int           // LOCAL_MAX_KEYS, default 1_000_000; above it the sweeper evicts oldest-idle first
}

// maxShards bounds Config.Shards so the per-call shard bitset stays on the stack.
const maxShards = 4096

// Backend implements limit.Store in memory.
type Backend struct {
	cfg  Config
	div  int64 // 1 for the primary view; Scale sets it on a derived view
	core *core // shared between the primary and every Scale view
}

// core is the shared state behind a Backend and its Scale views.
type core struct {
	shards []shard
	mask   uint32 // len(shards)-1; shards is a power of two
	quota  int    // MaxKeys / len(shards), rounded up

	keys    atomic.Int64
	evicted atomic.Int64
	swept   atomic.Int64
	applied atomic.Int64
	clock   atomic.Int64 // the newest now seen by Apply, unix microseconds: the sweeper's clock
}

// shard is one mutex-protected map, padded to its own cache line.
type shard struct {
	mu sync.Mutex
	m  map[string]entry
	_  [40]byte
}

// Stats is the atomics snapshot.
type Stats struct {
	Keys    int64 // live keys across all shards
	Evicted int64 // keys removed by the MaxKeys cap
	Swept   int64 // keys removed because they were idle past IdleTTL
	Applied int64 // Apply calls
}

// New builds a Backend; it does not start the sweeper (see Run).
func New(cfg Config) *Backend {
	if cfg.Shards <= 0 {
		cfg.Shards = 256
	}
	if cfg.Shards > maxShards {
		cfg.Shards = maxShards
	}
	if cfg.Shards&(cfg.Shards-1) != 0 { // round up to a power of two
		cfg.Shards = 1 << bits.Len(uint(cfg.Shards))
	}
	if cfg.IdleTTL <= 0 {
		cfg.IdleTTL = 10 * time.Minute
	}
	if cfg.SweepEvery <= 0 {
		cfg.SweepEvery = cfg.IdleTTL / 10
	}
	if cfg.MaxKeys <= 0 {
		cfg.MaxKeys = 1_000_000
	}
	c := &core{
		shards: make([]shard, cfg.Shards),
		mask:   uint32(cfg.Shards - 1), //nolint:gosec // bounded by maxShards
		quota:  (cfg.MaxKeys + cfg.Shards - 1) / cfg.Shards,
	}
	for i := range c.shards {
		c.shards[i].m = make(map[string]entry)
	}
	return &Backend{cfg: cfg, div: 1, core: c}
}

// Config returns the effective configuration (defaults applied).
func (b *Backend) Config() Config { return b.cfg }

// Run is the ONE goroutine: the idle-key sweeper. Every SweepEvery it calls
// Sweep with the newest now that Apply has seen (the caller's clock is the
// only clock; nothing here reads time.Now). It returns when ctx ends.
func (b *Backend) Run(ctx context.Context) error {
	t := time.NewTicker(b.cfg.SweepEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			if us := b.core.clock.Load(); us != 0 {
				b.Sweep(time.UnixMicro(us))
			}
		}
	}
}

// Apply implements limit.Store: every key is evaluated on a copy of its
// state while the involved shards are locked; the new states are written
// only if every key allowed. A denial mutates nothing.
func (b *Backend) Apply(_ context.Context, reqs []limit.Req, now time.Time) ([]limit.Decision, error) {
	return b.eval(reqs, now, true)
}

// Peek implements limit.Store (no mutation).
func (b *Backend) Peek(_ context.Context, reqs []limit.Req, now time.Time) ([]limit.Decision, error) {
	return b.eval(reqs, now, false)
}

// Reset implements limit.Store: the exact keys, then one pass over every
// shard for their templated variants (they hash to other shards). It is an
// admin operation, so O(keys held) is fine.
func (b *Backend) Reset(_ context.Context, keys []string) error {
	for _, k := range keys {
		sh := b.core.shardOf(k)
		sh.mu.Lock()
		if _, ok := sh.m[k]; ok {
			delete(sh.m, k)
			b.core.keys.Add(-1)
		}
		sh.mu.Unlock()
	}
	if len(keys) == 0 {
		return nil
	}
	for i := range b.core.shards {
		sh := &b.core.shards[i]
		sh.mu.Lock()
		for k := range sh.m {
			for _, key := range keys {
				if limit.IsVariant(k, key) {
					delete(sh.m, k)
					b.core.keys.Add(-1)
					break
				}
			}
		}
		sh.mu.Unlock()
	}
	return nil
}

// Name implements limit.Store: "local" for the primary view, "degraded" for
// a Scale view.
func (b *Backend) Name() string {
	if b.div > 1 {
		return limit.BackendDegraded
	}
	return limit.BackendLocal
}

// Scale returns a degraded view sharing b's shards where every Req's
// Params.Rate and Burst are divided by div (min 1), so N nodes over-admit by
// at most N/div while Redis is down. div <= 1 returns b itself.
func (b *Backend) Scale(div int64) *Backend {
	if div <= 1 {
		return b
	}
	return &Backend{cfg: b.cfg, div: div, core: b.core}
}

// Divisor reports the scale of this view (1 for the primary).
func (b *Backend) Divisor() int64 { return b.div }

// Stats reads the atomics.
func (b *Backend) Stats() Stats {
	return Stats{
		Keys:    b.core.keys.Load(),
		Evicted: b.core.evicted.Load(),
		Swept:   b.core.swept.Load(),
		Applied: b.core.applied.Load(),
	}
}

// params returns the Params this view enforces for p: p itself on the
// primary, p with Rate and Burst divided by div (min 1) on a Scale view.
func (b *Backend) params(p limit.Params) limit.Params {
	if b.div <= 1 {
		return p
	}
	p.Rate = max(p.Rate/b.div, 1)
	p.Burst = max(p.Burst/b.div, 1)
	p.EmissionUS = p.Period.Microseconds() / p.Rate
	return p
}

// stackReqs is how many reqs per call are handled without heap scratch.
const stackReqs = 8

// eval is Apply (commit) and Peek (!commit).
func (b *Backend) eval(reqs []limit.Req, now time.Time, commit bool) ([]limit.Decision, error) {
	n := len(reqs)
	out := make([]limit.Decision, n)
	if n == 0 {
		return out, nil
	}
	nowUS := now.UnixMicro()
	for i := range reqs {
		if reqs[i].Params.EmissionUS <= 0 || reqs[i].Params.Algo == limit.AlgorithmUnspecified {
			return nil, fmt.Errorf("%w: req %d (%s)", limit.ErrInvalidParams, i, reqs[i].Policy)
		}
	}
	if commit {
		b.core.applied.Add(1)
		b.core.observe(nowUS)
	}
	backend := b.Name()

	if n == 1 { // the hot path: one key, one shard
		r := &reqs[0]
		p := b.params(r.Params)
		sh := b.core.shardOf(r.Key)
		sh.mu.Lock()
		old, had := sh.m[r.Key]
		ne, d := step(old, p, max(r.Cost, 1), nowUS)
		if commit && d.Allowed {
			b.core.store(sh, r.Key, ne, p, nowUS, had)
		}
		sh.mu.Unlock()
		d.Policy, d.Backend = r.Policy, backend
		out[0] = d
		return out, nil
	}

	// General case: lock every involved shard in ascending index order (no
	// deadlock across concurrent multi-key calls), dry-run all, commit all.
	var (
		stackNew [stackReqs]entry
		stackHad [stackReqs]bool
		set      [maxShards / 64]uint64
	)
	news, had := stackNew[:], stackHad[:]
	if n > stackReqs {
		news, had = make([]entry, n), make([]bool, n)
	}
	for i := range reqs {
		idx := b.core.index(reqs[i].Key)
		set[idx/64] |= 1 << (idx % 64)
	}
	b.core.lockSet(&set, true)
	all := true
	for i := range reqs {
		r := &reqs[i]
		sh := b.core.shardOf(r.Key)
		old, ok := sh.m[r.Key]
		news[i], out[i] = step(old, b.params(r.Params), max(r.Cost, 1), nowUS)
		had[i] = ok
		out[i].Policy, out[i].Backend = r.Policy, backend
		all = all && out[i].Allowed
	}
	if commit && all {
		for i := range reqs {
			r := &reqs[i]
			b.core.store(b.core.shardOf(r.Key), r.Key, news[i], b.params(r.Params), nowUS, had[i])
		}
	}
	b.core.lockSet(&set, false)
	return out, nil
}

// store writes e under key in sh (the caller holds sh.mu) and stamps it.
func (c *core) store(sh *shard, key string, e entry, p limit.Params, nowUS int64, had bool) {
	e.lastSeen = nowUS
	e.expires = nowUS + ttl(e, p, nowUS).Microseconds()
	sh.m[key] = e
	if !had {
		c.keys.Add(1)
	}
}

// lockSet locks (or unlocks) every shard whose bit is set, in index order.
func (c *core) lockSet(set *[maxShards / 64]uint64, lock bool) {
	for w := range set {
		for left := set[w]; left != 0; left &= left - 1 {
			sh := &c.shards[w*64+bits.TrailingZeros64(left)]
			if lock {
				sh.mu.Lock()
			} else {
				sh.mu.Unlock()
			}
		}
	}
}

// observe advances the sweeper clock to nowUS if it is newer (1 ms
// granularity keeps the hot path from writing a shared cache line per call).
func (c *core) observe(nowUS int64) {
	if cur := c.clock.Load(); nowUS > cur+1000 {
		c.clock.CompareAndSwap(cur, nowUS)
	}
}

// index is the shard index of key: FNV-1a 32 masked to the shard count.
func (c *core) index(key string) int {
	const (
		offset32 = 2166136261
		prime32  = 16777619
	)
	h := uint32(offset32)
	for i := 0; i < len(key); i++ {
		h ^= uint32(key[i])
		h *= prime32
	}
	return int(h & c.mask)
}

func (c *core) shardOf(key string) *shard { return &c.shards[c.index(key)] }
