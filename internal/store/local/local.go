// Package local is the in-process backend: 256 shards, each a mutex and a
// map[string]entry, evaluated with the pure algo.Step functions. It never
// starts a goroutine per request; Run is the ONE idle-key sweeper. Stats are
// atomics (the aigw/internal/gate style).
//
// Owner: workstream B (backends).
package local

import (
	"context"
	"errors"
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

// Backend implements limit.Store in memory.
type Backend struct {
	cfg Config
	div int64 // 1 for the primary view; Scale sets it on a derived view
}

// Stats is the atomics snapshot.
type Stats struct {
	Keys    int64
	Evicted int64
	Applied int64
}

// New builds a Backend; it does not start the sweeper (see Run).
func New(cfg Config) *Backend {
	if cfg.Shards <= 0 {
		cfg.Shards = 256
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
	return &Backend{cfg: cfg, div: 1}
}

// Config returns the effective configuration (defaults applied).
func (b *Backend) Config() Config { return b.cfg }

// Run is the ONE goroutine: the idle-key sweeper. It returns when ctx ends.
// STUB: blocks on ctx.
func (b *Backend) Run(ctx context.Context) error {
	<-ctx.Done()
	return ctx.Err()
}

// Apply implements limit.Store: dry-run every key, commit only if all allowed. STUB.
func (b *Backend) Apply(_ context.Context, _ []limit.Req, _ time.Time) ([]limit.Decision, error) {
	return nil, errors.New("stub: local.Backend.Apply")
}

// Peek implements limit.Store (no mutation). STUB.
func (b *Backend) Peek(_ context.Context, _ []limit.Req, _ time.Time) ([]limit.Decision, error) {
	return nil, errors.New("stub: local.Backend.Peek")
}

// Reset implements limit.Store. STUB.
func (b *Backend) Reset(_ context.Context, _ []string) error {
	return errors.New("stub: local.Backend.Reset")
}

// Name implements limit.Store: "local".
func (b *Backend) Name() string { return limit.BackendLocal }

// Scale returns a degraded view sharing b's shards where every Req's
// Params.Rate and Burst are divided by div (min 1), so N nodes over-admit by
// at most N/div while Redis is down. div <= 1 returns b itself.
func (b *Backend) Scale(div int64) *Backend {
	if div <= 1 {
		return b
	}
	return &Backend{cfg: b.cfg, div: div}
}

// Divisor reports the scale of this view (1 for the primary).
func (b *Backend) Divisor() int64 { return b.div }

// Stats reads the atomics. STUB: zero.
func (b *Backend) Stats() Stats { return Stats{} }
