// Package fallback wraps a primary limit.Store (Redis) and a local one behind
// a breaker (error ratio + latency budget over a sliding sample) with three
// degraded modes. Default is ModeLocal with a divisor: while open, each node
// enforces limit/divisor locally, so N nodes over-admit by at most N/divisor.
// Fail-open is available and loudly metered; it is not the default.
//
// Owner: workstream B (backends).
package fallback

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/crowbett/ratelimitd/internal/admin"
	"github.com/crowbett/ratelimitd/internal/limit"
)

// Mode is what answers while the breaker is open.
type Mode uint8

// Modes. ModeLocal is the default.
const (
	ModeLocal      Mode = iota // local.Scale(divisor).Apply answers; Decision.Backend = "degraded"
	ModeFailOpen               // every request allowed, Remaining = Limit
	ModeFailClosed             // every request denied, RetryAfter = ProbeEvery
)

var modeNames = [...]string{"local", "fail_open", "fail_closed"}

// String returns the FALLBACK_MODE spelling.
func (m Mode) String() string {
	if int(m) < len(modeNames) {
		return modeNames[m]
	}
	return fmt.Sprintf("mode(%d)", uint8(m))
}

// ParseMode maps the FALLBACK_MODE spelling to a Mode.
func ParseMode(s string) (Mode, error) {
	for i, n := range modeNames {
		if n == s {
			return Mode(i), nil
		}
	}
	return ModeLocal, fmt.Errorf("unknown fallback mode %q (local, fail_open, fail_closed)", s)
}

// Config tunes the breaker.
type Config struct {
	Mode            Mode
	DegradedDivisor int64 // FALLBACK_DIVISOR, default 1 (expected node count)

	FailureRatio  float64       // BREAKER_RATIO, default 0.5: open when failures/samples >= ratio
	MinSamples    int           // BREAKER_MIN, default 20: never open on fewer samples
	LatencyBudget time.Duration // a primary call slower than this counts as a failure; floor = REDIS_TIMEOUT

	OpenFor        time.Duration // BREAKER_OPEN_FOR, default 5s before the first half-open probe
	ProbeEvery     time.Duration // BREAKER_PROBE, default 500ms between probes while open
	HalfOpenProbes int           // consecutive probe successes required to close, default 3
}

// Store implements limit.Store: primary, else the degraded path.
type Store struct {
	primary limit.Store
	local   limit.Store
	cfg     Config
	log     *slog.Logger
	st      *admin.Snapshot
}

// New wraps primary and local. log and st may be nil.
func New(primary, local limit.Store, cfg Config, log *slog.Logger, st *admin.Snapshot) *Store {
	if log == nil {
		log = slog.Default()
	}
	if cfg.DegradedDivisor < 1 {
		cfg.DegradedDivisor = 1
	}
	return &Store{primary: primary, local: local, cfg: cfg, log: log, st: st}
}

// Run is the ONE goroutine: the half-open prober while the breaker is open.
// It returns when ctx ends. STUB: blocks on ctx.
func (s *Store) Run(ctx context.Context) error {
	<-ctx.Done()
	return ctx.Err()
}

// Apply implements limit.Store: primary; on error, timeout or over-budget
// latency the breaker records a failure and, once open, the degraded path
// answers per Mode. STUB.
func (s *Store) Apply(_ context.Context, _ []limit.Req, _ time.Time) ([]limit.Decision, error) {
	return nil, errors.New("stub: fallback.Store.Apply")
}

// Peek implements limit.Store. STUB.
func (s *Store) Peek(_ context.Context, _ []limit.Req, _ time.Time) ([]limit.Decision, error) {
	return nil, errors.New("stub: fallback.Store.Peek")
}

// Reset implements limit.Store: both backends, first error wins. STUB.
func (s *Store) Reset(_ context.Context, _ []string) error {
	return errors.New("stub: fallback.Store.Reset")
}

// Name implements limit.Store: the primary's name while closed, "degraded" while open.
func (s *Store) Name() string { return s.primary.Name() }

// State reports the breaker: open, since when, and the configured Mode.
// STUB: never open.
func (s *Store) State() (open bool, since time.Time, mode Mode) {
	return false, time.Time{}, s.cfg.Mode
}
