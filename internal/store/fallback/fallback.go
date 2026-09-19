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
	br      *breaker
	clock   func() time.Time // wall clock for latency and breaker timers (never for decisions)
	canary  []limit.Req
}

// New wraps primary and local. log and st may be nil.
//
// The local Store must already be the degraded view: callers pass
// local.New(cfg).Scale(cfg.DegradedDivisor), so ModeLocal answers with
// limit/divisor per node. The Store never scales anything itself;
// DegradedDivisor here is informational (State, logs).
//
// The breaker samples the last MinSamples primary outcomes (an error, or a
// call slower than LatencyBudget, is a failure) and opens when failures /
// MinSamples >= FailureRatio; it never opens before MinSamples outcomes.
func New(primary, local limit.Store, cfg Config, log *slog.Logger, st *admin.Snapshot) *Store {
	if log == nil {
		log = slog.Default()
	}
	if cfg.DegradedDivisor < 1 {
		cfg.DegradedDivisor = 1
	}
	if cfg.FailureRatio <= 0 || cfg.FailureRatio > 1 {
		cfg.FailureRatio = 0.5
	}
	if cfg.MinSamples <= 0 {
		cfg.MinSamples = 20
	}
	if cfg.OpenFor <= 0 {
		cfg.OpenFor = 5 * time.Second
	}
	if cfg.ProbeEvery <= 0 {
		cfg.ProbeEvery = 500 * time.Millisecond
	}
	if cfg.HalfOpenProbes <= 0 {
		cfg.HalfOpenProbes = 3
	}
	canary, _ := limit.NewParams(limit.GCRA, 1, time.Second, 1)
	return &Store{
		primary: primary,
		local:   local,
		cfg:     cfg,
		log:     log,
		st:      st,
		br:      newBreaker(cfg),
		clock:   time.Now,
		canary:  []limit.Req{{Key: limit.Key("_breaker", "canary", "probe"), Params: canary, Cost: 1, Policy: "probe"}},
	}
}

// Config returns the effective configuration (defaults applied).
func (s *Store) Config() Config { return s.cfg }

// Run is the ONE goroutine: the half-open prober. While the breaker is open
// it Peeks a canary key on the primary every ProbeEvery (once OpenFor has
// passed); HalfOpenProbes consecutive successes close the breaker. Request
// traffic never touches an open primary except through the same
// single-flight slot (see Apply). It returns when ctx ends.
func (s *Store) Run(ctx context.Context) error {
	t := time.NewTicker(s.cfg.ProbeEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			if s.br.acquireProbe(s.clock()) {
				_, err := s.timed(ctx, s.canary, s.clock(), true)
				s.probed(err == nil)
			}
		}
	}
}

// Apply implements limit.Store: primary; on error, timeout or over-budget
// latency the breaker records a failure and the degraded path answers this
// call; once open, the degraded path answers every call except the single
// half-open probe.
func (s *Store) Apply(ctx context.Context, reqs []limit.Req, now time.Time) ([]limit.Decision, error) {
	return s.call(ctx, reqs, now, false)
}

// Peek implements limit.Store (same routing, no mutation).
func (s *Store) Peek(ctx context.Context, reqs []limit.Req, now time.Time) ([]limit.Decision, error) {
	return s.call(ctx, reqs, now, true)
}

// Reset implements limit.Store: both backends (the primary only while the
// breaker is closed), first error wins.
func (s *Store) Reset(ctx context.Context, keys []string) error {
	var first error
	if !s.br.isOpen() {
		start := s.clock()
		err := s.primary.Reset(ctx, keys)
		if s.br.record(s.failed(err, s.clock().Sub(start)), s.clock()) {
			s.opened()
		}
		if err != nil {
			first = fmt.Errorf("primary: %w", err)
		}
	}
	if err := s.local.Reset(ctx, keys); err != nil && first == nil {
		first = fmt.Errorf("local: %w", err)
	}
	return first
}

// Name implements limit.Store: the primary's name while closed, "degraded" while open.
func (s *Store) Name() string {
	if s.br.isOpen() {
		return limit.BackendDegraded
	}
	return s.primary.Name()
}

// State reports the breaker: open, since when, and the configured Mode.
func (s *Store) State() (open bool, since time.Time, mode Mode) {
	open, since = s.br.state()
	return open, since, s.cfg.Mode
}

// call routes one Apply/Peek.
func (s *Store) call(ctx context.Context, reqs []limit.Req, now time.Time, peek bool) ([]limit.Decision, error) {
	if s.br.isOpen() {
		if s.br.acquireProbe(s.clock()) { // half-open: this request is the single-flight probe
			ds, err := s.timed(ctx, reqs, now, peek)
			s.probed(err == nil)
			if err == nil {
				return ds, nil
			}
			if !s.recoverable(err) {
				return nil, err
			}
		}
		return s.degraded(ctx, reqs, now, peek)
	}
	ds, err := s.timed(ctx, reqs, now, peek)
	if err == nil {
		return ds, nil
	}
	if !s.recoverable(err) {
		return nil, err
	}
	return s.degraded(ctx, reqs, now, peek)
}

// timed runs one primary call, records its outcome and trips the breaker
// when the window says so. A successful call slower than LatencyBudget is a
// failure for the breaker but its decisions are still returned.
func (s *Store) timed(ctx context.Context, reqs []limit.Req, now time.Time, peek bool) ([]limit.Decision, error) {
	start := s.clock()
	var (
		ds  []limit.Decision
		err error
	)
	if peek {
		ds, err = s.primary.Peek(ctx, reqs, now)
	} else {
		ds, err = s.primary.Apply(ctx, reqs, now)
	}
	end := s.clock()
	if s.br.record(s.failed(err, end.Sub(start)), end) {
		s.opened()
	}
	return ds, err
}

// failed decides whether one primary outcome counts against the breaker.
func (s *Store) failed(err error, latency time.Duration) bool {
	if err != nil {
		return s.recoverable(err)
	}
	return s.cfg.LatencyBudget > 0 && latency > s.cfg.LatencyBudget
}

// recoverable reports whether err is the backend's fault (so the degraded
// path should answer) rather than the caller's (invalid params, cancelled
// context), which is returned as is.
func (*Store) recoverable(err error) bool {
	return !errors.Is(err, limit.ErrInvalidParams) && !errors.Is(err, context.Canceled)
}

// probed feeds a half-open probe outcome back and logs a close.
func (s *Store) probed(ok bool) {
	if s.br.probeResult(ok) {
		s.closed()
	}
}

func (s *Store) opened() {
	s.st.SetFallbackActive(limit.BackendRedis, true)
	s.log.Warn("fallback: breaker open, degraded path active",
		slog.String("primary", s.primary.Name()),
		slog.String("mode", s.cfg.Mode.String()),
		slog.Int64("divisor", s.cfg.DegradedDivisor),
		slog.Duration("open_for", s.cfg.OpenFor))
}

func (s *Store) closed() {
	s.st.SetFallbackActive(limit.BackendRedis, false)
	s.log.Info("fallback: breaker closed, primary restored", slog.String("primary", s.primary.Name()))
}

// degraded answers per Mode while the primary is unavailable.
func (s *Store) degraded(ctx context.Context, reqs []limit.Req, now time.Time, peek bool) ([]limit.Decision, error) {
	switch s.cfg.Mode {
	case ModeLocal:
		var (
			ds  []limit.Decision
			err error
		)
		if peek {
			ds, err = s.local.Peek(ctx, reqs, now)
		} else {
			ds, err = s.local.Apply(ctx, reqs, now)
		}
		if err != nil {
			return nil, fmt.Errorf("%w: degraded path: %w", limit.ErrBackendDown, err)
		}
		for i := range ds {
			ds[i].Backend = limit.BackendDegraded
		}
		return ds, nil
	case ModeFailOpen:
		out := make([]limit.Decision, len(reqs))
		for i := range reqs {
			lim := capacity(reqs[i].Params)
			out[i] = limit.Decision{Allowed: true, Remaining: lim, Limit: lim, Policy: reqs[i].Policy, Backend: limit.BackendDegraded}
		}
		return out, nil
	case ModeFailClosed:
		out := make([]limit.Decision, len(reqs))
		for i := range reqs {
			out[i] = limit.Decision{
				RetryAfter: s.cfg.ProbeEvery,
				ResetAfter: s.cfg.ProbeEvery,
				Limit:      capacity(reqs[i].Params),
				Policy:     reqs[i].Policy,
				Backend:    limit.BackendDegraded,
			}
		}
		return out, nil
	}
	return nil, fmt.Errorf("%w: unknown fallback mode %s", limit.ErrBackendDown, s.cfg.Mode)
}

// capacity is the Limit a decision reports for p: Rate for the window
// algorithms, Burst for GCRA and the bucket.
func capacity(p limit.Params) int {
	if p.Algo == limit.SlidingWindow || p.Algo == limit.SlidingWindowExact {
		return int(p.Rate)
	}
	return int(p.Burst)
}

// ApplyMany implements limit.BatchStore. While the breaker is closed and the
// primary pipelines (limit.BatchStore), every group goes out in one round
// trip and each failed group is answered by the degraded path on its own;
// the batch counts as ONE breaker sample (success, or failure if any group
// failed or the whole pipeline was over budget). While open, or when the
// primary cannot batch, it is one call per group through the same routing as
// Apply, so half-open probing and modes behave identically.
func (s *Store) ApplyMany(ctx context.Context, groups [][]limit.Req, now time.Time) ([][]limit.Decision, []error, error) {
	out := make([][]limit.Decision, len(groups))
	errs := make([]error, len(groups))
	batch, ok := s.primary.(limit.BatchStore)
	if !ok || s.br.isOpen() {
		for i, reqs := range groups {
			out[i], errs[i] = s.Apply(ctx, reqs, now)
		}
		return out, errs, nil
	}
	start := s.clock()
	pout, perrs, err := batch.ApplyMany(ctx, groups, now)
	end := s.clock()
	if err != nil {
		if !s.recoverable(err) {
			return nil, nil, err
		}
		if s.br.record(true, end) {
			s.opened()
		}
		for i, reqs := range groups {
			out[i], errs[i] = s.degraded(ctx, reqs, now, false)
		}
		return out, errs, nil
	}
	failed := s.cfg.LatencyBudget > 0 && end.Sub(start) > s.cfg.LatencyBudget
	for i, reqs := range groups {
		switch {
		case perrs[i] == nil:
			out[i] = pout[i]
		case !s.recoverable(perrs[i]):
			errs[i] = perrs[i]
		default:
			failed = true
			out[i], errs[i] = s.degraded(ctx, reqs, now, false)
		}
	}
	if s.br.record(failed, end) {
		s.opened()
	}
	return out, errs, nil
}
