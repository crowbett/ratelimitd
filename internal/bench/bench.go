// Package bench is the load harness behind `ratelimitctl bench`: an open-loop
// pacer (per-worker ticker with a missed-tick counter, so a slow server shows
// as backlog instead of closing the loop), 5 s warmup excluded, per-worker
// latency samples merged and sorted once at the end, and the allowed ratio
// compared with the closed-form expectation.
//
// Owner: workstream D (client & load). Conventions from perf-lab.
package bench

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"time"

	"golang.org/x/sync/errgroup"
)

// Defaults applied by Run to zero Config fields.
const (
	DefaultWarmup   = 5 * time.Second
	DefaultDuration = 30 * time.Second
	DefaultTimeout  = 250 * time.Millisecond
	DefaultTenant   = "acme"
	// maxSamplesPerWorker caps the latency buffer (2M samples = 16 MB) so a
	// long run at 60k rps cannot exhaust memory; beyond it samples are dropped
	// and the percentiles are computed on the first samples.
	maxSamplesPerWorker = 2_000_000
)

// Config is one bench run.
type Config struct {
	Target   string        // gRPC or HTTP address
	Proto    string        // "grpc" | "http"
	Backend  string        // "redis" | "local" (informational; selected on the daemon)
	RPS      int           // offered rate, spread across Workers
	Keys     int           // distinct subjects; 1 = the hot-key ceiling run
	Workers  int           // 0 = GOMAXPROCS
	Duration time.Duration // measured window, after Warmup
	Warmup   time.Duration // default 5s; negative = none
	Tenant   string
	Policy   struct{ Rate, Burst int64 } // for ExpectedAllowedRatio; Rate is per second
	// Batch is how many subjects one tick sends: a gRPC BatchCheck of Batch
	// subjects, or Batch sequential HTTP checks. 0/1 = one Check per tick.
	// It is the knob that makes 60k rps reachable with ms-resolution tickers.
	Batch int
	// Timeout bounds one round trip (default 250ms); a miss counts as an error.
	Timeout time.Duration
	// Route and Method are sent with every check (policy matching / cost).
	Route, Method string
}

// Result is what the bench prints and what goes into README.md.
type Result struct {
	Achieved      float64 // decisions/s over Duration
	Offered       int64   // ticks scheduled in the window: Sent + MissedTicks (in requests)
	Allowed       int64
	Denied        int64
	Errors        int64
	MissedTicks   int64 // ticks a worker could not honour because the previous call was still in flight
	P50, P99, Max time.Duration
	AllowedRatio  float64
	ExpectedRatio float64
	DegradedFor   time.Duration // Elapsed * DegradedDecisions / Decisions

	Sent              int64  // requests actually issued in the window
	Decisions         int64  // Allowed + Denied
	DegradedDecisions int64  // decisions the daemon answered from its fallback
	FirstError        string // the first error seen in the window, for the report
	Elapsed           time.Duration
	Config            Config // as run, defaults applied
}

// checker is one wire protocol: a batch of subjects in, one outcome per
// subject out. Errors are per call (the whole batch), never per subject.
type checker interface {
	check(ctx context.Context, subjects []string) ([]outcome, error)
	close() error
}

type outcome struct {
	allowed  bool
	degraded bool
}

// worker is one pacer loop's private state; nothing is shared until merge.
type worker struct {
	id       int
	subjects []string // this worker's rotation
	samples  []time.Duration
	sent     int64
	allowed  int64
	denied   int64
	errs     int64
	degraded int64
	missed   int64
	firstErr error
}

// Run executes cfg and returns the Result.
func Run(ctx context.Context, cfg Config, log *slog.Logger) (Result, error) {
	if log == nil {
		log = slog.Default()
	}
	cfg = withDefaults(cfg)
	if err := validate(cfg); err != nil {
		return Result{}, err
	}
	chk, err := newChecker(ctx, cfg)
	if err != nil {
		return Result{}, err
	}
	defer func() {
		if cerr := chk.close(); cerr != nil {
			log.Warn("bench: close client", slog.String("err", cerr.Error()))
		}
	}()

	workers := make([]*worker, cfg.Workers)
	for i := range workers {
		workers[i] = newWorker(i, cfg)
	}
	perWorkerRPS := float64(cfg.RPS) / float64(cfg.Workers)
	interval := time.Duration(float64(time.Second) * float64(cfg.Batch) / perWorkerRPS)
	if interval < 50*time.Microsecond {
		log.Warn("bench: tick interval below 50us; raise Batch or Workers or expect missed ticks",
			slog.Duration("interval", interval))
	}
	log.Info("bench: start",
		slog.String("proto", cfg.Proto), slog.String("target", cfg.Target),
		slog.Int("rps", cfg.RPS), slog.Int("keys", cfg.Keys), slog.Int("workers", cfg.Workers),
		slog.Int("batch", cfg.Batch), slog.Duration("tick", interval),
		slog.Duration("warmup", cfg.Warmup), slog.Duration("duration", cfg.Duration))

	start := time.Now()
	window := window{from: start.Add(cfg.Warmup), to: start.Add(cfg.Warmup + cfg.Duration)}

	// The window is closed by the tick clock, not by a context deadline: a
	// call in flight at the end completes and is counted, instead of racing
	// the server's copy of the deadline and surfacing as a spurious
	// DeadlineExceeded. Only the caller's ctx aborts mid-call.
	g, gctx := errgroup.WithContext(ctx)
	for _, w := range workers {
		g.Go(func() error {
			w.loop(gctx, chk, interval, window, cfg.Batch, cfg.Timeout)
			return nil
		})
	}
	_ = g.Wait()
	if err := ctx.Err(); err != nil {
		return Result{}, fmt.Errorf("bench: aborted: %w", err)
	}
	return merge(cfg, workers, cfg.Duration), nil
}

// window is the measured interval: ticks before from are warmup, ticks at or
// after to end the loop.
type window struct{ from, to time.Time }

func withDefaults(cfg Config) Config {
	if cfg.Proto == "" {
		cfg.Proto = "grpc"
	}
	if cfg.Workers <= 0 {
		cfg.Workers = runtime.GOMAXPROCS(0)
	}
	if cfg.Batch <= 0 {
		cfg.Batch = 1
	}
	if cfg.Keys <= 0 {
		cfg.Keys = 1
	}
	if cfg.Duration <= 0 {
		cfg.Duration = DefaultDuration
	}
	switch {
	case cfg.Warmup == 0:
		cfg.Warmup = DefaultWarmup
	case cfg.Warmup < 0: // explicit "no warmup"
		cfg.Warmup = 0
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = DefaultTimeout
	}
	if cfg.Tenant == "" {
		cfg.Tenant = DefaultTenant
	}
	return cfg
}

func validate(cfg Config) error {
	var errs []error
	if cfg.Target == "" {
		errs = append(errs, errors.New("target is required"))
	}
	if cfg.Proto != "grpc" && cfg.Proto != "http" {
		errs = append(errs, fmt.Errorf("proto must be grpc or http, got %q", cfg.Proto))
	}
	if cfg.RPS <= 0 {
		errs = append(errs, fmt.Errorf("rps must be > 0, got %d", cfg.RPS))
	}
	if cfg.Batch > 64 {
		errs = append(errs, fmt.Errorf("batch must be <= 64, got %d", cfg.Batch))
	}
	if cfg.RPS < cfg.Workers*cfg.Batch {
		errs = append(errs, fmt.Errorf("rps %d is below workers*batch = %d: fewer workers", cfg.RPS, cfg.Workers*cfg.Batch))
	}
	if len(errs) > 0 {
		return fmt.Errorf("bench: config: %w", errors.Join(errs...))
	}
	return nil
}

// newWorker assigns worker i the subjects k with k % Workers == i (every
// key is offered by exactly one worker, so per-key load is even) and sizes
// its sample buffer for the measured window.
func newWorker(i int, cfg Config) *worker {
	subs := make([]string, 0, cfg.Keys/cfg.Workers+1)
	for k := i; k < cfg.Keys; k += cfg.Workers {
		subs = append(subs, "k"+strconv.Itoa(k))
	}
	if len(subs) == 0 { // more workers than keys: share the hot keys
		subs = append(subs, "k"+strconv.Itoa(i%cfg.Keys))
	}
	expect := int(float64(cfg.RPS)/float64(cfg.Workers)*cfg.Duration.Seconds()/float64(cfg.Batch)) + 1024
	return &worker{id: i, subjects: subs, samples: make([]time.Duration, 0, min(expect, maxSamplesPerWorker))}
}

// loop is the open-loop pacer: one call per tick, never more. A tick that
// fires while the previous call is still in flight is lost by the ticker
// (its channel holds one), and the gap between the tick's timestamp and the
// expected schedule is counted as missed ticks rather than being caught up,
// so a slow server shows as backlog in the report instead of silently
// throttling the offered load.
func (w *worker) loop(ctx context.Context, chk checker, interval time.Duration, win window, batch int, timeout time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	next := time.Now().Add(interval)
	subjects := make([]string, batch)
	pos := 0
	for {
		var tick time.Time
		select {
		case <-ctx.Done():
			return
		case tick = <-ticker.C:
		}
		// Ticks scheduled at next, next+interval, ... that the ticker dropped
		// while the previous call was in flight are missed; only those
		// scheduled inside the window count, so a stall across either edge
		// is attributed exactly (a stall past the end used to lose them).
		if late := tick.Sub(next); late >= interval {
			n := int64(late / interval)
			w.missed += scheduledIn(next, interval, n, win)
			next = next.Add(time.Duration(n) * interval)
		}
		if !tick.Before(win.to) {
			return
		}
		measuring := !tick.Before(win.from)
		next = next.Add(interval)

		for i := range subjects {
			subjects[i] = w.subjects[pos]
			pos++
			if pos == len(w.subjects) {
				pos = 0
			}
		}
		callCtx, cancel := context.WithTimeout(ctx, timeout)
		t0 := time.Now()
		outs, err := chk.check(callCtx, subjects)
		lat := time.Since(t0)
		cancel()
		if !measuring {
			continue
		}
		w.sent += int64(batch)
		if err != nil {
			if ctx.Err() != nil { // the caller aborted mid-call: not a server error
				w.sent -= int64(batch)
				return
			}
			w.errs += int64(batch)
			if w.firstErr == nil {
				w.firstErr = err
			}
			continue
		}
		if len(w.samples) < cap(w.samples) {
			w.samples = append(w.samples, lat)
		}
		for _, o := range outs {
			if o.allowed {
				w.allowed++
			} else {
				w.denied++
			}
			if o.degraded {
				w.degraded++
			}
		}
	}
}

// scheduledIn counts how many of the n ticks scheduled at first, first+interval,
// ... fall inside the window [from, to).
func scheduledIn(first time.Time, interval time.Duration, n int64, win window) int64 {
	var c int64
	for k := range n {
		at := first.Add(time.Duration(k) * interval)
		if !at.Before(win.from) && at.Before(win.to) {
			c++
		}
	}
	return c
}

// merge folds the workers into one Result: counters summed, samples
// concatenated and sorted once. elapsed is the measured window; ticks are
// attributed to it by their timestamp, so it is exactly cfg.Duration.
func merge(cfg Config, workers []*worker, elapsed time.Duration) Result {
	r := Result{Config: cfg, Elapsed: elapsed}
	total := 0
	for _, w := range workers {
		total += len(w.samples)
	}
	samples := make([]time.Duration, 0, total)
	for _, w := range workers {
		r.Sent += w.sent
		r.Allowed += w.allowed
		r.Denied += w.denied
		r.Errors += w.errs
		r.DegradedDecisions += w.degraded
		r.MissedTicks += w.missed
		if r.FirstError == "" && w.firstErr != nil {
			r.FirstError = w.firstErr.Error()
		}
		samples = append(samples, w.samples...)
	}
	r.Decisions = r.Allowed + r.Denied
	r.Offered = r.Sent + r.MissedTicks*int64(cfg.Batch)
	if len(samples) > 0 {
		slices.Sort(samples)
		r.P50 = samples[len(samples)/2]
		r.P99 = samples[(len(samples)*99)/100]
		r.Max = samples[len(samples)-1]
	}
	if elapsed > 0 {
		r.Achieved = float64(r.Decisions) / elapsed.Seconds()
	}
	if r.Decisions > 0 {
		r.AllowedRatio = float64(r.Allowed) / float64(r.Decisions)
		r.DegradedFor = time.Duration(float64(elapsed) * float64(r.DegradedDecisions) / float64(r.Decisions))
	}
	if cfg.Policy.Rate > 0 {
		perKey := r.Decisions / int64(cfg.Keys)
		burst := cfg.Policy.Burst
		if cfg.Warmup > 0 {
			// The warmup already drained every key's burst when the offered
			// rate exceeds the limit, so the steady-state expectation is the
			// rate term alone.
			burst = 0
		}
		r.ExpectedRatio = ExpectedAllowedRatio(cfg.Policy.Rate, burst, perKey, elapsed)
	}
	return r
}

// ExpectedAllowedRatio is the closed form min(offered, rate*d + burst) / offered
// for one key at a steady offered rate: rate is events per second, burst the
// policy capacity, offered the requests sent to that key during d. It is 1
// when offered is 0 (nothing to deny) and computed in integer microseconds
// before the final division.
func ExpectedAllowedRatio(rate, burst, offered int64, d time.Duration) float64 {
	if offered <= 0 {
		return 1
	}
	admissible := rate*d.Microseconds()/1_000_000 + burst
	if admissible < 0 {
		admissible = 0
	}
	if admissible > offered {
		admissible = offered
	}
	return float64(admissible) / float64(offered)
}

// Pass reports whether r meets the docs/PLAN.md macro criterion: >= minRPS
// decisions/s, p99 < maxP99, allowed ratio within tolerance of expected,
// zero errors. The returned reasons name every failed clause.
func (r Result) Pass(minRPS float64, maxP99 time.Duration, tolerance float64) (bool, []string) {
	var reasons []string
	if r.Achieved < minRPS {
		reasons = append(reasons, fmt.Sprintf("achieved %.0f/s < %.0f/s", r.Achieved, minRPS))
	}
	if r.P99 >= maxP99 {
		reasons = append(reasons, fmt.Sprintf("p99 %s >= %s", r.P99, maxP99))
	}
	if diff := r.AllowedRatio - r.ExpectedRatio; diff > tolerance || diff < -tolerance {
		reasons = append(reasons, fmt.Sprintf("allowed ratio %.4f vs expected %.4f (tolerance %.4f)", r.AllowedRatio, r.ExpectedRatio, tolerance))
	}
	if r.Errors > 0 {
		reasons = append(reasons, fmt.Sprintf("%d errors", r.Errors))
	}
	return len(reasons) == 0, reasons
}

// String is the one-line summary the CLI prints.
func (r Result) String() string {
	s := fmt.Sprintf("%s %s rps=%d keys=%d workers=%d batch=%d: achieved=%.0f/s p50=%s p99=%s max=%s allowed=%.4f expected=%.4f errors=%d missed=%d degraded=%d",
		r.Config.Proto, r.Config.Backend, r.Config.RPS, r.Config.Keys, r.Config.Workers, r.Config.Batch,
		r.Achieved, r.P50, r.P99, r.Max, r.AllowedRatio, r.ExpectedRatio, r.Errors, r.MissedTicks, r.DegradedDecisions)
	if r.FirstError != "" {
		s += " first_error=" + strconv.Quote(r.FirstError)
	}
	return s
}

// TableHeader is the README.md table header WriteTable's row belongs under.
const TableHeader = "| proto | backend | offered rps | keys | workers | batch | achieved/s | p50 | p99 | max | allowed (expected) | errors | missed ticks | degraded |\n" +
	"|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n"

// WriteTable writes the README.md row for r (see TableHeader).
func (r Result) WriteTable(w io.Writer) error {
	backend := r.Config.Backend
	if backend == "" {
		backend = "-"
	}
	var b strings.Builder
	fmt.Fprintf(&b, "| %s | %s | %d | %d | %d | %d | %.0f | %s | %s | %s | %.4f (%.4f) | %d | %d | %d |\n",
		r.Config.Proto, backend, r.Config.RPS, r.Config.Keys, r.Config.Workers, r.Config.Batch,
		r.Achieved, fmtDur(r.P50), fmtDur(r.P99), fmtDur(r.Max), r.AllowedRatio, r.ExpectedRatio,
		r.Errors, r.MissedTicks, r.DegradedDecisions)
	if _, err := io.WriteString(w, b.String()); err != nil {
		return fmt.Errorf("bench: write table: %w", err)
	}
	return nil
}

// fmtDur renders a latency with two decimals in ms below a second.
func fmtDur(d time.Duration) string {
	if d < time.Second {
		return strconv.FormatFloat(float64(d)/float64(time.Millisecond), 'f', 2, 64) + "ms"
	}
	return d.Round(time.Millisecond).String()
}
