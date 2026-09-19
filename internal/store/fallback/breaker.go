package fallback

import (
	"sync"
	"time"
)

// breaker is the sample-window circuit breaker. It is closed (primary
// answers), open (degraded path answers, probes are refused until OpenFor has
// passed) or half-open (still degraded, but one probe at a time reaches the
// primary; HalfOpenProbes consecutive successes close it, one failure resets
// the count). All fields are guarded by mu; the clock is injectable for
// tests.
type breaker struct {
	mu sync.Mutex

	ratio      float64
	window     []bool // ring of the last len(window) primary outcomes: true = failure
	pos        int
	filled     int
	failures   int
	openFor    time.Duration
	probeEvery time.Duration
	needProbes int

	open      bool
	openedAt  time.Time
	lastProbe time.Time
	probing   bool // a half-open probe is in flight (single flight)
	successes int  // consecutive half-open successes
}

func newBreaker(cfg Config) *breaker {
	return &breaker{
		ratio:      cfg.FailureRatio,
		window:     make([]bool, cfg.MinSamples),
		openFor:    cfg.OpenFor,
		probeEvery: cfg.ProbeEvery,
		needProbes: cfg.HalfOpenProbes,
	}
}

// isOpen is the hot-path check.
func (b *breaker) isOpen() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.open
}

// record adds one closed-state outcome and reports whether it tripped the
// breaker (only the transition returns true).
func (b *breaker) record(failed bool, now time.Time) (tripped bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.open {
		return false
	}
	if b.window[b.pos] {
		b.failures--
	}
	b.window[b.pos] = failed
	if failed {
		b.failures++
	}
	b.pos = (b.pos + 1) % len(b.window)
	if b.filled < len(b.window) {
		b.filled++
	}
	if b.filled < len(b.window) || float64(b.failures)/float64(b.filled) < b.ratio {
		return false
	}
	b.trip(now)
	return true
}

// trip opens the breaker (mu held).
func (b *breaker) trip(now time.Time) {
	b.open = true
	b.openedAt = now
	b.lastProbe = time.Time{}
	b.successes = 0
	b.probing = false
}

// acquireProbe reports whether the caller may send one probe to the primary:
// the breaker is open, OpenFor has passed since it opened, ProbeEvery has
// passed since the last probe, and no other probe is in flight.
func (b *breaker) acquireProbe(now time.Time) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.open || b.probing || now.Sub(b.openedAt) < b.openFor {
		return false
	}
	if !b.lastProbe.IsZero() && now.Sub(b.lastProbe) < b.probeEvery {
		return false
	}
	b.probing = true
	b.lastProbe = now
	return true
}

// probeResult releases the probe slot and reports whether the breaker just
// closed.
func (b *breaker) probeResult(ok bool) (closed bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.probing = false
	if !b.open {
		return false
	}
	if !ok {
		b.successes = 0
		return false
	}
	b.successes++
	if b.successes < b.needProbes {
		return false
	}
	b.reset()
	return true
}

// reset closes the breaker with a clean window (mu held).
func (b *breaker) reset() {
	b.open = false
	b.openedAt = time.Time{}
	b.successes = 0
	b.probing = false
	b.failures = 0
	b.filled = 0
	b.pos = 0
	clear(b.window)
}

// state is the State() view.
func (b *breaker) state() (open bool, since time.Time) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.open, b.openedAt
}
