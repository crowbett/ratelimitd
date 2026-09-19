package fallback

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/crowbett/ratelimitd/internal/admin"
	"github.com/crowbett/ratelimitd/internal/limit"
)

var (
	t0     = time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC)
	gcraP  = mustParams(limit.GCRA, 100, time.Second, 10)
	swP    = mustParams(limit.SlidingWindow, 60, time.Minute, 60)
	oneReq = []limit.Req{{Key: limit.Key("t", "s", "p"), Params: gcraP, Cost: 1, Policy: "p"}}
)

func mustParams(algo limit.Algorithm, rate int64, period time.Duration, burst int64) limit.Params {
	p, err := limit.NewParams(algo, rate, period, burst)
	if err != nil {
		panic(err)
	}
	return p
}

// fakeClock is the Store's wall clock under test.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

// fakeStore is a scripted limit.Store: fail switches every call to
// ErrBackendDown, lag advances the clock per call (latency budget), block
// parks calls until released (single-flight probes).
type fakeStore struct {
	name    string
	clock   *fakeClock
	fail    atomic.Bool
	errOnce atomic.Pointer[error]
	lag     atomic.Int64 // nanoseconds the clock advances per call
	block   atomic.Pointer[chan struct{}]

	calls       atomic.Int64
	peeks       atomic.Int64
	inflight    atomic.Int64
	maxInflight atomic.Int64
	mu          sync.Mutex
	keys        []string
	resets      [][]string
}

func newFake(name string, clk *fakeClock) *fakeStore { return &fakeStore{name: name, clock: clk} }

func (f *fakeStore) answer(ctx context.Context, reqs []limit.Req, peek bool) ([]limit.Decision, error) {
	f.calls.Add(1)
	if peek {
		f.peeks.Add(1)
	}
	if n := f.inflight.Add(1); n > f.maxInflight.Load() {
		f.maxInflight.Store(n)
	}
	defer f.inflight.Add(-1)
	f.mu.Lock()
	for _, r := range reqs {
		f.keys = append(f.keys, r.Key)
	}
	f.mu.Unlock()
	if b := f.block.Load(); b != nil {
		select {
		case <-*b:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if lag := f.lag.Load(); lag > 0 && f.clock != nil {
		f.clock.advance(time.Duration(lag))
	}
	if e := f.errOnce.Swap(nil); e != nil {
		return nil, *e
	}
	if f.fail.Load() {
		return nil, fmt.Errorf("%w: %s: i/o timeout", limit.ErrBackendDown, f.name)
	}
	out := make([]limit.Decision, len(reqs))
	for i, r := range reqs {
		out[i] = limit.Decision{Allowed: true, Remaining: int(r.Params.Burst) - 1, Limit: int(r.Params.Burst), Policy: r.Policy, Backend: f.name}
	}
	return out, nil
}

func (f *fakeStore) Apply(ctx context.Context, reqs []limit.Req, _ time.Time) ([]limit.Decision, error) {
	return f.answer(ctx, reqs, false)
}

func (f *fakeStore) Peek(ctx context.Context, reqs []limit.Req, _ time.Time) ([]limit.Decision, error) {
	return f.answer(ctx, reqs, true)
}

func (f *fakeStore) Reset(_ context.Context, keys []string) error {
	f.mu.Lock()
	f.resets = append(f.resets, keys)
	f.mu.Unlock()
	if f.fail.Load() {
		return fmt.Errorf("%w: %s", limit.ErrBackendDown, f.name)
	}
	return nil
}

func (f *fakeStore) Name() string { return f.name }

func (f *fakeStore) sawKey(key string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, k := range f.keys {
		if k == key {
			return true
		}
	}
	return false
}

type harness struct {
	clk     *fakeClock
	primary *fakeStore
	local   *fakeStore
	snap    *admin.Snapshot
	store   *Store
}

func newHarness(cfg Config) *harness {
	clk := &fakeClock{t: t0}
	h := &harness{
		clk:     clk,
		primary: newFake(limit.BackendRedis, clk),
		local:   newFake(limit.BackendLocal, clk),
		snap:    admin.NewSnapshot(),
	}
	h.store = New(h.primary, h.local, cfg, nil, h.snap)
	h.store.clock = clk.now
	return h
}

func (h *harness) apply(tb testing.TB) limit.Decision {
	tb.Helper()
	ds, err := h.store.Apply(context.Background(), oneReq, h.clk.now())
	if err != nil {
		tb.Fatalf("Apply: %v", err)
	}
	if len(ds) != 1 {
		tb.Fatalf("len(ds) = %d", len(ds))
	}
	return ds[0]
}

func (h *harness) open(tb testing.TB) {
	tb.Helper()
	h.primary.fail.Store(true)
	for range h.store.cfg.MinSamples {
		h.apply(tb)
	}
	if open, _, _ := h.store.State(); !open {
		tb.Fatal("breaker did not open")
	}
}

func TestDefaults(t *testing.T) {
	t.Parallel()
	cfg := New(nil, nil, Config{}, nil, nil).Config()
	want := Config{DegradedDivisor: 1, FailureRatio: 0.5, MinSamples: 20, OpenFor: 5 * time.Second, ProbeEvery: 500 * time.Millisecond, HalfOpenProbes: 3}
	if cfg != want {
		t.Fatalf("defaults = %+v, want %+v", cfg, want)
	}
	for _, m := range []Mode{ModeLocal, ModeFailOpen, ModeFailClosed} {
		got, err := ParseMode(m.String())
		if err != nil || got != m {
			t.Errorf("ParseMode(%q) = %v, %v", m, got, err)
		}
	}
	if _, err := ParseMode("nope"); err == nil {
		t.Error("ParseMode accepted nope")
	}
	if Mode(9).String() != "mode(9)" {
		t.Error("Mode(9).String()")
	}
}

func TestOpensExactlyAtMinSamples(t *testing.T) {
	t.Parallel()
	healthy := newHarness(Config{MinSamples: 5})
	if d := healthy.apply(t); d.Backend != limit.BackendRedis || healthy.store.Name() != limit.BackendRedis {
		t.Fatalf("healthy: %+v %s", d, healthy.store.Name())
	}
	h := newHarness(Config{MinSamples: 5, FailureRatio: 0.5, OpenFor: time.Second})
	h.primary.fail.Store(true)
	for i := 1; i <= 4; i++ {
		d := h.apply(t) // failed calls are answered by the degraded path even before the breaker opens
		if d.Backend != limit.BackendDegraded || !d.Allowed {
			t.Fatalf("failure %d: %+v", i, d)
		}
		if open, _, _ := h.store.State(); open {
			t.Fatalf("open after %d failures (MinSamples 5)", i)
		}
	}
	h.apply(t)
	open, since, mode := h.store.State()
	if !open || !since.Equal(t0) || mode != ModeLocal {
		t.Fatalf("State = %v %v %v", open, since, mode)
	}
	if h.store.Name() != limit.BackendDegraded || !h.snap.Counters().FallbackActive {
		t.Fatalf("Name %q FallbackActive %v", h.store.Name(), h.snap.Counters().FallbackActive)
	}
	// Open: the primary is not touched (OpenFor has not passed).
	calls := h.primary.calls.Load()
	for range 10 {
		if d := h.apply(t); d.Backend != limit.BackendDegraded {
			t.Fatalf("while open: %+v", d)
		}
	}
	if h.primary.calls.Load() != calls {
		t.Fatalf("primary called %d times while open", h.primary.calls.Load()-calls)
	}
	// The window counts the fake's real successes: 4 of 10 with ratio 0.5 stays closed.
	h2 := newHarness(Config{MinSamples: 10, FailureRatio: 0.5})
	for i := range 10 {
		h2.primary.fail.Store(i == 1 || i == 2 || i == 4 || i == 5) // 4/10
		h2.apply(t)
	}
	if open, _, _ := h2.store.State(); open {
		t.Fatal("opened below the ratio")
	}
	h2.primary.fail.Store(true) // the 11th sample evicts the success in slot 0: 5/10
	h2.apply(t)
	if open, _, _ := h2.store.State(); !open {
		t.Fatal("did not open at the ratio")
	}
}

func TestLatencyBudget(t *testing.T) {
	t.Parallel()
	h := newHarness(Config{MinSamples: 3, FailureRatio: 1, LatencyBudget: 10 * time.Millisecond})
	h.primary.lag.Store(int64(20 * time.Millisecond))
	for i := range 3 {
		if d := h.apply(t); d.Backend != limit.BackendRedis { // slow but successful: the answer still counts
			t.Fatalf("call %d: %+v", i, d)
		}
	}
	if open, _, _ := h.store.State(); !open {
		t.Fatal("slow primary did not open the breaker")
	}
	h.primary.lag.Store(int64(5 * time.Millisecond))
	h3 := newHarness(Config{MinSamples: 3, FailureRatio: 1, LatencyBudget: 10 * time.Millisecond})
	h3.primary.lag.Store(int64(5 * time.Millisecond))
	for range 6 {
		h3.apply(t)
	}
	if open, _, _ := h3.store.State(); open {
		t.Fatal("fast primary opened the breaker")
	}
}

func TestNonRecoverableErrors(t *testing.T) {
	t.Parallel()
	h := newHarness(Config{MinSamples: 1, FailureRatio: 1})
	bad := fmt.Errorf("%w: nope", limit.ErrInvalidParams)
	h.primary.errOnce.Store(&bad)
	if _, err := h.store.Apply(context.Background(), oneReq, t0); !errors.Is(err, limit.ErrInvalidParams) {
		t.Fatalf("err = %v", err)
	}
	canceled := context.Canceled
	h.primary.errOnce.Store(&canceled)
	if _, err := h.store.Apply(context.Background(), oneReq, t0); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v", err)
	}
	if open, _, _ := h.store.State(); open {
		t.Fatal("caller errors counted against the breaker")
	}
	if h.local.calls.Load() != 0 {
		t.Fatal("caller errors went to the degraded path")
	}
}

func TestModeMatrix(t *testing.T) {
	t.Parallel()
	reqs := []limit.Req{
		{Key: "a", Params: gcraP, Cost: 1, Policy: "g"},
		{Key: "b", Params: swP, Cost: 1, Policy: "w"},
	}
	tests := []struct {
		mode Mode
		want []limit.Decision
	}{
		{ModeLocal, []limit.Decision{
			{Allowed: true, Remaining: 9, Limit: 10, Policy: "g", Backend: limit.BackendDegraded},
			{Allowed: true, Remaining: 59, Limit: 60, Policy: "w", Backend: limit.BackendDegraded},
		}},
		{ModeFailOpen, []limit.Decision{
			{Allowed: true, Remaining: 10, Limit: 10, Policy: "g", Backend: limit.BackendDegraded},
			{Allowed: true, Remaining: 60, Limit: 60, Policy: "w", Backend: limit.BackendDegraded},
		}},
		{ModeFailClosed, []limit.Decision{
			{RetryAfter: 50 * time.Millisecond, ResetAfter: 50 * time.Millisecond, Limit: 10, Policy: "g", Backend: limit.BackendDegraded},
			{RetryAfter: 50 * time.Millisecond, ResetAfter: 50 * time.Millisecond, Limit: 60, Policy: "w", Backend: limit.BackendDegraded},
		}},
	}
	for _, tt := range tests {
		t.Run(tt.mode.String(), func(t *testing.T) {
			t.Parallel()
			h := newHarness(Config{Mode: tt.mode, MinSamples: 2, ProbeEvery: 50 * time.Millisecond, OpenFor: time.Hour})
			h.open(t)
			for _, peek := range []bool{false, true} {
				var (
					ds  []limit.Decision
					err error
				)
				if peek {
					ds, err = h.store.Peek(context.Background(), reqs, t0)
				} else {
					ds, err = h.store.Apply(context.Background(), reqs, t0)
				}
				if err != nil {
					t.Fatal(err)
				}
				for i := range tt.want {
					if ds[i] != tt.want[i] {
						t.Errorf("peek=%v ds[%d] = %+v, want %+v", peek, i, ds[i], tt.want[i])
					}
				}
			}
			wantLocal := int64(0)
			if tt.mode == ModeLocal {
				wantLocal = 2 + int64(h.store.cfg.MinSamples) // the two calls above plus the failed ones
			}
			if h.local.calls.Load() != wantLocal {
				t.Errorf("local calls = %d, want %d", h.local.calls.Load(), wantLocal)
			}
		})
	}
}

func TestHalfOpenSingleFlightAndClose(t *testing.T) {
	t.Parallel()
	h := newHarness(Config{MinSamples: 2, OpenFor: time.Second, ProbeEvery: 100 * time.Millisecond, HalfOpenProbes: 2})
	h.open(t)
	h.primary.fail.Store(false)
	calls := h.primary.calls.Load()

	// Before OpenFor: no probe at all.
	h.apply(t)
	if h.primary.calls.Load() != calls {
		t.Fatal("probed before OpenFor")
	}
	h.clk.advance(time.Second)

	// 16 concurrent requests in half-open: exactly one reaches the primary.
	gate := make(chan struct{})
	h.primary.block.Store(&gate)
	var (
		wg       sync.WaitGroup
		degraded atomic.Int64
		primary  atomic.Int64
	)
	for range 16 {
		wg.Go(func() {
			switch d := h.apply(t); d.Backend {
			case limit.BackendDegraded:
				degraded.Add(1)
			case limit.BackendRedis:
				primary.Add(1)
			}
		})
	}
	deadline := time.Now().Add(2 * time.Second)
	for h.primary.inflight.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	time.Sleep(20 * time.Millisecond) // let the other 15 take the degraded path
	close(gate)
	wg.Wait()
	h.primary.block.Store(nil)
	if primary.Load() != 1 || degraded.Load() != 15 || h.primary.maxInflight.Load() != 1 {
		t.Fatalf("primary %d degraded %d maxInflight %d", primary.Load(), degraded.Load(), h.primary.maxInflight.Load())
	}
	if open, _, _ := h.store.State(); !open { // one success of the two required
		t.Fatal("closed after a single probe")
	}

	// A failed probe resets the count; then two successes close it.
	h.clk.advance(100 * time.Millisecond)
	h.primary.fail.Store(true)
	if d := h.apply(t); d.Backend != limit.BackendDegraded {
		t.Fatalf("failed probe answered %+v", d)
	}
	h.primary.fail.Store(false)
	for i := range 2 {
		h.clk.advance(100 * time.Millisecond)
		if d := h.apply(t); d.Backend != limit.BackendRedis {
			t.Fatalf("probe %d: %+v", i, d)
		}
		if open, _, _ := h.store.State(); open != (i == 0) {
			t.Fatalf("after probe %d open=%v", i, open)
		}
	}
	if h.store.Name() != limit.BackendRedis || h.snap.Counters().FallbackActive {
		t.Fatalf("after close: Name %q FallbackActive %v", h.store.Name(), h.snap.Counters().FallbackActive)
	}
	// Closed with a clean window: a single failure does not re-open.
	h.primary.fail.Store(true)
	h.apply(t)
	if open, _, _ := h.store.State(); open {
		t.Fatal("re-opened on one failure after close")
	}
}

func TestRunProber(t *testing.T) {
	t.Parallel()
	h := newHarness(Config{MinSamples: 2, OpenFor: time.Millisecond, ProbeEvery: 5 * time.Millisecond, HalfOpenProbes: 2})
	h.store.clock = time.Now // the prober's ticker is real time
	h.open(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- h.store.Run(ctx) }()
	time.Sleep(30 * time.Millisecond) // failing probes keep it open
	if open, _, _ := h.store.State(); !open {
		t.Fatal("closed while the primary still fails")
	}
	h.primary.fail.Store(false)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if open, _, _ := h.store.State(); !open {
			break
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v", err)
	}
	if open, _, _ := h.store.State(); open {
		t.Fatal("prober did not close the breaker")
	}
	if h.primary.peeks.Load() < 2 || !h.primary.sawKey(h.store.canary[0].Key) {
		t.Fatalf("peeks %d, canary seen %v", h.primary.peeks.Load(), h.primary.sawKey(h.store.canary[0].Key))
	}
}

func TestReset(t *testing.T) {
	t.Parallel()
	h := newHarness(Config{MinSamples: 2})
	if err := h.store.Reset(context.Background(), []string{"a", "b"}); err != nil {
		t.Fatal(err)
	}
	if len(h.primary.resets) != 1 || len(h.local.resets) != 1 {
		t.Fatalf("resets primary %d local %d", len(h.primary.resets), len(h.local.resets))
	}
	h.open(t)
	if err := h.store.Reset(context.Background(), []string{"c"}); err != nil {
		t.Fatalf("Reset while open = %v", err)
	}
	if len(h.primary.resets) != 1 || len(h.local.resets) != 2 {
		t.Fatalf("resets while open: primary %d local %d", len(h.primary.resets), len(h.local.resets))
	}
	h4 := newHarness(Config{MinSamples: 20})
	h4.primary.fail.Store(true)
	if err := h4.store.Reset(context.Background(), []string{"d"}); !errors.Is(err, limit.ErrBackendDown) {
		t.Fatalf("Reset with failing primary = %v", err)
	}
}

func TestConcurrentFlips(t *testing.T) {
	t.Parallel()
	h := newHarness(Config{MinSamples: 4, FailureRatio: 0.5, OpenFor: 2 * time.Millisecond, ProbeEvery: time.Millisecond, HalfOpenProbes: 1})
	h.store.clock = time.Now
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = h.store.Run(ctx) }()
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for range 16 {
		wg.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				ds, err := h.store.Apply(ctx, oneReq, time.Now())
				if err != nil || len(ds) != 1 {
					t.Errorf("Apply = %v, %v", ds, err)
					return
				}
				h.store.Name()
				h.store.State()
			}
		})
	}
	for i := range 100 {
		h.primary.fail.Store(i%2 == 0)
		time.Sleep(time.Millisecond)
	}
	close(stop)
	wg.Wait()
	if h.primary.calls.Load() == 0 || h.local.calls.Load() == 0 {
		t.Fatalf("primary %d local %d", h.primary.calls.Load(), h.local.calls.Load())
	}
}
