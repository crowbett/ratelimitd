package fallback

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/crowbett/ratelimitd/internal/limit"
)

// batchFake is fakeStore plus limit.BatchStore: ApplyMany answers every
// group through the scripted answer, with per-group failures (failGroups)
// and a whole-pipeline failure (pipeErr) on top.
type batchFake struct {
	*fakeStore
	batches    atomic.Int64
	failGroups map[int]error
	pipeErr    atomic.Pointer[error]
}

func (b *batchFake) ApplyMany(ctx context.Context, groups [][]limit.Req, _ time.Time) ([][]limit.Decision, []error, error) {
	b.batches.Add(1)
	if lag := b.lag.Load(); lag > 0 && b.clock != nil {
		b.clock.advance(time.Duration(lag))
	}
	if e := b.pipeErr.Load(); e != nil {
		return nil, nil, *e
	}
	out := make([][]limit.Decision, len(groups))
	errs := make([]error, len(groups))
	for i, g := range groups {
		if e, ok := b.failGroups[i]; ok {
			errs[i] = e
			continue
		}
		out[i], errs[i] = b.answer(ctx, g, false)
	}
	return out, errs, nil
}

func groupsOf(n int) [][]limit.Req {
	out := make([][]limit.Req, n)
	for i := range out {
		out[i] = []limit.Req{{Key: limit.Key("t", fmt.Sprintf("s%d", i), "p"), Params: gcraP, Cost: 1, Policy: "p"}}
	}
	return out
}

func newBatchHarness(cfg Config) (*harness, *batchFake) {
	clk := &fakeClock{t: t0}
	bf := &batchFake{fakeStore: newFake(limit.BackendRedis, clk), failGroups: map[int]error{}}
	h := &harness{clk: clk, primary: bf.fakeStore, local: newFake(limit.BackendLocal, clk), snap: nil}
	h.store = New(bf, h.local, cfg, nil, nil)
	h.store.clock = clk.now
	return h, bf
}

func TestApplyManyPipelinesWhileClosed(t *testing.T) {
	t.Parallel()
	h, bf := newBatchHarness(Config{Mode: ModeLocal, MinSamples: 4, LatencyBudget: 50 * time.Millisecond})
	out, errs, err := h.store.ApplyMany(context.Background(), groupsOf(3), h.clk.now())
	if err != nil {
		t.Fatal(err)
	}
	if bf.batches.Load() != 1 || bf.calls.Load() != 3 || h.local.calls.Load() != 0 {
		t.Fatalf("batches=%d primary calls=%d local calls=%d, want one pipeline of three", bf.batches.Load(), bf.calls.Load(), h.local.calls.Load())
	}
	for i := range out {
		if errs[i] != nil || len(out[i]) != 1 || out[i][0].Backend != limit.BackendRedis {
			t.Fatalf("group %d: %+v, %v", i, out[i], errs[i])
		}
	}
	if open, _, _ := h.store.State(); open {
		t.Fatal("breaker opened on a clean batch")
	}
}

func TestApplyManyDegradesFailedGroupsOnly(t *testing.T) {
	t.Parallel()
	h, bf := newBatchHarness(Config{Mode: ModeLocal, MinSamples: 2, FailureRatio: 0.5})
	bf.failGroups[1] = fmt.Errorf("%w: redis timeout", limit.ErrBackendDown)
	bf.failGroups[2] = fmt.Errorf("%w: bad cost", limit.ErrInvalidParams) // the caller's fault: returned as is
	out, errs, err := h.store.ApplyMany(context.Background(), groupsOf(4), h.clk.now())
	if err != nil {
		t.Fatal(err)
	}
	if out[0][0].Backend != limit.BackendRedis || errs[0] != nil {
		t.Fatalf("group 0 = %+v, %v", out[0], errs[0])
	}
	if errs[1] != nil || out[1][0].Backend != limit.BackendDegraded {
		t.Fatalf("failed group 1 not answered by the degraded path: %+v, %v", out[1], errs[1])
	}
	if !errors.Is(errs[2], limit.ErrInvalidParams) || out[2] != nil {
		t.Fatalf("invalid group 2 = %+v, %v; want ErrInvalidParams passed through", out[2], errs[2])
	}
	if out[3][0].Backend != limit.BackendRedis || errs[3] != nil {
		t.Fatalf("group 3 = %+v, %v", out[3], errs[3])
	}
	if h.local.calls.Load() != 1 {
		t.Fatalf("local calls = %d, want exactly the failed group", h.local.calls.Load())
	}
	// The batch is ONE breaker sample: a second failing batch reaches
	// MinSamples=2 at ratio 1.0 and opens it.
	if open, _, _ := h.store.State(); open {
		t.Fatal("breaker opened after one sample")
	}
	if _, _, err := h.store.ApplyMany(context.Background(), groupsOf(2), h.clk.now()); err != nil {
		t.Fatal(err)
	}
	if open, _, _ := h.store.State(); !open {
		t.Fatal("breaker did not open after two failed batches")
	}
}

func TestApplyManyPipelineFailure(t *testing.T) {
	t.Parallel()
	t.Run("recoverable: every group degraded, breaker sampled", func(t *testing.T) {
		t.Parallel()
		h, bf := newBatchHarness(Config{Mode: ModeFailOpen, MinSamples: 1, FailureRatio: 1})
		e := fmt.Errorf("%w: pipeline exec", limit.ErrBackendDown)
		bf.pipeErr.Store(&e)
		out, errs, err := h.store.ApplyMany(context.Background(), groupsOf(2), h.clk.now())
		if err != nil {
			t.Fatal(err)
		}
		for i := range out {
			if errs[i] != nil || !out[i][0].Allowed || out[i][0].Backend != limit.BackendDegraded {
				t.Fatalf("group %d = %+v, %v; want fail-open degraded", i, out[i], errs[i])
			}
		}
		if open, _, _ := h.store.State(); !open {
			t.Fatal("a failed pipeline with MinSamples=1 must open the breaker")
		}
	})
	t.Run("cancelled context is returned, not degraded", func(t *testing.T) {
		t.Parallel()
		h, bf := newBatchHarness(Config{Mode: ModeLocal, MinSamples: 1})
		e := fmt.Errorf("pipeline: %w", context.Canceled)
		bf.pipeErr.Store(&e)
		out, errs, err := h.store.ApplyMany(context.Background(), groupsOf(2), h.clk.now())
		if !errors.Is(err, context.Canceled) || out != nil || errs != nil {
			t.Fatalf("= %v, %v, %v; want context.Canceled alone", out, errs, err)
		}
		if open, _, _ := h.store.State(); open {
			t.Fatal("a cancelled call counted against the breaker")
		}
	})
}

func TestApplyManyOverBudgetCountsAsFailure(t *testing.T) {
	t.Parallel()
	h, bf := newBatchHarness(Config{Mode: ModeLocal, MinSamples: 1, FailureRatio: 1, LatencyBudget: 10 * time.Millisecond})
	bf.lag.Store(int64(20 * time.Millisecond))
	out, errs, err := h.store.ApplyMany(context.Background(), groupsOf(2), h.clk.now())
	if err != nil {
		t.Fatal(err)
	}
	for i := range out {
		if errs[i] != nil || out[i][0].Backend != limit.BackendRedis {
			t.Fatalf("group %d = %+v, %v; a slow success still returns the primary's decisions", i, out[i], errs[i])
		}
	}
	if open, _, _ := h.store.State(); !open {
		t.Fatal("over-budget batch did not trip the breaker")
	}
}

func TestApplyManyWhileOpenGoesThroughApply(t *testing.T) {
	t.Parallel()
	h, bf := newBatchHarness(Config{Mode: ModeLocal, MinSamples: 2, FailureRatio: 0.5, OpenFor: time.Minute, ProbeEvery: time.Second})
	h.open(t)
	before := bf.batches.Load()
	out, errs, err := h.store.ApplyMany(context.Background(), groupsOf(3), h.clk.now())
	if err != nil {
		t.Fatal(err)
	}
	if bf.batches.Load() != before {
		t.Fatal("pipelined while the breaker was open")
	}
	for i := range out {
		if errs[i] != nil || out[i][0].Backend != limit.BackendDegraded {
			t.Fatalf("group %d = %+v, %v; want degraded", i, out[i], errs[i])
		}
	}
}

func TestApplyManyWithoutBatchStoreFallsBackToApply(t *testing.T) {
	t.Parallel()
	h := newHarness(Config{Mode: ModeLocal})
	out, errs, err := h.store.ApplyMany(context.Background(), groupsOf(3), h.clk.now())
	if err != nil {
		t.Fatal(err)
	}
	if h.primary.calls.Load() != 3 {
		t.Fatalf("primary calls = %d, want one Apply per group", h.primary.calls.Load())
	}
	for i := range out {
		if errs[i] != nil || out[i][0].Backend != limit.BackendRedis {
			t.Fatalf("group %d = %+v, %v", i, out[i], errs[i])
		}
	}
}
