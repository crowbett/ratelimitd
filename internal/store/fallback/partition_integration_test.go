//go:build integration

package fallback

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"testing"
	"time"

	"github.com/crowbett/ratelimitd/internal/admin"
	"github.com/crowbett/ratelimitd/internal/limit"
	"github.com/crowbett/ratelimitd/internal/store/local"
	"github.com/crowbett/ratelimitd/internal/store/redisx"
	"github.com/crowbett/ratelimitd/internal/testproxy"
)

// TestPartition is the docs/PLAN.md partition case: redisx through the
// blackhole proxy, fallback(local) with 50 ms timeouts. The breaker must open
// within MinSamples failures and under 500 ms, decisions keep flowing as
// "degraded" with over-admission bounded by the scaled local limits,
// rl_fallback_active goes 1, and after Restore the prober closes the breaker
// within 2*ProbeEvery.
func TestPartition(t *testing.T) {
	addr := os.Getenv("TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("TEST_REDIS_ADDR not set")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	proxy, err := testproxy.New(ctx, addr)
	if err != nil {
		t.Fatal(err)
	}
	defer proxy.Close()

	const (
		rate    = 200
		burst   = 20
		divisor = 2
		timeout = 50 * time.Millisecond
		probe   = 100 * time.Millisecond
	)
	snap := admin.NewSnapshot()
	prefix := fmt.Sprintf("test:partition:%d:%d:", os.Getpid(), rand.Int64())
	rx, err := redisx.New(redisx.Config{
		Addr: proxy.Addr(), PoolSize: 8, KeyPrefix: prefix,
		DialTimeout: timeout, ReadTimeout: timeout, WriteTimeout: timeout,
	}, nil, snap)
	if err != nil {
		t.Fatal(err)
	}
	defer rx.Close()
	if err := rx.Ping(ctx); err != nil {
		t.Fatalf("Ping through the proxy: %v", err)
	}

	loc := local.New(local.Config{}).Scale(divisor)
	fb := New(rx, loc, Config{
		Mode: ModeLocal, DegradedDivisor: divisor,
		FailureRatio: 0.5, MinSamples: 5, LatencyBudget: timeout,
		OpenFor: probe, ProbeEvery: probe, HalfOpenProbes: 1,
	}, nil, snap)
	go func() { _ = fb.Run(ctx) }()

	p, _ := limit.NewParams(limit.GCRA, rate, time.Second, burst)
	key := limit.Key("partition", "subject", "api")
	reqs := []limit.Req{{Key: key, Params: p, Cost: 1, Policy: "api"}}
	apply := func() (limit.Decision, error) {
		ds, err := fb.Apply(ctx, reqs, time.Now())
		if err != nil {
			return limit.Decision{}, err
		}
		return ds[0], nil
	}
	t.Cleanup(func() { _ = rx.Reset(context.Background(), []string{key}) })

	// Healthy: Redis answers.
	for i := range 3 {
		d, err := apply()
		if err != nil || d.Backend != limit.BackendRedis || !d.Allowed {
			t.Fatalf("healthy call %d: %+v %v", i, d, err)
		}
	}

	// Partition: the breaker opens within MinSamples failures and < 500 ms.
	proxy.Blackhole()
	start := time.Now()
	failures := 0
	for {
		d, err := apply()
		if err != nil {
			t.Fatalf("during partition: %v", err)
		}
		if d.Backend != limit.BackendDegraded {
			t.Fatalf("during partition: %+v", d)
		}
		failures++
		if open, _, _ := fb.State(); open {
			break
		}
		if failures > 10 {
			t.Fatalf("breaker still closed after %d failures", failures)
		}
	}
	openIn := time.Since(start)
	t.Logf("breaker opened after %d failed calls in %v", failures, openIn)
	if failures > 5 || openIn >= 500*time.Millisecond {
		t.Fatalf("breaker opened after %d failures in %v (want <= 5 and < 500ms)", failures, openIn)
	}
	if !snap.Counters().FallbackActive || fb.Name() != limit.BackendDegraded {
		t.Fatalf("FallbackActive %v Name %q", snap.Counters().FallbackActive, fb.Name())
	}

	// Degraded: decisions keep flowing; over-admission is bounded by the
	// scaled local limits: rate/divisor per second plus burst/divisor.
	var (
		allowed  int
		calls    int
		first    time.Time
		last     time.Time
		errCount int
	)
	gap := time.Now().Add(time.Second)
	for time.Now().Before(gap) {
		now := time.Now()
		ds, err := fb.Apply(ctx, reqs, now)
		calls++
		if err != nil {
			errCount++
			continue
		}
		if ds[0].Backend != limit.BackendDegraded {
			t.Fatalf("degraded call answered by %q", ds[0].Backend)
		}
		if first.IsZero() {
			first = now
		}
		last = now
		if ds[0].Allowed {
			allowed++
		}
		time.Sleep(200 * time.Microsecond)
	}
	if open, _, _ := fb.State(); !open {
		t.Fatal("breaker closed while Redis was still blackholed")
	}
	elapsed := last.Sub(first).Seconds()
	bound := float64(rate)/divisor*elapsed + float64(burst)/divisor
	t.Logf("degraded: %d calls, %d allowed over %.2fs, bound %.1f, errors %d", calls, allowed, elapsed, bound, errCount)
	if errCount != 0 {
		t.Fatalf("%d errors on the degraded path", errCount)
	}
	if float64(allowed) > bound {
		t.Fatalf("over-admission: %d allowed > %.1f", allowed, bound)
	}
	if float64(allowed) < bound*0.5 {
		t.Fatalf("degraded path admitted %d, far below the local limit %.1f", allowed, bound)
	}

	// Restore: the prober closes the breaker within 2*ProbeEvery.
	proxy.Restore()
	restored := time.Now()
	for {
		if open, _, _ := fb.State(); !open {
			break
		}
		if time.Since(restored) > 2*probe {
			t.Fatalf("breaker still open %v after Restore (want <= %v)", time.Since(restored), 2*probe)
		}
		time.Sleep(5 * time.Millisecond)
	}
	closeIn := time.Since(restored)
	t.Logf("breaker closed %v after Restore", closeIn)
	if snap.Counters().FallbackActive || fb.Name() != limit.BackendRedis {
		t.Fatalf("after close: FallbackActive %v Name %q", snap.Counters().FallbackActive, fb.Name())
	}
	d, err := apply()
	if err != nil || d.Backend != limit.BackendRedis {
		t.Fatalf("after Restore: %+v %v", d, err)
	}
	if c := snap.Counters(); c.BackendErrors == 0 {
		t.Fatal("no backend errors were recorded across the partition")
	}
}
