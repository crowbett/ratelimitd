//go:build integration

package lease

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/crowbett/ratelimitd/internal/limit"
	"github.com/crowbett/ratelimitd/internal/store/local"
	"github.com/crowbett/ratelimitd/internal/store/redisx"
)

// TestSharedBoundRedis: 4 nodes (4 Stores, each with its own redisx client
// and pool, as 4 daemons would have) x 8 goroutines hammer one shared GCRA
// policy through real Redis at ~4x its rate; every 8th request also carries
// a per-subject policy (so the all-or-nothing path and its refunds run
// against Redis too without the subject round trip capping the offered load). Admitted units must stay within
// [0.9*rate*elapsed - nodes*chunk, rate*elapsed + burst], and the Redis
// calls for the shared key must be ~admitted/chunk, not one per request.
func TestSharedBoundRedis(t *testing.T) {
	addr := os.Getenv("TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("TEST_REDIS_ADDR not set")
	}
	dur := 5 * time.Second
	if testing.Short() {
		dur = 2 * time.Second
	}
	const (
		rate    = 5_000
		burst   = 500
		nodes   = 4
		workers = 8
	)
	prefix := fmt.Sprintf("test:lease:%d:%d:", os.Getpid(), rand.Int64())
	sp, err := limit.NewParams(limit.GCRA, rate, time.Second, burst)
	if err != nil {
		t.Fatal(err)
	}
	sh := limit.Req{Key: "rl:{g=ingress}:p=ingress", Params: sp, Cost: 1, Policy: "ingress", Shared: true}
	up, err := limit.NewParams(limit.GCRA, 1_000_000, time.Second, 1_000_000)
	if err != nil {
		t.Fatal(err)
	}
	cfg := Config{Span: 5 * time.Millisecond, TTL: 50 * time.Millisecond}
	stores := make([]*Store, nodes)
	for i := range stores {
		rb, err := redisx.New(redisx.Config{Addr: addr, PoolSize: 16, KeyPrefix: prefix, TTLSlack: time.Minute, ReadTimeout: time.Second}, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = rb.Close() })
		stores[i] = New(rb, rb, local.New(local.Config{}), nil, cfg)
	}
	t.Cleanup(func() {
		rb, _ := redisx.New(redisx.Config{Addr: addr, KeyPrefix: prefix}, nil, nil)
		_ = rb.Reset(context.Background(), []string{sh.Key})
		_ = rb.Close()
	})

	var (
		admitted, errs atomic.Int64
		wg             sync.WaitGroup
	)
	ctx := context.Background()
	start := time.Now()
	deadline := start.Add(dur)
	interval := time.Duration(float64(time.Second) * nodes * workers / (4 * rate))
	for n := range nodes {
		for w := range workers {
			wg.Go(func() {
				sub := limit.Req{Key: limit.Key("lease", fmt.Sprintf("n%dw%d", n, w), "api"), Params: up, Cost: 1, Policy: "api"}
				next := time.Now()
				for i := 0; ; i++ {
					now := time.Now()
					if !now.Before(deadline) {
						return
					}
					reqs := []limit.Req{sh}
					if i%8 == 0 {
						reqs = append(reqs, sub)
					}
					ds, err := stores[n].Apply(ctx, reqs, now)
					switch {
					case err != nil:
						errs.Add(1)
					case limit.Strictest(ds).Allowed:
						admitted.Add(1)
					}
					if next = next.Add(interval); time.Until(next) > 0 {
						time.Sleep(time.Until(next))
					}
				}
			})
		}
	}
	wg.Wait()
	elapsed := time.Since(start).Seconds()
	got := float64(admitted.Load())
	chunk := float64(stores[0].chunk(sh.Params, 1))
	lo, hi := rate*elapsed*0.9-nodes*chunk, rate*elapsed+burst
	var leases, denied int64
	for _, s := range stores {
		leases += s.Stats().Leases
		denied += s.Stats().Denied
	}
	t.Logf("elapsed %.2fs admitted %.0f bound [%.0f, %.0f]; %d lease calls (%d denied) for %.0f admits = %.1f admits per Redis call; errors %d",
		elapsed, got, lo, hi, leases, denied, got, got/float64(max(leases, 1)), errs.Load())
	if errs.Load() != 0 {
		t.Fatalf("%d errors", errs.Load())
	}
	if denied == 0 {
		t.Fatal("no lease was ever denied: the offered load never reached the limit, the bound was not tested")
	}
	if got > hi {
		t.Fatalf("admitted %.0f > rate*elapsed + burst %.0f: the shared limit over-admitted", got, hi)
	}
	if got < lo {
		t.Fatalf("admitted %.0f < %.0f: leasing stranded too much budget", got, lo)
	}
}
