//go:build integration

package redisx

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/crowbett/ratelimitd/internal/limit"
)

// evalshaStats reads cmdstat_evalsha and cmdstat_fcall from INFO
// commandstats: total calls and total microseconds of Redis time spent
// inside them (whichever path the backend uses).
func evalshaStats(tb testing.TB, b *Backend) (calls, usec int64) {
	tb.Helper()
	info, err := b.rdb.Info(context.Background(), "commandstats").Result()
	if err != nil {
		tb.Fatal(err)
	}
	for line := range strings.SplitSeq(info, "\n") {
		line = strings.TrimSpace(line)
		rest, ok := strings.CutPrefix(line, "cmdstat_evalsha:")
		if !ok {
			if rest, ok = strings.CutPrefix(line, "cmdstat_fcall:"); !ok {
				continue
			}
		}
		for kv := range strings.SplitSeq(rest, ",") {
			k, v, _ := strings.Cut(kv, "=")
			switch k {
			case "calls":
				n, _ := strconv.ParseInt(v, 10, 64)
				calls += n
			case "usec":
				n, _ := strconv.ParseInt(v, 10, 64)
				usec += n
			}
		}
	}
	return calls, usec
}

// benchBackend is newBackend with a timeout generous enough for a loaded box
// and a pool sized for the parallel callers; it only uses API that predates
// the single-pass change, so the file also builds against the "before" tree
// for A/B runs.
func benchBackend(b *testing.B) *Backend {
	b.Helper()
	addr := os.Getenv("TEST_REDIS_ADDR")
	if addr == "" {
		b.Skip("TEST_REDIS_ADDR not set")
	}
	prefix := fmt.Sprintf("bench:%d:%d:", os.Getpid(), rand.Int64())
	be, err := New(Config{Addr: addr, PoolSize: 128, KeyPrefix: prefix, TTLSlack: time.Minute, ReadTimeout: 2 * time.Second}, nil, nil)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		ctx := context.Background()
		iter := be.rdb.Scan(ctx, 0, prefix+"*", 1000).Iterator()
		var keys []string
		for iter.Next(ctx) {
			keys = append(keys, iter.Val())
		}
		for len(keys) > 0 {
			n := min(len(keys), 1000)
			_ = be.rdb.Del(ctx, keys[:n]...).Err()
			keys = keys[n:]
		}
		_ = be.Close()
	})
	if err := be.Ping(context.Background()); err != nil {
		b.Fatal(err)
	}
	return be
}

// BenchmarkApplyPolicies is the multi-policy matrix: one Apply (one EVALSHA)
// per op for one subject carrying n policies, 10k subjects, 64 concurrent
// callers. ns/op is client-observed throughput; redis-us/call is the Redis CPU
// time per script call from INFO commandstats, the number the single-pass
// dispatcher is meant to cut. Every policy allows (rates far above the load),
// so every call commits: the worst case for the old dry-run + commit loop.
func BenchmarkApplyPolicies(b *testing.B) {
	algos := map[string][]limit.Algorithm{
		"gcra1":  {limit.GCRA},
		"gcra2":  {limit.GCRA, limit.GCRA},
		"gcra4":  {limit.GCRA, limit.GCRA, limit.GCRA, limit.GCRA},
		"mixed4": {limit.GCRA, limit.TokenBucket, limit.SlidingWindow, limit.GCRA},
	}
	for _, name := range []string{"gcra1", "gcra2", "gcra4", "mixed4"} {
		b.Run(name, func(b *testing.B) {
			be := benchBackend(b)
			const subjects = 10_000
			ps := algos[name]
			groups := make([][]limit.Req, subjects)
			for s := range groups {
				tag := limit.HashTag("bench", strconv.Itoa(s))
				for i, a := range ps {
					pol := fmt.Sprintf("p%d", i)
					groups[s] = append(groups[s], limit.Req{
						Key:    limit.KeyPrefix + tag + ":p=" + pol,
						Params: mustParams(b, a, 1_000_000, time.Second, 1_000_000),
						Cost:   1,
						Policy: pol,
					})
				}
			}
			var next atomic.Int64
			ctx := context.Background()
			c0, u0 := evalshaStats(b, be)
			b.SetParallelism(8) // x GOMAXPROCS callers
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					g := groups[next.Add(1)%subjects]
					if _, err := be.Apply(ctx, g, time.Now()); err != nil {
						b.Error(err)
						return
					}
				}
			})
			b.StopTimer()
			c1, u1 := evalshaStats(b, be)
			if c1 > c0 {
				b.ReportMetric(float64(u1-u0)/float64(c1-c0), "redis-us/call")
			}
		})
	}
}
