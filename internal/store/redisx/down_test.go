package redisx

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/crowbett/ratelimitd/internal/admin"
	"github.com/crowbett/ratelimitd/internal/limit"
)

// closedAddr returns a loopback address nothing listens on, so every dial is
// refused immediately (no Redis needed; the success paths are covered by the
// integration tests behind TEST_REDIS_ADDR).
func closedAddr(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := lis.Addr().String()
	_ = lis.Close()
	return addr
}

func newDown(t *testing.T) (*Backend, *admin.Snapshot) {
	t.Helper()
	snap := admin.NewSnapshot()
	b, err := New(Config{Addr: closedAddr(t), ReadTimeout: 20 * time.Millisecond, DialTimeout: 50 * time.Millisecond, KeyPrefix: "test:"},
		slog.New(slog.DiscardHandler), snap)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	return b, snap
}

// TestDownRedis pins the transport-failure path of every method: each
// returns ErrBackendDown (so the fallback breaker matches it), records a
// backend error under its kind, never loads the script, and ApplyMany
// reports the pipeline failure as the third result with per-group errors
// left nil.
func TestDownRedis(t *testing.T) {
	// Not parallel: the subtests share one backend and the error-series
	// assertion at the end reads what they recorded, in order.
	b, snap := newDown(t)
	if b.Config().Addr == "" || b.Config().PoolSize != 64 || b.Config().WriteTimeout != 20*time.Millisecond || b.Config().TTLSlack != time.Second {
		t.Fatalf("defaults not applied: %+v", b.Config())
	}
	if b.Name() != limit.BackendRedis {
		t.Fatalf("Name = %q", b.Name())
	}
	if b.ScriptLoads() != 0 {
		t.Fatalf("script loaded against a closed port: %d", b.ScriptLoads())
	}
	ctx := context.Background()
	p, _ := limit.NewParams(limit.GCRA, 100, time.Second, 10)
	reqs := []limit.Req{{Key: limit.Key("t", "s", "p"), Params: p, Cost: 1, Policy: "p"}}

	calls := map[string]func() error{
		"Ping": func() error { return b.Ping(ctx) },
		"Apply": func() error {
			ds, err := b.Apply(ctx, reqs, time.Now())
			if ds != nil {
				t.Errorf("Apply returned decisions %+v with error %v", ds, err)
			}
			return err
		},
		"Peek": func() error {
			_, err := b.Peek(ctx, reqs, time.Now())
			return err
		},
		"Reset": func() error { return b.Reset(ctx, []string{"a", "b"}) },
		"Skew": func() error {
			_, err := b.Skew(ctx, time.Now())
			return err
		},
	}
	for name, call := range calls {
		t.Run(name, func(t *testing.T) {
			err := call()
			if !errors.Is(err, limit.ErrBackendDown) {
				t.Fatalf("%s = %v, want ErrBackendDown", name, err)
			}
		})
	}
	t.Run("ApplyMany", func(t *testing.T) {
		groups := [][]limit.Req{reqs, nil, reqs}
		out, errs, err := b.ApplyMany(ctx, groups, time.Now())
		if !errors.Is(err, limit.ErrBackendDown) {
			t.Fatalf("pipeline error = %v, want ErrBackendDown", err)
		}
		if len(out) != 3 || len(errs) != 3 {
			t.Fatalf("out/errs sized %d/%d for 3 groups", len(out), len(errs))
		}
		for i := range groups {
			if out[i] != nil || errs[i] != nil {
				t.Fatalf("group %d = %+v, %v; the pipeline error is the only error", i, out[i], errs[i])
			}
		}
	})
	t.Run("empty inputs need no round trip", func(t *testing.T) {
		if ds, err := b.Apply(ctx, nil, time.Now()); err != nil || len(ds) != 0 {
			t.Fatalf("Apply(nil) = %v, %v", ds, err)
		}
		if err := b.Reset(ctx, nil); err != nil {
			t.Fatalf("Reset(nil) = %v", err)
		}
		out, errs, err := b.ApplyMany(ctx, [][]limit.Req{{}}, time.Now())
		if err != nil || errs[0] != nil || out[0] == nil || len(out[0]) != 0 {
			t.Fatalf("ApplyMany(empty group) = %v, %v, %v", out, errs, err)
		}
	})
	t.Run("invalid params never reach the wire", func(t *testing.T) {
		bad := []limit.Req{{Key: "k", Params: limit.Params{Algo: limit.GCRA}, Cost: 1}}
		if _, err := b.Apply(ctx, bad, time.Now()); !errors.Is(err, limit.ErrInvalidParams) || errors.Is(err, limit.ErrBackendDown) {
			t.Fatalf("Apply(invalid) = %v, want ErrInvalidParams only", err)
		}
		_, errs, err := b.ApplyMany(ctx, [][]limit.Req{bad}, time.Now())
		if err != nil || !errors.Is(errs[0], limit.ErrInvalidParams) {
			t.Fatalf("ApplyMany(invalid) = %v, %v", errs, err)
		}
	})
	if kinds := snap.BackendErrors(); len(kinds) == 0 {
		t.Fatal("no backend errors recorded")
	} else {
		for _, k := range kinds {
			if k.Key.Backend != limit.BackendRedis || (k.Key.Kind != KindConn && k.Key.Kind != KindTimeout) {
				t.Fatalf("unexpected error series %+v", k)
			}
		}
	}
}

// TestPackPrefixesKeys pins the outer KeyPrefix and the exact-variant nonce.
func TestPackPrefixesKeys(t *testing.T) {
	t.Parallel()
	b, _ := newDown(t)
	g, _ := limit.NewParams(limit.GCRA, 100, time.Second, 10)
	x, _ := limit.NewParams(limit.SlidingWindowExact, 3, time.Second, 3)
	reqs := []limit.Req{
		{Key: limit.Key("t", "s", "a"), Params: g, Cost: 1, Policy: "a"},
		{Key: limit.Key("t", "s", "b"), Params: x, Cost: 1, Policy: "b"},
	}
	keys, args := b.pack(reqs, time.UnixMicro(1_700_000_000_000_000), true)
	if keys[0] != "test:rl:{t=t|s=s}:p=a" || keys[1] != "test:rl:{t=t|s=s}:p=b" {
		t.Fatalf("keys = %v", keys)
	}
	if len(args) != 2*argvPerKey+1 || args[len(args)-1] != "1" {
		t.Fatalf("args = %v", args)
	}
	nonce, _ := args[argvPerKey-1].(int64)
	if nonce == 0 {
		t.Fatal("no nonce for a sliding_window_exact req")
	}
	if n2, _ := args[2*argvPerKey-1].(int64); n2 != nonce {
		t.Fatalf("nonce differs across keys of one call: %d vs %d", nonce, n2)
	}
	keys, args = b.pack(reqs[:1], time.UnixMicro(1), false)
	if len(keys) != 1 || args[argvPerKey-1] != int64(0) || args[argvPerKey] != "0" {
		t.Fatalf("gcra-only call: keys=%v args=%v; want nonce 0 and peek flag", keys, args)
	}
}
