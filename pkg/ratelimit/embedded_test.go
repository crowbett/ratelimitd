package ratelimit

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/crowbett/ratelimitd/internal/limit"
	"github.com/crowbett/ratelimitd/internal/policy"
)

// fakeResolver stands in for policy.Registry: tenant "acme" has one GCRA
// policy "api" (rate 10/s, burst 5) plus, on route "/v1/search", a stricter
// "search" (rate 10/s, burst 2); subject "vip" is allow-listed; subject
// "banned" is deny-listed; any other tenant is unknown.
type fakeResolver struct {
	api, search limit.Params
}

func newFakeResolver(t *testing.T) *fakeResolver {
	t.Helper()
	api, err := limit.NewParams(limit.GCRA, 10, time.Second, 5)
	if err != nil {
		t.Fatal(err)
	}
	search, err := limit.NewParams(limit.GCRA, 10, time.Second, 2)
	if err != nil {
		t.Fatal(err)
	}
	return &fakeResolver{api: api, search: search}
}

func (f *fakeResolver) Resolve(tenant, subject string, _ []policy.Descriptor, route, _ string) ([]limit.Req, *limit.Decision, error) {
	if tenant != "acme" {
		return nil, nil, limit.ErrUnknownTenant
	}
	switch subject {
	case "vip":
		return nil, &limit.Decision{Allowed: true, Policy: "allow", Backend: limit.BackendLocal}, nil
	case "banned":
		return nil, &limit.Decision{Allowed: false, Policy: "deny", Backend: limit.BackendLocal}, nil
	}
	reqs := []limit.Req{{Key: limit.Key(tenant, subject, "api"), Params: f.api, Cost: 1, Policy: "api"}}
	if route == "/v1/search" {
		reqs = append(reqs, limit.Req{Key: limit.Key(tenant, subject, "search"), Params: f.search, Cost: 1, Policy: "search"})
	}
	return reqs, nil, nil
}

// gcraStore is a tiny reference GCRA store (the docs/PLAN.md Lua arithmetic
// in Go, integer microseconds) so the Embedded path can be exercised while
// internal/store/local is another workstream's stub. Dry-run then commit,
// all-or-nothing, a denial never mutates state.
type gcraStore struct {
	mu   sync.Mutex
	tat  map[string]int64
	fail error
}

func (s *gcraStore) Apply(_ context.Context, reqs []limit.Req, now time.Time) ([]limit.Decision, error) {
	if s.fail != nil {
		return nil, s.fail
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.tat == nil {
		s.tat = make(map[string]int64)
	}
	nowUS := now.UnixMicro()
	out := make([]limit.Decision, len(reqs))
	newTAT := make([]int64, len(reqs))
	allowed := true
	for i, r := range reqs {
		emission := r.Params.EmissionUS
		dvt := r.Params.Burst * emission
		tat := max(s.tat[r.Key], nowUS)
		nt := tat + int64(r.Cost)*emission
		diff := nowUS - (nt - dvt)
		d := limit.Decision{Limit: int(r.Params.Burst), Policy: r.Policy, Backend: limit.BackendLocal}
		if diff < 0 {
			allowed = false
			d.Remaining = int((dvt - (tat - nowUS)) / emission)
			d.RetryAfter = time.Duration(-diff) * time.Microsecond
			d.ResetAfter = time.Duration(tat-nowUS) * time.Microsecond
		} else {
			d.Allowed = true
			d.Remaining = int((dvt - (nt - nowUS)) / emission)
			d.ResetAfter = time.Duration(nt-nowUS) * time.Microsecond
			newTAT[i] = nt
		}
		out[i] = d
	}
	if allowed {
		for i, r := range reqs {
			s.tat[r.Key] = newTAT[i]
		}
	}
	return out, nil
}

func (s *gcraStore) Peek(ctx context.Context, reqs []limit.Req, now time.Time) ([]limit.Decision, error) {
	return s.Apply(ctx, reqs, now)
}

func (s *gcraStore) Reset(context.Context, []string) error { return nil }
func (s *gcraStore) Name() string                          { return limit.BackendLocal }

func newTestEmbedded(t *testing.T, store limit.Store, clock func() time.Time) *embedded {
	t.Helper()
	ran := make(chan struct{})
	run := func(ctx context.Context) error {
		<-ctx.Done()
		close(ran)
		return ctx.Err()
	}
	e := newEmbedded(newFakeResolver(t), store, run, newOptions([]Option{WithClock(clock)}))
	t.Cleanup(func() {
		if err := e.Close(); err != nil {
			t.Error(err)
		}
		select {
		case <-ran:
		case <-time.After(time.Second):
			t.Error("Close did not stop the background goroutine")
		}
	})
	return e
}

func fixedClock(at time.Time) func() time.Time { return func() time.Time { return at } }

func TestEmbeddedDeniesAtBurstPlusOne(t *testing.T) {
	t.Parallel()
	const burst = 5
	now := time.Unix(1_700_000_000, 0)
	e := newTestEmbedded(t, &gcraStore{}, fixedClock(now))
	ctx := context.Background()
	req := Request{Tenant: "acme", Subject: "k1"}
	for i := 1; i <= burst; i++ {
		res, err := e.Check(ctx, req)
		if err != nil {
			t.Fatalf("call %d: %v", i, err)
		}
		if !res.Allowed {
			t.Fatalf("call %d: denied, want allowed (burst %d)", i, burst)
		}
		if res.Remaining != burst-i || res.Limit != burst || res.Policy != "api" {
			t.Fatalf("call %d: %+v", i, res)
		}
	}
	res, err := e.Check(ctx, req)
	if err != nil {
		t.Fatalf("call %d: %v (a denial must not be an error)", burst+1, err)
	}
	if res.Allowed {
		t.Fatalf("call %d: allowed, want denied", burst+1)
	}
	if res.Remaining != 0 || res.RetryAfter != 100*time.Millisecond || res.Limit != burst || res.Backend != limit.BackendLocal {
		t.Fatalf("denial = %+v", res)
	}
	// Another subject is untouched by k1's state.
	res, err = e.Check(ctx, Request{Tenant: "acme", Subject: "k2"})
	if err != nil || !res.Allowed || res.Remaining != burst-1 {
		t.Fatalf("k2: %+v %v", res, err)
	}
	// Advancing the clock by one emission interval admits exactly one more.
	e.clock = fixedClock(now.Add(100 * time.Millisecond))
	if res, err = e.Check(ctx, req); err != nil || !res.Allowed || res.Remaining != 0 {
		t.Fatalf("after 100ms: %+v %v", res, err)
	}
	if res, err = e.Check(ctx, req); err != nil || res.Allowed {
		t.Fatalf("after 100ms, second call: %+v %v", res, err)
	}
}

func TestEmbeddedPaths(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_700_000_000, 0)
	t.Run("strictest of several policies, all-or-nothing", func(t *testing.T) {
		t.Parallel()
		store := &gcraStore{}
		e := newTestEmbedded(t, store, fixedClock(now))
		req := Request{Tenant: "acme", Subject: "k1", Route: "/v1/search"}
		for i := 1; i <= 2; i++ {
			res, err := e.Check(context.Background(), req)
			if err != nil || !res.Allowed || res.Policy != "search" || res.Limit != 2 {
				t.Fatalf("call %d: %+v %v", i, res, err)
			}
		}
		res, err := e.Check(context.Background(), req)
		if err != nil || res.Allowed || res.Policy != "search" {
			t.Fatalf("call 3: %+v %v", res, err)
		}
		// The denial committed nothing: "api" still has burst-2 left.
		res, err = e.Check(context.Background(), Request{Tenant: "acme", Subject: "k1"})
		if err != nil || !res.Allowed || res.Remaining != 2 {
			t.Fatalf("api after search denial: %+v %v (want remaining 2)", res, err)
		}
	})
	t.Run("cost multiplies", func(t *testing.T) {
		t.Parallel()
		e := newTestEmbedded(t, &gcraStore{}, fixedClock(now))
		res, err := e.Check(context.Background(), Request{Tenant: "acme", Subject: "k1", Cost: 5})
		if err != nil || !res.Allowed || res.Remaining != 0 {
			t.Fatalf("cost 5: %+v %v", res, err)
		}
		res, err = e.Check(context.Background(), Request{Tenant: "acme", Subject: "k1"})
		if err != nil || res.Allowed {
			t.Fatalf("after cost 5: %+v %v", res, err)
		}
	})
	t.Run("short-circuit allow and deny lists", func(t *testing.T) {
		t.Parallel()
		e := newTestEmbedded(t, &gcraStore{}, fixedClock(now))
		res, err := e.Check(context.Background(), Request{Tenant: "acme", Subject: "vip"})
		if err != nil || !res.Allowed || res.Policy != "allow" {
			t.Fatalf("vip: %+v %v", res, err)
		}
		res, err = e.Check(context.Background(), Request{Tenant: "acme", Subject: "banned"})
		if err != nil || res.Allowed || res.Policy != "deny" {
			t.Fatalf("banned: %+v %v", res, err)
		}
	})
	t.Run("unknown tenant is a wrapped sentinel", func(t *testing.T) {
		t.Parallel()
		e := newTestEmbedded(t, &gcraStore{}, fixedClock(now))
		_, err := e.Check(context.Background(), Request{Tenant: "nope", Subject: "k1"})
		if !errors.Is(err, limit.ErrUnknownTenant) {
			t.Fatalf("err = %v, want ErrUnknownTenant", err)
		}
	})
	t.Run("store error is wrapped, never ErrDenied", func(t *testing.T) {
		t.Parallel()
		e := newTestEmbedded(t, &gcraStore{fail: limit.ErrBackendDown}, fixedClock(now))
		_, err := e.Check(context.Background(), Request{Tenant: "acme", Subject: "k1"})
		if !errors.Is(err, limit.ErrBackendDown) || errors.Is(err, ErrDenied) {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("closed client refuses", func(t *testing.T) {
		t.Parallel()
		e := newEmbedded(newFakeResolver(t), &gcraStore{}, nil, newOptions(nil))
		if err := e.Close(); err != nil {
			t.Fatal(err)
		}
		if err := e.Close(); err != nil {
			t.Fatal("second Close:", err)
		}
		if _, err := e.Check(context.Background(), Request{Tenant: "acme", Subject: "k1"}); err == nil {
			t.Fatal("Check after Close succeeded")
		}
	})
}

// TestEmbeddedRealStack drives the public constructor over the real
// policy.Registry and store/local. Those belong to workstreams A and B; while
// either is still a stub the test skips instead of failing, and it becomes a
// real burst+1 check the moment they land.
func TestEmbeddedRealStack(t *testing.T) {
	t.Parallel()
	set, err := policy.Parse(strings.NewReader(`
version: 1
tenants:
  acme:
    policies:
      - { name: api, algorithm: gcra, rate: 10, per: 1s, burst: 5, key: "{tenant}:{subject}" }
`))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(1_700_000_000, 0)
	c, err := Embedded(set, WithClock(fixedClock(now)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	req := Request{Tenant: "acme", Subject: "k1"}
	for i := 1; i <= 6; i++ {
		res, err := c.Check(context.Background(), req)
		if err != nil {
			if strings.Contains(err.Error(), "stub:") {
				t.Skipf("sibling package still a stub: %v", err)
			}
			t.Fatalf("call %d: %v", i, err)
		}
		if want := i <= 5; res.Allowed != want {
			t.Fatalf("call %d: allowed=%v want %v (%+v)", i, res.Allowed, want, res)
		}
	}
}

func TestEmbeddedRejectsInvalidSet(t *testing.T) {
	t.Parallel()
	set := policy.Set{SchemaVersion: 1, Tenants: map[string]policy.Tenant{
		"acme": {ID: "acme", Policies: []policy.Policy{{Name: "api", Rate: 0, Per: time.Second, Burst: 1, Key: "{subject}"}}},
	}}
	if _, err := Embedded(set); !errors.Is(err, limit.ErrInvalidParams) {
		t.Fatalf("err = %v, want ErrInvalidParams", err)
	}
}
