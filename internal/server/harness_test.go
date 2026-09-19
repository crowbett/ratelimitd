package server_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	ratelimitv1 "github.com/crowbett/ratelimitd/gen/ratelimit/v1"
	"github.com/crowbett/ratelimitd/internal/admin"
	"github.com/crowbett/ratelimitd/internal/interceptor"
	"github.com/crowbett/ratelimitd/internal/limit"
	"github.com/crowbett/ratelimitd/internal/policy"
	"github.com/crowbett/ratelimitd/internal/server"
)

// testSet is the small policy set every test resolves against: tenant acme
// with two policies (api on {subject}, search on /v1/search* at cost 10 for
// bulk) and a deny list, plus one global policy.
func testSet(t *testing.T) policy.Set {
	t.Helper()
	set := policy.Set{
		SchemaVersion: 1,
		Defaults:      policy.Defaults{Algorithm: "gcra", Cost: 1},
		Tenants: map[string]policy.Tenant{
			"acme": {
				ID:      "acme",
				Default: "api",
				Deny:    []string{"banned"},
				Allow:   []string{"vip"},
				Policies: []policy.Policy{
					{Name: "api", Algorithm: "gcra", Rate: 100, Per: time.Second, Burst: 10, Key: "{tenant}:{subject}"},
					{
						Name: "search", Algorithm: "token_bucket", Rate: 10, Per: time.Second, Burst: 5, Key: "{tenant}:{subject}:{route}",
						Match: policy.Match{Routes: []string{"/v1/search*"}}, Cost: map[string]int{"/v1/search/bulk*": 10},
					},
				},
			},
			"empty": {ID: "empty"},
		},
		Global:   []policy.Policy{{Name: "global_ingress", Algorithm: "gcra", Rate: 1000, Per: time.Second, Burst: 100, Key: "global:{subject}"}},
		Version:  "deadbeefcafef00d",
		LoadedAt: time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC),
	}
	if err := set.Validate(); err != nil {
		t.Fatalf("test set invalid: %v", err)
	}
	return set
}

// fakeResolver mimics policy.Registry.Resolve against a Set: deny/allow lists
// short-circuit on the subject, route globs select "search", and every
// matching policy renders limit.Key with its Params and route cost.
type fakeResolver struct {
	set policy.Set
	err error // when set, returned as-is
}

func (r *fakeResolver) Resolve(tenant, subject string, _ []policy.Descriptor, route, _ string) ([]limit.Req, *limit.Decision, error) {
	if r.err != nil {
		return nil, nil, r.err
	}
	t, ok := r.set.Tenants[tenant]
	if !ok {
		return nil, nil, fmt.Errorf("%w: %s", limit.ErrUnknownTenant, tenant)
	}
	for _, d := range t.Deny {
		if d == subject {
			return nil, &limit.Decision{Allowed: false, Policy: "deny", RetryAfter: time.Hour}, nil
		}
	}
	for _, a := range t.Allow {
		if a == subject {
			return nil, &limit.Decision{Allowed: true, Policy: "allow", Remaining: 1, Limit: 1}, nil
		}
	}
	var reqs []limit.Req
	candidates := append([]policy.Policy(nil), t.Policies...)
	if len(t.Policies) > 0 { // the fake exempts a policy-less tenant from global so ErrNoPolicy is reachable
		candidates = append(candidates, r.set.Global...)
	}
	for i := range candidates {
		p := &candidates[i]
		if len(p.Match.Routes) > 0 && !matchGlob(p.Match.Routes, route) {
			continue
		}
		params, err := p.Params(r.set.Defaults.Algorithm)
		if err != nil {
			return nil, nil, err
		}
		cost := 1
		for glob, c := range p.Cost {
			if matchGlob([]string{glob}, route) {
				cost = c
			}
		}
		reqs = append(reqs, limit.Req{Key: limit.Key(tenant, subject, p.Name), Params: params, Cost: cost, Policy: p.Name})
	}
	if len(reqs) == 0 {
		return nil, nil, fmt.Errorf("%w: tenant %s route %q", limit.ErrNoPolicy, tenant, route)
	}
	return reqs, nil, nil
}

func matchGlob(globs []string, s string) bool {
	for _, g := range globs {
		if strings.HasSuffix(g, "*") && strings.HasPrefix(s, strings.TrimSuffix(g, "*")) || g == s {
			return true
		}
	}
	return false
}

// applyCall records one Store.Apply invocation.
type applyCall struct {
	reqs []limit.Req
	now  time.Time
}

// fakeStore answers Apply from a per-key script (deny when the key contains
// "deny" or the policy is in denyPolicies), records every call, and can be
// told to fail or panic.
type fakeStore struct {
	mu           sync.Mutex
	calls        []applyCall
	resets       [][]string
	err          error
	panics       bool
	denyPolicies map[string]bool
	backend      string
}

func newFakeStore() *fakeStore {
	return &fakeStore{backend: limit.BackendRedis, denyPolicies: map[string]bool{}}
}

func (f *fakeStore) Apply(_ context.Context, reqs []limit.Req, now time.Time) ([]limit.Decision, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.panics {
		panic("fake store exploded")
	}
	if f.err != nil {
		return nil, f.err
	}
	f.calls = append(f.calls, applyCall{reqs: append([]limit.Req(nil), reqs...), now: now})
	out := make([]limit.Decision, len(reqs))
	for i, r := range reqs {
		d := limit.Decision{
			Allowed:    true,
			Limit:      int(r.Params.Burst),
			Remaining:  int(r.Params.Burst) - r.Cost,
			ResetAfter: time.Duration(r.Cost*int(r.Params.EmissionUS)) * time.Microsecond,
			Policy:     r.Policy,
			Backend:    f.backend,
		}
		if f.denyPolicies[r.Policy] || strings.Contains(r.Key, "deny") {
			d.Allowed, d.Remaining, d.RetryAfter = false, 0, 250*time.Millisecond
		}
		out[i] = d
	}
	return out, nil
}

func (f *fakeStore) Peek(ctx context.Context, reqs []limit.Req, now time.Time) ([]limit.Decision, error) {
	return f.Apply(ctx, reqs, now)
}

func (f *fakeStore) Reset(_ context.Context, keys []string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	f.resets = append(f.resets, append([]string(nil), keys...))
	return nil
}

func (f *fakeStore) Name() string { return f.backend }

func (f *fakeStore) applies() []applyCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]applyCall(nil), f.calls...)
}

// fakeRegistry implements server.PolicyRegistry over a Set, applying
// Upsert/Delete in memory and bumping the version.
type fakeRegistry struct {
	mu  sync.Mutex
	set policy.Set
	err error
	n   int
}

func (r *fakeRegistry) Snapshot() *policy.Set {
	r.mu.Lock()
	defer r.mu.Unlock()
	s := r.set
	return &s
}

func (r *fakeRegistry) Upsert(_ context.Context, tenant string, p policy.Policy) (*policy.Set, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err != nil {
		return nil, r.err
	}
	next := r.set
	if tenant == "" {
		next.Global = replaceOrAppend(next.Global, p)
	} else {
		next.Tenants = copyTenants(next.Tenants)
		t := next.Tenants[tenant]
		t.ID = tenant
		t.Policies = replaceOrAppend(t.Policies, p)
		next.Tenants[tenant] = t
	}
	r.n++
	next.Version = fmt.Sprintf("v%d", r.n)
	r.set = next
	s := next
	return &s, nil
}

func (r *fakeRegistry) Delete(_ context.Context, tenant, name string) (*policy.Set, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err != nil {
		return nil, r.err
	}
	next := r.set
	drop := func(ps []policy.Policy) []policy.Policy {
		out := ps[:0:0]
		for i := range ps {
			if ps[i].Name != name {
				out = append(out, ps[i])
			}
		}
		return out
	}
	if tenant == "" {
		next.Global = drop(next.Global)
	} else {
		next.Tenants = copyTenants(next.Tenants)
		t := next.Tenants[tenant]
		t.Policies = drop(t.Policies)
		next.Tenants[tenant] = t
	}
	r.n++
	next.Version = fmt.Sprintf("v%d", r.n)
	r.set = next
	s := next
	return &s, nil
}

func replaceOrAppend(ps []policy.Policy, p policy.Policy) []policy.Policy {
	out := make([]policy.Policy, 0, len(ps)+1)
	found := false
	for i := range ps {
		if ps[i].Name == p.Name {
			out = append(out, p)
			found = true
		} else {
			out = append(out, ps[i])
		}
	}
	if !found {
		out = append(out, p)
	}
	return out
}

func copyTenants(in map[string]policy.Tenant) map[string]policy.Tenant {
	out := make(map[string]policy.Tenant, len(in)+1)
	for k, v := range in {
		out[k] = v
	}
	return out
}

// env is one bufconn server with the production interceptor chain and both
// services registered, plus clients for each.
type env struct {
	store *fakeStore
	reg   *fakeRegistry
	snap  *admin.Snapshot
	check ratelimitv1.RateLimitServiceClient
	admin ratelimitv1.AdminServiceClient
}

func newEnv(t *testing.T, cfg server.Config) *env {
	t.Helper()
	set := testSet(t)
	e := &env{store: newFakeStore(), reg: &fakeRegistry{set: set}, snap: admin.NewSnapshot()}
	logger := slog.New(slog.DiscardHandler)
	srv := grpc.NewServer(grpc.ChainUnaryInterceptor(
		interceptor.UnaryRecover(logger),
		interceptor.UnaryRequestID(),
		interceptor.UnaryLogging(logger),
	))
	server.NewCheckServer(e.store, &fakeResolver{set: set}, e.snap, logger, cfg).Register(srv)
	as := server.NewAdminServer(e.reg, e.store, e.snap, logger)
	as.SetBuildInfo("test-version", time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC))
	as.Register(srv)

	lis := bufconn.Listen(1 << 20)
	go func() { _ = srv.Serve(lis) }()
	conn, err := grpc.NewClient("passthrough:///bufconn",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = conn.Close()
		srv.Stop()
		_ = lis.Close()
	})
	e.check = ratelimitv1.NewRateLimitServiceClient(conn)
	e.admin = ratelimitv1.NewAdminServiceClient(conn)
	return e
}

var errBoom = errors.New("boom")
