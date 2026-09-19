package server_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	ratelimitv1 "github.com/crowbett/ratelimitd/gen/ratelimit/v1"
	"github.com/crowbett/ratelimitd/internal/limit"
	"github.com/crowbett/ratelimitd/internal/policy"
	"github.com/crowbett/ratelimitd/internal/server"
)

func TestGetPolicy(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		req      *ratelimitv1.GetPolicyRequest
		wantCode codes.Code
		wantAlgo ratelimitv1.Algorithm
		wantCost map[string]uint32
	}{
		{name: "tenant policy", req: &ratelimitv1.GetPolicyRequest{Tenant: "acme", Name: "search"}, wantAlgo: ratelimitv1.Algorithm_ALGORITHM_TOKEN_BUCKET, wantCost: map[string]uint32{"/v1/search/bulk*": 10}},
		{name: "global policy", req: &ratelimitv1.GetPolicyRequest{Name: "global_ingress"}, wantAlgo: ratelimitv1.Algorithm_ALGORITHM_GCRA},
		{name: "missing policy", req: &ratelimitv1.GetPolicyRequest{Tenant: "acme", Name: "nope"}, wantCode: codes.NotFound},
		{name: "unknown tenant", req: &ratelimitv1.GetPolicyRequest{Tenant: "zzz", Name: "api"}, wantCode: codes.NotFound},
		{name: "bad tenant name", req: &ratelimitv1.GetPolicyRequest{Tenant: "Bad Name", Name: "api"}, wantCode: codes.InvalidArgument},
		{name: "bad policy name", req: &ratelimitv1.GetPolicyRequest{Tenant: "acme", Name: "x{y}"}, wantCode: codes.InvalidArgument},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			e := newEnv(t, server.Config{})
			resp, err := e.admin.GetPolicy(t.Context(), tt.req)
			if got := status.Code(err); got != tt.wantCode {
				t.Fatalf("code = %v (%v), want %v", got, err, tt.wantCode)
			}
			if err != nil {
				return
			}
			p := resp.GetPolicy()
			if p.GetName() != tt.req.GetName() || p.GetAlgorithm() != tt.wantAlgo || !p.GetEnabled() || p.GetPer().AsDuration() != time.Second {
				t.Errorf("policy = %v", p)
			}
			if len(tt.wantCost) != len(p.GetCost()) {
				t.Errorf("cost = %v, want %v", p.GetCost(), tt.wantCost)
			}
			for k, v := range tt.wantCost {
				if p.GetCost()[k] != v {
					t.Errorf("cost[%s] = %d, want %d", k, p.GetCost()[k], v)
				}
			}
		})
	}
}

func TestListPolicies(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		tenant      string
		wantCode    codes.Code
		wantTenants []string
		wantCounts  []int
	}{
		{name: "all: global first then tenants sorted", wantTenants: []string{"", "acme", "empty"}, wantCounts: []int{1, 2, 0}},
		{name: "one tenant", tenant: "acme", wantTenants: []string{"acme"}, wantCounts: []int{2}},
		{name: "unknown tenant", tenant: "zzz", wantCode: codes.NotFound},
		{name: "bad tenant", tenant: "A", wantCode: codes.InvalidArgument},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			e := newEnv(t, server.Config{})
			resp, err := e.admin.ListPolicies(t.Context(), &ratelimitv1.ListPoliciesRequest{Tenant: tt.tenant})
			if got := status.Code(err); got != tt.wantCode {
				t.Fatalf("code = %v (%v), want %v", got, err, tt.wantCode)
			}
			if err != nil {
				return
			}
			if resp.GetVersion() != "deadbeefcafef00d" {
				t.Errorf("version = %q", resp.GetVersion())
			}
			if len(resp.GetTenants()) != len(tt.wantTenants) {
				t.Fatalf("tenants = %v", resp.GetTenants())
			}
			for i, tp := range resp.GetTenants() {
				if tp.GetTenant() != tt.wantTenants[i] || len(tp.GetPolicies()) != tt.wantCounts[i] {
					t.Errorf("tenants[%d] = %s/%d, want %s/%d", i, tp.GetTenant(), len(tp.GetPolicies()), tt.wantTenants[i], tt.wantCounts[i])
				}
			}
		})
	}
}

func TestUpsertPolicy(t *testing.T) {
	t.Parallel()
	good := &ratelimitv1.Policy{
		Name: "uploads", Algorithm: ratelimitv1.Algorithm_ALGORITHM_SLIDING_WINDOW, Rate: 5, Per: durationpb.New(time.Minute), Burst: 5,
		Key: "{tenant}:{subject}", Match: &ratelimitv1.Match{Routes: []string{"/v1/upload*"}, Descriptors: []*ratelimitv1.Descriptor{{Key: "plan", Value: "free"}}},
		Cost: map[string]uint32{"/v1/upload/big*": 4}, Enabled: true,
	}
	tests := []struct {
		name     string
		req      *ratelimitv1.UpsertPolicyRequest
		setup    func(e *env)
		wantCode codes.Code
		wantMsg  string
		wantVer  string
		check    func(t *testing.T, set *policy.Set)
	}{
		{
			name: "insert into a tenant", req: &ratelimitv1.UpsertPolicyRequest{Tenant: "acme", Policy: good}, wantVer: "v1",
			check: func(t *testing.T, set *policy.Set) {
				t.Helper()
				ps := set.Tenants["acme"].Policies
				if len(ps) != 3 || ps[2].Name != "uploads" || ps[2].Algorithm != "sliding_window" || ps[2].Per != time.Minute ||
					ps[2].Cost["/v1/upload/big*"] != 4 || ps[2].Match.Descriptors["plan"] != "free" || !ps[2].IsEnabled() || ps[2].Enabled != nil {
					t.Errorf("policies = %+v", ps)
				}
			},
		},
		{
			name: "replace an existing tenant policy, disabled", wantVer: "v1",
			req: &ratelimitv1.UpsertPolicyRequest{Tenant: "acme", Policy: &ratelimitv1.Policy{Name: "api", Rate: 1, Per: durationpb.New(time.Second), Burst: 1, Key: "{tenant}:{subject}"}},
			check: func(t *testing.T, set *policy.Set) {
				t.Helper()
				ps := set.Tenants["acme"].Policies
				if len(ps) != 2 || ps[0].Name != "api" || ps[0].Rate != 1 || ps[0].Algorithm != "" || ps[0].IsEnabled() {
					t.Errorf("policies = %+v", ps)
				}
			},
		},
		{
			name: "insert global", req: &ratelimitv1.UpsertPolicyRequest{Policy: good}, wantVer: "v1",
			check: func(t *testing.T, set *policy.Set) {
				t.Helper()
				if len(set.Global) != 2 || set.Global[1].Name != "uploads" {
					t.Errorf("global = %+v", set.Global)
				}
			},
		},
		{
			name: "new tenant is created", req: &ratelimitv1.UpsertPolicyRequest{Tenant: "newco", Policy: good}, wantVer: "v1",
			check: func(t *testing.T, set *policy.Set) {
				t.Helper()
				if len(set.Tenants["newco"].Policies) != 1 {
					t.Errorf("tenants = %+v", set.Tenants)
				}
			},
		},
		{name: "nil policy", req: &ratelimitv1.UpsertPolicyRequest{Tenant: "acme"}, wantCode: codes.InvalidArgument, wantMsg: "policy is required"},
		{
			name: "bad name", req: &ratelimitv1.UpsertPolicyRequest{Tenant: "acme", Policy: &ratelimitv1.Policy{Name: "Bad/Name", Rate: 1, Per: durationpb.New(time.Second), Burst: 1, Key: "k"}},
			wantCode: codes.InvalidArgument, wantMsg: "must match",
		},
		{
			name: "invalid numbers are reported together", req: &ratelimitv1.UpsertPolicyRequest{Tenant: "acme", Policy: &ratelimitv1.Policy{Name: "bad", Rate: 0, Burst: 0, Key: ""}},
			wantCode: codes.InvalidArgument, wantMsg: "rate must be > 0",
		},
		{
			name: "unknown algorithm enum", req: &ratelimitv1.UpsertPolicyRequest{Tenant: "acme", Policy: &ratelimitv1.Policy{Name: "bad", Algorithm: 99, Rate: 1, Per: durationpb.New(time.Second), Burst: 1, Key: "k"}},
			wantCode: codes.InvalidArgument, wantMsg: "unknown algorithm 99",
		},
		{
			name: "registry failure is Internal", req: &ratelimitv1.UpsertPolicyRequest{Tenant: "acme", Policy: good},
			setup: func(e *env) { e.reg.err = errBoom }, wantCode: codes.Internal, wantMsg: "internal error",
		},
		{
			name: "registry ErrInvalidParams maps", req: &ratelimitv1.UpsertPolicyRequest{Tenant: "acme", Policy: good},
			setup: func(e *env) { e.reg.err = fmt.Errorf("template: %w", limit.ErrInvalidParams) }, wantCode: codes.InvalidArgument, wantMsg: "template",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			e := newEnv(t, server.Config{})
			if tt.setup != nil {
				tt.setup(e)
			}
			resp, err := e.admin.UpsertPolicy(t.Context(), tt.req)
			if got := status.Code(err); got != tt.wantCode {
				t.Fatalf("code = %v (%v), want %v", got, err, tt.wantCode)
			}
			if tt.wantMsg != "" && !strings.Contains(status.Convert(err).Message(), tt.wantMsg) {
				t.Errorf("message = %q, want %q", status.Convert(err).Message(), tt.wantMsg)
			}
			if err != nil {
				if e.reg.Snapshot().Version != "deadbeefcafef00d" {
					t.Error("registry mutated on a rejected upsert")
				}
				return
			}
			if resp.GetVersion() != tt.wantVer || resp.GetPolicy().GetName() != tt.req.GetPolicy().GetName() {
				t.Errorf("resp = %v", resp)
			}
			if e.snap.Counters().PolicyVersion != tt.wantVer {
				t.Errorf("snapshot policy version = %q, want %q", e.snap.Counters().PolicyVersion, tt.wantVer)
			}
			if tt.check != nil {
				tt.check(t, e.reg.Snapshot())
			}
		})
	}
}

func TestDeletePolicy(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		req      *ratelimitv1.DeletePolicyRequest
		wantCode codes.Code
		wantLeft int
	}{
		{name: "tenant policy", req: &ratelimitv1.DeletePolicyRequest{Tenant: "acme", Name: "search"}, wantLeft: 1},
		{name: "global policy", req: &ratelimitv1.DeletePolicyRequest{Name: "global_ingress"}, wantLeft: 0},
		{name: "tenant default cannot be deleted (validate)", req: &ratelimitv1.DeletePolicyRequest{Tenant: "acme", Name: "api"}, wantCode: codes.InvalidArgument},
		{name: "missing", req: &ratelimitv1.DeletePolicyRequest{Tenant: "acme", Name: "nope"}, wantCode: codes.NotFound},
		{name: "unknown tenant", req: &ratelimitv1.DeletePolicyRequest{Tenant: "zzz", Name: "api"}, wantCode: codes.NotFound},
		{name: "bad name", req: &ratelimitv1.DeletePolicyRequest{Tenant: "acme", Name: ""}, wantCode: codes.InvalidArgument},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			e := newEnv(t, server.Config{})
			resp, err := e.admin.DeletePolicy(t.Context(), tt.req)
			if got := status.Code(err); got != tt.wantCode {
				t.Fatalf("code = %v (%v), want %v", got, err, tt.wantCode)
			}
			if err != nil {
				return
			}
			if resp.GetVersion() != "v1" {
				t.Errorf("version = %q", resp.GetVersion())
			}
			set := e.reg.Snapshot()
			left := len(set.Global)
			if tt.req.GetTenant() != "" {
				left = len(set.Tenants[tt.req.GetTenant()].Policies)
			}
			if left != tt.wantLeft {
				t.Errorf("policies left = %d, want %d", left, tt.wantLeft)
			}
		})
	}
}

func TestResetKey(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		req      *ratelimitv1.ResetKeyRequest
		setup    func(e *env)
		wantCode codes.Code
		wantKeys []string
	}{
		{
			name: "every policy of the subject", req: &ratelimitv1.ResetKeyRequest{Tenant: "acme", Subject: "k1"},
			wantKeys: []string{"rl:{t=acme|s=k1}:p=api", "rl:{t=acme|s=k1}:p=search", "rl:{t=acme|s=k1}:p=global_ingress"},
		},
		{
			name: "one policy", req: &ratelimitv1.ResetKeyRequest{Tenant: "acme", Subject: "k1", Policy: "search"},
			wantKeys: []string{"rl:{t=acme|s=k1}:p=search"},
		},
		{name: "unknown policy", req: &ratelimitv1.ResetKeyRequest{Tenant: "acme", Subject: "k1", Policy: "nope"}, wantCode: codes.NotFound},
		{name: "bad policy name", req: &ratelimitv1.ResetKeyRequest{Tenant: "acme", Subject: "k1", Policy: "No"}, wantCode: codes.InvalidArgument},
		{name: "unknown tenant", req: &ratelimitv1.ResetKeyRequest{Tenant: "zzz", Subject: "k1"}, wantCode: codes.NotFound},
		{name: "missing subject", req: &ratelimitv1.ResetKeyRequest{Tenant: "acme"}, wantCode: codes.InvalidArgument},
		{
			name: "store failure", req: &ratelimitv1.ResetKeyRequest{Tenant: "acme", Subject: "k1"},
			setup: func(e *env) { e.store.err = limit.ErrBackendDown }, wantCode: codes.Unavailable,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			e := newEnv(t, server.Config{})
			if tt.setup != nil {
				tt.setup(e)
			}
			resp, err := e.admin.ResetKey(t.Context(), tt.req)
			if got := status.Code(err); got != tt.wantCode {
				t.Fatalf("code = %v (%v), want %v", got, err, tt.wantCode)
			}
			if err != nil {
				return
			}
			if int(resp.GetRemoved()) != len(tt.wantKeys) || len(e.store.resets) != 1 {
				t.Fatalf("removed = %d, resets = %v", resp.GetRemoved(), e.store.resets)
			}
			for i, k := range e.store.resets[0] {
				if k != tt.wantKeys[i] {
					t.Errorf("keys[%d] = %q, want %q", i, k, tt.wantKeys[i])
				}
			}
		})
	}
}

func TestStats(t *testing.T) {
	t.Parallel()
	e := newEnv(t, server.Config{})
	e.snap.RecordDecision("acme", "api", true, "redis", time.Millisecond)
	e.snap.RecordDecision("acme", "api", false, "redis", time.Millisecond)
	e.snap.RecordBackendError("redis", "timeout")
	e.snap.SetFallbackActive("redis", true)
	e.snap.SetClockSkew(30 * time.Millisecond)
	e.snap.SetLocalKeys(12)
	resp, err := e.admin.Stats(t.Context(), &ratelimitv1.StatsRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetVersion() != "test-version" || resp.GetStartedAt().AsTime().Year() != 2026 || resp.GetPolicyVersion() != "deadbeefcafef00d" ||
		resp.GetDecisionsAllowed() != 1 || resp.GetDecisionsDenied() != 1 || resp.GetBackendErrors() != 1 || !resp.GetFallbackActive() ||
		resp.GetClockSkew().AsDuration() != 30*time.Millisecond || resp.GetLocalKeys() != 12 || resp.GetFallbackTotal().AsDuration() < 0 {
		t.Errorf("stats = %v", resp)
	}
}

func TestToStatus(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		err  error
		want codes.Code
		msg  string
	}{
		{"nil", nil, codes.OK, ""},
		{"unknown tenant wrapped", fmt.Errorf("resolve: %w", limit.ErrUnknownTenant), codes.NotFound, "resolve: unknown tenant"},
		{"no policy", limit.ErrNoPolicy, codes.FailedPrecondition, "no policy matched"},
		{"invalid params", fmt.Errorf("x: %w", limit.ErrInvalidParams), codes.InvalidArgument, "invalid policy params"},
		{"validation", &server.ValidationError{Problems: []string{"a", "b"}}, codes.InvalidArgument, "invalid request: a; b"},
		{"backend down", limit.ErrBackendDown, codes.Unavailable, "backend unavailable"},
		{"canceled", context.Canceled, codes.Canceled, "context canceled"},
		{"deadline", fmt.Errorf("apply: %w", context.DeadlineExceeded), codes.DeadlineExceeded, "deadline"},
		{"status passes through", status.Error(codes.PermissionDenied, "nope"), codes.PermissionDenied, "nope"},
		{"unknown is internal and hidden", errors.New("secret sauce"), codes.Internal, "internal error"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := server.ToStatus(tt.err)
			if tt.err == nil {
				if got != nil {
					t.Fatalf("ToStatus(nil) = %v", got)
				}
				return
			}
			if status.Code(got) != tt.want {
				t.Errorf("code = %v, want %v", status.Code(got), tt.want)
			}
			if !strings.Contains(status.Convert(got).Message(), tt.msg) {
				t.Errorf("message = %q, want %q", status.Convert(got).Message(), tt.msg)
			}
			if tt.want == codes.Internal && strings.Contains(status.Convert(got).Message(), "secret") {
				t.Error("internal error leaked")
			}
		})
	}
}

func TestPolicyRoundTrip(t *testing.T) {
	t.Parallel()
	disabled := false
	in := policy.Policy{
		Name: "p", Algorithm: "sliding_window_exact", Rate: 7, Per: 2 * time.Second, Burst: 3, Key: "{ip}",
		Match:   policy.Match{Routes: []string{"/a*"}, Methods: []string{"GET"}, Descriptors: map[string]string{"b": "2", "a": "1"}},
		Cost:    map[string]int{"/a/b*": 5},
		Enabled: &disabled,
	}
	pb := server.PolicyToProto(in)
	if pb.GetAlgorithm() != ratelimitv1.Algorithm_ALGORITHM_SLIDING_WINDOW_EXACT || pb.GetEnabled() ||
		len(pb.GetMatch().GetDescriptors()) != 2 || pb.GetMatch().GetDescriptors()[0].GetKey() != "a" {
		t.Fatalf("PolicyToProto = %v", pb)
	}
	out, err := server.PolicyFromProto(pb)
	if err != nil {
		t.Fatal(err)
	}
	if out.Name != in.Name || out.Algorithm != in.Algorithm || out.Rate != in.Rate || out.Per != in.Per || out.Burst != in.Burst ||
		out.Key != in.Key || out.IsEnabled() || out.Cost["/a/b*"] != 5 || out.Match.Descriptors["b"] != "2" || out.Match.Methods[0] != "GET" {
		t.Errorf("round trip: %+v", out)
	}
	// Unknown YAML spelling renders as unspecified rather than failing.
	if got := server.PolicyToProto(policy.Policy{Name: "x", Algorithm: "nope"}).GetAlgorithm(); got != ratelimitv1.Algorithm_ALGORITHM_UNSPECIFIED {
		t.Errorf("unknown algorithm = %v", got)
	}
	if _, err := server.PolicyFromProto(&ratelimitv1.Policy{Name: "x", Match: &ratelimitv1.Match{Descriptors: []*ratelimitv1.Descriptor{{Value: "v"}}}}); err == nil {
		t.Error("descriptor without key accepted")
	}
}
