package server_test

import (
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	ratelimitv1 "github.com/crowbett/ratelimitd/gen/ratelimit/v1"
	"github.com/crowbett/ratelimitd/internal/admin"
	"github.com/crowbett/ratelimitd/internal/interceptor"
	"github.com/crowbett/ratelimitd/internal/limit"
	"github.com/crowbett/ratelimitd/internal/server"
)

func descriptors(n int) []*ratelimitv1.Descriptor {
	out := make([]*ratelimitv1.Descriptor, n)
	for i := range out {
		out[i] = &ratelimitv1.Descriptor{Key: "k" + string(rune('a'+i)), Value: "v"}
	}
	return out
}

func TestCheck(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		req      *ratelimitv1.CheckRequest
		setup    func(e *env)
		wantCode codes.Code
		wantResp *ratelimitv1.CheckResponse // Code/Policy/Backend/Degraded/Limit/Remaining compared
		wantMsg  string
		wantRec  *admin.DecisionKey // RecordDecision label set, nil = none
		wantCost []int              // costs of the reqs the store saw
	}{
		{
			name: "allowed; strictest is the smallest remaining (api over global)",
			req:  &ratelimitv1.CheckRequest{Tenant: "acme", Subject: "k1", Route: "/v1/users"},
			wantResp: &ratelimitv1.CheckResponse{
				Code: ratelimitv1.Code_CODE_OK, Policy: "api", Backend: "redis", Limit: 10, Remaining: 9,
			},
			wantRec:  &admin.DecisionKey{Tenant: "acme", Policy: "api", Allowed: true, Backend: "redis"},
			wantCost: []int{1, 1},
		},
		{
			name:  "denied is CODE_OVER_LIMIT with OK status, strictest is the denying policy",
			req:   &ratelimitv1.CheckRequest{Tenant: "acme", Subject: "k1", Route: "/v1/search/bulk/x"},
			setup: func(e *env) { e.store.denyPolicies["search"] = true },
			wantResp: &ratelimitv1.CheckResponse{
				Code: ratelimitv1.Code_CODE_OVER_LIMIT, Policy: "search", Backend: "redis", Limit: 5, Remaining: 0,
			},
			wantRec:  &admin.DecisionKey{Tenant: "acme", Policy: "search", Allowed: false, Backend: "redis"},
			wantCost: []int{1, 10, 1}, // bulk route costs 10 on the search policy
		},
		{
			name:     "request cost multiplies the route cost",
			req:      &ratelimitv1.CheckRequest{Tenant: "acme", Subject: "k1", Route: "/v1/search/bulk/x", Cost: 3},
			wantResp: &ratelimitv1.CheckResponse{Code: ratelimitv1.Code_CODE_OK, Policy: "search", Backend: "redis", Limit: 5, Remaining: 0},
			wantRec:  &admin.DecisionKey{Tenant: "acme", Policy: "search", Allowed: true, Backend: "redis"},
			wantCost: []int{3, 30, 3},
		},
		{
			name:     "degraded backend flags the response",
			req:      &ratelimitv1.CheckRequest{Tenant: "acme", Subject: "k1"},
			setup:    func(e *env) { e.store.backend = limit.BackendDegraded },
			wantResp: &ratelimitv1.CheckResponse{Code: ratelimitv1.Code_CODE_OK, Policy: "api", Backend: "degraded", Degraded: true, Limit: 10, Remaining: 9},
			wantRec:  &admin.DecisionKey{Tenant: "acme", Policy: "api", Allowed: true, Backend: "degraded"},
			wantCost: []int{1, 1},
		},
		{
			name:     "deny list short-circuits without touching the store",
			req:      &ratelimitv1.CheckRequest{Tenant: "acme", Subject: "banned"},
			wantResp: &ratelimitv1.CheckResponse{Code: ratelimitv1.Code_CODE_OVER_LIMIT, Policy: "deny"},
			wantRec:  &admin.DecisionKey{Tenant: "acme", Policy: "deny", Allowed: false, Backend: "none"},
		},
		{
			name:     "allow list short-circuits",
			req:      &ratelimitv1.CheckRequest{Tenant: "acme", Subject: "vip"},
			wantResp: &ratelimitv1.CheckResponse{Code: ratelimitv1.Code_CODE_OK, Policy: "allow", Limit: 1, Remaining: 1},
			wantRec:  &admin.DecisionKey{Tenant: "acme", Policy: "allow", Allowed: true, Backend: "none"},
		},
		{name: "empty tenant", req: &ratelimitv1.CheckRequest{Subject: "k1"}, wantCode: codes.InvalidArgument, wantMsg: "tenant: required"},
		{name: "empty subject", req: &ratelimitv1.CheckRequest{Tenant: "acme"}, wantCode: codes.InvalidArgument, wantMsg: "subject: required"},
		{
			name: "subject too long", req: &ratelimitv1.CheckRequest{Tenant: "acme", Subject: strings.Repeat("x", 257)},
			wantCode: codes.InvalidArgument, wantMsg: "subject: at most 256 bytes",
		},
		{
			name: "non-printable tenant", req: &ratelimitv1.CheckRequest{Tenant: "ac\x01me", Subject: "k1"},
			wantCode: codes.InvalidArgument, wantMsg: "tenant: byte 2 is not printable ASCII",
		},
		{
			name: "unicode subject", req: &ratelimitv1.CheckRequest{Tenant: "acme", Subject: "ké"},
			wantCode: codes.InvalidArgument, wantMsg: "subject: byte 1 is not printable ASCII",
		},
		{
			name: "too many descriptors", req: &ratelimitv1.CheckRequest{Tenant: "acme", Subject: "k1", Descriptors: descriptors(17)},
			wantCode: codes.InvalidArgument, wantMsg: "descriptors: at most 16, got 17",
		},
		{
			name: "descriptor key required", req: &ratelimitv1.CheckRequest{Tenant: "acme", Subject: "k1", Descriptors: []*ratelimitv1.Descriptor{{Value: "v"}}},
			wantCode: codes.InvalidArgument, wantMsg: "descriptors[0].key: required",
		},
		{
			name: "cost too high", req: &ratelimitv1.CheckRequest{Tenant: "acme", Subject: "k1", Cost: 10001},
			wantCode: codes.InvalidArgument, wantMsg: "cost: must be 1..10000",
		},
		{
			name: "every problem reported at once", req: &ratelimitv1.CheckRequest{Cost: 1 << 30},
			wantCode: codes.InvalidArgument, wantMsg: "tenant: required; subject: required; cost: must be 1..10000, got 1073741824",
		},
		{name: "unknown tenant", req: &ratelimitv1.CheckRequest{Tenant: "nobody", Subject: "k1"}, wantCode: codes.NotFound, wantMsg: "unknown tenant"},
		{name: "no policy matched", req: &ratelimitv1.CheckRequest{Tenant: "empty", Subject: "k1"}, wantCode: codes.FailedPrecondition, wantMsg: "no policy matched"},
		{
			name: "backend down (fail_closed reports it)", req: &ratelimitv1.CheckRequest{Tenant: "acme", Subject: "k1"},
			setup: func(e *env) { e.store.err = limit.ErrBackendDown }, wantCode: codes.Unavailable, wantMsg: "backend unavailable",
		},
		{
			name: "other store error is Internal without the message", req: &ratelimitv1.CheckRequest{Tenant: "acme", Subject: "k1"},
			setup: func(e *env) { e.store.err = errBoom }, wantCode: codes.Internal, wantMsg: "store redis: internal error",
		},
		{
			name: "store panic is recovered by the interceptor chain", req: &ratelimitv1.CheckRequest{Tenant: "acme", Subject: "k1"},
			setup: func(e *env) { e.store.panics = true }, wantCode: codes.Internal, wantMsg: "internal error",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			e := newEnv(t, server.Config{})
			if tt.setup != nil {
				tt.setup(e)
			}
			var header metadata.MD
			ctx := metadata.AppendToOutgoingContext(t.Context(), interceptor.RequestIDHeader, "req-"+tt.name)
			resp, err := e.check.Check(ctx, tt.req, grpc.Header(&header))
			if got := status.Code(err); got != tt.wantCode {
				t.Fatalf("code = %v (%v), want %v", got, err, tt.wantCode)
			}
			if tt.wantMsg != "" && !strings.Contains(status.Convert(err).Message(), tt.wantMsg) {
				t.Errorf("message = %q, want it to contain %q", status.Convert(err).Message(), tt.wantMsg)
			}
			if tt.wantCode == codes.Internal && strings.Contains(status.Convert(err).Message(), "boom") {
				t.Errorf("internal error leaked the cause: %v", err)
			}
			if got := header.Get(interceptor.RequestIDHeader); len(got) != 1 || got[0] != "req-"+tt.name {
				t.Errorf("x-request-id header = %v, want the caller's id", got)
			}
			if tt.wantResp != nil {
				if resp.GetCode() != tt.wantResp.GetCode() || resp.GetPolicy() != tt.wantResp.GetPolicy() ||
					resp.GetBackend() != tt.wantResp.GetBackend() || resp.GetDegraded() != tt.wantResp.GetDegraded() ||
					resp.GetLimit() != tt.wantResp.GetLimit() || resp.GetRemaining() != tt.wantResp.GetRemaining() {
					t.Errorf("resp = %v, want %v", resp, tt.wantResp)
				}
				if resp.GetCode() == ratelimitv1.Code_CODE_OVER_LIMIT && resp.GetRetryAfter().AsDuration() <= 0 {
					t.Errorf("denied without retry_after: %v", resp)
				}
			}
			ds := e.snap.Decisions()
			if tt.wantRec == nil {
				if len(ds) != 0 {
					t.Errorf("RecordDecision called on error: %+v", ds)
				}
			} else if len(ds) != 1 || ds[0].Key != *tt.wantRec || ds[0].N != 1 {
				t.Errorf("RecordDecision = %+v, want one %+v", ds, *tt.wantRec)
			}
			if tt.wantCost != nil {
				calls := e.store.applies()
				if len(calls) != 1 {
					t.Fatalf("store calls = %d, want 1", len(calls))
				}
				for i, r := range calls[0].reqs {
					if i < len(tt.wantCost) && r.Cost != tt.wantCost[i] {
						t.Errorf("req[%d] %s cost = %d, want %d", i, r.Policy, r.Cost, tt.wantCost[i])
					}
					if !strings.HasPrefix(r.Key, "rl:{t=acme|s=k1}:p=") {
						t.Errorf("req[%d] key = %q", i, r.Key)
					}
				}
				if len(calls[0].reqs) != len(tt.wantCost) {
					t.Errorf("reqs = %d, want %d", len(calls[0].reqs), len(tt.wantCost))
				}
			}
		})
	}
}

func TestCheckClientClock(t *testing.T) {
	t.Parallel()
	past := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, tt := range []struct {
		name  string
		trust bool
	}{{"ignored by default", false}, {"honoured when trusted", true}} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			e := newEnv(t, server.Config{TrustClientClock: tt.trust})
			before := time.Now()
			_, err := e.check.Check(t.Context(), &ratelimitv1.CheckRequest{Tenant: "acme", Subject: "k1", Now: timestamppb.New(past)})
			if err != nil {
				t.Fatal(err)
			}
			calls := e.store.applies()
			if len(calls) != 1 {
				t.Fatalf("calls = %d", len(calls))
			}
			got := calls[0].now
			if tt.trust && !got.Equal(past) {
				t.Errorf("now = %v, want the client's %v", got, past)
			}
			if !tt.trust && got.Before(before) {
				t.Errorf("now = %v, want the server clock (>= %v)", got, before)
			}
		})
	}
}

func TestBatchCheck(t *testing.T) {
	t.Parallel()
	check := func(tenant, subject, route string) *ratelimitv1.CheckRequest {
		return &ratelimitv1.CheckRequest{Tenant: tenant, Subject: subject, Route: route}
	}
	tests := []struct {
		name      string
		req       *ratelimitv1.BatchCheckRequest
		setup     func(e *env)
		wantCode  codes.Code
		wantMsg   string
		wantCodes []ratelimitv1.Code
		wantPols  []string
		wantCalls int // store.Apply invocations
	}{
		{
			name:      "positional results, one Apply per subject",
			req:       &ratelimitv1.BatchCheckRequest{Checks: []*ratelimitv1.CheckRequest{check("acme", "a", ""), check("acme", "b", "/v1/search"), check("acme", "banned", "")}},
			setup:     func(e *env) { e.store.denyPolicies["search"] = true },
			wantCodes: []ratelimitv1.Code{ratelimitv1.Code_CODE_OK, ratelimitv1.Code_CODE_OVER_LIMIT, ratelimitv1.Code_CODE_OVER_LIMIT},
			wantPols:  []string{"api", "search", "deny"},
			wantCalls: 2,
		},
		{
			name: "same subject twice is two Applies (keys would collide in one)",
			req: &ratelimitv1.BatchCheckRequest{Checks: []*ratelimitv1.CheckRequest{
				check("acme", "a", "/v1/search"), check("acme", "a", "/v1/other"), check("acme", "b", ""),
			}},
			setup:     func(e *env) { e.store.denyPolicies["search"] = true },
			wantCodes: []ratelimitv1.Code{ratelimitv1.Code_CODE_OVER_LIMIT, ratelimitv1.Code_CODE_OK, ratelimitv1.Code_CODE_OK},
			wantPols:  []string{"search", "api", "api"},
			wantCalls: 3,
		},
		{name: "empty batch", req: &ratelimitv1.BatchCheckRequest{}, wantCode: codes.InvalidArgument, wantMsg: "at least one"},
		{
			name: "too many", req: &ratelimitv1.BatchCheckRequest{Checks: make([]*ratelimitv1.CheckRequest, 65)},
			wantCode: codes.InvalidArgument, wantMsg: "at most 64, got 65",
		},
		{
			name:     "invalid check names its index",
			req:      &ratelimitv1.BatchCheckRequest{Checks: []*ratelimitv1.CheckRequest{check("acme", "a", ""), check("acme", "", "")}},
			wantCode: codes.InvalidArgument, wantMsg: "checks[1]: invalid request: subject: required",
		},
		{
			name:     "unknown tenant names its index",
			req:      &ratelimitv1.BatchCheckRequest{Checks: []*ratelimitv1.CheckRequest{check("acme", "a", ""), check("zzz", "a", "")}},
			wantCode: codes.NotFound, wantMsg: "checks[1]: unknown tenant",
		},
		{
			name:     "backend down",
			req:      &ratelimitv1.BatchCheckRequest{Checks: []*ratelimitv1.CheckRequest{check("acme", "a", "")}},
			setup:    func(e *env) { e.store.err = limit.ErrBackendDown },
			wantCode: codes.Unavailable, wantMsg: "checks[0]: backend unavailable",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			e := newEnv(t, server.Config{})
			if tt.setup != nil {
				tt.setup(e)
			}
			resp, err := e.check.BatchCheck(t.Context(), tt.req)
			if got := status.Code(err); got != tt.wantCode {
				t.Fatalf("code = %v (%v), want %v", got, err, tt.wantCode)
			}
			if tt.wantMsg != "" && !strings.Contains(status.Convert(err).Message(), tt.wantMsg) {
				t.Errorf("message = %q, want %q", status.Convert(err).Message(), tt.wantMsg)
			}
			if err != nil {
				if len(e.snap.Decisions()) != 0 {
					t.Error("RecordDecision called on a failed batch")
				}
				return
			}
			if len(resp.GetResults()) != len(tt.wantCodes) {
				t.Fatalf("results = %d, want %d", len(resp.GetResults()), len(tt.wantCodes))
			}
			for i, r := range resp.GetResults() {
				if r.GetCode() != tt.wantCodes[i] || r.GetPolicy() != tt.wantPols[i] {
					t.Errorf("results[%d] = %v/%s, want %v/%s", i, r.GetCode(), r.GetPolicy(), tt.wantCodes[i], tt.wantPols[i])
				}
			}
			if got := len(e.store.applies()); got != tt.wantCalls {
				t.Errorf("store.Apply calls = %d, want %d", got, tt.wantCalls)
			}
			var recorded uint64
			for _, d := range e.snap.Decisions() {
				recorded += d.N
			}
			if recorded != uint64(len(tt.wantCodes)) {
				t.Errorf("RecordDecision count = %d, want %d", recorded, len(tt.wantCodes))
			}
		})
	}
}

func TestDecisionToProto(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		in   limit.Decision
		want *ratelimitv1.CheckResponse
	}{
		{
			name: "allowed",
			in:   limit.Decision{Allowed: true, Remaining: 3, Limit: 10, ResetAfter: 700 * time.Millisecond, Policy: "api", Backend: "redis"},
			want: &ratelimitv1.CheckResponse{Code: ratelimitv1.Code_CODE_OK, Limit: 10, Remaining: 3, Policy: "api", Backend: "redis"},
		},
		{
			name: "denied degraded, negatives clamp",
			in:   limit.Decision{Allowed: false, Remaining: -1, Limit: 1 << 40, RetryAfter: -time.Second, Policy: "p", Backend: "degraded"},
			want: &ratelimitv1.CheckResponse{Code: ratelimitv1.Code_CODE_OVER_LIMIT, Limit: 1<<32 - 1, Remaining: 0, Policy: "p", Backend: "degraded", Degraded: true},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := server.DecisionToProto(tt.in)
			if got.GetCode() != tt.want.GetCode() || got.GetLimit() != tt.want.GetLimit() || got.GetRemaining() != tt.want.GetRemaining() ||
				got.GetPolicy() != tt.want.GetPolicy() || got.GetBackend() != tt.want.GetBackend() || got.GetDegraded() != tt.want.GetDegraded() {
				t.Errorf("got %v, want %v", got, tt.want)
			}
			if got.GetRetryAfter().AsDuration() != max(tt.in.RetryAfter, 0) || got.GetResetAfter().AsDuration() != max(tt.in.ResetAfter, 0) {
				t.Errorf("durations = %v/%v", got.GetRetryAfter().AsDuration(), got.GetResetAfter().AsDuration())
			}
		})
	}
}
