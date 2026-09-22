package main

import (
	"context"
	"log/slog"
	"net"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthv1 "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	ratelimitv1 "github.com/crowbett/ratelimitd/gen/ratelimit/v1"
	"github.com/crowbett/ratelimitd/internal/admin"
	"github.com/crowbett/ratelimitd/internal/policy"
	"github.com/crowbett/ratelimitd/internal/server"
	"github.com/crowbett/ratelimitd/internal/store/local"
)

const testToken = "0123456789abcdef0123456789abcdef"

const testPolicies = `version: 1
defaults: { algorithm: gcra, cost: 1 }
tenants:
  acme:
    default: api
    policies:
      - { name: api, algorithm: gcra, rate: 100, per: 1s, burst: 10, key: "{tenant}:{subject}" }
`

// planes is the production data plane and control plane, each on its own
// loopback TCP listener, built by the same constructors run() uses.
type planes struct {
	data, control *grpc.ClientConn
}

func startPlanes(t *testing.T) planes {
	t.Helper()
	set, err := policy.Parse(strings.NewReader(testPolicies))
	if err != nil {
		t.Fatalf("parse policies: %v", err)
	}
	logger := slog.New(slog.DiscardHandler)
	reg := policy.NewRegistry(set, logger)
	store := local.New(local.Config{Shards: 1})
	snap := admin.NewSnapshot()

	dataSrv := newDataPlane(logger, server.NewCheckServer(store, reg, snap, logger, server.Config{}), health.NewServer())
	controlSrv := newControlPlane(logger, testToken, server.NewAdminServer(reg, store, snap, logger), health.NewServer())

	serve := func(srv *grpc.Server) *grpc.ClientConn {
		t.Helper()
		lis, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen: %v", err)
		}
		go func() { _ = srv.Serve(lis) }()
		conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		t.Cleanup(func() {
			_ = conn.Close()
			srv.Stop()
		})
		return conn
	}
	return planes{data: serve(dataSrv), control: serve(controlSrv)}
}

// TestPlaneSeparation pins the listener split: AdminService lives only on
// the control plane (ADMIN_GRPC_ADDR), the data plane (GRPC_ADDR) answers
// every admin RPC Unimplemented even with a valid token, the control plane
// keeps the ADMIN_TOKEN gate, and RateLimitService is not on the control
// plane.
func TestPlaneSeparation(t *testing.T) {
	t.Parallel()
	p := startPlanes(t)
	authed := server.WithAdminToken(context.Background(), testToken)

	adminCalls := map[string]func(context.Context, ratelimitv1.AdminServiceClient) error{
		"Stats": func(ctx context.Context, c ratelimitv1.AdminServiceClient) error {
			_, err := c.Stats(ctx, &ratelimitv1.StatsRequest{})
			return err
		},
		"ListPolicies": func(ctx context.Context, c ratelimitv1.AdminServiceClient) error {
			_, err := c.ListPolicies(ctx, &ratelimitv1.ListPoliciesRequest{})
			return err
		},
		"GetPolicy": func(ctx context.Context, c ratelimitv1.AdminServiceClient) error {
			_, err := c.GetPolicy(ctx, &ratelimitv1.GetPolicyRequest{Tenant: "acme", Name: "api"})
			return err
		},
		"ResetKey": func(ctx context.Context, c ratelimitv1.AdminServiceClient) error {
			_, err := c.ResetKey(ctx, &ratelimitv1.ResetKeyRequest{Tenant: "acme", Subject: "k1"})
			return err
		},
		"DeletePolicy": func(ctx context.Context, c ratelimitv1.AdminServiceClient) error {
			_, err := c.DeletePolicy(ctx, &ratelimitv1.DeletePolicyRequest{Tenant: "acme", Name: "nope"})
			return err
		},
	}

	t.Run("data plane refuses admin RPCs", func(t *testing.T) {
		t.Parallel()
		c := ratelimitv1.NewAdminServiceClient(p.data)
		for name, call := range adminCalls {
			for _, ctx := range []context.Context{context.Background(), authed} {
				if got := status.Code(call(ctx, c)); got != codes.Unimplemented {
					t.Errorf("%s on the data plane: code = %s, want Unimplemented", name, got)
				}
			}
		}
	})

	t.Run("control plane gates admin RPCs", func(t *testing.T) {
		t.Parallel()
		c := ratelimitv1.NewAdminServiceClient(p.control)
		for name, call := range adminCalls {
			if got := status.Code(call(context.Background(), c)); got != codes.Unauthenticated {
				t.Errorf("%s without a token: code = %s, want Unauthenticated", name, got)
			}
		}
		if _, err := c.Stats(authed, &ratelimitv1.StatsRequest{}); err != nil {
			t.Errorf("Stats with the token: %v", err)
		}
		if _, err := c.GetPolicy(authed, &ratelimitv1.GetPolicyRequest{Tenant: "acme", Name: "api"}); err != nil {
			t.Errorf("GetPolicy with the token: %v", err)
		}
	})

	t.Run("data plane serves checks, control plane does not", func(t *testing.T) {
		t.Parallel()
		req := &ratelimitv1.CheckRequest{Tenant: "acme", Subject: "k1"}
		if _, err := ratelimitv1.NewRateLimitServiceClient(p.data).Check(context.Background(), req); err != nil {
			t.Errorf("Check on the data plane: %v", err)
		}
		_, err := ratelimitv1.NewRateLimitServiceClient(p.control).Check(authed, req)
		if got := status.Code(err); got != codes.Unimplemented {
			t.Errorf("Check on the control plane: code = %s, want Unimplemented", got)
		}
	})

	t.Run("health names only the plane's own service", func(t *testing.T) {
		t.Parallel()
		cases := []struct {
			conn    *grpc.ClientConn
			service string
			want    codes.Code
		}{
			{p.data, "", codes.OK},
			{p.data, ratelimitv1.RateLimitService_ServiceDesc.ServiceName, codes.OK},
			{p.data, ratelimitv1.AdminService_ServiceDesc.ServiceName, codes.NotFound},
			{p.control, "", codes.OK},
			{p.control, ratelimitv1.AdminService_ServiceDesc.ServiceName, codes.OK},
			{p.control, ratelimitv1.RateLimitService_ServiceDesc.ServiceName, codes.NotFound},
		}
		for _, tc := range cases {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_, err := healthv1.NewHealthClient(tc.conn).Check(ctx, &healthv1.HealthCheckRequest{Service: tc.service})
			cancel()
			if got := status.Code(err); got != tc.want {
				t.Errorf("health %q on %s: code = %s, want %s", tc.service, tc.conn.Target(), got, tc.want)
			}
		}
	})
}

func TestIsLoopback(t *testing.T) {
	t.Parallel()
	tests := []struct {
		addr string
		want bool
	}{
		{"127.0.0.1:50054", true},
		{"127.1.2.3:50054", true},
		{"[::1]:50054", true},
		{"localhost:50054", true},
		{":50054", false},
		{"0.0.0.0:50054", false},
		{"[::]:50054", false},
		{"10.0.0.5:50054", false},
		{"admin.internal:50054", false},
		{"no-port", false},
	}
	for _, tt := range tests {
		if got := isLoopback(tt.addr); got != tt.want {
			t.Errorf("isLoopback(%q) = %v, want %v", tt.addr, got, tt.want)
		}
	}
}
