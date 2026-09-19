package server_test

import (
	"context"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	ratelimitv1 "github.com/crowbett/ratelimitd/gen/ratelimit/v1"
	"github.com/crowbett/ratelimitd/internal/server"
)

const testAdminToken = "0123456789abcdef0123456789abcdef"

// TestAdminAuth pins the control-plane gate: with ADMIN_TOKEN set, every
// AdminService RPC needs "authorization: Bearer <token>", the data plane
// never does, and a refusal names neither the expected nor the presented
// token.
func TestAdminAuth(t *testing.T) {
	t.Parallel()
	e := newEnvWithToken(t, server.Config{}, testAdminToken)
	ctx := context.Background()

	cases := []struct {
		name string
		ctx  context.Context
		want codes.Code
	}{
		{"no metadata", ctx, codes.Unauthenticated},
		{"wrong token", server.WithAdminToken(ctx, "not-the-token-not-the-token-nope"), codes.Unauthenticated},
		{"wrong token, same length", server.WithAdminToken(ctx, "0123456789abcdef0123456789abcdeg"), codes.Unauthenticated},
		{"wrong scheme", metadata.AppendToOutgoingContext(ctx, server.AuthorizationHeader, "Basic "+testAdminToken), codes.Unauthenticated},
		{"bare token without scheme", metadata.AppendToOutgoingContext(ctx, server.AuthorizationHeader, testAdminToken), codes.Unauthenticated},
		{"empty header", metadata.AppendToOutgoingContext(ctx, server.AuthorizationHeader, ""), codes.Unauthenticated},
		{"right token", server.WithAdminToken(ctx, testAdminToken), codes.OK},
		{"right token, lower-case scheme", metadata.AppendToOutgoingContext(ctx, server.AuthorizationHeader, "bearer "+testAdminToken), codes.OK},
		{"right token after a wrong one", server.WithAdminToken(server.WithAdminToken(ctx, "wrong-wrong-wrong-wrong"), testAdminToken), codes.OK},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			for name, call := range map[string]func(context.Context) error{
				"Stats": func(ctx context.Context) error {
					_, err := e.admin.Stats(ctx, &ratelimitv1.StatsRequest{})
					return err
				},
				"ListPolicies": func(ctx context.Context) error {
					_, err := e.admin.ListPolicies(ctx, &ratelimitv1.ListPoliciesRequest{})
					return err
				},
				"ResetKey": func(ctx context.Context) error {
					_, err := e.admin.ResetKey(ctx, &ratelimitv1.ResetKeyRequest{Tenant: "acme", Subject: "k1"})
					return err
				},
				"DeletePolicy(missing)": func(ctx context.Context) error {
					// NotFound only when the gate let it through.
					_, err := e.admin.DeletePolicy(ctx, &ratelimitv1.DeletePolicyRequest{Tenant: "acme", Name: "nope"})
					if status.Code(err) == codes.NotFound {
						return nil
					}
					return err
				},
			} {
				err := call(tc.ctx)
				if got := status.Code(err); got != tc.want {
					t.Errorf("%s: code = %s, want %s (err %v)", name, got, tc.want, err)
				}
				if err != nil {
					if msg := status.Convert(err).Message(); strings.Contains(msg, testAdminToken) || strings.Contains(msg, "not-the-token") {
						t.Errorf("%s: refusal echoes a token: %q", name, msg)
					}
				}
			}
			// The data plane is never gated.
			if _, err := e.check.Check(tc.ctx, &ratelimitv1.CheckRequest{Tenant: "acme", Subject: "k1"}); err != nil {
				t.Errorf("Check: %v, want the data plane ungated", err)
			}
		})
	}
}

// TestAdminAuthDisabled pins the default: no token, no gate.
func TestAdminAuthDisabled(t *testing.T) {
	t.Parallel()
	e := newEnvWithToken(t, server.Config{}, "")
	if _, err := e.admin.Stats(context.Background(), &ratelimitv1.StatsRequest{}); err != nil {
		t.Fatalf("Stats without a configured token: %v", err)
	}
}
