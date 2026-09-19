package server

import (
	"context"
	"log/slog"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	ratelimitv1 "github.com/crowbett/ratelimitd/gen/ratelimit/v1"
	"github.com/crowbett/ratelimitd/internal/admin"
	"github.com/crowbett/ratelimitd/internal/limit"
	"github.com/crowbett/ratelimitd/internal/policy"
)

// AdminServer implements ratelimitv1.AdminServiceServer. It is registered on
// the admin listener only.
type AdminServer struct {
	ratelimitv1.UnimplementedAdminServiceServer
	reg   *policy.Registry
	store limit.Store
	snap  *admin.Snapshot
	log   *slog.Logger
}

// NewAdminServer wires the control plane. snap may be nil; log nil = slog.Default().
func NewAdminServer(reg *policy.Registry, store limit.Store, snap *admin.Snapshot, log *slog.Logger) *AdminServer {
	if log == nil {
		log = slog.Default()
	}
	return &AdminServer{reg: reg, store: store, snap: snap, log: log}
}

// Register attaches the service to srv.
func (s *AdminServer) Register(srv grpc.ServiceRegistrar) {
	ratelimitv1.RegisterAdminServiceServer(srv, s)
}

// GetPolicy returns one policy. STUB.
func (s *AdminServer) GetPolicy(_ context.Context, _ *ratelimitv1.GetPolicyRequest) (*ratelimitv1.GetPolicyResponse, error) {
	return nil, status.Error(codes.Unimplemented, "stub: server.AdminServer.GetPolicy")
}

// ListPolicies lists policies by tenant. STUB.
func (s *AdminServer) ListPolicies(_ context.Context, _ *ratelimitv1.ListPoliciesRequest) (*ratelimitv1.ListPoliciesResponse, error) {
	return nil, status.Error(codes.Unimplemented, "stub: server.AdminServer.ListPolicies")
}

// UpsertPolicy validates, rewrites the file and swaps the registry. STUB.
func (s *AdminServer) UpsertPolicy(_ context.Context, _ *ratelimitv1.UpsertPolicyRequest) (*ratelimitv1.UpsertPolicyResponse, error) {
	return nil, status.Error(codes.Unimplemented, "stub: server.AdminServer.UpsertPolicy")
}

// DeletePolicy removes a policy. STUB.
func (s *AdminServer) DeletePolicy(_ context.Context, _ *ratelimitv1.DeletePolicyRequest) (*ratelimitv1.DeletePolicyResponse, error) {
	return nil, status.Error(codes.Unimplemented, "stub: server.AdminServer.DeletePolicy")
}

// ResetKey clears limiter state for a subject. STUB.
func (s *AdminServer) ResetKey(_ context.Context, _ *ratelimitv1.ResetKeyRequest) (*ratelimitv1.ResetKeyResponse, error) {
	return nil, status.Error(codes.Unimplemented, "stub: server.AdminServer.ResetKey")
}

// Stats returns the Snapshot counters. STUB.
func (s *AdminServer) Stats(_ context.Context, _ *ratelimitv1.StatsRequest) (*ratelimitv1.StatsResponse, error) {
	return nil, status.Error(codes.Unimplemented, "stub: server.AdminServer.Stats")
}
