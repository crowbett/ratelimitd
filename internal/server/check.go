// Package server holds the gRPC adapters: RateLimitService (check.go),
// AdminService (admin.go), proto <-> domain converters (convert.go) shared
// with internal/httpapi, and the error -> status table (errors.go).
// Validation happens up front; a denial is CODE_OVER_LIMIT with an OK status.
//
// Owner: workstream C (service edge). The constructor shapes here are the
// integrator's proposal, not frozen by docs/PLAN.md; C may amend them and
// must update cmd/ratelimitd when it does.
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

// Limits bound a request (docs/PLAN.md, Data flow step 1).
const (
	MaxNameLen     = 256   // tenant and subject: 1..256 printable ASCII
	MaxDescriptors = 16    // per CheckRequest
	MaxCost        = 10000 // per CheckRequest
	MaxBatch       = 64    // CheckRequests per BatchCheck
)

// Config is what the adapters need beyond their dependencies.
type Config struct {
	// TrustClientClock honours CheckRequest.now (tests, bench); default false.
	TrustClientClock bool
}

// CheckServer implements ratelimitv1.RateLimitServiceServer.
type CheckServer struct {
	ratelimitv1.UnimplementedRateLimitServiceServer
	store limit.Store
	reg   *policy.Registry
	snap  *admin.Snapshot
	log   *slog.Logger
	cfg   Config
}

// NewCheckServer wires the data plane. snap may be nil; log nil = slog.Default().
func NewCheckServer(store limit.Store, reg *policy.Registry, snap *admin.Snapshot, log *slog.Logger, cfg Config) *CheckServer {
	if log == nil {
		log = slog.Default()
	}
	return &CheckServer{store: store, reg: reg, snap: snap, log: log, cfg: cfg}
}

// Register attaches the service to srv.
func (s *CheckServer) Register(srv grpc.ServiceRegistrar) {
	ratelimitv1.RegisterRateLimitServiceServer(srv, s)
}

// Check decides for one subject. STUB: Unimplemented.
func (s *CheckServer) Check(_ context.Context, _ *ratelimitv1.CheckRequest) (*ratelimitv1.CheckResponse, error) {
	return nil, status.Error(codes.Unimplemented, "stub: server.CheckServer.Check")
}

// BatchCheck decides for up to MaxBatch subjects. STUB: Unimplemented.
func (s *CheckServer) BatchCheck(_ context.Context, _ *ratelimitv1.BatchCheckRequest) (*ratelimitv1.BatchCheckResponse, error) {
	return nil, status.Error(codes.Unimplemented, "stub: server.CheckServer.BatchCheck")
}
