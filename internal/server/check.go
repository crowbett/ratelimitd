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
	"math"
	"time"

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

// Resolver is the slice of *policy.Registry the data plane needs. It is an
// interface so the adapters can be tested against a fake while the registry
// is workstream A's.
type Resolver interface {
	// Resolve returns the Reqs to send to the store, or a short-circuit
	// Decision (allow/deny list hit) with nil reqs. Unknown tenant ->
	// limit.ErrUnknownTenant; nothing matched -> limit.ErrNoPolicy.
	Resolve(tenant, subject string, d []policy.Descriptor, route, method string) ([]limit.Req, *limit.Decision, error)
}

// CheckServer implements ratelimitv1.RateLimitServiceServer.
type CheckServer struct {
	ratelimitv1.UnimplementedRateLimitServiceServer
	store limit.Store
	reg   Resolver
	snap  *admin.Snapshot
	log   *slog.Logger
	cfg   Config
	now   func() time.Time
}

// NewCheckServer wires the data plane. snap may be nil; log nil = slog.Default().
func NewCheckServer(store limit.Store, reg Resolver, snap *admin.Snapshot, log *slog.Logger, cfg Config) *CheckServer {
	if log == nil {
		log = slog.Default()
	}
	return &CheckServer{store: store, reg: reg, snap: snap, log: log, cfg: cfg, now: time.Now}
}

// Register attaches the service to srv.
func (s *CheckServer) Register(srv grpc.ServiceRegistrar) {
	ratelimitv1.RegisterRateLimitServiceServer(srv, s)
}

// Check decides for one subject. A denial is CODE_OVER_LIMIT with an OK
// status; validation -> INVALID_ARGUMENT, unknown tenant -> NOT_FOUND, no
// matching policy -> FAILED_PRECONDITION, backend down with no fallback ->
// UNAVAILABLE.
func (s *CheckServer) Check(ctx context.Context, req *ratelimitv1.CheckRequest) (*ratelimitv1.CheckResponse, error) {
	d, err := s.Decide(ctx, InputFromProto(req))
	if err != nil {
		return nil, err
	}
	return DecisionToProto(d), nil
}

// BatchCheck decides for up to MaxBatch subjects, one store.Apply per check
// (the Store contract is one subject per call); results are positional. Any
// invalid check fails the whole batch with INVALID_ARGUMENT naming its index.
func (s *CheckServer) BatchCheck(ctx context.Context, req *ratelimitv1.BatchCheckRequest) (*ratelimitv1.BatchCheckResponse, error) {
	checks := req.GetChecks()
	if len(checks) == 0 {
		return nil, status.Error(codes.InvalidArgument, "invalid request: checks: at least one required")
	}
	if len(checks) > MaxBatch {
		return nil, status.Errorf(codes.InvalidArgument, "invalid request: checks: at most %d, got %d", MaxBatch, len(checks))
	}
	inputs := make([]Input, len(checks))
	for i, c := range checks {
		inputs[i] = InputFromProto(c)
		if err := inputs[i].Validate(); err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "checks[%d]: %v", i, err)
		}
	}
	ds, err := s.DecideBatch(ctx, inputs)
	if err != nil {
		return nil, err
	}
	out := &ratelimitv1.BatchCheckResponse{Results: make([]*ratelimitv1.CheckResponse, len(ds))}
	for i, d := range ds {
		out.Results[i] = DecisionToProto(d)
	}
	return out, nil
}

// InputFromProto converts a CheckRequest without validating it.
func InputFromProto(req *ratelimitv1.CheckRequest) Input {
	in := Input{
		Tenant:      req.GetTenant(),
		Subject:     req.GetSubject(),
		Descriptors: DescriptorsFromProto(req.GetDescriptors()),
		Route:       req.GetRoute(),
		Method:      req.GetMethod(),
		Cost:        int(min(req.GetCost(), math.MaxInt32)), // fits int on every platform; Validate bounds it
	}
	if ts := req.GetNow(); ts != nil && ts.IsValid() {
		in.Now = ts.AsTime()
	}
	return in
}

// Decide is the shared decision path of both surfaces: validate -> resolve
// -> short-circuit or store.Apply -> Strictest -> RecordDecision. The error
// is always a gRPC status (see ToStatus); httpapi maps its code to HTTP.
func (s *CheckServer) Decide(ctx context.Context, in Input) (limit.Decision, error) {
	if err := in.Validate(); err != nil {
		return limit.Decision{}, ToStatus(err)
	}
	start := s.now()
	now := s.clock(in, start)

	reqs, short, err := s.reg.Resolve(in.Tenant, in.Subject, in.Descriptors, in.Route, in.Method)
	if err != nil {
		return limit.Decision{}, ToStatus(err)
	}
	var d limit.Decision
	if short != nil {
		d = *short
	} else {
		if len(reqs) == 0 {
			return limit.Decision{}, ToStatus(limit.ErrNoPolicy)
		}
		applyCost(reqs, in.Cost)
		ds, err := s.store.Apply(ctx, reqs, now)
		if err != nil {
			return limit.Decision{}, s.storeError(ctx, err, in)
		}
		if len(ds) != len(reqs) {
			s.log.ErrorContext(ctx, "store returned a short answer", slog.String("store", s.store.Name()), slog.Int("reqs", len(reqs)), slog.Int("decisions", len(ds)))
			return limit.Decision{}, status.Error(codes.Internal, "internal error")
		}
		d = limit.Strictest(ds)
	}
	s.snap.RecordDecision(in.Tenant, d.Policy, d.Allowed, d.Backend, s.now().Sub(start))
	return d, nil
}

// DecideBatch answers every input positionally. Every CheckRequest is exactly
// one subject (the Store contract: one hash slot per call), so the groups are
// never merged; they are pipelined in one round trip when the store
// implements limit.BatchStore, else applied one by one. Inputs must already
// be validated. The first error aborts the batch and names the offending index.
func (s *CheckServer) DecideBatch(ctx context.Context, inputs []Input) ([]limit.Decision, error) {
	start := s.now()
	out := make([]limit.Decision, len(inputs))
	groups := make([][]limit.Req, len(inputs)) // nil where the input short-circuited
	for i := range inputs {
		in := &inputs[i]
		reqs, short, err := s.reg.Resolve(in.Tenant, in.Subject, in.Descriptors, in.Route, in.Method)
		if err != nil {
			return nil, batchErr(i, ToStatus(err))
		}
		if short != nil {
			out[i] = *short
			continue
		}
		if len(reqs) == 0 {
			return nil, batchErr(i, ToStatus(limit.ErrNoPolicy))
		}
		applyCost(reqs, in.Cost)
		groups[i] = reqs
	}
	if err := s.applyGroups(ctx, inputs, groups, out, start); err != nil {
		return nil, err
	}
	latency := s.now().Sub(start)
	for i, d := range out {
		s.snap.RecordDecision(inputs[i].Tenant, d.Policy, d.Allowed, d.Backend, latency)
	}
	return out, nil
}

// applyGroups evaluates every non-nil group: in one pipelined round trip when
// the store implements limit.BatchStore, else one Apply per group.
func (s *CheckServer) applyGroups(ctx context.Context, inputs []Input, groups [][]limit.Req, out []limit.Decision, start time.Time) error {
	if bs, ok := s.store.(limit.BatchStore); ok {
		live := make([][]limit.Req, 0, len(groups))
		idx := make([]int, 0, len(groups))
		for i, g := range groups {
			if g != nil {
				live = append(live, g)
				idx = append(idx, i)
			}
		}
		if len(live) == 0 {
			return nil
		}
		// One now for the batch: the inputs' clocks only differ under TrustClientClock.
		dss, errs, err := bs.ApplyMany(ctx, live, s.clock(inputs[idx[0]], start))
		if err != nil {
			return batchErr(idx[0], s.storeError(ctx, err, inputs[idx[0]]))
		}
		for j, i := range idx {
			if errs[j] != nil {
				return batchErr(i, s.storeError(ctx, errs[j], inputs[i]))
			}
			if len(dss[j]) != len(live[j]) {
				s.log.ErrorContext(ctx, "store returned a short answer", slog.String("store", s.store.Name()), slog.Int("reqs", len(live[j])), slog.Int("decisions", len(dss[j])))
				return status.Error(codes.Internal, "internal error")
			}
			out[i] = limit.Strictest(dss[j])
		}
		return nil
	}
	for i, reqs := range groups {
		if reqs == nil {
			continue
		}
		ds, err := s.store.Apply(ctx, reqs, s.clock(inputs[i], start))
		if err != nil {
			return batchErr(i, s.storeError(ctx, err, inputs[i]))
		}
		if len(ds) != len(reqs) {
			s.log.ErrorContext(ctx, "store returned a short answer", slog.String("store", s.store.Name()), slog.Int("reqs", len(reqs)), slog.Int("decisions", len(ds)))
			return status.Error(codes.Internal, "internal error")
		}
		out[i] = limit.Strictest(ds)
	}
	return nil
}

// clock returns the decision time: the caller's when trusted and set, else
// the server's.
func (s *CheckServer) clock(in Input, serverNow time.Time) time.Time {
	if s.cfg.TrustClientClock && !in.Now.IsZero() {
		return in.Now
	}
	return serverNow
}

// applyCost multiplies each req's policy cost (route cost table, default 1)
// by the caller's cost; a req cost below 1 is treated as 1.
func applyCost(reqs []limit.Req, cost int) {
	if cost < 1 {
		cost = 1
	}
	for i := range reqs {
		reqs[i].Cost = max(reqs[i].Cost, 1) * cost
	}
}

func batchErr(i int, err error) error {
	st, _ := status.FromError(err)
	return status.Errorf(st.Code(), "checks[%d]: %s", i, st.Message())
}

// storeError logs and maps a store failure. ErrBackendDown is what the
// fallback store returns in fail_closed mode when it reports the outage;
// every other error is Internal.
func (s *CheckServer) storeError(ctx context.Context, err error, in Input) error {
	s.log.ErrorContext(ctx, "store apply failed",
		slog.String("store", s.store.Name()), slog.String("tenant", in.Tenant), slog.Any("err", err))
	if st := ToStatus(err); status.Code(st) != codes.Internal {
		return st
	}
	return status.Errorf(codes.Internal, "store %s: internal error", s.store.Name())
}
