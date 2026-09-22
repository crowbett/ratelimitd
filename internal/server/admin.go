package server

import (
	"context"
	"log/slog"
	"sort"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	ratelimitv1 "github.com/crowbett/ratelimitd/gen/ratelimit/v1"
	"github.com/crowbett/ratelimitd/internal/admin"
	"github.com/crowbett/ratelimitd/internal/limit"
	"github.com/crowbett/ratelimitd/internal/policy"
)

// PolicyRegistry is the slice of *policy.Registry the control plane needs;
// an interface so the admin RPCs can be tested against a fake.
type PolicyRegistry interface {
	Snapshot() *policy.Set
	Upsert(ctx context.Context, tenant string, p policy.Policy) (*policy.Set, error)
	Delete(ctx context.Context, tenant, name string) (*policy.Set, error)
}

// AdminServer implements ratelimitv1.AdminServiceServer. cmd/ratelimitd
// serves it on its own control-plane listener (ADMIN_GRPC_ADDR, loopback by
// default), never on the data plane (GRPC_ADDR); UnaryAdminAuth additionally
// gates it behind ADMIN_TOKEN when one is configured.
type AdminServer struct {
	ratelimitv1.UnimplementedAdminServiceServer
	reg       PolicyRegistry
	store     limit.Store
	snap      *admin.Snapshot
	log       *slog.Logger
	version   string
	startedAt time.Time
}

// NewAdminServer wires the control plane. snap may be nil; log nil = slog.Default().
func NewAdminServer(reg PolicyRegistry, store limit.Store, snap *admin.Snapshot, log *slog.Logger) *AdminServer {
	if log == nil {
		log = slog.Default()
	}
	return &AdminServer{reg: reg, store: store, snap: snap, log: log, startedAt: time.Now()}
}

// SetBuildInfo records what Stats reports as version and started_at
// (defaults: "" and the constructor time).
func (s *AdminServer) SetBuildInfo(version string, startedAt time.Time) {
	s.version = version
	if !startedAt.IsZero() {
		s.startedAt = startedAt
	}
}

// Register attaches the service to srv.
func (s *AdminServer) Register(srv grpc.ServiceRegistrar) {
	ratelimitv1.RegisterAdminServiceServer(srv, s)
}

// GetPolicy returns one policy of a tenant ("" = global).
func (s *AdminServer) GetPolicy(_ context.Context, req *ratelimitv1.GetPolicyRequest) (*ratelimitv1.GetPolicyResponse, error) {
	if err := checkTenantName(req.GetTenant()); err != nil {
		return nil, err
	}
	if !policy.NameRE.MatchString(req.GetName()) {
		return nil, status.Errorf(codes.InvalidArgument, "policy name %q must match %s", req.GetName(), policy.NameRE)
	}
	ps, err := s.policiesOf(req.GetTenant())
	if err != nil {
		return nil, err
	}
	for i := range ps {
		if ps[i].Name == req.GetName() {
			return &ratelimitv1.GetPolicyResponse{Policy: PolicyToProto(ps[i])}, nil
		}
	}
	return nil, status.Errorf(codes.NotFound, "policy %q not found in %s", req.GetName(), scopeName(req.GetTenant()))
}

// ListPolicies lists the policies of one tenant, or every tenant (sorted by
// id) plus the global list (tenant "") when the tenant is empty.
func (s *AdminServer) ListPolicies(_ context.Context, req *ratelimitv1.ListPoliciesRequest) (*ratelimitv1.ListPoliciesResponse, error) {
	if err := checkTenantName(req.GetTenant()); err != nil {
		return nil, err
	}
	set := s.reg.Snapshot()
	resp := &ratelimitv1.ListPoliciesResponse{Version: set.Version}
	if req.GetTenant() != "" {
		t, ok := set.Tenants[req.GetTenant()]
		if !ok {
			return nil, status.Errorf(codes.NotFound, "%v: %s", limit.ErrUnknownTenant, req.GetTenant())
		}
		resp.Tenants = []*ratelimitv1.TenantPolicies{tenantToProto(req.GetTenant(), t.Policies)}
		return resp, nil
	}
	ids := make([]string, 0, len(set.Tenants))
	for id := range set.Tenants {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	resp.Tenants = make([]*ratelimitv1.TenantPolicies, 0, len(ids)+1)
	resp.Tenants = append(resp.Tenants, tenantToProto("", set.Global))
	for _, id := range ids {
		resp.Tenants = append(resp.Tenants, tenantToProto(id, set.Tenants[id].Policies))
	}
	return resp, nil
}

// UpsertPolicy validates, rewrites the file and swaps the registry. The
// candidate set is validated here first so a bad policy is INVALID_ARGUMENT
// with the full problem list, whatever the registry wraps.
func (s *AdminServer) UpsertPolicy(ctx context.Context, req *ratelimitv1.UpsertPolicyRequest) (*ratelimitv1.UpsertPolicyResponse, error) {
	if err := checkTenantName(req.GetTenant()); err != nil {
		return nil, err
	}
	p, err := PolicyFromProto(req.GetPolicy())
	if err != nil {
		return nil, ToStatus(err)
	}
	if err := s.preflight(req.GetTenant(), func(ps []policy.Policy) []policy.Policy { return upsertInto(ps, p) }); err != nil {
		return nil, err
	}
	set, err := s.reg.Upsert(ctx, req.GetTenant(), p)
	if err != nil {
		s.log.ErrorContext(ctx, "upsert policy failed", slog.String("tenant", req.GetTenant()), slog.String("policy", p.Name), slog.Any("err", err))
		return nil, ToStatus(err)
	}
	s.snap.SetPolicyVersion(set.Version)
	s.log.InfoContext(ctx, "policy upserted", slog.String("tenant", req.GetTenant()), slog.String("policy", p.Name), slog.String("version", set.Version))
	return &ratelimitv1.UpsertPolicyResponse{Policy: PolicyToProto(p), Version: set.Version}, nil
}

// DeletePolicy removes a policy by tenant + name; NOT_FOUND when absent.
func (s *AdminServer) DeletePolicy(ctx context.Context, req *ratelimitv1.DeletePolicyRequest) (*ratelimitv1.DeletePolicyResponse, error) {
	if err := checkTenantName(req.GetTenant()); err != nil {
		return nil, err
	}
	if !policy.NameRE.MatchString(req.GetName()) {
		return nil, status.Errorf(codes.InvalidArgument, "policy name %q must match %s", req.GetName(), policy.NameRE)
	}
	ps, err := s.policiesOf(req.GetTenant())
	if err != nil {
		return nil, err
	}
	if !hasPolicy(ps, req.GetName()) {
		return nil, status.Errorf(codes.NotFound, "policy %q not found in %s", req.GetName(), scopeName(req.GetTenant()))
	}
	if err := s.preflight(req.GetTenant(), func(ps []policy.Policy) []policy.Policy { return deleteFrom(ps, req.GetName()) }); err != nil {
		return nil, err
	}
	set, err := s.reg.Delete(ctx, req.GetTenant(), req.GetName())
	if err != nil {
		s.log.ErrorContext(ctx, "delete policy failed", slog.String("tenant", req.GetTenant()), slog.String("policy", req.GetName()), slog.Any("err", err))
		return nil, ToStatus(err)
	}
	s.snap.SetPolicyVersion(set.Version)
	s.log.InfoContext(ctx, "policy deleted", slog.String("tenant", req.GetTenant()), slog.String("policy", req.GetName()), slog.String("version", set.Version))
	return &ratelimitv1.DeletePolicyResponse{Version: set.Version}, nil
}

// ResetKey clears limiter state for one subject: one policy when named, else
// every policy (tenant + global) of the subject. removed is the number of
// keys addressed (the Store port does not report hits).
func (s *AdminServer) ResetKey(ctx context.Context, req *ratelimitv1.ResetKeyRequest) (*ratelimitv1.ResetKeyResponse, error) {
	in := Input{Tenant: req.GetTenant(), Subject: req.GetSubject()}
	if err := in.Validate(); err != nil {
		return nil, ToStatus(err)
	}
	set := s.reg.Snapshot()
	t, ok := set.Tenants[req.GetTenant()]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "%v: %s", limit.ErrUnknownTenant, req.GetTenant())
	}
	var keys []string
	if name := req.GetPolicy(); name != "" {
		if !policy.NameRE.MatchString(name) {
			return nil, status.Errorf(codes.InvalidArgument, "policy name %q must match %s", name, policy.NameRE)
		}
		if !hasPolicy(t.Policies, name) && !hasPolicy(set.Global, name) {
			return nil, status.Errorf(codes.NotFound, "policy %q not found for tenant %s", name, req.GetTenant())
		}
		keys = []string{limit.Key(req.GetTenant(), req.GetSubject(), name)}
	} else {
		keys = make([]string, 0, len(t.Policies)+len(set.Global))
		for i := range t.Policies {
			keys = append(keys, limit.Key(req.GetTenant(), req.GetSubject(), t.Policies[i].Name))
		}
		for i := range set.Global {
			keys = append(keys, limit.Key(req.GetTenant(), req.GetSubject(), set.Global[i].Name))
		}
	}
	if err := s.store.Reset(ctx, keys); err != nil {
		s.log.ErrorContext(ctx, "reset keys failed", slog.String("store", s.store.Name()), slog.Int("keys", len(keys)), slog.Any("err", err))
		return nil, ToStatus(err)
	}
	return &ratelimitv1.ResetKeyResponse{Removed: clampU32(len(keys))}, nil
}

// Stats returns the Snapshot counters (the gRPC view of /admin/stats).
func (s *AdminServer) Stats(_ context.Context, _ *ratelimitv1.StatsRequest) (*ratelimitv1.StatsResponse, error) {
	c := s.snap.Counters()
	policyVersion := c.PolicyVersion
	if policyVersion == "" {
		policyVersion = s.reg.Snapshot().Version
	}
	return &ratelimitv1.StatsResponse{
		Version:          s.version,
		StartedAt:        timestamppb.New(s.startedAt),
		PolicyVersion:    policyVersion,
		DecisionsAllowed: c.Allowed,
		DecisionsDenied:  c.Denied,
		BackendErrors:    c.BackendErrors,
		FallbackActive:   c.FallbackActive,
		FallbackTotal:    durationpb.New(c.FallbackTotal),
		ClockSkew:        durationpb.New(c.ClockSkew),
		LocalKeys:        uint64(max(c.LocalKeys, 0)),
	}, nil
}

// policiesOf returns the tenant's policies, or the global list for "".
func (s *AdminServer) policiesOf(tenant string) ([]policy.Policy, error) {
	set := s.reg.Snapshot()
	if tenant == "" {
		return set.Global, nil
	}
	t, ok := set.Tenants[tenant]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "%v: %s", limit.ErrUnknownTenant, tenant)
	}
	return t.Policies, nil
}

// preflight validates the set as it would look after mutate is applied to
// the tenant's (or global) policy list, without touching the registry.
// Unknown tenants are created by an upsert, matching Registry.Upsert.
func (s *AdminServer) preflight(tenant string, mutate func([]policy.Policy) []policy.Policy) error {
	cur := s.reg.Snapshot()
	next := *cur
	if tenant == "" {
		next.Global = mutate(cur.Global)
	} else {
		next.Tenants = make(map[string]policy.Tenant, len(cur.Tenants)+1)
		for id, t := range cur.Tenants {
			next.Tenants[id] = t
		}
		t := next.Tenants[tenant]
		t.ID = tenant
		t.Policies = mutate(t.Policies)
		next.Tenants[tenant] = t
	}
	if next.SchemaVersion == 0 {
		next.SchemaVersion = 1
	}
	if err := next.Validate(); err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	return nil
}

func upsertInto(ps []policy.Policy, p policy.Policy) []policy.Policy {
	out := make([]policy.Policy, 0, len(ps)+1)
	replaced := false
	for i := range ps {
		if ps[i].Name == p.Name {
			out = append(out, p)
			replaced = true
			continue
		}
		out = append(out, ps[i])
	}
	if !replaced {
		out = append(out, p)
	}
	return out
}

func deleteFrom(ps []policy.Policy, name string) []policy.Policy {
	out := make([]policy.Policy, 0, len(ps))
	for i := range ps {
		if ps[i].Name != name {
			out = append(out, ps[i])
		}
	}
	return out
}

func hasPolicy(ps []policy.Policy, name string) bool {
	for i := range ps {
		if ps[i].Name == name {
			return true
		}
	}
	return false
}

func tenantToProto(id string, ps []policy.Policy) *ratelimitv1.TenantPolicies {
	out := &ratelimitv1.TenantPolicies{Tenant: id, Policies: make([]*ratelimitv1.Policy, 0, len(ps))}
	for i := range ps {
		out.Policies = append(out.Policies, PolicyToProto(ps[i]))
	}
	return out
}

// checkTenantName accepts "" (global) or a NameRE-conforming tenant id.
func checkTenantName(tenant string) error {
	if tenant == "" || policy.NameRE.MatchString(tenant) {
		return nil
	}
	return status.Errorf(codes.InvalidArgument, "tenant %q must match %s", tenant, policy.NameRE)
}

func scopeName(tenant string) string {
	if tenant == "" {
		return "global"
	}
	return "tenant " + tenant
}
