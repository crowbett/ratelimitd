package server

import (
	"errors"

	ratelimitv1 "github.com/crowbett/ratelimitd/gen/ratelimit/v1"
	"github.com/crowbett/ratelimitd/internal/limit"
	"github.com/crowbett/ratelimitd/internal/policy"
)

// DecisionToProto maps a domain Decision to the wire shape: Allowed ->
// CODE_OK / CODE_OVER_LIMIT, ints clamped to uint32, durations as
// google.protobuf.Duration, degraded = Backend == limit.BackendDegraded. STUB.
func DecisionToProto(_ limit.Decision) *ratelimitv1.CheckResponse {
	return &ratelimitv1.CheckResponse{Code: ratelimitv1.Code_CODE_UNSPECIFIED}
}

// DescriptorsFromProto converts the Envoy-shaped list. STUB: nil.
func DescriptorsFromProto(_ []*ratelimitv1.Descriptor) []policy.Descriptor { return nil }

// PolicyToProto renders a policy for the admin API. STUB.
func PolicyToProto(_ policy.Policy) *ratelimitv1.Policy { return &ratelimitv1.Policy{} }

// PolicyFromProto parses an admin API policy; numbers are validated by
// policy.Set.Validate after the upsert, not here. STUB.
func PolicyFromProto(_ *ratelimitv1.Policy) (policy.Policy, error) {
	return policy.Policy{}, errors.New("stub: server.PolicyFromProto")
}
