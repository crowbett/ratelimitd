package server

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"

	ratelimitv1 "github.com/crowbett/ratelimitd/gen/ratelimit/v1"
	"github.com/crowbett/ratelimitd/internal/limit"
	"github.com/crowbett/ratelimitd/internal/policy"
)

// DecisionToProto maps a domain Decision to the wire shape: Allowed ->
// CODE_OK / CODE_OVER_LIMIT, ints clamped to uint32, durations as
// google.protobuf.Duration, degraded = Backend == limit.BackendDegraded.
func DecisionToProto(d limit.Decision) *ratelimitv1.CheckResponse {
	code := ratelimitv1.Code_CODE_OVER_LIMIT
	if d.Allowed {
		code = ratelimitv1.Code_CODE_OK
	}
	return &ratelimitv1.CheckResponse{
		Code:       code,
		Limit:      clampU32(d.Limit),
		Remaining:  clampU32(d.Remaining),
		RetryAfter: durationpb.New(max(d.RetryAfter, 0)),
		ResetAfter: durationpb.New(max(d.ResetAfter, 0)),
		Policy:     d.Policy,
		Backend:    d.Backend,
		Degraded:   d.Backend == limit.BackendDegraded,
	}
}

func clampU32(n int) uint32 {
	if n < 0 {
		return 0
	}
	if n > math.MaxUint32 {
		return math.MaxUint32
	}
	return uint32(n)
}

// DescriptorsFromProto converts the Envoy-shaped list; nil in, nil out.
func DescriptorsFromProto(in []*ratelimitv1.Descriptor) []policy.Descriptor {
	if len(in) == 0 {
		return nil
	}
	out := make([]policy.Descriptor, 0, len(in))
	for _, d := range in {
		out = append(out, policy.Descriptor{Key: d.GetKey(), Value: d.GetValue()})
	}
	return out
}

// PolicyToProto renders a policy for the admin API. An empty or unknown
// algorithm spelling becomes ALGORITHM_UNSPECIFIED (the file default applies).
func PolicyToProto(p policy.Policy) *ratelimitv1.Policy {
	out := &ratelimitv1.Policy{
		Name:    p.Name,
		Rate:    p.Rate,
		Per:     durationpb.New(p.Per),
		Burst:   p.Burst,
		Key:     p.Key,
		Enabled: p.IsEnabled(),
	}
	if p.Algorithm != "" {
		if a, err := limit.ParseAlgorithm(p.Algorithm); err == nil {
			out.Algorithm = ratelimitv1.Algorithm(a)
		}
	}
	if len(p.Match.Routes) > 0 || len(p.Match.Methods) > 0 || len(p.Match.Descriptors) > 0 {
		m := &ratelimitv1.Match{Routes: append([]string(nil), p.Match.Routes...), Methods: append([]string(nil), p.Match.Methods...)}
		keys := make([]string, 0, len(p.Match.Descriptors))
		for k := range p.Match.Descriptors {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			m.Descriptors = append(m.Descriptors, &ratelimitv1.Descriptor{Key: k, Value: p.Match.Descriptors[k]})
		}
		out.Match = m
	}
	if len(p.Cost) > 0 {
		out.Cost = make(map[string]uint32, len(p.Cost))
		for glob, c := range p.Cost {
			out.Cost[glob] = clampU32(c)
		}
	}
	return out
}

// PolicyFromProto parses an admin API policy: the name must match
// policy.NameRE and the algorithm enum must be known; numbers are validated
// by policy.Set.Validate after the upsert, not here.
func PolicyFromProto(in *ratelimitv1.Policy) (policy.Policy, error) {
	if in == nil {
		return policy.Policy{}, fmt.Errorf("%w: policy is required", limit.ErrInvalidParams)
	}
	if !policy.NameRE.MatchString(in.GetName()) {
		return policy.Policy{}, fmt.Errorf("%w: policy name %q must match %s", limit.ErrInvalidParams, in.GetName(), policy.NameRE)
	}
	p := policy.Policy{
		Name:  in.GetName(),
		Rate:  in.GetRate(),
		Burst: in.GetBurst(),
		Key:   in.GetKey(),
	}
	if in.GetPer() != nil {
		if err := in.GetPer().CheckValid(); err != nil {
			return policy.Policy{}, fmt.Errorf("%w: per: %w", limit.ErrInvalidParams, err)
		}
		p.Per = in.GetPer().AsDuration()
	}
	switch a := in.GetAlgorithm(); a {
	case ratelimitv1.Algorithm_ALGORITHM_UNSPECIFIED:
		// "" = defaults.algorithm at validate time.
	case ratelimitv1.Algorithm_ALGORITHM_GCRA, ratelimitv1.Algorithm_ALGORITHM_TOKEN_BUCKET,
		ratelimitv1.Algorithm_ALGORITHM_SLIDING_WINDOW, ratelimitv1.Algorithm_ALGORITHM_SLIDING_WINDOW_EXACT:
		p.Algorithm = limit.Algorithm(a).String()
	default:
		return policy.Policy{}, fmt.Errorf("%w: unknown algorithm %d", limit.ErrInvalidParams, int32(a))
	}
	if m := in.GetMatch(); m != nil {
		p.Match.Routes = append([]string(nil), m.GetRoutes()...)
		p.Match.Methods = append([]string(nil), m.GetMethods()...)
		if len(m.GetDescriptors()) > 0 {
			p.Match.Descriptors = make(map[string]string, len(m.GetDescriptors()))
			for _, d := range m.GetDescriptors() {
				if d.GetKey() == "" {
					return policy.Policy{}, fmt.Errorf("%w: match descriptor key is required", limit.ErrInvalidParams)
				}
				p.Match.Descriptors[d.GetKey()] = d.GetValue()
			}
		}
	}
	if len(in.GetCost()) > 0 {
		p.Cost = make(map[string]int, len(in.GetCost()))
		for glob, c := range in.GetCost() {
			if c > math.MaxInt32 {
				return policy.Policy{}, fmt.Errorf("%w: cost for %q is out of range", limit.ErrInvalidParams, glob)
			}
			p.Cost[glob] = int(c)
		}
	}
	// The wire form has no "unset": enabled=false disables explicitly,
	// enabled=true is the default and stays nil so the YAML omits it.
	if !in.GetEnabled() {
		f := false
		p.Enabled = &f
	}
	return p, nil
}

// Input is one validated check, shared by the gRPC and JSON adapters so both
// surfaces make identical decisions (CheckServer.Decide).
type Input struct {
	Tenant      string
	Subject     string
	Descriptors []policy.Descriptor
	Route       string
	Method      string
	// Cost is the caller's multiplier on the policy's route cost; 0 means 1.
	Cost int
	// Now is the caller's clock; zero means the server clock. Only honoured
	// when Config.TrustClientClock is set.
	Now time.Time
}

// Validate applies the request bounds of docs/PLAN.md "Data flow" step 1:
// tenant/subject 1..MaxNameLen printable ASCII, <= MaxDescriptors, cost
// 1..MaxCost, route/method/descriptor fields printable ASCII <= MaxNameLen.
// Every problem is reported at once; the error is a plain error (the
// adapters wrap it into INVALID_ARGUMENT / 400).
func (in *Input) Validate() error {
	var errs []string
	if msg := checkName("tenant", in.Tenant, true); msg != "" {
		errs = append(errs, msg)
	}
	if msg := checkName("subject", in.Subject, true); msg != "" {
		errs = append(errs, msg)
	}
	if len(in.Descriptors) > MaxDescriptors {
		errs = append(errs, fmt.Sprintf("descriptors: at most %d, got %d", MaxDescriptors, len(in.Descriptors)))
	}
	for i, d := range in.Descriptors {
		if msg := checkName(fmt.Sprintf("descriptors[%d].key", i), d.Key, true); msg != "" {
			errs = append(errs, msg)
		}
		if msg := checkName(fmt.Sprintf("descriptors[%d].value", i), d.Value, false); msg != "" {
			errs = append(errs, msg)
		}
	}
	if msg := checkName("route", in.Route, false); msg != "" {
		errs = append(errs, msg)
	}
	if msg := checkName("method", in.Method, false); msg != "" {
		errs = append(errs, msg)
	}
	if in.Cost < 0 || in.Cost > MaxCost {
		errs = append(errs, fmt.Sprintf("cost: must be 1..%d, got %d", MaxCost, in.Cost))
	}
	if len(errs) == 0 {
		return nil
	}
	return &ValidationError{Problems: errs}
}

// ValidationError lists every request problem; adapters map it to
// INVALID_ARGUMENT / 400.
type ValidationError struct {
	Problems []string
}

func (e *ValidationError) Error() string {
	return "invalid request: " + strings.Join(e.Problems, "; ")
}

// checkName bounds a string field: printable ASCII (0x20..0x7E), at most
// MaxNameLen bytes, non-empty when required. It returns "" when fine.
func checkName(field, v string, required bool) string {
	if v == "" {
		if required {
			return field + ": required"
		}
		return ""
	}
	if len(v) > MaxNameLen {
		return fmt.Sprintf("%s: at most %d bytes, got %d", field, MaxNameLen, len(v))
	}
	for i := 0; i < len(v); i++ {
		if c := v[i]; c < 0x20 || c > 0x7e {
			return fmt.Sprintf("%s: byte %d is not printable ASCII", field, i)
		}
	}
	return ""
}
