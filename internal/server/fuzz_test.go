package server_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	ratelimitv1 "github.com/crowbett/ratelimitd/gen/ratelimit/v1"
	"github.com/crowbett/ratelimitd/internal/limit"
	"github.com/crowbett/ratelimitd/internal/policy"
	"github.com/crowbett/ratelimitd/internal/server"
)

// FuzzInputValidate is the request boundary of both surfaces: a CheckRequest
// with arbitrary strings, cost and descriptor count -> InputFromProto ->
// Validate never panics; whatever it accepts is 1..MaxNameLen printable
// ASCII in every string field, at most MaxDescriptors of them, cost within
// 0..MaxCost; whatever it rejects is a *ValidationError naming the field.
func FuzzInputValidate(f *testing.F) {
	f.Add("acme", "k1", "/v1/search", "GET", "ip", "203.0.113.7", uint32(1), uint8(1))
	f.Add("", "", "", "", "", "", uint32(0), uint8(0))
	f.Add("acme", "k\x00", "", "", "", "", uint32(10001), uint8(17))
	f.Add(strings.Repeat("a", 257), "s", "", "", "", "", uint32(1), uint8(0))
	f.Add("t", "s", "", "", "", strings.Repeat("v", 256), uint32(1<<31), uint8(16))
	f.Add("tenant~", "sub ject", "/p a t h", "GET\x7f", "ü", "\t", uint32(1), uint8(2))
	f.Fuzz(func(t *testing.T, tenant, subject, route, method, dkey, dval string, cost uint32, nds uint8) {
		if len(tenant)+len(subject)+len(route)+len(method)+len(dkey)+len(dval) > 4096 {
			t.Skip()
		}
		req := &ratelimitv1.CheckRequest{Tenant: tenant, Subject: subject, Route: route, Method: method, Cost: cost, Now: timestamppb.New(time.Unix(1, 0))}
		for i := 0; i < int(nds); i++ {
			req.Descriptors = append(req.Descriptors, &ratelimitv1.Descriptor{Key: dkey, Value: dval})
		}
		in := server.InputFromProto(req)
		if in.Cost < 0 {
			t.Fatalf("cost %d from %d", in.Cost, cost)
		}
		if len(in.Descriptors) != int(nds) {
			t.Fatalf("%d descriptors converted from %d", len(in.Descriptors), nds)
		}
		err := in.Validate()
		if err == nil {
			for name, v := range map[string]string{"tenant": tenant, "subject": subject, "route": route, "method": method, "dkey": dkey, "dval": dval} {
				if (name == "dkey" || name == "dval") && nds == 0 {
					continue
				}
				if len(v) > server.MaxNameLen {
					t.Fatalf("accepted %s of %d bytes", name, len(v))
				}
				for i := 0; i < len(v); i++ {
					if v[i] < 0x20 || v[i] > 0x7e {
						t.Fatalf("accepted %s with byte %#x at %d", name, v[i], i)
					}
				}
			}
			if tenant == "" || subject == "" || (nds > 0 && dkey == "") {
				t.Fatalf("accepted an empty required field: %+v", in)
			}
			if nds > server.MaxDescriptors || cost > server.MaxCost {
				t.Fatalf("accepted %d descriptors / cost %d", nds, cost)
			}
			return
		}
		var verr *server.ValidationError
		if !errors.As(err, &verr) || len(verr.Problems) == 0 || !strings.HasPrefix(err.Error(), "invalid request: ") {
			t.Fatalf("rejection %v is not a ValidationError with problems", err)
		}
		for _, p := range verr.Problems {
			if strings.ContainsAny(p, "\x00\n") {
				t.Fatalf("problem %q echoes raw input bytes", p)
			}
		}
		if st := server.ToStatus(err); st == nil || !strings.Contains(st.Error(), "InvalidArgument") {
			t.Fatalf("ToStatus(%v) = %v, want InvalidArgument", err, st)
		}
	})
}

// FuzzPolicyRoundTrip: the admin wire shape -> PolicyFromProto never panics,
// rejects a name outside policy.NameRE and an out-of-range cost, and
// whatever it accepts renders back through PolicyToProto to an equal message
// (the file form is the source of truth; the wire form must not lose fields).
func FuzzPolicyRoundTrip(f *testing.F) {
	f.Add("api", int32(1), int64(100), int64(1), int64(10), "{tenant}:{subject}", "/v1/*", "GET", "ip", "*", "/v1/bulk*", uint32(10), true)
	f.Add("", int32(0), int64(0), int64(0), int64(0), "", "", "", "", "", "", uint32(0), false)
	f.Add("Bad Name", int32(2), int64(-1), int64(-1), int64(-1), "{", "[", "", "k", "v", "", uint32(1<<31), true)
	f.Add("x", int32(9), int64(1), int64(1_000_000_000), int64(1), "{subject}", "", "", "", "", "", uint32(1), true)
	f.Add("sw", int32(3), int64(60), int64(60_000_000_000), int64(60), "{subject}", "", "POST", "apikey", "", "*", uint32(0), false)
	f.Fuzz(func(t *testing.T, name string, algo int32, rate, perUS, burst int64, key, route, method, dkey, dval, costGlob string, cost uint32, enabled bool) {
		if len(name)+len(key)+len(route)+len(method)+len(dkey)+len(dval)+len(costGlob) > 2048 {
			t.Skip()
		}
		in := &ratelimitv1.Policy{
			Name: name, Algorithm: ratelimitv1.Algorithm(algo), Rate: rate, Per: durationpb.New(time.Duration(perUS) * time.Microsecond),
			Burst: burst, Key: key, Enabled: enabled,
		}
		if route != "" || method != "" || dkey != "" {
			in.Match = &ratelimitv1.Match{}
			if route != "" {
				in.Match.Routes = []string{route}
			}
			if method != "" {
				in.Match.Methods = []string{method}
			}
			if dkey != "" {
				in.Match.Descriptors = []*ratelimitv1.Descriptor{{Key: dkey, Value: dval}}
			}
		}
		if costGlob != "" {
			in.Cost = map[string]uint32{costGlob: cost}
		}
		p, err := server.PolicyFromProto(in)
		if err != nil {
			if !errors.Is(err, limit.ErrInvalidParams) {
				t.Fatalf("rejection %v does not wrap ErrInvalidParams", err)
			}
			if policy.NameRE.MatchString(name) && cost < 1<<31 && algo >= 0 && algo <= 4 {
				t.Fatalf("rejected a well-formed wire policy: %v", err)
			}
			return
		}
		if !policy.NameRE.MatchString(p.Name) {
			t.Fatalf("accepted name %q", p.Name)
		}
		if p.IsEnabled() != enabled {
			t.Fatalf("enabled %v -> %v", enabled, p.IsEnabled())
		}
		out := server.PolicyToProto(p)
		if !proto.Equal(in, out) {
			t.Fatalf("round trip differs:\n in: %v\nout: %v\n(policy %+v)", in, out, p)
		}
		// A rejected-by-Validate policy must still be a pure value: rendering
		// it again yields the same message (no hidden state).
		if again := server.PolicyToProto(p); !proto.Equal(out, again) {
			t.Fatalf("PolicyToProto is not deterministic")
		}
	})
}

// FuzzDecisionToProto: every Decision maps to a CheckResponse whose numbers
// are clamped (never negative, never truncated below MaxUint32) and whose
// code mirrors Allowed.
func FuzzDecisionToProto(f *testing.F) {
	f.Add(true, 5, 10, int64(0), int64(1500), "api", "redis")
	f.Add(false, -1, -1, int64(-5), int64(-5), "deny", "degraded")
	f.Add(true, 1<<40, 1<<40, int64(1<<62), int64(1<<62), "", "")
	f.Fuzz(func(t *testing.T, allowed bool, remaining, lim int, retryNS, resetNS int64, pol, backend string) {
		d := limit.Decision{Allowed: allowed, Remaining: remaining, Limit: lim, RetryAfter: time.Duration(retryNS), ResetAfter: time.Duration(resetNS), Policy: pol, Backend: backend}
		r := server.DecisionToProto(d)
		if (r.GetCode() == ratelimitv1.Code_CODE_OK) != allowed {
			t.Fatalf("code %v for allowed=%v", r.GetCode(), allowed)
		}
		if int64(r.GetRemaining()) != clamp(int64(remaining)) || int64(r.GetLimit()) != clamp(int64(lim)) {
			t.Fatalf("remaining/limit %d/%d from %d/%d", r.GetRemaining(), r.GetLimit(), remaining, lim)
		}
		if r.GetRetryAfter().AsDuration() != max(d.RetryAfter, 0) || r.GetResetAfter().AsDuration() != max(d.ResetAfter, 0) {
			t.Fatalf("durations %v/%v from %v/%v", r.GetRetryAfter().AsDuration(), r.GetResetAfter().AsDuration(), d.RetryAfter, d.ResetAfter)
		}
		if r.GetDegraded() != (backend == limit.BackendDegraded) || r.GetPolicy() != pol || r.GetBackend() != backend {
			t.Fatalf("labels %+v from %+v", r, d)
		}
	})
}

func clamp(n int64) int64 {
	if n < 0 {
		return 0
	}
	if n > 1<<32-1 {
		return 1<<32 - 1
	}
	return n
}
