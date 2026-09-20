package httpapi_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/crowbett/ratelimitd/internal/httpapi"
	"github.com/crowbett/ratelimitd/internal/server"
)

// FuzzServeCheck posts arbitrary bytes with an arbitrary Content-Type at
// POST /v1/check: the handler never panics, answers one of the documented
// statuses with a JSON body of the matching shape (CheckResponse on 200,
// RFC 9457 Problem otherwise, both no-store), caps the body at MaxBodyBytes
// (413, never a read past it) and never echoes raw request bytes that are
// not printable ASCII into a problem detail.
func FuzzServeCheck(f *testing.F) {
	f.Add(`{"tenant":"acme","subject":"k1"}`, "application/json")
	f.Add(`{"tenant":"acme","subject":"k1","route":"/v1/search/bulk","cost":2,"now":"2026-09-19T12:00:00Z"}`, "application/json; charset=utf-8")
	f.Add(`{"tenant":"acme","subject":"deny-me"}`, "")
	f.Add(`{"tenant":"nobody","subject":"k1"}`, "application/json")
	f.Add(`{"tenant":"acme","subject":"banned"}`, "application/json")
	f.Add(`{"tenant":"acme","subject":"k1","extra":1}`, "application/json")
	f.Add(`{"tenant":"acme","subject":"k1"} {"again":true}`, "application/json")
	f.Add(`{"tenant":"acme","subject":"k1","now":"yesterday"}`, "application/json")
	f.Add(`{"tenant":"acme","subject":"k1","cost":-1}`, "application/json")
	f.Add(`{"tenant":"acme","subject":"k1","descriptors":[{"key":"ip","value":"1.2.3.4"}]}`, "application/json")
	f.Add(`{"tenant":"acme","subject":"k1","descriptors":[{"key":"\u0000"}]}`, "application/json")
	f.Add(``, "application/json")
	f.Add(`[]`, "application/json")
	f.Add(`{"tenant":1}`, "application/json")
	f.Add(`{"tenant":"acme","subject":"k1"}`, "text/plain")
	f.Add(`{"tenant":"acme","subject":"k1"}`, "APPLICATION/JSON")
	f.Add(`{"tenant":"acme","subject":"`+strings.Repeat("s", 300)+`"}`, "application/json")
	f.Add(`{"tenant":"acme","subject":"k1","pad":"`+strings.Repeat("x", httpapi.MaxBodyBytes)+`"}`, "application/json")
	f.Add("\xff\xfe{", "application/json")
	f.Fuzz(func(t *testing.T, body, contentType string) {
		if len(body) > 2*httpapi.MaxBodyBytes || len(contentType) > 256 {
			t.Skip()
		}
		if strings.ContainsAny(contentType, "\r\n") {
			t.Skip() // not a header value; net/http would reject it on the wire
		}
		h, _ := newHTTP(t, store{}, server.Config{TrustClientClock: true})
		rec := post(t, h, body, contentType)
		if cc := rec.Header().Get("Cache-Control"); cc != "no-store" {
			t.Fatalf("Cache-Control = %q", cc)
		}
		switch rec.Code {
		case http.StatusOK:
			if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
				t.Fatalf("200 with Content-Type %q", ct)
			}
			var resp httpapi.CheckResponse
			if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
				t.Fatalf("200 body is not a CheckResponse: %v: %s", err, rec.Body.Bytes())
			}
			if resp.Limit < 0 || resp.Remaining < 0 || resp.RetryAfterMS < 0 || resp.ResetAfterMS < 0 {
				t.Fatalf("negative number in %+v", resp)
			}
			if resp.Allowed && resp.RetryAfterMS != 0 {
				t.Fatalf("allowed with retry_after %d", resp.RetryAfterMS)
			}
			if resp.Degraded != (resp.Backend == "degraded") {
				t.Fatalf("degraded flag disagrees with backend in %+v", resp)
			}
		case http.StatusBadRequest, http.StatusRequestEntityTooLarge, http.StatusUnsupportedMediaType, http.StatusNotFound, http.StatusUnprocessableEntity:
			if ct := rec.Header().Get("Content-Type"); ct != httpapi.ProblemContentType {
				t.Fatalf("%d with Content-Type %q", rec.Code, ct)
			}
			var prob httpapi.Problem
			if err := json.Unmarshal(rec.Body.Bytes(), &prob); err != nil {
				t.Fatalf("%d body is not a Problem: %v: %s", rec.Code, err, rec.Body.Bytes())
			}
			if prob.Status != rec.Code || prob.Title != http.StatusText(rec.Code) || prob.Instance != "/v1/check" || prob.Code == "" {
				t.Fatalf("problem %+v does not match status %d", prob, rec.Code)
			}
			for i := 0; i < len(prob.Detail); i++ {
				if c := prob.Detail[i]; c < 0x20 || c == 0x7f {
					t.Fatalf("problem detail echoes control byte %#x: %q", c, prob.Detail)
				}
			}
			if rec.Code == http.StatusRequestEntityTooLarge && len(body) <= httpapi.MaxBodyBytes {
				t.Fatalf("413 for a %d-byte body under the %d cap", len(body), httpapi.MaxBodyBytes)
			}
			if rec.Code == http.StatusUnsupportedMediaType && (contentType == "" || strings.HasPrefix(strings.ToLower(contentType), "application/json")) {
				t.Fatalf("415 for Content-Type %q", contentType)
			}
		default:
			t.Fatalf("unexpected status %d: %s", rec.Code, rec.Body.Bytes())
		}
	})
}

// FuzzToInput: the JSON body -> server.Input conversion never panics; "now"
// is accepted only as RFC 3339 and comes back as the same instant; the other
// fields are copied verbatim (Validate, not ToInput, is the bounds check).
func FuzzToInput(f *testing.F) {
	f.Add("acme", "k1", "/r", "GET", "k", "v", 3, "2026-09-19T12:00:00.123456789Z")
	f.Add("", "", "", "", "", "", 0, "")
	f.Add("a", "b", "", "", "", "", -1, "2026-09-19T12:00:00+02:00")
	f.Add("a", "b", "", "", "", "", 1, "2026-09-19 12:00:00")
	f.Add("a", "b", "", "", "", "", 1, "0001-01-01T00:00:00Z")
	f.Add("a", "b", "", "", "", "", 1, "9999-12-31T23:59:59Z")
	f.Fuzz(func(t *testing.T, tenant, subject, route, method, dkey, dval string, cost int, now string) {
		body := httpapi.CheckRequest{Tenant: tenant, Subject: subject, Route: route, Method: method, Cost: cost, Now: now}
		if dkey != "" || dval != "" {
			body.Descriptors = []httpapi.Descriptor{{Key: dkey, Value: dval}}
		}
		in, err := httpapi.ToInput(body)
		if err != nil {
			if now == "" {
				t.Fatalf("error %v without a now", err)
			}
			if _, perr := time.Parse(time.RFC3339Nano, now); perr == nil {
				t.Fatalf("rejected RFC 3339 now %q: %v", now, err)
			}
			if in.Tenant != "" || in.Subject != "" || in.Descriptors != nil || !in.Now.IsZero() {
				t.Fatalf("error with a non-zero Input %+v", in)
			}
			return
		}
		if in.Tenant != tenant || in.Subject != subject || in.Route != route || in.Method != method || in.Cost != cost {
			t.Fatalf("fields not copied: %+v from %+v", in, body)
		}
		if len(in.Descriptors) != len(body.Descriptors) || (len(in.Descriptors) == 1 && (in.Descriptors[0].Key != dkey || in.Descriptors[0].Value != dval)) {
			t.Fatalf("descriptors %+v from %+v", in.Descriptors, body.Descriptors)
		}
		if now == "" {
			if !in.Now.IsZero() {
				t.Fatalf("now set without input: %v", in.Now)
			}
			return
		}
		want, perr := time.Parse(time.RFC3339Nano, now)
		if perr != nil {
			t.Fatalf("accepted now %q that RFC 3339 rejects: %v", now, perr)
		}
		if !in.Now.Equal(want) {
			t.Fatalf("now %v, want %v", in.Now, want)
		}
	})
}
