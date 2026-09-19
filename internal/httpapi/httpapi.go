// Package httpapi is the JSON adapter: POST /v1/check on HTTP_ADDR, using the
// same validation path and converters as the gRPC Check so both surfaces
// return identical decisions (a golden test proves it).
//
// Owner: workstream C (service edge).
package httpapi

import (
	"log/slog"
	"net/http"

	"github.com/crowbett/ratelimitd/internal/admin"
	"github.com/crowbett/ratelimitd/internal/limit"
	"github.com/crowbett/ratelimitd/internal/policy"
	"github.com/crowbett/ratelimitd/internal/server"
)

// Descriptor is the JSON key/value pair.
type Descriptor struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// CheckRequest is the POST /v1/check body (same fields as ratelimit.v1.CheckRequest;
// "now" is RFC 3339 and honoured only with TRUST_CLIENT_CLOCK).
type CheckRequest struct {
	Tenant      string       `json:"tenant"`
	Subject     string       `json:"subject"`
	Descriptors []Descriptor `json:"descriptors,omitempty"`
	Route       string       `json:"route,omitempty"`
	Method      string       `json:"method,omitempty"`
	Cost        int          `json:"cost,omitempty"`
	Now         string       `json:"now,omitempty"`
}

// CheckResponse is the JSON decision. Durations are integer milliseconds.
type CheckResponse struct {
	Allowed      bool   `json:"allowed"`
	Limit        int    `json:"limit"`
	Remaining    int    `json:"remaining"`
	RetryAfterMS int64  `json:"retry_after_ms"`
	ResetAfterMS int64  `json:"reset_after_ms"`
	Policy       string `json:"policy"`
	Backend      string `json:"backend"`
	Degraded     bool   `json:"degraded"`
}

// New returns the /v1 mux: POST /v1/check. Errors are RFC 9457
// application/problem+json. STUB: 501 on every route.
func New(_ limit.Store, _ *policy.Registry, _ *admin.Snapshot, _ *slog.Logger, _ server.Config) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/check", func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "stub: httpapi.Check", http.StatusNotImplemented)
	})
	return mux
}
