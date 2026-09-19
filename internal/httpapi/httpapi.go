// Package httpapi is the JSON adapter: POST /v1/check on HTTP_ADDR, using the
// same validation path and converters as the gRPC Check so both surfaces
// return identical decisions (a golden test proves it).
//
// Owner: workstream C (service edge).
package httpapi

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/crowbett/ratelimitd/internal/admin"
	"github.com/crowbett/ratelimitd/internal/limit"
	"github.com/crowbett/ratelimitd/internal/policy"
	"github.com/crowbett/ratelimitd/internal/server"
)

// MaxBodyBytes bounds a POST /v1/check body (16 descriptors of 256+256 bytes
// fit with room to spare).
const MaxBodyBytes = 64 << 10

// ProblemContentType is the RFC 9457 media type used for every error.
const ProblemContentType = "application/problem+json"

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

// Problem is the RFC 9457 error body.
type Problem struct {
	Type     string `json:"type"`
	Title    string `json:"title"`
	Status   int    `json:"status"`
	Detail   string `json:"detail,omitempty"`
	Instance string `json:"instance,omitempty"`
	// Code is the gRPC code name the same failure gets on the gRPC surface.
	Code string `json:"code,omitempty"`
}

// New returns the /v1 mux: POST /v1/check. Errors are RFC 9457
// application/problem+json; unknown routes and methods are problems too.
func New(store limit.Store, reg server.Resolver, snap *admin.Snapshot, log *slog.Logger, cfg server.Config) http.Handler {
	if log == nil {
		log = slog.Default()
	}
	h := &handler{check: server.NewCheckServer(store, reg, snap, log, cfg), log: log}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/check", h.serveCheck)
	mux.HandleFunc("/v1/check", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Allow", http.MethodPost)
		writeProblem(w, r, http.StatusMethodNotAllowed, codes.Unimplemented, "method "+r.Method+" is not allowed; use POST")
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		writeProblem(w, r, http.StatusNotFound, codes.NotFound, "no such route")
	})
	return mux
}

type handler struct {
	check *server.CheckServer
	log   *slog.Logger
}

func (h *handler) serveCheck(w http.ResponseWriter, r *http.Request) {
	if ct := r.Header.Get("Content-Type"); ct != "" && !strings.HasPrefix(strings.ToLower(ct), "application/json") {
		writeProblem(w, r, http.StatusUnsupportedMediaType, codes.InvalidArgument, "Content-Type must be application/json")
		return
	}
	var body CheckRequest
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, MaxBodyBytes))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&body); err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			writeProblem(w, r, http.StatusRequestEntityTooLarge, codes.InvalidArgument, "body exceeds "+tooBig.Error())
			return
		}
		writeProblem(w, r, http.StatusBadRequest, codes.InvalidArgument, "invalid JSON: "+err.Error())
		return
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		writeProblem(w, r, http.StatusBadRequest, codes.InvalidArgument, "invalid JSON: a single object is expected")
		return
	}
	in, err := ToInput(body)
	if err != nil {
		writeProblem(w, r, http.StatusBadRequest, codes.InvalidArgument, err.Error())
		return
	}
	d, err := h.check.Decide(r.Context(), in)
	if err != nil {
		st := status.Convert(err)
		writeProblem(w, r, HTTPStatus(st.Code()), st.Code(), st.Message())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(w).Encode(FromDecision(d))
}

// ToInput converts the JSON body to the shared Input; "now" must be RFC 3339
// when present.
func ToInput(body CheckRequest) (server.Input, error) {
	in := server.Input{Tenant: body.Tenant, Subject: body.Subject, Route: body.Route, Method: body.Method, Cost: body.Cost}
	if len(body.Descriptors) > 0 {
		in.Descriptors = make([]policy.Descriptor, 0, len(body.Descriptors))
		for _, d := range body.Descriptors {
			in.Descriptors = append(in.Descriptors, policy.Descriptor{Key: d.Key, Value: d.Value})
		}
	}
	if body.Now != "" {
		t, err := time.Parse(time.RFC3339Nano, body.Now)
		if err != nil {
			return server.Input{}, errors.New("invalid request: now: must be RFC 3339")
		}
		in.Now = t
	}
	return in, nil
}

// FromDecision maps a Decision to the JSON shape: the same clamping as
// server.DecisionToProto, durations as integer milliseconds rounded up so a
// sub-millisecond retry never reads as 0.
func FromDecision(d limit.Decision) CheckResponse {
	return CheckResponse{
		Allowed:      d.Allowed,
		Limit:        max(d.Limit, 0),
		Remaining:    max(d.Remaining, 0),
		RetryAfterMS: ceilMS(d.RetryAfter),
		ResetAfterMS: ceilMS(d.ResetAfter),
		Policy:       d.Policy,
		Backend:      d.Backend,
		Degraded:     d.Backend == limit.BackendDegraded,
	}
}

func ceilMS(d time.Duration) int64 {
	if d <= 0 {
		return 0
	}
	return int64((d + time.Millisecond - 1) / time.Millisecond)
}

// HTTPStatus maps the gRPC codes server.ToStatus produces to HTTP statuses.
func HTTPStatus(c codes.Code) int {
	switch c {
	case codes.OK:
		return http.StatusOK
	case codes.InvalidArgument:
		return http.StatusBadRequest
	case codes.NotFound:
		return http.StatusNotFound
	case codes.FailedPrecondition:
		return http.StatusUnprocessableEntity
	case codes.Unavailable:
		return http.StatusServiceUnavailable
	case codes.DeadlineExceeded:
		return http.StatusGatewayTimeout
	case codes.Canceled:
		return 499 // client closed request (nginx convention)
	case codes.Unimplemented:
		return http.StatusNotImplemented
	case codes.ResourceExhausted:
		return http.StatusTooManyRequests
	case codes.PermissionDenied:
		return http.StatusForbidden
	case codes.Unauthenticated:
		return http.StatusUnauthorized
	default:
		return http.StatusInternalServerError
	}
}

func writeProblem(w http.ResponseWriter, r *http.Request, httpStatus int, code codes.Code, detail string) {
	w.Header().Set("Content-Type", ProblemContentType)
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(httpStatus)
	_ = json.NewEncoder(w).Encode(Problem{
		Type:     "about:blank",
		Title:    http.StatusText(httpStatus),
		Status:   httpStatus,
		Detail:   detail,
		Instance: r.URL.Path,
		Code:     code.String(),
	})
}
