package httpapi_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/timestamppb"

	ratelimitv1 "github.com/crowbett/ratelimitd/gen/ratelimit/v1"
	"github.com/crowbett/ratelimitd/internal/admin"
	"github.com/crowbett/ratelimitd/internal/httpapi"
	"github.com/crowbett/ratelimitd/internal/interceptor"
	"github.com/crowbett/ratelimitd/internal/limit"
	"github.com/crowbett/ratelimitd/internal/policy"
	"github.com/crowbett/ratelimitd/internal/server"
)

var update = flag.Bool("update", false, "rewrite golden files")

// fixedNow keeps the fake store's decisions deterministic for the golden.
var fixedNow = time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC)

// resolver is a tiny stand-in for policy.Registry.Resolve: tenant "acme" has
// policies api (burst 10) and, for /v1/search*, search (burst 5, cost 10 on
// bulk); subject "banned" is deny-listed; tenant "nobody" is unknown.
type resolver struct{}

func (resolver) Resolve(tenant, subject string, _ []policy.Descriptor, route, _ string) ([]limit.Req, *limit.Decision, error) {
	if tenant != "acme" {
		return nil, nil, fmt.Errorf("%w: %s", limit.ErrUnknownTenant, tenant)
	}
	if subject == "banned" {
		return nil, &limit.Decision{Allowed: false, Policy: "deny", RetryAfter: time.Hour}, nil
	}
	api, _ := limit.NewParams(limit.GCRA, 100, time.Second, 10)
	reqs := []limit.Req{{Key: limit.Key(tenant, subject, "api"), Params: api, Cost: 1, Policy: "api"}}
	if strings.HasPrefix(route, "/v1/search") {
		search, _ := limit.NewParams(limit.TokenBucket, 10, time.Second, 5)
		cost := 1
		if strings.HasPrefix(route, "/v1/search/bulk") {
			cost = 10
		}
		reqs = append(reqs, limit.Req{Key: limit.Key(tenant, subject, "search"), Params: search, Cost: cost, Policy: "search"})
	}
	return reqs, nil, nil
}

// store denies any key whose subject contains "deny"; otherwise it allows
// with Remaining = burst - cost. RetryAfter is deliberately sub-millisecond
// plus a bit so the JSON ceil() is exercised.
type store struct {
	err error
}

func (s store) Apply(_ context.Context, reqs []limit.Req, _ time.Time) ([]limit.Decision, error) {
	if s.err != nil {
		return nil, s.err
	}
	out := make([]limit.Decision, len(reqs))
	for i, r := range reqs {
		d := limit.Decision{
			Allowed: true, Limit: int(r.Params.Burst), Remaining: int(r.Params.Burst) - r.Cost,
			ResetAfter: time.Duration(r.Cost)*time.Duration(r.Params.EmissionUS)*time.Microsecond + 1500*time.Microsecond,
			Policy:     r.Policy, Backend: limit.BackendRedis,
		}
		if strings.Contains(r.Key, "deny") {
			d.Allowed, d.Remaining, d.RetryAfter = false, 0, 2500*time.Microsecond
		}
		out[i] = d
	}
	return out, nil
}

func (s store) Peek(ctx context.Context, reqs []limit.Req, now time.Time) ([]limit.Decision, error) {
	return s.Apply(ctx, reqs, now)
}
func (store) Reset(context.Context, []string) error { return nil }
func (store) Name() string                          { return limit.BackendRedis }

func newHTTP(t *testing.T, st limit.Store, cfg server.Config) (http.Handler, *admin.Snapshot) {
	t.Helper()
	snap := admin.NewSnapshot()
	return httpapi.New(st, resolver{}, snap, slog.New(slog.DiscardHandler), cfg), snap
}

func post(t *testing.T, h http.Handler, body, contentType string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/v1/check", strings.NewReader(body))
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestCheckJSON(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		body       string
		ct         string
		st         limit.Store
		wantStatus int
		wantCode   string // problem.code on errors
		wantDetail string
		want       *httpapi.CheckResponse
	}{
		{
			name: "allowed", body: `{"tenant":"acme","subject":"k1"}`, wantStatus: http.StatusOK,
			want: &httpapi.CheckResponse{Allowed: true, Limit: 10, Remaining: 9, ResetAfterMS: 12, Policy: "api", Backend: "redis"},
		},
		{
			name: "denied is 200 with allowed=false; retry_after rounds up", body: `{"tenant":"acme","subject":"deny-me","route":"/v1/search"}`, wantStatus: http.StatusOK,
			want: &httpapi.CheckResponse{Allowed: false, Limit: 10, Remaining: 0, RetryAfterMS: 3, ResetAfterMS: 12, Policy: "api", Backend: "redis"},
		},
		{
			name: "cost multiplies", body: `{"tenant":"acme","subject":"k1","route":"/v1/search/bulk","cost":2}`, wantStatus: http.StatusOK,
			want: &httpapi.CheckResponse{Allowed: true, Limit: 5, Remaining: 0, ResetAfterMS: 2002, Policy: "search", Backend: "redis"},
		},
		{
			name: "deny list", body: `{"tenant":"acme","subject":"banned"}`, wantStatus: http.StatusOK,
			want: &httpapi.CheckResponse{Allowed: false, RetryAfterMS: 3_600_000, Policy: "deny"},
		},
		{name: "validation", body: `{"tenant":"","subject":"k1","cost":20000}`, wantStatus: http.StatusBadRequest, wantCode: "InvalidArgument", wantDetail: "tenant: required; cost: must be 1..10000"},
		{name: "unknown tenant", body: `{"tenant":"nobody","subject":"k1"}`, wantStatus: http.StatusNotFound, wantCode: "NotFound", wantDetail: "unknown tenant"},
		{name: "malformed json", body: `{"tenant":`, wantStatus: http.StatusBadRequest, wantCode: "InvalidArgument", wantDetail: "invalid JSON"},
		{name: "unknown field", body: `{"tenant":"acme","subject":"k1","bogus":1}`, wantStatus: http.StatusBadRequest, wantCode: "InvalidArgument", wantDetail: `unknown field "bogus"`},
		{name: "trailing garbage", body: `{"tenant":"acme","subject":"k1"} {}`, wantStatus: http.StatusBadRequest, wantCode: "InvalidArgument", wantDetail: "single object"},
		{name: "bad now", body: `{"tenant":"acme","subject":"k1","now":"yesterday"}`, wantStatus: http.StatusBadRequest, wantCode: "InvalidArgument", wantDetail: "now: must be RFC 3339"},
		{name: "wrong content type", body: `{}`, ct: "text/plain", wantStatus: http.StatusUnsupportedMediaType, wantCode: "InvalidArgument"},
		{name: "too large", body: `{"tenant":"` + strings.Repeat("x", httpapi.MaxBodyBytes) + `"}`, wantStatus: http.StatusRequestEntityTooLarge, wantCode: "InvalidArgument"},
		{name: "backend down", body: `{"tenant":"acme","subject":"k1"}`, st: store{err: limit.ErrBackendDown}, wantStatus: http.StatusServiceUnavailable, wantCode: "Unavailable"},
		{name: "internal", body: `{"tenant":"acme","subject":"k1"}`, st: store{err: errors.New("boom")}, wantStatus: http.StatusInternalServerError, wantCode: "Internal", wantDetail: "internal error"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			st := tt.st
			if st == nil {
				st = store{}
			}
			h, snap := newHTTP(t, st, server.Config{})
			ct := tt.ct
			if ct == "" {
				ct = "application/json"
			}
			rec := post(t, h, tt.body, ct)
			if rec.Code != tt.wantStatus {
				t.Fatalf("status = %d, body %s", rec.Code, rec.Body.String())
			}
			if rec.Header().Get("Cache-Control") != "no-store" {
				t.Errorf("Cache-Control = %q", rec.Header().Get("Cache-Control"))
			}
			if tt.want != nil {
				if got := rec.Header().Get("Content-Type"); got != "application/json" {
					t.Errorf("Content-Type = %q", got)
				}
				var got httpapi.CheckResponse
				if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
					t.Fatal(err)
				}
				if got != *tt.want {
					t.Errorf("got %+v, want %+v", got, *tt.want)
				}
				if len(snap.Decisions()) != 1 {
					t.Errorf("RecordDecision = %+v", snap.Decisions())
				}
				return
			}
			if got := rec.Header().Get("Content-Type"); got != httpapi.ProblemContentType {
				t.Errorf("Content-Type = %q", got)
			}
			var p httpapi.Problem
			if err := json.Unmarshal(rec.Body.Bytes(), &p); err != nil {
				t.Fatalf("problem body %q: %v", rec.Body.String(), err)
			}
			if p.Status != tt.wantStatus || p.Type != "about:blank" || p.Title != http.StatusText(tt.wantStatus) || p.Instance != "/v1/check" || p.Code != tt.wantCode {
				t.Errorf("problem = %+v", p)
			}
			if tt.wantDetail != "" && !strings.Contains(p.Detail, tt.wantDetail) {
				t.Errorf("detail = %q, want %q", p.Detail, tt.wantDetail)
			}
			if strings.Contains(p.Detail, "boom") {
				t.Errorf("internal error leaked: %q", p.Detail)
			}
			if len(snap.Decisions()) != 0 {
				t.Error("RecordDecision called on error")
			}
		})
	}
}

func TestRoutesAreProblems(t *testing.T) {
	t.Parallel()
	h, _ := newHTTP(t, store{}, server.Config{})
	for _, tt := range []struct {
		method, path string
		want         int
	}{
		{http.MethodGet, "/v1/check", http.StatusMethodNotAllowed},
		{http.MethodPost, "/v1/nope", http.StatusNotFound},
		{http.MethodGet, "/", http.StatusNotFound},
	} {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(tt.method, tt.path, http.NoBody))
		if rec.Code != tt.want || rec.Header().Get("Content-Type") != httpapi.ProblemContentType {
			t.Errorf("%s %s = %d %q", tt.method, tt.path, rec.Code, rec.Header().Get("Content-Type"))
		}
		if tt.want == http.StatusMethodNotAllowed && rec.Header().Get("Allow") != http.MethodPost {
			t.Errorf("Allow = %q", rec.Header().Get("Allow"))
		}
	}
}

func TestHTTPStatusTable(t *testing.T) {
	t.Parallel()
	if httpapi.HTTPStatus(0) != http.StatusOK || httpapi.HTTPStatus(3) != http.StatusBadRequest || httpapi.HTTPStatus(5) != http.StatusNotFound ||
		httpapi.HTTPStatus(9) != http.StatusUnprocessableEntity || httpapi.HTTPStatus(14) != http.StatusServiceUnavailable ||
		httpapi.HTTPStatus(13) != http.StatusInternalServerError || httpapi.HTTPStatus(2) != http.StatusInternalServerError {
		t.Error("HTTPStatus table drifted")
	}
}

// TestGoldenBothSurfaces sends the same requests through the gRPC Check
// (bufconn, production interceptors) and POST /v1/check, normalises both
// answers to the JSON shape and compares them to each other and to
// testdata/check.golden.json.
func TestGoldenBothSurfaces(t *testing.T) {
	t.Parallel()
	st := store{}
	snap := admin.NewSnapshot()
	logger := slog.New(slog.DiscardHandler)
	cfg := server.Config{TrustClientClock: true}

	h := httpapi.New(st, resolver{}, snap, logger, cfg)
	srv := grpc.NewServer(grpc.ChainUnaryInterceptor(interceptor.UnaryRecover(logger), interceptor.UnaryRequestID(), interceptor.UnaryLogging(logger)))
	server.NewCheckServer(st, resolver{}, snap, logger, cfg).Register(srv)
	lis := bufconn.Listen(1 << 20)
	go func() { _ = srv.Serve(lis) }()
	conn, err := grpc.NewClient("passthrough:///bufconn",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close(); srv.Stop() })
	client := ratelimitv1.NewRateLimitServiceClient(conn)

	cases := []httpapi.CheckRequest{
		{Tenant: "acme", Subject: "k1", Now: fixedNow.Format(time.RFC3339Nano)},
		{Tenant: "acme", Subject: "k1", Route: "/v1/search/bulk/x", Method: "POST", Cost: 2, Descriptors: []httpapi.Descriptor{{Key: "apikey", Value: "abc"}}},
		{Tenant: "acme", Subject: "deny-me", Route: "/v1/search"},
		{Tenant: "acme", Subject: "banned"},
	}
	type row struct {
		Request httpapi.CheckRequest  `json:"request"`
		GRPC    httpapi.CheckResponse `json:"grpc"`
		HTTP    httpapi.CheckResponse `json:"http"`
	}
	rows := make([]row, 0, len(cases))
	for _, c := range cases {
		body, _ := json.Marshal(c)
		rec := post(t, h, string(body), "application/json")
		if rec.Code != http.StatusOK {
			t.Fatalf("http %+v: %d %s", c, rec.Code, rec.Body.String())
		}
		var viaHTTP httpapi.CheckResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &viaHTTP); err != nil {
			t.Fatal(err)
		}

		req := &ratelimitv1.CheckRequest{Tenant: c.Tenant, Subject: c.Subject, Route: c.Route, Method: c.Method, Cost: uint32(c.Cost)}
		for _, d := range c.Descriptors {
			req.Descriptors = append(req.Descriptors, &ratelimitv1.Descriptor{Key: d.Key, Value: d.Value})
		}
		if c.Now != "" {
			req.Now = timestamppb.New(fixedNow)
		}
		resp, err := client.Check(t.Context(), req)
		if err != nil {
			t.Fatalf("grpc %+v: %v", c, err)
		}
		viaGRPC := fromProto(resp)
		if viaGRPC != viaHTTP {
			t.Errorf("surfaces differ for %+v:\n grpc %+v\n http %+v", c, viaGRPC, viaHTTP)
		}
		rows = append(rows, row{Request: c, GRPC: viaGRPC, HTTP: viaHTTP})
	}

	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetIndent("", "  ")
	if err := enc.Encode(rows); err != nil {
		t.Fatal(err)
	}
	golden := filepath.Join("testdata", "check.golden.json")
	if *update {
		if err := os.MkdirAll("testdata", 0o750); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(golden, buf.Bytes(), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(golden)
	if err != nil {
		t.Fatalf("read golden (run with -update to create): %v", err)
	}
	want = bytes.ReplaceAll(want, []byte("\r\n"), []byte("\n"))
	if !bytes.Equal(buf.Bytes(), want) {
		t.Errorf("golden differs:\n--- got ---\n%s\n--- want ---\n%s", buf.String(), want)
	}
}

// fromProto normalises the gRPC answer the way the JSON adapter does, using
// the same rounding rule (ceil to whole milliseconds).
func fromProto(r *ratelimitv1.CheckResponse) httpapi.CheckResponse {
	return httpapi.CheckResponse{
		Allowed:      r.GetCode() == ratelimitv1.Code_CODE_OK,
		Limit:        int(r.GetLimit()),
		Remaining:    int(r.GetRemaining()),
		RetryAfterMS: ceilMS(r.GetRetryAfter().AsDuration()),
		ResetAfterMS: ceilMS(r.GetResetAfter().AsDuration()),
		Policy:       r.GetPolicy(),
		Backend:      r.GetBackend(),
		Degraded:     r.GetDegraded(),
	}
}

func ceilMS(d time.Duration) int64 {
	if d <= 0 {
		return 0
	}
	return int64((d + time.Millisecond - 1) / time.Millisecond)
}
