package ratelimit

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// fakeClient scripts Check; Close is a no-op.
type fakeClient struct {
	check func(ctx context.Context, req Request) (Result, error)
	calls int
	last  Request
}

func (f *fakeClient) Check(ctx context.Context, req Request) (Result, error) {
	f.calls++
	f.last = req
	return f.check(ctx, req)
}

func (f *fakeClient) Close() error { return nil }

func allowClient(res Result) *fakeClient {
	res.Allowed = true
	return &fakeClient{check: func(context.Context, Request) (Result, error) { return res, nil }}
}

func denyClient(res Result) *fakeClient {
	res.Allowed = false
	return &fakeClient{check: func(context.Context, Request) (Result, error) { return res, nil }}
}

func errClient(err error) *fakeClient {
	return &fakeClient{check: func(context.Context, Request) (Result, error) { return Result{}, err }}
}

func keyFromHeader(r *http.Request) Request {
	return Request{Tenant: "acme", Subject: r.Header.Get("X-API-Key"), Route: r.URL.Path, Method: r.Method}
}

func okHandler(t *testing.T) http.Handler {
	t.Helper()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, ok := ResultFromContext(r.Context()); !ok {
			t.Error("ResultFromContext: no result on an allowed request")
		}
		w.WriteHeader(http.StatusNoContent)
	})
}

func doMW(t *testing.T, c Client, next http.Handler, opts ...MWOption) *httptest.ResponseRecorder {
	t.Helper()
	h := Middleware(c, keyFromHeader, opts...)(next)
	req := httptest.NewRequest(http.MethodGet, "/v1/things", http.NoBody)
	req.Header.Set("X-API-Key", "k1")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestMiddlewareHeaderRounding(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name       string
		res        Result
		wantLimit  string
		wantRemain string
		wantReset  string
		wantRetry  string // "" = header absent (allowed)
		wantStatus int
	}{
		{"allowed zero reset", Result{Limit: 100, Remaining: 99}, "100", "99", "0", "", http.StatusNoContent},
		{"allowed 1ns rounds up", Result{Limit: 100, Remaining: 1, ResetAfter: time.Nanosecond}, "100", "1", "1", "", http.StatusNoContent},
		{"allowed exact second", Result{Limit: 100, Remaining: 0, ResetAfter: time.Second}, "100", "0", "1", "", http.StatusNoContent},
		{"allowed 1s+1ns", Result{Limit: 100, Remaining: 5, ResetAfter: time.Second + time.Nanosecond}, "100", "5", "2", "", http.StatusNoContent},
		{"allowed 1500ms", Result{Limit: 7, Remaining: 3, ResetAfter: 1500 * time.Millisecond}, "7", "3", "2", "", http.StatusNoContent},
		{"allowed negative remaining clamps", Result{Limit: 7, Remaining: -2}, "7", "0", "0", "", http.StatusNoContent},
		{"denied zero retry floors at 1", Result{Allowed: false, Limit: 10, Remaining: 0, RetryAfter: 0, ResetAfter: 900 * time.Millisecond}, "10", "0", "1", "1", http.StatusTooManyRequests},
		{"denied 200ms", Result{Allowed: false, Limit: 10, RetryAfter: 200 * time.Millisecond, ResetAfter: 10 * time.Second}, "10", "0", "10", "1", http.StatusTooManyRequests},
		{"denied exact second", Result{Allowed: false, Limit: 10, RetryAfter: time.Second, ResetAfter: 61 * time.Second}, "10", "0", "61", "1", http.StatusTooManyRequests},
		{"denied 1200ms", Result{Allowed: false, Limit: 10, RetryAfter: 1200 * time.Millisecond, ResetAfter: 59*time.Second + 999*time.Millisecond}, "10", "0", "60", "2", http.StatusTooManyRequests},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var c *fakeClient
			if tc.wantStatus == http.StatusTooManyRequests {
				c = denyClient(tc.res)
			} else {
				c = allowClient(tc.res)
			}
			rec := doMW(t, c, okHandler(t))
			if rec.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d", rec.Code, tc.wantStatus)
			}
			h := rec.Header()
			if got := h.Get(HeaderLimit); got != tc.wantLimit {
				t.Errorf("%s = %q, want %q", HeaderLimit, got, tc.wantLimit)
			}
			if got := h.Get(HeaderRemaining); got != tc.wantRemain {
				t.Errorf("%s = %q, want %q", HeaderRemaining, got, tc.wantRemain)
			}
			if got := h.Get(HeaderReset); got != tc.wantReset {
				t.Errorf("%s = %q, want %q", HeaderReset, got, tc.wantReset)
			}
			if got := h.Get(HeaderRetryAfter); got != tc.wantRetry {
				t.Errorf("%s = %q, want %q", HeaderRetryAfter, got, tc.wantRetry)
			}
		})
	}
}

func TestMiddlewareDenyBody(t *testing.T) {
	t.Parallel()
	c := denyClient(Result{Limit: 10, RetryAfter: 2500 * time.Millisecond, Policy: "api"})
	rec := doMW(t, c, okHandler(t))
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); ct != ProblemJSON {
		t.Fatalf("Content-Type = %q, want %q", ct, ProblemJSON)
	}
	var p Problem
	if err := json.NewDecoder(rec.Body).Decode(&p); err != nil {
		t.Fatalf("decode: %v", err)
	}
	want := Problem{
		Type: "about:blank", Title: "Too Many Requests", Status: 429, Instance: "/v1/things",
		Policy: "api", RetryAfter: 3, Detail: `rate limited by policy "api"; retry after 3s`,
	}
	if p != want {
		t.Fatalf("problem = %+v, want %+v", p, want)
	}
}

func TestMiddlewareOnError(t *testing.T) {
	t.Parallel()
	boom := errors.New("daemon unreachable")
	cases := []struct {
		name       string
		opts       []MWOption
		wantStatus int
		wantRetry  string
		wantLog    string
	}{
		{"default fail-open", nil, http.StatusNoContent, "", "fail_open"},
		{"explicit fail-open", []MWOption{WithOnError(FailOpen)}, http.StatusNoContent, "", "fail_open"},
		{"fail-closed", []MWOption{WithOnError(FailClosed)}, http.StatusTooManyRequests, "1", "fail_closed"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var logBuf strings.Builder
			log := slog.New(slog.NewTextHandler(&logBuf, nil))
			next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if _, ok := ResultFromContext(r.Context()); ok {
					t.Error("ResultFromContext: result present after a client error")
				}
				w.WriteHeader(http.StatusNoContent)
			})
			rec := doMW(t, errClient(boom), next, append(tc.opts, WithMWLogger(log))...)
			if rec.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d", rec.Code, tc.wantStatus)
			}
			if got := rec.Header().Get(HeaderRetryAfter); got != tc.wantRetry {
				t.Errorf("Retry-After = %q, want %q", got, tc.wantRetry)
			}
			if got := rec.Header().Get(HeaderLimit); got != "" {
				t.Errorf("RateLimit-Limit = %q on a client error, want none", got)
			}
			if !strings.Contains(logBuf.String(), tc.wantLog) || !strings.Contains(logBuf.String(), boom.Error()) {
				t.Errorf("log = %q, want %q and %q", logBuf.String(), tc.wantLog, boom.Error())
			}
			if tc.wantStatus == http.StatusTooManyRequests {
				body, _ := io.ReadAll(rec.Body)
				if !strings.Contains(string(body), "rate limiter unavailable") {
					t.Errorf("body = %s, want the unavailable problem", body)
				}
			}
		})
	}
}

func TestMiddlewareOptions(t *testing.T) {
	t.Parallel()
	t.Run("headers off", func(t *testing.T) {
		t.Parallel()
		rec := doMW(t, denyClient(Result{Limit: 10, RetryAfter: time.Second}), okHandler(t), WithHeaders(false))
		if rec.Code != http.StatusTooManyRequests {
			t.Fatalf("status = %d", rec.Code)
		}
		for _, h := range []string{HeaderLimit, HeaderRemaining, HeaderReset, HeaderRetryAfter} {
			if got := rec.Header().Get(h); got != "" {
				t.Errorf("%s = %q, want none", h, got)
			}
		}
	})
	t.Run("custom deny handler sees the result", func(t *testing.T) {
		t.Parallel()
		deny := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			res, ok := ResultFromContext(r.Context())
			if !ok || res.Policy != "search" {
				t.Errorf("deny handler result = %+v ok=%v", res, ok)
			}
			w.WriteHeader(http.StatusServiceUnavailable)
		})
		rec := doMW(t, denyClient(Result{Limit: 1, RetryAfter: 3 * time.Second, Policy: "search"}), okHandler(t), WithDenyHandler(deny))
		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("status = %d, want 503 from the custom handler", rec.Code)
		}
		if got := rec.Header().Get(HeaderRetryAfter); got != "3" {
			t.Errorf("Retry-After = %q, want 3 (set before the custom handler runs)", got)
		}
	})
	t.Run("cost function and key func reach the client", func(t *testing.T) {
		t.Parallel()
		c := allowClient(Result{Limit: 1, Remaining: 1})
		rec := doMW(t, c, okHandler(t), WithCost(func(r *http.Request) int { return len(r.URL.Path) }))
		if rec.Code != http.StatusNoContent {
			t.Fatalf("status = %d", rec.Code)
		}
		want := Request{Tenant: "acme", Subject: "k1", Route: "/v1/things", Method: http.MethodGet, Cost: len("/v1/things")}
		if c.last.Tenant != want.Tenant || c.last.Subject != want.Subject || c.last.Route != want.Route ||
			c.last.Method != want.Method || c.last.Cost != want.Cost {
			t.Errorf("client saw %+v, want %+v", c.last, want)
		}
		if c.calls != 1 {
			t.Errorf("calls = %d, want 1", c.calls)
		}
	})
}

func TestCeilSeconds(t *testing.T) {
	t.Parallel()
	cases := []struct {
		d     time.Duration
		ceil  int64
		retry int64
	}{
		{-time.Second, 0, 1},
		{0, 0, 1},
		{time.Nanosecond, 1, 1},
		{999 * time.Millisecond, 1, 1},
		{time.Second, 1, 1},
		{time.Second + time.Nanosecond, 2, 2},
		{90 * time.Second, 90, 90},
	}
	for _, tc := range cases {
		if got := ceilSeconds(tc.d); got != tc.ceil {
			t.Errorf("ceilSeconds(%s) = %d, want %d", tc.d, got, tc.ceil)
		}
		if got := retryAfterSeconds(tc.d); got != tc.retry {
			t.Errorf("retryAfterSeconds(%s) = %d, want %d", tc.d, got, tc.retry)
		}
	}
}
