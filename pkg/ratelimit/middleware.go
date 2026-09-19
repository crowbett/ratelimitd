package ratelimit

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"
	"time"
)

// Header names set by Middleware (draft-ietf-httpapi-ratelimit-headers) and
// the RFC 9457 media type of the default 429 body.
const (
	HeaderLimit      = "RateLimit-Limit"
	HeaderRemaining  = "RateLimit-Remaining"
	HeaderReset      = "RateLimit-Reset"
	HeaderRetryAfter = "Retry-After"
	ProblemJSON      = "application/problem+json"
)

// KeyFunc derives the Request from an incoming HTTP request (tenant, subject
// such as an API key or client IP, route, method, descriptors).
type KeyFunc func(*http.Request) Request

// Problem is the RFC 9457 body of the default 429 response.
type Problem struct {
	Type       string `json:"type"`
	Title      string `json:"title"`
	Status     int    `json:"status"`
	Detail     string `json:"detail"`
	Instance   string `json:"instance,omitempty"`
	Policy     string `json:"policy,omitempty"`
	RetryAfter int64  `json:"retry_after,omitempty"` // seconds, rounded up, min 1
}

type resultKey struct{}

// ResultFromContext returns the Result Middleware attached to the request
// (available to the next handler and to the WithDenyHandler). ok is false
// when the check errored or Middleware did not run.
func ResultFromContext(ctx context.Context) (res Result, ok bool) {
	res, ok = ctx.Value(resultKey{}).(Result)
	return res, ok
}

// Middleware checks every request through c. On every response it sets
// RateLimit-Limit, RateLimit-Remaining and RateLimit-Reset (seconds, rounded
// up); on a denial it sets Retry-After (seconds, rounded up, min 1) and
// answers 429 application/problem+json (or the WithDenyHandler). A client
// error applies WithOnError (default FailOpen, logged).
func Middleware(c Client, kf KeyFunc, opts ...MWOption) func(http.Handler) http.Handler {
	o := mwOptions{headers: true, log: slog.Default()}
	for _, opt := range opts {
		opt(&o)
	}
	if o.log == nil {
		o.log = slog.Default()
	}
	if o.deny == nil {
		o.deny = http.HandlerFunc(denyProblem)
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			req := kf(r)
			if o.cost != nil {
				req.Cost = o.cost(r)
			}
			res, err := c.Check(r.Context(), req)
			if err != nil {
				o.log.Warn("ratelimit: check failed",
					slog.String("tenant", req.Tenant),
					slog.String("route", req.Route),
					slog.String("on_error", o.onError.String()),
					slog.String("err", err.Error()))
				if o.onError == FailClosed {
					if o.headers {
						w.Header().Set(HeaderRetryAfter, "1")
					}
					o.deny.ServeHTTP(w, r)
					return
				}
				next.ServeHTTP(w, r)
				return
			}
			if o.headers {
				setRateLimitHeaders(w.Header(), res)
			}
			r = r.WithContext(context.WithValue(r.Context(), resultKey{}, res))
			if !res.Allowed {
				if o.headers {
					w.Header().Set(HeaderRetryAfter, strconv.FormatInt(retryAfterSeconds(res.RetryAfter), 10))
				}
				o.deny.ServeHTTP(w, r)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// setRateLimitHeaders writes the three RateLimit-* values.
func setRateLimitHeaders(h http.Header, res Result) {
	h.Set(HeaderLimit, strconv.Itoa(res.Limit))
	h.Set(HeaderRemaining, strconv.Itoa(max(res.Remaining, 0)))
	h.Set(HeaderReset, strconv.FormatInt(ceilSeconds(res.ResetAfter), 10))
}

// ceilSeconds rounds d up to whole seconds; d <= 0 is 0.
func ceilSeconds(d time.Duration) int64 {
	if d <= 0 {
		return 0
	}
	return int64((d + time.Second - 1) / time.Second)
}

// retryAfterSeconds is ceilSeconds with a floor of 1: a client must never be
// told to retry immediately after a denial.
func retryAfterSeconds(d time.Duration) int64 {
	return max(ceilSeconds(d), 1)
}

// denyProblem is the default 429: an RFC 9457 problem document. When the
// check itself failed (FailClosed) there is no Result in the context and the
// detail says so.
func denyProblem(w http.ResponseWriter, r *http.Request) {
	p := Problem{
		Type:     "about:blank",
		Title:    http.StatusText(http.StatusTooManyRequests),
		Status:   http.StatusTooManyRequests,
		Instance: r.URL.Path,
	}
	if res, ok := ResultFromContext(r.Context()); ok {
		p.Policy = res.Policy
		p.RetryAfter = retryAfterSeconds(res.RetryAfter)
		p.Detail = "rate limited by policy " + strconv.Quote(res.Policy) +
			"; retry after " + strconv.FormatInt(p.RetryAfter, 10) + "s"
	} else {
		p.RetryAfter = 1
		p.Detail = "rate limiter unavailable; retry after 1s"
	}
	w.Header().Set("Content-Type", ProblemJSON)
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(http.StatusTooManyRequests)
	_ = json.NewEncoder(w).Encode(p) // the status is already committed; nothing useful to do on a write error
}
