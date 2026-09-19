// Package ratelimit is the public client library for ratelimitd: a gRPC
// Client (Dial), an in-process Embedded client that needs no daemon, a
// net/http Middleware that sets RateLimit-* headers and 429s, and gRPC
// server interceptors. A denial is Result.Allowed == false, never an error;
// ErrDenied is what Middleware/interceptors turn it into.
//
// Owner: workstream D (client & load). Signatures frozen in docs/PLAN.md.
package ratelimit

import (
	"context"
	"errors"
	"time"

	"github.com/crowbett/ratelimitd/internal/limit"
	"github.com/crowbett/ratelimitd/internal/policy"
)

// ErrDenied is returned by Middleware/UnaryInterceptor on a denial; match
// with errors.Is.
var ErrDenied = limit.ErrDenied

// Descriptor is the Envoy RLS key/value shape.
type Descriptor struct {
	Key   string
	Value string
}

// Request identifies what to limit.
type Request struct {
	Tenant      string
	Subject     string
	Route       string
	Method      string
	Descriptors []Descriptor
	Cost        int // 0 means 1
}

// Result is the decision.
type Result struct {
	Allowed    bool
	Limit      int
	Remaining  int
	RetryAfter time.Duration
	ResetAfter time.Duration
	Policy     string
}

// Client is the port both Dial and Embedded satisfy.
type Client interface {
	Check(ctx context.Context, req Request) (Result, error)
	Close() error
}

// Dial connects to a ratelimitd gRPC endpoint with keepalive and a retry on
// UNAVAILABLE. STUB.
func Dial(_ context.Context, target string, _ ...Option) (Client, error) {
	_ = target
	return nil, errors.New("stub: ratelimit.Dial")
}

// Embedded runs the limiter in-process over the local backend with set's
// policies; no daemon, no Redis. STUB.
func Embedded(_ policy.Set, _ ...Option) (Client, error) {
	return nil, errors.New("stub: ratelimit.Embedded")
}
