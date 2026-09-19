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
	"fmt"
	"log/slog"
	"time"

	"github.com/crowbett/ratelimitd/internal/limit"
	"github.com/crowbett/ratelimitd/internal/policy"
	"github.com/crowbett/ratelimitd/internal/store/local"
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
	// Backend is "redis", "local" or "degraded"; Degraded is true while the
	// daemon answers from its local fallback. Informational (bench, logs).
	Backend  string
	Degraded bool
}

// Client is the port both Dial and Embedded satisfy.
type Client interface {
	Check(ctx context.Context, req Request) (Result, error)
	Close() error
}

// BatchChecker is implemented by the Dial client: one round trip for several
// independent subjects (ratelimit.v1.BatchCheck, at most 64 per call).
// Results are positional with reqs.
type BatchChecker interface {
	BatchCheck(ctx context.Context, reqs []Request) ([]Result, error)
}

// Dial connects to a ratelimitd gRPC endpoint with keepalive and a retry on
// UNAVAILABLE. It does not block: the connection is established lazily by the
// first Check. ctx only aborts an already-cancelled call.
func Dial(ctx context.Context, target string, opts ...Option) (Client, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("ratelimit: dial %s: %w", target, err)
	}
	o := newOptions(opts)
	if o.timeout <= 0 {
		o.timeout = DefaultTimeout
	}
	return dialGRPC(target, o)
}

// Embedded runs the limiter in-process over the local backend with set's
// policies; no daemon, no Redis. It validates set, starts the ONE idle-key
// sweeper goroutine of the local backend and stops it on Close.
func Embedded(set policy.Set, opts ...Option) (Client, error) {
	if err := set.Validate(); err != nil {
		return nil, fmt.Errorf("ratelimit: embedded: %w", err)
	}
	o := newOptions(opts)
	reg := policy.NewRegistry(set, o.log)
	backend := local.New(o.local)
	return newEmbedded(reg, backend, backend.Run, o), nil
}

func newOptions(opts []Option) *options {
	o := &options{log: slog.Default(), clock: time.Now}
	for _, opt := range opts {
		opt(o)
	}
	if o.log == nil {
		o.log = slog.Default()
	}
	if o.clock == nil {
		o.clock = time.Now
	}
	return o
}

// fromDecision maps the domain Decision to the public Result.
func fromDecision(d limit.Decision) Result {
	return Result{
		Allowed:    d.Allowed,
		Limit:      d.Limit,
		Remaining:  d.Remaining,
		RetryAfter: d.RetryAfter,
		ResetAfter: d.ResetAfter,
		Policy:     d.Policy,
		Backend:    d.Backend,
		Degraded:   d.Backend == limit.BackendDegraded,
	}
}
