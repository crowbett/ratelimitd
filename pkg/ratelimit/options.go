package ratelimit

import (
	"log/slog"
	"net/http"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/crowbett/ratelimitd/internal/store/local"
)

// DefaultTimeout bounds every Dial-backed Check when WithTimeout is not given.
const DefaultTimeout = 50 * time.Millisecond

// Option configures Dial and Embedded.
type Option func(*options)

type options struct {
	timeout time.Duration
	creds   credentials.TransportCredentials
	log     *slog.Logger
	local   local.Config
	clock   func() time.Time
	dial    []grpc.DialOption
}

// WithTimeout bounds every Check call (default 50ms for Dial, none for Embedded).
func WithTimeout(d time.Duration) Option { return func(o *options) { o.timeout = d } }

// WithTransportCredentials sets the gRPC credentials (default insecure).
func WithTransportCredentials(c credentials.TransportCredentials) Option {
	return func(o *options) { o.creds = c }
}

// WithLogger sets the logger for client-side errors (default slog.Default()).
func WithLogger(l *slog.Logger) Option { return func(o *options) { o.log = l } }

// WithLocalConfig tunes the Embedded backend (shards, idle TTL, max keys).
func WithLocalConfig(c local.Config) Option { return func(o *options) { o.local = c } }

// WithClock sets the clock Embedded stamps every decision with (default
// time.Now). The caller's clock is authoritative: nothing inside a decision
// reads time.Now. Dial ignores it (the daemon's clock decides).
func WithClock(now func() time.Time) Option { return func(o *options) { o.clock = now } }

// WithDialOptions appends raw gRPC dial options (a bufconn dialer in tests,
// an interceptor, a service config). They are applied after the built-in
// credentials and keepalive options, so they win on conflict.
func WithDialOptions(opts ...grpc.DialOption) Option {
	return func(o *options) { o.dial = append(o.dial, opts...) }
}

// OnError selects what Middleware/interceptors do when the Client itself
// fails (daemon unreachable, timeout): FailOpen lets the request through and
// logs; FailClosed answers 429 / RESOURCE_EXHAUSTED.
type OnError uint8

// OnError values. FailOpen is the default.
const (
	FailOpen OnError = iota
	FailClosed
)

// String returns "fail_open" or "fail_closed".
func (m OnError) String() string {
	if m == FailClosed {
		return "fail_closed"
	}
	return "fail_open"
}

// MWOption configures Middleware.
type MWOption func(*mwOptions)

type mwOptions struct {
	onError OnError
	deny    http.Handler
	headers bool
	cost    func(*http.Request) int
	log     *slog.Logger
}

// WithOnError sets the client-error behaviour (default FailOpen).
func WithOnError(m OnError) MWOption { return func(o *mwOptions) { o.onError = m } }

// WithDenyHandler replaces the default 429 application/problem+json response.
// The RateLimit-* and Retry-After headers are already set when it runs and
// the Result is available through ResultFromContext.
func WithDenyHandler(h http.Handler) MWOption { return func(o *mwOptions) { o.deny = h } }

// WithHeaders toggles RateLimit-Limit / RateLimit-Remaining / RateLimit-Reset
// on every response and Retry-After on 429 (default true).
func WithHeaders(on bool) MWOption { return func(o *mwOptions) { o.headers = on } }

// WithCost derives the request cost (default: the KeyFunc's Request.Cost, 0 = 1).
func WithCost(f func(*http.Request) int) MWOption { return func(o *mwOptions) { o.cost = f } }

// WithMWLogger sets the logger Middleware uses for client errors (default
// slog.Default()). Named apart from WithLogger because that one is an Option
// for Dial/Embedded and the two option types are frozen.
func WithMWLogger(l *slog.Logger) MWOption { return func(o *mwOptions) { o.log = l } }
