package ratelimit

import (
	"log/slog"
	"net/http"
	"time"

	"google.golang.org/grpc/credentials"

	"github.com/crowbett/ratelimitd/internal/store/local"
)

// Option configures Dial and Embedded.
type Option func(*options)

type options struct {
	timeout time.Duration
	creds   credentials.TransportCredentials
	log     *slog.Logger
	local   local.Config
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

// OnError selects what Middleware/interceptors do when the Client itself
// fails (daemon unreachable, timeout): FailOpen lets the request through and
// logs; FailClosed answers 429 / RESOURCE_EXHAUSTED.
type OnError uint8

// OnError values. FailOpen is the default.
const (
	FailOpen OnError = iota
	FailClosed
)

// MWOption configures Middleware.
type MWOption func(*mwOptions)

type mwOptions struct {
	onError OnError
	deny    http.Handler
	headers bool
	cost    func(*http.Request) int
}

// WithOnError sets the client-error behaviour (default FailOpen).
func WithOnError(m OnError) MWOption { return func(o *mwOptions) { o.onError = m } }

// WithDenyHandler replaces the default 429 response (text/plain "rate limited").
func WithDenyHandler(h http.Handler) MWOption { return func(o *mwOptions) { o.deny = h } }

// WithHeaders toggles RateLimit-Limit / RateLimit-Remaining / RateLimit-Reset
// on every response and Retry-After on 429 (default true).
func WithHeaders(on bool) MWOption { return func(o *mwOptions) { o.headers = on } }

// WithCost derives the request cost (default 1).
func WithCost(f func(*http.Request) int) MWOption { return func(o *mwOptions) { o.cost = f } }
