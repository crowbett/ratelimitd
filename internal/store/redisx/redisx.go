// Package redisx is the go-redis backend: one embedded Lua script
// (lua/dispatch.lua + gcra/tokenbucket/slidingwindow.lua concatenated at
// build time), EVALSHA with NOSCRIPT -> reload behind a dependency-free
// singleflight, Pipeline across subjects in BatchCheck, TIME for the skew
// probe. The caller's clock is passed as ARGV; Redis TIME is never used
// inside the script (docs/PLAN.md, Decision 3).
//
// Owner: workstream B (backends). The lua/ directory (embed.FS) is B's too.
package redisx

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/crowbett/ratelimitd/internal/admin"
	"github.com/crowbett/ratelimitd/internal/limit"
)

// Config is the connection and key namespace.
type Config struct {
	Addr     string // REDIS_ADDR, default 127.0.0.1:6380
	Username string
	Password string
	DB       int
	PoolSize int // REDIS_POOL_SIZE, default 64
	MinIdle  int

	DialTimeout  time.Duration
	ReadTimeout  time.Duration // REDIS_TIMEOUT, default 50ms
	WriteTimeout time.Duration // REDIS_TIMEOUT

	Cluster   bool          // REDIS_CLUSTER
	KeyPrefix string        // outer namespace in front of limit.Key, default ""
	TTLSlack  time.Duration // added to every PX so a key outlives its reset by this much, default 1s
}

// Backend implements limit.Store over Redis.
type Backend struct {
	cfg Config
	log *slog.Logger
	st  *admin.Snapshot
}

// New connects (lazily; the first PING is the caller's readiness probe),
// loads the script and returns the Backend. log and st may be nil. STUB:
// returns a Backend that errors on every call.
func New(cfg Config, log *slog.Logger, st *admin.Snapshot) (*Backend, error) {
	if log == nil {
		log = slog.Default()
	}
	return &Backend{cfg: cfg, log: log, st: st}, nil
}

// Config returns the configuration the backend was built with.
func (b *Backend) Config() Config { return b.cfg }

// Apply implements limit.Store: one EVALSHA carrying every req of the
// subject (same hash slot), dry-run then commit inside the script. STUB.
func (b *Backend) Apply(_ context.Context, _ []limit.Req, _ time.Time) ([]limit.Decision, error) {
	return nil, errors.New("stub: redisx.Backend.Apply")
}

// Peek implements limit.Store (commit=0 only). STUB.
func (b *Backend) Peek(_ context.Context, _ []limit.Req, _ time.Time) ([]limit.Decision, error) {
	return nil, errors.New("stub: redisx.Backend.Peek")
}

// Reset implements limit.Store (UNLINK). STUB.
func (b *Backend) Reset(_ context.Context, _ []string) error {
	return errors.New("stub: redisx.Backend.Reset")
}

// Name implements limit.Store: "redis".
func (b *Backend) Name() string { return limit.BackendRedis }

// Ping is the readiness probe. STUB.
func (b *Backend) Ping(_ context.Context) error {
	return errors.New("stub: redisx.Backend.Ping")
}

// Skew returns Redis TIME minus now (positive = Redis is ahead). The daemon
// samples it every 10 s, exports rl_clock_skew_seconds, warns above 250 ms
// and refuses to start above MAX_CLOCK_SKEW. STUB.
func (b *Backend) Skew(_ context.Context, _ time.Time) (time.Duration, error) {
	return 0, errors.New("stub: redisx.Backend.Skew")
}

// ScriptLoads is how many times the script was (re)loaded; a spike means the
// script cache was flushed. STUB: 0.
func (b *Backend) ScriptLoads() uint64 { return 0 }

// Close releases the pool (teardown stage 3). STUB.
func (b *Backend) Close() error { return nil }
