// Package redisx is the go-redis backend: one embedded Lua script
// (lua/dispatch.lua + gcra/tokenbucket/slidingwindow.lua concatenated at
// init), EVALSHA with NOSCRIPT -> reload behind a dependency-free
// singleflight, Pipeline across subjects in ApplyMany, TIME for the skew
// probe. The caller's clock is passed as ARGV; Redis TIME is never used
// inside the script (docs/PLAN.md, Decision 3).
//
// Owner: workstream B (backends). The lua/ directory (embed.FS) is B's too.
package redisx

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"

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
	rdb redis.UniversalClient

	// Script reload singleflight: a caller that saw NOSCRIPT with generation
	// g reloads only if the generation is still g; everyone else just
	// retries EVALSHA.
	loadMu sync.Mutex
	gen    atomic.Uint64
	loads  atomic.Uint64
}

// New builds the client (connections are dialled lazily; the first PING is
// the caller's readiness probe) and tries to load the script once, logging
// but not failing if Redis is not reachable yet: the NOSCRIPT path reloads
// it on first use. log and st may be nil.
func New(cfg Config, log *slog.Logger, st *admin.Snapshot) (*Backend, error) {
	if log == nil {
		log = slog.Default()
	}
	if cfg.Addr == "" {
		cfg.Addr = "127.0.0.1:6380"
	}
	if cfg.PoolSize <= 0 {
		cfg.PoolSize = 64
	}
	if cfg.ReadTimeout <= 0 {
		cfg.ReadTimeout = 50 * time.Millisecond
	}
	if cfg.WriteTimeout <= 0 {
		cfg.WriteTimeout = cfg.ReadTimeout
	}
	if cfg.DialTimeout <= 0 {
		cfg.DialTimeout = 5 * cfg.ReadTimeout
	}
	if cfg.TTLSlack <= 0 {
		cfg.TTLSlack = time.Second
	}
	opts := &redis.UniversalOptions{
		Addrs:        []string{cfg.Addr},
		Username:     cfg.Username,
		Password:     cfg.Password,
		DB:           cfg.DB,
		PoolSize:     cfg.PoolSize,
		MinIdleConns: cfg.MinIdle,
		DialTimeout:  cfg.DialTimeout,
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
		PoolTimeout:  cfg.ReadTimeout + cfg.DialTimeout,
		// A rate limiter must not silently re-run a script on a timeout
		// (it would double-spend); the fallback breaker owns retries.
		MaxRetries:    -1,
		IsClusterMode: cfg.Cluster,
	}
	b := &Backend{cfg: cfg, log: log, st: st, rdb: redis.NewUniversalClient(opts)}
	ctx, cancel := context.WithTimeout(context.Background(), cfg.DialTimeout+cfg.ReadTimeout)
	defer cancel()
	if err := b.reload(ctx, b.gen.Load()); err != nil {
		log.Warn("redisx: initial script load failed; will reload on NOSCRIPT",
			slog.String("addr", cfg.Addr), slog.Any("err", err))
	}
	return b, nil
}

// Config returns the configuration the backend was built with.
func (b *Backend) Config() Config { return b.cfg }

// Apply implements limit.Store: one EVALSHA carrying every req of the
// subject (same hash slot), dry-run then commit inside the script.
func (b *Backend) Apply(ctx context.Context, reqs []limit.Req, now time.Time) ([]limit.Decision, error) {
	return b.eval(ctx, reqs, now, true)
}

// Peek implements limit.Store (commit=0 forced).
func (b *Backend) Peek(ctx context.Context, reqs []limit.Req, now time.Time) ([]limit.Decision, error) {
	return b.eval(ctx, reqs, now, false)
}

// ApplyMany is Apply for several subjects in one round trip (BatchCheck):
// one EVALSHA per group, pipelined. out[i] answers groups[i]; a group whose
// call failed has a nil slice and its error in errs[i]. The returned error is
// non-nil only when the pipeline itself could not run.
func (b *Backend) ApplyMany(ctx context.Context, groups [][]limit.Req, now time.Time) (out [][]limit.Decision, errs []error, err error) {
	out = make([][]limit.Decision, len(groups))
	errs = make([]error, len(groups))
	for i, g := range groups {
		if e := validate(g); e != nil {
			errs[i] = e
		}
	}
	for attempt := 0; attempt < 2; attempt++ {
		gen := b.gen.Load()
		pipe := b.rdb.Pipeline()
		cmds := make([]*redis.Cmd, len(groups))
		for i, g := range groups {
			if errs[i] != nil || out[i] != nil || len(g) == 0 {
				continue
			}
			keys, args := b.pack(g, now, true)
			cmds[i] = script.EvalSha(ctx, pipe, keys, args...)
		}
		if _, e := pipe.Exec(ctx); e != nil && !isRedisReply(e) {
			return out, errs, b.fail(e)
		}
		noscript := false
		for i, g := range groups {
			c := cmds[i]
			if c == nil {
				continue
			}
			vals, e := c.Int64Slice()
			switch {
			case e == nil:
				out[i], errs[i] = unpack(vals, g)
			case redis.HasErrorPrefix(e, "NOSCRIPT"):
				noscript = true
			default:
				errs[i] = b.fail(e)
			}
		}
		if !noscript {
			break
		}
		if e := b.reload(ctx, gen); e != nil {
			return out, errs, b.fail(e)
		}
	}
	for i := range groups {
		if out[i] == nil && errs[i] == nil {
			if len(groups[i]) == 0 {
				out[i] = []limit.Decision{}
			} else {
				errs[i] = fmt.Errorf("%w: script missing after reload", limit.ErrBackendDown)
			}
		}
	}
	return out, errs, nil
}

// Reset implements limit.Store (DEL).
func (b *Backend) Reset(ctx context.Context, keys []string) error {
	if len(keys) == 0 {
		return nil
	}
	full := make([]string, len(keys))
	for i, k := range keys {
		full[i] = b.cfg.KeyPrefix + k
	}
	if err := b.rdb.Del(ctx, full...).Err(); err != nil {
		return b.fail(err)
	}
	return nil
}

// Name implements limit.Store: "redis".
func (b *Backend) Name() string { return limit.BackendRedis }

// Ping is the readiness probe.
func (b *Backend) Ping(ctx context.Context) error {
	if err := b.rdb.Ping(ctx).Err(); err != nil {
		return b.fail(err)
	}
	return nil
}

// Skew returns Redis TIME minus now (positive = Redis is ahead). The daemon
// samples it every 10 s, exports rl_clock_skew_seconds, warns above 250 ms
// and refuses to start above MAX_CLOCK_SKEW.
func (b *Backend) Skew(ctx context.Context, now time.Time) (time.Duration, error) {
	t, err := b.rdb.Time(ctx).Result()
	if err != nil {
		return 0, b.fail(err)
	}
	return t.Sub(now), nil
}

// ScriptLoads is how many times the script was (re)loaded; a spike means the
// script cache was flushed.
func (b *Backend) ScriptLoads() uint64 { return b.loads.Load() }

// Close releases the pool (teardown stage 3).
func (b *Backend) Close() error { return b.rdb.Close() }

// eval runs the script once, reloading it exactly once on NOSCRIPT.
func (b *Backend) eval(ctx context.Context, reqs []limit.Req, now time.Time, commit bool) ([]limit.Decision, error) {
	if len(reqs) == 0 {
		return []limit.Decision{}, nil
	}
	if err := validate(reqs); err != nil {
		return nil, err
	}
	keys, args := b.pack(reqs, now, commit)
	gen := b.gen.Load()
	vals, err := script.EvalSha(ctx, b.rdb, keys, args...).Int64Slice()
	if err != nil && redis.HasErrorPrefix(err, "NOSCRIPT") {
		if err = b.reload(ctx, gen); err == nil {
			vals, err = script.EvalSha(ctx, b.rdb, keys, args...).Int64Slice()
		}
	}
	if err != nil {
		return nil, b.fail(err)
	}
	return unpack(vals, reqs)
}

// pack builds KEYS (prefixed) and ARGV for one script call.
func (b *Backend) pack(reqs []limit.Req, now time.Time, commit bool) (keys []string, args []any) {
	keys = make([]string, len(reqs))
	var nonce int64
	for i := range reqs {
		keys[i] = b.cfg.KeyPrefix + reqs[i].Key
		if reqs[i].Params.Algo == limit.SlidingWindowExact && nonce == 0 {
			nonce = rand.Int64() // member uniqueness for the ZSET variant only
		}
	}
	args = packArgs(make([]any, 0, len(reqs)*argvPerKey+1), reqs, now.UnixMicro(), b.cfg.TTLSlack.Milliseconds(), nonce, commit)
	return keys, args
}

// reload loads the script if nobody has since the caller observed
// generation seen (mutex + generation = a dependency-free singleflight), and
// counts every real load.
func (b *Backend) reload(ctx context.Context, seen uint64) error {
	b.loadMu.Lock()
	defer b.loadMu.Unlock()
	if b.gen.Load() != seen {
		return nil
	}
	if err := script.Load(ctx, b.rdb).Err(); err != nil {
		return err
	}
	b.gen.Add(1)
	b.loads.Add(1)
	b.st.AddScriptLoad()
	return nil
}

// fail records the error under its kind and wraps it: transport failures
// (timeouts, refused/reset connections, pool exhaustion, a closed client,
// a Redis that is loading or read-only) become limit.ErrBackendDown so the
// fallback breaker can match them; script and argument errors are returned
// as they are, wrapped with context.
func (b *Backend) fail(err error) error {
	kind, down := classify(err)
	b.st.RecordBackendError(limit.BackendRedis, kind)
	if down {
		return fmt.Errorf("%w: redis %s: %w", limit.ErrBackendDown, kind, err)
	}
	return fmt.Errorf("redisx: %s: %w", kind, err)
}

// isRedisReply reports whether err is an error reply from the server (as
// opposed to a transport failure).
func isRedisReply(err error) bool {
	var re redis.Error
	return errors.As(err, &re)
}
