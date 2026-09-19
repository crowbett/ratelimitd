// Package config loads the ratelimitd configuration from the environment. No
// globals: Load returns a value that cmd/ratelimitd threads into every
// constructor, and Validate reports every problem at once. Every knob is in
// the env table of docs/PLAN.md.
//
// Owner: integrator.
package config

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config is everything cmd/ratelimitd needs.
type Config struct {
	ServiceName string // SERVICE_NAME, default "ratelimitd"
	Env         string // APP_ENV: dev | staging | prod
	GRPCAddr    string // GRPC_ADDR, default ":50053"
	HTTPAddr    string // HTTP_ADDR, default ":8093"
	AdminAddr   string // ADMIN_ADDR, default "127.0.0.1:9130"

	LogLevel  slog.Level // LOG_LEVEL
	LogFormat string     // LOG_FORMAT: json | text

	StopTimeout      time.Duration // SHUTDOWN_TIMEOUT: stage-1 drain budget, default 10s
	MaxClockSkew     time.Duration // MAX_CLOCK_SKEW: refuse to start above it, default 2s
	TrustClientClock bool          // TRUST_CLIENT_CLOCK: honour CheckRequest.now, default false

	Redis    Redis
	Policy   Policy
	Fallback Fallback
	Breaker  Breaker
	Local    Local
	OTel     OTel
}

// Redis is the primary backend.
type Redis struct {
	Addr     string        // REDIS_ADDR, default 127.0.0.1:6380
	PoolSize int           // REDIS_POOL_SIZE, default 64
	Cluster  bool          // REDIS_CLUSTER, default false
	Timeout  time.Duration // REDIS_TIMEOUT: read+write timeout and the latency budget floor, default 50ms
}

// Policy is the hot-reloaded policy file.
type Policy struct {
	File string        // POLICY_FILE, default policies.yaml
	Poll time.Duration // POLICY_POLL: mtime+size poll interval, default 2s
}

// Fallback is the degraded behaviour.
type Fallback struct {
	Mode    string // FALLBACK_MODE: local | fail_open | fail_closed, default local
	Divisor int64  // FALLBACK_DIVISOR: per-node quota split while degraded, default 1
}

// Breaker tunes when the fallback engages and recovers.
type Breaker struct {
	Ratio   float64       // BREAKER_RATIO, default 0.5
	Min     int           // BREAKER_MIN: samples before the ratio counts, default 20
	OpenFor time.Duration // BREAKER_OPEN_FOR, default 5s
	Probe   time.Duration // BREAKER_PROBE, default 500ms
}

// Local tunes the in-process backend and its GC.
type Local struct {
	Shards  int           // LOCAL_SHARDS, default 256
	IdleTTL time.Duration // LOCAL_IDLE_TTL, default 10m
	MaxKeys int           // LOCAL_MAX_KEYS, default 1000000
}

// OTel selects telemetry export.
type OTel struct {
	Enabled  bool   // OTEL_ENABLED
	Endpoint string // OTEL_EXPORTER_OTLP_ENDPOINT, default http://localhost:4317
}

// Load reads the environment and validates the result.
func Load() (Config, error) {
	cfg := Config{
		ServiceName:  getenv("SERVICE_NAME", "ratelimitd"),
		Env:          getenv("APP_ENV", "dev"),
		GRPCAddr:     getenv("GRPC_ADDR", ":50053"),
		HTTPAddr:     getenv("HTTP_ADDR", ":8093"),
		AdminAddr:    getenv("ADMIN_ADDR", "127.0.0.1:9130"),
		LogFormat:    strings.ToLower(getenv("LOG_FORMAT", "json")),
		StopTimeout:  10 * time.Second,
		MaxClockSkew: 2 * time.Second,
		Redis:        Redis{Addr: getenv("REDIS_ADDR", "127.0.0.1:6380"), PoolSize: 64, Timeout: 50 * time.Millisecond},
		Policy:       Policy{File: getenv("POLICY_FILE", "policies.yaml"), Poll: 2 * time.Second},
		Fallback:     Fallback{Mode: strings.ToLower(getenv("FALLBACK_MODE", "local")), Divisor: 1},
		Breaker:      Breaker{Ratio: 0.5, Min: 20, OpenFor: 5 * time.Second, Probe: 500 * time.Millisecond},
		Local:        Local{Shards: 256, IdleTTL: 10 * time.Minute, MaxKeys: 1_000_000},
		OTel:         OTel{Endpoint: getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317")},
	}

	var err error
	if err = cfg.LogLevel.UnmarshalText([]byte(getenv("LOG_LEVEL", "info"))); err != nil {
		return Config{}, fmt.Errorf("LOG_LEVEL: %w", err)
	}
	durs := []struct {
		key string
		dst *time.Duration
	}{
		{"SHUTDOWN_TIMEOUT", &cfg.StopTimeout},
		{"MAX_CLOCK_SKEW", &cfg.MaxClockSkew},
		{"REDIS_TIMEOUT", &cfg.Redis.Timeout},
		{"POLICY_POLL", &cfg.Policy.Poll},
		{"BREAKER_OPEN_FOR", &cfg.Breaker.OpenFor},
		{"BREAKER_PROBE", &cfg.Breaker.Probe},
		{"LOCAL_IDLE_TTL", &cfg.Local.IdleTTL},
	}
	for _, d := range durs {
		if *d.dst, err = getDuration(d.key, *d.dst); err != nil {
			return Config{}, err
		}
	}
	ints := []struct {
		key string
		dst *int
	}{
		{"REDIS_POOL_SIZE", &cfg.Redis.PoolSize},
		{"BREAKER_MIN", &cfg.Breaker.Min},
		{"LOCAL_SHARDS", &cfg.Local.Shards},
		{"LOCAL_MAX_KEYS", &cfg.Local.MaxKeys},
	}
	for _, i := range ints {
		if *i.dst, err = getInt(i.key, *i.dst); err != nil {
			return Config{}, err
		}
	}
	var div int
	if div, err = getInt("FALLBACK_DIVISOR", int(cfg.Fallback.Divisor)); err != nil {
		return Config{}, err
	}
	cfg.Fallback.Divisor = int64(div)
	if cfg.Breaker.Ratio, err = getFloat("BREAKER_RATIO", cfg.Breaker.Ratio); err != nil {
		return Config{}, err
	}
	bools := []struct {
		key string
		dst *bool
	}{
		{"REDIS_CLUSTER", &cfg.Redis.Cluster},
		{"TRUST_CLIENT_CLOCK", &cfg.TrustClientClock},
		{"OTEL_ENABLED", &cfg.OTel.Enabled},
	}
	for _, b := range bools {
		if *b.dst, err = getBool(b.key, *b.dst); err != nil {
			return Config{}, err
		}
	}

	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// Validate reports every problem at once.
func (c Config) Validate() error {
	var errs []error
	if c.GRPCAddr == "" {
		errs = append(errs, errors.New("GRPC_ADDR must not be empty"))
	}
	if c.HTTPAddr == "" {
		errs = append(errs, errors.New("HTTP_ADDR must not be empty"))
	}
	if c.AdminAddr == "" {
		errs = append(errs, errors.New("ADMIN_ADDR must not be empty"))
	}
	if c.LogFormat != "json" && c.LogFormat != "text" {
		errs = append(errs, fmt.Errorf("LOG_FORMAT must be json or text, got %q", c.LogFormat))
	}
	if c.StopTimeout <= 0 {
		errs = append(errs, errors.New("SHUTDOWN_TIMEOUT must be > 0"))
	}
	if c.MaxClockSkew <= 0 {
		errs = append(errs, errors.New("MAX_CLOCK_SKEW must be > 0"))
	}
	if c.Redis.Addr == "" {
		errs = append(errs, errors.New("REDIS_ADDR must not be empty"))
	}
	if c.Redis.PoolSize < 1 || c.Redis.PoolSize > 4096 {
		errs = append(errs, fmt.Errorf("REDIS_POOL_SIZE must be 1..4096, got %d", c.Redis.PoolSize))
	}
	if c.Redis.Timeout < time.Millisecond {
		errs = append(errs, errors.New("REDIS_TIMEOUT must be >= 1ms"))
	}
	if c.Policy.File == "" {
		errs = append(errs, errors.New("POLICY_FILE must not be empty"))
	}
	if c.Policy.Poll < 100*time.Millisecond {
		errs = append(errs, errors.New("POLICY_POLL must be >= 100ms"))
	}
	switch c.Fallback.Mode {
	case "local", "fail_open", "fail_closed":
	default:
		errs = append(errs, fmt.Errorf("FALLBACK_MODE must be local, fail_open or fail_closed, got %q", c.Fallback.Mode))
	}
	if c.Fallback.Divisor < 1 {
		errs = append(errs, fmt.Errorf("FALLBACK_DIVISOR must be >= 1, got %d", c.Fallback.Divisor))
	}
	if c.Breaker.Ratio <= 0 || c.Breaker.Ratio > 1 {
		errs = append(errs, fmt.Errorf("BREAKER_RATIO must be in (0, 1], got %g", c.Breaker.Ratio))
	}
	if c.Breaker.Min < 1 {
		errs = append(errs, fmt.Errorf("BREAKER_MIN must be >= 1, got %d", c.Breaker.Min))
	}
	if c.Breaker.OpenFor <= 0 {
		errs = append(errs, errors.New("BREAKER_OPEN_FOR must be > 0"))
	}
	if c.Breaker.Probe <= 0 || c.Breaker.Probe > c.Breaker.OpenFor {
		errs = append(errs, fmt.Errorf("BREAKER_PROBE must be > 0 and <= BREAKER_OPEN_FOR (%s)", c.Breaker.OpenFor))
	}
	if c.Local.Shards < 1 || c.Local.Shards&(c.Local.Shards-1) != 0 {
		errs = append(errs, fmt.Errorf("LOCAL_SHARDS must be a power of two >= 1, got %d", c.Local.Shards))
	}
	if c.Local.IdleTTL < time.Second {
		errs = append(errs, errors.New("LOCAL_IDLE_TTL must be >= 1s"))
	}
	if c.Local.MaxKeys < 1 {
		errs = append(errs, fmt.Errorf("LOCAL_MAX_KEYS must be >= 1, got %d", c.Local.MaxKeys))
	}
	if c.OTel.Enabled && c.OTel.Endpoint == "" {
		errs = append(errs, errors.New("OTEL_EXPORTER_OTLP_ENDPOINT is required when OTEL_ENABLED=true"))
	}
	return errors.Join(errs...)
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getDuration(key string, def time.Duration) (time.Duration, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	return d, nil
}

func getInt(key string, def int) (int, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	return n, nil
}

func getFloat(key string, def float64) (float64, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	return f, nil
}

func getBool(key string, def bool) (bool, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return false, fmt.Errorf("%s: %w", key, err)
	}
	return b, nil
}
