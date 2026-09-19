// Package obs wires OpenTelemetry traces and metrics (OTLP/gRPC push), a slog
// handler that stamps trace/span ids onto records, and the observable
// instruments that read the same admin.Snapshot as the /metrics exposer.
//
// Everything is opt-in: with Config.Enabled false, Setup installs nothing and
// the tracer / meter fall back to the SDK's no-op globals.
//
// Owner: workstream C (service edge). Shape copied from jobrun/internal/obs.
package obs

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/crowbett/ratelimitd/internal/admin"
)

// MeterName and TracerName are the instrumentation scopes.
const (
	MeterName  = "ratelimitd"
	TracerName = "ratelimitd"
)

// Config selects what Setup installs.
type Config struct {
	Enabled        bool
	ServiceName    string
	ServiceVersion string
	Environment    string // dev | staging | prod -> deployment.environment.name
	// Endpoint is the OTLP/gRPC collector URL, e.g. "http://localhost:4317".
	Endpoint string
	// MetricInterval is the periodic reader export interval (0 = 15s).
	MetricInterval time.Duration
	// Logger receives SDK errors; nil = slog.Default().
	Logger *slog.Logger
}

// StopFunc flushes and stops every provider Setup started.
type StopFunc func(context.Context) error

// Setup configures the global TracerProvider, MeterProvider and propagators.
// The returned stop must be called (with its own timeout) as the last
// teardown stage. With cfg.Enabled == false it is a no-op. STUB when enabled.
func Setup(_ context.Context, cfg Config) (StopFunc, error) {
	if !cfg.Enabled {
		return func(context.Context) error { return nil }, nil
	}
	return nil, errors.New("stub: obs.Setup")
}

// RegisterSnapshot exposes s as observable instruments sampled at every
// export: rl_decisions_total, rl_backend_errors_total, rl_fallback_active,
// rl_fallback_seconds_total, rl_redis_script_loads_total,
// rl_policy_reload_errors_total, rl_clock_skew_seconds, rl_local_keys,
// rl_local_evicted_total. STUB.
func RegisterSnapshot(_ *admin.Snapshot) error {
	return errors.New("stub: obs.RegisterSnapshot")
}

// NewLogHandler wraps h so records carry trace_id/span_id when a span is in
// the context. STUB: returns h unchanged.
func NewLogHandler(h slog.Handler) slog.Handler { return h }
