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
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"
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
	// A plain "host:port" or an http:// URL is dialled without TLS.
	Endpoint string
	// MetricInterval is the periodic reader export interval (0 = 15s, or
	// OTEL_METRIC_EXPORT_INTERVAL when that is set).
	MetricInterval time.Duration
	// Logger receives SDK errors; nil = slog.Default().
	Logger *slog.Logger
}

// StopFunc flushes and stops every provider Setup started.
type StopFunc func(context.Context) error

// Setup configures the global TracerProvider, MeterProvider and propagators.
// The returned stop must be called (with its own timeout) as the last
// teardown stage so the final spans and metrics are exported. With
// cfg.Enabled == false it is a no-op and the stop returns nil.
func Setup(ctx context.Context, cfg Config) (StopFunc, error) {
	if !cfg.Enabled {
		return func(context.Context) error { return nil }, nil
	}

	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	// Export hiccups (collector down) are warnings, not stderr spam.
	otel.SetErrorHandler(otel.ErrorHandlerFunc(func(err error) {
		logger.Warn("otel sdk error", slog.Any("err", err))
	}))

	res, err := newResource(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("otel resource: %w", err)
	}

	var stops []StopFunc
	stop := func(ctx context.Context) error {
		var errs []error
		// Reverse order: stop producers before the providers they write to.
		for i := len(stops) - 1; i >= 0; i-- {
			if err := stops[i](ctx); err != nil {
				errs = append(errs, err)
			}
		}
		return errors.Join(errs...)
	}
	fail := func(err error) (StopFunc, error) {
		_ = stop(ctx)
		return nil, err
	}

	traceExp, err := otlptracegrpc.New(ctx, traceOptions(cfg.Endpoint)...)
	if err != nil {
		return fail(fmt.Errorf("otlp trace exporter: %w", err))
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithResource(res),
		sdktrace.WithBatcher(traceExp),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.AlwaysSample())),
	)
	stops = append(stops, tp.Shutdown)
	otel.SetTracerProvider(tp)

	metricExp, err := otlpmetricgrpc.New(ctx, metricOptions(cfg.Endpoint)...)
	if err != nil {
		return fail(fmt.Errorf("otlp metric exporter: %w", err))
	}
	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(res),
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExp, readerOptions(cfg.MetricInterval)...)),
	)
	stops = append(stops, mp.Shutdown)
	otel.SetMeterProvider(mp)

	// W3C trace-context + baggage on incoming/outgoing requests.
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	logger.Info("otel enabled",
		slog.String("endpoint", cfg.Endpoint),
		slog.String("version", cfg.ServiceVersion),
		slog.String("env", cfg.Environment),
	)
	return stop, nil
}

// Tracer returns the ratelimitd tracer from the global provider.
func Tracer() trace.Tracer { return otel.Tracer(TracerName) }

func newResource(ctx context.Context, cfg Config) (*resource.Resource, error) {
	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName(cfg.ServiceName),
			semconv.ServiceVersion(cfg.ServiceVersion),
			semconv.DeploymentEnvironmentNameKey.String(cfg.Environment),
		),
		resource.WithTelemetrySDK(),
		resource.WithProcessRuntimeName(),
		resource.WithProcessRuntimeVersion(),
		resource.WithHost(),
		resource.WithFromEnv(), // OTEL_RESOURCE_ATTRIBUTES / OTEL_SERVICE_NAME override the above
	)
	// resource.New returns a partial resource plus ErrPartialResource / schema
	// conflicts for non-fatal detector issues; keep going with what we have.
	if err != nil && !errors.Is(err, resource.ErrPartialResource) && !errors.Is(err, resource.ErrSchemaURLConflict) {
		return nil, err
	}
	return res, nil
}

// insecure reports whether endpoint should be dialled without TLS: anything
// that is not an explicit https:// URL (local collectors speak plaintext gRPC).
func insecure(endpoint string) bool {
	return !strings.HasPrefix(strings.ToLower(endpoint), "https://")
}

func traceOptions(endpoint string) []otlptracegrpc.Option {
	opts := []otlptracegrpc.Option{otlptracegrpc.WithTimeout(5 * time.Second)}
	if endpoint != "" {
		opts = append(opts, otlptracegrpc.WithEndpointURL(normalizeEndpoint(endpoint)))
	}
	if insecure(endpoint) {
		opts = append(opts, otlptracegrpc.WithInsecure())
	}
	return opts
}

func metricOptions(endpoint string) []otlpmetricgrpc.Option {
	opts := []otlpmetricgrpc.Option{otlpmetricgrpc.WithTimeout(5 * time.Second)}
	if endpoint != "" {
		opts = append(opts, otlpmetricgrpc.WithEndpointURL(normalizeEndpoint(endpoint)))
	}
	if insecure(endpoint) {
		opts = append(opts, otlpmetricgrpc.WithInsecure())
	}
	return opts
}

// normalizeEndpoint turns a bare "host:port" into the URL form WithEndpointURL wants.
func normalizeEndpoint(endpoint string) string {
	if strings.Contains(endpoint, "://") {
		return endpoint
	}
	return "http://" + endpoint
}

func readerOptions(interval time.Duration) []sdkmetric.PeriodicReaderOption {
	if interval > 0 {
		return []sdkmetric.PeriodicReaderOption{sdkmetric.WithInterval(interval)}
	}
	if v := os.Getenv("OTEL_METRIC_EXPORT_INTERVAL"); v != "" {
		return nil // let the SDK honour the env var
	}
	return []sdkmetric.PeriodicReaderOption{sdkmetric.WithInterval(15 * time.Second)}
}
