package obs

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/crowbett/ratelimitd/internal/admin"
)

func TestSetupDisabledIsNoop(t *testing.T) {
	t.Parallel()
	stop, err := Setup(t.Context(), Config{Enabled: false})
	if err != nil {
		t.Fatal(err)
	}
	if stop == nil {
		t.Fatal("stop must not be nil")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	if err := stop(ctx); err != nil {
		t.Fatalf("no-op stop: %v", err)
	}
	if err := stop(ctx); err != nil {
		t.Fatalf("no-op stop twice: %v", err)
	}
	// Registering on the global no-op meter is free and error-free.
	if err := RegisterSnapshot(admin.NewSnapshot()); err != nil {
		t.Fatalf("RegisterSnapshot on the no-op meter: %v", err)
	}
	if err := RegisterSnapshot(nil); err != nil {
		t.Fatalf("RegisterSnapshot(nil): %v", err)
	}
	if Tracer() == nil {
		t.Fatal("Tracer() nil")
	}
}

func TestEndpointOptions(t *testing.T) {
	t.Parallel()
	tests := []struct {
		endpoint     string
		wantInsecure bool
		wantURL      string
	}{
		{"localhost:4317", true, "http://localhost:4317"},
		{"http://collector:4317", true, "http://collector:4317"},
		{"https://otel.example.com", false, "https://otel.example.com"},
		{"HTTPS://otel.example.com", false, "HTTPS://otel.example.com"},
		{"", true, ""},
	}
	for _, tt := range tests {
		t.Run(tt.endpoint, func(t *testing.T) {
			t.Parallel()
			if got := insecure(tt.endpoint); got != tt.wantInsecure {
				t.Errorf("insecure = %v, want %v", got, tt.wantInsecure)
			}
			if tt.endpoint != "" {
				if got := normalizeEndpoint(tt.endpoint); got != tt.wantURL {
					t.Errorf("normalizeEndpoint = %q, want %q", got, tt.wantURL)
				}
			}
			if n := len(traceOptions(tt.endpoint)); n < 1 || n > 3 {
				t.Errorf("traceOptions returned %d options", n)
			}
			if n := len(metricOptions(tt.endpoint)); n < 1 || n > 3 {
				t.Errorf("metricOptions returned %d options", n)
			}
		})
	}
	if got := readerOptions(3 * time.Second); len(got) != 1 {
		t.Errorf("readerOptions(3s) = %d options", len(got))
	}
}

func TestLogHandlerAddsTraceIDs(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	log := slog.New(NewLogHandler(slog.NewJSONHandler(&buf, nil)).WithAttrs([]slog.Attr{slog.String("svc", "x")}).WithGroup("g"))

	log.InfoContext(t.Context(), "no span")
	if strings.Contains(buf.String(), TraceIDKey) {
		t.Errorf("trace_id present without a span: %s", buf.String())
	}

	buf.Reset()
	tp := sdktrace.NewTracerProvider()
	ctx, span := tp.Tracer("t").Start(t.Context(), "s")
	log.InfoContext(ctx, "with span")
	span.End()
	out := buf.String()
	if !strings.Contains(out, span.SpanContext().TraceID().String()) || !strings.Contains(out, span.SpanContext().SpanID().String()) {
		t.Errorf("log lacks the span ids: %s", out)
	}
	if !strings.Contains(out, `"svc":"x"`) {
		t.Errorf("WithAttrs lost: %s", out)
	}
}

// TestRegisterSnapshotReadsCounters installs a manual-reader provider as the
// global one (RegisterSnapshot reads otel.Meter), so it is not parallel.
func TestRegisterSnapshotReadsCounters(t *testing.T) { //nolint:paralleltest // swaps the global meter provider
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	swapMeterProvider(t, mp)

	s := admin.NewSnapshot()
	s.RecordDecision("acme", "api", true, "redis", time.Millisecond)
	s.RecordDecision("acme", "api", false, "redis", time.Millisecond)
	s.RecordBackendError("redis", "timeout")
	s.SetFallbackActive("redis", true)
	s.AddScriptLoad()
	s.SetLocalKeys(7)
	if err := RegisterSnapshot(s); err != nil {
		t.Fatal(err)
	}

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(t.Context(), &rm); err != nil {
		t.Fatal(err)
	}
	got := map[string]int{}
	for _, sm := range rm.ScopeMetrics {
		if sm.Scope.Name != MeterName {
			continue
		}
		for _, m := range sm.Metrics {
			switch data := m.Data.(type) {
			case metricdata.Sum[int64]:
				for _, dp := range data.DataPoints {
					got[m.Name] += int(dp.Value)
				}
			case metricdata.Gauge[int64]:
				for _, dp := range data.DataPoints {
					got[m.Name] += int(dp.Value)
				}
			case metricdata.Sum[float64], metricdata.Gauge[float64]:
				got[m.Name] = -1 // present
			}
		}
	}
	want := map[string]int{
		"rl_decisions_total": 2, "rl_backend_errors_total": 1, "rl_fallback_active": 1,
		"rl_redis_script_loads_total": 1, "rl_local_keys": 7, "rl_local_evicted_total": 0, "rl_policy_reload_errors_total": 0,
		"rl_fallback_seconds_total": -1, "rl_clock_skew_seconds": -1,
	}
	for name, n := range want {
		if got[name] != n {
			t.Errorf("%s = %d, want %d (all: %v)", name, got[name], n, got)
		}
	}
}

func swapMeterProvider(t *testing.T, mp *sdkmetric.MeterProvider) {
	t.Helper()
	prev := otel.GetMeterProvider()
	otel.SetMeterProvider(mp)
	t.Cleanup(func() {
		otel.SetMeterProvider(prev)
		_ = mp.Shutdown(context.Background())
	})
}
