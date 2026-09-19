package obs

import (
	"context"
	"log/slog"

	"go.opentelemetry.io/otel/trace"
)

// Trace-correlation attribute names on log records. They match what the OTel
// collector / Loki expect, so Grafana's "logs -> trace" links work out of the box.
const (
	TraceIDKey = "trace_id"
	SpanIDKey  = "span_id"
)

// LogHandler is a slog.Handler that appends trace_id/span_id to every record
// logged with a context carrying a valid OTel span. It costs nothing when
// there is no span (the common case with OTel disabled).
type LogHandler struct {
	slog.Handler
}

// NewLogHandler wraps h so records carry trace_id/span_id when a span is in
// the context.
func NewLogHandler(h slog.Handler) slog.Handler { return &LogHandler{Handler: h} }

// Handle implements slog.Handler.
func (h *LogHandler) Handle(ctx context.Context, r slog.Record) error {
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
		r.AddAttrs(
			slog.String(TraceIDKey, sc.TraceID().String()),
			slog.String(SpanIDKey, sc.SpanID().String()),
		)
	}
	return h.Handler.Handle(ctx, r)
}

// WithAttrs implements slog.Handler, keeping the wrapper around the derived handler.
func (h *LogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &LogHandler{Handler: h.Handler.WithAttrs(attrs)}
}

// WithGroup implements slog.Handler, keeping the wrapper around the derived handler.
func (h *LogHandler) WithGroup(name string) slog.Handler {
	return &LogHandler{Handler: h.Handler.WithGroup(name)}
}
