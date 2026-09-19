package admin

import (
	"errors"
	"log/slog"
	"net/http"
	"time"
)

// ContentType is the Prometheus text exposition media type.
const ContentType = "text/plain; version=0.0.4; charset=utf-8"

// Deps is what the admin listener needs. Ready returns nil once policies are
// loaded and the first Redis PING has succeeded or the breaker has settled
// into a degraded mode; /readyz is 503 until then.
type Deps struct {
	Snapshot  *Snapshot
	Ready     func() error
	Build     map[string]string // labels of rl_build_info; version + go_version at least
	Version   string
	StartedAt time.Time
	Log       *slog.Logger
}

// NewServer returns the admin http.Server (ReadHeaderTimeout set) serving
// /metrics, /admin/stats, /healthz and /readyz on addr. STUB: every route 501.
func NewServer(addr string, d Deps) *http.Server {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", Metrics(d.Snapshot, d.Build))
	mux.Handle("GET /admin/stats", Stats(d))
	mux.Handle("GET /healthz", Healthz())
	mux.Handle("GET /readyz", Readyz(d.Ready))
	return &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
}

// Metrics serves the Snapshot in Prometheus text format 0.0.4, written by
// hand on a pooled buffer (no client library).
func Metrics(_ *Snapshot, _ map[string]string) http.Handler {
	return stub("admin.Metrics")
}

// Stats serves a JSON summary: version, uptime, Counters, breaker state.
func Stats(_ Deps) http.Handler {
	return stub("admin.Stats")
}

// Healthz is liveness: 200 while the process runs.
func Healthz() http.Handler {
	return stub("admin.Healthz")
}

// Readyz is readiness: 200 when ready() returns nil, else 503 with the reason.
func Readyz(_ func() error) http.Handler {
	return stub("admin.Readyz")
}

// ErrNotReady is what Ready returns before the limiter can answer.
var ErrNotReady = errors.New("not ready")

func stub(name string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "stub: "+name, http.StatusNotImplemented)
	})
}
