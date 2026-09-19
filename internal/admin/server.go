package admin

import (
	"encoding/json"
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
	// Breaker reports the fallback store's state for /admin/stats; nil = unknown.
	Breaker func() BreakerState
}

// BreakerState is the fallback store's view for /admin/stats.
type BreakerState struct {
	Open  bool      `json:"open"`
	Since time.Time `json:"since,omitzero"`
	Mode  string    `json:"mode"`
}

// NewServer returns the admin http.Server (ReadHeaderTimeout set) serving
// /metrics, /admin/stats, /healthz and /readyz on addr.
func NewServer(addr string, d Deps) *http.Server {
	if d.Log == nil {
		d.Log = slog.Default()
	}
	return &http.Server{
		Addr:              addr,
		Handler:           Handler(d),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       2 * time.Minute,
		ErrorLog:          slog.NewLogLogger(d.Log.Handler(), slog.LevelWarn),
	}
}

// Handler mounts the admin surface on a fresh mux:
//
//	GET /metrics      Prometheus text
//	GET /admin/stats  JSON summary
//	GET /healthz      liveness, "ok"
//	GET /readyz       readiness, 503 until Deps.Ready returns nil
func Handler(d Deps) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", Metrics(d.Snapshot, d.Build))
	mux.Handle("GET /admin/stats", Stats(d))
	mux.Handle("GET /healthz", Healthz())
	mux.Handle("GET /readyz", Readyz(d.Ready))
	return mux
}

// StatsResponse is the /admin/stats body.
type StatsResponse struct {
	Version       string        `json:"version"`
	StartedAt     time.Time     `json:"started_at"`
	UptimeSeconds float64       `json:"uptime_seconds"`
	PolicyVersion string        `json:"policy_version"`
	Counters      Counters      `json:"counters"`
	Breaker       *BreakerState `json:"breaker,omitempty"`
}

// Stats serves a JSON summary: version, uptime, Counters, breaker state.
func Stats(d Deps) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		c := d.Snapshot.Counters()
		resp := StatsResponse{
			Version:       d.Version,
			StartedAt:     d.StartedAt,
			PolicyVersion: c.PolicyVersion,
			Counters:      c,
		}
		if !d.StartedAt.IsZero() {
			resp.UptimeSeconds = time.Since(d.StartedAt).Seconds()
		}
		if d.Breaker != nil {
			b := d.Breaker()
			resp.Breaker = &b
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		_ = json.NewEncoder(w).Encode(resp)
	})
}

// Healthz is liveness: 200 while the process runs.
func Healthz() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write([]byte("ok\n"))
	})
}

// Readyz is readiness: 200 when ready() returns nil, else 503 with the reason.
// A nil ready func means always ready.
func Readyz(ready func() error) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")
		if ready != nil {
			if err := ready(); err != nil {
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = w.Write([]byte("not ready: " + err.Error() + "\n"))
				return
			}
		}
		_, _ = w.Write([]byte("ready\n"))
	})
}

// ErrNotReady is what Ready returns before the limiter can answer.
var ErrNotReady = errors.New("not ready")
