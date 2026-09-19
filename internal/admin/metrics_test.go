package admin

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

var update = flag.Bool("update", false, "rewrite golden files")

// fixture builds a Snapshot with one value in every family so the golden
// covers every render path; the fallback clock is frozen.
func fixture(t *testing.T) *Snapshot {
	t.Helper()
	s := NewSnapshot()
	base := time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC)
	now := base
	s.now = func() time.Time { return now }

	s.RecordDecision("acme", "api", true, "redis", 120*time.Microsecond)
	s.RecordDecision("acme", "api", true, "redis", 30*time.Microsecond)
	s.RecordDecision("acme", "api", false, "redis", 900*time.Microsecond)
	s.RecordDecision("acme", "search", false, "degraded", 15*time.Millisecond)
	s.RecordDecision("acme", "deny", false, "", 0)
	s.RecordDecision("beta", "per_ip", true, "local", 3*time.Second)
	s.RecordBackendError("redis", "timeout")
	s.RecordBackendError("redis", "timeout")
	s.RecordBackendError("redis", "noscript")
	s.SetFallbackActive("redis", true)
	now = base.Add(1500 * time.Millisecond)
	s.SetFallbackActive("redis", false)
	s.SetFallbackActive("redis", true)
	now = base.Add(2 * time.Second)
	s.AddScriptLoad()
	s.AddScriptLoad()
	s.SetPolicyVersion("6f1d2c3b4a596877")
	s.AddPolicyReloadError()
	s.SetClockSkew(-12500 * time.Microsecond)
	s.SetLocalKeys(1234)
	s.AddLocalEvicted(7)
	return s
}

func TestMetricsGolden(t *testing.T) {
	t.Parallel()
	h := Metrics(fixture(t), map[string]string{"version": "v1.2.3", "go_version": "go1.27.0", "quote": `a"b\c`})
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", http.NoBody))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); ct != ContentType {
		t.Errorf("Content-Type = %q", ct)
	}
	if cl := rec.Header().Get("Content-Length"); cl == "" {
		t.Error("Content-Length missing")
	}
	got := rec.Body.Bytes()

	golden := filepath.Join("testdata", "metrics.golden")
	if *update {
		if err := os.MkdirAll("testdata", 0o750); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(golden, got, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(golden)
	if err != nil {
		t.Fatalf("read golden (run with -update to create): %v", err)
	}
	want = bytes.ReplaceAll(want, []byte("\r\n"), []byte("\n"))
	if !bytes.Equal(got, want) {
		t.Errorf("/metrics differs from %s:\n--- got ---\n%s\n--- want ---\n%s", golden, got, want)
	}
}

func TestMetricsFamiliesPresent(t *testing.T) {
	t.Parallel()
	families := []string{
		"rl_decisions_total", "rl_decision_latency_seconds", "rl_backend_errors_total", "rl_fallback_active",
		"rl_fallback_seconds_total", "rl_redis_script_loads_total", "rl_policy_version_info",
		"rl_policy_reload_errors_total", "rl_clock_skew_seconds", "rl_local_keys", "rl_local_evicted_total", "rl_build_info",
	}
	var b bytes.Buffer
	mh, _ := Metrics(fixture(t), nil).(*metricsHandler)
	mh.render(&b)
	out := b.String()
	for _, f := range families {
		if !strings.Contains(out, "# TYPE "+f+" ") {
			t.Errorf("family %s missing", f)
		}
	}
	// Every HELP has exactly one TYPE and vice versa.
	if h, ty := strings.Count(out, "# HELP "), strings.Count(out, "# TYPE "); h != ty || h != len(families) {
		t.Errorf("HELP=%d TYPE=%d, want %d each", h, ty, len(families))
	}
	// Nil snapshot renders build info only.
	b.Reset()
	mh, _ = Metrics(nil, map[string]string{"version": "x"}).(*metricsHandler)
	mh.render(&b)
	if got := b.String(); !strings.Contains(got, `rl_build_info{go_version="`) || strings.Contains(got, "rl_decisions_total") {
		t.Errorf("nil snapshot render = %q", got)
	}
}

func TestHandlerRoutes(t *testing.T) {
	t.Parallel()
	ready := errors.New("policies not loaded")
	readyErr := ready
	started := time.Now().Add(-90 * time.Second)
	d := Deps{
		Snapshot:  fixture(t),
		Ready:     func() error { return readyErr },
		Version:   "v9",
		StartedAt: started,
		Breaker:   func() BreakerState { return BreakerState{Open: true, Mode: "local"} },
	}
	h := Handler(d)
	get := func(path string) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, http.NoBody))
		return rec
	}

	if rec := get("/healthz"); rec.Code != http.StatusOK || rec.Body.String() != "ok\n" {
		t.Errorf("/healthz = %d %q", rec.Code, rec.Body.String())
	}
	if rec := get("/readyz"); rec.Code != http.StatusServiceUnavailable || !strings.Contains(rec.Body.String(), ready.Error()) {
		t.Errorf("/readyz not ready = %d %q", rec.Code, rec.Body.String())
	}
	readyErr = nil
	if rec := get("/readyz"); rec.Code != http.StatusOK || rec.Body.String() != "ready\n" {
		t.Errorf("/readyz ready = %d %q", rec.Code, rec.Body.String())
	}
	if rec := get("/metrics"); rec.Code != http.StatusOK || rec.Header().Get("Content-Type") != ContentType {
		t.Errorf("/metrics = %d %q", rec.Code, rec.Header().Get("Content-Type"))
	}
	rec := get("/admin/stats")
	if rec.Code != http.StatusOK || rec.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("/admin/stats = %d %q", rec.Code, rec.Header().Get("Content-Type"))
	}
	var resp StatsResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Version != "v9" || resp.PolicyVersion != "6f1d2c3b4a596877" || resp.UptimeSeconds < 89 ||
		resp.Counters.Allowed != 3 || resp.Counters.Denied != 3 || resp.Breaker == nil || !resp.Breaker.Open {
		t.Errorf("stats = %+v", resp)
	}
	if rec := get("/nope"); rec.Code != http.StatusNotFound {
		t.Errorf("/nope = %d", rec.Code)
	}
	post := httptest.NewRecorder()
	h.ServeHTTP(post, httptest.NewRequest(http.MethodPost, "/metrics", http.NoBody))
	if post.Code != http.StatusMethodNotAllowed {
		t.Errorf("POST /metrics = %d", post.Code)
	}
	srv := NewServer("127.0.0.1:0", Deps{})
	if srv.ReadHeaderTimeout == 0 || srv.Handler == nil {
		t.Error("NewServer must set ReadHeaderTimeout and a handler")
	}
}

func TestReadyzNilFunc(t *testing.T) {
	t.Parallel()
	rec := httptest.NewRecorder()
	Readyz(nil).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", http.NoBody))
	if rec.Code != http.StatusOK {
		t.Errorf("nil ready = %d", rec.Code)
	}
}

func BenchmarkMetrics(b *testing.B) {
	s := NewSnapshot()
	tenants := []string{"acme", "beta", "gamma", "delta"}
	policies := []string{"api", "search", "per_ip", "global_ingress"}
	backends := []string{"redis", "local", "degraded"}
	for i := range 4096 {
		s.RecordDecision(tenants[i%4], policies[(i/4)%4], i%3 == 0, backends[(i/16)%3], time.Duration(i)*time.Microsecond)
	}
	s.RecordBackendError("redis", "timeout")
	s.SetFallbackActive("redis", true)
	s.SetPolicyVersion("abc")
	mh, _ := Metrics(s, map[string]string{"version": "bench"}).(*metricsHandler)
	var buf bytes.Buffer
	mh.render(&buf)
	b.SetBytes(int64(buf.Len()))
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		buf.Reset()
		mh.render(&buf)
	}
}

func BenchmarkRecordDecision(b *testing.B) {
	s := NewSnapshot()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			s.RecordDecision("acme", "api", i%5 != 0, "redis", time.Duration(i%1000)*time.Microsecond)
			i++
		}
	})
}
