package bench

import (
	"context"
	"encoding/json"
	"log/slog"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"

	ratelimitv1 "github.com/crowbett/ratelimitd/gen/ratelimit/v1"
	"github.com/crowbett/ratelimitd/internal/httpapi"
)

// fakeServer answers every check: allow/deny alternate per call (so the
// allowed ratio is 0.5), every other call is marked degraded, and each call
// sleeps delay.
type fakeServer struct {
	ratelimitv1.UnimplementedRateLimitServiceServer
	delay time.Duration
	calls atomic.Int64
	batch atomic.Int64
}

func (s *fakeServer) decide() *ratelimitv1.CheckResponse {
	n := s.calls.Add(1)
	code := ratelimitv1.Code_CODE_OK
	if n%2 == 0 {
		code = ratelimitv1.Code_CODE_OVER_LIMIT
	}
	return &ratelimitv1.CheckResponse{Code: code, Limit: 10, Degraded: n%4 == 0, Backend: "redis"}
}

func (s *fakeServer) Check(_ context.Context, _ *ratelimitv1.CheckRequest) (*ratelimitv1.CheckResponse, error) {
	if s.delay > 0 {
		time.Sleep(s.delay)
	}
	return s.decide(), nil
}

func (s *fakeServer) BatchCheck(_ context.Context, req *ratelimitv1.BatchCheckRequest) (*ratelimitv1.BatchCheckResponse, error) {
	s.batch.Add(1)
	if s.delay > 0 {
		time.Sleep(s.delay)
	}
	out := &ratelimitv1.BatchCheckResponse{Results: make([]*ratelimitv1.CheckResponse, len(req.GetChecks()))}
	for i := range req.GetChecks() {
		out.Results[i] = s.decide()
	}
	return out, nil
}

// startGRPC serves srv on a loopback port and returns its address.
func startGRPC(t *testing.T, srv *fakeServer) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	gs := grpc.NewServer()
	ratelimitv1.RegisterRateLimitServiceServer(gs, srv)
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)
	return lis.Addr().String()
}

func quietLog() *slog.Logger {
	return slog.New(slog.NewTextHandler(discard{}, &slog.HandlerOptions{Level: slog.LevelError}))
}

type discard struct{}

func (discard) Write(p []byte) (int, error) { return len(p), nil }

// The pacer-accuracy tests are deliberately not parallel: they measure wall
// clock against a ticker and would fight each other (and the race detector)
// for the same cores.
func TestRunPacerHoldsRPS(t *testing.T) {
	srv := &fakeServer{}
	cfg := Config{
		Target: startGRPC(t, srv), Proto: "grpc", RPS: 1000, Keys: 50, Workers: 16,
		Duration: 2 * time.Second, Warmup: 200 * time.Millisecond, Backend: "fake", Timeout: time.Second,
		Policy: struct{ Rate, Burst int64 }{Rate: 5, Burst: 1},
	}
	res, err := Run(context.Background(), cfg, quietLog())
	if err != nil {
		t.Fatal(err)
	}
	t.Log(res.String())
	if res.Errors != 0 {
		t.Fatalf("errors = %d: %s", res.Errors, res.FirstError)
	}
	// The pacer's own promise is the offered rate (sent + missed ticks).
	if offered := float64(res.Offered) / res.Elapsed.Seconds(); math.Abs(offered-1000)/1000 > 0.02 {
		t.Fatalf("offered %.0f/s, want 1000/s +-2%%", offered)
	}
	if dev := math.Abs(res.Achieved-1000) / 1000; dev > 0.05 {
		t.Fatalf("achieved %.0f/s deviates %.1f%% from 1000/s (missed ticks %d)", res.Achieved, dev*100, res.MissedTicks)
	}
	if res.Decisions != res.Allowed+res.Denied || res.Sent != res.Decisions {
		t.Fatalf("counters inconsistent: %+v", res)
	}
	if r := res.AllowedRatio; r < 0.49 || r > 0.51 {
		t.Fatalf("allowed ratio %.3f, want ~0.5", r)
	}
	if res.DegradedDecisions == 0 || res.DegradedFor <= 0 {
		t.Fatalf("degraded not counted: %+v", res)
	}
	if res.P50 <= 0 || res.P99 < res.P50 || res.Max < res.P99 {
		t.Fatalf("percentiles out of order: p50=%s p99=%s max=%s", res.P50, res.P99, res.Max)
	}
	// Each key gets ~1000/50 = 20 rps offered against rate 5 (burst ignored
	// after warmup): ~25 % expected, computed from what was actually offered.
	perKey := res.Decisions / int64(cfg.Keys)
	if want := ExpectedAllowedRatio(5, 0, perKey, res.Elapsed); res.ExpectedRatio != want || want < 0.23 || want > 0.27 {
		t.Fatalf("expected ratio %.4f, want %.4f (~0.25)", res.ExpectedRatio, want)
	}
	if srv.batch.Load() != 0 {
		t.Fatalf("batch=1 must use Check, saw %d BatchCheck calls", srv.batch.Load())
	}
}

func TestRunBatchUsesBatchCheck(t *testing.T) {
	srv := &fakeServer{}
	cfg := Config{
		Target: startGRPC(t, srv), Proto: "grpc", RPS: 2000, Keys: 100, Workers: 4, Batch: 8,
		Duration: time.Second, Warmup: 100 * time.Millisecond, Timeout: time.Second,
	}
	res, err := Run(context.Background(), cfg, quietLog())
	if err != nil {
		t.Fatal(err)
	}
	t.Log(res.String())
	if res.Errors != 0 || srv.batch.Load() == 0 {
		t.Fatalf("errors=%d (%s) batch calls=%d", res.Errors, res.FirstError, srv.batch.Load())
	}
	if dev := math.Abs(res.Achieved-2000) / 2000; dev > 0.05 {
		t.Fatalf("achieved %.0f/s deviates %.1f%% from 2000/s", res.Achieved, dev*100)
	}
	if res.Sent%8 != 0 {
		t.Fatalf("sent %d is not a multiple of the batch size", res.Sent)
	}
}

func TestRunCountsMissedTicks(t *testing.T) {
	t.Parallel()
	srv := &fakeServer{delay: 20 * time.Millisecond}
	cfg := Config{
		Target: startGRPC(t, srv), Proto: "grpc", RPS: 200, Keys: 10, Workers: 1,
		Duration: time.Second, Warmup: 100 * time.Millisecond, Timeout: time.Second,
	}
	res, err := Run(context.Background(), cfg, quietLog())
	if err != nil {
		t.Fatal(err)
	}
	t.Log(res.String())
	if res.MissedTicks == 0 {
		t.Fatalf("missed ticks = 0 with a 20ms server on 5ms ticks: %+v", res)
	}
	if res.Errors != 0 {
		t.Fatalf("errors = %d", res.Errors)
	}
	// ~50 calls/s fit into the window; the rest must show up as backlog.
	if res.Achieved > 60 {
		t.Fatalf("achieved %.0f/s from a 20ms server, want <= ~50", res.Achieved)
	}
	if res.Offered < 150 {
		t.Fatalf("offered %d = sent %d + missed %d, want ~200", res.Offered, res.Sent, res.MissedTicks)
	}
}

func TestRunHTTP(t *testing.T) {
	var calls atomic.Int64
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/check" {
			http.Error(w, "wrong route", http.StatusNotFound)
			return
		}
		var body httpapi.CheckRequest
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Tenant != "t1" || !strings.HasPrefix(body.Subject, "k") {
			http.Error(w, "bad body", http.StatusBadRequest)
			return
		}
		n := calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(httpapi.CheckResponse{Allowed: n%2 == 1, Limit: 10, Backend: "local"})
	}))
	t.Cleanup(ts.Close)
	cfg := Config{
		Target: strings.TrimPrefix(ts.URL, "http://"), Proto: "http", RPS: 500, Keys: 5, Workers: 8, Tenant: "t1",
		Duration: time.Second, Warmup: 100 * time.Millisecond, Timeout: time.Second,
	}
	res, err := Run(context.Background(), cfg, quietLog())
	if err != nil {
		t.Fatal(err)
	}
	t.Log(res.String())
	if res.Errors != 0 {
		t.Fatalf("errors = %d: %s", res.Errors, res.FirstError)
	}
	if dev := math.Abs(res.Achieved-500) / 500; dev > 0.05 {
		t.Fatalf("achieved %.0f/s deviates %.1f%% from 500/s", res.Achieved, dev*100)
	}
	if r := res.AllowedRatio; r < 0.45 || r > 0.55 {
		t.Fatalf("allowed ratio %.3f, want ~0.5", r)
	}
}

func TestRunErrorsAndCancel(t *testing.T) {
	t.Parallel()
	t.Run("unreachable target counts errors, not a failure", func(t *testing.T) {
		t.Parallel()
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		addr := lis.Addr().String()
		_ = lis.Close() // nothing listens here now
		cfg := Config{Target: addr, Proto: "grpc", RPS: 100, Workers: 1, Duration: 300 * time.Millisecond, Warmup: -1, Timeout: 50 * time.Millisecond}
		res, err := Run(context.Background(), cfg, quietLog())
		if err != nil {
			t.Fatal(err)
		}
		if res.Errors == 0 || res.Decisions != 0 {
			t.Fatalf("res = %+v", res)
		}
	})
	t.Run("cancelled context aborts", func(t *testing.T) {
		t.Parallel()
		addr := startGRPC(t, &fakeServer{})
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		cfg := Config{Target: addr, Proto: "grpc", RPS: 100, Workers: 1, Duration: 10 * time.Second, Warmup: -1}
		if _, err := Run(ctx, cfg, quietLog()); err == nil {
			t.Fatal("no error after the context expired")
		}
	})
	t.Run("config validation", func(t *testing.T) {
		t.Parallel()
		cases := []Config{
			{},
			{Target: "x", Proto: "quic", RPS: 1},
			{Target: "x", Proto: "grpc", RPS: 0},
			{Target: "x", Proto: "grpc", RPS: 10, Workers: 4, Batch: 65},
			{Target: "x", Proto: "grpc", RPS: 3, Workers: 4},
		}
		for i, c := range cases {
			if _, err := Run(context.Background(), c, quietLog()); err == nil {
				t.Errorf("case %d: accepted %+v", i, c)
			}
		}
	})
}

func TestExpectedAllowedRatio(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name          string
		rate, burst   int64
		offered       int64
		d             time.Duration
		want          float64
		wantTolerance float64
	}{
		{"under the limit", 1000, 200, 500, time.Second, 1, 0},
		{"exactly the limit", 1000, 0, 1000, time.Second, 1, 0},
		{"2x over, no burst", 1000, 0, 2000, time.Second, 0.5, 0},
		{"2x over with burst", 1000, 200, 2000, time.Second, 0.6, 0},
		{"burst alone covers", 10, 100, 50, time.Second, 1, 0},
		{"30s window", 1000, 200, 60_000, 30 * time.Second, (30_000 + 200) / 60_000.0, 0},
		{"fractional seconds", 1000, 0, 1000, 1500 * time.Millisecond, 1, 0},
		{"sub-second", 1000, 0, 1000, 250 * time.Millisecond, 0.25, 0},
		{"per-minute rate expressed per second", 1, 60, 120, time.Minute, 1, 0},
		{"zero offered", 1000, 0, 0, time.Second, 1, 0},
		{"zero duration is burst only", 1000, 5, 10, 0, 0.5, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := ExpectedAllowedRatio(tc.rate, tc.burst, tc.offered, tc.d)
			if math.Abs(got-tc.want) > tc.wantTolerance+1e-12 {
				t.Fatalf("ExpectedAllowedRatio(%d, %d, %d, %s) = %v, want %v", tc.rate, tc.burst, tc.offered, tc.d, got, tc.want)
			}
		})
	}
}

func TestResultRendering(t *testing.T) {
	t.Parallel()
	r := Result{
		Achieved: 52345.6, P50: 812 * time.Microsecond, P99: 3400 * time.Microsecond, Max: 1200 * time.Millisecond,
		AllowedRatio: 0.1234, ExpectedRatio: 0.1250, Errors: 0, MissedTicks: 3, DegradedDecisions: 0,
		Config: Config{Proto: "grpc", Backend: "redis", RPS: 60000, Keys: 10000, Workers: 16, Batch: 8},
	}
	var b strings.Builder
	if err := r.WriteTable(&b); err != nil {
		t.Fatal(err)
	}
	want := "| grpc | redis | 60000 | 10000 | 16 | 8 | 52346 | 0.81ms | 3.40ms | 1.2s | 0.1234 (0.1250) | 0 | 3 | 0 |\n"
	if b.String() != want {
		t.Fatalf("row =\n%s\nwant\n%s", b.String(), want)
	}
	if !strings.HasPrefix(TableHeader, "| proto |") || strings.Count(TableHeader, "|") != strings.Count(want, "|")*2 {
		t.Fatalf("header does not match the row: %q", TableHeader)
	}
	if s := r.String(); !strings.Contains(s, "achieved=52346/s") || !strings.Contains(s, "p99=3.4ms") {
		t.Fatalf("String() = %q", s)
	}
	ok, reasons := r.Pass(50_000, 5*time.Millisecond, 0.01)
	if !ok {
		t.Fatalf("Pass = false: %v", reasons)
	}
	r.Errors, r.P99 = 2, 6*time.Millisecond
	ok, reasons = r.Pass(50_000, 5*time.Millisecond, 0.01)
	if ok || len(reasons) != 2 {
		t.Fatalf("Pass = %v %v, want two failed clauses", ok, reasons)
	}
}
