// Package bench is the load harness behind `ratelimitctl bench`: an open-loop
// pacer (per-worker ticker with a missed-tick counter, so a slow server shows
// as backlog instead of closing the loop), 5 s warmup excluded, per-worker
// latency samples merged and sorted once at the end, and the allowed ratio
// compared with the closed-form expectation.
//
// Owner: workstream D (client & load). Conventions from perf-lab.
package bench

import (
	"context"
	"errors"
	"log/slog"
	"time"
)

// Config is one bench run.
type Config struct {
	Target   string        // gRPC or HTTP address
	Proto    string        // "grpc" | "http"
	Backend  string        // "redis" | "local" (informational; selected on the daemon)
	RPS      int           // offered rate, spread across Workers
	Keys     int           // distinct subjects; 1 = the hot-key ceiling run
	Workers  int           // 0 = GOMAXPROCS
	Duration time.Duration // measured window, after Warmup
	Warmup   time.Duration // default 5s
	Tenant   string
	Policy   struct{ Rate, Burst int64 } // for ExpectedAllowedRatio
}

// Result is what the bench prints and what goes into README.md.
type Result struct {
	Achieved      float64 // decisions/s over Duration
	Offered       int64
	Allowed       int64
	Denied        int64
	Errors        int64
	MissedTicks   int64
	P50, P99, Max time.Duration
	AllowedRatio  float64
	ExpectedRatio float64
	DegradedFor   time.Duration
}

// Run executes cfg and returns the Result. STUB.
func Run(_ context.Context, _ Config, _ *slog.Logger) (Result, error) {
	return Result{}, errors.New("stub: bench.Run")
}

// ExpectedAllowedRatio is the closed form min(offered, rate*d + burst) / offered
// for one key at a steady offered rate. STUB: returns 0.
func ExpectedAllowedRatio(_, _, _ int64, _ time.Duration) float64 { return 0 }
