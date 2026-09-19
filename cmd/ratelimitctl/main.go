// Command ratelimitctl is the CLI for a running ratelimitd.
//
//	ratelimitctl -addr localhost:50053 health
//
// STEP 1 STATE: health check only. check | policy get|list|upsert | reset |
// bench land in step 11.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthv1 "google.golang.org/grpc/health/grpc_health_v1"
)

func main() {
	addr := flag.String("addr", envOr("GRPC_ADDR", "localhost:50053"), "ratelimitd gRPC address")
	timeout := flag.Duration("timeout", 5*time.Second, "per-call timeout")
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "usage: ratelimitctl [flags] health\n\nflags:\n")
		flag.PrintDefaults()
	}
	flag.Parse()

	cmd := flag.Arg(0)
	if cmd == "" {
		cmd = "health"
	}
	if cmd != "health" {
		fmt.Fprintf(os.Stderr, "unknown command %q (only health is implemented in step 1)\n", cmd)
		os.Exit(2)
	}
	if err := runHealth(*addr, *timeout); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func runHealth(addr string, timeout time.Duration) error {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	hc, err := healthv1.NewHealthClient(conn).Check(ctx, &healthv1.HealthCheckRequest{})
	if err != nil {
		return fmt.Errorf("health check: %w", err)
	}
	fmt.Printf("health: %s\n", hc.GetStatus())
	return nil
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
