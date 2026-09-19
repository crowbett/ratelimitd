// Command client is a minimal smoke-test client: one unary Echo, one EchoStream and
// a health check against a running server.
//
//	go run ./cmd/client -addr localhost:50051 -msg hello -count 3
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthv1 "google.golang.org/grpc/health/grpc_health_v1"

	echov1 "github.com/crowbett/ratelimitd/gen/echo/v1"
)

func main() {
	addr := flag.String("addr", envOr("GRPC_ADDR", "localhost:50051"), "server address")
	msg := flag.String("msg", "hello", "message to echo")
	count := flag.Int("count", 3, "number of stream messages")
	timeout := flag.Duration("timeout", 5*time.Second, "per-call timeout")
	flag.Parse()

	if err := run(*addr, *msg, int32(*count), *timeout); err != nil { //nolint:gosec // flag value, small
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run(addr, msg string, count int32, timeout time.Duration) error {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	// Health.
	hc, err := healthv1.NewHealthClient(conn).Check(ctx, &healthv1.HealthCheckRequest{Service: echov1.EchoService_ServiceDesc.ServiceName})
	if err != nil {
		return fmt.Errorf("health check: %w", err)
	}
	fmt.Printf("health: %s\n", hc.GetStatus())

	client := echov1.NewEchoServiceClient(conn)

	// Unary.
	resp, err := client.Echo(ctx, &echov1.EchoRequest{Message: msg})
	if err != nil {
		return fmt.Errorf("echo: %w", err)
	}
	fmt.Printf("echo: %q (request_id=%s)\n", resp.GetMessage(), resp.GetRequestId())

	// Server stream.
	stream, err := client.EchoStream(ctx, &echov1.EchoStreamRequest{Message: msg, Count: count})
	if err != nil {
		return fmt.Errorf("echo stream: %w", err)
	}
	for {
		r, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("stream recv: %w", err)
		}
		fmt.Printf("stream[%d]: %q\n", r.GetSeq(), r.GetMessage())
	}
	return nil
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
