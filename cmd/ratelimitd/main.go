// Command ratelimitd is the rate limiter daemon: gRPC data plane, HTTP JSON
// adapter and the admin listener in one process.
//
// Startup order (docs/PLAN.md): config -> logger -> obs -> stores (redis,
// local, fallback) -> policy registry -> gRPC + HTTP + admin -> background
// goroutines (watcher, prober, sweeper, skew sampler). Teardown is staged:
// 1 stop accepting, 2 cancel the background ctx and wait, 3 close the Redis
// pool and flush OTel.
//
// STEP 1 STATE: config + logger + health/reflection gRPC only. The service
// wiring lands in step 11 once the workstreams deliver.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthv1 "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"

	"github.com/crowbett/ratelimitd/internal/config"
	"github.com/crowbett/ratelimitd/internal/interceptor"
	"github.com/crowbett/ratelimitd/internal/obs"
)

// version is injected at build time: -ldflags="-X main.version=v1.2.3".
var version = "dev"

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "fatal:", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	logger := newLogger(cfg)
	slog.SetDefault(logger)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	lis, err := (&net.ListenConfig{}).Listen(ctx, "tcp", cfg.GRPCAddr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", cfg.GRPCAddr, err)
	}

	srv := grpc.NewServer(
		grpc.ChainUnaryInterceptor(
			interceptor.UnaryRecover(logger), // outermost: catches panics from everything below
			interceptor.UnaryRequestID(),
			interceptor.UnaryLogging(logger),
		),
		grpc.ChainStreamInterceptor(
			interceptor.StreamRecover(logger),
			interceptor.StreamRequestID(),
			interceptor.StreamLogging(logger),
		),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionIdle: 5 * time.Minute,
			Time:              2 * time.Minute,
			Timeout:           20 * time.Second,
		}),
	)

	// Health: `grpc.health.v1.Health/Check` for load balancers and k8s grpc probes.
	// Per-service SERVING statuses are set in step 11 when the services register.
	healthSrv := health.NewServer()
	healthv1.RegisterHealthServer(srv, healthSrv)
	healthSrv.SetServingStatus("", healthv1.HealthCheckResponse_SERVING)

	// Reflection: lets grpcurl / evans discover services without the .proto files.
	reflection.Register(srv)

	errCh := make(chan error, 1)
	go func() {
		logger.Info("grpc server listening", "addr", lis.Addr().String(), "version", version,
			"redis", cfg.Redis.Addr, "policy_file", cfg.Policy.File, "fallback", cfg.Fallback.Mode)
		errCh <- srv.Serve(lis)
	}()

	select {
	case <-ctx.Done():
		logger.Info("stop signal received; starting staged teardown")
	case err := <-errCh:
		return fmt.Errorf("serve: %w", err)
	}

	// Stage 1: flip health to NOT_SERVING so balancers stop routing, then drain.
	healthSrv.Shutdown()
	done := make(chan struct{})
	go func() {
		srv.GracefulStop()
		close(done)
	}()
	select {
	case <-done:
		logger.Info("teardown 1/3: grpc drained")
	case <-time.After(cfg.StopTimeout):
		logger.Warn("teardown 1/3: graceful stop timed out; forcing", "timeout", cfg.StopTimeout)
		srv.Stop()
	}
	logger.Info("teardown 2/3: background goroutines stopped (none yet)")
	logger.Info("teardown 3/3: redis pool closed, otel flushed (none yet)")
	return nil
}

func newLogger(cfg config.Config) *slog.Logger {
	opts := &slog.HandlerOptions{Level: cfg.LogLevel}
	var h slog.Handler
	if cfg.LogFormat == "text" {
		h = slog.NewTextHandler(os.Stdout, opts)
	} else {
		h = slog.NewJSONHandler(os.Stdout, opts)
	}
	return slog.New(obs.NewLogHandler(h)).With("service", cfg.ServiceName)
}
