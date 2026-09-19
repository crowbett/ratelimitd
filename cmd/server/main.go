// Command server is the gRPC server entrypoint.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthv1 "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"

	echov1 "github.com/crowbett/ratelimitd/gen/echo/v1"
	"github.com/crowbett/ratelimitd/internal/interceptor"
	"github.com/crowbett/ratelimitd/internal/server"
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
	addr := envOr("GRPC_ADDR", ":50051")
	shutdownTimeout, err := time.ParseDuration(envOr("SHUTDOWN_TIMEOUT", "10s"))
	if err != nil {
		return fmt.Errorf("SHUTDOWN_TIMEOUT: %w", err)
	}

	logger := newLogger()
	slog.SetDefault(logger)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	lis, err := (&net.ListenConfig{}).Listen(ctx, "tcp", addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", addr, err)
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

	// Application services.
	server.NewEchoServer().Register(srv)

	// Health: `grpc.health.v1.Health/Check` for load balancers and k8s grpc probes.
	healthSrv := health.NewServer()
	healthv1.RegisterHealthServer(srv, healthSrv)
	healthSrv.SetServingStatus(echov1.EchoService_ServiceDesc.ServiceName, healthv1.HealthCheckResponse_SERVING)

	// Reflection: lets grpcurl / evans discover services without the .proto files.
	reflection.Register(srv)

	errCh := make(chan error, 1)
	go func() {
		logger.Info("grpc server listening", "addr", lis.Addr().String(), "version", version)
		errCh <- srv.Serve(lis)
	}()

	select {
	case <-ctx.Done():
		logger.Info("shutdown signal received")
	case err := <-errCh:
		return fmt.Errorf("serve: %w", err)
	}

	// Flip health to NOT_SERVING first so balancers stop routing new calls, then drain.
	healthSrv.Shutdown()

	done := make(chan struct{})
	go func() {
		srv.GracefulStop()
		close(done)
	}()
	select {
	case <-done:
		logger.Info("server stopped gracefully")
	case <-time.After(shutdownTimeout):
		logger.Warn("graceful stop timed out; forcing", "timeout", shutdownTimeout)
		srv.Stop()
	}
	return nil
}

func newLogger() *slog.Logger {
	var level slog.Level
	if err := level.UnmarshalText([]byte(envOr("LOG_LEVEL", "info"))); err != nil {
		level = slog.LevelInfo
	}
	opts := &slog.HandlerOptions{Level: level}
	var h slog.Handler
	if strings.EqualFold(envOr("LOG_FORMAT", "json"), "text") {
		h = slog.NewTextHandler(os.Stdout, opts)
	} else {
		h = slog.NewJSONHandler(os.Stdout, opts)
	}
	return slog.New(h).With("service", "ratelimitd")
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
