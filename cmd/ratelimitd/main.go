// Command ratelimitd is the rate limiter daemon: gRPC data plane, HTTP JSON
// adapter, the gRPC control plane (AdminService, loopback by default) and the
// admin HTTP listener in one process.
//
// Startup order (docs/PLAN.md): config -> logger -> obs -> stores (redis,
// local, fallback) -> policy registry -> gRPC + admin gRPC + HTTP + admin -> background
// goroutines (watcher, prober, sweeper, skew sampler). Teardown is staged:
// 1 stop accepting, 2 cancel the background ctx and wait, 3 close the Redis
// pool and flush OTel.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthv1 "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"

	ratelimitv1 "github.com/crowbett/ratelimitd/gen/ratelimit/v1"
	"github.com/crowbett/ratelimitd/internal/admin"
	"github.com/crowbett/ratelimitd/internal/config"
	"github.com/crowbett/ratelimitd/internal/httpapi"
	"github.com/crowbett/ratelimitd/internal/interceptor"
	"github.com/crowbett/ratelimitd/internal/limit"
	"github.com/crowbett/ratelimitd/internal/obs"
	"github.com/crowbett/ratelimitd/internal/policy"
	"github.com/crowbett/ratelimitd/internal/server"
	"github.com/crowbett/ratelimitd/internal/store/fallback"
	"github.com/crowbett/ratelimitd/internal/store/lease"
	"github.com/crowbett/ratelimitd/internal/store/local"
	"github.com/crowbett/ratelimitd/internal/store/redisx"
)

// version is injected at build time: -ldflags="-X main.version=v1.2.3".
var version = "dev"

// Budgets for the teardown stages that are not user-configurable (stage 1 is
// SHUTDOWN_TIMEOUT).
const (
	backgroundStopBudget = 5 * time.Second
	otelFlushBudget      = 5 * time.Second
	skewSampleEvery      = 10 * time.Second
	skewWarnAbove        = 250 * time.Millisecond
	startupPingTimeout   = 2 * time.Second
)

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

	// ---- observability -------------------------------------------------
	otelStop, err := obs.Setup(ctx, obs.Config{
		Enabled: cfg.OTel.Enabled, ServiceName: cfg.ServiceName, ServiceVersion: version,
		Environment: cfg.Env, Endpoint: cfg.OTel.Endpoint, Logger: logger,
	})
	if err != nil {
		return fmt.Errorf("otel setup: %w", err)
	}
	snap := admin.NewSnapshot()
	if err := obs.RegisterSnapshot(snap); err != nil {
		return fmt.Errorf("otel instruments: %w", err)
	}

	// ---- policies -------------------------------------------------------
	set, err := policy.Load(cfg.Policy.File)
	if err != nil {
		return fmt.Errorf("load policies %s: %w", cfg.Policy.File, err)
	}
	reg := policy.NewRegistry(set, logger)
	snap.SetPolicyVersion(set.Version)
	logger.Info("policies loaded", "file", cfg.Policy.File, "version", set.Version, "tenants", len(set.Tenants), "global", len(set.Global))

	// ---- stores ---------------------------------------------------------
	loc := local.New(local.Config{Shards: cfg.Local.Shards, IdleTTL: cfg.Local.IdleTTL, MaxKeys: cfg.Local.MaxKeys})
	rdb, err := redisx.New(redisx.Config{
		Addr: cfg.Redis.Addr, PoolSize: cfg.Redis.PoolSize, Cluster: cfg.Redis.Cluster,
		DialTimeout: cfg.Redis.Timeout, ReadTimeout: cfg.Redis.Timeout, WriteTimeout: cfg.Redis.Timeout,
	}, logger, snap)
	if err != nil {
		return fmt.Errorf("redis backend: %w", err)
	}
	mode, err := fallback.ParseMode(cfg.Fallback.Mode)
	if err != nil {
		return fmt.Errorf("FALLBACK_MODE: %w", err)
	}
	store := fallback.New(rdb, loc.Scale(cfg.Fallback.Divisor), fallback.Config{
		Mode: mode, DegradedDivisor: cfg.Fallback.Divisor,
		FailureRatio: cfg.Breaker.Ratio, MinSamples: cfg.Breaker.Min, LatencyBudget: cfg.Redis.Timeout,
		OpenFor: cfg.Breaker.OpenFor, ProbeEvery: cfg.Breaker.Probe,
	}, logger, snap)
	// Shared policies (one counter across subjects) are served from leased
	// budget; everything else goes to the fallback store unchanged.
	var degradedLease lease.Source = loc.Scale(cfg.Fallback.Divisor)
	switch mode {
	case fallback.ModeFailOpen:
		degradedLease = lease.FailOpen{}
	case fallback.ModeFailClosed:
		degradedLease = lease.FailClosed{RetryAfter: cfg.Breaker.Probe}
	case fallback.ModeLocal:
	}
	breakerOpen := func() bool { open, _, _ := store.State(); return open }
	limiter := lease.New(store, rdb, degradedLease, breakerOpen, lease.Config{Span: cfg.Lease.Span, TTL: cfg.Lease.TTL})

	// Startup probe: a reachable Redis must agree on the time (Decision 3); an
	// unreachable one is not fatal - the breaker settles into FALLBACK_MODE.
	var redisUp atomic.Bool
	if skew, perr := probeRedis(ctx, rdb, cfg.MaxClockSkew, logger); perr != nil {
		logger.Warn("redis unreachable at startup; degraded mode until the prober recovers", "addr", cfg.Redis.Addr, "mode", mode, "err", perr)
	} else {
		if skew > cfg.MaxClockSkew || -skew > cfg.MaxClockSkew {
			return fmt.Errorf("clock skew vs redis %v exceeds MAX_CLOCK_SKEW %v", skew, cfg.MaxClockSkew)
		}
		redisUp.Store(true)
		snap.SetClockSkew(skew)
	}

	// ---- listeners ------------------------------------------------------
	lc := &net.ListenConfig{}
	grpcLis, err := lc.Listen(ctx, "tcp", cfg.GRPCAddr)
	if err != nil {
		return fmt.Errorf("listen grpc %s: %w", cfg.GRPCAddr, err)
	}
	httpLis, err := lc.Listen(ctx, "tcp", cfg.HTTPAddr)
	if err != nil {
		return fmt.Errorf("listen http %s: %w", cfg.HTTPAddr, err)
	}
	adminGRPCLis, err := lc.Listen(ctx, "tcp", cfg.AdminGRPCAddr)
	if err != nil {
		return fmt.Errorf("listen admin grpc %s: %w", cfg.AdminGRPCAddr, err)
	}
	adminLis, err := lc.Listen(ctx, "tcp", cfg.AdminAddr)
	if err != nil {
		return fmt.Errorf("listen admin %s: %w", cfg.AdminAddr, err)
	}

	srvCfg := server.Config{TrustClientClock: cfg.TrustClientClock}
	if cfg.AdminToken == "" {
		if isLoopback(cfg.AdminGRPCAddr) {
			logger.Info("ADMIN_TOKEN unset: AdminService is unauthenticated, reachable from this host only", "admin_grpc_addr", cfg.AdminGRPCAddr)
		} else {
			logger.Warn("ADMIN_TOKEN unset: AdminService (policy CRUD, ResetKey, Stats) answers unauthenticated on a non-loopback ADMIN_GRPC_ADDR; "+
				"set ADMIN_TOKEN, bind it to loopback or front it with an authenticating proxy", "admin_grpc_addr", cfg.AdminGRPCAddr)
		}
	}
	startedAt := time.Now()
	healthSrv := health.NewServer()
	grpcSrv := newDataPlane(logger, server.NewCheckServer(limiter, reg, snap, logger, srvCfg), healthSrv)
	adminRPC := server.NewAdminServer(reg, limiter, snap, logger)
	adminRPC.SetBuildInfo(version, startedAt)
	adminHealth := health.NewServer()
	adminGRPCSrv := newControlPlane(logger, cfg.AdminToken, adminRPC, adminHealth)

	httpSrv := &http.Server{
		Handler:           httpapi.New(limiter, reg, snap, logger, srvCfg),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	// /readyz: policies are loaded (they are, or we would not be here) and the
	// limiter can answer - Redis reachable, or a degraded mode that answers
	// (local / fail_open: the fallback store serves every failed primary call
	// from the degraded path even before the breaker trips).
	ready := func() error {
		if redisUp.Load() || mode != fallback.ModeFailClosed {
			return nil
		}
		return errors.New("redis unreachable and FALLBACK_MODE=fail_closed")
	}
	adminSrv := admin.NewServer(cfg.AdminAddr, admin.Deps{
		Snapshot: snap, Ready: ready, Version: version, StartedAt: startedAt, Log: logger,
		Build: map[string]string{"version": version, "go_version": runtime.Version()},
		Breaker: func() admin.BreakerState {
			open, since, m := store.State()
			return admin.BreakerState{Open: open, Since: since, Mode: m.String()}
		},
	})

	// ---- background goroutines (stage 2 stops them) -----------------------
	bgCtx, bgCancel := context.WithCancel(context.Background())
	defer bgCancel()
	bg, bgCtx := errgroup.WithContext(bgCtx)
	bg.Go(func() error { return ignoreCanceled(reg.Watch(bgCtx, cfg.Policy.File, cfg.Policy.Poll)) })
	bg.Go(func() error { return ignoreCanceled(store.Run(bgCtx)) })
	bg.Go(func() error { return ignoreCanceled(loc.Run(bgCtx)) })
	bg.Go(func() error {
		sampleSkew(bgCtx, rdb, snap, &redisUp, cfg.MaxClockSkew, logger)
		return nil
	})
	bg.Go(func() error {
		mirrorRegistry(bgCtx, reg, snap, cfg.Policy.Poll)
		return nil
	})

	// ---- serve -----------------------------------------------------------
	srvs := servers{
		grpc: grpcSrv, adminGRPC: adminGRPCSrv, http: httpSrv, admin: adminSrv,
		health: []*health.Server{healthSrv, adminHealth},
	}
	errCh := make(chan error, 4)
	go func() {
		logger.Info("grpc listening", "addr", grpcLis.Addr().String(), "version", version,
			"redis", cfg.Redis.Addr, "policy_file", cfg.Policy.File, "fallback", cfg.Fallback.Mode)
		errCh <- srvErr("grpc", grpcSrv.Serve(grpcLis))
	}()
	go func() {
		logger.Info("admin grpc listening", "addr", adminGRPCLis.Addr().String(), "auth", cfg.AdminToken != "")
		errCh <- srvErr("admin grpc", adminGRPCSrv.Serve(adminGRPCLis))
	}()
	go func() {
		logger.Info("http listening", "addr", httpLis.Addr().String())
		errCh <- srvErr("http", httpSrv.Serve(httpLis))
	}()
	go func() {
		logger.Info("admin listening", "addr", adminLis.Addr().String())
		errCh <- srvErr("admin", adminSrv.Serve(adminLis))
	}()

	select {
	case <-ctx.Done():
		logger.Info("stop signal received; starting staged teardown", "stages", 3)
	case err := <-errCh:
		logger.Error("listener failed; starting staged teardown", "err", err)
		teardown(cfg, logger, srvs, bgCancel, bg, rdb, otelStop)
		return err
	}
	teardown(cfg, logger, srvs, bgCancel, bg, rdb, otelStop)
	return nil
}

// servers is every listener stage 1 of teardown drains.
type servers struct {
	grpc      *grpc.Server // data plane: RateLimitService, health, reflection
	adminGRPC *grpc.Server // control plane: AdminService, health, reflection
	http      *http.Server // HTTP JSON data plane
	admin     *http.Server // /metrics, /healthz, /readyz, /admin/stats
	health    []*health.Server
}

// teardown runs the three stages, each on its own budget, never on the
// (already cancelled) signal context.
func teardown(cfg config.Config, logger *slog.Logger, srvs servers,
	bgCancel context.CancelFunc, bg *errgroup.Group, rdb *redisx.Backend, otelStop obs.StopFunc,
) {
	// Stage 1: stop accepting. Health flips first so balancers stop routing;
	// both gRPC servers drain concurrently on the one stage-1 budget.
	for _, h := range srvs.health {
		h.Shutdown()
	}
	stage1, cancel1 := context.WithTimeout(context.Background(), cfg.StopTimeout)
	defer cancel1()
	grpcs := []*grpc.Server{srvs.grpc, srvs.adminGRPC}
	done := make(chan struct{})
	go func() {
		var wg sync.WaitGroup
		for _, g := range grpcs {
			wg.Go(g.GracefulStop)
		}
		wg.Wait()
		close(done)
	}()
	var timedOut bool
	select {
	case <-done:
	case <-stage1.Done():
		for _, g := range grpcs {
			g.Stop()
		}
		timedOut = true
	}
	for _, s := range []*http.Server{srvs.http, srvs.admin} {
		if err := s.Shutdown(stage1); err != nil {
			_ = s.Close()
			timedOut = true
		}
	}
	if timedOut {
		logger.Warn("teardown 1/3: drain timed out; connections forced closed", "timeout", cfg.StopTimeout)
	} else {
		logger.Info("teardown 1/3: grpc, admin grpc, http and admin drained")
	}

	// Stage 2: background goroutines.
	bgCancel()
	stage2, cancel2 := context.WithTimeout(context.Background(), backgroundStopBudget)
	defer cancel2()
	waited := make(chan error, 1)
	go func() { waited <- bg.Wait() }()
	select {
	case err := <-waited:
		if err != nil {
			logger.Warn("teardown 2/3: background goroutine error", "err", err)
		} else {
			logger.Info("teardown 2/3: watcher, prober, sweeper, skew sampler and registry mirror stopped")
		}
	case <-stage2.Done():
		logger.Warn("teardown 2/3: background goroutines did not stop in time", "timeout", backgroundStopBudget)
	}

	// Stage 3: Redis pool and OTel flush.
	if err := rdb.Close(); err != nil {
		logger.Warn("teardown 3/3: redis close", "err", err)
	}
	stage3, cancel3 := context.WithTimeout(context.Background(), otelFlushBudget)
	defer cancel3()
	if err := otelStop(stage3); err != nil {
		logger.Warn("teardown 3/3: otel flush", "err", err)
	} else {
		logger.Info("teardown 3/3: redis pool closed, otel flushed")
	}
}

// serverOptions is the interceptor chain and keepalive policy both gRPC
// servers share: Recover (outermost, catches panics from everything below) ->
// RequestID -> Logging -> extra. Extra interceptors run after logging, so an
// auth refusal is logged as Unauthenticated and the token never is.
func serverOptions(logger *slog.Logger, unary []grpc.UnaryServerInterceptor, stream []grpc.StreamServerInterceptor) []grpc.ServerOption {
	return []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(append([]grpc.UnaryServerInterceptor{
			interceptor.UnaryRecover(logger),
			interceptor.UnaryRequestID(),
			interceptor.UnaryLogging(logger),
		}, unary...)...),
		grpc.ChainStreamInterceptor(append([]grpc.StreamServerInterceptor{
			interceptor.StreamRecover(logger),
			interceptor.StreamRequestID(),
			interceptor.StreamLogging(logger),
		}, stream...)...),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionIdle: 5 * time.Minute,
			Time:              2 * time.Minute,
			Timeout:           20 * time.Second,
		}),
	}
}

// newDataPlane is the GRPC_ADDR server: RateLimitService, health and
// reflection. It never registers AdminService, so an admin RPC here is
// Unimplemented whatever token it carries.
func newDataPlane(logger *slog.Logger, check *server.CheckServer, hs *health.Server) *grpc.Server {
	srv := grpc.NewServer(serverOptions(logger, nil, nil)...)
	check.Register(srv)
	healthv1.RegisterHealthServer(srv, hs)
	hs.SetServingStatus("", healthv1.HealthCheckResponse_SERVING)
	hs.SetServingStatus(ratelimitv1.RateLimitService_ServiceDesc.ServiceName, healthv1.HealthCheckResponse_SERVING)
	reflection.Register(srv)
	return srv
}

// newControlPlane is the ADMIN_GRPC_ADDR server: AdminService behind the
// ADMIN_TOKEN gate (defence in depth on top of the loopback bind), health
// and reflection.
func newControlPlane(logger *slog.Logger, token string, adm *server.AdminServer, hs *health.Server) *grpc.Server {
	srv := grpc.NewServer(serverOptions(logger,
		[]grpc.UnaryServerInterceptor{server.UnaryAdminAuth(token)},
		[]grpc.StreamServerInterceptor{server.StreamAdminAuth(token)},
	)...)
	adm.Register(srv)
	healthv1.RegisterHealthServer(srv, hs)
	hs.SetServingStatus("", healthv1.HealthCheckResponse_SERVING)
	hs.SetServingStatus(ratelimitv1.AdminService_ServiceDesc.ServiceName, healthv1.HealthCheckResponse_SERVING)
	reflection.Register(srv)
	return srv
}

// isLoopback reports whether addr (host:port) binds a loopback interface
// only; an empty host (":50054") binds every interface.
func isLoopback(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil || host == "" {
		return false
	}
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// probeRedis pings once and measures the skew; a ping failure is returned.
func probeRedis(ctx context.Context, rdb *redisx.Backend, maxSkew time.Duration, logger *slog.Logger) (time.Duration, error) {
	pctx, cancel := context.WithTimeout(ctx, startupPingTimeout)
	defer cancel()
	if err := rdb.Ping(pctx); err != nil {
		return 0, err
	}
	skew, err := rdb.Skew(pctx, time.Now())
	if err != nil {
		return 0, fmt.Errorf("redis TIME: %w", err)
	}
	if skew > skewWarnAbove || -skew > skewWarnAbove {
		logger.Warn("clock skew vs redis", "skew", skew, "max", maxSkew)
	}
	logger.Info("redis reachable", "skew", skew)
	return skew, nil
}

// sampleSkew is the fourth background goroutine: every 10 s it refreshes
// rl_clock_skew_seconds and the readiness flag.
func sampleSkew(ctx context.Context, rdb *redisx.Backend, snap *admin.Snapshot, up *atomic.Bool, maxSkew time.Duration, logger *slog.Logger) {
	t := time.NewTicker(skewSampleEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		sctx, cancel := context.WithTimeout(ctx, startupPingTimeout)
		skew, err := rdb.Skew(sctx, time.Now())
		cancel()
		if err != nil {
			if up.Swap(false) {
				logger.Warn("redis skew sample failed", "err", err)
			}
			continue
		}
		if !up.Swap(true) {
			logger.Info("redis reachable again", "skew", skew)
		}
		snap.SetClockSkew(skew)
		if skew > maxSkew || -skew > maxSkew {
			logger.Error("clock skew vs redis exceeds MAX_CLOCK_SKEW; decisions may over-admit", "skew", skew, "max", maxSkew)
		} else if skew > skewWarnAbove || -skew > skewWarnAbove {
			logger.Warn("clock skew vs redis", "skew", skew)
		}
	}
}

// mirrorRegistry copies the registry's reload counters and the live policy
// version into the Snapshot every poll interval (policy imports no admin, so
// the registry cannot bump the metric itself).
func mirrorRegistry(ctx context.Context, reg *policy.Registry, snap *admin.Snapshot, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	var seen uint64
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		if n := reg.ReloadErrors(); n > seen {
			for range n - seen {
				snap.AddPolicyReloadError()
			}
			seen = n
		}
		if set := reg.Snapshot(); set != nil {
			snap.SetPolicyVersion(set.Version)
		}
	}
}

func ignoreCanceled(err error) error {
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

func srvErr(name string, err error) error {
	if err == nil || errors.Is(err, http.ErrServerClosed) || errors.Is(err, grpc.ErrServerStopped) {
		return nil
	}
	return fmt.Errorf("%s: %w", name, err)
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

// Compile-time proof that the stores satisfy the port.
var (
	_ limit.Store      = (*fallback.Store)(nil)
	_ limit.Store      = (*local.Backend)(nil)
	_ limit.Store      = (*redisx.Backend)(nil)
	_ limit.Store      = (*lease.Store)(nil)
	_ limit.BatchStore = (*lease.Store)(nil)
	_ lease.Source     = (*redisx.Backend)(nil)
	_ lease.Source     = (*local.Backend)(nil)
)
