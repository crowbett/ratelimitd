// Command ratelimitctl is the CLI for a running ratelimitd.
//
//	ratelimitctl [-addr host:port] [-admin-addr host:port] [-timeout 5s] [-token t] <command> [flags]
//
//	health   [-admin]                       gRPC health of the data plane (-admin: the control plane)
//	check    -tenant t -subject s [-route /p -method GET -cost 1 -d key=value ...] [-n N]
//	policy   list [-tenant t] | get -tenant t -name n | delete -tenant t -name n
//	policy   upsert -tenant t -name n -algorithm gcra -rate 100 -per 1s -burst 20 -key "{tenant}:{subject}"
//	reset    -tenant t -subject s [-policy p]
//	stats
//	bench    [-rps 60000 -keys 10000 -duration 30s -warmup 5s -workers 0 -proto grpc|http -tenant t -rate R -burst B]
//
// check and bench talk to the data plane (-addr, env GRPC_ADDR); policy,
// reset and stats talk to the AdminService on the control plane
// (-admin-addr, env ADMIN_GRPC_ADDR, loopback by default) and send -token
// (env ADMIN_TOKEN) there only.
//
// A denied check prints the decision and exits 3; transport errors exit 1;
// usage errors exit 2.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthv1 "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/protobuf/types/known/durationpb"

	ratelimitv1 "github.com/crowbett/ratelimitd/gen/ratelimit/v1"
	"github.com/crowbett/ratelimitd/internal/bench"
	"github.com/crowbett/ratelimitd/internal/server"
)

const (
	exitOK     = 0
	exitError  = 1
	exitUsage  = 2
	exitDenied = 3
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	global := flag.NewFlagSet("ratelimitctl", flag.ContinueOnError)
	global.SetOutput(stderr)
	addr := global.String("addr", envOr("GRPC_ADDR", "localhost:50053"), "ratelimitd data-plane gRPC address (check, bench)")
	adminAddr := global.String("admin-addr", envOr("ADMIN_GRPC_ADDR", "127.0.0.1:50054"), "ratelimitd control-plane gRPC address (policy, reset, stats)")
	httpAddr := global.String("http", envOr("HTTP_ADDR", "http://localhost:8093"), "ratelimitd HTTP base URL (bench -proto http)")
	timeout := global.Duration("timeout", 5*time.Second, "per-call timeout")
	token := global.String("token", envOr("ADMIN_TOKEN", ""), "bearer token for policy/reset/stats when the daemon runs with ADMIN_TOKEN (env ADMIN_TOKEN)")
	global.Usage = func() {
		fmt.Fprintf(stderr, "usage: ratelimitctl [-addr host:port] [-admin-addr host:port] [-timeout d] [-token t] <health|check|policy|reset|stats|bench> [flags]\n\n")
		global.PrintDefaults()
	}
	if err := global.Parse(args); err != nil {
		return exitUsage
	}
	rest := global.Args()
	if len(rest) == 0 {
		global.Usage()
		return exitUsage
	}
	cmd, rest := rest[0], rest[1:]

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// grpc.NewClient does not connect until the first RPC, so the plane a
	// command does not use costs nothing.
	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Fprintln(stderr, "error: dial:", err)
		return exitError
	}
	defer conn.Close()
	adminConn, err := grpc.NewClient(*adminAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Fprintln(stderr, "error: dial admin:", err)
		return exitError
	}
	defer adminConn.Close()
	c := &cli{
		conn: conn, adminConn: adminConn, addr: *addr, httpAddr: *httpAddr,
		timeout: *timeout, token: *token, out: stdout, errOut: stderr,
	}

	var code int
	switch cmd {
	case "health":
		code = c.health(ctx, rest)
	case "check":
		code = c.check(ctx, rest)
	case "policy":
		code = c.policy(ctx, rest)
	case "reset":
		code = c.reset(ctx, rest)
	case "stats":
		code = c.stats(ctx)
	case "bench":
		code = c.bench(ctx, rest)
	default:
		fmt.Fprintf(stderr, "unknown command %q\n", cmd)
		global.Usage()
		code = exitUsage
	}
	return code
}

type cli struct {
	conn      *grpc.ClientConn // data plane: RateLimitService, health
	adminConn *grpc.ClientConn // control plane: AdminService, health
	addr      string
	httpAddr  string
	timeout   time.Duration
	token     string // sent as "authorization: Bearer <token>" on control-plane RPCs only
	out       io.Writer
	errOut    io.Writer
}

// call bounds a data-plane RPC; the admin token never travels there.
func (c *cli) call(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, c.timeout)
}

// adminCall bounds a control-plane RPC and attaches the admin token.
func (c *cli) adminCall(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(server.WithAdminToken(ctx, c.token), c.timeout)
}

func (c *cli) fail(err error) int {
	fmt.Fprintln(c.errOut, "error:", err)
	return exitError
}

func (c *cli) health(ctx context.Context, args []string) int {
	fs := flag.NewFlagSet("health", flag.ContinueOnError)
	fs.SetOutput(c.errOut)
	adminPlane := fs.Bool("admin", false, "check the control plane (-admin-addr) instead of the data plane")
	if err := fs.Parse(args); err != nil {
		return exitUsage
	}
	conn, call := c.conn, c.call
	if *adminPlane {
		conn, call = c.adminConn, c.adminCall
	}
	cctx, cancel := call(ctx)
	defer cancel()
	hc, err := healthv1.NewHealthClient(conn).Check(cctx, &healthv1.HealthCheckRequest{})
	if err != nil {
		return c.fail(fmt.Errorf("health check: %w", err))
	}
	fmt.Fprintf(c.out, "health: %s\n", hc.GetStatus())
	return exitOK
}

// descriptors collects repeated -d key=value flags.
type descriptors []*ratelimitv1.Descriptor

func (d *descriptors) String() string { return strconv.Itoa(len(*d)) }

func (d *descriptors) Set(s string) error {
	k, v, ok := strings.Cut(s, "=")
	if !ok || k == "" {
		return fmt.Errorf("descriptor %q: want key=value", s)
	}
	*d = append(*d, &ratelimitv1.Descriptor{Key: k, Value: v})
	return nil
}

func (c *cli) check(ctx context.Context, args []string) int {
	fs := flag.NewFlagSet("check", flag.ContinueOnError)
	fs.SetOutput(c.errOut)
	tenant := fs.String("tenant", "", "tenant id (required)")
	subject := fs.String("subject", "", "subject, e.g. an API key or client IP (required)")
	route := fs.String("route", "", "request route for policy matching")
	method := fs.String("method", "", "request method for policy matching")
	cost := fs.Int("cost", 1, "units to consume (1..10000)")
	n := fs.Int("n", 1, "repeat the check N times (prints each decision)")
	var ds descriptors
	fs.Var(&ds, "d", "descriptor key=value (repeatable)")
	if err := fs.Parse(args); err != nil {
		return exitUsage
	}
	if *tenant == "" || *subject == "" || *cost < 1 || *cost > 10000 {
		fmt.Fprintln(c.errOut, "check: -tenant and -subject are required; -cost is 1..10000")
		return exitUsage
	}
	client := ratelimitv1.NewRateLimitServiceClient(c.conn)
	code := exitOK
	for range *n {
		cctx, cancel := c.call(ctx)
		resp, err := client.Check(cctx, &ratelimitv1.CheckRequest{
			Tenant: *tenant, Subject: *subject, Descriptors: ds, Route: *route, Method: *method, Cost: uint32(*cost),
		})
		cancel()
		if err != nil {
			return c.fail(fmt.Errorf("check: %w", err))
		}
		allowed := resp.GetCode() == ratelimitv1.Code_CODE_OK
		verdict := "ALLOW"
		if !allowed {
			verdict = "DENY "
			code = exitDenied
		}
		fmt.Fprintf(c.out, "%s policy=%s limit=%d remaining=%d retry_after=%s reset_after=%s backend=%s degraded=%v\n",
			verdict, resp.GetPolicy(), resp.GetLimit(), resp.GetRemaining(),
			resp.GetRetryAfter().AsDuration(), resp.GetResetAfter().AsDuration(), resp.GetBackend(), resp.GetDegraded())
	}
	return code
}

func (c *cli) policy(ctx context.Context, args []string) int {
	if len(args) == 0 {
		fmt.Fprintln(c.errOut, "policy: want list | get | upsert | delete")
		return exitUsage
	}
	sub, args := args[0], args[1:]
	fs := flag.NewFlagSet("policy "+sub, flag.ContinueOnError)
	fs.SetOutput(c.errOut)
	tenant := fs.String("tenant", "", "tenant id (\"\" = global policies for list)")
	name := fs.String("name", "", "policy name")
	algo := fs.String("algorithm", "gcra", "gcra | token_bucket | sliding_window | sliding_window_exact")
	rate := fs.Int64("rate", 0, "events per -per")
	per := fs.Duration("per", time.Second, "period")
	burst := fs.Int64("burst", 0, "capacity")
	key := fs.String("key", "{tenant}:{subject}", "key template")
	disabled := fs.Bool("disabled", false, "upsert the policy disabled")
	if err := fs.Parse(args); err != nil {
		return exitUsage
	}
	client := ratelimitv1.NewAdminServiceClient(c.adminConn)
	cctx, cancel := c.adminCall(ctx)
	defer cancel()
	switch sub {
	case "list":
		resp, err := client.ListPolicies(cctx, &ratelimitv1.ListPoliciesRequest{Tenant: *tenant})
		if err != nil {
			return c.fail(err)
		}
		tw := tabwriter.NewWriter(c.out, 0, 4, 2, ' ', 0)
		fmt.Fprintln(tw, "tenant\tname\talgorithm\trate\tper\tburst\tkey\tenabled")
		for _, t := range resp.GetTenants() {
			for _, p := range t.GetPolicies() {
				fmt.Fprintf(tw, "%s\t%s\t%s\t%d\t%s\t%d\t%s\t%v\n", t.GetTenant(), p.GetName(), algoName(p.GetAlgorithm()),
					p.GetRate(), p.GetPer().AsDuration(), p.GetBurst(), p.GetKey(), p.GetEnabled())
			}
		}
		_ = tw.Flush()
		fmt.Fprintf(c.out, "version: %s\n", resp.GetVersion())
	case "get":
		if *tenant == "" || *name == "" {
			fmt.Fprintln(c.errOut, "policy get: -tenant and -name are required")
			return exitUsage
		}
		resp, err := client.GetPolicy(cctx, &ratelimitv1.GetPolicyRequest{Tenant: *tenant, Name: *name})
		if err != nil {
			return c.fail(err)
		}
		printPolicy(c.out, resp.GetPolicy())
	case "upsert":
		if *tenant == "" || *name == "" || *rate <= 0 || *burst <= 0 {
			fmt.Fprintln(c.errOut, "policy upsert: -tenant, -name, -rate > 0 and -burst > 0 are required")
			return exitUsage
		}
		a, ok := algoFromName(*algo)
		if !ok {
			fmt.Fprintf(c.errOut, "policy upsert: unknown -algorithm %q\n", *algo)
			return exitUsage
		}
		resp, err := client.UpsertPolicy(cctx, &ratelimitv1.UpsertPolicyRequest{Tenant: *tenant, Policy: &ratelimitv1.Policy{
			Name: *name, Algorithm: a, Rate: *rate, Per: durationpb.New(*per), Burst: *burst, Key: *key, Enabled: !*disabled,
		}})
		if err != nil {
			return c.fail(err)
		}
		printPolicy(c.out, resp.GetPolicy())
		fmt.Fprintf(c.out, "version: %s\n", resp.GetVersion())
	case "delete":
		if *tenant == "" || *name == "" {
			fmt.Fprintln(c.errOut, "policy delete: -tenant and -name are required")
			return exitUsage
		}
		resp, err := client.DeletePolicy(cctx, &ratelimitv1.DeletePolicyRequest{Tenant: *tenant, Name: *name})
		if err != nil {
			return c.fail(err)
		}
		fmt.Fprintf(c.out, "deleted; version: %s\n", resp.GetVersion())
	default:
		fmt.Fprintf(c.errOut, "policy: unknown subcommand %q\n", sub)
		return exitUsage
	}
	return exitOK
}

func (c *cli) reset(ctx context.Context, args []string) int {
	fs := flag.NewFlagSet("reset", flag.ContinueOnError)
	fs.SetOutput(c.errOut)
	tenant := fs.String("tenant", "", "tenant id (required)")
	subject := fs.String("subject", "", "subject (required)")
	pol := fs.String("policy", "", "one policy only (default: every policy of the subject)")
	if err := fs.Parse(args); err != nil {
		return exitUsage
	}
	if *tenant == "" || *subject == "" {
		fmt.Fprintln(c.errOut, "reset: -tenant and -subject are required")
		return exitUsage
	}
	cctx, cancel := c.adminCall(ctx)
	defer cancel()
	resp, err := ratelimitv1.NewAdminServiceClient(c.adminConn).ResetKey(cctx, &ratelimitv1.ResetKeyRequest{Tenant: *tenant, Subject: *subject, Policy: *pol})
	if err != nil {
		return c.fail(err)
	}
	fmt.Fprintf(c.out, "removed: %d\n", resp.GetRemoved())
	return exitOK
}

func (c *cli) stats(ctx context.Context) int {
	cctx, cancel := c.adminCall(ctx)
	defer cancel()
	s, err := ratelimitv1.NewAdminServiceClient(c.adminConn).Stats(cctx, &ratelimitv1.StatsRequest{})
	if err != nil {
		return c.fail(err)
	}
	tw := tabwriter.NewWriter(c.out, 0, 4, 2, ' ', 0)
	fmt.Fprintf(tw, "version\t%s\nstarted_at\t%s\npolicy_version\t%s\ndecisions_allowed\t%d\ndecisions_denied\t%d\nbackend_errors\t%d\nfallback_active\t%v\nfallback_total\t%s\nclock_skew\t%s\nlocal_keys\t%d\n",
		s.GetVersion(), s.GetStartedAt().AsTime().Format(time.RFC3339), s.GetPolicyVersion(), s.GetDecisionsAllowed(), s.GetDecisionsDenied(),
		s.GetBackendErrors(), s.GetFallbackActive(), s.GetFallbackTotal().AsDuration(), s.GetClockSkew().AsDuration(), s.GetLocalKeys())
	_ = tw.Flush()
	return exitOK
}

func (c *cli) bench(ctx context.Context, args []string) int {
	fs := flag.NewFlagSet("bench", flag.ContinueOnError)
	fs.SetOutput(c.errOut)
	var cfg bench.Config
	fs.IntVar(&cfg.RPS, "rps", 60000, "offered decisions per second")
	fs.IntVar(&cfg.Keys, "keys", 10000, "distinct subjects (1 = the hot-key ceiling)")
	fs.IntVar(&cfg.Workers, "workers", 0, "concurrent workers (0 = GOMAXPROCS)")
	fs.DurationVar(&cfg.Duration, "duration", 30*time.Second, "measured window after warmup")
	fs.DurationVar(&cfg.Warmup, "warmup", 5*time.Second, "warmup, excluded from the numbers")
	fs.StringVar(&cfg.Proto, "proto", "grpc", "grpc | http")
	fs.StringVar(&cfg.Backend, "backend", "redis", "informational label: which backend the daemon is running on")
	fs.StringVar(&cfg.Tenant, "tenant", "bench", "tenant whose policy is under test")
	fs.Int64Var(&cfg.Policy.Rate, "rate", 0, "policy rate (per second) for the expected-allowed ratio; 0 = skip the check")
	fs.Int64Var(&cfg.Policy.Burst, "burst", 0, "policy burst for the expected-allowed ratio")
	fs.IntVar(&cfg.Batch, "batch", 8, "subjects per tick: gRPC BatchCheck of N, or N sequential HTTP checks (1 = one Check per tick)")
	fs.DurationVar(&cfg.Timeout, "call-timeout", bench.DefaultTimeout, "per round-trip timeout; a miss counts as an error")
	fs.StringVar(&cfg.Route, "route", "", "route sent with every check")
	fs.StringVar(&cfg.Method, "method", "", "method sent with every check")
	minRPS := fs.Float64("pass-rps", 50000, "pass gate: minimum achieved decisions/s")
	maxP99 := fs.Duration("pass-p99", 5*time.Millisecond, "pass gate: p99 must be below this")
	tol := fs.Float64("pass-tolerance", 0.01, "pass gate: allowed ratio within this of expected")
	if err := fs.Parse(args); err != nil {
		return exitUsage
	}
	cfg.Target = c.addr
	if cfg.Proto == "http" {
		cfg.Target = c.httpAddr
	}
	log := slog.New(slog.NewTextHandler(c.errOut, nil))
	res, err := bench.Run(ctx, cfg, log)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return 130
		}
		return c.fail(fmt.Errorf("bench: %w", err))
	}
	fmt.Fprintln(c.out, res.String())
	fmt.Fprintln(c.out)
	fmt.Fprint(c.out, bench.TableHeader)
	if err := res.WriteTable(c.out); err != nil {
		return c.fail(err)
	}
	ok, reasons := res.Pass(*minRPS, *maxP99, *tol)
	if !ok {
		fmt.Fprintf(c.out, "\nFAIL: %s\n", strings.Join(reasons, "; "))
		return exitDenied
	}
	fmt.Fprintf(c.out, "\nPASS: >= %.0f/s, p99 < %s, allowed ratio within %.2f of expected, 0 errors\n", *minRPS, *maxP99, *tol)
	return exitOK
}

func printPolicy(w io.Writer, p *ratelimitv1.Policy) {
	fmt.Fprintf(w, "name=%s algorithm=%s rate=%d per=%s burst=%d key=%s enabled=%v\n", p.GetName(), algoName(p.GetAlgorithm()),
		p.GetRate(), p.GetPer().AsDuration(), p.GetBurst(), p.GetKey(), p.GetEnabled())
	if m := p.GetMatch(); m != nil {
		fmt.Fprintf(w, "  match: routes=%v methods=%v descriptors=%d\n", m.GetRoutes(), m.GetMethods(), len(m.GetDescriptors()))
	}
	for route, cost := range p.GetCost() {
		fmt.Fprintf(w, "  cost: %s = %d\n", route, cost)
	}
}

var algoNames = map[ratelimitv1.Algorithm]string{
	ratelimitv1.Algorithm_ALGORITHM_GCRA:                 "gcra",
	ratelimitv1.Algorithm_ALGORITHM_TOKEN_BUCKET:         "token_bucket",
	ratelimitv1.Algorithm_ALGORITHM_SLIDING_WINDOW:       "sliding_window",
	ratelimitv1.Algorithm_ALGORITHM_SLIDING_WINDOW_EXACT: "sliding_window_exact",
}

func algoName(a ratelimitv1.Algorithm) string {
	if n, ok := algoNames[a]; ok {
		return n
	}
	return a.String()
}

func algoFromName(s string) (ratelimitv1.Algorithm, bool) {
	for a, n := range algoNames {
		if n == s {
			return a, true
		}
	}
	return ratelimitv1.Algorithm_ALGORITHM_UNSPECIFIED, false
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
