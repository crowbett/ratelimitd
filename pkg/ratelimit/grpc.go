package ratelimit

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"

	ratelimitv1 "github.com/crowbett/ratelimitd/gen/ratelimit/v1"
	"github.com/crowbett/ratelimitd/internal/limit"
)

// MaxBatch is the largest BatchCheck the daemon accepts.
const MaxBatch = 64

// ErrUnspecified is wrapped when the daemon answers CODE_UNSPECIFIED; it is
// a protocol error, not a decision, so Middleware treats it as a client error.
var ErrUnspecified = errors.New("ratelimit: unspecified decision code")

// grpcClient is the Dial client.
type grpcClient struct {
	conn    *grpc.ClientConn
	rl      ratelimitv1.RateLimitServiceClient
	timeout time.Duration
	log     *slog.Logger
}

var (
	_ Client       = (*grpcClient)(nil)
	_ BatchChecker = (*grpcClient)(nil)
)

func dialGRPC(target string, o *options) (*grpcClient, error) {
	creds := o.creds
	if creds == nil {
		creds = insecure.NewCredentials()
	}
	dialOpts := make([]grpc.DialOption, 0, 2+len(o.dial))
	dialOpts = append(dialOpts,
		grpc.WithTransportCredentials(creds),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                30 * time.Second,
			Timeout:             10 * time.Second,
			PermitWithoutStream: true,
		}),
	)
	dialOpts = append(dialOpts, o.dial...)
	conn, err := grpc.NewClient(target, dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("ratelimit: dial %s: %w", target, err)
	}
	return &grpcClient{
		conn:    conn,
		rl:      ratelimitv1.NewRateLimitServiceClient(conn),
		timeout: o.timeout,
		log:     o.log,
	}, nil
}

// Check implements Client. A denial is Result{Allowed: false}, not an error;
// a transport failure (after one retry on UNAVAILABLE) is a wrapped error
// that still answers status.Code.
func (c *grpcClient) Check(ctx context.Context, req Request) (Result, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	in := toProto(req)
	resp, err := c.rl.Check(ctx, in)
	if err != nil && retryable(ctx, err) {
		c.log.Debug("ratelimit: retrying check after UNAVAILABLE", slog.String("tenant", req.Tenant))
		resp, err = c.rl.Check(ctx, in)
	}
	if err != nil {
		return Result{}, fmt.Errorf("ratelimit: check %s: %w", req.Tenant, err)
	}
	res, err := fromProto(resp)
	if err != nil {
		return Result{}, fmt.Errorf("ratelimit: check %s: %w", req.Tenant, err)
	}
	return res, nil
}

// BatchCheck implements BatchChecker with the same timeout and retry rule.
func (c *grpcClient) BatchCheck(ctx context.Context, reqs []Request) ([]Result, error) {
	if len(reqs) == 0 {
		return nil, nil
	}
	if len(reqs) > MaxBatch {
		return nil, fmt.Errorf("ratelimit: batch of %d exceeds %d", len(reqs), MaxBatch)
	}
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	in := &ratelimitv1.BatchCheckRequest{Checks: make([]*ratelimitv1.CheckRequest, len(reqs))}
	for i := range reqs {
		in.Checks[i] = toProto(reqs[i])
	}
	resp, err := c.rl.BatchCheck(ctx, in)
	if err != nil && retryable(ctx, err) {
		resp, err = c.rl.BatchCheck(ctx, in)
	}
	if err != nil {
		return nil, fmt.Errorf("ratelimit: batch check: %w", err)
	}
	results := resp.GetResults()
	if len(results) != len(reqs) {
		return nil, fmt.Errorf("ratelimit: batch check: %d results for %d requests", len(results), len(reqs))
	}
	out := make([]Result, len(results))
	for i, r := range results {
		out[i], err = fromProto(r)
		if err != nil {
			return nil, fmt.Errorf("ratelimit: batch check [%d]: %w", i, err)
		}
	}
	return out, nil
}

// Close implements Client.
func (c *grpcClient) Close() error {
	if err := c.conn.Close(); err != nil {
		return fmt.Errorf("ratelimit: close: %w", err)
	}
	return nil
}

// retryable is true for exactly one retry: UNAVAILABLE while the per-call
// deadline still has room.
func retryable(ctx context.Context, err error) bool {
	return status.Code(err) == codes.Unavailable && ctx.Err() == nil
}

func toProto(req Request) *ratelimitv1.CheckRequest {
	out := &ratelimitv1.CheckRequest{
		Tenant:  req.Tenant,
		Subject: req.Subject,
		Route:   req.Route,
		Method:  req.Method,
	}
	if req.Cost > 0 {
		out.Cost = uint32(req.Cost) //nolint:gosec // validated by the daemon (<= 10000)
	}
	if len(req.Descriptors) > 0 {
		out.Descriptors = make([]*ratelimitv1.Descriptor, len(req.Descriptors))
		for i, d := range req.Descriptors {
			out.Descriptors[i] = &ratelimitv1.Descriptor{Key: d.Key, Value: d.Value}
		}
	}
	return out
}

func fromProto(resp *ratelimitv1.CheckResponse) (Result, error) {
	var allowed bool
	switch resp.GetCode() {
	case ratelimitv1.Code_CODE_OK:
		allowed = true
	case ratelimitv1.Code_CODE_OVER_LIMIT:
		allowed = false
	case ratelimitv1.Code_CODE_UNSPECIFIED:
		return Result{}, ErrUnspecified
	default:
		return Result{}, fmt.Errorf("%w: code %d", ErrUnspecified, resp.GetCode())
	}
	backend := resp.GetBackend()
	return Result{
		Allowed:    allowed,
		Limit:      int(resp.GetLimit()),
		Remaining:  int(resp.GetRemaining()),
		RetryAfter: resp.GetRetryAfter().AsDuration(),
		ResetAfter: resp.GetResetAfter().AsDuration(),
		Policy:     resp.GetPolicy(),
		Backend:    backend,
		Degraded:   resp.GetDegraded() || backend == limit.BackendDegraded,
	}, nil
}
