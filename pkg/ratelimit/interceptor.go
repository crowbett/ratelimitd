package ratelimit

import (
	"context"
	"log/slog"
	"strconv"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// Trailing metadata keys set by the interceptors (lower-case, as gRPC
// metadata requires); the values are the HTTP header values verbatim.
const (
	MDLimit      = "ratelimit-limit"
	MDRemaining  = "ratelimit-remaining"
	MDReset      = "ratelimit-reset"
	MDRetryAfter = "retry-after"
)

// DeniedError is what the interceptors return on a denial: a gRPC
// RESOURCE_EXHAUSTED status that also unwraps to ErrDenied, so server-side
// logging can errors.Is it while the wire sees the status.
type DeniedError struct {
	Result Result
	st     *status.Status
}

func (e *DeniedError) Error() string { return e.st.Message() }

// GRPCStatus lets status.FromError / status.Code see RESOURCE_EXHAUSTED.
func (e *DeniedError) GRPCStatus() *status.Status { return e.st }

// Unwrap yields ErrDenied.
func (e *DeniedError) Unwrap() error { return ErrDenied }

// unavailableError is the FailClosed answer: RESOURCE_EXHAUSTED that unwraps
// to the client error.
type unavailableError struct {
	err error
	st  *status.Status
}

func (e *unavailableError) Error() string              { return e.st.Message() }
func (e *unavailableError) GRPCStatus() *status.Status { return e.st }
func (e *unavailableError) Unwrap() error              { return e.err }

// UnaryInterceptor checks every unary RPC through c; kf derives the Request
// from the context (metadata) and the full method name. A denial returns
// codes.ResourceExhausted wrapping ErrDenied with the RateLimit values in
// the trailing metadata; a client error applies onErr (default FailOpen).
func UnaryInterceptor(c Client, kf func(context.Context, string) Request, onErr ...OnError) grpc.UnaryServerInterceptor {
	mode := firstMode(onErr)
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if err := check(ctx, c, kf, info.FullMethod, mode, grpc.SetTrailer); err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
}

// StreamInterceptor is the stream variant: one check when the stream opens.
func StreamInterceptor(c Client, kf func(context.Context, string) Request, onErr ...OnError) grpc.StreamServerInterceptor {
	mode := firstMode(onErr)
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		setTrailer := func(_ context.Context, md metadata.MD) error {
			ss.SetTrailer(md)
			return nil
		}
		if err := check(ss.Context(), c, kf, info.FullMethod, mode, setTrailer); err != nil {
			return err
		}
		return handler(srv, ss)
	}
}

func firstMode(onErr []OnError) OnError {
	if len(onErr) > 0 {
		return onErr[0]
	}
	return FailOpen
}

// check runs one decision and returns nil to let the RPC through. trailer
// is grpc.SetTrailer for unary and ServerStream.SetTrailer for streams.
func check(ctx context.Context, c Client, kf func(context.Context, string) Request, method string, mode OnError,
	trailer func(context.Context, metadata.MD) error,
) error {
	req := kf(ctx, method)
	res, err := c.Check(ctx, req)
	if err != nil {
		slog.Default().Warn("ratelimit: check failed",
			slog.String("tenant", req.Tenant),
			slog.String("method", method),
			slog.String("on_error", mode.String()),
			slog.String("err", err.Error()))
		if mode == FailClosed {
			_ = trailer(ctx, metadata.Pairs(MDRetryAfter, "1"))
			return &unavailableError{
				err: err,
				st:  status.New(codes.ResourceExhausted, "rate limiter unavailable: "+err.Error()),
			}
		}
		return nil
	}
	md := metadata.Pairs(
		MDLimit, strconv.Itoa(res.Limit),
		MDRemaining, strconv.Itoa(max(res.Remaining, 0)),
		MDReset, strconv.FormatInt(ceilSeconds(res.ResetAfter), 10),
	)
	if res.Allowed {
		_ = trailer(ctx, md)
		return nil
	}
	retry := retryAfterSeconds(res.RetryAfter)
	md.Set(MDRetryAfter, strconv.FormatInt(retry, 10))
	_ = trailer(ctx, md)
	return &DeniedError{
		Result: res,
		st: status.New(codes.ResourceExhausted,
			"rate limited by policy "+strconv.Quote(res.Policy)+"; retry after "+strconv.FormatInt(retry, 10)+"s"),
	}
}
