// Package interceptor holds hand-written gRPC interceptors: request id, logging and
// panic recovery. No third-party middleware packages are required.
package interceptor

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"log/slog"
	"runtime/debug"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type ctxKey int

const requestIDKey ctxKey = iota

// RequestIDHeader is the metadata key used to propagate request identifiers.
const RequestIDHeader = "x-request-id"

// RequestIDFrom returns the request id stored in ctx, or "".
func RequestIDFrom(ctx context.Context) string {
	id, _ := ctx.Value(requestIDKey).(string)
	return id
}

// withRequestID reuses an incoming x-request-id or generates a new one, stores it in
// the context and sends it back as a response header.
func withRequestID(ctx context.Context) context.Context {
	var id string
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if v := md.Get(RequestIDHeader); len(v) > 0 {
			id = v[0]
		}
	}
	if id == "" {
		id = newRequestID()
	}
	// Best effort: SetHeader only fails if headers were already sent.
	_ = grpc.SetHeader(ctx, metadata.Pairs(RequestIDHeader, id))
	return context.WithValue(ctx, requestIDKey, id)
}

func newRequestID() string {
	var b [12]byte
	if _, err := rand.Read(b[:]); err != nil {
		return time.Now().UTC().Format("20060102150405.000000000")
	}
	return hex.EncodeToString(b[:])
}

// UnaryRequestID is the unary variant of the request id interceptor.
func UnaryRequestID() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		return handler(withRequestID(ctx), req)
	}
}

// StreamRequestID is the stream variant of the request id interceptor.
func StreamRequestID() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		return handler(srv, &wrappedStream{ServerStream: ss, ctx: withRequestID(ss.Context())})
	}
}

// UnaryLogging emits one structured log line per unary RPC.
func UnaryLogging(logger *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		start := time.Now()
		resp, err := handler(ctx, req)
		logRPC(ctx, logger, info.FullMethod, "unary", time.Since(start), err)
		return resp, err
	}
}

// StreamLogging emits one structured log line per stream RPC (when the stream ends).
func StreamLogging(logger *slog.Logger) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		start := time.Now()
		err := handler(srv, ss)
		logRPC(ss.Context(), logger, info.FullMethod, "stream", time.Since(start), err)
		return err
	}
}

func logRPC(ctx context.Context, logger *slog.Logger, method, kind string, d time.Duration, err error) {
	code := status.Code(err)
	level := slog.LevelInfo
	switch code {
	case codes.OK, codes.Canceled:
	case codes.Internal, codes.Unknown, codes.DataLoss, codes.Unimplemented, codes.Unavailable:
		level = slog.LevelError
	default:
		level = slog.LevelWarn
	}
	attrs := []slog.Attr{
		slog.String("method", method),
		slog.String("kind", kind),
		slog.String("code", code.String()),
		slog.Duration("duration", d),
		slog.String("request_id", RequestIDFrom(ctx)),
	}
	if err != nil {
		attrs = append(attrs, slog.String("error", err.Error()))
	}
	logger.LogAttrs(ctx, level, "rpc", attrs...)
}

// UnaryRecover converts handler panics into codes.Internal and logs the stack.
func UnaryRecover(logger *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		defer func() {
			// recover() only works when called directly from the deferred function.
			if rec := recover(); rec != nil {
				err = panicToError(ctx, logger, info.FullMethod, rec)
			}
		}()
		return handler(ctx, req)
	}
}

// StreamRecover converts stream handler panics into codes.Internal and logs the stack.
func StreamRecover(logger *slog.Logger) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) (err error) {
		defer func() {
			if rec := recover(); rec != nil {
				err = panicToError(ss.Context(), logger, info.FullMethod, rec)
			}
		}()
		return handler(srv, ss)
	}
}

func panicToError(ctx context.Context, logger *slog.Logger, method string, rec any) error {
	logger.ErrorContext(ctx, "panic recovered",
		slog.String("method", method),
		slog.Any("panic", rec),
		slog.String("request_id", RequestIDFrom(ctx)),
		slog.String("stack", string(debug.Stack())),
	)
	return status.Error(codes.Internal, "internal error")
}

// wrappedStream overrides Context so downstream handlers see the enriched context.
type wrappedStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (w *wrappedStream) Context() context.Context { return w.ctx }
