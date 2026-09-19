package ratelimit

import (
	"context"

	"google.golang.org/grpc"
)

// UnaryInterceptor checks every unary RPC through c; kf derives the Request
// from the context (metadata) and the full method name. A denial returns
// codes.ResourceExhausted wrapping ErrDenied; a client error applies onErr.
// STUB: passes every call through.
func UnaryInterceptor(_ Client, _ func(context.Context, string) Request, _ ...OnError) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		return handler(ctx, req)
	}
}

// StreamInterceptor is the stream variant: one check when the stream opens.
// STUB: passes every stream through.
func StreamInterceptor(_ Client, _ func(context.Context, string) Request, _ ...OnError) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		return handler(srv, ss)
	}
}
