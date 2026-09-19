package server

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// AdminServicePrefix is the full-method prefix of every AdminService RPC; the
// auth interceptors gate exactly these and let the data plane, health and
// reflection through untouched.
const AdminServicePrefix = "/ratelimit.v1.AdminService/"

// AuthorizationHeader is the metadata key the admin token travels in, as
// "Bearer <token>" (case-insensitive scheme).
const AuthorizationHeader = "authorization"

// adminAuth holds the token's digest so the comparison is constant time and
// independent of the presented length (subtle.ConstantTimeCompare returns
// early on a length mismatch; comparing SHA-256 sums removes that signal).
type adminAuth struct {
	sum [sha256.Size]byte
}

func newAdminAuth(token string) adminAuth {
	return adminAuth{sum: sha256.Sum256([]byte(token))}
}

// check returns nil when ctx carries the token, else an Unauthenticated
// status whose message never echoes what was presented.
func (a adminAuth) check(ctx context.Context) error {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return status.Error(codes.Unauthenticated, "admin token required: authorization: Bearer <ADMIN_TOKEN>")
	}
	vals := md.Get(AuthorizationHeader)
	if len(vals) == 0 {
		return status.Error(codes.Unauthenticated, "admin token required: authorization: Bearer <ADMIN_TOKEN>")
	}
	for _, v := range vals {
		scheme, tok, found := strings.Cut(v, " ")
		if !found || !strings.EqualFold(scheme, "Bearer") {
			continue
		}
		got := sha256.Sum256([]byte(strings.TrimSpace(tok)))
		if subtle.ConstantTimeCompare(got[:], a.sum[:]) == 1 {
			return nil
		}
	}
	return status.Error(codes.Unauthenticated, "invalid admin token")
}

// UnaryAdminAuth requires "authorization: Bearer <token>" on every unary RPC
// under AdminServicePrefix (policy CRUD, ResetKey, Stats) and passes every
// other method through. The token is compared in constant time and is never
// logged or echoed. An empty token disables the check; the caller decides
// whether an unauthenticated control plane is acceptable (cmd/ratelimitd
// warns at startup).
func UnaryAdminAuth(token string) grpc.UnaryServerInterceptor {
	if token == "" {
		return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
			return handler(ctx, req)
		}
	}
	a := newAdminAuth(token)
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if strings.HasPrefix(info.FullMethod, AdminServicePrefix) {
			if err := a.check(ctx); err != nil {
				return nil, err
			}
		}
		return handler(ctx, req)
	}
}

// StreamAdminAuth is the stream variant of UnaryAdminAuth (AdminService has
// no streaming RPCs today; the gate is installed so a future one is covered).
func StreamAdminAuth(token string) grpc.StreamServerInterceptor {
	if token == "" {
		return func(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
			return handler(srv, ss)
		}
	}
	a := newAdminAuth(token)
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if strings.HasPrefix(info.FullMethod, AdminServicePrefix) {
			if err := a.check(ss.Context()); err != nil {
				return err
			}
		}
		return handler(srv, ss)
	}
}

// WithAdminToken returns ctx with the outgoing metadata a client needs to
// pass UnaryAdminAuth; an empty token returns ctx unchanged.
func WithAdminToken(ctx context.Context, token string) context.Context {
	if token == "" {
		return ctx
	}
	return metadata.AppendToOutgoingContext(ctx, AuthorizationHeader, "Bearer "+token)
}
