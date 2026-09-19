package ratelimit

import (
	"context"
	"errors"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	ratelimitv1 "github.com/crowbett/ratelimitd/gen/ratelimit/v1"
)

// protected is the service behind the interceptor; it just answers OK.
func protected() *fakeServer {
	return &fakeServer{check: func(context.Context, *ratelimitv1.CheckRequest) (*ratelimitv1.CheckResponse, error) {
		return okResponse(1), nil
	}}
}

func methodKey(ctx context.Context, method string) Request {
	tenant := "anon"
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if v := md.Get("x-tenant"); len(v) > 0 {
			tenant = v[0]
		}
	}
	return Request{Tenant: tenant, Subject: "svc", Route: method}
}

// callThrough dials a bufconn server guarded by UnaryInterceptor(limiter)
// and returns the RPC's error plus trailers; srv.calls says whether the
// handler ran.
func callThrough(t *testing.T, limiter Client, onErr ...OnError) (*fakeServer, metadata.MD, error) {
	t.Helper()
	srv := protected()
	dialOpt := startBufconn(t, srv, grpc.ChainUnaryInterceptor(UnaryInterceptor(limiter, methodKey, onErr...)))
	// Any Client works to reach the guarded service; reuse Dial for the conn.
	c, err := Dial(context.Background(), "passthrough:///bufnet", dialOpt, WithTimeout(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	gc, ok := c.(*grpcClient)
	if !ok {
		t.Fatalf("Dial returned %T", c)
	}
	var trailer metadata.MD
	ctx := metadata.AppendToOutgoingContext(context.Background(), "x-tenant", "acme")
	_, err = gc.rl.Check(ctx, &ratelimitv1.CheckRequest{Tenant: "x", Subject: "y"}, grpc.Trailer(&trailer))
	return srv, trailer, err
}

func TestUnaryInterceptor(t *testing.T) {
	t.Parallel()
	t.Run("allowed passes with RateLimit trailers", func(t *testing.T) {
		t.Parallel()
		limiter := allowClient(Result{Limit: 100, Remaining: 42, ResetAfter: 2500 * time.Millisecond})
		srv, trailer, err := callThrough(t, limiter)
		if err != nil {
			t.Fatal(err)
		}
		if srv.calls.Load() != 1 {
			t.Fatal("handler did not run")
		}
		if limiter.last.Tenant != "acme" || limiter.last.Route != ratelimitv1.RateLimitService_Check_FullMethodName {
			t.Fatalf("limiter saw %+v", limiter.last)
		}
		wantTrailer(t, trailer, MDLimit, "100")
		wantTrailer(t, trailer, MDRemaining, "42")
		wantTrailer(t, trailer, MDReset, "3")
		if v := trailer.Get(MDRetryAfter); len(v) != 0 {
			t.Errorf("retry-after = %v on an allowed call", v)
		}
	})
	t.Run("denied is RESOURCE_EXHAUSTED with retry-after and no handler call", func(t *testing.T) {
		t.Parallel()
		limiter := denyClient(Result{Limit: 10, Remaining: 0, RetryAfter: 1200 * time.Millisecond, ResetAfter: 9 * time.Second, Policy: "api"})
		srv, trailer, err := callThrough(t, limiter)
		if status.Code(err) != codes.ResourceExhausted {
			t.Fatalf("code = %v (%v), want ResourceExhausted", status.Code(err), err)
		}
		if srv.calls.Load() != 0 {
			t.Fatal("handler ran on a denial")
		}
		wantTrailer(t, trailer, MDLimit, "10")
		wantTrailer(t, trailer, MDRemaining, "0")
		wantTrailer(t, trailer, MDReset, "9")
		wantTrailer(t, trailer, MDRetryAfter, "2")
		if msg := status.Convert(err).Message(); msg != `rate limited by policy "api"; retry after 2s` {
			t.Errorf("message = %q", msg)
		}
	})
	t.Run("client error fail-open by default", func(t *testing.T) {
		t.Parallel()
		srv, trailer, err := callThrough(t, errClient(errors.New("down")))
		if err != nil {
			t.Fatal(err)
		}
		if srv.calls.Load() != 1 {
			t.Fatal("handler did not run under fail-open")
		}
		if v := trailer.Get(MDLimit); len(v) != 0 {
			t.Errorf("ratelimit-limit = %v after a client error", v)
		}
	})
	t.Run("client error fail-closed", func(t *testing.T) {
		t.Parallel()
		srv, trailer, err := callThrough(t, errClient(errors.New("down")), FailClosed)
		if status.Code(err) != codes.ResourceExhausted {
			t.Fatalf("code = %v, want ResourceExhausted", status.Code(err))
		}
		if srv.calls.Load() != 0 {
			t.Fatal("handler ran under fail-closed")
		}
		wantTrailer(t, trailer, MDRetryAfter, "1")
	})
}

func wantTrailer(t *testing.T, md metadata.MD, key, want string) {
	t.Helper()
	v := md.Get(key)
	if len(v) != 1 || v[0] != want {
		t.Errorf("trailer %s = %v, want [%s]", key, v, want)
	}
}

// TestDeniedErrorShape checks the in-process view a server-side logging
// interceptor gets: errors.Is(err, ErrDenied) and status.FromError agree.
func TestDeniedErrorShape(t *testing.T) {
	t.Parallel()
	limiter := denyClient(Result{Limit: 1, RetryAfter: time.Second, Policy: "p"})
	ic := UnaryInterceptor(limiter, methodKey)
	var ran bool
	_, err := ic(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "/svc/M"},
		func(context.Context, any) (any, error) { ran = true; return nil, nil })
	if ran {
		t.Fatal("handler ran")
	}
	if !errors.Is(err, ErrDenied) {
		t.Fatalf("errors.Is(ErrDenied) false: %v", err)
	}
	var de *DeniedError
	if !errors.As(err, &de) || de.Result.Policy != "p" {
		t.Fatalf("errors.As DeniedError: %v", err)
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.ResourceExhausted {
		t.Fatalf("status = %v ok=%v", st, ok)
	}
}

// fakeStream is the minimal grpc.ServerStream the stream interceptor touches.
type fakeStream struct {
	grpc.ServerStream
	ctx     context.Context
	trailer metadata.MD
}

func (s *fakeStream) Context() context.Context  { return s.ctx }
func (s *fakeStream) SetTrailer(md metadata.MD) { s.trailer = metadata.Join(s.trailer, md) }

func TestStreamInterceptor(t *testing.T) {
	t.Parallel()
	info := &grpc.StreamServerInfo{FullMethod: "/svc/Stream", IsServerStream: true}
	cases := []struct {
		name     string
		limiter  Client
		onErr    []OnError
		wantCode codes.Code
		wantRun  bool
		wantMD   map[string]string
	}{
		{
			"allowed", allowClient(Result{Limit: 5, Remaining: 4, ResetAfter: time.Second}), nil, codes.OK, true,
			map[string]string{MDLimit: "5", MDRemaining: "4", MDReset: "1"},
		},
		{
			"denied", denyClient(Result{Limit: 5, RetryAfter: 10 * time.Millisecond}), nil, codes.ResourceExhausted, false,
			map[string]string{MDLimit: "5", MDRemaining: "0", MDReset: "0", MDRetryAfter: "1"},
		},
		{"error fail-open", errClient(errors.New("down")), nil, codes.OK, true, nil},
		{
			"error fail-closed", errClient(errors.New("down")),
			[]OnError{FailClosed},
			codes.ResourceExhausted, false,
			map[string]string{MDRetryAfter: "1"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ss := &fakeStream{ctx: context.Background()}
			var ran bool
			err := StreamInterceptor(tc.limiter, methodKey, tc.onErr...)(nil, ss, info,
				func(any, grpc.ServerStream) error { ran = true; return nil })
			if status.Code(err) != tc.wantCode {
				t.Fatalf("code = %v (%v), want %v", status.Code(err), err, tc.wantCode)
			}
			if ran != tc.wantRun {
				t.Fatalf("handler ran = %v, want %v", ran, tc.wantRun)
			}
			for k, v := range tc.wantMD {
				wantTrailer(t, ss.trailer, k, v)
			}
			if tc.wantMD == nil && len(ss.trailer) != 0 {
				t.Fatalf("trailer = %v, want none", ss.trailer)
			}
		})
	}
}
