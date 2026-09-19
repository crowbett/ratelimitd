package ratelimit

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/durationpb"

	ratelimitv1 "github.com/crowbett/ratelimitd/gen/ratelimit/v1"
)

// fakeServer is a scripted ratelimit.v1.RateLimitServiceServer.
type fakeServer struct {
	ratelimitv1.UnimplementedRateLimitServiceServer
	check func(ctx context.Context, req *ratelimitv1.CheckRequest) (*ratelimitv1.CheckResponse, error)
	calls atomic.Int64
}

func (s *fakeServer) Check(ctx context.Context, req *ratelimitv1.CheckRequest) (*ratelimitv1.CheckResponse, error) {
	s.calls.Add(1)
	return s.check(ctx, req)
}

func (s *fakeServer) BatchCheck(ctx context.Context, req *ratelimitv1.BatchCheckRequest) (*ratelimitv1.BatchCheckResponse, error) {
	out := &ratelimitv1.BatchCheckResponse{Results: make([]*ratelimitv1.CheckResponse, 0, len(req.GetChecks()))}
	for _, c := range req.GetChecks() {
		r, err := s.Check(ctx, c)
		if err != nil {
			return nil, err
		}
		out.Results = append(out.Results, r)
	}
	return out, nil
}

// startBufconn serves srv on an in-memory listener and returns a Dial option
// that reaches it.
func startBufconn(t *testing.T, srv ratelimitv1.RateLimitServiceServer, serverOpts ...grpc.ServerOption) Option {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	gs := grpc.NewServer(serverOpts...)
	ratelimitv1.RegisterRateLimitServiceServer(gs, srv)
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(func() {
		gs.Stop()
		_ = lis.Close()
	})
	return WithDialOptions(grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
		return lis.DialContext(ctx)
	}))
}

func dialTest(t *testing.T, srv ratelimitv1.RateLimitServiceServer, opts ...Option) Client {
	t.Helper()
	opts = append(opts, startBufconn(t, srv))
	c, err := Dial(context.Background(), "passthrough:///bufnet", opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := c.Close(); err != nil {
			t.Error(err)
		}
	})
	return c
}

func okResponse(remaining uint32) *ratelimitv1.CheckResponse {
	return &ratelimitv1.CheckResponse{
		Code: ratelimitv1.Code_CODE_OK, Limit: 200, Remaining: remaining,
		ResetAfter: durationpb.New(1500 * time.Millisecond), Policy: "api", Backend: "redis",
	}
}

func TestDialCheck(t *testing.T) {
	t.Parallel()
	t.Run("allowed maps fields and forwards the request", func(t *testing.T) {
		t.Parallel()
		var seen *ratelimitv1.CheckRequest
		srv := &fakeServer{check: func(_ context.Context, req *ratelimitv1.CheckRequest) (*ratelimitv1.CheckResponse, error) {
			seen = req
			return okResponse(199), nil
		}}
		c := dialTest(t, srv)
		res, err := c.Check(context.Background(), Request{
			Tenant: "acme", Subject: "k1", Route: "/v1/x", Method: "GET", Cost: 3,
			Descriptors: []Descriptor{{Key: "ip", Value: "10.0.0.1"}},
		})
		if err != nil {
			t.Fatal(err)
		}
		want := Result{Allowed: true, Limit: 200, Remaining: 199, ResetAfter: 1500 * time.Millisecond, Policy: "api", Backend: "redis"}
		if res != want {
			t.Fatalf("res = %+v, want %+v", res, want)
		}
		if seen.GetTenant() != "acme" || seen.GetSubject() != "k1" || seen.GetRoute() != "/v1/x" || seen.GetMethod() != "GET" ||
			seen.GetCost() != 3 || len(seen.GetDescriptors()) != 1 || seen.GetDescriptors()[0].GetValue() != "10.0.0.1" {
			t.Fatalf("server saw %v", seen)
		}
	})
	t.Run("over limit is a result, not an error", func(t *testing.T) {
		t.Parallel()
		srv := &fakeServer{check: func(context.Context, *ratelimitv1.CheckRequest) (*ratelimitv1.CheckResponse, error) {
			return &ratelimitv1.CheckResponse{
				Code: ratelimitv1.Code_CODE_OVER_LIMIT, Limit: 200, Remaining: 0,
				RetryAfter: durationpb.New(250 * time.Millisecond), ResetAfter: durationpb.New(2 * time.Second),
				Policy: "api", Backend: "degraded", Degraded: true,
			}, nil
		}}
		c := dialTest(t, srv)
		res, err := c.Check(context.Background(), Request{Tenant: "acme", Subject: "k1"})
		if err != nil {
			t.Fatalf("denial returned error %v", err)
		}
		if res.Allowed || res.RetryAfter != 250*time.Millisecond || res.ResetAfter != 2*time.Second || !res.Degraded || res.Backend != "degraded" {
			t.Fatalf("res = %+v", res)
		}
	})
	t.Run("unspecified code is a client error", func(t *testing.T) {
		t.Parallel()
		srv := &fakeServer{check: func(context.Context, *ratelimitv1.CheckRequest) (*ratelimitv1.CheckResponse, error) {
			return &ratelimitv1.CheckResponse{}, nil
		}}
		c := dialTest(t, srv)
		_, err := c.Check(context.Background(), Request{Tenant: "acme", Subject: "k1"})
		if !errors.Is(err, ErrUnspecified) {
			t.Fatalf("err = %v, want ErrUnspecified", err)
		}
	})
	t.Run("retries once on UNAVAILABLE", func(t *testing.T) {
		t.Parallel()
		srv := &fakeServer{}
		srv.check = func(context.Context, *ratelimitv1.CheckRequest) (*ratelimitv1.CheckResponse, error) {
			if srv.calls.Load() == 1 {
				return nil, status.Error(codes.Unavailable, "warming up")
			}
			return okResponse(10), nil
		}
		c := dialTest(t, srv)
		res, err := c.Check(context.Background(), Request{Tenant: "acme", Subject: "k1"})
		if err != nil || !res.Allowed {
			t.Fatalf("res = %+v err = %v", res, err)
		}
		if n := srv.calls.Load(); n != 2 {
			t.Fatalf("server calls = %d, want 2", n)
		}
	})
	t.Run("persistent UNAVAILABLE is a wrapped status after exactly two attempts", func(t *testing.T) {
		t.Parallel()
		srv := &fakeServer{check: func(context.Context, *ratelimitv1.CheckRequest) (*ratelimitv1.CheckResponse, error) {
			return nil, status.Error(codes.Unavailable, "down")
		}}
		c := dialTest(t, srv)
		_, err := c.Check(context.Background(), Request{Tenant: "acme", Subject: "k1"})
		if err == nil {
			t.Fatal("no error")
		}
		if status.Code(err) != codes.Unavailable {
			t.Fatalf("status.Code = %v, want Unavailable (err %v)", status.Code(err), err)
		}
		if errors.Is(err, ErrDenied) {
			t.Fatal("a transport failure must not be ErrDenied")
		}
		if n := srv.calls.Load(); n != 2 {
			t.Fatalf("server calls = %d, want 2", n)
		}
	})
	t.Run("other codes are not retried", func(t *testing.T) {
		t.Parallel()
		srv := &fakeServer{check: func(context.Context, *ratelimitv1.CheckRequest) (*ratelimitv1.CheckResponse, error) {
			return nil, status.Error(codes.NotFound, "unknown tenant")
		}}
		c := dialTest(t, srv)
		_, err := c.Check(context.Background(), Request{Tenant: "nope", Subject: "k1"})
		if status.Code(err) != codes.NotFound {
			t.Fatalf("status.Code = %v, want NotFound", status.Code(err))
		}
		if n := srv.calls.Load(); n != 1 {
			t.Fatalf("server calls = %d, want 1", n)
		}
	})
	t.Run("per-call timeout", func(t *testing.T) {
		t.Parallel()
		srv := &fakeServer{check: func(ctx context.Context, _ *ratelimitv1.CheckRequest) (*ratelimitv1.CheckResponse, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		}}
		c := dialTest(t, srv, WithTimeout(30*time.Millisecond))
		start := time.Now()
		_, err := c.Check(context.Background(), Request{Tenant: "acme", Subject: "k1"})
		if status.Code(err) != codes.DeadlineExceeded {
			t.Fatalf("status.Code = %v, want DeadlineExceeded (err %v)", status.Code(err), err)
		}
		if el := time.Since(start); el > time.Second {
			t.Fatalf("took %s, want ~30ms", el)
		}
	})
	t.Run("batch", func(t *testing.T) {
		t.Parallel()
		srv := &fakeServer{check: func(_ context.Context, req *ratelimitv1.CheckRequest) (*ratelimitv1.CheckResponse, error) {
			if req.GetSubject() == "hot" {
				return &ratelimitv1.CheckResponse{Code: ratelimitv1.Code_CODE_OVER_LIMIT, Limit: 1, RetryAfter: durationpb.New(time.Second)}, nil
			}
			return okResponse(5), nil
		}}
		c := dialTest(t, srv)
		bc, ok := c.(BatchChecker)
		if !ok {
			t.Fatal("Dial client does not implement BatchChecker")
		}
		out, err := bc.BatchCheck(context.Background(), []Request{
			{Tenant: "acme", Subject: "a"}, {Tenant: "acme", Subject: "hot"}, {Tenant: "acme", Subject: "b"},
		})
		if err != nil {
			t.Fatal(err)
		}
		if len(out) != 3 || !out[0].Allowed || out[1].Allowed || !out[2].Allowed || out[1].RetryAfter != time.Second {
			t.Fatalf("out = %+v", out)
		}
		if _, err := bc.BatchCheck(context.Background(), make([]Request, MaxBatch+1)); err == nil {
			t.Fatal("oversized batch accepted")
		}
		if out, err := bc.BatchCheck(context.Background(), nil); err != nil || out != nil {
			t.Fatalf("empty batch: %v %v", out, err)
		}
	})
}

func TestDialCancelledContext(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := Dial(ctx, "127.0.0.1:1"); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}
