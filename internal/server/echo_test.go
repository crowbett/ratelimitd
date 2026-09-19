package server_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	echov1 "github.com/crowbett/ratelimitd/gen/echo/v1"
	"github.com/crowbett/ratelimitd/internal/interceptor"
	"github.com/crowbett/ratelimitd/internal/server"
)

// newTestClient spins up an in-memory gRPC server with the production interceptor
// chain and returns a connected client. Everything is torn down via t.Cleanup.
func newTestClient(t *testing.T) echov1.EchoServiceClient {
	t.Helper()

	logger := slog.New(slog.DiscardHandler)
	lis := bufconn.Listen(1024 * 1024)

	srv := grpc.NewServer(
		grpc.ChainUnaryInterceptor(
			interceptor.UnaryRecover(logger),
			interceptor.UnaryRequestID(),
			interceptor.UnaryLogging(logger),
		),
		grpc.ChainStreamInterceptor(
			interceptor.StreamRecover(logger),
			interceptor.StreamRequestID(),
			interceptor.StreamLogging(logger),
		),
	)
	server.NewEchoServer().Register(srv)

	go func() { _ = srv.Serve(lis) }()

	conn, err := grpc.NewClient("passthrough:///bufconn",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() {
		_ = conn.Close()
		srv.Stop()
		_ = lis.Close()
	})
	return echov1.NewEchoServiceClient(conn)
}

func TestEcho(t *testing.T) {
	t.Parallel()
	client := newTestClient(t)

	tests := []struct {
		name      string
		msg       string
		requestID string // sent as x-request-id metadata when non-empty
		wantCode  codes.Code
	}{
		{name: "ok", msg: "hello", wantCode: codes.OK},
		{name: "propagates request id", msg: "hi", requestID: "abc-123", wantCode: codes.OK},
		{name: "empty message", msg: "", wantCode: codes.InvalidArgument},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			if tt.requestID != "" {
				ctx = metadata.AppendToOutgoingContext(ctx, interceptor.RequestIDHeader, tt.requestID)
			}

			var header metadata.MD
			resp, err := client.Echo(ctx, &echov1.EchoRequest{Message: tt.msg}, grpc.Header(&header))
			if got := status.Code(err); got != tt.wantCode {
				t.Fatalf("code = %v, want %v (err %v)", got, tt.wantCode, err)
			}
			if tt.wantCode != codes.OK {
				return
			}
			if resp.GetMessage() != tt.msg {
				t.Errorf("message = %q, want %q", resp.GetMessage(), tt.msg)
			}
			if resp.GetRequestId() == "" {
				t.Error("response request_id is empty")
			}
			if tt.requestID != "" && resp.GetRequestId() != tt.requestID {
				t.Errorf("request_id = %q, want %q", resp.GetRequestId(), tt.requestID)
			}
			if got := header.Get(interceptor.RequestIDHeader); len(got) != 1 || got[0] != resp.GetRequestId() {
				t.Errorf("header %s = %v, want [%s]", interceptor.RequestIDHeader, got, resp.GetRequestId())
			}
		})
	}
}

func TestEchoStream(t *testing.T) {
	t.Parallel()
	client := newTestClient(t)

	tests := []struct {
		name     string
		msg      string
		count    int32
		wantN    int
		wantCode codes.Code
	}{
		{name: "three", msg: "x", count: 3, wantN: 3, wantCode: codes.OK},
		{name: "zero means one", msg: "x", count: 0, wantN: 1, wantCode: codes.OK},
		{name: "negative means one", msg: "x", count: -5, wantN: 1, wantCode: codes.OK},
		{name: "empty message", msg: "", count: 1, wantCode: codes.InvalidArgument},
		{name: "too many", msg: "x", count: server.MaxStreamCount + 1, wantCode: codes.InvalidArgument},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			stream, err := client.EchoStream(t.Context(), &echov1.EchoStreamRequest{Message: tt.msg, Count: tt.count})
			if err != nil {
				t.Fatalf("open stream: %v", err)
			}

			var got []*echov1.EchoStreamResponse
			for {
				resp, err := stream.Recv()
				if errors.Is(err, io.EOF) {
					break
				}
				if err != nil {
					if code := status.Code(err); code != tt.wantCode {
						t.Fatalf("code = %v, want %v (err %v)", code, tt.wantCode, err)
					}
					return
				}
				got = append(got, resp)
			}
			if tt.wantCode != codes.OK {
				t.Fatalf("stream ended cleanly, want code %v", tt.wantCode)
			}
			if len(got) != tt.wantN {
				t.Fatalf("received %d messages, want %d", len(got), tt.wantN)
			}
			for i, r := range got {
				if r.GetSeq() != int32(i+1) || r.GetMessage() != tt.msg {
					t.Errorf("msg[%d] = (%q, %d), want (%q, %d)", i, r.GetMessage(), r.GetSeq(), tt.msg, i+1)
				}
			}
		})
	}
}

func TestEchoStreamCancel(t *testing.T) {
	t.Parallel()
	client := newTestClient(t)

	ctx, cancel := context.WithCancel(t.Context())
	stream, err := client.EchoStream(ctx, &echov1.EchoStreamRequest{Message: "x", Count: server.MaxStreamCount})
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	if _, err := stream.Recv(); err != nil {
		t.Fatalf("first recv: %v", err)
	}
	cancel()

	for {
		_, err := stream.Recv()
		if err == nil {
			continue
		}
		if code := status.Code(err); code != codes.Canceled {
			t.Fatalf("code after cancel = %v, want Canceled", code)
		}
		return
	}
}
