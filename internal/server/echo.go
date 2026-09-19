// Package server implements the echo.v1.EchoService.
package server

import (
	"context"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	echov1 "github.com/crowbett/ratelimitd/gen/echo/v1"
	"github.com/crowbett/ratelimitd/internal/interceptor"
)

// MaxStreamCount bounds EchoStream so a client cannot ask for an unbounded stream.
const MaxStreamCount = 1000

// EchoServer implements echov1.EchoServiceServer.
type EchoServer struct {
	echov1.UnimplementedEchoServiceServer
}

// NewEchoServer returns a ready-to-register EchoServer.
func NewEchoServer() *EchoServer { return &EchoServer{} }

// Register attaches the service to a grpc.Server.
func (s *EchoServer) Register(srv grpc.ServiceRegistrar) {
	echov1.RegisterEchoServiceServer(srv, s)
}

// Echo returns the message unchanged along with the request id from the context.
func (s *EchoServer) Echo(ctx context.Context, req *echov1.EchoRequest) (*echov1.EchoResponse, error) {
	if req.GetMessage() == "" {
		return nil, status.Error(codes.InvalidArgument, "message must not be empty")
	}
	return &echov1.EchoResponse{
		Message:   req.GetMessage(),
		RequestId: interceptor.RequestIDFrom(ctx),
	}, nil
}

// EchoStream sends the message back count times (count <= 0 is treated as 1).
func (s *EchoServer) EchoStream(req *echov1.EchoStreamRequest, stream grpc.ServerStreamingServer[echov1.EchoStreamResponse]) error {
	if req.GetMessage() == "" {
		return status.Error(codes.InvalidArgument, "message must not be empty")
	}
	count := req.GetCount()
	if count <= 0 {
		count = 1
	}
	if count > MaxStreamCount {
		return status.Errorf(codes.InvalidArgument, "count must be <= %d", MaxStreamCount)
	}

	for seq := int32(1); seq <= count; seq++ {
		if err := stream.Context().Err(); err != nil {
			return status.FromContextError(err).Err()
		}
		if err := stream.Send(&echov1.EchoStreamResponse{Message: req.GetMessage(), Seq: seq}); err != nil {
			return fmt.Errorf("send seq %d: %w", seq, err)
		}
	}
	return nil
}
