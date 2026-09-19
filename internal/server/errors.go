package server

import (
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/crowbett/ratelimitd/internal/limit"
)

// ToStatus maps domain errors to gRPC statuses: ErrUnknownTenant / ErrNoPolicy
// -> NotFound, ErrInvalidParams -> InvalidArgument, ErrBackendDown ->
// Unavailable, a *status.Status passes through, anything else -> Internal.
// A nil error returns nil.
func ToStatus(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, limit.ErrUnknownTenant), errors.Is(err, limit.ErrNoPolicy):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, limit.ErrInvalidParams):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, limit.ErrBackendDown):
		return status.Error(codes.Unavailable, err.Error())
	}
	if _, ok := status.FromError(err); ok {
		return err
	}
	return status.Error(codes.Internal, "internal error")
}
