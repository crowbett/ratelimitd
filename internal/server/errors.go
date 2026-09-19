package server

import (
	"context"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/crowbett/ratelimitd/internal/limit"
)

// ToStatus maps domain errors to gRPC statuses: ErrUnknownTenant -> NotFound,
// ErrNoPolicy -> FailedPrecondition (the tenant exists but nothing matches),
// ErrInvalidParams and *ValidationError -> InvalidArgument, ErrBackendDown ->
// Unavailable, context.Canceled -> Canceled, context.DeadlineExceeded ->
// DeadlineExceeded, a *status.Status passes through, anything else ->
// Internal (the message is not leaked). A nil error returns nil.
func ToStatus(err error) error {
	var verr *ValidationError
	switch {
	case err == nil:
		return nil
	case errors.Is(err, limit.ErrUnknownTenant):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, limit.ErrNoPolicy):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, limit.ErrInvalidParams), errors.As(err, &verr):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, limit.ErrBackendDown):
		return status.Error(codes.Unavailable, err.Error())
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, err.Error())
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, err.Error())
	}
	if _, ok := status.FromError(err); ok {
		return err
	}
	return status.Error(codes.Internal, "internal error")
}
