package limit

import "errors"

// Sentinel errors. Callers match with errors.Is; adapters map them to gRPC
// codes / HTTP statuses (server/httpapi own that table).
var (
	// ErrUnknownTenant means the tenant is not in the policy set.
	ErrUnknownTenant = errors.New("unknown tenant")
	// ErrNoPolicy means no policy (tenant or global) matched the request.
	ErrNoPolicy = errors.New("no policy matched")
	// ErrInvalidParams means a policy's numbers do not form valid Params.
	ErrInvalidParams = errors.New("invalid policy params")
	// ErrBackendDown means the store could not answer (and no fallback applied).
	ErrBackendDown = errors.New("backend unavailable")
	// ErrDenied is returned by pkg/ratelimit when a decision is not allowed;
	// the daemon itself never returns it (a denial is a successful RPC).
	ErrDenied = errors.New("rate limited")
)
