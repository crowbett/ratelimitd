package policy

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/crowbett/ratelimitd/internal/limit"
)

// Registry publishes the current Set through an atomic.Pointer so hot-path
// readers never take a lock. Writers (Watch, Upsert) validate first and swap
// only a Set that passed.
type Registry struct {
	set atomic.Pointer[Set]
	log *slog.Logger
}

// NewRegistry returns a Registry serving initial. log may be nil (slog.Default).
func NewRegistry(initial Set, log *slog.Logger) *Registry {
	if log == nil {
		log = slog.Default()
	}
	r := &Registry{log: log}
	r.set.Store(&initial)
	return r
}

// Snapshot returns the current Set; the pointer is immutable, never write to it.
func (r *Registry) Snapshot() *Set { return r.set.Load() }

// Upsert validates p inside tenant ("" = global), rewrites the policy file
// atomically (temp + rename), swaps the snapshot and returns it. The watcher
// sees its own write and no-ops on the content hash. STUB.
func (r *Registry) Upsert(_ context.Context, _ string, _ Policy) (*Set, error) {
	return nil, errors.New("stub: policy.Registry.Upsert")
}

// Delete removes policy name from tenant the same way Upsert adds one. STUB.
func (r *Registry) Delete(_ context.Context, _, _ string) (*Set, error) {
	return nil, errors.New("stub: policy.Registry.Delete")
}

// Watch polls path's mtime+size every `every`, re-reads on change, compares
// the content hash, validates and swaps. It is the ONE watcher goroutine and
// returns when ctx ends; a bad file is logged and counted, the last good Set
// stays. STUB: returns ctx.Err() once ctx is done.
func (r *Registry) Watch(ctx context.Context, _ string, _ time.Duration) error {
	<-ctx.Done()
	return ctx.Err()
}

// Resolve is the hot path: it returns the Reqs to send to the store for one
// request, or a short-circuit Decision (allow/deny list hit) with nil reqs.
// Unknown tenant -> limit.ErrUnknownTenant; nothing matched -> limit.ErrNoPolicy.
// STUB.
func (r *Registry) Resolve(_, _ string, _ []Descriptor, _, _ string) ([]limit.Req, *limit.Decision, error) {
	return nil, nil, errors.New("stub: policy.Registry.Resolve")
}
