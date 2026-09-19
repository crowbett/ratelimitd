package policy

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/crowbett/ratelimitd/internal/limit"
)

// Registry publishes the current Set through an atomic.Pointer so hot-path
// readers never take a lock. Writers (Watch, Upsert, Delete) validate first,
// serialise on a mutex and swap only a Set that passed.
type Registry struct {
	snap         atomic.Pointer[snapshot]
	log          *slog.Logger
	mu           sync.Mutex // writers only
	path         atomic.Pointer[string]
	reloadErrors atomic.Uint64
	reloads      atomic.Uint64
}

// NewRegistry returns a Registry serving initial. log may be nil (slog.Default).
// initial should have passed Validate; if it cannot be compiled the Registry
// serves it with no tenants (every Resolve is ErrUnknownTenant) and logs why.
func NewRegistry(initial Set, log *slog.Logger) *Registry {
	if log == nil {
		log = slog.Default()
	}
	r := &Registry{log: log}
	snap, err := compile(&initial)
	if err != nil {
		log.Error("policy: initial set does not compile; serving no tenants", "err", err)
		snap = &snapshot{set: &initial, tenants: map[string]*compiledTenant{}, defaultCost: 1}
	}
	r.snap.Store(snap)
	return r
}

// Snapshot returns the current Set; the pointer is immutable, never write to it.
func (r *Registry) Snapshot() *Set { return r.snap.Load().set }

// ReloadErrors counts Watch cycles that found a file it could not swap in.
func (r *Registry) ReloadErrors() uint64 { return r.reloadErrors.Load() }

// Reloads counts successful swaps from the file (Upsert/Delete excluded).
func (r *Registry) Reloads() uint64 { return r.reloads.Load() }

// Upsert validates p inside tenant ("" = global), rewrites the policy file
// atomically (temp + rename), swaps the snapshot and returns it. The watcher
// sees its own write and no-ops on the content hash. An unknown tenant is
// created. Without a Watch (no path yet) the swap is in memory only.
func (r *Registry) Upsert(ctx context.Context, tenant string, p Policy) (*Set, error) {
	return r.rewrite(ctx, "upsert", tenant, p.Name, func(s *Set) error {
		if tenant == "" {
			s.Global = replacePolicy(s.Global, p)
			return nil
		}
		t := s.Tenants[tenant]
		t.ID = tenant
		t.Policies = replacePolicy(t.Policies, p)
		s.Tenants[tenant] = t
		return nil
	})
}

// Delete removes policy name from tenant the same way Upsert adds one.
// Unknown tenant -> limit.ErrUnknownTenant; unknown policy -> limit.ErrNoPolicy.
func (r *Registry) Delete(ctx context.Context, tenant, name string) (*Set, error) {
	return r.rewrite(ctx, "delete", tenant, name, func(s *Set) error {
		if tenant == "" {
			if !hasPolicy(s.Global, name) {
				return fmt.Errorf("%w: global policy %q", limit.ErrNoPolicy, name)
			}
			s.Global = removePolicy(s.Global, name)
			return nil
		}
		t, ok := s.Tenants[tenant]
		if !ok {
			return fmt.Errorf("%w: %q", limit.ErrUnknownTenant, tenant)
		}
		if !hasPolicy(t.Policies, name) {
			return fmt.Errorf("%w: tenant %q policy %q", limit.ErrNoPolicy, tenant, name)
		}
		t.Policies = removePolicy(t.Policies, name)
		if t.Default == name {
			t.Default = ""
		}
		s.Tenants[tenant] = t
		return nil
	})
}

// rewrite is the shared admin path: clone -> mutate -> Validate -> compile ->
// render -> write file -> swap.
func (r *Registry) rewrite(ctx context.Context, op, tenant, name string, mutate func(*Set) error) (*Set, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	next := r.snap.Load().set.clone()
	if err := mutate(next); err != nil {
		return nil, err
	}
	if err := next.Validate(); err != nil {
		return nil, fmt.Errorf("%w: %s %s/%s: %w", limit.ErrInvalidParams, op, tenant, name, err)
	}
	var buf bytes.Buffer
	if err := Write(&buf, *next); err != nil {
		return nil, fmt.Errorf("%s %s/%s: %w", op, tenant, name, err)
	}
	next.Version = contentHash(buf.Bytes())
	next.LoadedAt = time.Now()
	snap, err := compile(next)
	if err != nil {
		return nil, fmt.Errorf("%w: %s %s/%s: %w", limit.ErrInvalidParams, op, tenant, name, err)
	}
	if path := r.path.Load(); path != nil {
		if err := writeAtomic(*path, buf.Bytes()); err != nil {
			return nil, fmt.Errorf("%s %s/%s: %w", op, tenant, name, err)
		}
	}
	r.snap.Store(snap)
	r.log.Info("policy: "+op, "tenant", tenant, "policy", name, "version", next.Version)
	return next, nil
}

func replacePolicy(ps []Policy, p Policy) []Policy {
	out := make([]Policy, 0, len(ps)+1)
	replaced := false
	for i := range ps {
		if ps[i].Name == p.Name {
			out = append(out, p)
			replaced = true
			continue
		}
		out = append(out, ps[i])
	}
	if !replaced {
		out = append(out, p)
	}
	return out
}

func removePolicy(ps []Policy, name string) []Policy {
	out := make([]Policy, 0, len(ps))
	for i := range ps {
		if ps[i].Name != name {
			out = append(out, ps[i])
		}
	}
	return out
}

// clone copies the containers a rewrite touches (the tenants map and every
// policies slice); Policy values are copied as-is and never mutated in place.
func (s *Set) clone() *Set {
	out := *s
	out.Tenants = make(map[string]Tenant, len(s.Tenants)+1)
	for id, t := range s.Tenants {
		t.Policies = append([]Policy(nil), t.Policies...)
		t.Allow = append([]string(nil), t.Allow...)
		t.Deny = append([]string(nil), t.Deny...)
		out.Tenants[id] = t
	}
	out.Global = append([]Policy(nil), s.Global...)
	return &out
}

// writeAtomic writes data to a temp file beside path and renames it over
// path. Windows can refuse the rename while a reader holds the file open, so
// it retries briefly.
func writeAtomic(path string, data []byte) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, "."+filepath.Base(path)+".*.tmp")
	if err != nil {
		return fmt.Errorf("create temp: %w", err)
	}
	tmpName := tmp.Name()
	cleanup := func() { _ = os.Remove(tmpName) }
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		cleanup()
		return fmt.Errorf("write temp: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		cleanup()
		return fmt.Errorf("sync temp: %w", err)
	}
	if err := tmp.Close(); err != nil {
		cleanup()
		return fmt.Errorf("close temp: %w", err)
	}
	for attempt := 0; ; attempt++ {
		err = os.Rename(tmpName, path)
		if err == nil {
			return nil
		}
		if attempt >= 20 {
			cleanup()
			return fmt.Errorf("rename: %w", err)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// Watch polls path's mtime+size every `every`, re-reads on change, compares
// the content hash, validates and swaps. It is the ONE watcher goroutine and
// returns when ctx ends; a bad file is logged and counted, the last good Set
// stays. every <= 0 means 2s. Watch also records path for Upsert/Delete.
func (r *Registry) Watch(ctx context.Context, path string, every time.Duration) error {
	if every <= 0 {
		every = 2 * time.Second
	}
	r.path.Store(&path)
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	var lastMod time.Time
	var lastSize int64 = -1
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
		fi, err := os.Stat(path)
		if err != nil {
			r.reloadFailed("stat", path, err)
			continue
		}
		if fi.ModTime().Equal(lastMod) && fi.Size() == lastSize {
			continue
		}
		lastMod, lastSize = fi.ModTime(), fi.Size()
		if err := r.reload(path); err != nil {
			r.reloadFailed("reload", path, err)
		}
	}
}

func (r *Registry) reloadFailed(stage, path string, err error) {
	n := r.reloadErrors.Add(1)
	r.log.Error("policy: reload failed; keeping the last good set", "stage", stage, "path", path, "err", err, "reload_errors", n)
}

// reload reads path and swaps it in unless its content hash is already live.
// The read happens under the writers' mutex so it is ordered against
// Upsert/Delete (file write + swap): it sees either the old file and the old
// version or the new file and the new version, never a half state.
func (r *Registry) reload(path string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	raw, err := os.ReadFile(path) //nolint:gosec // operator-supplied POLICY_FILE
	if err != nil {
		return fmt.Errorf("read: %w", err)
	}
	if contentHash(raw) == r.snap.Load().set.Version {
		return nil // our own write, or a touch without a change
	}
	set, err := Parse(bytes.NewReader(raw))
	if err != nil {
		return err
	}
	if err := set.Validate(); err != nil {
		return fmt.Errorf("validate: %w", err)
	}
	snap, err := compile(&set)
	if err != nil {
		return fmt.Errorf("compile: %w", err)
	}
	r.snap.Store(snap)
	n := r.reloads.Add(1)
	r.log.Info("policy: reloaded", "path", path, "version", set.Version, "tenants", len(set.Tenants), "reloads", n)
	return nil
}

// Resolve is the hot path: it returns the Reqs to send to the store for one
// request, or a short-circuit Decision (deny list -> Allowed=false,
// Policy="deny"; allow list -> Allowed=true, Policy="allow") with nil reqs.
// Every enabled matching policy (tenant first, then global; the tenant's
// default when no tenant policy matched) yields one Req with precomputed
// Params, its rendered key and the route's cost (cost table, else
// defaults.cost). Unknown tenant -> limit.ErrUnknownTenant; nothing matched
// -> limit.ErrNoPolicy. It allocates the []Req and one key buffer.
func (r *Registry) Resolve(tenant, subject string, d []Descriptor, route, method string) ([]limit.Req, *limit.Decision, error) {
	req := request{tenant: tenant, subject: subject, route: route, method: method, ds: d}
	return r.snap.Load().resolve(&req)
}
