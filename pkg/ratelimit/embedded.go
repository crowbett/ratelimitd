package ratelimit

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/crowbett/ratelimitd/internal/limit"
	"github.com/crowbett/ratelimitd/internal/policy"
)

// resolver is the slice of policy.Registry the embedded client needs; a
// fake stands in for it in tests.
type resolver interface {
	Resolve(tenant, subject string, d []policy.Descriptor, route, method string) ([]limit.Req, *limit.Decision, error)
}

// embedded is the in-process client: Resolve -> Apply -> Strictest, the same
// path the daemon takes, with the caller's clock (WithClock) as the only
// source of time.
type embedded struct {
	res     resolver
	store   limit.Store
	clock   func() time.Time
	timeout time.Duration
	log     *slog.Logger

	cancel context.CancelFunc
	wg     sync.WaitGroup
	once   sync.Once
	closed chan struct{}
}

var _ Client = (*embedded)(nil)

// newEmbedded wires the client; run, when non-nil, is the backend's ONE
// background goroutine (the idle-key sweeper) and is stopped by Close.
func newEmbedded(res resolver, store limit.Store, run func(context.Context) error, o *options) *embedded {
	ctx, cancel := context.WithCancel(context.Background())
	e := &embedded{
		res:     res,
		store:   store,
		clock:   o.clock,
		timeout: o.timeout,
		log:     o.log,
		cancel:  cancel,
		closed:  make(chan struct{}),
	}
	if run != nil {
		e.wg.Add(1)
		go func() {
			defer e.wg.Done()
			if err := run(ctx); err != nil && !errors.Is(err, context.Canceled) {
				e.log.Warn("ratelimit: embedded sweeper stopped", slog.String("err", err.Error()))
			}
		}()
	}
	return e
}

// Check implements Client. The caller's Cost (0 = 1) multiplies each matched
// policy's route cost. A denial is Result{Allowed: false}, never an error.
func (e *embedded) Check(ctx context.Context, req Request) (Result, error) {
	select {
	case <-e.closed:
		return Result{}, errors.New("ratelimit: embedded client is closed")
	default:
	}
	if e.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, e.timeout)
		defer cancel()
	}
	reqs, short, err := e.res.Resolve(req.Tenant, req.Subject, toPolicyDescriptors(req.Descriptors), req.Route, req.Method)
	if err != nil {
		return Result{}, fmt.Errorf("ratelimit: resolve %s/%s: %w", req.Tenant, req.Subject, err)
	}
	if short != nil {
		return fromDecision(*short), nil
	}
	if len(reqs) == 0 {
		return Result{}, fmt.Errorf("ratelimit: resolve %s/%s: %w", req.Tenant, req.Subject, limit.ErrNoPolicy)
	}
	if req.Cost > 1 {
		for i := range reqs {
			reqs[i].Cost = max(reqs[i].Cost, 1) * req.Cost
		}
	}
	ds, err := e.store.Apply(ctx, reqs, e.clock())
	if err != nil {
		return Result{}, fmt.Errorf("ratelimit: apply %s/%s: %w", req.Tenant, req.Subject, err)
	}
	if len(ds) != len(reqs) {
		return Result{}, fmt.Errorf("ratelimit: apply %s/%s: %d decisions for %d keys", req.Tenant, req.Subject, len(ds), len(reqs))
	}
	return fromDecision(limit.Strictest(ds)), nil
}

// Close implements Client: stops the sweeper and waits for it.
func (e *embedded) Close() error {
	e.once.Do(func() {
		close(e.closed)
		e.cancel()
		e.wg.Wait()
	})
	return nil
}

func toPolicyDescriptors(ds []Descriptor) []policy.Descriptor {
	if len(ds) == 0 {
		return nil
	}
	out := make([]policy.Descriptor, len(ds))
	for i, d := range ds {
		out[i] = policy.Descriptor{Key: d.Key, Value: d.Value}
	}
	return out
}
