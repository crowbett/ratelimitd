package bench

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/crowbett/ratelimitd/internal/httpapi"
	"github.com/crowbett/ratelimitd/pkg/ratelimit"
)

func newChecker(ctx context.Context, cfg Config) (checker, error) {
	switch cfg.Proto {
	case "grpc":
		return newGRPCChecker(ctx, cfg)
	case "http":
		return newHTTPChecker(cfg), nil
	default:
		return nil, fmt.Errorf("bench: unknown proto %q", cfg.Proto)
	}
}

// grpcChecker drives ratelimit.v1 through the public client: Check for a
// batch of one, BatchCheck otherwise. reqs is reused across calls per worker
// via a pool so the hot loop does not allocate a slice per tick.
type grpcChecker struct {
	client ratelimit.Client
	batch  ratelimit.BatchChecker
	cfg    Config
	pool   sync.Pool
}

func newGRPCChecker(ctx context.Context, cfg Config) (*grpcChecker, error) {
	c, err := ratelimit.Dial(ctx, cfg.Target, ratelimit.WithTimeout(cfg.Timeout))
	if err != nil {
		return nil, fmt.Errorf("bench: %w", err)
	}
	g := &grpcChecker{client: c, cfg: cfg}
	if bc, ok := c.(ratelimit.BatchChecker); ok {
		g.batch = bc
	}
	g.pool.New = func() any {
		s := make([]ratelimit.Request, 0, cfg.Batch)
		return &s
	}
	return g, nil
}

func (g *grpcChecker) request(subject string) ratelimit.Request {
	return ratelimit.Request{Tenant: g.cfg.Tenant, Subject: subject, Route: g.cfg.Route, Method: g.cfg.Method}
}

func (g *grpcChecker) check(ctx context.Context, subjects []string) ([]outcome, error) {
	if len(subjects) == 1 || g.batch == nil {
		out := make([]outcome, 0, len(subjects))
		for _, s := range subjects {
			res, err := g.client.Check(ctx, g.request(s))
			if err != nil {
				return nil, err
			}
			out = append(out, outcome{allowed: res.Allowed, degraded: res.Degraded})
		}
		return out, nil
	}
	reqsp, ok := g.pool.Get().(*[]ratelimit.Request)
	if !ok {
		return nil, errors.New("bench: request pool corrupted")
	}
	reqs := (*reqsp)[:0]
	for _, s := range subjects {
		reqs = append(reqs, g.request(s))
	}
	results, err := g.batch.BatchCheck(ctx, reqs)
	*reqsp = reqs[:0]
	g.pool.Put(reqsp)
	if err != nil {
		return nil, err
	}
	out := make([]outcome, len(results))
	for i, r := range results {
		out[i] = outcome{allowed: r.Allowed, degraded: r.Degraded}
	}
	return out, nil
}

func (g *grpcChecker) close() error { return g.client.Close() }

// httpChecker posts /v1/check JSON bodies over a Transport sized to the
// worker count so every worker holds one keep-alive connection.
type httpChecker struct {
	url    string
	cfg    Config
	client *http.Client
}

func newHTTPChecker(cfg Config) *httpChecker {
	target := cfg.Target
	if !strings.Contains(target, "://") {
		target = "http://" + target
	}
	target = strings.TrimSuffix(strings.TrimSuffix(target, "/"), "/v1/check")
	dialer := &net.Dialer{Timeout: cfg.Timeout, KeepAlive: 30 * time.Second}
	tr := &http.Transport{
		Proxy:                 nil, // never pick up HTTP_PROXY for a loopback bench
		DialContext:           dialer.DialContext,
		MaxConnsPerHost:       cfg.Workers,
		MaxIdleConns:          cfg.Workers,
		MaxIdleConnsPerHost:   cfg.Workers,
		IdleConnTimeout:       90 * time.Second,
		ResponseHeaderTimeout: cfg.Timeout,
		ForceAttemptHTTP2:     false,
		DisableCompression:    true,
	}
	return &httpChecker{
		url:    target + "/v1/check",
		cfg:    cfg,
		client: &http.Client{Transport: tr, Timeout: cfg.Timeout},
	}
}

func (h *httpChecker) check(ctx context.Context, subjects []string) ([]outcome, error) {
	out := make([]outcome, 0, len(subjects))
	var buf bytes.Buffer
	for _, s := range subjects {
		buf.Reset()
		body := httpapi.CheckRequest{Tenant: h.cfg.Tenant, Subject: s, Route: h.cfg.Route, Method: h.cfg.Method}
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			return nil, fmt.Errorf("bench: encode: %w", err)
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, h.url, bytes.NewReader(buf.Bytes()))
		if err != nil {
			return nil, fmt.Errorf("bench: request: %w", err)
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := h.client.Do(req)
		if err != nil {
			return nil, fmt.Errorf("bench: post: %w", err)
		}
		o, err := decodeHTTP(resp)
		if err != nil {
			return nil, err
		}
		out = append(out, o)
	}
	return out, nil
}

func decodeHTTP(resp *http.Response) (outcome, error) {
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return outcome{}, fmt.Errorf("bench: http %d: %s", resp.StatusCode, strings.TrimSpace(string(msg)))
	}
	var body httpapi.CheckResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return outcome{}, fmt.Errorf("bench: decode: %w", err)
	}
	return outcome{allowed: body.Allowed, degraded: body.Degraded || body.Backend == "degraded"}, nil
}

func (h *httpChecker) close() error {
	h.client.CloseIdleConnections()
	return nil
}
