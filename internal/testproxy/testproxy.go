// Package testproxy is a TCP proxy used by tests to sit between the client
// and Redis on a loopback port and inject failures without toxiproxy:
// Blackhole drops in-flight and new connections without RST (the `docker
// pause` case that killing the container does not reproduce), Delay adds
// latency, Restore heals.
//
// Owner: workstream B (backends).
package testproxy

import (
	"context"
	"errors"
	"time"
)

// Proxy forwards 127.0.0.1:<port> -> target until Close.
type Proxy struct {
	target string
	addr   string
}

// New listens on a random loopback port and starts forwarding to target
// ("host:port"). STUB: returns an error.
func New(_ context.Context, target string) (*Proxy, error) {
	_ = target
	return nil, errors.New("stub: testproxy.New")
}

// Addr is the listen address to point the client at.
func (p *Proxy) Addr() string { return p.addr }

// Target is the upstream address.
func (p *Proxy) Target() string { return p.target }

// Blackhole stops forwarding: existing connections stall (no RST, no FIN) and
// new connections are accepted then never serviced. STUB.
func (p *Proxy) Blackhole() {}

// Delay adds d of one-way latency to every forwarded byte; 0 removes it. STUB.
func (p *Proxy) Delay(_ time.Duration) {}

// Restore ends Blackhole and Delay; stalled connections are closed so the
// client reconnects. STUB.
func (p *Proxy) Restore() {}

// Close stops the listener and every forwarded connection. STUB.
func (p *Proxy) Close() error { return nil }
