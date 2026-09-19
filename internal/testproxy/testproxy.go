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
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// Proxy forwards 127.0.0.1:<port> -> target until Close.
type Proxy struct {
	target string
	addr   string
	ln     net.Listener

	blackhole atomic.Bool
	delay     atomic.Int64 // nanoseconds per forwarded chunk

	mu    sync.Mutex
	wake  chan struct{} // closed and replaced on Restore/Close: parked copiers re-check the mode
	conns map[net.Conn]struct{}
	done  chan struct{}
	wg    sync.WaitGroup
	once  sync.Once
}

// New listens on a random loopback port and starts forwarding to target
// ("host:port"). The listener closes when ctx ends or on Close.
func New(ctx context.Context, target string) (*Proxy, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", "127.0.0.1:0")
	if err != nil {
		return nil, fmt.Errorf("testproxy: listen: %w", err)
	}
	p := &Proxy{
		target: target,
		addr:   ln.Addr().String(),
		ln:     ln,
		wake:   make(chan struct{}),
		conns:  make(map[net.Conn]struct{}),
		done:   make(chan struct{}),
	}
	p.wg.Add(1)
	go p.accept()
	go func() {
		select {
		case <-ctx.Done():
			_ = p.Close()
		case <-p.done:
		}
	}()
	return p, nil
}

// Addr is the listen address to point the client at.
func (p *Proxy) Addr() string { return p.addr }

// Target is the upstream address.
func (p *Proxy) Target() string { return p.target }

// Blackhole stops forwarding: existing connections stall (no RST, no FIN;
// bytes already in flight are parked, not dropped on the floor) and new
// connections are accepted then never serviced.
func (p *Proxy) Blackhole() {
	p.blackhole.Store(true)
}

// Delay adds d of one-way latency to every forwarded chunk; 0 removes it.
func (p *Proxy) Delay(d time.Duration) {
	p.delay.Store(int64(d))
}

// Restore ends Blackhole and Delay; connections that were stalled are
// closed so the client reconnects cleanly instead of resuming a half-dead
// stream.
func (p *Proxy) Restore() {
	wasBlackholed := p.blackhole.Swap(false)
	p.delay.Store(0)
	p.mu.Lock()
	close(p.wake)
	p.wake = make(chan struct{})
	if wasBlackholed {
		for c := range p.conns {
			_ = c.Close()
		}
	}
	p.mu.Unlock()
}

// Close stops the listener and every forwarded connection.
func (p *Proxy) Close() error {
	var err error
	p.once.Do(func() {
		close(p.done)
		err = p.ln.Close()
		p.mu.Lock()
		close(p.wake)
		p.wake = make(chan struct{})
		for c := range p.conns {
			_ = c.Close()
		}
		p.mu.Unlock()
		p.wg.Wait()
	})
	return err
}

// Conns is how many sockets (client and upstream sides) are currently open.
func (p *Proxy) Conns() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.conns)
}

func (p *Proxy) accept() {
	defer p.wg.Done()
	for {
		c, err := p.ln.Accept()
		if err != nil {
			return
		}
		if !p.track(c) {
			_ = c.Close()
			return
		}
		p.wg.Add(1)
		go p.serve(c)
	}
}

// serve dials upstream (unless blackholed, in which case the client socket
// is parked until Restore or Close) and pumps both directions.
func (p *Proxy) serve(client net.Conn) {
	defer p.wg.Done()
	defer p.untrack(client)
	if !p.waitOpen() {
		return
	}
	d := net.Dialer{Timeout: 5 * time.Second}
	upstream, err := d.DialContext(context.Background(), "tcp", p.target)
	if err != nil {
		return
	}
	if !p.track(upstream) {
		_ = upstream.Close()
		return
	}
	defer p.untrack(upstream)
	var pumps sync.WaitGroup
	pumps.Add(2)
	go func() { defer pumps.Done(); p.pump(client, upstream) }()
	go func() { defer pumps.Done(); p.pump(upstream, client) }()
	pumps.Wait()
}

// pump copies src -> dst chunk by chunk, honouring Delay and parking while
// blackholed. When src ends, dst gets a write-side close so the peer sees a
// clean EOF.
func (p *Proxy) pump(src, dst net.Conn) {
	buf := make([]byte, 32*1024)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			if !p.waitOpen() {
				return
			}
			if d := p.delay.Load(); d > 0 {
				time.Sleep(time.Duration(d))
			}
			if _, werr := dst.Write(buf[:n]); werr != nil {
				return
			}
		}
		if err != nil {
			if tc, ok := dst.(*net.TCPConn); ok && errors.Is(err, io.EOF) {
				_ = tc.CloseWrite()
			} else {
				_ = dst.Close()
			}
			return
		}
	}
}

// waitOpen blocks while the proxy is blackholed; it returns false once the
// proxy is closed, or once a Restore ended the blackhole this call was parked
// in (the connection is being closed anyway).
func (p *Proxy) waitOpen() bool {
	for {
		if !p.blackhole.Load() {
			select {
			case <-p.done:
				return false
			default:
				return true
			}
		}
		p.mu.Lock()
		wake := p.wake
		p.mu.Unlock()
		select {
		case <-wake:
			return false
		case <-p.done:
			return false
		}
	}
}

func (p *Proxy) track(c net.Conn) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	select {
	case <-p.done:
		return false
	default:
	}
	p.conns[c] = struct{}{}
	return true
}

func (p *Proxy) untrack(c net.Conn) {
	p.mu.Lock()
	delete(p.conns, c)
	p.mu.Unlock()
	_ = c.Close()
}
