package testproxy

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"
)

// echoServer is the upstream: it echoes every byte back.
func echoServer(tb testing.TB) string {
	tb.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				_, _ = io.Copy(c, c)
			}()
		}
	}()
	return ln.Addr().String()
}

func dial(tb testing.TB, addr string) net.Conn {
	tb.Helper()
	c, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = c.Close() })
	return c
}

// roundTrip writes msg and expects it back within timeout.
func roundTrip(tb testing.TB, c net.Conn, msg string, timeout time.Duration) error {
	tb.Helper()
	if _, err := c.Write([]byte(msg)); err != nil {
		return err
	}
	buf := make([]byte, len(msg))
	_ = c.SetReadDeadline(time.Now().Add(timeout))
	defer c.SetReadDeadline(time.Time{})
	if _, err := io.ReadFull(c, buf); err != nil {
		return err
	}
	if string(buf) != msg {
		tb.Fatalf("echo = %q, want %q", buf, msg)
	}
	return nil
}

func TestForwardDelayBlackholeRestore(t *testing.T) {
	t.Parallel()
	p, err := New(context.Background(), echoServer(t))
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if p.Target() == "" || p.Addr() == "" {
		t.Fatal("empty Addr/Target")
	}

	c := dial(t, p.Addr())
	if err := roundTrip(t, c, "hello", time.Second); err != nil {
		t.Fatalf("forward: %v", err)
	}

	p.Delay(60 * time.Millisecond)
	start := time.Now()
	if err := roundTrip(t, c, "slow", 2*time.Second); err != nil {
		t.Fatalf("delayed: %v", err)
	}
	if el := time.Since(start); el < 100*time.Millisecond { // two one-way delays
		t.Fatalf("Delay(60ms) round trip took %v", el)
	}
	p.Delay(0)

	p.Blackhole()
	// The existing socket stalls: the write is accepted, no reply, no reset.
	err = roundTrip(t, c, "void", 200*time.Millisecond)
	var ne net.Error
	if !errors.As(err, &ne) || !ne.Timeout() {
		t.Fatalf("blackholed round trip = %v, want a read timeout", err)
	}
	// A new connection is accepted (TCP handshake completes) and then stalls too.
	c2 := dial(t, p.Addr())
	err = roundTrip(t, c2, "void2", 200*time.Millisecond)
	if !errors.As(err, &ne) || !ne.Timeout() {
		t.Fatalf("new blackholed conn = %v, want a read timeout", err)
	}

	p.Restore()
	// Stalled connections were closed so the client reconnects.
	_ = c.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := c.Read(make([]byte, 1)); err == nil {
		t.Fatal("stalled connection survived Restore")
	}
	c3 := dial(t, p.Addr())
	if err := roundTrip(t, c3, "back", time.Second); err != nil {
		t.Fatalf("after Restore: %v", err)
	}
	deadline := time.Now().Add(time.Second)
	for p.Conns() != 2 && time.Now().Before(deadline) { // c3 and its upstream
		time.Sleep(5 * time.Millisecond)
	}
	if n := p.Conns(); n != 2 {
		t.Fatalf("Conns = %d, want 2", n)
	}

	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if n := p.Conns(); n != 0 {
		t.Fatalf("Conns after Close = %d", n)
	}
	if _, err := net.DialTimeout("tcp", p.Addr(), 200*time.Millisecond); err == nil {
		t.Fatal("listener still accepting after Close")
	}
	if err := p.Close(); err != nil {
		t.Fatalf("second Close = %v", err)
	}
}

func TestContextCloses(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	p, err := New(ctx, echoServer(t))
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if _, err := net.DialTimeout("tcp", p.Addr(), 100*time.Millisecond); err != nil {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("listener still accepting after ctx cancel")
}

func TestUpstreamEOFPropagates(t *testing.T) {
	t.Parallel()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		_, _ = c.Write([]byte("bye"))
		_ = c.Close()
	}()
	p, err := New(context.Background(), ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	c := dial(t, p.Addr())
	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	got, err := io.ReadAll(c)
	if err != nil || string(got) != "bye" {
		t.Fatalf("ReadAll = %q, %v", got, err)
	}
}
