package security

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"testing"
	"time"
)

// A cancelled context must interrupt a dial that is still being established.
// Dial could not do this for TLS: it goes through tls.DialWithDialer, which
// runs the handshake against an internal background context, so a peer that
// accepts the connection and then goes silent held the caller until the dial
// timeout (#901).
func TestDialContext_CancelInterruptsAStalledHandshake(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	accepted := make(chan struct{})
	release := make(chan struct{})
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		close(accepted)
		// Accept and say nothing: the handshake stalls here.
		<-release
		conn.Close()
	}()
	defer close(release)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-accepted
		cancel()
	}()

	// A dial timeout far longer than the test, so only the cancellation can
	// end this.
	start := time.Now()
	_, err = DialContext(ctx, "tcp", listener.Addr().String(), 30*time.Second,
		&tls.Config{InsecureSkipVerify: true}) //nolint:gosec // test peer
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected the dial to fail")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want it to satisfy errors.Is(context.Canceled)", err)
	}
	if elapsed > 5*time.Second {
		t.Fatalf("took %v; the cancellation did not interrupt the handshake", elapsed)
	}
}

// The timeout argument still bounds the whole of connection establishment,
// including the TLS handshake, with no context deadline in play. crypto/tls's
// dial applies net.Dialer.Timeout to both the connect and HandshakeContext, so
// moving from DialWithDialer to tls.Dialer.DialContext does not leave a
// stalled handshake unbounded.
func TestDialContext_TimeoutStillBoundsTheHandshake(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	release := make(chan struct{})
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		<-release
		conn.Close()
	}()
	defer close(release)

	start := time.Now()
	_, err = DialContext(context.Background(), "tcp", listener.Addr().String(),
		300*time.Millisecond, &tls.Config{InsecureSkipVerify: true}) //nolint:gosec // test peer
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected the dial to time out")
	}
	if elapsed > 5*time.Second {
		t.Fatalf("took %v; the timeout did not bound the handshake", elapsed)
	}
}

// Plain TCP honours cancellation too, and Dial keeps working by delegating.
func TestDialContext_PlainAndDialDelegation(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			conn.Close()
		}
	}()

	conn, err := DialContext(context.Background(), "tcp", listener.Addr().String(), time.Second, nil)
	if err != nil {
		t.Fatalf("plain DialContext: %v", err)
	}
	conn.Close()

	conn, err = Dial("tcp", listener.Addr().String(), time.Second, nil)
	if err != nil {
		t.Fatalf("Dial delegation: %v", err)
	}
	conn.Close()

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := DialContext(cancelled, "tcp", listener.Addr().String(), time.Second, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("plain cancelled dial err = %v, want context.Canceled", err)
	}
}
