package replication

import (
	"context"
	"crypto/tls"
	"net"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

func TestReceiverStopCancelsStalledTLSHandshake(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	accepted := make(chan net.Conn, 1)
	release := make(chan struct{})
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		accepted <- conn
		<-release
		_ = conn.Close()
	}()

	receiver := NewReceiver(&ReceiverConfig{
		ReaderID:     "reader-1",
		WriterAddr:   listener.Addr().String(),
		TLSConfig:    &tls.Config{InsecureSkipVerify: true}, //nolint:gosec // the test peer deliberately never completes TLS
		SharedSecret: "test-shared-secret-32-bytes-long!",
		Logger:       zerolog.Nop(),
	})
	if err := receiver.Start(context.Background()); err != nil {
		t.Fatalf("start receiver: %v", err)
	}

	var peerConn net.Conn
	stopDone := make(chan error, 1)
	stopping := false
	stopFinished := false
	t.Cleanup(func() {
		close(release)
		_ = listener.Close()
		if peerConn != nil {
			_ = peerConn.Close()
		}
		if !stopping {
			_ = receiver.Stop()
		}
		if stopping && !stopFinished {
			select {
			case <-stopDone:
			case <-time.After(2 * time.Second):
				t.Error("receiver stop goroutine did not finish during cleanup")
			}
		}
	})

	select {
	case peerConn = <-accepted:
	case <-time.After(time.Second):
		t.Fatal("receiver did not connect to the TLS peer")
	}

	start := time.Now()
	stopping = true
	go func() { stopDone <- receiver.Stop() }()
	select {
	case err := <-stopDone:
		stopFinished = true
		if err != nil {
			t.Fatalf("stop receiver: %v", err)
		}
		if elapsed := time.Since(start); elapsed > time.Second {
			t.Fatalf("receiver shutdown took %v; want under 1s", elapsed)
		}
	case <-time.After(time.Second):
		t.Fatal("receiver shutdown waited for the stalled TLS dial timeout")
	}
}
