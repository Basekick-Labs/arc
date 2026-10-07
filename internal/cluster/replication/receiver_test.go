package replication

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

func TestConnect_CancelInterruptsAStalledTLSHandshake(t *testing.T) {
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
		<-release
		_ = conn.Close()
	}()
	defer close(release)

	r := NewReceiver(&ReceiverConfig{
		WriterAddr:   listener.Addr().String(),
		TLSConfig:    &tls.Config{InsecureSkipVerify: true}, //nolint:gosec // test peer
		SharedSecret: "test-shared-secret",
		Logger:       zerolog.Nop(),
	})
	r.ctx, r.cancelFunc = context.WithCancel(context.Background())
	defer r.cancelFunc()
	go func() {
		<-accepted
		r.cancelFunc()
	}()

	start := time.Now()
	err = r.connect()
	if err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("connect error = %v, want context.Canceled", err)
	}
	if elapsed := time.Since(start); elapsed > 4*time.Second {
		t.Fatalf("cancelled TLS handshake took %v", elapsed)
	}
}
