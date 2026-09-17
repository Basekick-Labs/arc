package filereplication

import (
	"bytes"
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/protocol"
	"github.com/basekick-labs/arc/internal/cluster/raft"
)

func startStalledFetchPeer(t *testing.T, sendAck bool, payload []byte) (string, <-chan struct{}) {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	reached := make(chan struct{})
	finished := make(chan struct{})
	stop := make(chan struct{})

	go func() {
		defer close(finished)

		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		_ = conn.SetDeadline(time.Now().Add(3 * time.Second))

		if _, err := protocol.ReceiveMessage(conn, time.Second); err != nil {
			return
		}

		if sendAck {
			ack := &protocol.FetchFileAckHeader{
				Status:    "ok",
				SizeBytes: int64(len(payload)),
				SHA256:    sha256Hex(payload),
			}

			err := protocol.SendMessage(conn, &protocol.Message{
				Type:    protocol.MsgFetchFileAck,
				Payload: ack,
			}, time.Second)
			if err != nil {
				return
			}

			if _, err := conn.Write(payload[:1]); err != nil {
				return
			}
		}

		close(reached)
		select {
		case <-stop:
		case <-time.After(1500 * time.Millisecond):
		}
	}()

	t.Cleanup(func() {
		close(stop)
		_ = listener.Close()
		<-finished
	})

	return listener.Addr().String(), reached
}

func TestFetchClientOverallDeadlineStalledPeer(t *testing.T) {
	payload := []byte("parquet-data")

	for _, tc := range []struct {
		name    string
		sendAck bool
	}{
		{name: "ack_header"},
		{name: "partial_body", sendAck: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			addr, reached := startStalledFetchPeer(t, tc.sendAck, payload)

			client := newFetchClient(t, "reader-1", "test-secret")
			entry := &raft.FileEntry{
				Path:      "db/cpu/2026/09/17/09/file.parquet",
				SizeBytes: int64(len(payload)),
				SHA256:    sha256Hex(payload),
			}

			ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
			defer cancel()

			var dst bytes.Buffer
			start := time.Now()

			_, err := client.Fetch(ctx, addr, entry, &dst, 0, nil)
			elapsed := time.Since(start)

			select {
			case <-reached:
			default:
				t.Fatal("peer never reached the intended stall point")
			}

			if err == nil {
				t.Fatal("stalled fetch unexpectedly succeeded")
			}

			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("expected deadline exceeded, got: %v", err)
			}

			// The socket deadline can fire just before the context timer.
			// The returned error and elapsed time are the fetch contract.

			if elapsed > time.Second {
				t.Fatalf("fetch exceeded overall deadline: elapsed=%s, error=%v", elapsed, err)
			}
		})
	}
}

func TestFetchClientCancellationStalledPeer(t *testing.T) {
	payload := []byte("parquet-data")

	for _, tc := range []struct {
		name    string
		sendAck bool
	}{
		{name: "ack_header"},
		{name: "partial_body", sendAck: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			addr, reached := startStalledFetchPeer(t, tc.sendAck, payload)

			client := newFetchClient(t, "reader-1", "test-secret")
			entry := &raft.FileEntry{
				Path:      "db/cpu/2026/09/17/09/file.parquet",
				SizeBytes: int64(len(payload)),
				SHA256:    sha256Hex(payload),
			}

			// No deadline: cancellation alone must unblock network reads.
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			result := make(chan error, 1)

			go func() {
				var dst bytes.Buffer
				_, err := client.Fetch(ctx, addr, entry, &dst, 0, nil)
				result <- err
			}()

			select {
			case <-reached:
			case <-time.After(2 * time.Second):
				cancel()
				t.Fatal("peer never reached the intended stall point")
			}

			cancel()

			select {
			case err := <-result:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("expected context cancellation, got: %v", err)
				}
			case <-time.After(750 * time.Millisecond):
				t.Fatal("fetch did not stop promptly after cancellation")
			}
		})
	}
}
