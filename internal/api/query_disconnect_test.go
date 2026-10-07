package api

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"
)

func newTCPConnPair(t *testing.T) (net.Conn, net.Conn) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })

	client, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })

	server, err := listener.Accept()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Close() })
	return server, client
}

func TestWatchClientDisconnectCancelsQuery(t *testing.T) {
	serverConn, clientConn := newTCPConnPair(t)

	queryCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	disconnected := make(chan struct{})
	watchClientDisconnect(queryCtx, serverConn, func() {
		close(disconnected)
		cancel()
	})

	if err := clientConn.Close(); err != nil {
		t.Fatalf("close client connection: %v", err)
	}

	select {
	case <-disconnected:
	case <-time.After(time.Second):
		t.Fatal("client disconnect was not observed")
	}

	select {
	case <-queryCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("query context was not cancelled")
	}
}

func TestWatchClientDisconnectStopsWithQuery(t *testing.T) {
	serverConn, _ := newTCPConnPair(t)

	queryCtx, cancel := context.WithCancel(context.Background())
	disconnected := make(chan struct{})
	watchClientDisconnect(queryCtx, serverConn, func() {
		close(disconnected)
	})

	cancel()

	select {
	case <-disconnected:
		t.Fatal("query completion was reported as a client disconnect")
	case <-time.After(300 * time.Millisecond):
	}
}

func TestWatchClientDisconnectPreservesPipelinedHTTPRequest(t *testing.T) {
	serverConn, clientConn := newTCPConnPair(t)
	reader := bufio.NewReader(serverConn)

	if _, err := fmt.Fprint(clientConn, "GET /long-query HTTP/1.1\r\nHost: example.test\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	first, err := http.ReadRequest(reader)
	if err != nil {
		t.Fatalf("read first request: %v", err)
	}
	_ = first.Body.Close()

	queryCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	disconnected := make(chan struct{})
	watchClientDisconnect(queryCtx, serverConn, func() {
		close(disconnected)
		cancel()
	})

	const nextRequest = "GET /follow-up HTTP/1.1\r\nHost: example.test\r\n\r\n"
	if _, err := fmt.Fprint(clientConn, nextRequest); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(time.Second)
	for {
		pending, disconnected, supported := peekClientConnection(serverConn)
		if !supported {
			t.Fatal("TCP peek is not supported for test connection")
		}
		if pending {
			break
		}
		if disconnected || time.Now().After(deadline) {
			t.Fatal("pipelined request was not visible to non-consuming peek")
		}
		time.Sleep(time.Millisecond)
	}

	second, err := http.ReadRequest(reader)
	if err != nil {
		t.Fatalf("read pipelined request after disconnect watcher: %v", err)
	}
	_ = second.Body.Close()
	if second.URL.Path != "/follow-up" {
		t.Fatalf("second request path = %q, want /follow-up", second.URL.Path)
	}
	select {
	case <-disconnected:
		t.Fatal("pipelined request was mistaken for a disconnect")
	default:
	}
	cancel()
}
