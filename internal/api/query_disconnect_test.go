package api

import (
	"context"
	"net"
	"testing"
	"time"
)

func TestWatchClientDisconnectCancelsQuery(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()

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
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

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
