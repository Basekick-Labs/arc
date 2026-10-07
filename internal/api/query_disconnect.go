package api

import (
	"context"
	"net"
	"time"
)

const clientDisconnectPollInterval = 250 * time.Millisecond

// watchClientDisconnect cancels query work when the client closes the
// connection before the query has produced a response. fasthttp's request
// context is only cancelled during server shutdown, so a non-consuming TCP
// peek is used while DuckDB is still executing. If another request is pending,
// monitoring stops and leaves those bytes for the HTTP server.
func watchClientDisconnect(queryCtx context.Context, conn net.Conn, onDisconnect func()) {
	if conn == nil || isSyntheticTestConn(conn) {
		return
	}

	go func() {
		for {
			select {
			case <-queryCtx.Done():
				return
			default:
			}

			pending, disconnected, supported := peekClientConnection(conn)
			if !supported || pending {
				// A byte may belong to a pipelined keep-alive request. Leave it
				// for the HTTP server rather than consuming it in this probe.
				return
			}
			if disconnected {
				onDisconnect()
				return
			}

			timer := time.NewTimer(clientDisconnectPollInterval)
			select {
			case <-queryCtx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
		}
	}()
}

// Fiber's in-process App.Test connection uses an empty byte buffer and a
// zero-address TCPAddr. It reports EOF as soon as the request bytes have been
// consumed, which is not a client disconnect and must not cancel test queries.
func isSyntheticTestConn(conn net.Conn) bool {
	addr, ok := conn.RemoteAddr().(*net.TCPAddr)
	return ok && addr.Port == 0 && addr.IP.IsUnspecified()
}
