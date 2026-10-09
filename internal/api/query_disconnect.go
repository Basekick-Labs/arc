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
// monitoring stops for this query and leaves those bytes for the HTTP server.
// As with net/http request-context cancellation, a client half-close is treated
// as a disconnect even if it still intends to read the response.
func watchClientDisconnect(queryCtx context.Context, conn net.Conn, onDisconnect func()) <-chan struct{} {
	done := make(chan struct{})
	if conn == nil {
		close(done)
		return done
	}

	go func() {
		defer close(done)
		for {
			select {
			case <-queryCtx.Done():
				return
			default:
			}

			pending, disconnected, supported := peekClientConnection(conn)
			if !supported || pending {
				// A byte may belong to a pipelined keep-alive request. Leave it
				// for the HTTP server; monitoring ends, so a later disconnect on
				// this connection will not be observed for this query.
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
	return done
}
