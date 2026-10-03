package api

import (
	"context"
	"errors"
	"net"
	"time"
)

const clientDisconnectPollInterval = 250 * time.Millisecond

// watchClientDisconnect cancels query work when the client closes the
// connection before the query has produced a response. fasthttp's request
// context is only cancelled during server shutdown, so a connection read is
// the only signal available while DuckDB is still executing.
//
// The request body has already been consumed by the time query handlers call
// this function. The one-byte probe is therefore only used while this
// request owns the connection; it is stopped as soon as queryCtx is done.
func watchClientDisconnect(queryCtx context.Context, conn net.Conn, onDisconnect func()) {
	if conn == nil || isSyntheticTestConn(conn) {
		return
	}

	go func() {
		defer func() {
			_ = conn.SetReadDeadline(time.Time{})
		}()

		probe := make([]byte, 1)
		for {
			select {
			case <-queryCtx.Done():
				return
			default:
			}

			_ = conn.SetReadDeadline(time.Now().Add(clientDisconnectPollInterval))
			_, err := conn.Read(probe)
			if err == nil {
				continue
			}

			var netErr net.Error
			if errors.As(err, &netErr) && netErr.Timeout() {
				continue
			}

			select {
			case <-queryCtx.Done():
				return
			default:
				onDisconnect()
				return
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
