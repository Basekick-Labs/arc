//go:build !aix && !darwin && !dragonfly && !freebsd && !linux && !netbsd && !openbsd && !solaris

package api

import "net"

// Platforms without a non-consuming TCP peek do not probe the connection;
// consuming bytes could corrupt a subsequent keep-alive request.
func peekClientConnection(net.Conn) (pending, disconnected, supported bool) {
	return false, false, false
}
