//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris

package api

import (
	"net"
	"syscall"

	"golang.org/x/sys/unix"
)

// peekClientConnection inspects TCP data without consuming it. Pending bytes
// remain available to the HTTP server for a pipelined keep-alive request.
func peekClientConnection(conn net.Conn) (pending, disconnected, supported bool) {
	if tlsConn, ok := conn.(interface{ NetConn() net.Conn }); ok {
		conn = tlsConn.NetConn()
	}

	sysConn, ok := conn.(syscall.Conn)
	if !ok {
		return false, false, false
	}
	rawConn, err := sysConn.SyscallConn()
	if err != nil {
		return false, false, false
	}

	supported = true
	controlErr := rawConn.Control(func(fd uintptr) {
		for {
			n, _, recvErr := unix.Recvfrom(int(fd), []byte{0}, unix.MSG_PEEK|unix.MSG_DONTWAIT)
			switch recvErr {
			case nil:
				if n > 0 {
					pending = true
				} else {
					disconnected = true
				}
				return
			case unix.EINTR:
				continue
			case unix.EAGAIN:
				return
			default:
				disconnected, supported = classifyPeekError(recvErr)
				return
			}
		}
	})
	if controlErr != nil {
		return false, false, false
	}
	return pending, disconnected, supported
}

// classifyPeekError treats known peer shutdown errors as disconnects. Unknown
// socket errors disable the watcher rather than cancelling a healthy query.
func classifyPeekError(err error) (disconnected, supported bool) {
	switch err {
	case unix.ECONNRESET, unix.ECONNABORTED, unix.ESHUTDOWN:
		return true, true
	default:
		return false, false
	}
}
