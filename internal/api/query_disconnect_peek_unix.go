//go:build aix || darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris

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
				disconnected = true
				return
			}
		}
	})
	if controlErr != nil {
		return false, false, false
	}
	return pending, disconnected, supported
}
