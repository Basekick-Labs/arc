package security

import (
	"errors"
	"net"
	"time"

	"github.com/hashicorp/raft"
)

// PlainTCPStreamLayer is a plaintext raft.StreamLayer over TCP.
//
// hashicorp/raft ships raft.NewTCPTransport, but its internal TCPStreamLayer
// has unexported fields and no exported constructor, so it cannot be wrapped by
// AuthenticatedStreamLayer. This reimplements the minimal layer — including the
// advertise-address validity checks raft's own newTCPTransport performs
// (tcp_transport.go): reject a nil/unspecified advertise address, since peers
// could not dial back a node advertising 0.0.0.0. Omitting those checks would
// let a node bootstrap alone but silently fail when a second node tries to
// connect.
type PlainTCPStreamLayer struct {
	advertise net.Addr
	listener  *net.TCPListener
}

var (
	errNotTCP          = errors.New("local address is not a TCP address")
	errNotAdvertisable = errors.New("local bind address is not advertisable")
)

// NewPlainTCPStreamLayer binds bindAddr and returns a plaintext stream layer.
func NewPlainTCPStreamLayer(bindAddr string, advertise net.Addr) (*PlainTCPStreamLayer, error) {
	list, err := net.Listen("tcp", bindAddr)
	if err != nil {
		return nil, err
	}
	s := &PlainTCPStreamLayer{
		advertise: advertise,
		listener:  list.(*net.TCPListener),
	}
	// Mirror raft's newTCPTransport advertise-address validation.
	addr, ok := s.Addr().(*net.TCPAddr)
	if !ok {
		list.Close()
		return nil, errNotTCP
	}
	if addr.IP == nil || addr.IP.IsUnspecified() {
		list.Close()
		return nil, errNotAdvertisable
	}
	return s, nil
}

// Dial connects to a Raft peer over plain TCP.
func (t *PlainTCPStreamLayer) Dial(address raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	return net.DialTimeout("tcp", string(address), timeout)
}

// Accept waits for an incoming plaintext connection.
func (t *PlainTCPStreamLayer) Accept() (net.Conn, error) { return t.listener.Accept() }

// Close closes the listener.
func (t *PlainTCPStreamLayer) Close() error { return t.listener.Close() }

// Addr returns the advertised address if set, else the listener address.
func (t *PlainTCPStreamLayer) Addr() net.Addr {
	if t.advertise != nil {
		return t.advertise
	}
	return t.listener.Addr()
}

var _ raft.StreamLayer = (*PlainTCPStreamLayer)(nil)
