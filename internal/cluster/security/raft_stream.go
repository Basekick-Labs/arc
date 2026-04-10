package security

import (
	"crypto/tls"
	"net"
	"time"

	"github.com/hashicorp/raft"
)

// TLSStreamLayer implements raft.StreamLayer with TLS.
type TLSStreamLayer struct {
	listener  net.Listener
	advertise net.Addr
	tlsConfig *tls.Config
}

// NewTLSStreamLayer creates a TLS-enabled stream layer for Raft.
func NewTLSStreamLayer(bindAddr string, advertise net.Addr, tlsCfg *tls.Config) (*TLSStreamLayer, error) {
	listener, err := tls.Listen("tcp", bindAddr, tlsCfg)
	if err != nil {
		return nil, err
	}
	return &TLSStreamLayer{
		listener:  listener,
		advertise: advertise,
		tlsConfig: tlsCfg,
	}, nil
}

// Accept waits for incoming TLS connections.
func (t *TLSStreamLayer) Accept() (net.Conn, error) {
	return t.listener.Accept()
}

// Close closes the listener.
func (t *TLSStreamLayer) Close() error {
	return t.listener.Close()
}

// Addr returns the listener address or the advertised address if set.
func (t *TLSStreamLayer) Addr() net.Addr {
	if t.advertise != nil {
		return t.advertise
	}
	return t.listener.Addr()
}

// Dial connects to a Raft peer with TLS.
func (t *TLSStreamLayer) Dial(address raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	dialer := &net.Dialer{Timeout: timeout}
	return tls.DialWithDialer(dialer, "tcp", string(address), t.tlsConfig)
}

// Compile-time check that TLSStreamLayer satisfies raft.StreamLayer.
var _ raft.StreamLayer = (*TLSStreamLayer)(nil)
