package security

import (
	"crypto/hmac"
	"crypto/rand"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/hashicorp/raft"
)

// GHSA-wwfh-qrfq-6f8g: Arc's Raft consensus transport performed no
// application-layer authentication. Every OTHER cluster channel (join,
// heartbeat, leave, replication, sync, file-fetch, cache-invalidate) is
// HMAC-authenticated with cluster.shared_secret; the Raft path was the sole
// exception. Any peer that could reach the Raft bind port could speak the
// hashicorp/raft wire protocol and inject a forged higher-term AppendEntries
// carrying CommandCreateToken with admin permissions — full cluster compromise.
//
// This file adds a mutual challenge-response HMAC handshake that runs at
// CONNECTION ESTABLISHMENT, wrapping any raft.StreamLayer. Both peers prove
// knowledge of the shared secret before the connection is handed to
// hashicorp/raft. It works with or without TLS, mirroring the "HMAC even
// without TLS" posture of the coordinator channels.
//
// SCOPE / LIMITATION: this authenticates connection ESTABLISHMENT, not
// per-packet integrity. On a plaintext transport a full on-path MitM could
// still inject into an established stream after a successful handshake. The
// handshake fully blocks the actual vulnerability — a peer that can reach the
// port minting a token — which is the documented threat. Enable
// cluster.tls_enabled for on-path-attacker protection.

const (
	// raftAuthNonceLen is the challenge nonce size in bytes. 32 bytes of CSPRNG
	// output makes a fresh challenge per connection collision-infeasible, so a
	// captured response is useless against the next connection's challenge — no
	// nonce cache is needed (unlike the stateless HTTP message HMACs).
	raftAuthNonceLen = 32
	// raftAuthMACLen is the HMAC-SHA256 output size in bytes.
	raftAuthMACLen = 32
	// raftAuthDeadline bounds the whole handshake. It MUST sit below the Raft
	// HeartbeatTimeout (default 500ms) and well below ElectionTimeout (default
	// 1s): the hashicorp/raft listen loop calls Accept() serially (one
	// goroutine), so a handshake that blocks inside Accept() blocks acceptance
	// of every other peer. A tight deadline caps how long one slow/malicious
	// peer can stall the loop before it is dropped. 400ms leaves headroom under
	// the 500ms heartbeat.
	raftAuthDeadline = 400 * time.Millisecond
)

// AuthenticatedStreamLayer wraps a raft.StreamLayer and performs a mutual
// HMAC handshake on every accepted and dialed connection before handing the
// raw net.Conn to hashicorp/raft.
//
// The handshake reads FIXED-SIZE fields with io.ReadFull directly on the raw
// conn — never a bufio.Reader or a msgpack decoder. hashicorp/raft wraps the
// returned conn in its OWN bufio reader afterward (net_transport.go handleConn
// / getConn), so any bytes a buffered reader pulled past the handshake would be
// silently dropped and desynchronise the very first RPC. Fixed-length
// unbuffered reads consume exactly the handshake and not one byte more.
type AuthenticatedStreamLayer struct {
	inner        raft.StreamLayer
	sharedSecret string
	clusterName  string
}

// NewAuthenticatedStreamLayer wraps inner with the shared-secret handshake.
// sharedSecret must be non-empty (callers fail closed before reaching here).
func NewAuthenticatedStreamLayer(inner raft.StreamLayer, sharedSecret, clusterName string) *AuthenticatedStreamLayer {
	return &AuthenticatedStreamLayer{
		inner:        inner,
		sharedSecret: sharedSecret,
		clusterName:  clusterName,
	}
}

var _ raft.StreamLayer = (*AuthenticatedStreamLayer)(nil)

// Accept accepts an inner connection and runs the server side of the handshake.
// On any handshake failure the connection is closed and the error is returned;
// hashicorp/raft's listen loop logs and continues, so a rejected attacker
// connection simply dies.
func (a *AuthenticatedStreamLayer) Accept() (net.Conn, error) {
	conn, err := a.inner.Accept()
	if err != nil {
		return nil, err
	}
	if err := a.serverHandshake(conn); err != nil {
		conn.Close()
		return nil, fmt.Errorf("raft auth handshake (accept): %w", err)
	}
	return conn, nil
}

// Dial dials an inner connection and runs the client side of the handshake.
func (a *AuthenticatedStreamLayer) Dial(address raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	conn, err := a.inner.Dial(address, timeout)
	if err != nil {
		return nil, err
	}
	if err := a.clientHandshake(conn); err != nil {
		conn.Close()
		return nil, fmt.Errorf("raft auth handshake (dial): %w", err)
	}
	return conn, nil
}

// Close closes the inner listener.
func (a *AuthenticatedStreamLayer) Close() error { return a.inner.Close() }

// Addr returns the inner listener address.
func (a *AuthenticatedStreamLayer) Addr() net.Addr { return a.inner.Addr() }

// raftAuthMAC computes the handshake HMAC over (label, nonce, clusterName).
// The label provides direction domain-separation; clusterName binds the MAC to
// this cluster. Fields are NUL-delimited so no field can be smuggled across a
// boundary to collide with a different arrangement (the discipline ComputeHMAC
// uses). The raw 32-byte MAC is written on the wire (no hex envelope — this is
// a binary handshake, not an HTTP header).
func (a *AuthenticatedStreamLayer) raftAuthMAC(label MsgType, nonce []byte) []byte {
	msg := fmt.Sprintf("%s\x00%s\x00", label, a.clusterName)
	return computeRawHMAC(a.sharedSecret, msg+string(nonce))
}

// serverHandshake: server sends its challenge, the client answers with a MAC
// over it and sends its own challenge, the server verifies then answers the
// client's challenge. Both peers thus prove secret knowledge.
//
//	S -> C : nonceS
//	C -> S : macClient = HMAC(raft-auth,    clusterName, nonceS) || nonceC
//	S -> C : macServer = HMAC(raft-auth-resp, clusterName, nonceC)
func (a *AuthenticatedStreamLayer) serverHandshake(conn net.Conn) error {
	if err := conn.SetDeadline(time.Now().Add(raftAuthDeadline)); err != nil {
		return err
	}

	nonceS := make([]byte, raftAuthNonceLen)
	if _, err := rand.Read(nonceS); err != nil {
		return err
	}
	if _, err := conn.Write(nonceS); err != nil {
		return fmt.Errorf("write server nonce: %w", err)
	}

	macClient := make([]byte, raftAuthMACLen)
	if _, err := io.ReadFull(conn, macClient); err != nil {
		return fmt.Errorf("read client mac: %w", err)
	}
	nonceC := make([]byte, raftAuthNonceLen)
	if _, err := io.ReadFull(conn, nonceC); err != nil {
		return fmt.Errorf("read client nonce: %w", err)
	}

	expected := a.raftAuthMAC(MsgTypeRaftAuth, nonceS)
	if !hmac.Equal(expected, macClient) {
		return fmt.Errorf("client failed authentication")
	}

	macServer := a.raftAuthMAC(MsgTypeRaftAuthResp, nonceC)
	if _, err := conn.Write(macServer); err != nil {
		return fmt.Errorf("write server mac: %w", err)
	}

	// Clear the handshake deadline; raft manages its own per-RPC timeouts.
	return conn.SetDeadline(time.Time{})
}

// clientHandshake is the dial-side mirror of serverHandshake.
func (a *AuthenticatedStreamLayer) clientHandshake(conn net.Conn) error {
	if err := conn.SetDeadline(time.Now().Add(raftAuthDeadline)); err != nil {
		return err
	}

	nonceS := make([]byte, raftAuthNonceLen)
	if _, err := io.ReadFull(conn, nonceS); err != nil {
		return fmt.Errorf("read server nonce: %w", err)
	}

	macClient := a.raftAuthMAC(MsgTypeRaftAuth, nonceS)
	if _, err := conn.Write(macClient); err != nil {
		return fmt.Errorf("write client mac: %w", err)
	}
	nonceC := make([]byte, raftAuthNonceLen)
	if _, err := rand.Read(nonceC); err != nil {
		return err
	}
	if _, err := conn.Write(nonceC); err != nil {
		return fmt.Errorf("write client nonce: %w", err)
	}

	macServer := make([]byte, raftAuthMACLen)
	if _, err := io.ReadFull(conn, macServer); err != nil {
		return fmt.Errorf("read server mac: %w", err)
	}
	expected := a.raftAuthMAC(MsgTypeRaftAuthResp, nonceC)
	if !hmac.Equal(expected, macServer) {
		return fmt.Errorf("server failed authentication")
	}

	return conn.SetDeadline(time.Time{})
}
