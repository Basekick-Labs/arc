package security

import (
	"crypto/hmac"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/raft"
	"github.com/rs/zerolog"
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
	// 1s): the DIAL side runs inline in hashicorp/raft's per-peer replication
	// and heartbeat goroutines, so a peer that never answers holds that
	// goroutine for the full deadline. The ACCEPT side no longer constrains
	// it — server handshakes run off raft's accept loop (see acceptLoop) — but
	// it still bounds how long one silent connection holds an in-flight slot.
	raftAuthDeadline = 400 * time.Millisecond
	// raftAuthMaxInflight caps concurrent server-side handshakes. Without a
	// bound, a flood of silent connections ends at file-descriptor exhaustion
	// for the whole process. At 1024 slots each held at most raftAuthDeadline,
	// a peer must sustain ~2,500 silent connections a second to delay raft's
	// accepts; when every slot is busy the loop stops accepting and the kernel
	// backlog absorbs the excess.
	raftAuthMaxInflight = 1024
	// raftAuthWarnInterval rate-limits the Warn logged when a peer fails
	// authentication, so a probe stays visible without flooding the log.
	raftAuthWarnInterval = 10 * time.Second
	// raftAcceptBaseDelay/raftAcceptMaxDelay back off acceptLoop on a listener
	// error other than close (e.g. EMFILE), mirroring net/http.Server.Serve.
	raftAcceptBaseDelay = 5 * time.Millisecond
	raftAcceptMaxDelay  = 1 * time.Second
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
//
// Server handshakes run concurrently, off the goroutine that calls Accept
// (#1431). hashicorp/raft's listen loop backs off (5ms doubling to 1s) after
// every Accept error and resets only on success. When Accept ran the handshake
// inline and returned its failure, a node stalled for 15s or more woke to a
// backlog of dials its peers had already abandoned: each failed, raft slept up
// to 1s, every newer dial outlived its peer's 400ms deadline in the backlog
// and failed in turn, and the node never rejoined. Accept now returns only
// authenticated connections, so a failed handshake never reaches raft's
// backoff, and a slow or silent peer cannot hold up the others.
type AuthenticatedStreamLayer struct {
	inner        raft.StreamLayer
	sharedSecret string
	clusterName  string
	logger       zerolog.Logger

	startOnce sync.Once
	closeOnce sync.Once
	closeCh   chan struct{}
	ready     chan net.Conn
	slots     chan struct{}

	// lastAuthWarn is the UnixNano of the last authentication-failure Warn;
	// suppressedAuthFailures counts the failures not logged since.
	lastAuthWarn           atomic.Int64
	suppressedAuthFailures atomic.Int64
}

// NewAuthenticatedStreamLayer wraps inner with the shared-secret handshake.
// sharedSecret must be non-empty (callers fail closed before reaching here).
func NewAuthenticatedStreamLayer(inner raft.StreamLayer, sharedSecret, clusterName string) *AuthenticatedStreamLayer {
	return &AuthenticatedStreamLayer{
		inner:        inner,
		sharedSecret: sharedSecret,
		clusterName:  clusterName,
		logger:       zerolog.Nop(),
		closeCh:      make(chan struct{}),
		ready:        make(chan net.Conn),
		slots:        make(chan struct{}, raftAuthMaxInflight),
	}
}

// WithLogger sets the logger for rejected connections and listener errors,
// which raft no longer sees. Call it before the layer is handed to raft.
func (a *AuthenticatedStreamLayer) WithLogger(logger zerolog.Logger) *AuthenticatedStreamLayer {
	a.logger = logger
	return a
}

var _ raft.StreamLayer = (*AuthenticatedStreamLayer)(nil)

// Accept returns the next connection whose peer completed the handshake. A
// connection that fails it is closed and logged here, never returned, so it
// cannot trip hashicorp/raft's accept backoff (#1431). Accept returns
// net.ErrClosed once the layer is closed.
func (a *AuthenticatedStreamLayer) Accept() (net.Conn, error) {
	a.startOnce.Do(func() { go a.acceptLoop() })
	select {
	case conn := <-a.ready:
		// A handshake that finished as Close ran may win the select above.
		select {
		case <-a.closeCh:
			conn.Close()
			return nil, net.ErrClosed
		default:
			return conn, nil
		}
	case <-a.closeCh:
		return nil, net.ErrClosed
	}
}

// acceptLoop accepts inner connections and authenticates each on its own
// goroutine, at most raftAuthMaxInflight at a time.
func (a *AuthenticatedStreamLayer) acceptLoop() {
	var delay time.Duration
	for {
		select {
		case a.slots <- struct{}{}:
		case <-a.closeCh:
			return
		}
		conn, err := a.inner.Accept()
		if err != nil {
			<-a.slots
			select {
			case <-a.closeCh:
				return
			default:
			}
			if errors.Is(err, net.ErrClosed) {
				// The inner listener is gone without Close being called on
				// this layer; nothing more can be accepted.
				a.logger.Error().Err(err).Msg("Raft listener closed; no further peer connections will be accepted")
				a.closeOnce.Do(func() { close(a.closeCh) })
				return
			}
			if delay == 0 {
				delay = raftAcceptBaseDelay
			} else if delay *= 2; delay > raftAcceptMaxDelay {
				delay = raftAcceptMaxDelay
			}
			a.logger.Error().Err(err).Dur("retry_in", delay).Msg("Raft listener accept failed")
			select {
			case <-time.After(delay):
				continue
			case <-a.closeCh:
				return
			}
		}
		delay = 0
		go a.authenticate(conn)
	}
}

// authenticate runs the server handshake on conn and hands it to Accept, or
// closes it.
func (a *AuthenticatedStreamLayer) authenticate(conn net.Conn) {
	err := a.serverHandshake(conn)
	<-a.slots
	if err != nil {
		conn.Close()
		a.logHandshakeFailure(conn, err)
		return
	}
	select {
	case a.ready <- conn:
	case <-a.closeCh:
		conn.Close()
	}
}

// logHandshakeFailure separates a peer that answered with the wrong MAC — the
// security signal, logged at Warn — from a connection that died mid-handshake,
// which is routine after a stall (its dialer gave up) and logged at Debug.
func (a *AuthenticatedStreamLayer) logHandshakeFailure(conn net.Conn, err error) {
	if !errors.Is(err, errRaftAuthClientMAC) {
		a.logger.Debug().Err(err).Str("remote_addr", conn.RemoteAddr().String()).Msg("Raft peer connection dropped during authentication handshake")
		return
	}
	now := time.Now().UnixNano()
	last := a.lastAuthWarn.Load()
	if now-last < int64(raftAuthWarnInterval) || !a.lastAuthWarn.CompareAndSwap(last, now) {
		a.suppressedAuthFailures.Add(1)
		return
	}
	a.logger.Warn().
		Str("remote_addr", conn.RemoteAddr().String()).
		Int64("suppressed_since_last_warning", a.suppressedAuthFailures.Swap(0)).
		Msg("Rejected Raft connection: peer failed shared-secret authentication (wrong cluster.shared_secret or cluster name, or not an Arc node)")
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

// Close stops accepting, unblocks Accept, and closes the inner listener.
func (a *AuthenticatedStreamLayer) Close() error {
	a.closeOnce.Do(func() { close(a.closeCh) })
	return a.inner.Close()
}

// Addr returns the inner listener address.
func (a *AuthenticatedStreamLayer) Addr() net.Addr { return a.inner.Addr() }

// errRaftAuthClientMAC marks a peer that completed the exchange with a MAC
// that does not verify.
var errRaftAuthClientMAC = errors.New("client failed authentication")

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
		return errRaftAuthClientMAC
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
