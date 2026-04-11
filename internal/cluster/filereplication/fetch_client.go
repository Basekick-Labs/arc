package filereplication

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"fmt"
	"io"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/protocol"
	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/cluster/security"
)

// FetchClient is the concrete Fetcher that the puller uses to download file
// bytes from a peer over the coordinator TCP protocol. It dials a fresh
// connection per fetch (no pooling in Phase 2), authenticates with HMAC
// headers, reads the MsgFetchFileAck header, and streams the raw body into
// the destination writer while computing SHA-256 for verification.
type FetchClient struct {
	// SelfNodeID is our node ID, embedded in each request's HMAC payload so
	// the origin peer can identify the caller.
	SelfNodeID string
	// ClusterName is our cluster name — included in the HMAC payload to
	// prevent cross-cluster replay.
	ClusterName string
	// SharedSecret is the cluster-wide HMAC key. Required — callers must
	// refuse to construct a FetchClient without it.
	SharedSecret string
	// TLSConfig wraps the outbound dial in TLS when non-nil. Reuses the
	// cluster TLS config (PR #382) so inter-node traffic is end-to-end
	// encrypted under the cluster PKI, independent of the public API cert.
	TLSConfig *tls.Config
	// DialTimeout is the maximum time to establish the TCP (+TLS) connection.
	// Default 10s if zero.
	DialTimeout time.Duration
	// ResponseHeaderTimeout bounds how long we wait for the MsgFetchFileAck
	// header after sending the request. Default 15s if zero. The body stream
	// itself is bounded by the ctx passed to Fetch.
	ResponseHeaderTimeout time.Duration
}

// NewFetchClient validates the required fields and returns a ready-to-use
// client. Peer replication requires the shared secret, so a missing secret
// is a hard error rather than a fallback to unauthenticated operation.
func NewFetchClient(c FetchClient) (*FetchClient, error) {
	if c.SelfNodeID == "" {
		return nil, fmt.Errorf("filereplication: FetchClient.SelfNodeID is required")
	}
	if c.ClusterName == "" {
		return nil, fmt.Errorf("filereplication: FetchClient.ClusterName is required")
	}
	if c.SharedSecret == "" {
		return nil, fmt.Errorf("filereplication: FetchClient.SharedSecret is required (peer replication must be authenticated)")
	}
	if c.DialTimeout == 0 {
		c.DialTimeout = 10 * time.Second
	}
	if c.ResponseHeaderTimeout == 0 {
		c.ResponseHeaderTimeout = 15 * time.Second
	}
	return &c, nil
}

// Fetch dials the peer, sends a MsgFetchFile request, reads the ack header,
// streams body bytes into dst (while computing SHA-256), and verifies that
// the computed hash matches the manifest SHA-256 from entry.SHA256.
//
// Returns the number of body bytes written and any error. On a checksum
// mismatch the error wraps ErrChecksumMismatch so the puller can distinguish
// mismatches from transport errors.
//
// The connection is always closed before returning.
func (f *FetchClient) Fetch(ctx context.Context, peerAddr string, entry *raft.FileEntry, dst io.Writer) (int64, error) {
	if entry == nil {
		return 0, fmt.Errorf("fetch: entry is nil")
	}
	if entry.SHA256 == "" {
		return 0, fmt.Errorf("fetch: entry has empty SHA256 (manifest is missing checksum)")
	}
	if entry.SizeBytes < 0 {
		return 0, fmt.Errorf("fetch: entry has negative SizeBytes=%d", entry.SizeBytes)
	}

	// Step 1: dial. Use security.Dial which wraps tls.DialWithDialer if TLS
	// is configured, otherwise falls back to plain net.DialTimeout.
	conn, err := security.Dial("tcp", peerAddr, f.DialTimeout, f.TLSConfig)
	if err != nil {
		return 0, fmt.Errorf("dial %s: %w", peerAddr, err)
	}
	defer conn.Close()

	// Honor any deadline the context carries. If there's no deadline we use
	// a tight default — the Puller always passes a bounded context, so this
	// fallback is only defensive in case a caller wires Fetch directly.
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	} else {
		_ = conn.SetDeadline(time.Now().Add(60 * time.Second))
	}

	// Step 2: build and send the MsgFetchFile request. The HMAC binds the
	// request to this specific {nodeID, clusterName, path, timestamp} tuple
	// so a stolen MAC can't be replayed — neither outside the ±5min freshness
	// window nor to fetch a different file within it.
	nonce, err := security.GenerateNonce()
	if err != nil {
		return 0, fmt.Errorf("generate nonce: %w", err)
	}
	ts := time.Now().Unix()
	mac := security.ComputeFetchHMAC(f.SharedSecret, nonce, f.SelfNodeID, f.ClusterName, entry.Path, ts)

	req := &protocol.FetchFileRequest{
		Path:      entry.Path,
		NodeID:    f.SelfNodeID,
		Nonce:     nonce,
		Timestamp: ts,
		HMAC:      mac,
	}
	if err := protocol.SendMessage(conn, &protocol.Message{
		Type:    protocol.MsgFetchFile,
		Payload: req,
	}, 10*time.Second); err != nil {
		return 0, fmt.Errorf("send fetch request: %w", err)
	}

	// Step 3: read the ack header. The header is a framed protocol message,
	// but the body that follows is NOT framed — we'll switch to raw reads.
	ackMsg, err := protocol.ReceiveMessage(conn, f.ResponseHeaderTimeout)
	if err != nil {
		return 0, fmt.Errorf("receive ack header: %w", err)
	}
	if ackMsg.Type != protocol.MsgFetchFileAck {
		return 0, fmt.Errorf("unexpected ack type: %v (wanted MsgFetchFileAck)", ackMsg.Type)
	}
	ack, ok := ackMsg.Payload.(*protocol.FetchFileAckHeader)
	if !ok {
		return 0, fmt.Errorf("ack payload has wrong type: %T", ackMsg.Payload)
	}
	if ack.Status != "ok" {
		return 0, fmt.Errorf("peer rejected fetch: %s", ack.Error)
	}
	if ack.SizeBytes < 0 {
		return 0, fmt.Errorf("ack has negative SizeBytes=%d", ack.SizeBytes)
	}
	if ack.SizeBytes != entry.SizeBytes {
		return 0, fmt.Errorf("ack size mismatch: peer=%d manifest=%d", ack.SizeBytes, entry.SizeBytes)
	}
	if ack.SHA256 != entry.SHA256 {
		// Peer disagrees with our manifest about this file's hash — shouldn't
		// happen in a consistent cluster, but treat it as a mismatch rather
		// than silently pulling possibly-wrong bytes.
		return 0, fmt.Errorf("%w: ack hash=%s manifest=%s", ErrChecksumMismatch, ack.SHA256, entry.SHA256)
	}

	// Step 4: stream exactly SizeBytes of raw body from the connection into
	// dst, tee'ing through a SHA-256 hasher.
	hasher := sha256.New()
	mw := io.MultiWriter(dst, hasher)
	written, err := io.CopyN(mw, conn, ack.SizeBytes)
	if err != nil {
		return written, fmt.Errorf("stream body: %w (wrote %d of %d)", err, written, ack.SizeBytes)
	}

	// Step 5: verify the computed hash matches the expected hash.
	computed := hex.EncodeToString(hasher.Sum(nil))
	if computed != entry.SHA256 {
		return written, fmt.Errorf("%w: computed=%s expected=%s", ErrChecksumMismatch, computed, entry.SHA256)
	}
	return written, nil
}

// Compile-time check that FetchClient satisfies Fetcher.
var _ Fetcher = (*FetchClient)(nil)

// NewRegistryResolver adapts any function that maps node IDs to addresses
// into a PeerResolver. This is the simplest plumbing between the coordinator
// (which owns the registry) and the puller (which needs a resolver).
func NewRegistryResolver(fn func(nodeID string) (string, bool)) PeerResolver {
	return funcResolver(fn)
}

type funcResolver func(string) (string, bool)

func (f funcResolver) ResolvePeer(nodeID string) (string, bool) {
	return f(nodeID)
}
