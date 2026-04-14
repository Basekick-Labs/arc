package filereplication

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"fmt"
	"hash"
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
// byteOffset is the byte position to resume from (0 = full fetch). When
// byteOffset > 0, prefixHasher must be a hash.Hash pre-fed with bytes
// [0, byteOffset) from the partial local file. Fetch streams bytes
// [byteOffset, entry.SizeBytes) through the same hasher and verifies the
// final digest against entry.SHA256. When byteOffset == 0, prefixHasher
// must be nil; Fetch creates a fresh sha256 hasher internally.
//
// Returns the number of tail bytes written to dst and any error. On a
// checksum mismatch the error wraps ErrChecksumMismatch. On a bad-offset
// rejection from the peer the error wraps ErrBadOffset.
//
// The connection is always closed before returning.
func (f *FetchClient) Fetch(ctx context.Context, peerAddr string, entry *raft.FileEntry, dst io.Writer, byteOffset int64, prefixHasher hash.Hash) (int64, error) {
	if entry == nil {
		return 0, fmt.Errorf("fetch: entry is nil")
	}
	if entry.SHA256 == "" {
		return 0, fmt.Errorf("fetch: entry has empty SHA256 (manifest is missing checksum)")
	}
	if entry.SizeBytes < 0 {
		return 0, fmt.Errorf("fetch: entry has negative SizeBytes=%d", entry.SizeBytes)
	}
	if byteOffset < 0 {
		return 0, fmt.Errorf("fetch: negative byteOffset=%d", byteOffset)
	}
	if byteOffset > 0 && prefixHasher == nil {
		return 0, fmt.Errorf("fetch: byteOffset=%d but prefixHasher is nil", byteOffset)
	}
	if byteOffset == 0 && prefixHasher != nil {
		return 0, fmt.Errorf("fetch: byteOffset=0 but prefixHasher is non-nil")
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
	// window nor to fetch a different file within it. ByteOffset is NOT bound
	// into the HMAC — binding it would invalidate the MAC on resume since the
	// path is already bound; offset is positional within that file.
	nonce, err := security.GenerateNonce()
	if err != nil {
		return 0, fmt.Errorf("generate nonce: %w", err)
	}
	ts := time.Now().Unix()
	mac := security.ComputeFetchHMAC(f.SharedSecret, nonce, f.SelfNodeID, f.ClusterName, entry.Path, ts)

	req := &protocol.FetchFileRequest{
		Path:       entry.Path,
		NodeID:     f.SelfNodeID,
		Nonce:      nonce,
		Timestamp:  ts,
		HMAC:       mac,
		ByteOffset: byteOffset,
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
		// bad_offset means the server rejected our resume offset. The puller
		// should delete the partial file and retry from zero — not fall through
		// to another peer (the file exists there, the offset is just stale).
		if ack.Code == protocol.AckCodeBadOffset {
			return 0, fmt.Errorf("%w: %s", ErrBadOffset, ack.Error)
		}
		// not_found or manifest: peer simply doesn't hold this file.
		if isFileNotOnPeerAck(ack) {
			return 0, fmt.Errorf("%w: %s", ErrFileNotOnPeer, ack.Error)
		}
		return 0, fmt.Errorf("peer rejected fetch: %s", ack.Error)
	}
	// Validate the ack's offset echo — confirms the server honored our offset.
	if ack.ByteOffset != byteOffset {
		return 0, fmt.Errorf("byte offset echo mismatch: requested=%d acked=%d", byteOffset, ack.ByteOffset)
	}
	expectedTail := entry.SizeBytes - byteOffset
	if ack.SizeBytes < 0 {
		return 0, fmt.Errorf("ack has negative SizeBytes=%d", ack.SizeBytes)
	}
	if ack.SizeBytes != expectedTail {
		return 0, fmt.Errorf("ack size mismatch: peer tail=%d expected tail=%d (offset=%d)", ack.SizeBytes, expectedTail, byteOffset)
	}
	if ack.SHA256 != entry.SHA256 {
		// Peer disagrees with our manifest about this file's hash — shouldn't
		// happen in a consistent cluster, but treat it as a mismatch rather
		// than silently pulling possibly-wrong bytes.
		return 0, fmt.Errorf("%w: ack hash=%s manifest=%s", ErrChecksumMismatch, ack.SHA256, entry.SHA256)
	}

	// Step 4: stream exactly ack.SizeBytes (tail) of raw body from the
	// connection into dst, tee'ing through the SHA-256 hasher.
	// For a full fetch (byteOffset==0), create a fresh hasher.
	// For a resume, reuse the caller-supplied prefixHasher which already
	// absorbed the prefix bytes — so the final digest covers the whole file.
	var hasher hash.Hash
	if prefixHasher != nil {
		hasher = prefixHasher
	} else {
		hasher = sha256.New()
	}
	mw := io.MultiWriter(dst, hasher)
	written, err := io.CopyN(mw, conn, ack.SizeBytes)
	if err != nil {
		return written, fmt.Errorf("stream body: %w (wrote %d of %d tail bytes)", err, written, ack.SizeBytes)
	}

	// Step 5: verify the computed hash matches the expected whole-file hash.
	computed := hex.EncodeToString(hasher.Sum(nil))
	if computed != entry.SHA256 {
		return written, fmt.Errorf("%w: computed=%s expected=%s", ErrChecksumMismatch, computed, entry.SHA256)
	}
	return written, nil
}

// Compile-time check that FetchClient satisfies Fetcher.
var _ Fetcher = (*FetchClient)(nil)

// isFileNotOnPeerAck reports whether an error ack indicates the peer simply
// doesn't have the requested file (as opposed to auth failure, backend error,
// or Raft unavailability, which should NOT trigger multi-peer fallback).
//
// The primary signal is the typed Code field added in Phase 3. If Code is
// empty (Phase 2 peer predating the field), we fall back to exact-match on
// ack.Error against protocol.ErrMsgFileNotInManifest / ErrMsgFileNotFound,
// which are the strings the Phase 2 server emits. Exact match (not
// substring) prevents an adversary from crafting an Error value that
// confuses the check — e.g. "file not found on local backend: /etc/passwd"
// would NOT match. The server-side strings and this fallback live together
// in internal/cluster/protocol so a refactor touches both sites at once.
func isFileNotOnPeerAck(ack *protocol.FetchFileAckHeader) bool {
	if ack == nil {
		return false
	}
	switch ack.Code {
	case protocol.AckCodeNotFound, protocol.AckCodeManifest:
		return true
	case "":
		// Phase 2 peer — fall back to exact-match on the known strings.
		return ack.Error == protocol.ErrMsgFileNotInManifest ||
			ack.Error == protocol.ErrMsgFileNotFound
	default:
		return false
	}
}

// NewRegistryResolver adapts a function that maps (originNodeID, path) to
// an ordered list of candidate peer addresses into a PeerResolver. Typical
// usage: return the origin address first (if still healthy) followed by
// any other healthy peers — that way Phase 3 catch-up still works after a
// Kubernetes pod rotation when the original writer is gone.
func NewRegistryResolver(fn func(originNodeID, path string) []string) PeerResolver {
	return funcResolver(fn)
}

type funcResolver func(string, string) []string

func (f funcResolver) ResolvePeers(originNodeID, path string) []string {
	return f(originNodeID, path)
}
