package filereplication

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/protocol"
	"github.com/basekick-labs/arc/internal/cluster/raft"
)

// fakePeer is a minimal TCP server that speaks the Arc cluster protocol for
// fetch requests. Each test constructs one, scripts a single response, and
// points a FetchClient at its listen address.
type fakePeer struct {
	t        *testing.T
	listener net.Listener

	// Scripted response
	mu          sync.Mutex
	ack         protocol.FetchFileAckHeader
	body        []byte
	validateReq func(*protocol.FetchFileRequest) error // optional
	errBefore   bool                                   // if true, close the conn without replying

	wg sync.WaitGroup
}

func startFakePeer(t *testing.T) *fakePeer {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	p := &fakePeer{t: t, listener: l}
	p.wg.Add(1)
	go p.acceptLoop()
	return p
}

func (p *fakePeer) addr() string { return p.listener.Addr().String() }

func (p *fakePeer) scriptOK(sum string, body []byte) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.ack = protocol.FetchFileAckHeader{
		Status:    "ok",
		SizeBytes: int64(len(body)),
		SHA256:    sum,
	}
	p.body = body
}

func (p *fakePeer) scriptError(reason string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.ack = protocol.FetchFileAckHeader{Status: "error", Error: reason}
	p.body = nil
}

// scriptErrorCode sets both Code and Error on the next ack. Phase 3 peers
// populate Code; Phase 2 peers leave it empty and the client falls back to
// exact-matching Error.
func (p *fakePeer) scriptErrorCode(code protocol.AckErrorCode, reason string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.ack = protocol.FetchFileAckHeader{Status: "error", Code: code, Error: reason}
	p.body = nil
}

func (p *fakePeer) scriptCloseBeforeReply() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.errBefore = true
}

func (p *fakePeer) setRequestValidator(fn func(*protocol.FetchFileRequest) error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.validateReq = fn
}

func (p *fakePeer) stop() {
	p.listener.Close()
	p.wg.Wait()
}

func (p *fakePeer) acceptLoop() {
	defer p.wg.Done()
	for {
		conn, err := p.listener.Accept()
		if err != nil {
			return
		}
		go p.handleConn(conn)
	}
}

func (p *fakePeer) handleConn(conn net.Conn) {
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))

	// Read one message — expect MsgFetchFile.
	msg, err := protocol.ReceiveMessage(conn, 2*time.Second)
	if err != nil {
		return
	}
	req, ok := msg.Payload.(*protocol.FetchFileRequest)
	if !ok {
		return
	}

	p.mu.Lock()
	validator := p.validateReq
	closeBefore := p.errBefore
	ack := p.ack
	body := p.body
	p.mu.Unlock()

	if closeBefore {
		return
	}
	if validator != nil {
		if err := validator(req); err != nil {
			_ = protocol.SendMessage(conn, &protocol.Message{
				Type:    protocol.MsgFetchFileAck,
				Payload: &protocol.FetchFileAckHeader{Status: "error", Error: err.Error()},
			}, 2*time.Second)
			return
		}
	}

	// Echo the requested ByteOffset back in the ack so the client can validate it.
	// For resume requests, also trim the body and adjust SizeBytes to the tail.
	sendAck := ack
	sendBody := body
	if ack.Status == "ok" && req.ByteOffset > 0 && req.ByteOffset <= int64(len(body)) {
		sendAck.ByteOffset = req.ByteOffset
		sendAck.SizeBytes = int64(len(body)) - req.ByteOffset
		sendBody = body[req.ByteOffset:]
	}

	if err := protocol.SendMessage(conn, &protocol.Message{
		Type:    protocol.MsgFetchFileAck,
		Payload: &sendAck,
	}, 2*time.Second); err != nil {
		return
	}
	if sendAck.Status != "ok" {
		return
	}
	if len(sendBody) > 0 {
		_, _ = conn.Write(sendBody)
	}
}

// --- Tests ---------------------------------------------------------------

func sha256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func newFetchClient(t *testing.T, selfID, secret string) *FetchClient {
	t.Helper()
	fc, err := NewFetchClient(FetchClient{
		SelfNodeID:            selfID,
		ClusterName:           "test-cluster",
		SharedSecret:          secret,
		DialTimeout:           2 * time.Second,
		ResponseHeaderTimeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewFetchClient: %v", err)
	}
	return fc
}

func TestFetchClientHappyPath(t *testing.T) {
	body := []byte("parquet bytes 0123456789")
	sum := sha256Hex(body)

	peer := startFakePeer(t)
	defer peer.stop()
	peer.scriptOK(sum, body)

	fc := newFetchClient(t, "reader-1", "shared-secret-xyz")
	entry := &raft.FileEntry{
		Path:      "db/cpu/file.parquet",
		SizeBytes: int64(len(body)),
		SHA256:    sum,
	}
	var dst bytes.Buffer
	n, err := fc.Fetch(context.Background(), peer.addr(), entry, &dst, 0, nil)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if n != int64(len(body)) {
		t.Errorf("bytes written: got %d, want %d", n, len(body))
	}
	if dst.String() != string(body) {
		t.Errorf("body mismatch")
	}
}

func TestFetchClientChecksumMismatch(t *testing.T) {
	body := []byte("real body")
	wrongSum := sha256Hex([]byte("fake"))

	peer := startFakePeer(t)
	defer peer.stop()
	// Peer sends real bytes but the ack advertises a different checksum that
	// the client will accept (we match advertised vs manifest), then the
	// computed hash of the actual bytes won't match the manifest.
	peer.scriptOK(wrongSum, body)

	fc := newFetchClient(t, "reader-1", "shared-secret-xyz")
	manifestSum := sha256Hex(body) // the manifest thinks the file hashes to this
	entry := &raft.FileEntry{
		Path:      "db/cpu/mismatch.parquet",
		SizeBytes: int64(len(body)),
		SHA256:    manifestSum,
	}
	var dst bytes.Buffer
	_, err := fc.Fetch(context.Background(), peer.addr(), entry, &dst, 0, nil)
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	if !errors.Is(err, ErrChecksumMismatch) {
		t.Errorf("expected ErrChecksumMismatch, got %v", err)
	}
}

// TestFetchClientBodyHashMismatch covers the case where the peer returns the
// correct advertised SHA-256 in the ack header (matching our manifest) but
// actually sends bytes with a different hash. The client computes the hash
// on the wire and should reject the bytes as ErrChecksumMismatch.
func TestFetchClientBodyHashMismatch(t *testing.T) {
	actualBody := []byte("totally different bytes")
	claimedBody := []byte("parquet bytes 0123456789")
	claimedSum := sha256Hex(claimedBody)

	peer := startFakePeer(t)
	defer peer.stop()
	peer.scriptOK(claimedSum, actualBody)

	fc := newFetchClient(t, "reader-1", "shared-secret-xyz")
	entry := &raft.FileEntry{
		Path:      "db/cpu/tampered.parquet",
		SizeBytes: int64(len(actualBody)),
		SHA256:    claimedSum,
	}
	var dst bytes.Buffer
	_, err := fc.Fetch(context.Background(), peer.addr(), entry, &dst, 0, nil)
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	if !errors.Is(err, ErrChecksumMismatch) {
		t.Errorf("expected ErrChecksumMismatch, got %v", err)
	}
}

func TestFetchClientErrorAck(t *testing.T) {
	peer := startFakePeer(t)
	defer peer.stop()
	peer.scriptError("file not in manifest")

	fc := newFetchClient(t, "reader-1", "shared-secret-xyz")
	entry := &raft.FileEntry{
		Path:      "db/cpu/missing.parquet",
		SizeBytes: 42,
		SHA256:    sha256Hex([]byte("dummy")),
	}
	var dst bytes.Buffer
	_, err := fc.Fetch(context.Background(), peer.addr(), entry, &dst, 0, nil)
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
}

func TestFetchClientSizeMismatch(t *testing.T) {
	body := []byte("short")
	sum := sha256Hex(body)
	peer := startFakePeer(t)
	defer peer.stop()
	peer.scriptOK(sum, body)

	fc := newFetchClient(t, "reader-1", "shared-secret-xyz")
	// Manifest claims 100 bytes but peer says 5 — should fail.
	entry := &raft.FileEntry{
		Path:      "db/cpu/size.parquet",
		SizeBytes: 100,
		SHA256:    sum,
	}
	var dst bytes.Buffer
	_, err := fc.Fetch(context.Background(), peer.addr(), entry, &dst, 0, nil)
	if err == nil {
		t.Fatalf("expected size mismatch error, got nil")
	}
}

func TestFetchClientDialError(t *testing.T) {
	fc := newFetchClient(t, "reader-1", "shared-secret-xyz")
	entry := &raft.FileEntry{
		Path:      "db/cpu/dead.parquet",
		SizeBytes: 10,
		SHA256:    sha256Hex([]byte("xxxxxxxxxx")),
	}
	var dst bytes.Buffer
	// Port 1 on loopback should refuse connections.
	_, err := fc.Fetch(context.Background(), "127.0.0.1:1", entry, &dst, 0, nil)
	if err == nil {
		t.Fatalf("expected dial error, got nil")
	}
}

func TestFetchClientHMACSent(t *testing.T) {
	body := []byte("hmac test body")
	sum := sha256Hex(body)

	peer := startFakePeer(t)
	defer peer.stop()
	peer.scriptOK(sum, body)

	var captured *protocol.FetchFileRequest
	peer.setRequestValidator(func(r *protocol.FetchFileRequest) error {
		cp := *r
		captured = &cp
		return nil
	})

	fc := newFetchClient(t, "reader-1", "shared-secret-xyz")
	entry := &raft.FileEntry{
		Path:      "db/cpu/auth.parquet",
		SizeBytes: int64(len(body)),
		SHA256:    sum,
	}
	var dst bytes.Buffer
	if _, err := fc.Fetch(context.Background(), peer.addr(), entry, &dst, 0, nil); err != nil {
		t.Fatalf("Fetch: %v", err)
	}

	if captured == nil {
		t.Fatal("peer did not capture request")
	}
	if captured.NodeID != "reader-1" {
		t.Errorf("NodeID: got %q, want reader-1", captured.NodeID)
	}
	if captured.HMAC == "" {
		t.Errorf("HMAC should be set")
	}
	if captured.Nonce == "" {
		t.Errorf("Nonce should be set")
	}
	if captured.Timestamp == 0 {
		t.Errorf("Timestamp should be set")
	}
	if captured.Path != entry.Path {
		t.Errorf("Path: got %q, want %q", captured.Path, entry.Path)
	}
}

func TestFetchClientPeerDropsBeforeReply(t *testing.T) {
	peer := startFakePeer(t)
	defer peer.stop()
	peer.scriptCloseBeforeReply()

	fc := newFetchClient(t, "reader-1", "shared-secret-xyz")
	entry := &raft.FileEntry{
		Path:      "db/cpu/drop.parquet",
		SizeBytes: 10,
		SHA256:    sha256Hex([]byte("xxxxxxxxxx")),
	}
	var dst bytes.Buffer
	_, err := fc.Fetch(context.Background(), peer.addr(), entry, &dst, 0, nil)
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
}

func TestFetchClientConfigValidation(t *testing.T) {
	cases := []struct {
		name string
		c    FetchClient
	}{
		{
			name: "missing self",
			c:    FetchClient{ClusterName: "c", SharedSecret: "s"},
		},
		{
			name: "missing cluster name",
			c:    FetchClient{SelfNodeID: "n", SharedSecret: "s"},
		},
		{
			name: "missing shared secret",
			c:    FetchClient{SelfNodeID: "n", ClusterName: "c"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewFetchClient(tc.c)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
		})
	}
}

// Sanity: ensure io.CopyN-driven read path works with body larger than
// the protocol MaxMessageSize. The body is raw bytes, not a framed message,
// so this should not trigger the codec's 1 MB limit.
func TestFetchClientLargeBody(t *testing.T) {
	const size = 2 * 1024 * 1024 // 2 MiB
	body := bytes.Repeat([]byte("a"), size)
	sum := sha256Hex(body)

	peer := startFakePeer(t)
	defer peer.stop()
	peer.scriptOK(sum, body)

	fc := newFetchClient(t, "reader-1", "shared-secret-xyz")
	entry := &raft.FileEntry{
		Path:      "db/cpu/large.parquet",
		SizeBytes: int64(len(body)),
		SHA256:    sum,
	}
	// Discard dst — we only care that the Fetch succeeds on a large body.
	n, err := fc.Fetch(context.Background(), peer.addr(), entry, io.Discard, 0, nil)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if n != int64(size) {
		t.Errorf("bytes: got %d, want %d", n, size)
	}
}

// --- Phase 3 tests: ErrFileNotOnPeer mapping ---

// TestFetchClientErrFileNotOnPeerCodeNotFound verifies the machine-readable
// Phase 3 Code field drives the fallback decision. A peer that responds with
// Code="not_found" should trigger ErrFileNotOnPeer so the puller can fall
// through to the next candidate.
func TestFetchClientErrFileNotOnPeerCodeNotFound(t *testing.T) {
	peer := startFakePeer(t)
	defer peer.stop()
	peer.scriptErrorCode(protocol.AckCodeNotFound, protocol.ErrMsgFileNotFound)

	fc := newFetchClient(t, "reader-1", "shared-secret-xyz")
	entry := &raft.FileEntry{
		Path:      "db/cpu/missing.parquet",
		SizeBytes: 42,
		SHA256:    sha256Hex([]byte("dummy")),
	}
	var dst bytes.Buffer
	_, err := fc.Fetch(context.Background(), peer.addr(), entry, &dst, 0, nil)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, ErrFileNotOnPeer) {
		t.Errorf("expected ErrFileNotOnPeer, got %v", err)
	}
}

// TestFetchClientErrFileNotOnPeerCodeManifest verifies Code="manifest" (file
// not in the Raft manifest on the peer) also triggers fallback — a peer
// that doesn't know about the file shouldn't fail the entire attempt.
func TestFetchClientErrFileNotOnPeerCodeManifest(t *testing.T) {
	peer := startFakePeer(t)
	defer peer.stop()
	peer.scriptErrorCode(protocol.AckCodeManifest, protocol.ErrMsgFileNotInManifest)

	fc := newFetchClient(t, "reader-1", "shared-secret-xyz")
	entry := &raft.FileEntry{
		Path:      "db/cpu/offmanifest.parquet",
		SizeBytes: 42,
		SHA256:    sha256Hex([]byte("dummy")),
	}
	var dst bytes.Buffer
	_, err := fc.Fetch(context.Background(), peer.addr(), entry, &dst, 0, nil)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, ErrFileNotOnPeer) {
		t.Errorf("expected ErrFileNotOnPeer, got %v", err)
	}
}

// TestFetchClientErrFileNotOnPeerPhase2Fallback verifies that when Code is
// empty (Phase 2 peer that predates the field), the client falls back to
// substring-matching the human-readable error text. This locks in the
// backward-compat contract so a Phase 3 node can catch up from a Phase 2
// origin.
func TestFetchClientErrFileNotOnPeerPhase2Fallback(t *testing.T) {
	cases := []struct {
		name   string
		reason string
	}{
		{"not_in_manifest", "file not in manifest"},
		{"not_on_backend", "file not found on local backend"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			peer := startFakePeer(t)
			defer peer.stop()
			// scriptError leaves Code empty — Phase 2 shape.
			peer.scriptError(tc.reason)

			fc := newFetchClient(t, "reader-1", "shared-secret-xyz")
			entry := &raft.FileEntry{
				Path:      "db/cpu/phase2-fallback.parquet",
				SizeBytes: 42,
				SHA256:    sha256Hex([]byte("dummy")),
			}
			var dst bytes.Buffer
			_, err := fc.Fetch(context.Background(), peer.addr(), entry, &dst, 0, nil)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if !errors.Is(err, ErrFileNotOnPeer) {
				t.Errorf("expected ErrFileNotOnPeer for reason %q (Phase 2 peer fallback), got %v", tc.reason, err)
			}
		})
	}
}

// TestFetchClientAuthErrorIsNotFallback verifies that auth failures do NOT
// map to ErrFileNotOnPeer — otherwise a misconfigured cluster would silently
// fall through every healthy peer and give up with a generic "max attempts"
// error, hiding the real problem.
func TestFetchClientAuthErrorIsNotFallback(t *testing.T) {
	peer := startFakePeer(t)
	defer peer.stop()
	peer.scriptErrorCode(protocol.AckCodeAuth, "authentication failed")

	fc := newFetchClient(t, "reader-1", "shared-secret-xyz")
	entry := &raft.FileEntry{
		Path:      "db/cpu/auth-fail.parquet",
		SizeBytes: 42,
		SHA256:    sha256Hex([]byte("dummy")),
	}
	var dst bytes.Buffer
	_, err := fc.Fetch(context.Background(), peer.addr(), entry, &dst, 0, nil)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if errors.Is(err, ErrFileNotOnPeer) {
		t.Errorf("auth errors must NOT map to ErrFileNotOnPeer, got %v", err)
	}
}

// TestFetchClientBackendErrorIsNotFallback — same reasoning: a peer with a
// broken backend isn't "file not here", it's an operational problem that
// should surface rather than cause silent fallback.
func TestFetchClientBackendErrorIsNotFallback(t *testing.T) {
	peer := startFakePeer(t)
	defer peer.stop()
	peer.scriptErrorCode(protocol.AckCodeBackend, "backend error")

	fc := newFetchClient(t, "reader-1", "shared-secret-xyz")
	entry := &raft.FileEntry{
		Path:      "db/cpu/backend-fail.parquet",
		SizeBytes: 42,
		SHA256:    sha256Hex([]byte("dummy")),
	}
	var dst bytes.Buffer
	_, err := fc.Fetch(context.Background(), peer.addr(), entry, &dst, 0, nil)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if errors.Is(err, ErrFileNotOnPeer) {
		t.Errorf("backend errors must NOT map to ErrFileNotOnPeer, got %v", err)
	}
}

// --- Resume / ByteOffset tests -------------------------------------------

// TestFetchClientResume_HappyPath verifies that a client can resume a partial
// transfer. The peer serves only the tail bytes; the client chains the
// provided prefixHasher and verifies the full-file SHA-256.
func TestFetchClientResume_HappyPath(t *testing.T) {
	fullBody := []byte("0123456789abcdefghij") // 20 bytes
	prefix := fullBody[:10]
	tail := fullBody[10:]
	fullSum := sha256Hex(fullBody)

	peer := startFakePeer(t)
	defer peer.stop()
	// Script the full body — the fake peer will slice it based on ByteOffset.
	peer.scriptOK(fullSum, fullBody)

	fc := newFetchClient(t, "reader-1", "shared-secret-xyz")
	entry := &raft.FileEntry{
		Path:      "db/cpu/resume.parquet",
		SizeBytes: int64(len(fullBody)),
		SHA256:    fullSum,
	}

	// Pre-seed the hasher with the prefix bytes, as the puller would.
	prefixHasher := sha256.New()
	prefixHasher.Write(prefix)

	var dst bytes.Buffer
	n, err := fc.Fetch(context.Background(), peer.addr(), entry, &dst, int64(len(prefix)), prefixHasher)
	if err != nil {
		t.Fatalf("Fetch resume: %v", err)
	}
	if n != int64(len(tail)) {
		t.Errorf("bytes written: got %d, want %d (tail only)", n, len(tail))
	}
	if !bytes.Equal(dst.Bytes(), tail) {
		t.Errorf("body mismatch: got %q, want %q", dst.Bytes(), tail)
	}
}

// TestFetchClientResume_BadOffsetAck verifies that AckCodeBadOffset maps to
// ErrBadOffset — not ErrFileNotOnPeer (which would trigger peer fallback).
func TestFetchClientResume_BadOffsetAck(t *testing.T) {
	peer := startFakePeer(t)
	defer peer.stop()
	peer.scriptErrorCode(protocol.AckCodeBadOffset, "offset 999 >= file size 42")

	fc := newFetchClient(t, "reader-1", "shared-secret-xyz")
	entry := &raft.FileEntry{
		Path:      "db/cpu/bad-offset.parquet",
		SizeBytes: 42,
		SHA256:    sha256Hex([]byte("dummy")),
	}
	prefixHasher := sha256.New()
	prefixHasher.Write(make([]byte, 10))

	var dst bytes.Buffer
	_, err := fc.Fetch(context.Background(), peer.addr(), entry, &dst, 10, prefixHasher)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, ErrBadOffset) {
		t.Errorf("expected ErrBadOffset, got %v", err)
	}
	if errors.Is(err, ErrFileNotOnPeer) {
		t.Errorf("ErrBadOffset must NOT map to ErrFileNotOnPeer")
	}
}

// TestFetchClientResume_OffsetEchoMismatch verifies that if the peer echoes a
// different ByteOffset than requested, Fetch returns a protocol error.
func TestFetchClientResume_OffsetEchoMismatch(t *testing.T) {
	fullBody := []byte("hello world full body")
	fullSum := sha256Hex(fullBody)

	peer := startFakePeer(t)
	defer peer.stop()
	// Script the ack with a different ByteOffset than the client will request.
	peer.mu.Lock()
	peer.ack = protocol.FetchFileAckHeader{
		Status:     "ok",
		SizeBytes:  int64(len(fullBody)) - 5,
		SHA256:     fullSum,
		ByteOffset: 999, // wrong echo
	}
	peer.body = fullBody[5:]
	peer.mu.Unlock()

	fc := newFetchClient(t, "reader-1", "shared-secret-xyz")
	entry := &raft.FileEntry{
		Path:      "db/cpu/echo-mismatch.parquet",
		SizeBytes: int64(len(fullBody)),
		SHA256:    fullSum,
	}
	prefixHasher := sha256.New()
	prefixHasher.Write(fullBody[:5])

	var dst bytes.Buffer
	_, err := fc.Fetch(context.Background(), peer.addr(), entry, &dst, 5, prefixHasher)
	if err == nil {
		t.Fatal("expected error on offset echo mismatch, got nil")
	}
}
