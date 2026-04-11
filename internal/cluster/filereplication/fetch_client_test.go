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

	if err := protocol.SendMessage(conn, &protocol.Message{
		Type:    protocol.MsgFetchFileAck,
		Payload: &ack,
	}, 2*time.Second); err != nil {
		return
	}
	if ack.Status != "ok" {
		return
	}
	if len(body) > 0 {
		_, _ = conn.Write(body)
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
	n, err := fc.Fetch(context.Background(), peer.addr(), entry, &dst)
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
	_, err := fc.Fetch(context.Background(), peer.addr(), entry, &dst)
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
	_, err := fc.Fetch(context.Background(), peer.addr(), entry, &dst)
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
	_, err := fc.Fetch(context.Background(), peer.addr(), entry, &dst)
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
	_, err := fc.Fetch(context.Background(), peer.addr(), entry, &dst)
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
	_, err := fc.Fetch(context.Background(), "127.0.0.1:1", entry, &dst)
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
	if _, err := fc.Fetch(context.Background(), peer.addr(), entry, &dst); err != nil {
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
	_, err := fc.Fetch(context.Background(), peer.addr(), entry, &dst)
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
	n, err := fc.Fetch(context.Background(), peer.addr(), entry, io.Discard)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if n != int64(size) {
		t.Errorf("bytes: got %d, want %d", n, size)
	}
}
