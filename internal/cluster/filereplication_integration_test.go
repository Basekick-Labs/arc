package cluster

// Integration test for peer file replication (Enterprise Phase 2).
//
// This test constructs two coordinator-like halves in-process:
//   - An "origin" half that serves MsgFetchFile requests using the real
//     handleFetchFile handler, over a real TCP listener, backed by a real
//     raft.ClusterFSM that holds one FileEntry.
//   - A "puller" half that uses the real filereplication.FetchClient to
//     pull the file into a fake backend.
//
// It does NOT instantiate a full NewCoordinator (which requires a valid
// Enterprise license) or a running Raft cluster — the licensing and Raft
// paths are exercised at the unit level and in the manual docker test.
// What this test catches is the end-to-end protocol round-trip: real
// handler ↔ real fetch client, including HMAC validation, manifest
// lookup, body streaming, and checksum verification.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/filereplication"
	"github.com/basekick-labs/arc/internal/cluster/protocol"
	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/cluster/security"
	"github.com/basekick-labs/arc/internal/config"
	hraft "github.com/hashicorp/raft"
	"github.com/rs/zerolog"
)

// seedFileInFSM is a test helper that marshals a RegisterFile command and
// drives fsm.Apply directly, mimicking a Raft commit without actually
// running Raft. Index is fixed at 1 — tests that need multiple entries
// should set distinct indices.
func seedFileInFSM(t *testing.T, fsm *raft.ClusterFSM, file raft.FileEntry) {
	t.Helper()
	payload, err := json.Marshal(raft.RegisterFilePayload{File: file})
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	cmd := raft.Command{Type: raft.CommandRegisterFile, Payload: payload}
	data, err := json.Marshal(cmd)
	if err != nil {
		t.Fatalf("marshal command: %v", err)
	}
	result := fsm.Apply(&hraft.Log{Index: 1, Data: data})
	if err, ok := result.(error); ok && err != nil {
		t.Fatalf("fsm.Apply: %v", err)
	}
}

// --- Minimal in-memory backend (shared with filereplication unit tests,
//     duplicated here because the packages are different) ---

type memBackend struct {
	mu    sync.Mutex
	files map[string][]byte
}

func newMemBackend() *memBackend {
	return &memBackend{files: make(map[string][]byte)}
}

func (m *memBackend) Write(ctx context.Context, path string, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]byte, len(data))
	copy(cp, data)
	m.files[path] = cp
	return nil
}

func (m *memBackend) WriteReader(ctx context.Context, path string, reader io.Reader, size int64) error {
	data, err := io.ReadAll(reader)
	if err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.files[path] = data
	return nil
}

func (m *memBackend) Read(ctx context.Context, path string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	data, ok := m.files[path]
	if !ok {
		return nil, io.EOF
	}
	return data, nil
}

func (m *memBackend) ReadTo(ctx context.Context, path string, writer io.Writer) error {
	data, err := m.Read(ctx, path)
	if err != nil {
		return err
	}
	_, err = writer.Write(data)
	return err
}

func (m *memBackend) List(ctx context.Context, prefix string) ([]string, error) {
	return nil, nil
}
func (m *memBackend) Delete(ctx context.Context, path string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.files, path)
	return nil
}
func (m *memBackend) Exists(ctx context.Context, path string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.files[path]
	return ok, nil
}
func (m *memBackend) Close() error       { return nil }
func (m *memBackend) Type() string       { return "mem" }
func (m *memBackend) ConfigJSON() string { return "{}" }
func (m *memBackend) ReadToAt(_ context.Context, path string, w io.Writer, offset int64) error {
	m.mu.Lock()
	data, ok := m.files[path]
	m.mu.Unlock()
	if !ok {
		return fmt.Errorf("file not found: %s", path)
	}
	if offset > int64(len(data)) {
		return fmt.Errorf("offset %d > file size %d", offset, len(data))
	}
	_, err := w.Write(data[offset:])
	return err
}
func (m *memBackend) StatFile(_ context.Context, path string) (int64, error) {
	m.mu.Lock()
	data, ok := m.files[path]
	m.mu.Unlock()
	if !ok {
		return -1, nil
	}
	return int64(len(data)), nil
}
func (m *memBackend) AppendReader(_ context.Context, path string, r io.Reader, _ int64) error {
	tail, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	m.mu.Lock()
	m.files[path] = append(m.files[path], tail...)
	m.mu.Unlock()
	return nil
}

// --- Minimal origin-side server ---

// originServer wraps a real Coordinator struct with just enough fields set
// to satisfy handleFetchFile. It accepts one connection at a time on a real
// TCP listener and dispatches MsgFetchFile to the real handler.
type originServer struct {
	coord    *Coordinator
	listener net.Listener
	done     chan struct{}
	wg       sync.WaitGroup
}

func startOriginServer(t *testing.T, backend *memBackend, fsm *raft.ClusterFSM, sharedSecret, clusterName, nodeID string) *originServer {
	t.Helper()

	// Bind a raft.Node wrapper over the FSM without calling Start(). The fetch
	// handler only calls c.raftNode.FSM(), so a stopped Node with an attached
	// FSM is sufficient.
	raftNode, err := raft.NewNode(&raft.NodeConfig{
		NodeID:   nodeID,
		DataDir:  t.TempDir(), // unused because we never Start
		BindAddr: "127.0.0.1:0",
		Logger:   zerolog.Nop(),
	}, fsm)
	if err != nil {
		t.Fatalf("raft.NewNode: %v", err)
	}

	// Instantiate a bare Coordinator with the minimum fields handleFetchFile
	// touches. This is NOT a real cluster node — no health checker, no
	// registry, no license client, no listener lifecycle — just enough
	// machinery to route MsgFetchFile into the real handler.
	coord := &Coordinator{
		cfg: &config.ClusterConfig{
			SharedSecret: sharedSecret,
			ClusterName:  clusterName,
		},
		storage:   backend,
		raftNode:  raftNode,
		localNode: NewNode(nodeID, nodeID, RoleWriter, clusterName),
		logger:    zerolog.Nop(),
		ctx:       context.Background(),
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &originServer{
		coord:    coord,
		listener: listener,
		done:     make(chan struct{}),
	}
	s.wg.Add(1)
	go s.acceptLoop()
	return s
}

func (s *originServer) addr() string { return s.listener.Addr().String() }

func (s *originServer) stop() {
	close(s.done)
	s.listener.Close()
	s.wg.Wait()
}

func (s *originServer) acceptLoop() {
	defer s.wg.Done()
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return
		}
		go s.handleConn(conn)
	}
}

// handleConn decodes one MsgFetchFile and hands off to the real
// Coordinator.handleFetchFile method. The rest of the handlePeerConnection
// dispatch switch is not reproduced here — we only need fetch routing.
func (s *originServer) handleConn(conn net.Conn) {
	msg, err := protocol.ReceiveMessage(conn, 5*time.Second)
	if err != nil {
		conn.Close()
		return
	}
	if msg.Type != protocol.MsgFetchFile {
		conn.Close()
		return
	}
	req := msg.Payload.(*protocol.FetchFileRequest)
	// handleFetchFile takes ownership of the connection (closes it via defer).
	s.coord.handleFetchFile(conn, req)
}

// --- Test ----------------------------------------------------------------

// TestPhase2FetchRoundtrip wires the real handleFetchFile handler to the real
// filereplication.FetchClient and verifies that a file registered on the
// origin's FSM can be downloaded by the client, written into the client's
// backend, and verified via checksum.
func TestPhase2FetchRoundtrip(t *testing.T) {
	const (
		sharedSecret = "integration-secret"
		clusterName  = "test-cluster"
		originID     = "writer-1"
		pullerID     = "reader-1"
	)

	// Origin side: a real backend holding a real file + a FSM that knows about it.
	originBackend := newMemBackend()
	body := []byte("integration test parquet body — some bytes to replicate")
	hash := sha256.Sum256(body)
	hashHex := hex.EncodeToString(hash[:])
	path := "testdb/cpu/2026/04/11/14/integration.parquet"
	if err := originBackend.Write(context.Background(), path, body); err != nil {
		t.Fatalf("seed origin backend: %v", err)
	}

	originFSM := raft.NewClusterFSM(zerolog.Nop())
	// Seed the FSM with the file entry. We use applyRegisterFile directly
	// via a marshalled Command so the LSN and validation paths are exercised
	// the same way a real Raft commit would exercise them.
	seedFileInFSM(t, originFSM, raft.FileEntry{
		Path:          path,
		SHA256:        hashHex,
		SizeBytes:     int64(len(body)),
		Database:      "testdb",
		Measurement:   "cpu",
		PartitionTime: time.Date(2026, 4, 11, 14, 0, 0, 0, time.UTC),
		OriginNodeID:  originID,
		Tier:          "hot",
		CreatedAt:     time.Date(2026, 4, 11, 15, 0, 0, 0, time.UTC),
	})

	// Start the origin server.
	origin := startOriginServer(t, originBackend, originFSM, sharedSecret, clusterName, originID)
	defer origin.stop()

	// Puller side: a fresh backend and a real FetchClient pointing at origin.
	pullerBackend := newMemBackend()
	fetchClient, err := filereplication.NewFetchClient(filereplication.FetchClient{
		SelfNodeID:   pullerID,
		ClusterName:  clusterName,
		SharedSecret: sharedSecret,
		DialTimeout:  2 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewFetchClient: %v", err)
	}

	// Drive the puller directly — we don't need the full Puller worker pool
	// for this integration test, just one Fetch call into the client backend.
	entry := &raft.FileEntry{
		Path:      path,
		SHA256:    hashHex,
		SizeBytes: int64(len(body)),
	}
	// We use a pipe so the FetchClient writes flow through sha256 verification
	// and into the backend via WriteReader — matching how the Puller wires it.
	pr, pw := io.Pipe()
	writeDone := make(chan error, 1)
	go func() {
		writeDone <- pullerBackend.WriteReader(context.Background(), path, pr, entry.SizeBytes)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	n, fetchErr := fetchClient.Fetch(ctx, origin.addr(), entry, pw, 0, nil)
	if fetchErr != nil {
		_ = pw.CloseWithError(fetchErr)
		t.Fatalf("Fetch: %v", fetchErr)
	}
	if err := pw.Close(); err != nil {
		t.Fatalf("pipe close: %v", err)
	}
	if err := <-writeDone; err != nil {
		t.Fatalf("backend WriteReader: %v", err)
	}
	if n != int64(len(body)) {
		t.Errorf("bytes written: got %d, want %d", n, len(body))
	}

	// Verify the puller's backend now holds the file with the same bytes.
	got, err := pullerBackend.Read(context.Background(), path)
	if err != nil {
		t.Fatalf("puller Read: %v", err)
	}
	if string(got) != string(body) {
		t.Errorf("body mismatch: got %d bytes, want %d bytes", len(got), len(body))
	}
}

// TestPhase2FetchRejectsBadHMAC verifies the origin's handleFetchFile rejects
// requests signed with the wrong shared secret. The client side uses the
// correct protocol but mis-signs the HMAC, so the origin replies with an
// error ack and the fetch fails cleanly.
func TestPhase2FetchRejectsBadHMAC(t *testing.T) {
	const (
		originSecret = "right-secret"
		wrongSecret  = "wrong-secret"
		clusterName  = "test-cluster"
		originID     = "writer-1"
		pullerID     = "reader-1"
	)

	originBackend := newMemBackend()
	body := []byte("tiny body")
	hash := sha256.Sum256(body)
	hashHex := hex.EncodeToString(hash[:])
	path := "testdb/cpu/hmac.parquet"
	_ = originBackend.Write(context.Background(), path, body)

	fsm := raft.NewClusterFSM(zerolog.Nop())
	seedFileInFSM(t, fsm, raft.FileEntry{
		Path:          path,
		SHA256:        hashHex,
		SizeBytes:     int64(len(body)),
		Database:      "testdb",
		Measurement:   "cpu",
		PartitionTime: time.Date(2026, 4, 11, 14, 0, 0, 0, time.UTC),
		OriginNodeID:  originID,
		Tier:          "hot",
		CreatedAt:     time.Date(2026, 4, 11, 15, 0, 0, 0, time.UTC),
	})

	origin := startOriginServer(t, originBackend, fsm, originSecret, clusterName, originID)
	defer origin.stop()

	// Client uses the wrong secret.
	fetchClient, err := filereplication.NewFetchClient(filereplication.FetchClient{
		SelfNodeID:   pullerID,
		ClusterName:  clusterName,
		SharedSecret: wrongSecret,
		DialTimeout:  2 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewFetchClient: %v", err)
	}

	entry := &raft.FileEntry{Path: path, SHA256: hashHex, SizeBytes: int64(len(body))}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, fetchErr := fetchClient.Fetch(ctx, origin.addr(), entry, io.Discard, 0, nil)
	if fetchErr == nil {
		t.Fatalf("expected auth error, got nil")
	}
}

// TestPhase2FetchRejectsHMACPathReplay verifies that a MAC signed for file A
// cannot be replayed to fetch file B, even with the correct shared secret and
// a fresh timestamp. Regression test for the path-binding fix.
func TestPhase2FetchRejectsHMACPathReplay(t *testing.T) {
	const (
		sharedSecret = "replay-secret"
		clusterName  = "test-cluster"
		originID     = "writer-1"
		attackerID   = "attacker-1"
	)

	// Seed two distinct files in both backend and FSM so both would otherwise
	// be servable if auth passed.
	originBackend := newMemBackend()
	bodyA := []byte("file A contents")
	bodyB := []byte("file B contents — secret")
	pathA := "testdb/cpu/a.parquet"
	pathB := "testdb/cpu/b.parquet"
	sumA := sha256.Sum256(bodyA)
	sumB := sha256.Sum256(bodyB)
	sumAHex := hex.EncodeToString(sumA[:])
	sumBHex := hex.EncodeToString(sumB[:])
	_ = originBackend.Write(context.Background(), pathA, bodyA)
	_ = originBackend.Write(context.Background(), pathB, bodyB)

	fsm := raft.NewClusterFSM(zerolog.Nop())
	seedFileInFSM(t, fsm, raft.FileEntry{
		Path: pathA, SHA256: sumAHex, SizeBytes: int64(len(bodyA)),
		Database: "testdb", Measurement: "cpu",
		PartitionTime: time.Date(2026, 4, 11, 14, 0, 0, 0, time.UTC),
		OriginNodeID:  originID, Tier: "hot",
		CreatedAt: time.Date(2026, 4, 11, 15, 0, 0, 0, time.UTC),
	})
	// Seed file B with a different index so the FSM accepts both.
	payloadB, _ := json.Marshal(raft.RegisterFilePayload{File: raft.FileEntry{
		Path: pathB, SHA256: sumBHex, SizeBytes: int64(len(bodyB)),
		Database: "testdb", Measurement: "cpu",
		PartitionTime: time.Date(2026, 4, 11, 14, 0, 0, 0, time.UTC),
		OriginNodeID:  originID, Tier: "hot",
		CreatedAt: time.Date(2026, 4, 11, 15, 0, 0, 0, time.UTC),
	}})
	cmdB, _ := json.Marshal(raft.Command{Type: raft.CommandRegisterFile, Payload: payloadB})
	if result := fsm.Apply(&hraft.Log{Index: 2, Data: cmdB}); result != nil {
		if err, ok := result.(error); ok && err != nil {
			t.Fatalf("seed file B: %v", err)
		}
	}

	origin := startOriginServer(t, originBackend, fsm, sharedSecret, clusterName, originID)
	defer origin.stop()

	// Sign a fresh MAC for pathA, then send a request that claims pathB. A
	// correct server must reject this because the signed payload covers pathA
	// only.
	nonce, err := security.GenerateNonce()
	if err != nil {
		t.Fatalf("nonce: %v", err)
	}
	ts := time.Now().Unix()
	macForA := security.ComputeFetchHMAC(sharedSecret, nonce, attackerID, clusterName, pathA, ts)

	conn, err := net.DialTimeout("tcp", origin.addr(), 2*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	req := &protocol.FetchFileRequest{
		Path:      pathB, // Asking for B...
		NodeID:    attackerID,
		Nonce:     nonce,
		Timestamp: ts,
		HMAC:      macForA, // ...with a MAC signed for A.
	}
	if err := protocol.SendMessage(conn, &protocol.Message{
		Type:    protocol.MsgFetchFile,
		Payload: req,
	}, 2*time.Second); err != nil {
		t.Fatalf("send: %v", err)
	}

	ackMsg, err := protocol.ReceiveMessage(conn, 2*time.Second)
	if err != nil {
		t.Fatalf("receive ack: %v", err)
	}
	ack, ok := ackMsg.Payload.(*protocol.FetchFileAckHeader)
	if !ok {
		t.Fatalf("ack payload wrong type: %T", ackMsg.Payload)
	}
	if ack.Status == "ok" {
		t.Fatalf("replay attack succeeded — origin served %s with a MAC signed for %s", pathB, pathA)
	}
}

// TestPhase2FetchRejectsUnknownPath verifies that the origin refuses to serve
// a file that isn't in its manifest — even if the file were to exist on disk.
func TestPhase2FetchRejectsUnknownPath(t *testing.T) {
	const (
		sharedSecret = "auth"
		clusterName  = "test-cluster"
		originID     = "writer-1"
		pullerID     = "reader-1"
	)

	originBackend := newMemBackend()
	// Write a file directly to the backend without registering it in the FSM.
	body := []byte("off-manifest bytes")
	hash := sha256.Sum256(body)
	hashHex := hex.EncodeToString(hash[:])
	path := "testdb/cpu/offmanifest.parquet"
	_ = originBackend.Write(context.Background(), path, body)

	// Empty FSM — no file registrations.
	fsm := raft.NewClusterFSM(zerolog.Nop())

	origin := startOriginServer(t, originBackend, fsm, sharedSecret, clusterName, originID)
	defer origin.stop()

	fetchClient, err := filereplication.NewFetchClient(filereplication.FetchClient{
		SelfNodeID:   pullerID,
		ClusterName:  clusterName,
		SharedSecret: sharedSecret,
		DialTimeout:  2 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewFetchClient: %v", err)
	}

	entry := &raft.FileEntry{Path: path, SHA256: hashHex, SizeBytes: int64(len(body))}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, fetchErr := fetchClient.Fetch(ctx, origin.addr(), entry, io.Discard, 0, nil)
	if fetchErr == nil {
		t.Fatalf("expected manifest lookup error, got nil")
	}
}
