package cluster

import (
	"net"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/protocol"
	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/cluster/security"
	"github.com/rs/zerolog"
)

// fetchAck drives one real MsgFetchFile request through the real
// handleFetchFile over a real TCP connection and returns the ack header.
func fetchAck(t *testing.T, origin *originServer, sharedSecret, clusterName, nodeID, path string) *protocol.FetchFileAckHeader {
	t.Helper()
	nonce, err := security.GenerateNonce()
	if err != nil {
		t.Fatalf("nonce: %v", err)
	}
	ts := time.Now().Unix()
	mac := security.ComputeFetchHMAC(sharedSecret, nonce, nodeID, clusterName, path, ts)

	conn, err := net.DialTimeout("tcp", origin.addr(), 2*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	if err := protocol.SendMessage(conn, &protocol.Message{
		Type: protocol.MsgFetchFile,
		Payload: &protocol.FetchFileRequest{
			Path:      path,
			NodeID:    nodeID,
			Nonce:     nonce,
			Timestamp: ts,
			HMAC:      mac,
		},
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
	return ack
}

// TestFetchReportsUnusableKeyAsInvalidPath covers the serve side of #747. The
// manifest entry passes sanitizeFetchPath and the FSM lookup, so the request
// reaches the backend Exists call, which fails permanently. That used to be
// answered as AckCodeBackend, which reads as a transient fault on the peer and
// invites the requester to come back.
//
// The key here is a backslash on purpose, and that is the whole point of the
// test: it is the only reachable spelling that survives sanitizeFetchPath, so
// it is the only one that can reach the Exists call at all. A test written
// with, say, "db//x.parquet" would be green before and after the fix, because
// the sanitizer rejects that one several steps earlier.
func TestFetchReportsUnusableKeyAsInvalidPath(t *testing.T) {
	const (
		sharedSecret = "auth"
		clusterName  = "test-cluster"
		originID     = "writer-1"
		pullerID     = "reader-1"
	)
	// Accepted by raft.ValidateManifestPath, refused by storage.ValidateKey.
	const unusable = `testdb\cpu/2026/04/11/14/file.parquet`

	fsm := raft.NewClusterFSM(zerolog.Nop())
	seedFileInFSM(t, fsm, raft.FileEntry{
		Path:         unusable,
		SizeBytes:    32,
		Database:     "testdb",
		Measurement:  "cpu",
		OriginNodeID: originID,
		SHA256:       "deadbeef",
		CreatedAt:    time.Now().UTC(),
	})

	origin := startOriginServer(t, newMemBackend(), fsm, sharedSecret, clusterName, originID)
	defer origin.stop()

	ack := fetchAck(t, origin, sharedSecret, clusterName, pullerID, unusable)
	if ack.Status != "error" {
		t.Fatalf("ack status = %q, want error", ack.Status)
	}
	if ack.Code != protocol.AckCodeInvalidPath {
		t.Fatalf("ack code = %q, want %q: a permanently unusable key must not be reported as a retryable backend fault", ack.Code, protocol.AckCodeInvalidPath)
	}
}

// TestFetchSanitizerBoundary documents which spellings never reach the backend
// at all. It is cheap, and it is what stops someone later "simplifying" the
// backslash key in the test above into one of these, which would silently turn
// that test into a test of sanitizeFetchPath.
func TestFetchSanitizerBoundary(t *testing.T) {
	const (
		sharedSecret = "auth"
		clusterName  = "test-cluster"
		originID     = "writer-1"
		pullerID     = "reader-1"
	)
	fsm := raft.NewClusterFSM(zerolog.Nop())
	origin := startOriginServer(t, newMemBackend(), fsm, sharedSecret, clusterName, originID)
	defer origin.stop()

	// Rejected by sanitizeFetchPath before the manifest lookup or any backend
	// call, so they answer invalid_path without the new branch being involved.
	for _, path := range []string{
		"testdb/cpu/2026/04/11/14/",
		"testdb/./cpu/2026/04/11/14/f.parquet",
		"testdb//cpu/2026/04/11/14/f.parquet",
		"/testdb/cpu/2026/04/11/14/f.parquet",
	} {
		ack := fetchAck(t, origin, sharedSecret, clusterName, pullerID, path)
		if ack.Code != protocol.AckCodeInvalidPath {
			t.Errorf("path %q: ack code = %q, want %q", path, ack.Code, protocol.AckCodeInvalidPath)
		}
	}
}
