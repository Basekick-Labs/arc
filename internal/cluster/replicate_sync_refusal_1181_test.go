package cluster

import (
	"io"
	"net"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/protocol"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/rs/zerolog"
)

// A reader of a release before 27.01.1 still dials writers for the WAL
// replication stream during an upgrade (#888). The stream is gone (#1181):
// the writer answers with a refusal and closes the connection. Before #1181
// the refusal left the connection open (handlePeerConnection had handed it to
// a Sender that no longer exists), one socket per retry, every few seconds.
func TestLegacyReaderIsRefusedAndTheConnectionClosed(t *testing.T) {
	c := &Coordinator{
		cfg:       &config.ClusterConfig{ReplicationEnabled: true, SharedSecret: "s", ClusterName: "test-cluster"},
		localNode: NewNode("w", "w", RoleWriter, "test-cluster"),
		logger:    zerolog.Nop(),
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go c.handlePeerConnection(conn)
		}
	}()

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	req := &protocol.ReplicateSync{ReaderID: "reader-1", Nonce: "n", ClusterName: "test-cluster", Timestamp: time.Now().Unix(), HMAC: "00"}
	if err := protocol.SendMessage(conn, &protocol.Message{Type: protocol.MsgReplicateSync, Payload: req}, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	msg, err := protocol.ReceiveMessage(conn, 5*time.Second)
	if err != nil {
		t.Fatalf("no refusal: %v", err)
	}
	ack, ok := msg.Payload.(*protocol.ReplicateSyncAck)
	if !ok || msg.Type != protocol.MsgReplicateSyncAck || ack.CanResume || ack.Error == "" {
		t.Fatalf("answer %s %+v, want a refusing ack", msg.Type, msg.Payload)
	}
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := conn.Read(make([]byte, 1)); err != io.EOF {
		t.Fatalf("the writer kept the connection open after refusing it: %v", err)
	}
}
