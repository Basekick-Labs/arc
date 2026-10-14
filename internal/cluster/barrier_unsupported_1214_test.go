package cluster

import (
	"context"
	"errors"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/protocol"
	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/rs/zerolog"
)

// #1214: a leader whose build predates the manifest barrier refuses the
// forwarded command as one it does not allow. The follower's barrier says so
// with raft.ErrBarrierUnsupported, which the reconciler reports as its own
// abort reason, apart from a leader it could not reach.
func TestFollowerBarrier_OlderLeaderIsUnsupported(t *testing.T) {
	addrs := allocFreePorts(t, 2)
	raftA := startRaftNodeInDir(t, "writer-A", addrs[0], filepath.Join(t.TempDir(), "writer-A"), true)
	t.Cleanup(func() { _ = raftA.Stop() })
	if err := raftA.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("WaitForLeader: %v", err)
	}
	raftB := startRaftNodeInDir(t, "reader-B", addrs[1], filepath.Join(t.TempDir(), "reader-B"), false)
	t.Cleanup(func() { _ = raftB.Stop() })
	if err := raftA.AddVoter("reader-B", addrs[1], 10*time.Second); err != nil {
		t.Fatalf("AddVoter: %v", err)
	}
	if err := raftB.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("reader-B WaitForLeader: %v", err)
	}

	// The older leader: authenticated, and refusing the command type.
	old := &Coordinator{
		cfg:       &config.ClusterConfig{ClusterName: barrierTestCluster, SharedSecret: barrierTestSecret},
		localNode: NewNode("writer-A", "writer-A", RoleWriter, barrierTestCluster),
		logger:    zerolog.Nop(),
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			go func(conn net.Conn) {
				defer conn.Close()
				msg, err := protocol.ReceiveMessage(conn, 5*time.Second)
				if err != nil {
					return
				}
				req := msg.Payload.(*protocol.ForwardApplyRequest)
				old.sendForwardApplyError(conn, req.Nonce, protocol.ForwardCodeInvalidCommand, "command type not allowed via forwarding")
			}(conn)
		}
	}()

	follower := newFollowerCoordinator(t, "reader-B", raftB)
	leaderNode := NewNode("writer-A", "writer-A", RoleWriter, barrierTestCluster)
	leaderNode.Address = l.Addr().String()
	if err := follower.registry.Register(leaderNode); err != nil {
		t.Fatal(err)
	}
	err = follower.waitForManifestSync(context.Background(), raftB, 5*time.Second)
	if !errors.Is(err, raft.ErrBarrierUnsupported) {
		t.Fatalf("an older leader's refusal is %v, want ErrBarrierUnsupported", err)
	}
}
