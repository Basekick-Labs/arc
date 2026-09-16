package cluster

import (
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/cluster/security"
	hraft "github.com/hashicorp/raft"
)

// #862: suffrage follows the role. A reader or compactor that could win Raft
// leadership stalls every singleton task in shared-storage mode, because
// IsPrimaryWriter is "Raft leader AND RoleWriter" and no node then satisfies
// both halves.
//
// These drive the real handleJoinRequest against a real single-node Raft
// leader, because the dispatch lives in the `c.raftNode != nil` branch that a
// nil-Raft harness never reaches.

// joinSuffrageRig returns a leader coordinator and a real Raft transport for
// the joining node, so AddVoter has something to replicate to and its future
// can actually commit.
func joinSuffrageRig(t *testing.T) (*Coordinator, string) {
	t.Helper()

	leader := startRaftNode(t, "leader-node", allocFreePort(t), true)
	t.Cleanup(func() { _ = leader.Stop() })
	if err := leader.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("test Raft leader never elected: %v", err)
	}

	joinerAddr := allocFreePort(t)
	joiner := startRaftNode(t, "joining-node", joinerAddr, false)
	t.Cleanup(func() { _ = joiner.Stop() })

	c := newJoinTestCoordinator(t, joinTestSecret)
	c.raftNode = leader
	c.raftFSM = leader.FSM()
	return c, joinerAddr
}

func suffrageOf(t *testing.T, n *raft.Node, id string) (hraft.ServerSuffrage, bool) {
	t.Helper()
	cfg, err := n.GetConfiguration()
	if err != nil {
		t.Fatalf("GetConfiguration: %v", err)
	}
	for _, s := range cfg.Servers {
		if string(s.ID) == id {
			return s.Suffrage, true
		}
	}
	return 0, false
}

func joinWithRole(t *testing.T, c *Coordinator, raftAddr string, role NodeRole) {
	t.Helper()
	req := signedJoinRequest(joinTestSecret)
	req.NodeID = "joining-node"
	req.NodeName = "joining-node"
	req.Role = string(role)
	req.RaftAddr = raftAddr
	req.AuthHMAC = security.ComputeJoinHMAC(joinTestSecret, req.AuthNonce, req.AuthTimestamp, joinAuthFields(req))

	resp := deliverJoin(t, c, req)
	if resp == nil || !resp.Success {
		t.Fatalf("join as %q was rejected: %+v", role, resp)
	}
}

func TestJoinSuffrage_ReaderJoinsAsANonvoter(t *testing.T) {
	c, joinerAddr := joinSuffrageRig(t)
	joinWithRole(t, c, joinerAddr, RoleReader)

	suffrage, ok := suffrageOf(t, c.raftNode, "joining-node")
	if !ok {
		t.Fatal("the reader is not in the Raft configuration at all")
	}
	if suffrage != hraft.Nonvoter {
		t.Errorf("reader joined with suffrage %v, want Nonvoter: a reader that can win leadership stalls every singleton task", suffrage)
	}
}

func TestJoinSuffrage_CompactorJoinsAsANonvoter(t *testing.T) {
	c, joinerAddr := joinSuffrageRig(t)
	joinWithRole(t, c, joinerAddr, RoleCompactor)

	suffrage, ok := suffrageOf(t, c.raftNode, "joining-node")
	if !ok {
		t.Fatal("the compactor is not in the Raft configuration at all")
	}
	if suffrage != hraft.Nonvoter {
		t.Errorf("compactor joined with suffrage %v, want Nonvoter", suffrage)
	}
}

// The other direction matters just as much: a change that made everything a
// non-voter would leave a cluster unable to elect, and the reader tests above
// would not notice.
func TestJoinSuffrage_WriterJoinsAsAVoter(t *testing.T) {
	c, joinerAddr := joinSuffrageRig(t)
	joinWithRole(t, c, joinerAddr, RoleWriter)

	suffrage, ok := suffrageOf(t, c.raftNode, "joining-node")
	if !ok {
		t.Fatal("the writer is not in the Raft configuration at all")
	}
	if suffrage != hraft.Voter {
		t.Errorf("writer joined with suffrage %v, want Voter", suffrage)
	}
}

// Standalone is the default cluster.role and it ingests, so it votes.
func TestJoinSuffrage_StandaloneJoinsAsAVoter(t *testing.T) {
	c, joinerAddr := joinSuffrageRig(t)
	joinWithRole(t, c, joinerAddr, RoleStandalone)

	suffrage, ok := suffrageOf(t, c.raftNode, "joining-node")
	if !ok {
		t.Fatal("the standalone node is not in the Raft configuration at all")
	}
	if suffrage != hraft.Voter {
		t.Errorf("standalone joined with suffrage %v, want Voter", suffrage)
	}
}
