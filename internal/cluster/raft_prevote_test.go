package cluster

// #1272: a writer that can reach the leader but not hear from it (the leader
// cannot reach its new address, as with a restarted pod whose DNS name still
// resolves to the old IP) used to raise its term on every election timeout;
// once the leader reached it again, the higher term made the healthy leader
// step down. With pre-vote the term stays put and the leader keeps its role.

import (
	"strconv"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/rs/zerolog"
)

func startPreVoteNode(t *testing.T, id, addr, dir string, bootstrap, preVoteOff bool) *raft.Node {
	t.Helper()
	n, err := raft.NewNode(&raft.NodeConfig{
		NodeID:             id,
		SharedSecret:       "test-cluster-secret-32-bytes-long!",
		DataDir:            dir,
		BindAddr:           addr,
		AdvertiseAddr:      addr,
		Bootstrap:          bootstrap,
		ElectionTimeout:    1 * time.Second,
		HeartbeatTimeout:   500 * time.Millisecond,
		LeaderLeaseTimeout: 500 * time.Millisecond,
		PreVoteDisabled:    preVoteOff,
		Logger:             zerolog.Nop(),
	}, raft.NewClusterFSM(zerolog.Nop()))
	if err != nil {
		t.Fatalf("NewNode %s: %v", id, err)
	}
	if err := n.Start(); err != nil {
		t.Fatalf("Start %s: %v", id, err)
	}
	return n
}

func raftTerm(t *testing.T, n *raft.Node) uint64 {
	t.Helper()
	v, err := strconv.ParseUint(n.Stats()["term"], 10, 64)
	if err != nil {
		t.Fatalf("term: %v (%v)", err, n.Stats())
	}
	return v
}

// cutOffFollower runs the incident on a 3-voter cluster and reports whether
// the original leader kept leadership and the leader's term after it.
func cutOffFollower(t *testing.T, preVoteOff bool) (kept bool, termBefore, termAfter uint64) {
	t.Helper()
	addrs := []string{allocFreePort(t), allocFreePort(t), allocFreePort(t)}
	dirs := []string{t.TempDir(), t.TempDir(), t.TempDir()}
	a := startPreVoteNode(t, "w-a", addrs[0], dirs[0], true, preVoteOff)
	t.Cleanup(func() { _ = a.Stop() })
	if err := a.WaitForLeader(10 * time.Second); err != nil {
		t.Fatal(err)
	}
	b := startPreVoteNode(t, "w-b", addrs[1], dirs[1], false, preVoteOff)
	t.Cleanup(func() { _ = b.Stop() })
	c := startPreVoteNode(t, "w-c", addrs[2], dirs[2], false, preVoteOff)
	for _, p := range []struct{ id, addr string }{{"w-b", addrs[1]}, {"w-c", addrs[2]}} {
		if err := a.AddVoter(p.id, p.addr, 10*time.Second); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(2 * time.Second) // replication settles
	termBefore = raftTerm(t, a)

	// Restart C on another port: it reaches A and B, A's heartbeats go to the
	// old address and fail. C keeps timing out.
	_ = c.Stop()
	c = startPreVoteNode(t, "w-c", allocFreePort(t), dirs[2], false, preVoteOff)
	time.Sleep(6 * time.Second)
	if term := raftTerm(t, c); preVoteOff && term <= termBefore {
		t.Fatalf("setup: with pre-vote off the cut-off follower should campaign (term %d)", term)
	} else if !preVoteOff && term != termBefore {
		t.Fatalf("with pre-vote the cut-off follower raised its term %d -> %d", termBefore, term)
	}
	_ = c.Stop()
	// Back on its configured address: wait until the leader reaches it again
	// (after failed heartbeats the leader backs off for a while) and it
	// knows a leader again (itself included: with pre-vote off it can win).
	c = startPreVoteNode(t, "w-c", addrs[2], dirs[2], false, preVoteOff)
	t.Cleanup(func() { _ = c.Stop() })
	deadline := time.Now().Add(90 * time.Second)
	for c.LeaderID() == "" {
		if time.Now().After(deadline) {
			t.Fatalf("C never followed a leader again: %v", c.Stats())
		}
		time.Sleep(200 * time.Millisecond)
	}

	return a.IsLeader(), termBefore, raftTerm(t, a)
}

func TestPreVoteKeepsTheLeaderWhenAFollowerIsCutOff(t *testing.T) {
	if testing.Short() {
		t.Skip("3-node Raft cluster")
	}
	kept, before, after := cutOffFollower(t, false)
	if !kept || after != before {
		t.Fatalf("with pre-vote: leader kept %v, term %d -> %d; want kept and unchanged", kept, before, after)
	}
}

// The switch works: with pre-vote off the incident reproduces.
func TestPreVoteOffReproducesTheLeaderLoss(t *testing.T) {
	if testing.Short() {
		t.Skip("3-node Raft cluster")
	}
	kept, before, after := cutOffFollower(t, true)
	if kept && after == before {
		t.Fatalf("with pre-vote off the term did not move (%d): the cut-off follower did not disrupt; the test does not reproduce #1272", before)
	}
}
