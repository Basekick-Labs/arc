package cluster

// Follower-side barrier before the startup catch-up walk (#799), through
// real Raft (three voters) and the real leader-forwarding path (a leader
// coordinator serving MsgForwardApply over TCP with HMAC auth).
//
// The scenario mirrors a reader restart: B is stopped, the leader commits a
// backlog, B comes back and must not walk the manifest until that backlog has
// been applied locally. hashicorp/raft's Barrier returns ErrNotLeader on a
// follower at once, and after an outage the leader's replication to B sits in
// exponential backoff, so without the forwarded barrier B walks an empty
// manifest.

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/cluster/security"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/rs/zerolog"
)

const (
	barrierTestSecret  = "barrier-test-secret-32-bytes-long!!"
	barrierTestCluster = "barrier-cluster"
)

// startRaftNodeInDir is startRaftNode with an explicit data dir so a node can
// be stopped and restarted on the same log.
func startRaftNodeInDir(t *testing.T, nodeID, bindAddr, dir string, bootstrap bool) *raft.Node {
	t.Helper()
	fsm := raft.NewClusterFSM(zerolog.Nop())
	n, err := raft.NewNode(&raft.NodeConfig{
		NodeID:             nodeID,
		SharedSecret:       "test-cluster-secret-32-bytes-long!",
		DataDir:            dir,
		BindAddr:           bindAddr,
		AdvertiseAddr:      bindAddr,
		Bootstrap:          bootstrap,
		ElectionTimeout:    1 * time.Second,
		HeartbeatTimeout:   500 * time.Millisecond,
		LeaderLeaseTimeout: 500 * time.Millisecond,
		Logger:             zerolog.Nop(),
	}, fsm)
	if err != nil {
		t.Fatalf("NewNode %s: %v", nodeID, err)
	}
	if err := n.Start(); err != nil {
		t.Fatalf("Start %s: %v", nodeID, err)
	}
	return n
}

// leaderCoordinatorServer is the minimum coordinator a leader needs to serve
// forwarded applies: a listener whose connections go through the real
// handlePeerConnection dispatch, a registry that knows the requesters, and a
// nonce cache for replay protection.
type leaderCoordinatorServer struct {
	coord    *Coordinator
	listener net.Listener
	wg       sync.WaitGroup
}

func startLeaderCoordinatorServer(t *testing.T, raftNode *raft.Node, peers ...*Node) *leaderCoordinatorServer {
	t.Helper()
	local := NewNode("writer-A", "writer-A", RoleWriter, barrierTestCluster)
	registry := NewRegistry(&RegistryConfig{LocalNode: local, MaxNodes: 8, Logger: zerolog.Nop()})
	for _, p := range peers {
		if err := registry.Register(p); err != nil {
			t.Fatalf("register %s: %v", p.ID, err)
		}
	}
	coord := &Coordinator{
		cfg:        &config.ClusterConfig{ClusterName: barrierTestCluster, SharedSecret: barrierTestSecret},
		localNode:  local,
		registry:   registry,
		raftNode:   raftNode,
		raftFSM:    raftNode.FSM(),
		nonceCache: security.NewNonceCache(security.HMACTimestampTolerance),
		logger:     zerolog.Nop(),
		ctx:        context.Background(),
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &leaderCoordinatorServer{coord: coord, listener: l}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			s.wg.Add(1)
			go func() {
				defer s.wg.Done()
				coord.handlePeerConnection(conn)
			}()
		}
	}()
	t.Cleanup(func() { _ = l.Close(); s.wg.Wait() })
	return s
}

func (s *leaderCoordinatorServer) addr() string { return s.listener.Addr().String() }

// newFollowerCoordinator builds the follower side. Its registry is EMPTY on
// purpose: a restarted reader has not joined again and no AddNode entry is
// replayed by a snapshot restore, so the leader's coordinator address must
// come from the FSM node table (validator finding B1).
func newFollowerCoordinator(t *testing.T, id string, raftNode *raft.Node) *Coordinator {
	t.Helper()
	local := NewNode(id, id, RoleReader, barrierTestCluster)
	c := &Coordinator{
		cfg:       &config.ClusterConfig{ClusterName: barrierTestCluster, SharedSecret: barrierTestSecret},
		localNode: local,
		registry:  NewRegistry(&RegistryConfig{LocalNode: local, MaxNodes: 8, Logger: zerolog.Nop()}),
		raftNode:  raftNode,
		raftFSM:   raftNode.FSM(),
		logger:    zerolog.Nop(),
		ctx:       context.Background(),
	}
	// The forward path pools its connection to the leader; close it so the
	// leader's per-connection goroutine does not sit in its idle read.
	t.Cleanup(c.closeForwardConn)
	return c
}

func TestFollowerBarrier_WaitsForBacklogAfterRestart(t *testing.T) {
	addrs := allocFreePorts(t, 3)
	dirB := filepath.Join(t.TempDir(), "writer-B")

	raftA := startRaftNodeInDir(t, "writer-A", addrs[0], filepath.Join(t.TempDir(), "writer-A"), true)
	t.Cleanup(func() { _ = raftA.Stop() })
	if err := raftA.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("writer-A WaitForLeader: %v", err)
	}
	raftB := startRaftNodeInDir(t, "reader-B", addrs[1], dirB, false)
	t.Cleanup(func() { _ = raftB.Stop() }) // second Stop after the explicit one below is a harmless error
	raftC := startRaftNodeInDir(t, "writer-C", addrs[2], filepath.Join(t.TempDir(), "writer-C"), false)
	t.Cleanup(func() { _ = raftC.Stop() })
	if err := raftA.AddVoter("reader-B", addrs[1], 10*time.Second); err != nil {
		t.Fatalf("AddVoter reader-B: %v", err)
	}
	if err := raftA.AddVoter("writer-C", addrs[2], 10*time.Second); err != nil {
		t.Fatalf("AddVoter writer-C: %v", err)
	}

	// The leader's coordinator, reachable by the forward path, and its
	// address in the FSM node table (what a join normally puts there).
	peerB := NewNode("reader-B", "reader-B", RoleReader, barrierTestCluster)
	leader := startLeaderCoordinatorServer(t, raftA, peerB)
	if err := raftA.AddNode(&raft.NodeInfo{ID: "writer-A", Role: string(RoleWriter), Address: leader.addr()}, 5*time.Second); err != nil {
		t.Fatalf("AddNode writer-A: %v", err)
	}
	waitFor(t, 10*time.Second, func() bool {
		_, ok := raftB.FSM().GetNode("writer-A")
		return ok
	}, "reader-B never applied the leader's AddNode")

	// Outage: B goes down, the leader commits a backlog (quorum A+C), and
	// the leader's replication to B backs off exponentially in the meantime
	// (hashicorp/raft retries a refused follower after 10 ms·2^(failures-2)).
	// The test needs the production shape: B knows the leader again (first
	// heartbeat) while its backlog is still seconds away in that backoff.
	// Timing on a loaded runner can let the backlog win, so the cycle is
	// retried with a longer outage rather than failing on the first try.
	const backlog = 50
	backlogPath := func(attempt, i int) string {
		return fmt.Sprintf("testdb/cpu/2026/09/14/20/backlog-a%d-%02d.parquet", attempt, i)
	}
	cur := raftB
	var raftB2 *raft.Node
	attempt := 0
	for attempt = 1; attempt <= 3; attempt++ {
		if err := cur.Stop(); err != nil {
			t.Fatalf("stop reader-B (attempt %d): %v", attempt, err)
		}
		for i := 0; i < backlog; i++ {
			entry := makeFileEntry(backlogPath(attempt, i), []byte("x"), "writer-A")
			entry.CreatedAt = time.Now()
			if err := raftA.RegisterFile(entry, 5*time.Second); err != nil {
				t.Fatalf("RegisterFile %d: %v", i, err)
			}
		}
		time.Sleep(time.Duration(attempt) * 3 * time.Second)

		cur = startRaftNodeInDir(t, "reader-B", addrs[1], dirB, false)
		n := cur
		t.Cleanup(func() { _ = n.Stop() })
		if err := cur.WaitForLeader(15 * time.Second); err != nil {
			t.Fatalf("reader-B WaitForLeader after restart: %v", err)
		}
		if _, ok := cur.FSM().GetFile(backlogPath(attempt, backlog-1)); !ok {
			raftB2 = cur
			break
		}
		t.Logf("attempt %d: backlog already applied when the follower learned the leader; retrying with a longer outage", attempt)
	}
	if raftB2 == nil {
		t.Fatal("could not establish the precondition (backlog still in flight when the follower knows the leader) in 3 attempts")
	}
	follower := newFollowerCoordinator(t, "reader-B", raftB2)

	start := time.Now()
	if err := follower.waitForManifestSync(context.Background(), raftB2, 20*time.Second); err != nil {
		t.Fatalf("waitForManifestSync: %v", err)
	}
	missing := 0
	for i := 0; i < backlog; i++ {
		if _, ok := raftB2.FSM().GetFile(backlogPath(attempt, i)); !ok {
			missing++
		}
	}
	if missing != 0 {
		t.Fatalf("after the barrier %d of %d backlog entries are still missing on the restarted follower (waited %s)", missing, backlog, time.Since(start))
	}
	t.Logf("backlog of %d applied on the restarted follower after %s", backlog, time.Since(start))
}

// A leader that accepts the connection and never answers must not hang the
// startup walk. Each forwarded attempt is bounded by the protocol's fixed
// round-trip deadline (forwardApplyTimeout, 5 s; the open PR #787 proposes
// honoring the caller's context instead), and the retry loop checks the
// barrier deadline between attempts, so the wait ends within one round trip
// of the configured timeout.
func TestFollowerBarrier_TimesOutOnSilentLeader(t *testing.T) {
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

	silent := startHangingOrigin(t) // accepts, reads the request, never replies
	follower := newFollowerCoordinator(t, "reader-B", raftB)
	leaderNode := NewNode("writer-A", "writer-A", RoleWriter, barrierTestCluster)
	leaderNode.Address = silent.addr()
	if err := follower.registry.Register(leaderNode); err != nil {
		t.Fatalf("register: %v", err)
	}

	start := time.Now()
	err := follower.waitForManifestSync(context.Background(), raftB, 700*time.Millisecond)
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("expected a timeout error from a silent leader")
	}
	if limit := 700*time.Millisecond + forwardApplyTimeout + 2*time.Second; elapsed > limit {
		t.Fatalf("wait must end within one round trip of the barrier timeout (%s), took %s", limit, elapsed)
	}
	t.Logf("silent leader: %v after %s", err, elapsed)
}

// The barrier token is a security nonce; the FSM cap must keep headroom so a
// larger nonce can never turn every barrier into a silent fallback.
func TestFollowerBarrier_TokenFitsFSMCap(t *testing.T) {
	token, err := security.GenerateNonce()
	if err != nil {
		t.Fatalf("GenerateNonce: %v", err)
	}
	if len(token) > raft.MaxBarrierTokenLen/2 {
		t.Fatalf("nonce is %d bytes, cap is %d: keep at least 2x headroom", len(token), raft.MaxBarrierTokenLen)
	}
}
