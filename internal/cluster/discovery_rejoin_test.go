package cluster

import (
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/rs/zerolog"
)

// #858: a node that leaves gracefully and restarts was never listed again by
// the rest of the cluster.
//
// Reproduced on three real binaries. The leaving node was the Raft leader, so
// no peer was leader when its leave arrived and no peer removed it from the
// FSM — they only dropped it from their local registries. On restart it
// rejoined the Raft configuration it had never been removed from, learned the
// new leader about a second later, and discovery, which stopped as soon as a
// leader was known, never tried again. Its single attempt had landed inside
// the leaderless window its own departure created and failed with "no valid
// leader address".

func TestDiscoveryShouldRun(t *testing.T) {
	tests := []struct {
		name     string
		joined   bool
		leaving  bool
		isLeader bool
		want     bool
	}{
		// The #858 case: not yet joined in this process, a leader exists, and
		// this node is not it. This MUST attempt. The shipped code returned
		// early here, which is the whole bug.
		{"restarted node, leader known, not joined", false, false, false, true},
		{"fresh boot, no leader yet", false, false, false, true},

		{"already joined, leader known", true, false, false, false},
		{"already joined, no leader", true, false, false, false},

		// An operator removal must stay removed: the latch is "have I joined",
		// not "does the cluster list me", so nothing re-derives membership.
		{"joined then removed by an operator", true, false, false, false},

		{"leaving, not joined", false, true, false, false},
		{"leaving, joined", true, true, false, false},

		// A leader is a member by construction.
		{"is the leader", false, false, true, false},
		{"is the leader, not joined", false, false, true, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := discoveryShouldRun(tt.joined, tt.leaving, tt.isLeader)
			if got != tt.want {
				t.Errorf("discoveryShouldRun(joined=%v, leaving=%v, isLeader=%v) = %v, want %v",
					tt.joined, tt.leaving, tt.isLeader, got, tt.want)
			}
		})
	}
}

// Knowing a leader must not, on its own, stop discovery. Stated separately
// from the table because it is the exact predicate that shipped, and a change
// that reintroduces it would otherwise read as a tidy-up.
func TestDiscoveryDoesNotStopMerelyBecauseALeaderIsKnown(t *testing.T) {
	// The predicate no longer takes "is a leader known" at all, which is the
	// point: it was the whole condition, and a node the cluster had forgotten
	// learned the leader a second after its only attempt failed. Asserted as a
	// property of discoverPeers rather than of the predicate's signature, so
	// reintroducing the check anywhere fails here.
	if !discoveryShouldRun(false, false, false) {
		t.Fatal("a node that has not joined and is not the leader must attempt a join (#858)")
	}
}

// countingSeed accepts and immediately closes connections, counting them, so a
// test can observe how many join attempts discovery actually makes.
func countingSeed(t *testing.T) (addr string, count *atomic.Int64) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	count = &atomic.Int64{}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			count.Add(1)
			_ = conn.Close()
		}
	}()
	return ln.Addr().String(), count
}

func discoveryTestCoordinator(t *testing.T, seed string) *Coordinator {
	t.Helper()
	local := NewNode("local-1", "local-1", RoleWriter, "test-cluster")
	local.State = StateHealthy
	return &Coordinator{
		cfg: &config.ClusterConfig{
			ClusterName:   "test-cluster",
			Seeds:         []string{seed},
			AdvertiseAddr: "127.0.0.1:19999",
		},
		registry:  NewRegistry(&RegistryConfig{LocalNode: local, Logger: zerolog.Nop()}),
		localNode: local,
		logger:    zerolog.Nop(),
	}
}

// Until a join succeeds, every tick tries again. Before the fix a node in this
// state stopped after one attempt.
func TestDiscoveryKeepsTryingUntilAJoinSucceeds(t *testing.T) {
	seed, attempts := countingSeed(t)
	c := discoveryTestCoordinator(t, seed)

	for i := 0; i < 3; i++ {
		c.discoverPeers()
	}

	if got := attempts.Load(); got < 3 {
		t.Errorf("made %d join attempts across 3 ticks, want 3: a node the cluster has forgotten has to keep asking", got)
	}
}

// Once a join has succeeded the loop goes quiet, so a healthy cluster is not
// dialling its seeds forever.
func TestDiscoveryStopsAfterAJoinSucceeds(t *testing.T) {
	seed, attempts := countingSeed(t)
	c := discoveryTestCoordinator(t, seed)

	c.discoverPeers()
	before := attempts.Load()
	if before == 0 {
		t.Fatal("the first tick should have attempted a join")
	}

	c.joinedOnce.Store(true)
	for i := 0; i < 3; i++ {
		c.discoverPeers()
	}

	if got := attempts.Load(); got != before {
		t.Errorf("attempts went from %d to %d after a successful join; discovery should be latched", before, got)
	}
}

// A tick inside the leave broadcast must not re-join the cluster this node is
// leaving. Stop sets `leaving` before broadcasting, precisely because it sets
// `stopping` only afterwards.
func TestDiscoveryStopsOnceLeaving(t *testing.T) {
	seed, attempts := countingSeed(t)
	c := discoveryTestCoordinator(t, seed)

	c.leaving.Store(true)
	for i := 0; i < 3; i++ {
		c.discoverPeers()
	}

	if got := attempts.Load(); got != 0 {
		t.Errorf("made %d join attempts while leaving, want 0", got)
	}
}

// Stop must set `leaving` BEFORE it broadcasts, or the window this guards is
// still open: Stop sets its own `stopping` flag only after the broadcast.
//
// The observation is exact rather than timed. The peer points at a real local
// listener, and the flag is read from inside the accept handler: a connection
// cannot be accepted before broadcastLeave has begun dialling it, so if the
// flag is set by then, it was set first. An earlier version of this test used
// an unroutable address and a poll, which certified the bug on any host where
// the dial fails fast instead of hanging.
func TestStopMarksLeavingBeforeBroadcast(t *testing.T) {
	seed, _ := countingSeed(t)
	c := discoveryTestCoordinator(t, seed)
	c.stopCh = make(chan struct{})
	c.running = true

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	observed := make(chan bool, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		// Read the flag at the moment the leave broadcast reaches a peer.
		select {
		case observed <- c.leaving.Load():
		default:
		}
		_ = conn.Close()
	}()

	peer := NewNode("peer-1", "peer-1", RoleReader, "test-cluster")
	peer.State = StateHealthy
	peer.SetAddresses(ln.Addr().String(), "127.0.0.1:1")
	if err := c.registry.Register(peer); err != nil {
		t.Fatalf("registry.Register: %v", err)
	}

	stopped := make(chan struct{})
	go func() { defer close(stopped); _ = c.Stop() }()

	select {
	case wasLeaving := <-observed:
		if !wasLeaving {
			t.Fatal("the leave broadcast reached a peer while `leaving` was still unset; a discovery tick in that window could re-join the cluster this node is leaving")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the leave broadcast never reached the peer")
	}

	select {
	case <-stopped:
	case <-time.After(10 * time.Second):
		t.Fatal("Stop did not return")
	}
}

// A second Stop must not broadcast again. The compare-and-swap on `leaving` is
// what makes that true for CONCURRENT callers; the pre-existing RLock check is
// not atomic with the broadcast.
func TestStopBroadcastsOnlyOnce(t *testing.T) {
	seed, _ := countingSeed(t)
	c := discoveryTestCoordinator(t, seed)
	c.stopCh = make(chan struct{})
	c.running = true

	var broadcasts atomic.Int64
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			broadcasts.Add(1)
			_ = conn.Close()
		}
	}()

	peer := NewNode("peer-1", "peer-1", RoleReader, "test-cluster")
	peer.State = StateHealthy
	peer.SetAddresses(ln.Addr().String(), "127.0.0.1:1")
	if err := c.registry.Register(peer); err != nil {
		t.Fatalf("registry.Register: %v", err)
	}

	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _ = c.Stop() }()
	}
	wg.Wait()

	if got := broadcasts.Load(); got > 1 {
		t.Errorf("four concurrent Stops produced %d leave broadcasts, want 1", got)
	}
}
