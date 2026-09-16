package cluster

import (
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/raft"
)

// #872: with no failover manager, IsPrimaryWriter returned true for EVERY
// writer-role node on local storage, so retention, continuous queries and
// deletes ran on all of them.
//
// The fix elects a primary regardless of cluster.failover_enabled and of the
// writer_failover licence, because the licensed capability is named "Automatic
// writer failover" — replacing a primary that died, not having one. These
// tests pin both halves of that line.

// electionRigNoAuto is electionRig with automatic failover switched off, which
// is the unlicensed or flag-off cluster.
func electionRigNoAuto(t *testing.T, writerIDs ...string) (*WriterFailoverManager, *raft.Node, *Registry) {
	t.Helper()
	mgr, raftNode, reg := electionRig(t, writerIDs...)
	mgr.cfg.AutoFailover = false
	return mgr, raftNode, reg
}

// A cluster that cannot fail over must still get a primary. Without one, no
// node passes IsPrimaryWriter and the singleton schedulers never run — which
// is not a licensing outcome, it is a broken cluster.
func TestElectsAPrimaryWithoutAutomaticFailover(t *testing.T) {
	mgr, raftNode, _ := electionRigNoAuto(t, "writer-1", "writer-2")

	mgr.checkPrimaryHealth()

	if got := waitForFSMPrimary(t, raftNode, 5*time.Second); got == "" {
		t.Fatal("no primary was elected on a cluster without automatic failover; every writer would consider itself primary")
	}
}

// The other half of the line: once a primary exists, replacing it is the paid
// capability. A cluster without it must not quietly promote a successor.
func TestDoesNotReplaceAPrimaryWithoutAutomaticFailover(t *testing.T) {
	mgr, raftNode, reg := electionRigNoAuto(t, "writer-1", "writer-2")

	mgr.checkPrimaryHealth()
	first := waitForFSMPrimary(t, raftNode, 5*time.Second)
	if first == "" {
		t.Fatal("setup: no primary elected")
	}

	// The primary goes away. GetPrimaryWriter filters on health, so from the
	// registry's point of view there is now no primary at all — which is
	// exactly the state that used to trigger an election.
	if node, ok := reg.Get(first); ok {
		reg.UpdateNodeState(node.ID, StateDead)
	} else {
		mgr.cfg.Registry.Unregister(first)
	}

	for i := 0; i < 5; i++ {
		mgr.checkPrimaryHealth()
	}

	// Wait, rather than sampling once: a promotion would land asynchronously.
	fsmPrimaryStaysAt(t, raftNode, first, 2*time.Second)
}

// And with the licence and the flag, it does replace it.
func TestReplacesAPrimaryWithAutomaticFailover(t *testing.T) {
	mgr, raftNode, reg := electionRig(t, "writer-1", "writer-2")
	mgr.cfg.AutoFailover = true
	mgr.cfg.UnhealthyThreshold = 1

	mgr.checkPrimaryHealth()
	first := waitForFSMPrimary(t, raftNode, 5*time.Second)
	if first == "" {
		t.Fatal("setup: no primary elected")
	}

	reg.Unregister(first)
	for i := 0; i < 5; i++ {
		mgr.checkPrimaryHealth()
		if currentFSMPrimary(raftNode) != first {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Errorf("primary stayed %q with automatic failover enabled", first)
}

// The discriminator has to be the FSM's durable record, not the manager's
// in-memory primaryID. A leader restart or a leadership change gives a fresh
// manager with an empty field, and reading that would make a dead primary look
// like a cluster that never had one — electing a replacement, unlicensed.
func TestAFreshManagerDoesNotElectOverAnExistingPrimary(t *testing.T) {
	mgr, raftNode, reg := electionRigNoAuto(t, "writer-1", "writer-2")

	mgr.checkPrimaryHealth()
	first := waitForFSMPrimary(t, raftNode, 5*time.Second)
	if first == "" {
		t.Fatal("setup: no primary elected")
	}

	// A brand-new manager over the same cluster: this is what the next leader
	// starts with. Its in-memory primaryID is empty; the FSM's is not.
	fresh := NewWriterFailoverManager(&WriterFailoverConfig{
		Registry:     reg,
		RaftNode:     raftNode,
		AutoFailover: false,
		Logger:       testLogger(),
	})
	if fresh.primaryID != "" {
		t.Fatal("setup: a fresh manager should have no in-memory primary")
	}
	if fresh.designatedPrimaryID() != first {
		t.Fatalf("the FSM should still name %q as primary, got %q", first, fresh.designatedPrimaryID())
	}

	reg.Unregister(first)
	for i := 0; i < 5; i++ {
		fresh.checkPrimaryHealth()
	}

	fsmPrimaryStaysAt(t, raftNode, first, 2*time.Second)
}

// The manual hand-over is what an unlicensed cluster recovers with. Demoting
// clears the designation, which is precisely the condition the election reads.
func TestHandOverClearsTheDesignationSoTheClusterElects(t *testing.T) {
	mgr, raftNode, _ := electionRigNoAuto(t, "writer-1", "writer-2")

	mgr.checkPrimaryHealth()
	first := waitForFSMPrimary(t, raftNode, 5*time.Second)
	if first == "" {
		t.Fatal("setup: no primary elected")
	}

	if err := raftNode.DemoteWriter(first, 5*time.Second); err != nil {
		t.Fatalf("DemoteWriter: %v", err)
	}
	if got := raftNode.FSM().GetPrimaryWriterID(); got != "" {
		t.Fatalf("demote left the designation as %q, so nothing would elect", got)
	}

	// With no designation the election runs again, licence or not.
	for i := 0; i < 5; i++ {
		mgr.checkPrimaryHealth()
		if waitForFSMPrimary(t, raftNode, time.Second) != "" {
			return
		}
	}
	t.Error("no primary was elected after the hand-over")
}

// fsmPrimaryStaysAt fails if the FSM's designated primary ever moves away from
// want during d.
//
// Polling for a while is the whole point: executeFailover runs on its own
// goroutine, so reading the FSM immediately after driving the check loop sees
// the state BEFORE any promotion lands, and a test written that way passes
// whether or not the licence gate exists. Both of these tests did exactly that
// until a revert-run caught it.
func fsmPrimaryStaysAt(t *testing.T, n *raft.Node, want string, d time.Duration) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if got := currentFSMPrimary(n); got != want {
			t.Fatalf("the primary moved from %q to %q; replacing a primary is the licensed capability", want, got)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func currentFSMPrimary(n *raft.Node) string {
	return n.FSM().GetPrimaryWriterID()
}

// Guard the licence boundary itself: the two states must be distinguishable
// from durable data alone, with no reliance on process-local memory.
func TestTheLicenceBoundaryReadsOnlyDurableState(t *testing.T) {
	mgr, raftNode, _ := electionRigNoAuto(t, "writer-1")

	if mgr.designatedPrimaryID() != "" {
		t.Fatal("a cluster that has never elected should have no designated primary")
	}
	mgr.checkPrimaryHealth()
	if waitForFSMPrimary(t, raftNode, 5*time.Second) == "" {
		t.Fatal("setup: no primary elected")
	}
	if mgr.designatedPrimaryID() == "" {
		t.Error("after an election the designation must be readable from the FSM, or the boundary cannot be enforced across a leader change")
	}
}

// Blocker from review: a demotion announced nothing. Applying one changed only
// the Raft record, so the demoted node's own view still said "primary" and it
// kept running retention, continuous queries and deletes — while the leader
// still saw a live primary in its registry and so never elected a successor.
// Handing over a HEALTHY writer, which is the drain-before-upgrade case the
// endpoint exists for, was therefore a no-op that reported success.
func TestHandOverOfAHealthyPrimaryIsAnnounced(t *testing.T) {
	mgr, raftNode, reg := electionRigNoAuto(t, "writer-1", "writer-2")

	// Stand in for the coordinator's callback wiring.
	var demoted []string
	raftNode.FSM().SetWriterDemotedCallback(func(nodeID string) {
		demoted = append(demoted, nodeID)
		if n, ok := reg.Get(nodeID); ok {
			n.SetWriterState(WriterStateStandby)
			_ = reg.Register(n)
		}
	})

	mgr.checkPrimaryHealth()
	first := waitForFSMPrimary(t, raftNode, 5*time.Second)
	if first == "" {
		t.Fatal("setup: no primary elected")
	}

	// A hand-over is a promotion of somebody else, not a demotion followed by
	// an election: an election would be free to pick the same node straight
	// back, which is not a hand-over.
	newPrimary, err := mgr.HandOver(first)
	if err != nil {
		t.Fatalf("HandOver: %v", err)
	}
	if newPrimary == first || newPrimary == "" {
		t.Fatalf("hand-over returned %q, which is not a different writer", newPrimary)
	}
	if got := currentFSMPrimary(raftNode); got != newPrimary {
		t.Errorf("the cluster records %q as primary, want %q", got, newPrimary)
	}

	// And the outgoing node is recorded as standby, which is what stops it
	// running the singleton work. A promotion carries that as its back half
	// and announces both sides, which is exactly why a hand-over is written as
	// a promotion rather than a demotion plus an election.
	for _, n := range raftNode.FSM().GetAllNodes() {
		if n.ID == first && n.WriterState == string(WriterStatePrimary) {
			t.Errorf("the outgoing primary %q is still recorded as primary; it would go on running retention and continuous queries", first)
		}
	}
	_ = demoted
}

// The fallback, which is the path the demotion callback exists for: nobody to
// hand to. The designation is released anyway, so the cluster is not pinned to
// a writer that may never return, and the outgoing node is told.
func TestHandOverWithNobodyToHandTo(t *testing.T) {
	mgr, raftNode, reg := electionRigNoAuto(t, "writer-1")

	var demoted []string
	raftNode.FSM().SetWriterDemotedCallback(func(nodeID string) {
		demoted = append(demoted, nodeID)
	})

	mgr.checkPrimaryHealth()
	first := waitForFSMPrimary(t, raftNode, 5*time.Second)
	if first == "" {
		t.Fatal("setup: no primary elected")
	}
	_ = reg

	newPrimary, err := mgr.HandOver(first)
	if err != nil {
		t.Fatalf("HandOver: %v", err)
	}
	if newPrimary != "" {
		t.Errorf("hand-over reported %q took over, but this cluster has only one writer", newPrimary)
	}
	if got := raftNode.FSM().GetPrimaryWriterID(); got != "" {
		t.Errorf("the designation is still %q, so the cluster stays pinned to a writer nobody handed it to", got)
	}
	if len(demoted) == 0 {
		t.Error("the release fired no callback, so the outgoing primary still believes it is primary")
	}
}

// Second blocker: removing the designated primary left the record naming a
// node that no longer exists. The election only runs when nothing is
// designated, so it never ran again — no primary, and no retention, continuous
// queries or deletes anywhere, with nothing reporting why. Removing the dead
// primary is the obvious operator move, which made this the likely path.
func TestRemovingTheDesignatedPrimaryReleasesTheDesignation(t *testing.T) {
	mgr, raftNode, _ := electionRigNoAuto(t, "writer-1", "writer-2")

	mgr.checkPrimaryHealth()
	first := waitForFSMPrimary(t, raftNode, 5*time.Second)
	if first == "" {
		t.Fatal("setup: no primary elected")
	}

	if err := raftNode.RemoveNode(first, 5*time.Second); err != nil {
		t.Fatalf("RemoveNode: %v", err)
	}

	if got := raftNode.FSM().GetPrimaryWriterID(); got != "" {
		t.Fatalf("removing the primary left the designation as %q, so nothing would ever elect again", got)
	}
}
