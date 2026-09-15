package cluster

// Coordinator.Stop must not hold c.mu while it joins its subsystems (#813).
// The Raft join waits for the FSM apply goroutine; a callback on it that
// takes c.mu (or any code on a joined goroutine that does) deadlocked
// shutdown while Stop held the lock across the join. #797 removed one such
// callback; these tests pin the structure so the next one cannot bring the
// deadlock back.

import (
	"errors"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/raft"
	hraft "github.com/hashicorp/raft"
)

// gateWriterPromotedCallback installs a writer-promoted callback that parks
// until the gate opens and then calls IsRunning, which takes c.mu. It
// returns the channel that closes when the callback has entered.
func gateWriterPromotedCallback(c *Coordinator, gate <-chan struct{}, result chan<- bool) <-chan struct{} {
	entered := make(chan struct{})
	c.raftFSM.SetWriterPromotedCallback(func(newPrimaryID, oldPrimaryID string) {
		close(entered)
		<-gate
		result <- c.IsRunning()
	})
	return entered
}

// promoteWriterInBackground adds a writer to the FSM (PromoteWriter refuses
// an unknown node before it reaches the callback) and proposes the
// promotion on a goroutine.
func promoteWriterInBackground(t *testing.T, raftNode *raft.Node) <-chan error {
	t.Helper()
	if err := raftNode.AddNode(&raft.NodeInfo{ID: "writer-1", Name: "writer-1", Role: string(RoleWriter), Address: "127.0.0.1:1", State: string(StateHealthy)}, 5*time.Second); err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- raftNode.PromoteWriter("writer-1", "", 10*time.Second) }()
	return done
}

// openGateWhenRaftIsShuttingDown closes the gate once the Raft instance
// reports Shutdown, which hashicorp sets synchronously in Shutdown(): a
// positive signal that Stop is inside its Raft join. On the pre-#813 code
// State() blocks on n.mu (Node.Stop held it across the join), the gate never
// opens, and Stop never returns; the callers report that at their budget.
func openGateWhenRaftIsShuttingDown(raftNode *raft.Node, gate chan struct{}) {
	go func() {
		for raftNode.State() != hraft.Shutdown {
			time.Sleep(5 * time.Millisecond)
		}
		close(gate)
	}()
}

func TestStop_ReturnsWhileAnFSMCallbackTakesTheCoordinatorLock(t *testing.T) {
	c, raftNode := newShutdownRig(t, allocFreePort(t), 1000) // peer port closed: pulls fail fast
	gate := make(chan struct{})
	result := make(chan bool, 1)
	entered := gateWriterPromotedCallback(c, gate, result)
	promoteDone := promoteWriterInBackground(t, raftNode)
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("the writer-promoted callback never ran")
	}

	stopDone := make(chan error, 1)
	go func() { stopDone <- c.Stop() }()
	openGateWhenRaftIsShuttingDown(raftNode, gate)

	select {
	case err := <-stopDone:
		if err != nil {
			t.Fatalf("Stop: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("Stop did not return within 15s: c.mu was held across the Raft join while an FSM callback waited for it (#813); rig leaked on purpose")
	}
	select {
	case <-result:
	case <-time.After(5 * time.Second):
		t.Fatal("the callback never finished after Stop returned")
	}
	if err := <-promoteDone; err != nil {
		t.Logf("PromoteWriter resolved with %v once the callback was released", err)
	}
	if c.IsRunning() {
		t.Error("IsRunning() true after Stop")
	}
}

// A second Stop during the join returns at once without closing stopCh
// twice, a Start during the join is refused, and both Stops complete.
func TestStop_IsIdempotentAndRefusesStartDuringTheJoin(t *testing.T) {
	c, raftNode := newShutdownRig(t, allocFreePort(t), 1000)
	gate := make(chan struct{})
	result := make(chan bool, 1)
	entered := gateWriterPromotedCallback(c, gate, result)
	promoteDone := promoteWriterInBackground(t, raftNode)
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("the writer-promoted callback never ran")
	}

	firstStop := make(chan error, 1)
	go func() { firstStop <- c.Stop() }()
	// Wait until the first Stop is inside its Raft join (the callback is
	// parked, so the join cannot complete before the gate opens).
	deadline := time.Now().Add(10 * time.Second)
	for raftNode.State() != hraft.Shutdown {
		if time.Now().After(deadline) {
			t.Fatal("Raft never reported Shutdown while the first Stop was running")
		}
		time.Sleep(5 * time.Millisecond)
	}
	secondStop := make(chan error, 1)
	go func() { secondStop <- c.Stop() }()
	select {
	case err := <-secondStop:
		if err != nil {
			t.Fatalf("second Stop: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a second Stop blocked behind the first one's joins")
	}
	if err := c.Start(); !errors.Is(err, ErrAlreadyRunning) {
		t.Fatalf("Start during the join = %v; want ErrAlreadyRunning", err)
	}
	close(gate)
	select {
	case err := <-firstStop:
		if err != nil {
			t.Fatalf("first Stop: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("the first Stop did not return within 15s after the callback was released")
	}
	<-result
	<-promoteDone
	if c.IsRunning() {
		t.Error("IsRunning() true after Stop")
	}
}
