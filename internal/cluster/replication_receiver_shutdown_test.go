package cluster

import (
	"context"
	"testing"
	"time"
)

// TestStopReplicationReceiverBlocksRetargetDuringShutdown pins the guard the
// shutdown hook relies on. StopReplicationReceiver deliberately leaves c.ctx
// live — the coordinator must keep serving manifest applies until it stops as
// a component — so the retarget loop keeps running on that context, and a
// primary handover inside the shutdown window would otherwise make it install
// a fresh receiver into buffers that are closing (#853). The receiver pointer
// must not change after the hook has run.
func TestStopReplicationReceiverBlocksRetargetDuringShutdown(t *testing.T) {
	c := targetRig(t, "writer-1", "writer-1", "writer-2")

	c.mu.Lock()
	err := c.startReceiverWithAddr("writer-1:9000")
	c.mu.Unlock()
	if err != nil {
		t.Fatalf("startReceiverWithAddr: %v", err)
	}
	c.mu.RLock()
	before := c.replicationReceiver
	c.mu.RUnlock()
	if before == nil {
		t.Fatal("setup: no receiver was installed")
	}
	t.Cleanup(func() {
		c.mu.RLock()
		r := c.replicationReceiver
		c.mu.RUnlock()
		if r != nil {
			_ = r.Stop()
		}
	})

	// The shutdown hook runs.
	c.StopReplicationReceiver()

	// Then the cluster hands over to writer-2 while this node is still
	// shutting down; the loop would want to attach to it.
	if err := c.raftNode.PromoteWriter("writer-2", "writer-1", 5*time.Second); err != nil {
		t.Fatalf("PromoteWriter: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for c.raftNode.FSM().GetPrimaryWriterID() != "writer-2" {
		if time.Now().After(deadline) {
			t.Fatal("setup: FSM never recorded writer-2 as primary")
		}
		time.Sleep(10 * time.Millisecond)
	}

	c.reevaluateReplicationTarget(context.Background())

	c.mu.RLock()
	after := c.replicationReceiver
	c.mu.RUnlock()
	if after != before {
		t.Fatalf("a fresh receiver (writer %q) was installed after the shutdown hook stopped replication; it would apply into buffers that are closing", after.WriterAddr())
	}
}
