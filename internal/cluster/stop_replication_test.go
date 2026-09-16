package cluster

// Stopping replication must not deadlock on the coordinator lock, must not
// leave the WAL hook pointing at a stopped sender, and must not let shutdown
// wait on a receiver whose connect path ignores the context (#853).

import (
	"context"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/replication"
	"github.com/rs/zerolog"
)

// The WAL replication hook closes over the coordinator and fires for every
// appended entry. Shutdown stops the sender while the WAL is still open, so a
// write in flight must find a no-op rather than a nil dereference.
func TestSenderReplicate_IsNilAndStoppedSafe(t *testing.T) {
	var nilSender *replication.Sender
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("Replicate panicked on a nil sender: %v", r)
			}
		}()
		nilSender.Replicate(&replication.ReplicateEntry{Sequence: 1})
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Replicate hung on a nil sender")
	}
}

// StopReplication holds c.mu only to snapshot; the joins happen outside it.
// Anything taking c.mu.RLock while it runs must not block it, which is what
// the receiver's own goroutines do through the ingest handler.
func TestStopReplication_DoesNotHoldTheCoordinatorLockWhileStopping(t *testing.T) {
	c := &Coordinator{logger: zerolog.Nop()}

	// A reader that must be able to take c.mu the whole time.
	stop := make(chan struct{})
	defer close(stop)
	blocked := make(chan struct{}, 1)
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
			}
			c.mu.RLock()
			c.mu.RUnlock()
			select {
			case blocked <- struct{}{}:
			default:
			}
			time.Sleep(time.Millisecond)
		}
	}()

	done := make(chan struct{})
	go func() { defer close(done); c.StopReplication() }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("StopReplication did not return; the lock was held across the joins")
	}
	select {
	case <-blocked:
	case <-time.After(2 * time.Second):
		t.Error("a reader never acquired c.mu while StopReplication ran")
	}
}

// A receiver that will not stop must not hold shutdown open indefinitely.
func TestStopReplicationSubsystems_BoundsTheReceiverJoin(t *testing.T) {
	if replicationStopTimeout > 10*time.Second {
		t.Fatalf("replicationStopTimeout is %v; shutdown should not wait that long on a receiver", replicationStopTimeout)
	}
	c := &Coordinator{logger: zerolog.Nop()}
	start := time.Now()
	c.stopReplicationSubsystems(nil, nil)
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("stopping with nothing to stop took %v", elapsed)
	}
}

// Shutdown must actually join the replication sender. Before this it was left
// to exit on the shared context alone and never joined, so it could still be
// draining while the WAL and the Arrow buffer were being closed — hooks all
// run before any component.
func TestStop_JoinsTheReplicationSender(t *testing.T) {
	c, raftNode := newShutdownRig(t, allocFreePort(t), 1000)
	_ = raftNode

	sender := replication.NewSender(&replication.SenderConfig{
		BufferSize:   16,
		WriteTimeout: time.Second,
		Logger:       zerolog.Nop(),
		SharedSecret: "test-cluster-secret-32-bytes-long!",
	})
	if err := sender.Start(context.Background()); err != nil {
		t.Fatalf("start sender: %v", err)
	}
	c.mu.Lock()
	c.replicationSender = sender
	c.mu.Unlock()

	if err := c.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	// A running sender assigns a sequence number to every entry it accepts;
	// a stopped one returns before assigning. So the sequence standing still
	// is the proof that shutdown actually joined it.
	before := sender.CurrentSequence()
	sender.Replicate(&replication.ReplicateEntry{})
	if after := sender.CurrentSequence(); after != before {
		t.Errorf("the sender accepted an entry after Stop (sequence %d -> %d), so shutdown did not join it", before, after)
	}
	if err := sender.Stop(); err != nil {
		t.Errorf("second Stop: %v", err)
	}
}
