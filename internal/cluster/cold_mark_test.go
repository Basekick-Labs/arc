package cluster

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/protocol"
	"github.com/basekick-labs/arc/internal/cluster/raft"
)

// #1552: the cold mark, coordinator side.

var cmList = []string{raft.CapabilityColdMark}

func coldMarkRig(t *testing.T, readerCaps []string) *Coordinator {
	t.Helper()
	c := originRig(t, "writer-1", RoleWriter)
	capNodes(t, c, map[string][]string{"raft-1": cmList, "writer-1": cmList, "writer-2": cmList, "reader-1": readerCaps})
	return c
}

func coldMarkEnabled(c *Coordinator) func() bool {
	return func() bool { st, _ := c.ColdMarkStatus(); return st.Enabled }
}

func TestColdMarkEnableWaitsForEveryNode(t *testing.T) {
	c := coldMarkRig(t, nil)
	c.maybeEnableColdMark()
	if st, blocking := c.ColdMarkStatus(); st.Enabled || len(blocking) != 1 || blocking[0] != "reader-1" {
		t.Fatalf("status %+v blocking %v, want reader-1 blocking", st, blocking)
	}
	capNodes(t, c, map[string][]string{"reader-1": cmList})
	c.maybeEnableColdMark()
	waitFSM(t, coldMarkEnabled(c))

	// Once on, an older build cannot join.
	if why, _ := c.coldMarkJoinGate(&protocol.JoinRequest{NodeID: "w9"}); why == "" {
		t.Fatal("a build without the cold mark joined a cluster that has it on")
	}
	why, release := c.coldMarkJoinGate(&protocol.JoinRequest{NodeID: "w9", Capabilities: cmList})
	if why != "" {
		t.Fatalf("a capable node refused: %s", why)
	}
	release()
}

// The mark is raised, and committed, before the write runs.
func TestColdWriteRaisesTheMarkFirst(t *testing.T) {
	c := coldMarkRig(t, cmList)
	fsm := c.raftNode.FSM()
	ran := false
	err := c.ColdWrite(context.Background(), map[string]int32{"db/cpu": 42}, func() error {
		ran = true
		if !fsm.ColdMarkCovers("db/cpu", 42) {
			t.Fatal("write ran before the mark covered its day")
		}
		return nil
	})
	if err != nil || !ran {
		t.Fatalf("err %v ran %v", err, ran)
	}
}

// Every node supports the mark: one that cannot be raised stops the write.
// A covered day needs no proposal.
func TestColdWriteRefusedWhenTheMarkIsRequired(t *testing.T) {
	c := coldMarkRig(t, cmList)
	if err := c.ColdWrite(context.Background(), map[string]int32{"db/cpu": 10}, func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := c.raftNode.Stop(); err != nil {
		t.Fatal(err)
	}
	ran := false
	err := c.ColdWrite(context.Background(), map[string]int32{"db/cpu": 11}, func() error { ran = true; return nil })
	if err == nil || ran {
		t.Fatalf("err %v ran %v: a write whose mark could not be raised ran", err, ran)
	}
	if err := c.ColdWrite(context.Background(), map[string]int32{"db/cpu": 10}, func() error { ran = true; return nil }); err != nil || !ran {
		t.Fatalf("a covered day: err %v ran %v", err, ran)
	}
}

// A node without the capability: the cluster does not maintain the mark yet,
// so a write runs unmarked when the mark cannot be raised.
func TestColdWriteUnmarkedBeforeEveryNodeSupportsIt(t *testing.T) {
	c := coldMarkRig(t, nil)
	if err := c.raftNode.Stop(); err != nil {
		t.Fatal(err)
	}
	ran := false
	if err := c.ColdWrite(context.Background(), map[string]int32{"db/cpu": 11}, func() error { ran = true; return nil }); err != nil || !ran {
		t.Fatalf("err %v ran %v", err, ran)
	}
}

// The ack waits for a cold write in flight: the fence.
func TestColdMarkAckWaitsForWritesInFlight(t *testing.T) {
	c := coldMarkRig(t, cmList)
	started, release := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- c.ColdWrite(context.Background(), map[string]int32{"db/cpu": 1}, func() error {
			close(started)
			<-release
			return nil
		})
	}()
	<-started
	c.maybeEnableColdMark()
	waitFSM(t, coldMarkEnabled(c))
	acked := make(chan struct{})
	go func() { c.maybeAckColdMark(context.Background()); close(acked) }()
	select {
	case <-acked:
		t.Fatal("acked while a cold write was in flight")
	case <-time.After(200 * time.Millisecond):
	}
	close(release)
	<-acked
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	waitFSM(t, func() bool { return c.raftNode.FSM().ColdMarkAcked("writer-1") })
}

func TestSeedColdMarksNeedsABackend(t *testing.T) {
	c := coldMarkRig(t, cmList)
	if err := c.SeedColdMarks(context.Background(), nil, ""); err == nil {
		t.Fatal("seeded without a backend identity")
	}
	// Before the fence the FSM refuses it.
	err := c.SeedColdMarks(context.Background(), map[string]int32{"db/cpu": 1}, "s3://b/")
	if err == nil || errors.Is(err, context.Canceled) {
		t.Fatalf("seed before the fence: %v", err)
	}
}

// This node confirms it maintains the mark once enabled, at most every
// ColdMarkAliveEvery.
func TestColdMarkAliveIsProposedPeriodically(t *testing.T) {
	c := coldMarkRig(t, cmList)
	fsm := c.raftNode.FSM()
	now := time.Now()
	c.maybeColdMarkAlive(context.Background(), now)
	if !fsm.ColdMarkLastAlive("writer-1").IsZero() {
		t.Fatal("alive proposed before the mark is enabled")
	}
	c.maybeEnableColdMark()
	waitFSM(t, coldMarkEnabled(c))
	c.maybeColdMarkAlive(context.Background(), now)
	waitFSM(t, func() bool { return fsm.ColdMarkLastAlive("writer-1").Equal(now.UTC()) })
	c.maybeColdMarkAlive(context.Background(), now.Add(raft.ColdMarkAliveEvery-time.Second))
	time.Sleep(100 * time.Millisecond)
	if !fsm.ColdMarkLastAlive("writer-1").Equal(now.UTC()) {
		t.Fatal("alive proposed again within the interval")
	}
	next := now.Add(raft.ColdMarkAliveEvery)
	c.maybeColdMarkAlive(context.Background(), next)
	waitFSM(t, func() bool { return fsm.ColdMarkLastAlive("writer-1").Equal(next.UTC()) })

	// Not trusted: not seeded, and the other nodes never confirmed.
	if ok, why := c.ColdMarkTrusted(next, raft.ColdMarkAliveMaxAge, "s3://b/"); ok || why == "" {
		t.Fatalf("trusted: %v %q", ok, why)
	}
}

// A reader's maxAge must outlast a confirmation interval plus a loop tick.
func TestColdMarkTimingConstants(t *testing.T) {
	if !(coldMarkEvery < raft.ColdMarkAliveMaxAge-raft.ColdMarkAliveEvery) {
		t.Fatal("coldMarkEvery must be shorter than ColdMarkAliveMaxAge - ColdMarkAliveEvery")
	}
}

// Before the mark is required (a node lacks the capability), a cold write
// proposes nothing: an older build would refuse the command.
func TestColdWriteProposesNothingBeforeRequired(t *testing.T) {
	c := coldMarkRig(t, nil)
	if err := c.ColdWrite(context.Background(), map[string]int32{"db/cpu": 5}, func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	if _, has, _ := c.raftNode.FSM().ColdMark("db/cpu"); has {
		t.Fatal("a mark was proposed before every node supports it")
	}
}

// Once this node saw Enable applied, a failed raise refuses the write even if
// its FSM says the mark is not required (a restarted, lagging FSM).
func TestColdWriteStickyOnceEnabledSeen(t *testing.T) {
	c := coldMarkRig(t, nil) // reader-1 lacks the capability: not required
	c.coldMarkSeenEnabled.Store(true)
	if err := c.raftNode.Stop(); err != nil {
		t.Fatal(err)
	}
	ran := false
	if err := c.ColdWrite(context.Background(), map[string]int32{"db/cpu": 5}, func() error { ran = true; return nil }); err == nil || ran {
		t.Fatalf("err %v ran %v: a write ran unmarked after Enable was seen", err, ran)
	}
}

// The fence drains once: when the first Ack proposal fails after the drain,
// the retry does not wait for cold writes in flight.
func TestColdMarkFenceDrainsOnce(t *testing.T) {
	c := coldMarkRig(t, cmList)
	c.maybeEnableColdMark()
	waitFSM(t, coldMarkEnabled(c))
	if err := c.raftNode.Stop(); err != nil {
		t.Fatal(err)
	}
	c.maybeAckColdMark(context.Background()) // drains, then the proposal fails
	if !c.coldMarkDrained.Load() {
		t.Fatal("not marked drained")
	}
	c.coldWriteMu.RLock() // a cold write in flight
	defer c.coldWriteMu.RUnlock()
	done := make(chan struct{})
	go func() { c.maybeAckColdMark(context.Background()); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the retried ack waited for a cold write")
	}
}

// An Alive this node left in the future (its clock was ahead) is replaced at
// once.
func TestColdMarkAliveReplacesAFutureAlive(t *testing.T) {
	c := coldMarkRig(t, cmList)
	fsm := c.raftNode.FSM()
	c.maybeEnableColdMark()
	waitFSM(t, coldMarkEnabled(c))
	now := time.Now()
	c.maybeColdMarkAlive(context.Background(), now.Add(time.Hour))
	waitFSM(t, func() bool { return fsm.ColdMarkLastAlive("writer-1").After(now) })
	c.maybeColdMarkAlive(context.Background(), now)
	waitFSM(t, func() bool { return fsm.ColdMarkLastAlive("writer-1").Equal(now.UTC()) })
}

// ColdMarkDay reads the FSM with the raise time; the read barrier and reset
// go through Raft; a reset before enable is refused.
func TestColdMarkDayBarrierAndReset(t *testing.T) {
	c := coldMarkRig(t, cmList)
	if _, _, has := c.ColdMarkDay("db/cpu"); has {
		t.Fatal("mark before any write")
	}
	if err := c.ResetColdMark(context.Background(), "admin", "test"); !errors.Is(err, ErrColdMarkNotEnabled) {
		t.Fatalf("reset before enable: %v", err)
	}
	before := time.Now()
	if err := c.ColdWrite(context.Background(), map[string]int32{"db/cpu": 77}, func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := c.ColdMarkReadBarrier(context.Background()); err != nil {
		t.Fatal(err)
	}
	day, raised, has := c.ColdMarkDay("db/cpu")
	if !has || day != 77 || raised.Before(before.Add(-time.Second)) {
		t.Fatalf("day %d raised %v has %v", day, raised, has)
	}
	c.maybeEnableColdMark()
	waitFSM(t, coldMarkEnabled(c))
	if err := c.ResetColdMark(context.Background(), "admin", "test"); err != nil {
		t.Fatal(err)
	}
	if err := c.raftNode.Stop(); err != nil {
		t.Fatal(err)
	}
	if err := c.ColdMarkReadBarrier(context.Background()); err == nil {
		t.Fatal("a barrier succeeded with Raft stopped")
	}
}
