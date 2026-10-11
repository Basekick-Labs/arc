package cluster

// #1207 S2: a Pattern-1 writer that may no longer be the primary refuses
// writes before anything is written.

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/rs/zerolog"
)

type fenceRig struct {
	f       *ingestFence
	now     time.Time
	writer  bool
	voters  int
	others  int
	primary string
	leader  bool
	contact time.Time
}

func newFenceRig() *fenceRig {
	r := &fenceRig{now: time.Unix(1_000_000, 0), writer: true, voters: 3, others: 2, primary: "w1"}
	r.contact = r.now
	r.f = &ingestFence{
		fenceAfter:   7500 * time.Millisecond,
		exitHold:     2 * time.Second,
		contactRule:  true,
		localID:      "w1",
		logger:       zerolog.Nop(),
		now:          func() time.Time { return r.now },
		isWriter:     func() bool { return r.writer },
		voters:       func() int { return r.voters },
		otherWriters: func() int { return r.others },
		designated:   func() string { return r.primary },
		isLeader:     func() bool { return r.leader },
		lastContact:  func() time.Time { return r.contact },
	}
	return r
}

func (r *fenceRig) fenced() bool {
	r.f.evaluate()
	return r.f.gate() != nil
}

func TestIngestFenceRefusesAnIsolatedPrimary(t *testing.T) {
	r := newFenceRig()
	if r.fenced() {
		t.Fatal("a primary in contact with the leader is fenced")
	}
	r.now = r.now.Add(7 * time.Second)
	if r.fenced() {
		t.Fatal("fenced before fenceAfter")
	}
	r.now = r.now.Add(time.Second)
	if !r.fenced() {
		t.Fatal("a primary out of contact past fenceAfter still accepts writes")
	}
	if err := r.f.gate(); !errors.Is(err, ingest.ErrIngestFenced) {
		t.Fatalf("gate error %v does not wrap ErrIngestFenced", err)
	}
}

func TestIngestFenceNeverFencesTheLeader(t *testing.T) {
	r := newFenceRig()
	r.leader = true
	r.contact = time.Time{} // a leader never updates last contact
	if r.fenced() {
		t.Fatal("the Raft leader was fenced")
	}
}

func TestIngestFenceRefusesWhenAnotherWriterIsPrimary(t *testing.T) {
	r := newFenceRig()
	r.primary = "w2"
	if !r.fenced() {
		t.Fatal("a writer that is not the designated primary accepts writes")
	}
}

// Fencing where nothing can take over only costs availability.
func TestIngestFenceStaysOpenWhenNothingCouldReplaceThisNode(t *testing.T) {
	for name, mutate := range map[string]func(*fenceRig){
		"two voters":      func(r *fenceRig) { r.voters = 2 },
		"no other writer": func(r *fenceRig) { r.others = 0 },
		"not a writer":    func(r *fenceRig) { r.writer = false },
	} {
		t.Run(name, func(t *testing.T) {
			r := newFenceRig()
			mutate(r)
			r.now = r.now.Add(time.Minute) // long out of contact
			if r.fenced() {
				t.Fatal("fenced although no other writer could be promoted")
			}
		})
	}
}

func TestIngestFenceRefusesBeforeFirstContact(t *testing.T) {
	r := newFenceRig()
	r.contact = time.Time{}
	if !r.fenced() {
		t.Fatal("a writer that never heard from the leader accepts writes")
	}
}

// One heartbeat after a partition heals does not reopen ingest: contact must
// hold for exitHold first.
func TestIngestFenceHoldsUntilContactIsSteady(t *testing.T) {
	r := newFenceRig()
	r.now = r.now.Add(10 * time.Second)
	if !r.fenced() {
		t.Fatal("setup: not fenced")
	}
	r.contact = r.now
	if !r.fenced() {
		t.Fatal("reopened on the first heartbeat")
	}
	r.now = r.now.Add(time.Second)
	r.contact = r.now
	if !r.fenced() {
		t.Fatal("reopened before exitHold")
	}
	r.now = r.now.Add(1500 * time.Millisecond)
	r.contact = r.now
	if r.fenced() {
		t.Fatal("still fenced after steady contact for exitHold")
	}
}

func TestIngestFenceAfterStaysBelowFailoverDetection(t *testing.T) {
	log := zerolog.Nop()
	if got := ingestFenceAfter(5, 3, 0, time.Second, log); got != 7500*time.Millisecond {
		t.Fatalf("default fenceAfter = %v, want 7.5s", got)
	}
	if got := ingestFenceAfter(1, 2, 0, time.Second, log); got != 3*time.Second {
		t.Fatalf("floor: %v, want 3 election timeouts (3s)", got)
	}
	if got := ingestFenceAfter(5, 3, 60_000, time.Second, log); got != 15*time.Second {
		t.Fatalf("override not capped at detection time: %v", got)
	}
	if got := ingestFenceAfter(5, 3, 4_000, time.Second, log); got != 4*time.Second {
		t.Fatalf("override ignored: %v", got)
	}
}

// /ready/write follows the fence, so a load balancer stops routing to it.
func TestMayAcceptIngestFalseWhileFenced(t *testing.T) {
	r := newFenceRig()
	local := NewNode("w1", "w1", RoleWriter, "c")
	local.SetWriterState(WriterStatePrimary) // locally primary: ready unless fenced
	c := &Coordinator{cfg: &clusterConfigForFenceTest, localNode: local,
		writerFailoverMgr: &WriterFailoverManager{}, logger: zerolog.Nop()}
	c.ingestFence.Store(r.f)
	r.f.evaluate()
	if !c.MayAcceptIngest() {
		t.Fatal("setup: an unfenced primary is not ready")
	}
	r.now = r.now.Add(time.Minute) // isolated
	r.f.evaluate()
	if c.MayAcceptIngest() {
		t.Fatal("a fenced writer reports ready for writes")
	}
}

var clusterConfigForFenceTest = config.ClusterConfig{}

// Start holds c.mu for its whole body and starts the fence inside it. A fence
// start that takes c.mu itself deadlocks every writer at boot with automatic
// failover on — which the live rig, not a unit test, first showed.
func TestStartIngestFenceUnderTheCoordinatorLock(t *testing.T) {
	raftNode := startRaftNode(t, "w1", allocFreePort(t), true)
	t.Cleanup(func() { _ = raftNode.Stop() })
	c := &Coordinator{
		cfg:               &config.ClusterConfig{},
		localNode:         NewNode("w1", "w1", RoleWriter, "c"),
		raftNode:          raftNode,
		raftFSM:           raftNode.FSM(),
		writerFailoverMgr: &WriterFailoverManager{},
		autoFailover:      true,
		logger:            zerolog.Nop(),
	}
	c.ctx, c.cancel = context.WithCancel(context.Background())
	t.Cleanup(c.cancel)

	done := make(chan struct{})
	go func() {
		c.mu.Lock()
		c.startIngestFenceLocked()
		c.mu.Unlock()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("starting the ingest fence under c.mu deadlocked")
	}
	if c.IngestGate() == nil || c.ingestFence.Load() == nil {
		t.Fatal("fence not installed")
	}
}

// One heartbeat after a partition heals does not reopen ingest: contact must
// keep arriving through the hold.
func TestIngestFenceASingleHeartbeatDoesNotReopen(t *testing.T) {
	r := newFenceRig()
	r.now = r.now.Add(10 * time.Second)
	if !r.fenced() {
		t.Fatal("setup: not fenced")
	}
	r.contact = r.now         // one heartbeat
	for i := 0; i < 10; i++ { // 2.5 s of ticks with no further contact
		r.now = r.now.Add(250 * time.Millisecond)
		if !r.fenced() {
			t.Fatalf("reopened %v after a single heartbeat", time.Duration(i+1)*250*time.Millisecond)
		}
	}
}

// Contact alone does not mean this node applied a promotion made while it was
// away: it stays fenced until a manifest barrier completes.
func TestIngestFenceWaitsForTheCatchUpBarrier(t *testing.T) {
	r := newFenceRig()
	release := make(chan struct{})
	r.f.catchUp = func(ctx context.Context) error {
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	r.now = r.now.Add(10 * time.Second)
	if !r.fenced() {
		t.Fatal("setup: not fenced")
	}
	for i := 0; i < 20; i++ { // 5 s of steady contact, barrier still running
		r.now = r.now.Add(250 * time.Millisecond)
		r.contact = r.now
		if !r.fenced() {
			t.Fatal("reopened before this node caught up with the log")
		}
	}
	close(release)
	deadline := time.Now().Add(2 * time.Second)
	for {
		r.now = r.now.Add(250 * time.Millisecond)
		r.contact = r.now
		if !r.fenced() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("still fenced after the barrier completed")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// Without the failover licence there is no contact rule, but a writer that is
// not the designated primary (after a manual hand-over) still refuses writes.
func TestIngestFenceDesignationRuleNeedsNoLicence(t *testing.T) {
	r := newFenceRig()
	r.f.contactRule = false
	r.now = r.now.Add(time.Minute) // out of contact: irrelevant without the rule
	if r.fenced() {
		t.Fatal("contact rule applied without automatic failover")
	}
	r.primary = "w2"
	if !r.fenced() {
		t.Fatal("a writer that is not the designated primary accepts writes without the failover licence")
	}
}

// Through the real start path: without automatic failover (no licence) the
// fence is still installed for the designation rule.
func TestIngestFenceStartsWithoutAutomaticFailover(t *testing.T) {
	raftNode := startRaftNode(t, "w1", allocFreePort(t), true)
	t.Cleanup(func() { _ = raftNode.Stop() })
	c := &Coordinator{
		cfg:               &config.ClusterConfig{},
		localNode:         NewNode("w1", "w1", RoleWriter, "c"),
		raftNode:          raftNode,
		raftFSM:           raftNode.FSM(),
		writerFailoverMgr: &WriterFailoverManager{},
		autoFailover:      false,
		logger:            zerolog.Nop(),
	}
	c.ctx, c.cancel = context.WithCancel(context.Background())
	t.Cleanup(c.cancel)
	c.mu.Lock()
	c.startIngestFenceLocked()
	c.mu.Unlock()
	f := c.ingestFence.Load()
	if f == nil {
		t.Fatal("no fence without automatic failover: a demoted writer would accept writes")
	}
	if f.contactRule {
		t.Fatal("contact rule armed without automatic failover")
	}
}
