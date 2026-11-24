package cluster

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// fakeLeaseClock drives a readLease without a cluster.
type fakeLeaseClock struct {
	now      time.Time
	leader   bool
	leaderID string // the leader this node knows of, "" for none
	contact  time.Time
	changes  []bool
	syncErr  error
	syncs    int
	// onSync runs inside a sync (to change the world while it runs).
	onSync func()
}

func (f *fakeLeaseClock) lease() *readLease {
	l := newReadLease(func() bool { return f.leader }, func() time.Time { return f.contact }, func() string { return f.leaderID }, func(context.Context) error {
		f.syncs++
		if f.onSync != nil {
			f.onSync()
		}
		return f.syncErr
	})
	l.now = func() time.Time { return f.now }
	l.onChange = func(held bool, _ string) { f.changes = append(f.changes, held) }
	return l
}

func TestReadLease1825(t *testing.T) {
	ctx := context.Background()
	t0 := time.Unix(1_800_000_000, 0)

	t.Run("no reads before the first sync", func(t *testing.T) {
		f := &fakeLeaseClock{now: t0, leaderID: "A"}
		l := f.lease()
		if ok, why := l.Ready(); ok || !strings.Contains(why, "not yet synced") {
			t.Fatalf("before any step: %v %q", ok, why)
		}
		// No contact with a leader yet: no sync is tried.
		if l.step(ctx) || f.syncs != 0 {
			t.Fatalf("with no leader: held, or synced %d time(s)", f.syncs)
		}
		f.contact = t0.Add(-time.Second)
		if !l.step(ctx) || f.syncs != 1 {
			t.Fatalf("with a leader in contact: not held after a sync (%d)", f.syncs)
		}
		if ok, why := l.Ready(); !ok || why != "" {
			t.Fatalf("held: %v %q", ok, why)
		}
		// Held: no further sync while contact lasts.
		l.step(ctx)
		if f.syncs != 1 {
			t.Fatalf("synced %d times while the lease was held", f.syncs)
		}
	})

	t.Run("the leader itself syncs once and holds", func(t *testing.T) {
		f := &fakeLeaseClock{now: t0, leader: true}
		l := f.lease()
		if !l.step(ctx) || f.syncs != 1 {
			t.Fatal("a leader did not take the lease after its barrier")
		}
	})

	t.Run("lost contact ends the lease; the next contact syncs again", func(t *testing.T) {
		f := &fakeLeaseClock{now: t0, contact: t0, leaderID: "A"}
		l := f.lease()
		l.step(ctx)
		f.now = t0.Add(readLeaseTimeout - time.Millisecond)
		if !l.step(ctx) {
			t.Fatal("the lease ended before its timeout")
		}
		f.now = t0.Add(readLeaseTimeout)
		if l.step(ctx) {
			t.Fatal("the lease outlived lost contact")
		}
		if ok, why := l.Ready(); ok || !strings.Contains(why, "not heard from the cluster leader") {
			t.Fatalf("after lost contact: %v %q", ok, why)
		}
		f.contact = f.now
		if !l.step(ctx) || f.syncs != 2 {
			t.Fatalf("contact regained: held %v after %d sync(s); want a second sync first", l.held, f.syncs)
		}
	})

	t.Run("a failed sync keeps reads refused, with the reason", func(t *testing.T) {
		f := &fakeLeaseClock{now: t0, contact: t0, leaderID: "A", syncErr: errors.New("leader barrier: timeout")}
		l := f.lease()
		if l.step(ctx) {
			t.Fatal("held after a failed sync")
		}
		if ok, why := l.Ready(); ok || !strings.Contains(why, "leader barrier: timeout") {
			t.Fatalf("%v %q", ok, why)
		}
		// Not tried again on every tick: it backs off.
		f.syncErr = nil
		if l.step(ctx) || f.syncs != 1 {
			t.Fatalf("synced again %d time(s) at once after a failure", f.syncs-1)
		}
		f.now = f.now.Add(readLeaseRetryMax)
		f.contact = f.now
		if !l.step(ctx) || f.syncs != 2 {
			t.Fatal("not held after the pause and a sync that worked")
		}
	})

	t.Run("contact without a known leader is not contact", func(t *testing.T) {
		// Pre-vote off: nodes cut off from the leader grant each other
		// votes, and a granted vote counts as contact in hashicorp/raft.
		f := &fakeLeaseClock{now: t0, contact: t0}
		l := f.lease()
		if l.step(ctx) || f.syncs != 0 {
			t.Fatal("a node that never knew a leader took the lease")
		}
		f.leaderID = "A"
		if !l.step(ctx) {
			t.Fatal("not held with a known leader")
		}
		// An election: the leader unknown for a moment does not end it.
		f.leaderID = ""
		f.now, f.contact = t0.Add(readLeaseTimeout-time.Millisecond), t0.Add(readLeaseTimeout-time.Millisecond)
		if !l.step(ctx) {
			t.Fatal("a moment without a known leader ended the lease")
		}
		// Votes keep refreshing contact, but no leader for the whole
		// timeout: the lease ends.
		f.now, f.contact = t0.Add(readLeaseTimeout), t0.Add(readLeaseTimeout)
		if l.step(ctx) {
			t.Fatal("the lease outlived the leader being unknown for its whole timeout")
		}
	})

	t.Run("a new leader: the node syncs with it and keeps serving", func(t *testing.T) {
		f := &fakeLeaseClock{now: t0, contact: t0, leaderID: "A"}
		l := f.lease()
		l.step(ctx)
		f.leaderID = "B"
		f.syncErr = errors.New("not yet")
		if !l.step(ctx) || f.syncs != 2 {
			t.Fatalf("a change of leader stopped reads (syncs %d)", f.syncs)
		}
		f.syncErr = nil
		f.now = f.now.Add(readLeaseRetryMax)
		f.contact = f.now
		if !l.step(ctx) || f.syncs != 3 || l.leader != "B" {
			t.Fatalf("not synced with the new leader (%d syncs, leader %q)", f.syncs, l.leader)
		}
		l.step(ctx)
		if f.syncs != 3 {
			t.Fatal("synced again with the same leader")
		}
		if got := fmt.Sprint(f.changes); got != "[true]" {
			t.Fatalf("lease changes reported: %s", got)
		}
	})

	t.Run("contact lost while the sync ran: not held", func(t *testing.T) {
		f := &fakeLeaseClock{now: t0, contact: t0, leaderID: "A"}
		f.onSync = func() { f.now = t0.Add(readLeaseTimeout + time.Second) }
		l := f.lease()
		if l.step(ctx) {
			t.Fatal("a sync on a leader lost while it ran granted the lease")
		}
	})

	t.Run("nil lease: reads are not gated", func(t *testing.T) {
		var l *readLease
		if ok, _ := l.Ready(); !ok {
			t.Fatal("a nil lease refused reads")
		}
		var c *Coordinator
		if ok, _ := c.ReadReady(); !ok {
			t.Fatal("a nil coordinator refused reads")
		}
		if ok, _ := (&Coordinator{}).ReadReady(); !ok {
			t.Fatal("a coordinator with no lease refused reads")
		}
	})
}

// Through real Raft and the real forwarded barrier: a follower holds the
// lease after it synced, and loses it once the leader is gone for longer than
// the lease timeout.
func TestReadLease1825_RealRaft(t *testing.T) {
	addrs := allocFreePorts(t, 2)
	raftA := startRaftNodeInDir(t, "writer-A", addrs[0], filepath.Join(t.TempDir(), "writer-A"), true)
	stopped := atomic.Bool{}
	t.Cleanup(func() {
		if !stopped.Load() {
			_ = raftA.Stop()
		}
	})
	if err := raftA.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("WaitForLeader: %v", err)
	}
	raftB := startRaftNodeInDir(t, "reader-B", addrs[1], filepath.Join(t.TempDir(), "reader-B"), false)
	t.Cleanup(func() { _ = raftB.Stop() })
	if err := raftA.AddNonvoter("reader-B", addrs[1], 10*time.Second); err != nil {
		t.Fatalf("AddNonvoter: %v", err)
	}
	peerB := NewNode("reader-B", "reader-B", RoleReader, barrierTestCluster)
	leader := startLeaderCoordinatorServer(t, raftA, peerB)
	if err := raftA.AddNode(&raft.NodeInfo{ID: "writer-A", Role: string(RoleWriter), Address: leader.addr()}, 5*time.Second); err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	waitFor(t, 10*time.Second, func() bool {
		_, ok := raftB.FSM().GetNode("writer-A")
		return ok
	}, "reader-B never applied the leader's AddNode")
	follower := newFollowerCoordinator(t, "reader-B", raftB)
	l := newReadLease(raftB.IsLeader, raftB.LastContact, raftB.LeaderID, func(ctx context.Context) error {
		return follower.waitForManifestSync(ctx, raftB, readLeaseSyncTimeout)
	})
	ctx := context.Background()
	waitFor(t, 15*time.Second, func() bool { return l.step(ctx) }, "the follower never took the lease")

	if err := raftA.Stop(); err != nil {
		t.Fatalf("stop the leader: %v", err)
	}
	stopped.Store(true)
	start := time.Now()
	waitFor(t, readLeaseTimeout+5*time.Second, func() bool { return !l.step(ctx) }, "the follower kept the lease with its leader gone")
	if took := time.Since(start); took < readLeaseTimeout-time.Second {
		t.Fatalf("the lease ended %s after the leader went, sooner than the lease allows", took)
	}
	if ok, why := l.Ready(); ok || why == "" {
		t.Fatalf("after the leader went: %v %q", ok, why)
	}
}

// The lease applies only where nodes share one object store: on local
// storage, and with no Raft, startReadLease leaves reads ungated.
func TestStartReadLeaseOnlyOnASharedStore1825(t *testing.T) {
	local, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	c := &Coordinator{storage: local}
	c.startReadLease(context.Background())
	if c.readLease != nil {
		t.Fatal("a lease was started with no Raft")
	}
	addrs := allocFreePorts(t, 1)
	rn := startRaftNodeInDir(t, "writer-A", addrs[0], filepath.Join(t.TempDir(), "writer-A"), true)
	t.Cleanup(func() { _ = rn.Stop() })
	c = &Coordinator{storage: local, raftNode: rn}
	c.startReadLease(context.Background())
	if c.readLease != nil {
		t.Fatal("a lease was started on local storage")
	}
	if ok, _ := c.ReadReady(); !ok {
		t.Fatal("reads gated on local storage")
	}
}

// On a shared object store the lease is started, and refuses reads until the
// node has synced: then the leader of a one-node cluster takes it.
func TestStartReadLeaseOnASharedStore1852(t *testing.T) {
	addrs := allocFreePorts(t, 1)
	rn := startRaftNodeInDir(t, "writer-A", addrs[0], filepath.Join(t.TempDir(), "writer-A"), true)
	t.Cleanup(func() { _ = rn.Stop() })
	if err := rn.WaitForLeader(10 * time.Second); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	c := &Coordinator{storage: &storage.GCSBackend{}, raftNode: rn, logger: zerolog.Nop()}
	c.startReadLease(ctx)
	if c.readLease == nil {
		t.Fatal("no lease on a shared object store")
	}
	waitFor(t, 10*time.Second, func() bool { ok, _ := c.ReadReady(); return ok }, "the leader never took its read lease")
	st := c.readLease
	if ok, why := st.Ready(); !ok || why != "" {
		t.Fatalf("%v %q", ok, why)
	}
}

// A drop while a sync runs: the sync grants nothing, and the next step syncs
// again (its hook reopens the gate the drop closed).
func TestReadLeaseDropDuringSync1852(t *testing.T) {
	var l *readLease
	syncs := 0
	l = newReadLease(func() bool { return true }, func() time.Time { return time.Now() }, func() string { return "self" },
		func(context.Context) error {
			syncs++
			if syncs == 1 {
				l.drop("waived")
			}
			return nil
		})
	if l.step(context.Background()) {
		t.Fatal("granted by a sync a drop overlapped")
	}
	if !l.step(context.Background()) || syncs != 2 {
		t.Fatalf("not granted on the next sync (syncs %d)", syncs)
	}
}
