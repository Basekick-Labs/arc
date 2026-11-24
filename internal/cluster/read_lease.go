package cluster

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Reads on a cluster whose nodes share one object store (#1825, #1852).
//
// Every node reads the same bucket, and what a read leaves out of it comes
// from the node's Raft state: the inputs of a compaction whose output is in
// place (open_compactions.go), a file a DELETE replaced, the old and new files
// of a re-home job. A node whose state is behind the cluster's leaves out the
// wrong files, and reads rows twice or not at all:
//
//   - at start, a node applies its local snapshot and serves before the log
//     after it has been replayed. A graceful restart has also removed it from
//     the node table, so nothing in the cluster waits for it meanwhile;
//   - a node cut off from the leader keeps serving with whatever state it
//     had, for as long as the partition lasts, even if it is removed from
//     the cluster.
//
// So a node of such a cluster serves reads only while it holds a read lease:
// it has synced with the leader (a forwarded Raft barrier, or the leader's own
// barrier) since it last lost contact, and it has heard from the leader, and
// known who it is, within readLeaseTimeout. Losing contact for longer ends the
// lease; a sync comes before reads resume. A new leader is synced with too,
// while the node keeps serving: it was in contact throughout.
//
// A read that passed the gate before the lease ended: on a node wired for
// re-home (every node of a cluster sharing an object store), losing the lease
// cancels it, and any read that binds before the lease is taken again
// (SetReadLeaseHooks, readgate.CancelAll). Elsewhere it runs on with the
// exclusions it read, for as long as the query runs. What the lease does not
// bound: a node in contact may still be applying entries the leader has
// committed (the sync is a barrier when the lease is taken, not on every
// read). A job that must know a lost node has stopped reading waits
// readLeaseTimeout plus the leader's own step-down time (LeaderLeaseTimeout)
// plus readLeaseTick, and the time a cancelled read takes to stop.
//
// A cluster on local storage replicates files instead, and has its own gate
// (cluster.query_gate_on_catchup); standalone nodes have neither.

// readLeaseTimeout is how long a node keeps serving reads without hearing from
// the Raft leader. A leader heartbeats every HeartbeatTimeout/10 (50 ms at the
// default 500 ms), and an election at the default timings takes a second or
// two; this is long enough that one does not stop reads on every node, and
// short enough that a job waiting on a lost node is not held long. Raft
// timings raised far above their defaults make every failover a read outage.
const readLeaseTimeout = 5 * time.Second

// readLeaseTick is how often the lease is checked.
const readLeaseTick = 250 * time.Millisecond

// readLeaseSyncTimeout bounds one sync attempt.
const readLeaseSyncTimeout = 10 * time.Second

// readLease is the lease state. Its inputs are functions so that tests can
// drive it without a cluster.
type readLease struct {
	isLeader    func() bool
	lastContact func() time.Time
	sync        func(ctx context.Context) error
	now         func() time.Time

	leaderID func() string
	// onChange is told when the lease is granted or lost (for the log).
	onChange func(held bool, why string)

	mu   sync.Mutex
	held bool
	why  string
	// leader is the leader the lease was taken under: a new one ends it.
	leader string
	// nextSync is when a sync may be tried again after one failed.
	nextSync time.Time
	failures int

	// knownLeader is the last leader this node knew of, and when it last
	// knew it. Used by inContact, on the lease's own goroutine only.
	knownLeader   string
	knownLeaderAt time.Time
}

func newReadLease(isLeader func() bool, lastContact func() time.Time, leaderID func() string, sync func(ctx context.Context) error) *readLease {
	return &readLease{
		isLeader: isLeader, lastContact: lastContact, leaderID: leaderID, sync: sync, now: time.Now,
		why: "this node has not yet synced its cluster state with the leader since it started",
	}
}

// Ready reports whether this node may serve reads now, and why not.
func (l *readLease) Ready() (bool, string) {
	if l == nil {
		return true, ""
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.held, l.why
}

// inContact reports whether the leader is reachable from this node now, and
// which node it is: this node leads, or it has heard from the leader within
// readLeaseTimeout and has known who the leader is within readLeaseTimeout.
// Known, and not contact alone: with pre-vote off, nodes that cannot reach
// the leader grant each other votes, and a granted vote also counts as
// contact in hashicorp/raft, while a candidate's leader is unknown. A leader
// briefly unknown during an election does not end the lease.
func (l *readLease) inContact() (string, bool) {
	now := l.now()
	if l.isLeader() {
		l.knownLeader, l.knownLeaderAt = "self", now
		return "self", true
	}
	if id := l.leaderID(); id != "" {
		l.knownLeader, l.knownLeaderAt = id, now
	}
	if l.knownLeader == "" || now.Sub(l.knownLeaderAt) >= readLeaseTimeout {
		return l.knownLeader, false
	}
	last := l.lastContact()
	return l.knownLeader, !last.IsZero() && now.Sub(last) < readLeaseTimeout
}

// readLeaseRetryMax caps the pause between failed syncs.
const readLeaseRetryMax = 5 * time.Second

// step checks the lease once: drops it when contact is lost or the leader
// returns whether the lease is held afterwards.
// drop ends the lease now, as losing contact would: the next step syncs
// with the leader again before it is retaken (#1852: the leader waived this
// node's read fence).
func (l *readLease) drop(why string) {
	if l == nil {
		return
	}
	l.mu.Lock()
	was := l.held
	l.held, l.why = false, why
	l.mu.Unlock()
	if was {
		l.changed(false, why)
	}
}

func (l *readLease) step(ctx context.Context) bool {
	leader, contact := l.inContact()
	l.mu.Lock()
	held := l.held
	if held && !contact {
		l.held = false
		l.why = "this node has not heard from the cluster leader for " + readLeaseTimeout.String() + "; its cluster state may be behind"
	}
	lost := held && !l.held
	// A new leader: the node syncs with it, and keeps serving meanwhile.
	// It was in contact all along, and a failover should not stop reads.
	resync := held && !lost && leader != l.leader
	why := l.why
	wait := !l.nextSync.IsZero() && l.now().Before(l.nextSync)
	l.mu.Unlock()
	if lost {
		l.changed(false, why)
	}
	if (held && !lost && !resync) || !contact || wait {
		return held && !lost
	}
	sctx, cancel := context.WithTimeout(ctx, readLeaseSyncTimeout)
	err := l.sync(sctx)
	cancel()
	if err != nil {
		l.mu.Lock()
		if !l.held {
			l.why = "this node could not sync its cluster state with the leader: " + err.Error()
		}
		l.failures++
		pause := readLeaseTick << min(l.failures, 5)
		l.nextSync = l.now().Add(min(pause, readLeaseRetryMax))
		held = l.held
		l.mu.Unlock()
		return held
	}
	// The same leader, still in contact: a sync that succeeded on a leader
	// that was deposed while it ran is no proof of anything after it.
	if after, ok := l.inContact(); !ok || after != leader {
		l.mu.Lock()
		held = l.held
		l.mu.Unlock()
		return held
	}
	l.mu.Lock()
	granted := !l.held
	l.held, l.why, l.leader, l.failures, l.nextSync = true, "", leader, 0, time.Time{}
	l.mu.Unlock()
	if granted {
		l.changed(true, "")
	}
	return true
}

func (l *readLease) changed(held bool, why string) {
	if l.onChange != nil {
		l.onChange(held, why)
	}
}

// run checks the lease every readLeaseTick until ctx ends.
func (l *readLease) run(ctx context.Context) {
	t := time.NewTicker(readLeaseTick)
	defer t.Stop()
	for {
		l.step(ctx)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// runSyncedHook runs the synced hook until ctx ends. One run at a time: a
// run that outlived its sync is waited for by the next.
func (c *Coordinator) runSyncedHook(ctx context.Context, fn func()) error {
	c.syncedHookMu.Lock()
	prev := c.syncedHookDone
	if prev == nil {
		prev = make(chan struct{})
		close(prev)
	}
	c.syncedHookMu.Unlock()
	select {
	case <-prev:
	case <-ctx.Done():
		return fmt.Errorf("dropping the query caches before taking the read lease: %w", ctx.Err())
	}
	done := make(chan struct{})
	c.syncedHookMu.Lock()
	c.syncedHookDone = done
	c.syncedHookMu.Unlock()
	go func() {
		defer close(done)
		fn()
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("dropping the query caches before taking the read lease: %w", ctx.Err())
	}
}

// SetReadLeaseHooks sets what runs when this node loses its read lease
// (lost: stop the reads in flight) and each time it has synced with the
// leader, before it takes the lease (synced: drop its query caches). Both
// must be quick. Call before Start.
func (c *Coordinator) SetReadLeaseHooks(lost, synced func()) {
	if c == nil {
		return
	}
	if lost != nil {
		c.onReadLeaseLost.Store(&lost)
	}
	if synced != nil {
		c.onReadSynced.Store(&synced)
	}
}

// ReadReady reports whether this node may serve reads, and if not, why
// (read_lease.go). Always true where the lease does not apply: no Raft, or
// storage that is not a shared object store.
func (c *Coordinator) ReadReady() (bool, string) {
	if c == nil {
		return true, ""
	}
	return c.readLease.Ready()
}

// startReadLease starts the lease loop on a node that needs one. Call after
// Raft has started.
func (c *Coordinator) startReadLease(ctx context.Context) {
	if c.raftNode == nil || !IsSharedObjectStore(c.storage) {
		return
	}
	rn := c.raftNode
	l := newReadLease(rn.IsLeader, rn.LastContact, rn.LeaderID, func(ctx context.Context) error {
		if err := c.waitForManifestSync(ctx, rn, readLeaseSyncTimeout); err != nil {
			return err
		}
		// Before the lease is taken: caches filled while this node was
		// behind or cut off may name files the cluster has removed since
		// (#1825). Bounded by the sync's deadline: the hook may wait for a
		// pooled connection, and the lease loop must not stall on it.
		if fn := c.onReadSynced.Load(); fn != nil {
			return c.runSyncedHook(ctx, *fn)
		}
		return nil
	})
	l.onChange = func(held bool, why string) {
		if held {
			c.logger.Info().Msg("Read lease taken: this node's cluster state is synced with the leader, and it serves reads")
			return
		}
		c.logger.Warn().Str("why", why).Msg("Read lease lost: this node refuses reads (503) until it has synced with the cluster leader again")
		// And stops the reads in flight: a job that waits for this node
		// once it is removed relies on it reading nothing within a fixed
		// time of losing the leader (#1825).
		if fn := c.onReadLeaseLost.Load(); fn != nil {
			(*fn)()
		}
	}
	c.readLease = l
	go l.run(ctx)
}
