package cluster

// The cluster-wide compaction pause a cluster restore takes (#1087).
//
// Compaction commits in two Raft phases on the compactor's watcher ticks: the
// output is registered at output_written, the inputs are manifest-deleted a
// tick later at sources_deleted, after the subprocess has already deleted them
// from storage. A restore that snapshots the manifest between the two phases
// races the job: a replace restore deletes the output and re-registers the
// inputs, and phase 2 then manifest-deletes the restored inputs on every node.
// The only safe shape is no job in flight across the restore, so the restoring
// primary proposes a pause through Raft, every node stops at its next batch
// boundary, lets what is running finish, drains its pending phase-2 commits,
// and acks; the restore starts once every node in the FSM node table has
// acked, refreshes the pause while it runs, and resumes it at the end.
//
// The pause carries a TTL from the PROPOSER's clock, so a requester that dies
// mid-restore releases compaction within compactionPauseTTL with no operator
// action, and the FSM never reads a clock (Apply must be deterministic).

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/raft"
)

const (
	// compactionPauseTTL is how long a pause stays in force after the last
	// refresh the requester managed to commit. Expiry is judged on each
	// reading node by ITS clock against the requester's ExpiresAt, so the
	// difference between the TTL and compactionPauseRefreshInterval is the
	// clock skew the pause tolerates: a node whose clock runs further AHEAD
	// of the requester than that would see the pause lapse for a moment
	// before each refresh lands, and a tick in that moment would start a
	// batch the requester cannot see. The cluster refuses messages from
	// clocks more than security.HMACTimestampTolerance (5 min) apart, so
	// any node able to talk to the cluster is within 5 min of the requester;
	// 6 min minus the 30 s refresh leaves 5.5 min of tolerance, above that
	// bound. A node whose clock runs BEHIND sees the pause last longer, the
	// safe side. The cost is how long compaction stays paused after a
	// requester dies without resuming: up to six minutes.
	compactionPauseTTL = 6 * time.Minute
	// compactionPauseAckTimeout bounds the wait for every node to ack. A
	// node acks only once its in-flight batch has finished and its pending
	// phase-2 commits have drained, and a batch of max_files_per_batch
	// files can take minutes.
	compactionPauseAckTimeout = 10 * time.Minute
	compactionPauseAckPoll    = 250 * time.Millisecond
	// compactionPauseQuiesceTimeout bounds how long a node tries to quiesce
	// and ack one pause generation. Matches the requester's wait.
	compactionPauseQuiesceTimeout = 10 * time.Minute
	// A proposal refused only because no leader is known or reachable (an
	// election in progress) is retried for this long, as the backup adapter
	// retries its manifest batches.
	compactionPauseProposeBudget = 15 * time.Second
	compactionPauseProposeDelay  = 250 * time.Millisecond
	// compactionPauseAckRetryDelay spaces the ack attempts a node makes while
	// its pause generation stays in force; the ack loop runs for the quiesce
	// budget, not the propose budget, because a node that cannot reach a
	// leader for 15 s is exactly the node the restore is waiting for.
	compactionPauseAckRetryDelay = 1 * time.Second
	// compactionPauseResumeTimeout bounds the resume a failed pause attempt
	// issues on its way out, and the one a handle issues after a restore
	// whose own context may already be done.
	compactionPauseResumeTimeout = 30 * time.Second
)

// compactionPauseRefreshInterval is how often a held pause is refreshed. A
// variable only so the refresher test can shorten it; production never
// changes it.
var compactionPauseRefreshInterval = 30 * time.Second

// CompactionPaused reports whether a cluster-wide compaction pause is in force
// as read on THIS node: the FSM state says active and, by the local clock, it
// has not expired. The compaction manager consults it before every batch and
// the scheduler before every tick. False without a Raft manifest.
func (c *Coordinator) CompactionPaused() bool {
	if c.raftFSM == nil {
		return false
	}
	return compactionPauseInForce(c.raftFSM.CompactionPauseBrief(), time.Now())
}

func compactionPauseInForce(s raft.CompactionPauseState, now time.Time) bool {
	return s.Active && now.Before(s.ExpiresAt)
}

// CompactionPauseReason describes the pause in force for a log line or an
// error, or returns "" when compaction is not paused. The compaction scheduler
// reads it through its optional pauseReporter interface.
func (c *Coordinator) CompactionPauseReason() string {
	if c.raftFSM == nil {
		return ""
	}
	s := c.raftFSM.CompactionPauseBrief()
	if !compactionPauseInForce(s, time.Now()) {
		return ""
	}
	return fmt.Sprintf("%s (requested by %s, generation %d, expires %s)",
		s.Reason, s.RequestedBy, s.Generation, s.ExpiresAt.UTC().Format(time.RFC3339))
}

// CompactionPauseStatus is the compaction_pause key of Status(): whether a
// pause is in force, who holds it, until when, and which nodes have acked it.
// A record that is active but past its expiry is reported with expired=true:
// its requester stopped refreshing and the next pause takes it over.
func (c *Coordinator) CompactionPauseStatus() map[string]interface{} {
	out := map[string]interface{}{"active": false}
	if c.raftFSM == nil {
		return out
	}
	s := c.raftFSM.GetCompactionPause()
	out["generation"] = s.Generation
	if !s.Active {
		return out
	}
	inForce := compactionPauseInForce(s, time.Now())
	out["active"] = inForce
	out["expired"] = !inForce
	out["requested_by"] = s.RequestedBy
	out["reason"] = s.Reason
	out["requested_at"] = s.RequestedAt
	out["expires_at"] = s.ExpiresAt
	acks := make([]string, 0, len(s.Acks))
	for node := range s.Acks {
		acks = append(acks, node)
	}
	sort.Strings(acks)
	out["acks"] = acks
	// Who the requester is still waiting for, so an operator can see what
	// blocks a restore before its 10-minute timeout names them.
	pending := c.compactionPauseMissingAcks(s)
	if pending == nil {
		pending = []string{}
	}
	out["pending_acks"] = pending
	return out
}

// SetCompactionQuiescer registers main.go's hook: fn returns once this node
// has no compaction batch in flight and no phase-2 commit pending, or an
// error when the pause ended first or ctx ran out. Nil means nothing to
// quiesce (a node without compaction): the node acks at once. Set BEFORE
// Start, because a pause replayed from the Raft log fires the callback inside
// Start and a node with no hook yet would ack without having quiesced.
func (c *Coordinator) SetCompactionQuiescer(fn func(ctx context.Context) error) {
	c.mu.Lock()
	c.compactionQuiescer = fn
	c.mu.Unlock()
}

// CompactionPauseHandle is a pause this node holds. Resume releases it; Lost
// reports whether it stopped being this node's pause while held.
type CompactionPauseHandle struct {
	c           *Coordinator
	generation  uint64
	requestedBy string
	reason      string

	stop     chan struct{}
	done     chan struct{}
	stopOnce sync.Once

	mu        sync.Mutex
	expiresAt time.Time // ExpiresAt of the last proposal that committed
	lost      bool
	lostErr   error
	resumed   bool

	// afterPropose, when set, runs after every refresh proposal returns and
	// before the lapse check. A testing hook (nil in production, read under
	// mu) that lets a test make a refresh commit late.
	afterPropose func()
}

// Generation is the pause generation this handle holds.
func (h *CompactionPauseHandle) Generation() uint64 { return h.generation }

// PauseCompaction pauses compaction cluster-wide for reason and returns once
// every node in the FSM node table has acknowledged it (see
// compactionPauseWaitSet for who is waited for), with a refresher running
// that keeps the pause in force until Resume. Fails when another node holds a
// pause that has not expired, when the cluster has no Raft manifest, when a
// leader cannot be reached, or when a node does not ack within
// compactionPauseAckTimeout; in the last case the pause is resumed on the way
// out, best effort.
func (c *Coordinator) PauseCompaction(ctx context.Context, reason string) (*CompactionPauseHandle, error) {
	if c.raftNode == nil || c.raftFSM == nil {
		return nil, errors.New("compaction pause: this coordinator has no Raft file manifest (cluster.raft_data_dir unset)")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	requester := c.LocalNodeID()
	if requester == "" {
		return nil, errors.New("compaction pause: this node has no cluster node ID")
	}

	// The requester is routinely a Raft follower whose FSM trails the
	// leader; the generation CAS below needs the current value.
	if err := c.SyncManifest(ctx); err != nil {
		return nil, fmt.Errorf("compaction pause: could not sync the cluster state before reading it: %w", err)
	}

	var generation uint64
	var expiresAt time.Time
	for attempt := 0; ; attempt++ {
		cur := c.raftFSM.GetCompactionPause()
		generation = cur.Generation + 1
		now := time.Now()
		expiresAt = now.Add(compactionPauseTTL)
		cmd, err := raft.CompactionPauseCommand(raft.SetCompactionPausePayload{
			Paused:      true,
			Generation:  generation,
			RequestedBy: requester,
			Reason:      reason,
			Now:         now,
			ExpiresAt:   expiresAt,
		})
		if err != nil {
			return nil, err
		}
		err = c.proposeCompactionPause(ctx, cmd)
		if err == nil {
			break
		}
		// A generation conflict means this FSM was behind after all, or a
		// pause landed between the read and the proposal: re-sync and retry
		// once. Anything else, a live pause held by another node included,
		// is final.
		if attempt == 0 && raft.IsCompactionPauseConflict(err) {
			if serr := c.SyncManifest(ctx); serr != nil {
				return nil, fmt.Errorf("compaction pause: %w (re-sync failed too: %v)", err, serr)
			}
			continue
		}
		return nil, fmt.Errorf("compaction pause: %w", err)
	}
	c.logger.Info().
		Uint64("generation", generation).
		Str("reason", reason).
		Time("expires_at", expiresAt).
		Msg("Cluster-wide compaction pause proposed; waiting for every node to quiesce")

	h := &CompactionPauseHandle{
		c:           c,
		generation:  generation,
		requestedBy: requester,
		reason:      reason,
		stop:        make(chan struct{}),
		done:        make(chan struct{}),
		expiresAt:   expiresAt,
	}
	go h.refreshLoop()

	if err := c.waitCompactionQuiesced(ctx, generation); err != nil {
		rctx, cancel := context.WithTimeout(context.Background(), compactionPauseResumeTimeout)
		if rerr := h.Resume(rctx); rerr != nil {
			c.logger.Warn().Err(rerr).Uint64("generation", generation).Msg("Could not resume the compaction pause after the quiesce wait failed; it expires on its own within the TTL")
		}
		cancel()
		return nil, err
	}
	c.logger.Info().Uint64("generation", generation).Msg("Every cluster node acknowledged the compaction pause")
	return h, nil
}

// waitCompactionQuiesced polls the FSM until every node in the wait set has
// acked generation, or the pause stops being that generation, or the wait
// times out. The timeout error names the nodes that did not ack.
//
// The proposal returned once the LEADER applied it; on a follower (the
// Pattern 1 primary) this node's own FSM can trail it by a few milliseconds,
// so a local generation still below the one proposed is "not applied here
// yet", not "gone". Only a later generation, or this one inactive, is.
func (c *Coordinator) waitCompactionQuiesced(ctx context.Context, generation uint64) error {
	waitCtx, cancel := context.WithTimeout(ctx, compactionPauseAckTimeout)
	defer cancel()
	ticker := time.NewTicker(compactionPauseAckPoll)
	defer ticker.Stop()
	for {
		s := c.raftFSM.GetCompactionPause()
		var missing []string
		switch {
		case s.Generation < generation:
			missing = []string{c.LocalNodeID() + " (this node has not applied the pause yet)"}
		case !s.Active || s.Generation != generation:
			return fmt.Errorf("compaction pause generation %d is no longer in force (active %v, current generation %d)", generation, s.Active, s.Generation)
		default:
			missing = c.compactionPauseMissingAcks(s)
		}
		if len(missing) == 0 {
			return nil
		}
		select {
		case <-waitCtx.Done():
			if ctx.Err() != nil {
				return fmt.Errorf("wait for the compaction pause acks of %s cancelled: %w", strings.Join(missing, ", "), ctx.Err())
			}
			return fmt.Errorf("%d cluster nodes (%s) did not acknowledge the compaction pause within %s: a compaction batch may still be running on them, they hold compaction commits their completion watcher is not applying (check their logs for pending completion manifests), or they are not on 27.01.1", len(missing), strings.Join(missing, ", "), compactionPauseAckTimeout)
		case <-ticker.C:
		}
	}
}

// compactionPauseMissingAcks lists the wait-set nodes whose ack for the
// state's generation is not in s.Acks, sorted.
func (c *Coordinator) compactionPauseMissingAcks(s raft.CompactionPauseState) []string {
	registryState := func(id string) (NodeState, bool) {
		if c.registry == nil {
			return "", false
		}
		n, ok := c.registry.Get(id)
		if !ok {
			return "", false
		}
		return n.GetState(), true
	}
	var missing []string
	for _, id := range compactionPauseWaitSet(c.raftFSM.GetAllNodes(), registryState, c.GetActiveCompactorID(), c.LocalNodeID()) {
		if s.Acks[id] != s.Generation {
			missing = append(missing, id)
		}
	}
	return missing
}

// compactionPauseWaitSet is the set of node IDs whose ack a pause waits for,
// sorted. The FSM node table is authoritative: every node in it is waited for
// EXCEPT one the local registry POSITIVELY marks unhealthy or dead. A node
// the registry does not know (the registry is empty after a restart until
// peers heartbeat) is waited for, fail closed. The compactor lease holder is
// never skipped, and when no lease is assigned neither is any node whose role
// CanCompact (compactor, and standalone, which the compaction gate lets
// compact while no lease is assigned): those are the nodes that run
// compaction. The local node is always in the set, since the FSM table may
// not list it yet on a fresh leader.
func compactionPauseWaitSet(fsmNodes []*raft.NodeInfo, registryState func(id string) (NodeState, bool), activeCompactorID, localID string) []string {
	set := make(map[string]struct{}, len(fsmNodes)+1)
	if localID != "" {
		set[localID] = struct{}{}
	}
	for _, n := range fsmNodes {
		if n == nil || n.ID == "" {
			continue
		}
		mustWait := n.ID == localID || n.ID == activeCompactorID ||
			(activeCompactorID == "" && ParseRole(n.Role).GetCapabilities().CanCompact)
		if !mustWait && registryState != nil {
			if state, known := registryState(n.ID); known && (state == StateUnhealthy || state == StateDead) {
				continue
			}
		}
		set[n.ID] = struct{}{}
	}
	ids := make([]string, 0, len(set))
	for id := range set {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// refreshCommittedInTime reports whether a refresh that committed at
// committedAt kept the pause continuously in force: it did only if the
// previous ExpiresAt had not yet passed when the commit landed. A refresh
// that commits later revives a pause that had already lapsed on every node
// (their gates opened for the gap, a tick may have started a batch), so the
// requester must treat the pause as lost even though the FSM now reads as
// healthy again.
func refreshCommittedInTime(lastExpiry, committedAt time.Time) bool {
	return committedAt.Before(lastExpiry)
}

// refreshLoop keeps the pause in force: every compactionPauseRefreshInterval
// it proposes the same generation with a fresh ExpiresAt. A conflict means
// another requester took over an EXPIRED pause while this one ran, or
// resumed it: the handle is marked lost and the loop ends. Any other failure
// (no leader, a timeout) is retried next tick; if the last committed
// ExpiresAt passes meanwhile, or passes before a slow refresh commits, the
// handle is marked lost too, because compaction has resumed on every node by
// then.
func (h *CompactionPauseHandle) refreshLoop() {
	defer close(h.done)
	ticker := time.NewTicker(compactionPauseRefreshInterval)
	defer ticker.Stop()
	for {
		select {
		case <-h.stop:
			return
		case <-ticker.C:
		}
		if stop := h.refreshOnce(); stop {
			return
		}
	}
}

// refreshOnce is one refresh tick: it proposes the same generation with a
// fresh ExpiresAt and reports whether the loop should stop (the handle is
// lost). Split from the loop so a test can drive a tick directly.
func (h *CompactionPauseHandle) refreshOnce() (stop bool) {
	now := time.Now()
	h.mu.Lock()
	lastExpiry := h.expiresAt
	hook := h.afterPropose
	h.mu.Unlock()
	if !now.Before(lastExpiry) {
		h.markLost(now, fmt.Errorf("the pause expired at %s before a refresh could be committed (no leader reachable since the last one)", lastExpiry.UTC().Format(time.RFC3339)))
		return true
	}
	expiresAt := now.Add(compactionPauseTTL)
	cmd, err := raft.CompactionPauseCommand(raft.SetCompactionPausePayload{
		Paused:      true,
		Generation:  h.generation,
		RequestedBy: h.requestedBy,
		Reason:      h.reason,
		Now:         now,
		ExpiresAt:   expiresAt,
	})
	if err != nil {
		h.markLost(now, err)
		return true
	}
	ctx, cancel := context.WithTimeout(h.c.ctxOrBackground(), compactionPauseRefreshInterval)
	err = h.c.proposeCompactionPause(ctx, cmd)
	cancel()
	if hook != nil {
		hook()
	}
	if err == nil {
		committedAt := time.Now()
		if !refreshCommittedInTime(lastExpiry, committedAt) {
			h.markLost(committedAt, fmt.Errorf("the pause lapsed at %s before the refresh committed at %s; compaction may have run in the gap", lastExpiry.UTC().Format(time.RFC3339), committedAt.UTC().Format(time.RFC3339)))
			h.c.logger.Error().Uint64("generation", h.generation).Time("lapsed_at", lastExpiry).Time("refresh_committed_at", committedAt).Msg("A compaction pause refresh committed after the pause had lapsed on every node; the operation holding it must treat it as lost")
			return true
		}
		h.mu.Lock()
		h.expiresAt = expiresAt
		h.mu.Unlock()
		return false
	}
	if raft.IsCompactionPauseConflict(err) {
		h.markLost(time.Now(), err)
		h.c.logger.Error().Err(err).Uint64("generation", h.generation).Msg("The cluster-wide compaction pause was taken over or resumed by another node while this node held it; a compaction job may now race the operation that took it")
		return true
	}
	h.c.logger.Warn().Err(err).Uint64("generation", h.generation).Time("in_force_until", lastExpiry).Msg("Could not refresh the cluster-wide compaction pause; retrying at the next interval")
	return false
}

func (h *CompactionPauseHandle) markLost(at time.Time, err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.lost {
		return
	}
	h.lost = true
	h.lostErr = fmt.Errorf("lost at %s: %w", at.UTC().Format(time.RFC3339), err)
}

// Lost reports whether this pause stopped being in force for this node while
// held: the refresher hit a conflict or could not refresh before the expiry,
// or the FSM as read on this node now shows another generation, another
// requester, no pause, or one past its expiry by the local clock. A caller
// that depends on the pause (a restore between two register batches) checks
// it and aborts, because a compaction job may have run meanwhile.
func (h *CompactionPauseHandle) Lost() (bool, error) {
	h.mu.Lock()
	if h.lost {
		err := h.lostErr
		h.mu.Unlock()
		return true, err
	}
	h.mu.Unlock()
	if h.c.raftFSM == nil {
		return true, errors.New("no Raft manifest")
	}
	s := h.c.raftFSM.CompactionPauseBrief()
	now := time.Now()
	switch {
	case !s.Active:
		h.markLost(now, fmt.Errorf("the pause (generation %d) was resumed", h.generation))
	case s.Generation != h.generation || s.RequestedBy != h.requestedBy:
		h.markLost(now, fmt.Errorf("generation %d requested by %s replaced this generation %d", s.Generation, s.RequestedBy, h.generation))
	case !now.Before(s.ExpiresAt):
		h.markLost(now, fmt.Errorf("the pause expired at %s by this node clock", s.ExpiresAt.UTC().Format(time.RFC3339)))
	default:
		return false, nil
	}
	h.mu.Lock()
	err := h.lostErr
	h.mu.Unlock()
	return true, err
}

// Resume stops the refresher and releases the pause. Idempotent. When the
// handle is lost the release is still attempted, best effort: the record
// may still be this generation (it expired with no leader reachable) and
// clearing it is correct, while a stale generation is simply refused by the
// FSM; neither outcome is an error for the caller.
func (h *CompactionPauseHandle) Resume(ctx context.Context) error {
	h.stopOnce.Do(func() { close(h.stop) })
	<-h.done
	h.mu.Lock()
	if h.resumed {
		h.mu.Unlock()
		return nil
	}
	h.resumed = true
	lost := h.lost
	h.mu.Unlock()
	if ctx == nil {
		ctx = context.Background()
	}
	cmd, err := raft.CompactionPauseCommand(raft.SetCompactionPausePayload{
		Paused:      false,
		Generation:  h.generation,
		RequestedBy: h.requestedBy,
		Reason:      h.reason,
	})
	if err != nil {
		return err
	}
	if err := h.c.proposeCompactionPause(ctx, cmd); err != nil {
		if lost {
			h.c.logger.Debug().Err(err).Uint64("generation", h.generation).Msg("Resume of a lost compaction pause refused; nothing to release")
			return nil
		}
		return fmt.Errorf("compaction pause resume: %w", err)
	}
	h.c.logger.Info().Uint64("generation", h.generation).Str("reason", h.reason).Msg("Cluster-wide compaction pause released")
	return nil
}

// proposeCompactionPause applies cmd on the leader or forwards it, retrying
// for compactionPauseProposeBudget while the only failure is that no leader
// is known or reachable.
func (c *Coordinator) proposeCompactionPause(ctx context.Context, cmd *raft.Command) error {
	deadline := time.Now().Add(compactionPauseProposeBudget)
	for {
		err := c.applyOrForward(ctx, cmd)
		if err == nil || !isTransientLeaderError(err) {
			return err
		}
		if ctx.Err() != nil || !time.Now().Before(deadline) {
			return err
		}
		timer := time.NewTimer(compactionPauseProposeDelay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return err
		case <-timer.C:
		}
	}
}

// applyOrForward applies a command through the local Raft node when it is the
// leader and forwards it to the leader otherwise, inside the coordinator's
// lifecycle context as the manifest writes do.
func (c *Coordinator) applyOrForward(ctx context.Context, cmd *raft.Command) error {
	if c.raftNode == nil {
		return errors.New("raft not initialized")
	}
	ctx, cancel := c.manifestContext(ctx)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return err
	}
	if c.raftNode.IsLeader() {
		return c.raftNode.Apply(cmd, manifestApplyTimeout(ctx))
	}
	return c.forwardApplyToLeader(ctx, cmd)
}

// onCompactionPauseChanged is the FSM callback. It runs on the FSM goroutine
// (inside raft.NewRaft during a startup restore, with the coordinator and
// Raft node locks held by the caller), so it only spawns the quiesce.
func (c *Coordinator) onCompactionPauseChanged(paused bool, generation uint64) {
	if !paused {
		c.logger.Info().Uint64("generation", generation).Msg("Cluster-wide compaction pause ended on this node")
		return
	}
	c.logger.Info().Uint64("generation", generation).Msg("Cluster-wide compaction pause applied on this node; quiescing compaction")
	go c.quiesceAndAck(generation)
}

// quiesceAndAck runs the quiescer for one pause generation, then acks it,
// retrying the ack while the generation stays in force. Idempotent: a node
// that already acked (a replay) returns at once, and single-flight per
// generation: a second caller while one is running for the same generation
// returns at once too. Both the FSM callback (log replay inside Start) and
// ackCompactionPauseIfActive (right after Start) can start one for the same
// generation before the FSM has recorded the first ack; without the gate a
// restarted node ran the quiescer twice and proposed the ack twice.
func (c *Coordinator) quiesceAndAck(generation uint64) {
	if c.raftNode == nil || c.raftFSM == nil {
		return
	}
	c.mu.Lock()
	if _, inFlight := c.compactionPauseAckInFlight[generation]; inFlight {
		c.mu.Unlock()
		c.logger.Debug().Uint64("generation", generation).Msg("Compaction pause quiesce already in flight for this generation")
		return
	}
	if c.compactionPauseAckInFlight == nil {
		c.compactionPauseAckInFlight = make(map[uint64]struct{})
	}
	c.compactionPauseAckInFlight[generation] = struct{}{}
	quiescer := c.compactionQuiescer
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		delete(c.compactionPauseAckInFlight, generation)
		c.mu.Unlock()
	}()

	ctx, cancel := context.WithTimeout(c.ctxOrBackground(), compactionPauseQuiesceTimeout)
	defer cancel()

	nodeID := c.LocalNodeID()
	stillThisGeneration := func() bool {
		s := c.raftFSM.GetCompactionPause()
		if !s.Active || s.Generation != generation {
			return false
		}
		return s.Acks[nodeID] != generation
	}
	if !stillThisGeneration() {
		return
	}
	if quiescer != nil {
		if err := quiescer(ctx); err != nil {
			c.logger.Warn().Err(err).Uint64("generation", generation).Msg("Compaction did not quiesce for the cluster-wide pause; not acknowledging it")
			return
		}
	}
	cmd, err := raft.CompactionPauseAckCommand(raft.AckCompactionPausePayload{NodeID: nodeID, Generation: generation})
	if err != nil {
		c.logger.Error().Err(err).Msg("Could not build the compaction pause ack")
		return
	}
	for stillThisGeneration() {
		if err := c.applyOrForward(ctx, cmd); err == nil {
			// The FSM ignores an ack whose generation has moved meanwhile,
			// so this reports the proposal, not its recording.
			c.logger.Info().Uint64("generation", generation).Msg("Proposed the acknowledgement of the cluster-wide compaction pause: no compaction batch in flight and no phase-2 commit pending on this node")
			return
		} else {
			c.logger.Warn().Err(err).Uint64("generation", generation).Msg("Could not propose the compaction pause ack; retrying while the pause is in force")
		}
		timer := time.NewTimer(compactionPauseAckRetryDelay)
		select {
		case <-ctx.Done():
			timer.Stop()
			c.logger.Error().Uint64("generation", generation).Msg("Gave up acknowledging the cluster-wide compaction pause; the requester will time out naming this node")
			return
		case <-timer.C:
		}
	}
}

// ackCompactionPauseIfActive runs from Start: a pause restored from the local
// snapshot at startup, or applied before the quiescer was reachable, fires no
// callback this process acts on, so once a leader is known the active pause
// is quiesced and acked. Idempotent with the callback path.
func (c *Coordinator) ackCompactionPauseIfActive() {
	if c.raftNode == nil || c.raftFSM == nil {
		return
	}
	for i := 0; i < 300; i++ {
		select {
		case <-c.stopCh:
			return
		default:
		}
		if c.raftNode.LeaderID() != "" {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	s := c.raftFSM.CompactionPauseBrief()
	if !compactionPauseInForce(s, time.Now()) {
		return
	}
	c.quiesceAndAck(s.Generation)
}

// compactionPauseCommandSpeaksFor checks that a forwarded pause command names
// the node that signed the request: a Set must carry requested_by == the
// requester, an Ack node_id == the requester. Otherwise one authenticated
// peer could ack, or pause, on behalf of another.
func compactionPauseCommandSpeaksFor(cmd *raft.Command, requester string) error {
	switch cmd.Type {
	case raft.CommandSetCompactionPause:
		var p raft.SetCompactionPausePayload
		if err := json.Unmarshal(cmd.Payload, &p); err != nil {
			return fmt.Errorf("decode compaction pause payload: %w", err)
		}
		if p.RequestedBy != requester {
			return errors.New("requested_by names another node")
		}
	case raft.CommandAckCompactionPause:
		var p raft.AckCompactionPausePayload
		if err := json.Unmarshal(cmd.Payload, &p); err != nil {
			return fmt.Errorf("decode compaction pause ack payload: %w", err)
		}
		if p.NodeID != requester {
			return errors.New("node_id names another node")
		}
	}
	return nil
}
