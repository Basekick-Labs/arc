package cluster

// CompactorFailoverManager monitors the active compactor's health and
// automatically reassigns the compactor lease to another healthy node
// when the current holder becomes unhealthy. Only the Raft leader runs
// the check loop — all other nodes observe the resulting
// CommandAssignCompactor via the FSM callback.
//
// The compactor lease is tracked in the FSM's activeCompactorID field,
// separate from the operator-configured NodeRole. This means:
//   - RoleCompactor nodes are preferred for the initial claim
//   - On failover, a RoleWriter can hold the lease without restarting
//   - A writer holding the lease hands it back once a dedicated
//     RoleCompactor node is healthy and has stayed healthy for
//     PreemptThreshold consecutive checks (#876)
//
// That last rule is newer than the rest of this file. The lease used to be
// stable once assigned, full stop, which sounded like conservatism and was
// really a dead end: a compactor pod that joined even one tick after the
// leader's first assignment never got the lease for the life of the cluster,
// and there was no way to move it by hand either. Stability is still the
// default — nothing moves a lease between two writers, or off a dedicated
// compactor — but a cluster that was given a compactor now uses it.
//
// Pattern mirrors WriterFailoverManager in writer_failover.go.

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	arcRaft "github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/rs/zerolog"
)

// CompactorFailoverConfig holds configuration for the compactor failover manager.
type CompactorFailoverConfig struct {
	// Registry provides access to cluster node state.
	Registry *Registry

	// RaftNode for applying AssignCompactor commands via consensus.
	RaftNode *arcRaft.Node

	// RaftFSM to read the current activeCompactorID.
	RaftFSM *arcRaft.ClusterFSM

	// CheckInterval is how often the leader checks compactor health.
	// Default: 10s (compaction is less latency-sensitive than writes).
	CheckInterval time.Duration

	// FailoverTimeout bounds the Raft Apply for AssignCompactor.
	FailoverTimeout time.Duration

	// CooldownPeriod prevents repeated failovers within this window.
	CooldownPeriod time.Duration

	// UnhealthyThreshold is consecutive unhealthy checks before triggering
	// failover. At 10s interval, threshold=3 means ~30s detection time.
	UnhealthyThreshold int

	// PreemptThreshold is consecutive checks during which a dedicated
	// RoleCompactor node must be healthy, while a non-compactor holds the
	// lease, before the lease is handed to it (#876).
	//
	// Deliberately LARGER than UnhealthyThreshold, and not a symmetry
	// oversight: this counter's only job is to damp a flapping compactor,
	// and nothing is broken while it counts — a writer is compacting
	// perfectly well. Compaction runs hourly by default, so a slower
	// hand-over costs nothing, while an eager one risks interrupting a
	// cycle. Do not "fix" the two thresholds to match.
	//
	// Note it is NOT a guard against preempting mid-cycle; no threshold
	// value provides that, because the leader cannot see whether the
	// holder is inside RunCompactionCycleForTiers.
	PreemptThreshold int

	// PreemptCooldown bounds how often the lease may be preempted, and
	// doubles as the lifetime of an operator override: AssignTo stamps it,
	// so a manual assignment to a writer is not undone by preemption for
	// this long.
	//
	// Separate from CooldownPeriod on purpose. triggerFailoverLocked reads
	// CooldownPeriod before a HEALTH failover, so a preemption that stamped
	// it would delay recovery from a genuinely dead compactor — a
	// hand-over of a healthy lease must never do that.
	PreemptCooldown time.Duration

	Logger zerolog.Logger
}

// CompactorFailoverManager monitors compactor health and reassigns the
// compactor lease on failure. Only active on the Raft leader.
type CompactorFailoverManager struct {
	cfg      *CompactorFailoverConfig
	mu       sync.RWMutex
	logger   zerolog.Logger
	ctx      context.Context
	cancelFn context.CancelFunc
	running  atomic.Bool
	wg       sync.WaitGroup

	// State tracking
	consecutiveFails int
	failoverInProg   bool
	lastFailoverAt   time.Time

	// Preemption tracking, deliberately separate from the two fields above.
	//
	// Sharing consecutiveFails would corrupt it. Take a flapping holder W
	// while a healthy dedicated compactor C exists: tick 1 W unhealthy -> 1,
	// tick 2 W healthy and C available -> 2, tick 3 W unhealthy -> 3, and the
	// HEALTH branch fires on a holder that was unhealthy for two ticks out of
	// three. That breaks the contract written on UnhealthyThreshold
	// ("consecutive unhealthy checks") and makes which of two quite different
	// actions fires depend on which branch happened to land on the threshold.
	preemptSustain int
	lastPreemptAt  time.Time

	// Callbacks
	onFailoverStart    func(oldCompactorID, newCompactorID string)
	onFailoverComplete func(newCompactorID string, success bool)
}

// NewCompactorFailoverManager creates a new compactor failover manager.
func NewCompactorFailoverManager(cfg *CompactorFailoverConfig) *CompactorFailoverManager {
	if cfg.CheckInterval == 0 {
		cfg.CheckInterval = 10 * time.Second
	}
	if cfg.FailoverTimeout == 0 {
		cfg.FailoverTimeout = 30 * time.Second
	}
	if cfg.CooldownPeriod == 0 {
		cfg.CooldownPeriod = 60 * time.Second
	}
	if cfg.UnhealthyThreshold == 0 {
		cfg.UnhealthyThreshold = 3
	}
	if cfg.PreemptThreshold == 0 {
		cfg.PreemptThreshold = 6
	}
	if cfg.PreemptCooldown == 0 {
		// Derived from CooldownPeriod rather than hardcoded, so an operator
		// who raises cluster.failover_cooldown raises both. A constant here
		// would silently ignore that setting. This is the ONLY place the rule
		// lives; the coordinator passes CooldownPeriod and nothing else.
		//
		// Note CooldownPeriod has already been defaulted above, so
		// cluster.failover_cooldown=0 lands here as 60s and yields 600s. That
		// is "0 means default", inherited from CooldownPeriod's own handling,
		// and it is stated in the release notes because it is not obvious.
		cfg.PreemptCooldown = 10 * cfg.CooldownPeriod
	}
	if cfg.PreemptCooldown < 0 {
		// A negative cluster.failover_cooldown is not validated anywhere, and
		// a negative duration here would make time.Since always exceed it —
		// i.e. silently no suppression at all, including of an operator
		// override. Treat it as none, explicitly, rather than by accident.
		cfg.PreemptCooldown = 0
	}

	return &CompactorFailoverManager{
		cfg:    cfg,
		logger: cfg.Logger.With().Str("component", "compactor-failover").Logger(),
	}
}

// SetCallbacks sets callbacks for failover events.
func (m *CompactorFailoverManager) SetCallbacks(
	onStart func(oldCompactorID, newCompactorID string),
	onComplete func(newCompactorID string, success bool),
) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.onFailoverStart = onStart
	m.onFailoverComplete = onComplete
}

// Start begins the compactor failover manager.
func (m *CompactorFailoverManager) Start(ctx context.Context) error {
	m.ctx, m.cancelFn = context.WithCancel(ctx)
	m.running.Store(true)

	m.wg.Add(1)
	go m.checkLoop()

	m.logger.Info().
		Dur("check_interval", m.cfg.CheckInterval).
		Dur("cooldown_period", m.cfg.CooldownPeriod).
		Int("unhealthy_threshold", m.cfg.UnhealthyThreshold).
		Msg("Compactor failover manager started")

	return nil
}

// Stop gracefully shuts down the compactor failover manager.
func (m *CompactorFailoverManager) Stop() error {
	if !m.running.Load() {
		return nil
	}

	m.running.Store(false)
	m.cancelFn()
	m.wg.Wait()

	m.logger.Info().Msg("Compactor failover manager stopped")
	return nil
}

// checkLoop periodically checks the active compactor's health.
func (m *CompactorFailoverManager) checkLoop() {
	defer m.wg.Done()

	ticker := time.NewTicker(m.cfg.CheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			m.checkCompactorHealth()
		}
	}
}

// checkCompactorHealth verifies the active compactor is still healthy.
// Only the Raft leader acts — other nodes observe the FSM callback.
func (m *CompactorFailoverManager) checkCompactorHealth() {
	// Only the Raft leader coordinates failover.
	if m.cfg.RaftNode == nil || !m.cfg.RaftNode.IsLeader() {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// While a lease change is applying, this tick does nothing at all — it
	// does not even count an unhealthy holder. A holder that dies during a
	// Raft apply therefore costs up to FailoverTimeout before the unhealthy
	// count starts. Pre-existing for failover and initial assignment;
	// preemption adds one more occasion for it. AssignTo answers 409 over the
	// same window, which is why the endpoint is documented as retryable.
	if m.failoverInProg {
		return
	}

	activeID := m.cfg.RaftFSM.GetActiveCompactorID()

	// No active compactor — try to assign one.
	if activeID == "" {
		m.tryInitialAssignment()
		return
	}

	// Check if the active compactor is still healthy.
	node, ok := m.cfg.Registry.Get(activeID)
	if !ok || node.GetState() != StateHealthy {
		m.consecutiveFails++
		m.logger.Warn().
			Str("compactor_id", activeID).
			Int("consecutive_fails", m.consecutiveFails).
			Int("threshold", m.cfg.UnhealthyThreshold).
			Msg("Active compactor unhealthy")

		if m.consecutiveFails >= m.cfg.UnhealthyThreshold {
			m.triggerFailoverLocked(activeID)
		}
		return
	}

	// Compactor is healthy — reset counter.
	m.consecutiveFails = 0

	// #876: healthy is not the same as right. A writer that took the lease
	// because it was the only candidate at assignment time used to keep it
	// for the life of the cluster, so a compactor pod that joined one tick
	// late — or was invisible until the #870 chart fix — sat idle with its
	// own CPU budget while a writer compacted on top of ingest.
	m.considerPreemptionLocked(activeID, node.Role)
}

// considerPreemptionLocked decides whether to hand a healthy lease from a
// non-compactor to a dedicated compactor. Must hold m.mu.
//
// Nothing here is a failure: the holder is healthy and compacting correctly.
// That is why it has its own counter and its own cooldown, and why it must
// never touch the health-failover ones.
func (m *CompactorFailoverManager) considerPreemptionLocked(activeID string, holderRole NodeRole) {
	// A dedicated compactor already holds it. This is the steady state, and
	// it is also what stops two compactors passing the lease back and forth:
	// once the lease lands on one, this returns on every subsequent tick.
	if holderRole == RoleCompactor {
		m.preemptSustain = 0
		return
	}

	candidate := m.selectDedicatedCompactor(activeID)
	if candidate == "" {
		// No dedicated compactor exists. This is every cluster that never
		// deployed one, on every tick, so it must stay a cheap no-op.
		m.preemptSustain = 0
		return
	}

	if m.preemptSustain < m.cfg.PreemptThreshold {
		// Clamped at the threshold rather than left to grow: while the
		// cooldown blocks, this would otherwise climb to ~60 against a
		// threshold of 6 over a 600s window and read as a bug in Stats().
		// Behaviour is identical — the test below is >=, and the counter is
		// only ever reset, never decremented.
		m.preemptSustain++
	}
	if m.preemptSustain < m.cfg.PreemptThreshold {
		return
	}

	// The cooldown is also what makes an operator override stick: AssignTo
	// stamps it, so a deliberate assignment to a writer is not reversed on
	// the next sustained window.
	if !m.lastPreemptAt.IsZero() && time.Since(m.lastPreemptAt) < m.cfg.PreemptCooldown {
		m.logger.Debug().
			Str("holder", activeID).
			Str("candidate", candidate).
			Dur("cooldown_remaining", m.cfg.PreemptCooldown-time.Since(m.lastPreemptAt)).
			Msg("Compactor lease preemption held off — cooldown active")
		return
	}

	m.failoverInProg = true
	m.preemptSustain = 0

	m.logger.Info().
		Str("old_compactor", activeID).
		Str("new_compactor", candidate).
		Str("holder_role", string(holderRole)).
		Int("sustained_checks", m.cfg.PreemptThreshold).
		Msg("Handing the compactor lease to a dedicated compactor node")

	// Asynchronous on purpose: checkCompactorHealth holds m.mu for its whole
	// body, and a Raft Apply can block for FailoverTimeout. Doing this inline
	// would stall the ticker and Stats() for up to 30s under the write lock.
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		err := m.cfg.RaftNode.AssignCompactor(candidate, activeID, m.cfg.FailoverTimeout)
		if err != nil {
			m.logger.Error().Err(err).
				Str("new_compactor", candidate).
				Msg("Failed to hand the compactor lease to the dedicated compactor")
		} else {
			m.mu.Lock()
			m.lastPreemptAt = time.Now()
			m.mu.Unlock()
		}
		// completeAssignment, not completeFailover: see its doc comment.
		m.completeAssignment(err == nil)
	}()
}

// tryInitialAssignment attempts to assign a compactor when none is active.
// Runs the Raft Apply asynchronously to avoid blocking the check loop.
func (m *CompactorFailoverManager) tryInitialAssignment() {
	newID := m.selectNewCompactor("")
	if newID == "" {
		return
	}

	m.failoverInProg = true

	m.logger.Info().
		Str("node_id", newID).
		Msg("Assigning initial compactor lease")

	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		err := m.cfg.RaftNode.AssignCompactor(newID, "", m.cfg.FailoverTimeout)
		if err != nil {
			m.logger.Error().Err(err).
				Str("node_id", newID).
				Msg("Failed to assign initial compactor")
		}
		// completeAssignment, not completeFailover: an initial assignment is
		// not a failover and lost nothing, so there is nothing to back off
		// from. Routing it through completeFailover armed the health-failover
		// cooldown on every cluster start, which then swallowed a genuine
		// compactor death for the whole window. The writer manager split
		// exactly this out as completeElection, for exactly this reason
		// (writer_failover.go).
		m.completeAssignment(err == nil)
	}()
}

// triggerFailoverLocked initiates failover (must hold m.mu).
func (m *CompactorFailoverManager) triggerFailoverLocked(oldCompactorID string) {
	// Check cooldown.
	if !m.lastFailoverAt.IsZero() && time.Since(m.lastFailoverAt) < m.cfg.CooldownPeriod {
		m.logger.Warn().
			Dur("cooldown_remaining", m.cfg.CooldownPeriod-time.Since(m.lastFailoverAt)).
			Msg("Compactor failover skipped — cooldown period active")
		return
	}

	m.failoverInProg = true

	m.logger.Info().
		Str("old_compactor", oldCompactorID).
		Msg("Initiating compactor failover")

	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		m.executeFailover(oldCompactorID)
	}()
}

// executeFailover performs the actual failover operation.
func (m *CompactorFailoverManager) executeFailover(oldCompactorID string) {
	newCompactorID := m.selectNewCompactor(oldCompactorID)
	if newCompactorID == "" {
		m.logger.Error().
			Str("old_compactor", oldCompactorID).
			Msg("No healthy node available for compactor failover")
		m.completeFailover("", false)
		return
	}

	// Notify callback.
	m.mu.RLock()
	startCb := m.onFailoverStart
	m.mu.RUnlock()
	if startCb != nil {
		startCb(oldCompactorID, newCompactorID)
	}

	m.logger.Info().
		Str("old_compactor", oldCompactorID).
		Str("new_compactor", newCompactorID).
		Msg("Assigning compactor lease to new node")

	if err := m.cfg.RaftNode.AssignCompactor(newCompactorID, oldCompactorID, m.cfg.FailoverTimeout); err != nil {
		m.logger.Error().Err(err).
			Str("new_compactor", newCompactorID).
			Msg("Failed to assign compactor via Raft")
		m.completeFailover(newCompactorID, false)
		return
	}

	m.logger.Info().
		Str("new_compactor", newCompactorID).
		Str("old_compactor", oldCompactorID).
		Msg("Compactor failover completed successfully")

	m.completeFailover(newCompactorID, true)
}

// canHoldCompactorLease reports whether a role may be given the compactor
// lease at all.
//
// This is the single definition, and the selectors below are written to call
// it rather than to re-list roles, so the automatic and manual paths cannot
// drift. That matters because of an invariant: a lease must only ever land
// somewhere an automatic failover could also place it. Otherwise losing that
// node leaves the lease on a corpse that nothing can move, and — since
// compactionClusterGate switches to lease mode as soon as the lease is
// non-empty — compaction stops cluster-wide with no recovery.
//
// RoleStandalone is excluded for exactly that reason, found on a live rig: a
// cluster whose nodes never set cluster.role is all-standalone, no automatic
// path can pick any of them, and accepting a manual assignment there was a
// one-way door. (Such a cluster has a separate, pre-existing problem — every
// node runs compaction, because standalone's static capability allows it —
// filed as #892. Not this lever's to fix.)
//
// Do NOT reach for RoleCapabilities.CanCompact either: it is FALSE for
// RoleWriter ("compaction runs on dedicated nodes", role.go), which describes
// where compaction is meant to run, not who may hold the lease. Failover has
// always fallen back to a writer, and must, or a cluster whose only compactor
// dies stops compacting entirely.
func canHoldCompactorLease(role NodeRole) bool {
	return role == RoleCompactor || role == RoleWriter
}

// selectNewCompactor picks the best node to receive the compactor lease.
// Priority: healthy RoleCompactor nodes (other than the failed one), then
// healthy RoleWriter nodes. Readers are excluded because they typically
// don't have write access to shared storage.
//
// Candidates are taken in node-ID order. The registry returns them in Go map
// order, which is randomised per range, so without the min-scan two equally
// good candidates would be chosen arbitrarily and a retried apply could land
// somewhere other than the attempt before it.
//
// The fallback is expressed through canHoldCompactorLease rather than by
// naming RoleWriter, so this and the manual endpoint are provably the same
// set. TestEveryAssignableRoleIsAlsoSelectable pins that.
func (m *CompactorFailoverManager) selectNewCompactor(excludeNodeID string) string {
	// Prefer nodes that were deployed as compactors.
	if id := m.selectDedicatedCompactor(excludeNodeID); id != "" {
		return id
	}
	// Then anything else the lease may sit on.
	return lowestHealthyNodeID(m.cfg.Registry.GetHealthy(), excludeNodeID, func(r NodeRole) bool {
		return r != RoleCompactor && canHoldCompactorLease(r)
	})
}

// selectDedicatedCompactor returns the lowest-ID healthy RoleCompactor node,
// excluding one node.
//
// Deliberately narrower than selectNewCompactor, which falls back to writers.
// That fallback is right for failover — anyone is better than nobody — and
// wrong for preemption, where moving a healthy lease from one writer to
// another achieves nothing and costs an interrupted cycle. The caller checks
// "is a dedicated compactor available" and then asks for one; those are two
// separate registry reads, so this must not be able to answer with a writer
// if the compactor disappeared in between.
func (m *CompactorFailoverManager) selectDedicatedCompactor(excludeNodeID string) string {
	return lowestHealthyNodeID(m.cfg.Registry.GetCompactors(), excludeNodeID, nil)
}

// lowestHealthyNodeID returns the smallest node ID among healthy candidates
// that satisfy roleOK (nil means any role), or "" if there are none.
//
// The GetState check is redundant with GetCompactors/GetHealthy, which filter
// on IsHealthy today. It is kept so this function is correct for any node
// slice, including one from a helper that stops filtering.
func lowestHealthyNodeID(nodes []*Node, excludeNodeID string, roleOK func(NodeRole) bool) string {
	best := ""
	for _, node := range nodes {
		if node.ID == excludeNodeID || node.GetState() != StateHealthy {
			continue
		}
		if roleOK != nil && !roleOK(node.Role) {
			continue
		}
		if best == "" || node.ID < best {
			best = node.ID
		}
	}
	return best
}

// completeAssignment finishes an assignment that is NOT a failover: the
// initial claim, and a preemption toward a dedicated compactor.
//
// It deliberately does not stamp lastFailoverAt. triggerFailoverLocked reads
// that field before a health failover, so arming it here would make a
// compactor that dies shortly after an ordinary cluster start — or shortly
// after the lease moved to it — wait out a cooldown that exists to damp
// flapping failovers. Nothing failed and nothing was lost, so there is
// nothing to back off from.
//
// It does not invoke onFailoverComplete either: that callback announces a
// failover, and no failover happened.
func (m *CompactorFailoverManager) completeAssignment(success bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failoverInProg = false
	if success {
		m.consecutiveFails = 0
	}
}

// completeFailover marks failover as complete.
func (m *CompactorFailoverManager) completeFailover(newCompactorID string, success bool) {
	m.mu.Lock()
	m.failoverInProg = false
	if success {
		m.lastFailoverAt = time.Now()
		m.consecutiveFails = 0
	}
	completeCb := m.onFailoverComplete
	m.mu.Unlock()

	if completeCb != nil {
		completeCb(newCompactorID, success)
	}
}

// TriggerManualFailover triggers a manual compactor failover.
func (m *CompactorFailoverManager) TriggerManualFailover() error {
	m.mu.Lock()

	if m.failoverInProg {
		m.mu.Unlock()
		return fmt.Errorf("compactor failover already in progress")
	}

	activeID := m.cfg.RaftFSM.GetActiveCompactorID()
	if activeID == "" {
		m.mu.Unlock()
		return fmt.Errorf("no active compactor to failover from")
	}

	m.failoverInProg = true
	m.mu.Unlock()

	m.logger.Info().
		Str("current_compactor", activeID).
		Msg("Manual compactor failover initiated")

	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		m.executeFailover(activeID)
	}()

	return nil
}

// AssignTo hands the compactor lease to a named node at an operator's
// request. Returns the node that held it before.
//
// The target is explicit and required, which is the opposite of the writer
// hand-over endpoint (#872): there, clearing the designation is what lets the
// cluster's own election run, and naming a successor would be redundant.
// Here the automatic choice landing in the wrong place IS the problem being
// fixed, so "move it, you pick" would reproduce what the operator is
// overriding.
//
// This is also why the endpoint routes through the manager rather than
// calling RaftNode.AssignCompactor directly: failoverInProg is what keeps a
// manual assignment from racing an automatic one that has already chosen a
// different target.
func (m *CompactorFailoverManager) AssignTo(nodeID string) (oldCompactorID string, err error) {
	if m.cfg.RaftNode == nil {
		return "", fmt.Errorf("clustering is not configured with Raft")
	}
	if !m.cfg.RaftNode.IsLeader() {
		return "", ErrNotLeaderForTopology
	}

	node, ok := m.cfg.Registry.Get(nodeID)
	if !ok {
		return "", fmt.Errorf("%w: %q", ErrNodeNotFound, nodeID)
	}
	if !canHoldCompactorLease(node.Role) {
		return "", fmt.Errorf("%w: %q has role %q", ErrCannotHoldCompactorLease, nodeID, node.Role)
	}
	if node.GetState() != StateHealthy {
		return "", fmt.Errorf("%w: %q is %s", ErrNodeNotHealthy, nodeID, node.GetState())
	}

	m.mu.Lock()
	if m.failoverInProg {
		m.mu.Unlock()
		return "", ErrCompactorFailoverInProgress
	}
	current := m.cfg.RaftFSM.GetActiveCompactorID()
	if current == nodeID {
		m.mu.Unlock()
		return "", fmt.Errorf("%w: %q", ErrAlreadyCompactorLeaseHolder, nodeID)
	}
	m.failoverInProg = true
	m.mu.Unlock()

	// Deferred, not called at each exit. This runs on the Fiber handler
	// goroutine, and Fiber recovers handler panics — so a panic inside the
	// Raft apply would leave failoverInProg set forever, wedging every
	// automatic compactor failover on the leader with no log line and no way
	// back short of a restart.
	applyErr := error(nil)
	defer func() { m.completeAssignment(applyErr == nil) }()

	m.logger.Info().
		Str("old_compactor", current).
		Str("new_compactor", nodeID).
		Msg("Compactor lease assigned at an operator request")

	applyErr = m.cfg.RaftNode.AssignCompactor(nodeID, current, m.cfg.FailoverTimeout)
	if applyErr == nil {
		// Stamp the preempt cooldown, not the failover cooldown. Without
		// this, an operator who deliberately moves the lease onto a writer
		// has preemption take it straight back at the next sustained window
		// — the endpoint would advertise an action the same binary reverses
		// a minute later. The override lasts PreemptCooldown; the handler
		// reports that duration so nobody has to guess.
		m.mu.Lock()
		m.lastPreemptAt = time.Now()
		m.preemptSustain = 0
		m.mu.Unlock()
	}
	if applyErr != nil {
		return "", fmt.Errorf("failed to assign the compactor lease to %s: %w", nodeID, applyErr)
	}
	return current, nil
}

// PreemptStatus reports progress toward handing the lease to a dedicated
// compactor, for the cluster status endpoint.
func (m *CompactorFailoverManager) PreemptStatus() map[string]interface{} {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := map[string]interface{}{
		"sustained_checks": m.preemptSustain,
		"required_checks":  m.cfg.PreemptThreshold,
		"cooldown_seconds": int(m.cfg.PreemptCooldown.Seconds()),
	}
	if !m.lastPreemptAt.IsZero() {
		out["last_change_at"] = m.lastPreemptAt.Format(time.RFC3339)
		if remaining := m.cfg.PreemptCooldown - time.Since(m.lastPreemptAt); remaining > 0 {
			// The single most useful number when the lease is somewhere an
			// operator did not expect: how long until it may move again.
			out["cooldown_remaining_seconds"] = int(remaining.Seconds())
		}
	}
	return out
}

// PreemptCooldown reports how long an operator assignment suppresses
// automatic preemption, so callers can tell the operator.
func (m *CompactorFailoverManager) PreemptCooldown() time.Duration {
	return m.cfg.PreemptCooldown
}

// Stats returns failover manager statistics.
func (m *CompactorFailoverManager) Stats() map[string]interface{} {
	m.mu.RLock()
	defer m.mu.RUnlock()

	stats := map[string]interface{}{
		"running":              m.running.Load(),
		"active_compactor_id":  m.cfg.RaftFSM.GetActiveCompactorID(),
		"consecutive_fails":    m.consecutiveFails,
		"failover_in_progress": m.failoverInProg,
		"cooldown_period":      m.cfg.CooldownPeriod.String(),
		"preempt_sustain":      m.preemptSustain,
		"preempt_threshold":    m.cfg.PreemptThreshold,
		"preempt_cooldown":     m.cfg.PreemptCooldown.String(),
	}
	if !m.lastPreemptAt.IsZero() {
		stats["last_preempt_at"] = m.lastPreemptAt.Format(time.RFC3339)
	}

	if !m.lastFailoverAt.IsZero() {
		stats["last_failover_at"] = m.lastFailoverAt.Format(time.RFC3339)
	}

	return stats
}
