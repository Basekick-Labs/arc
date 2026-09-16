package cluster

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/rs/zerolog"
)

// compactorWarnInterval is the minimum duration between "no compactor
// elected" or "multiple compactors elected" warnings. The health loop
// ticks on checkInterval (default 5s), but we only want the warning to
// surface once per minute so SRE dashboards don't drown in duplicates.
const compactorWarnInterval = 60 * time.Second

// writersForHA is the number of writer-role nodes Arc's clustering docs and
// both Helm charts call for. Below it a cluster still runs, but it has no
// spare writer, which is what the warning below says out loud (#856).
const writersForHA = 3

// writerRedundancyGracePeriod suppresses the writer-redundancy warning for
// the first minute of a node's life. Peers join over the seconds after
// start, so without it every cluster warns on every boot.
const writerRedundancyGracePeriod = 60 * time.Second

// writerRedundancyMode selects which deployment pattern the writer-redundancy
// warning describes. The two patterns lose writer availability for different
// reasons, so they get different remediation text.
type writerRedundancyMode int32

const (
	// writerRedundancyOff disables the check. This is the zero value, so OSS
	// and non-cluster deployments never run it.
	writerRedundancyOff writerRedundancyMode = iota
	// writerRedundancyFailover is Pattern 1: per-node storage, one primary
	// writer, failover by Raft promotion. The promotion pool is writer-role
	// nodes only.
	writerRedundancyFailover
	// writerRedundancyLoadBalanced is Pattern 2: shared object storage, N
	// equivalent writers behind a load balancer. Raft promotion is
	// deliberately suppressed here, so redundancy is purely a matter of how
	// many writer backends the load balancer has.
	writerRedundancyLoadBalanced
)

// HealthChecker performs periodic health checks on cluster nodes.
// It monitors node heartbeats and updates node state based on check results.
type HealthChecker struct {
	registry           *Registry
	checkInterval      time.Duration
	checkTimeout       time.Duration
	unhealthyThreshold int

	// Phase 4: compactor-election warning. When WarnIfNoCompactor is true,
	// each tick of checkAllNodes also runs checkCompactorElected, which
	// surfaces rate-limited Warn logs when the cluster has zero or more
	// than one node in RoleCompactor. The flag is set from main.go based
	// on cfg.Cluster.Enabled && cfg.Cluster.ReplicationEnabled && cfg.Compaction.Enabled.
	// nil compaction or non-cluster deployments leave this false and
	// short-circuit past the check.
	//
	// Rate limiting uses SEPARATE timers for the two misconfiguration
	// modes so a cluster flapping between "0 compactors" and "2 compactors"
	// surfaces both warnings within the same minute instead of suppressing
	// whichever one lost the CAS race. Each timer is independently
	// throttled to compactorWarnInterval.
	warnIfNoCompactor        bool
	lastNoCompactorWarnAt    atomic.Int64 // unix nanos; throttles "no compactor elected"
	lastMultiCompactorWarnAt atomic.Int64 // unix nanos; throttles "multiple compactors elected"

	// Phase 5: raftFSM is set when compactor failover is configured. When
	// non-nil, checkCompactorElected also checks the FSM's activeCompactorID
	// to suppress the "no compactor" warning when failover has assigned
	// the lease to a non-RoleCompactor node (e.g. a writer after failover).
	raftFSM *raft.ClusterFSM

	// writerRedundancy holds a writerRedundancyMode. When it is not
	// writerRedundancyOff, each tick also runs checkWriterRedundancy, which
	// warns when the cluster has fewer than writersForHA writer-role nodes.
	// Readers are never promotion candidates, so writer-role nodes are the
	// entire redundancy pool in both patterns (#856).
	//
	// Atomic because the coordinator arms it from the goroutine that builds
	// the coordinator while the health loop reads it on its own goroutine.
	writerRedundancy           atomic.Int32
	lastWriterRedundancyWarnAt atomic.Int64

	// startedAt is when this checker was constructed; see
	// writerRedundancyGracePeriod.
	startedAt time.Time

	running bool
	stopCh  chan struct{}
	mu      sync.Mutex

	logger zerolog.Logger
}

// HealthCheckerConfig holds configuration for the health checker.
type HealthCheckerConfig struct {
	Registry           *Registry
	CheckInterval      time.Duration // How often to check nodes (default: 5s)
	CheckTimeout       time.Duration // Timeout for each check (default: 3s)
	UnhealthyThreshold int           // Failed checks before marking unhealthy (default: 3)
	// Phase 4: when true, the health loop logs rate-limited Warn messages
	// if zero or >1 nodes have RoleCompactor. Set from main.go based on
	// cluster + replication + compaction config. Zero value (false) means
	// OSS / standalone and the check is skipped.
	WarnIfNoCompactor bool
	Logger            zerolog.Logger
}

// NewHealthChecker creates a new health checker.
func NewHealthChecker(cfg *HealthCheckerConfig) *HealthChecker {
	// Set defaults
	checkInterval := cfg.CheckInterval
	if checkInterval == 0 {
		checkInterval = 5 * time.Second
	}

	checkTimeout := cfg.CheckTimeout
	if checkTimeout == 0 {
		checkTimeout = 3 * time.Second
	}

	unhealthyThreshold := cfg.UnhealthyThreshold
	if unhealthyThreshold == 0 {
		unhealthyThreshold = 3
	}

	return &HealthChecker{
		registry:           cfg.Registry,
		checkInterval:      checkInterval,
		checkTimeout:       checkTimeout,
		unhealthyThreshold: unhealthyThreshold,
		warnIfNoCompactor:  cfg.WarnIfNoCompactor,
		startedAt:          time.Now(),
		stopCh:             make(chan struct{}),
		logger:             cfg.Logger.With().Str("component", "health-checker").Logger(),
	}
}

// Start starts the health checker background loop.
func (h *HealthChecker) Start() {
	h.mu.Lock()
	if h.running {
		h.mu.Unlock()
		return
	}
	h.running = true
	h.mu.Unlock()

	go h.checkLoop()

	h.logger.Info().
		Dur("interval", h.checkInterval).
		Dur("timeout", h.checkTimeout).
		Int("unhealthy_threshold", h.unhealthyThreshold).
		Msg("Health checker started")
}

// Stop stops the health checker.
func (h *HealthChecker) Stop() {
	h.mu.Lock()
	if !h.running {
		h.mu.Unlock()
		return
	}
	h.running = false
	h.mu.Unlock()

	close(h.stopCh)
	h.logger.Info().Msg("Health checker stopped")
}

// IsRunning returns true if the health checker is running.
func (h *HealthChecker) IsRunning() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.running
}

// checkLoop performs periodic health checks on all nodes.
func (h *HealthChecker) checkLoop() {
	ticker := time.NewTicker(h.checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			h.checkAllNodes()
		case <-h.stopCh:
			return
		}
	}
}

// checkAllNodes checks the health of all registered nodes.
func (h *HealthChecker) checkAllNodes() {
	nodes := h.registry.GetAll()
	local := h.registry.Local()

	for _, node := range nodes {
		// Skip local node (always healthy if we're running)
		if local != nil && node.ID == local.ID {
			continue
		}

		// Check each remote node concurrently
		go h.checkNode(node)
	}

	// Phase 4: rate-limited compactor-election warning. Runs after the
	// per-node checks so the registry is as fresh as possible. Skipped
	// entirely when cluster + replication + compaction aren't all
	// enabled, keeping OSS paths silent.
	if h.warnIfNoCompactor {
		h.checkCompactorElected()
	}
	// #856: rate-limited warning when the cluster has too few writer-role
	// nodes to tolerate losing one. Armed by the coordinator only in the
	// cluster modes where writer redundancy is a thing.
	if mode := writerRedundancyMode(h.writerRedundancy.Load()); mode != writerRedundancyOff {
		h.checkWriterRedundancy(mode)
	}
}

// checkWriterRedundancy warns when the cluster has fewer writer-role nodes
// than writersForHA.
//
// The count is by ROLE, not by health: this warning is about how the cluster
// was deployed, not about who is up right now. A writer that is currently
// unhealthy is the failover manager's problem and has its own logging; a
// cluster that was only ever given one writer is a topology the operator has
// to change.
func (h *HealthChecker) checkWriterRedundancy(mode writerRedundancyMode) {
	if time.Since(h.startedAt) < writerRedundancyGracePeriod {
		return
	}

	// A one-node cluster is a development or single-node install. It has no
	// redundancy of any kind and its operator knows it, so telling them once a
	// minute is noise. The topology this warning exists for is the one that
	// LOOKS highly available — several nodes, one of them a writer.
	if h.registry.Count() <= 1 {
		return
	}

	writers := h.registry.CountByRole(RoleWriter)
	if writers >= writersForHA {
		// Reset so a cluster that loses a writer later warns immediately
		// rather than waiting out a stale cooldown.
		h.lastWriterRedundancyWarnAt.Store(0)
		return
	}

	if !h.claimWarnSlot(&h.lastWriterRedundancyWarnAt) {
		return
	}

	h.logger.Warn().
		Int("writer_nodes", writers).
		Int("writer_nodes_recommended", writersForHA).
		Msg(writerRedundancyMessage(mode, writers))
}

// writerRedundancyMessage builds the operator-facing text for
// checkWriterRedundancy. Split out so the wording is unit-testable without
// running a health loop.
//
// Every claim here has to hold for the code as it ships. In particular it does
// NOT say that two writers lose Raft quorum: today every node that joins
// becomes a voter regardless of role (coordinator.go AddVoter), so readers
// carry quorum too. What is true in both patterns is that the second writer is
// the only spare, and one failure consumes it.
func writerRedundancyMessage(mode writerRedundancyMode, writers int) string {
	if mode == writerRedundancyLoadBalanced {
		if writers < 2 {
			return "Shared-storage mode has fewer than two writer-role nodes: " +
				"Raft writer promotion is deliberately suppressed in this pattern, " +
				"so a writer loss takes ingest down until an operator restores it. " +
				"Run three nodes with ARC_CLUSTER_ROLE=writer behind the load balancer."
		}
		return "Shared-storage mode has only two writer-role nodes: losing one " +
			"leaves a single writer behind the load balancer with no remaining " +
			"redundancy, and no node can be taken out for a rolling upgrade. " +
			"Run three nodes with ARC_CLUSTER_ROLE=writer."
	}

	if writers < 2 {
		return "Writer failover is enabled but the cluster has fewer than two " +
			"writer-role nodes: readers are never promotion candidates, so a " +
			"writer loss cannot be failed over and ingest stops until an " +
			"operator intervenes. Run three nodes with ARC_CLUSTER_ROLE=writer."
	}
	return "Writer failover is enabled but the cluster has only two writer-role " +
		"nodes: one failover consumes the only spare and leaves a single writer " +
		"with nothing left to promote. Run three nodes with ARC_CLUSTER_ROLE=writer."
}

// checkCompactorElected enforces the Phase 4 "exactly one compactor"
// invariant via rate-limited Warn logs. Two failure modes:
//
//   - Zero compactors: operator forgot to set ARC_CLUSTER_ROLE=compactor
//     on any node. Compacted files will never be registered in the Raft
//     manifest and the cluster will slowly accumulate small source files.
//   - Multiple compactors: operator set RoleCompactor on more than one
//     node, re-introducing the shared-storage duplicate-output bug that
//     Phase 4 is designed to prevent.
//
// Both cases are non-fatal — Arc keeps running, queries still work, but
// the cluster is in a degraded state that requires operator attention.
// A Warn log (not Error) is the right level because the operator chose
// the configuration; we're flagging it, not failing on it.
//
// Design note on rate limiting: we always walk the registry (cheap — it's
// an in-memory map) and decide the message FIRST, then apply the
// rate-limit to the LOG EMISSION only. This way, the "correct config"
// branch unconditionally resets both timers — so when the cluster
// transitions broken → correct → broken, the second "broken" warning
// fires immediately instead of waiting out the full minute from the
// first one.
//
// The two failure modes have SEPARATE timers so a cluster flapping
// between 0 and 2 compactors surfaces both warnings within the same
// minute. Sharing a single timer would let whichever warning lost the
// CAS race silence the other for the whole interval — an operator
// watching a log-tail could see only "no compactor" while "multiple
// compactors" was the more recent condition.
// EnableWriterRedundancyWarning arms the writer-redundancy warning in the
// given deployment mode. Called from the coordinator, which knows which
// pattern is configured, after the health checker is constructed.
func (h *HealthChecker) EnableWriterRedundancyWarning(mode writerRedundancyMode) {
	h.writerRedundancy.Store(int32(mode))
}

// SetRaftFSM wires the Raft FSM so checkCompactorElected can check the
// Phase 5 active compactor lease. Called from coordinator wiring.
func (h *HealthChecker) SetRaftFSM(fsm *raft.ClusterFSM) {
	h.raftFSM = fsm
}

func (h *HealthChecker) checkCompactorElected() {
	compactors := h.registry.GetCompactors()

	// Phase 5: if the FSM has an active compactor lease assigned (via
	// failover), that counts as "a compactor is elected" even if no
	// RoleCompactor nodes are in the registry (e.g. a writer took over).
	activeCompactorID := ""
	if h.raftFSM != nil {
		activeCompactorID = h.raftFSM.GetActiveCompactorID()
	}

	if len(compactors) == 1 || (len(compactors) == 0 && activeCompactorID != "") {
		// Correctly configured (or failover is handling it).
		h.lastNoCompactorWarnAt.Store(0)
		h.lastMultiCompactorWarnAt.Store(0)
		return
	}

	if len(compactors) == 0 {
		if !h.claimWarnSlot(&h.lastNoCompactorWarnAt) {
			return
		}
		h.logger.Warn().
			Msg("No compactor elected: compacted files will accumulate. " +
				"Set ARC_CLUSTER_ROLE=compactor on one node and restart, " +
				"or enable automatic failover (cluster.failover_enabled).")
		return
	}

	// len(compactors) > 1
	if !h.claimWarnSlot(&h.lastMultiCompactorWarnAt) {
		return
	}
	ids := make([]string, 0, len(compactors))
	for _, c := range compactors {
		ids = append(ids, c.ID)
	}
	h.logger.Warn().
		Int("count", len(compactors)).
		Strs("compactor_ids", ids).
		Msg("Multiple compactors elected: shared storage may see " +
			"duplicate outputs. Only one node should have " +
			"ARC_CLUSTER_ROLE=compactor.")
}

// claimWarnSlot implements the CAS-based rate limit for a single warning
// timer. Returns true if the caller may log the warning — meaning the
// previous timer was either unset or older than compactorWarnInterval and
// this goroutine successfully swapped it to now. Returns false if another
// goroutine beat us to it, or if we're still inside the cooldown window.
//
// Extracted so the two compactor-election warnings can share identical
// rate-limit semantics without duplicating the CAS loop.
func (h *HealthChecker) claimWarnSlot(slot *atomic.Int64) bool {
	lastNanos := slot.Load()
	if lastNanos != 0 {
		if time.Since(time.Unix(0, lastNanos)) < compactorWarnInterval {
			return false
		}
	}
	now := time.Now().UnixNano()
	return slot.CompareAndSwap(lastNanos, now)
}

// checkNode checks the health of a single node.
// Uses atomic state transition to prevent TOCTOU race conditions.
func (h *HealthChecker) checkNode(node *Node) {
	ctx, cancel := context.WithTimeout(context.Background(), h.checkTimeout)
	defer cancel()

	healthy := h.performHealthCheck(ctx, node)

	// Process the health check against the REAL node in the registry (not
	// the clone we received from GetAll). The clone's LastHeartbeat is a
	// snapshot — the real node's LastHeartbeat is updated by handleHeartbeat.
	deadThreshold := h.unhealthyThreshold * 2
	transition := h.registry.ProcessHealthCheck(node.ID, healthy, h.unhealthyThreshold, deadThreshold)

	// Handle state transitions
	if transition != nil {
		switch transition.NewState {
		case StateHealthy:
			h.logger.Info().
				Str("node_id", node.ID).
				Str("role", string(node.Role)).
				Str("previous_state", string(transition.OldState)).
				Msg("Node became healthy")
			h.registry.NotifyHealthy(node)

		case StateUnhealthy:
			h.logger.Warn().
				Str("node_id", node.ID).
				Str("role", string(node.Role)).
				Int("failed_checks", transition.FailedChecks).
				Msg("Node became unhealthy")
			h.registry.NotifyUnhealthy(node)

		case StateDead:
			h.logger.Error().
				Str("node_id", node.ID).
				Str("role", string(node.Role)).
				Int("failed_checks", transition.FailedChecks).
				Msg("Node marked as dead")
		}
	}
}

// performHealthCheck performs the actual health check on a node.
// For Phase 2, this checks heartbeat freshness.
// Phase 3 will add active HTTP health endpoint checks.
func (h *HealthChecker) performHealthCheck(ctx context.Context, node *Node) bool {
	select {
	case <-ctx.Done():
		return false
	default:
	}

	// Read the REAL node's LastHeartbeat from the registry, not the
	// clone's snapshot. The heartbeat sender updates the real node
	// via registry.RecordHeartbeat; the clone is stale by the time
	// we run the health check.
	lastHeartbeat := h.registry.GetLastHeartbeat(node.ID)
	threshold := 3 * h.checkInterval

	return !lastHeartbeat.IsZero() && time.Since(lastHeartbeat) < threshold
}

// CheckNow performs an immediate health check on a specific node.
// This is useful for on-demand checks outside the regular interval.
func (h *HealthChecker) CheckNow(nodeID string) bool {
	node, exists := h.registry.Get(nodeID)
	if !exists {
		return false
	}

	ctx, cancel := context.WithTimeout(context.Background(), h.checkTimeout)
	defer cancel()

	return h.performHealthCheck(ctx, node)
}

// GetCheckInterval returns the configured check interval.
func (h *HealthChecker) GetCheckInterval() time.Duration {
	return h.checkInterval
}

// GetUnhealthyThreshold returns the configured unhealthy threshold.
func (h *HealthChecker) GetUnhealthyThreshold() int {
	return h.unhealthyThreshold
}

// Status returns the health checker status for monitoring.
func (h *HealthChecker) Status() map[string]interface{} {
	h.mu.Lock()
	running := h.running
	h.mu.Unlock()

	return map[string]interface{}{
		"running":             running,
		"check_interval_ms":   h.checkInterval.Milliseconds(),
		"check_timeout_ms":    h.checkTimeout.Milliseconds(),
		"unhealthy_threshold": h.unhealthyThreshold,
	}
}
