package cluster

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
)

// compactorWarnInterval is the minimum duration between "no compactor
// elected" or "multiple compactors elected" warnings. The health loop
// ticks on checkInterval (default 5s), but we only want the warning to
// surface once per minute so SRE dashboards don't drown in duplicates.
const compactorWarnInterval = 60 * time.Second

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
	warnIfNoCompactor         bool
	lastNoCompactorWarnAt     atomic.Int64 // unix nanos; throttles "no compactor elected"
	lastMultiCompactorWarnAt  atomic.Int64 // unix nanos; throttles "multiple compactors elected"

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
func (h *HealthChecker) checkCompactorElected() {
	compactors := h.registry.GetCompactors()
	if len(compactors) == 1 {
		// Correctly configured. Reset BOTH timers so the next
		// misconfiguration of either kind fires immediately without
		// waiting the full interval.
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
				"Set ARC_CLUSTER_ROLE=compactor on one node and restart.")
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

	// Use atomic state transition to prevent race conditions
	deadThreshold := h.unhealthyThreshold * 2
	transition := node.ProcessHealthCheckResult(healthy, h.unhealthyThreshold, deadThreshold)

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
	// Check if context is already cancelled
	select {
	case <-ctx.Done():
		return false
	default:
	}

	// Check heartbeat freshness
	// Consider healthy if heartbeat within 3x check interval
	lastHeartbeat := node.GetLastHeartbeat()
	threshold := 3 * h.checkInterval

	if time.Since(lastHeartbeat) < threshold {
		return true
	}

	// TODO (Phase 3): Add active health check
	// - HTTP GET to node's /health endpoint
	// - TCP connection test to coordinator port
	// - gRPC health check if using gRPC

	return false
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
