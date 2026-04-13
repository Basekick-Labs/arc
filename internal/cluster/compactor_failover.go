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
//   - When the original compactor returns, the current holder keeps it
//     (no preemption — the lease is stable once assigned)
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
}

// tryInitialAssignment attempts to assign a compactor when none is active.
// Prefers nodes with RoleCompactor; falls back to RoleWriter.
func (m *CompactorFailoverManager) tryInitialAssignment() {
	newID := m.selectNewCompactor("")
	if newID == "" {
		// No eligible node — the health checker's "No compactor elected"
		// warning handles operator visibility.
		return
	}

	m.logger.Info().
		Str("node_id", newID).
		Msg("Assigning initial compactor lease")

	if err := m.cfg.RaftNode.AssignCompactor(newID, "", m.cfg.FailoverTimeout); err != nil {
		m.logger.Error().Err(err).
			Str("node_id", newID).
			Msg("Failed to assign initial compactor")
	}
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

// selectNewCompactor picks the best node to receive the compactor lease.
// Priority: healthy RoleCompactor nodes (other than the failed one), then
// healthy RoleWriter nodes. Readers are excluded because they typically
// don't have write access to shared storage.
func (m *CompactorFailoverManager) selectNewCompactor(excludeNodeID string) string {
	// Prefer nodes that were deployed as compactors.
	for _, node := range m.cfg.Registry.GetCompactors() {
		if node.ID != excludeNodeID && node.GetState() == StateHealthy {
			return node.ID
		}
	}
	// Fall back to healthy writers.
	for _, node := range m.cfg.Registry.GetWriters() {
		if node.ID != excludeNodeID && node.GetState() == StateHealthy {
			return node.ID
		}
	}
	return ""
}

// completeFailover marks failover as complete.
func (m *CompactorFailoverManager) completeFailover(newCompactorID string, success bool) {
	m.mu.Lock()
	m.failoverInProg = false
	m.lastFailoverAt = time.Now()
	if success {
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
	}

	if !m.lastFailoverAt.IsZero() {
		stats["last_failover_at"] = m.lastFailoverAt.Format(time.RFC3339)
	}

	return stats
}
