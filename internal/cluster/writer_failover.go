package cluster

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	arcRaft "github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/rs/zerolog"
)

// WriterFailoverConfig holds configuration for the writer failover manager.
type WriterFailoverConfig struct {
	// Registry provides access to cluster node state
	Registry *Registry

	// RaftNode for applying promote/demote commands via consensus
	RaftNode *arcRaft.Node

	// HealthCheckInterval is how often to check writer health
	HealthCheckInterval time.Duration

	// FailoverTimeout is the timeout for the entire failover operation
	FailoverTimeout time.Duration

	// CooldownPeriod prevents repeated failovers within this window
	CooldownPeriod time.Duration

	// UnhealthyThreshold is consecutive unhealthy checks before triggering failover
	UnhealthyThreshold int

	// Logger for failover events
	Logger zerolog.Logger
}

// WriterFailoverManager monitors writer health and promotes standby writers
// when the primary fails. Only the Raft leader runs active health checks.
type WriterFailoverManager struct {
	cfg      *WriterFailoverConfig
	mu       sync.RWMutex
	logger   zerolog.Logger
	ctx      context.Context
	cancelFn context.CancelFunc
	running  atomic.Bool
	wg       sync.WaitGroup

	// State tracking
	primaryID        string
	consecutiveFails int
	failoverInProg   bool
	lastFailoverAt   time.Time

	// Callbacks
	onFailoverStart    func(oldPrimaryID, newPrimaryID string)
	onFailoverComplete func(newPrimaryID string, success bool)
}

// NewWriterFailoverManager creates a new writer failover manager.
func NewWriterFailoverManager(cfg *WriterFailoverConfig) *WriterFailoverManager {
	if cfg.HealthCheckInterval == 0 {
		cfg.HealthCheckInterval = 5 * time.Second
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

	return &WriterFailoverManager{
		cfg:    cfg,
		logger: cfg.Logger.With().Str("component", "writer-failover").Logger(),
	}
}

// SetCallbacks sets callbacks for failover events.
func (m *WriterFailoverManager) SetCallbacks(
	onStart func(oldPrimaryID, newPrimaryID string),
	onComplete func(newPrimaryID string, success bool),
) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.onFailoverStart = onStart
	m.onFailoverComplete = onComplete
}

// Start begins the writer failover manager.
func (m *WriterFailoverManager) Start(ctx context.Context) error {
	m.ctx, m.cancelFn = context.WithCancel(ctx)
	m.running.Store(true)

	m.wg.Add(1)
	go m.healthCheckLoop()

	m.logger.Info().
		Dur("health_check_interval", m.cfg.HealthCheckInterval).
		Dur("cooldown_period", m.cfg.CooldownPeriod).
		Int("unhealthy_threshold", m.cfg.UnhealthyThreshold).
		Msg("Writer failover manager started")

	return nil
}

// Stop gracefully shuts down the writer failover manager.
func (m *WriterFailoverManager) Stop() error {
	if !m.running.Load() {
		return nil
	}

	m.running.Store(false)
	m.cancelFn()
	m.wg.Wait()

	m.logger.Info().Msg("Writer failover manager stopped")
	return nil
}

// healthCheckLoop periodically checks the primary writer's health.
func (m *WriterFailoverManager) healthCheckLoop() {
	defer m.wg.Done()

	ticker := time.NewTicker(m.cfg.HealthCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			m.checkPrimaryHealth()
		}
	}
}

// checkPrimaryHealth checks whether the primary writer is healthy.
// Only the Raft leader should act on failures.
func (m *WriterFailoverManager) checkPrimaryHealth() {
	// Only the Raft leader coordinates failover
	if m.cfg.RaftNode == nil || !m.cfg.RaftNode.IsLeader() {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.failoverInProg {
		return
	}

	// Find the current primary writer
	primary := m.cfg.Registry.GetPrimaryWriter()
	if primary == nil {
		// No primary — check if there are any writers at all
		writers := m.cfg.Registry.GetWriters()
		if len(writers) == 0 {
			return
		}
		if m.primaryID == "" {
			// No primary has ever existed in this cluster: elect one.
			// Without this the manager deadlocks at boot — the failover
			// branch below needs a previous primary to fail over FROM, and
			// nothing else ever issues CommandPromoteWriter, so every node
			// stayed WriterState-less and IsPrimaryWriter() was false
			// cluster-wide, silently disabling the retention and CQ
			// schedulers and the delete/retention/CQ endpoints (#850).
			m.tryInitialElectionLocked()
			return
		}
		// We had a primary but it's gone — trigger failover
		m.consecutiveFails++
		if m.consecutiveFails >= m.cfg.UnhealthyThreshold {
			m.triggerFailoverLocked()
		}
		return
	}

	// Primary exists and is healthy — reset counter
	m.primaryID = primary.ID
	m.consecutiveFails = 0
}

// HandleWriterUnhealthy is called when a writer node becomes unhealthy.
// This is invoked by the registry's onNodeUnhealthy callback.
func (m *WriterFailoverManager) HandleWriterUnhealthy(node *Node) {
	if node.Role != RoleWriter {
		return
	}

	// Only the Raft leader coordinates failover
	if m.cfg.RaftNode == nil || !m.cfg.RaftNode.IsLeader() {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// Only care if it's the primary
	if node.GetWriterState() != WriterStatePrimary {
		return
	}

	if m.failoverInProg {
		return
	}

	m.primaryID = node.ID
	m.consecutiveFails++

	m.logger.Warn().
		Str("node_id", node.ID).
		Int("consecutive_fails", m.consecutiveFails).
		Int("threshold", m.cfg.UnhealthyThreshold).
		Msg("Primary writer unhealthy")

	if m.consecutiveFails >= m.cfg.UnhealthyThreshold {
		m.triggerFailoverLocked()
	}
}

// tryInitialElectionLocked elects the first primary writer of a cluster that
// has never had one (must hold lock). It is the writer-side counterpart of
// CompactorFailoverManager.tryInitialAssignment and mirrors it: the same
// in-progress guard, the same asynchronous Raft apply so the health-check loop
// is never blocked, and the same completion bookkeeping.
//
// Only the Raft leader reaches here (checkPrimaryHealth returns early
// otherwise), so exactly one node elects. An election is not a failover: there
// is no old primary to demote, no cooldown to respect on the way in, and none
// armed on the way out — see completeElection. A failing election therefore
// retries on the next tick rather than backing off, which is what the
// compactor's initial assignment does too; each attempt costs one Raft apply
// bounded by FailoverTimeout, so a quorum outage logs at most one error per
// timeout rather than per tick.
func (m *WriterFailoverManager) tryInitialElectionLocked() {
	if m.failoverInProg {
		return
	}
	newPrimaryID := m.selectNewPrimary("")
	if newPrimaryID == "" {
		return
	}
	m.failoverInProg = true
	m.logger.Info().
		Str("node_id", newPrimaryID).
		Msg("Electing initial primary writer")
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		err := m.cfg.RaftNode.PromoteWriter(newPrimaryID, "", m.cfg.FailoverTimeout)
		if err != nil {
			m.logger.Error().Err(err).
				Str("node_id", newPrimaryID).
				Msg("Failed to elect the initial primary writer")
		}
		m.completeElection(newPrimaryID, err == nil)
	}()
}

// completeElection finishes an initial election. It is deliberately NOT
// completeFailover: that arms the failover cooldown unconditionally, and an
// election happening at boot would then swallow a genuine writer failure for
// the whole cooldown period. An election is not a failover and nothing was
// lost, so there is nothing to back off from.
// It deliberately does not invoke onFailoverComplete either: that callback
// announces a failover, and no failover happened.
func (m *WriterFailoverManager) completeElection(newPrimaryID string, success bool) {
	m.mu.Lock()
	m.failoverInProg = false
	if success {
		m.primaryID = newPrimaryID
		m.consecutiveFails = 0
	}
	m.mu.Unlock()
}

// triggerFailoverLocked initiates failover (must hold lock).
func (m *WriterFailoverManager) triggerFailoverLocked() {
	// Check cooldown
	if !m.lastFailoverAt.IsZero() && time.Since(m.lastFailoverAt) < m.cfg.CooldownPeriod {
		m.logger.Warn().
			Dur("cooldown_remaining", m.cfg.CooldownPeriod-time.Since(m.lastFailoverAt)).
			Msg("Failover skipped — cooldown period active")
		return
	}

	m.failoverInProg = true
	oldPrimary := m.primaryID

	m.logger.Info().
		Str("old_primary", oldPrimary).
		Msg("Initiating writer failover")

	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		m.executeFailover(oldPrimary, true)
	}()
}

// executeFailover performs the actual failover operation.
func (m *WriterFailoverManager) executeFailover(oldPrimaryID string, allowSelf bool) {
	ctx, cancel := context.WithTimeout(m.ctx, m.cfg.FailoverTimeout)
	defer cancel()

	// allowSelf: the old primary is still a candidate when it is healthy and
	// alone, which is how a writer that lost its designation to a restart gets
	// it back instead of deadlocking. An operator-requested failover never
	// takes that path — being asked to move off a node and then landing back
	// on it is not a failover.
	newPrimaryID := m.selectPrimary(oldPrimaryID, allowSelf)
	if newPrimaryID == "" {
		m.logger.Error().
			Str("old_primary", oldPrimaryID).
			Msg("No healthy standby writer available for failover")
		m.completeFailover("", false)
		return
	}

	// Notify callback
	m.mu.RLock()
	startCb := m.onFailoverStart
	m.mu.RUnlock()
	if startCb != nil {
		startCb(oldPrimaryID, newPrimaryID)
	}

	m.logger.Info().
		Str("old_primary", oldPrimaryID).
		Str("new_primary", newPrimaryID).
		Msg("Promoting standby writer to primary")

	// Apply via Raft consensus
	if err := m.cfg.RaftNode.PromoteWriter(newPrimaryID, oldPrimaryID, m.cfg.FailoverTimeout); err != nil {
		m.logger.Error().Err(err).
			Str("new_primary", newPrimaryID).
			Msg("Failed to promote writer via Raft")
		m.completeFailover(newPrimaryID, false)
		return
	}

	// Brief wait for propagation
	select {
	case <-ctx.Done():
		m.logger.Error().Msg("Failover timeout waiting for propagation")
		m.completeFailover(newPrimaryID, false)
		return
	case <-time.After(500 * time.Millisecond):
	}

	m.logger.Info().
		Str("new_primary", newPrimaryID).
		Str("old_primary", oldPrimaryID).
		Msg("Writer failover completed successfully")

	m.completeFailover(newPrimaryID, true)
}

// selectNewPrimary picks the best standby writer to promote.
// Prefers standby writers; falls back to any healthy writer excluding the failed primary.
func (m *WriterFailoverManager) selectNewPrimary(excludeNodeID string) string {
	return m.selectPrimary(excludeNodeID, false)
}

// selectPrimary picks a writer to promote. It excludes excludeNodeID, the
// primary being failed away from, unless allowSelf is set and no other
// candidate exists.
//
// allowSelf exists because "no primary" does not always mean the primary
// died. A writer that merely restarted re-joins with a payload that carries
// no writer state, which clears its designation; the manager then looks for
// someone to fail over TO and, in the single-writer topology this feature
// documents, finds nobody, because the only candidate is the node it just
// excluded. The cluster would sit with no primary forever. GetWriters already
// filters to healthy nodes, so re-selecting the excluded node can never
// resurrect a dead one.
func (m *WriterFailoverManager) selectPrimary(excludeNodeID string, allowSelf bool) string {
	// GetStandbyWriters and GetWriters already filter for healthy nodes
	for _, node := range m.cfg.Registry.GetStandbyWriters() {
		if node.ID != excludeNodeID {
			return node.ID
		}
	}
	for _, node := range m.cfg.Registry.GetWriters() {
		if node.ID != excludeNodeID {
			return node.ID
		}
	}
	if allowSelf && excludeNodeID != "" {
		for _, node := range m.cfg.Registry.GetWriters() {
			if node.ID == excludeNodeID {
				return node.ID
			}
		}
	}
	return ""
}

// completeFailover marks failover as complete.
func (m *WriterFailoverManager) completeFailover(newPrimaryID string, success bool) {
	m.mu.Lock()
	m.failoverInProg = false
	m.lastFailoverAt = time.Now()
	if success {
		m.primaryID = newPrimaryID
		m.consecutiveFails = 0
	}
	completeCb := m.onFailoverComplete
	m.mu.Unlock()

	if completeCb != nil {
		completeCb(newPrimaryID, success)
	}
}

// TriggerManualFailover triggers a manual failover from the current primary.
func (m *WriterFailoverManager) TriggerManualFailover() error {
	m.mu.Lock()

	if m.failoverInProg {
		m.mu.Unlock()
		return fmt.Errorf("failover already in progress")
	}

	primary := m.cfg.Registry.GetPrimaryWriter()
	if primary == nil {
		m.mu.Unlock()
		return fmt.Errorf("no primary writer to failover from")
	}

	m.primaryID = primary.ID
	m.failoverInProg = true
	m.mu.Unlock()

	m.logger.Info().
		Str("current_primary", primary.ID).
		Msg("Manual writer failover initiated")

	// Tracked on the WaitGroup like the other two, so Stop joins it.
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		m.executeFailover(primary.ID, false)
	}()

	return nil
}

// Stats returns failover manager statistics.
func (m *WriterFailoverManager) Stats() map[string]interface{} {
	m.mu.RLock()
	defer m.mu.RUnlock()

	stats := map[string]interface{}{
		"running":              m.running.Load(),
		"primary_id":           m.primaryID,
		"consecutive_fails":    m.consecutiveFails,
		"failover_in_progress": m.failoverInProg,
		"cooldown_period":      m.cfg.CooldownPeriod.String(),
	}

	if !m.lastFailoverAt.IsZero() {
		stats["last_failover_at"] = m.lastFailoverAt.Format(time.RFC3339)
	}

	return stats
}
