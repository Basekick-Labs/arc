package sharding

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/basekick-labs/arc/internal/cluster"
	"github.com/rs/zerolog"
)

// FailoverConfig holds configuration for the failover manager.
type FailoverConfig struct {
	// ShardMap provides shard-to-node mappings
	ShardMap *ShardMap

	// MetaCluster for shard reassignment (optional, for leader nodes)
	MetaCluster *MetaCluster

	// LocalNode is this node
	LocalNode *cluster.Node

	// HealthCheckInterval is how often to check node health
	HealthCheckInterval time.Duration

	// FailoverTimeout is the timeout for failover operations
	FailoverTimeout time.Duration

	// UnhealthyThreshold is the number of failed health checks before marking unhealthy
	UnhealthyThreshold int

	// Logger for failover events
	Logger zerolog.Logger
}

// FailoverManager monitors shard health and triggers automatic failover.
// It runs on the meta cluster leader and coordinates shard reassignment.
type FailoverManager struct {
	cfg           *FailoverConfig
	nodeHealth    map[string]*nodeHealthState // nodeID -> health state
	shardHealth   map[int]*shardHealthState   // shardID -> health state
	mu            sync.RWMutex
	logger        zerolog.Logger
	ctx           context.Context
	cancelFn      context.CancelFunc
	running       atomic.Bool
	wg            sync.WaitGroup

	// Callbacks
	onFailoverStart    func(shardID int, oldPrimary, newPrimary string)
	onFailoverComplete func(shardID int, newPrimary string, success bool)
}

// nodeHealthState tracks health for a single node.
type nodeHealthState struct {
	nodeID          string
	consecutiveFail int
	lastCheck       time.Time
	lastSuccess     time.Time
	isHealthy       bool
}

// shardHealthState tracks health for a single shard.
type shardHealthState struct {
	shardID        int
	primaryID      string
	isPrimaryUp    bool
	lastCheck      time.Time
	failoverInProg bool
}

// NewFailoverManager creates a new failover manager.
func NewFailoverManager(cfg *FailoverConfig) *FailoverManager {
	if cfg.HealthCheckInterval == 0 {
		cfg.HealthCheckInterval = 5 * time.Second
	}
	if cfg.FailoverTimeout == 0 {
		cfg.FailoverTimeout = 30 * time.Second
	}
	if cfg.UnhealthyThreshold == 0 {
		cfg.UnhealthyThreshold = 3
	}

	return &FailoverManager{
		cfg:         cfg,
		nodeHealth:  make(map[string]*nodeHealthState),
		shardHealth: make(map[int]*shardHealthState),
		logger:      cfg.Logger.With().Str("component", "failover-manager").Logger(),
	}
}

// SetCallbacks sets callbacks for failover events.
func (m *FailoverManager) SetCallbacks(
	onStart func(shardID int, oldPrimary, newPrimary string),
	onComplete func(shardID int, newPrimary string, success bool),
) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.onFailoverStart = onStart
	m.onFailoverComplete = onComplete
}

// Start begins the failover manager.
func (m *FailoverManager) Start(ctx context.Context) error {
	m.ctx, m.cancelFn = context.WithCancel(ctx)
	m.running.Store(true)

	// Start health check loop
	m.wg.Add(1)
	go m.healthCheckLoop()

	m.logger.Info().
		Dur("health_check_interval", m.cfg.HealthCheckInterval).
		Int("unhealthy_threshold", m.cfg.UnhealthyThreshold).
		Msg("Failover manager started")

	return nil
}

// Stop gracefully shuts down the failover manager.
func (m *FailoverManager) Stop() error {
	if !m.running.Load() {
		return nil
	}

	m.running.Store(false)
	m.cancelFn()
	m.wg.Wait()

	m.logger.Info().Msg("Failover manager stopped")
	return nil
}

// healthCheckLoop periodically checks node and shard health.
func (m *FailoverManager) healthCheckLoop() {
	defer m.wg.Done()

	ticker := time.NewTicker(m.cfg.HealthCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			m.checkHealth()
		}
	}
}

// checkHealth checks the health of all nodes and shards.
func (m *FailoverManager) checkHealth() {
	// Only the meta cluster leader should perform health checks
	if m.cfg.MetaCluster != nil && !m.cfg.MetaCluster.IsLeader() {
		return
	}

	m.checkNodeHealth()
	m.checkShardHealth()
}

// checkNodeHealth checks the health of all nodes.
func (m *FailoverManager) checkNodeHealth() {
	// Collect all unique nodes across all shards
	nodeSet := make(map[string]*cluster.Node)
	numShards := m.cfg.ShardMap.NumShards()
	for shardID := 0; shardID < numShards; shardID++ {
		nodes := m.cfg.ShardMap.GetAllNodes(shardID)
		for _, node := range nodes {
			if node != nil {
				nodeSet[node.ID] = node
			}
		}
	}

	now := time.Now()

	m.mu.Lock()
	defer m.mu.Unlock()

	for _, node := range nodeSet {
		state, exists := m.nodeHealth[node.ID]
		if !exists {
			state = &nodeHealthState{
				nodeID:    node.ID,
				isHealthy: true,
			}
			m.nodeHealth[node.ID] = state
		}

		state.lastCheck = now

		// Check if node is healthy based on its reported state
		if node.IsHealthy() {
			state.consecutiveFail = 0
			state.lastSuccess = now
			state.isHealthy = true
		} else {
			state.consecutiveFail++
			if state.consecutiveFail >= m.cfg.UnhealthyThreshold {
				if state.isHealthy {
					m.logger.Warn().
						Str("node_id", node.ID).
						Int("consecutive_failures", state.consecutiveFail).
						Msg("Node marked unhealthy")
				}
				state.isHealthy = false
			}
		}
	}

	// Clean up removed nodes
	for nodeID := range m.nodeHealth {
		if _, exists := nodeSet[nodeID]; !exists {
			delete(m.nodeHealth, nodeID)
		}
	}
}

// checkShardHealth checks the health of all shard primaries and triggers failover if needed.
func (m *FailoverManager) checkShardHealth() {
	shardMap := m.cfg.ShardMap
	numShards := shardMap.NumShards()

	m.mu.Lock()
	defer m.mu.Unlock()

	for shardID := 0; shardID < numShards; shardID++ {
		primary := shardMap.GetPrimary(shardID)

		state, exists := m.shardHealth[shardID]
		if !exists {
			state = &shardHealthState{
				shardID: shardID,
			}
			m.shardHealth[shardID] = state
		}

		state.lastCheck = time.Now()

		// Update primary tracking
		if primary != nil {
			state.primaryID = primary.ID
			state.isPrimaryUp = primary.IsHealthy()
		} else {
			state.primaryID = ""
			state.isPrimaryUp = false
		}

		// Check if failover is needed
		if !state.isPrimaryUp && !state.failoverInProg && state.primaryID != "" {
			m.triggerFailoverLocked(shardID, state)
		}
	}
}

// triggerFailoverLocked initiates failover for a shard (must hold lock).
func (m *FailoverManager) triggerFailoverLocked(shardID int, state *shardHealthState) {
	if m.cfg.MetaCluster == nil {
		m.logger.Warn().
			Int("shard_id", shardID).
			Msg("Cannot trigger failover: no meta cluster")
		return
	}

	oldPrimary := state.primaryID
	state.failoverInProg = true

	m.logger.Info().
		Int("shard_id", shardID).
		Str("old_primary", oldPrimary).
		Msg("Initiating failover for shard")

	// Run failover in background
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		m.executeFailover(shardID, oldPrimary)
	}()
}

// executeFailover performs the actual failover operation.
func (m *FailoverManager) executeFailover(shardID int, oldPrimary string) {
	ctx, cancel := context.WithTimeout(m.ctx, m.cfg.FailoverTimeout)
	defer cancel()

	// Find a healthy replica to promote
	newPrimary := m.selectNewPrimary(shardID, oldPrimary)
	if newPrimary == "" {
		m.logger.Error().
			Int("shard_id", shardID).
			Msg("No healthy replica available for failover")
		m.completeFailover(shardID, "", false)
		return
	}

	// Notify callback
	m.mu.RLock()
	callback := m.onFailoverStart
	m.mu.RUnlock()
	if callback != nil {
		callback(shardID, oldPrimary, newPrimary)
	}

	m.logger.Info().
		Int("shard_id", shardID).
		Str("old_primary", oldPrimary).
		Str("new_primary", newPrimary).
		Msg("Promoting replica to primary")

	// Execute promotion via meta cluster
	if m.cfg.MetaCluster != nil && m.cfg.MetaCluster.IsLeader() {
		// Assign new primary
		if err := m.cfg.MetaCluster.AssignShard(shardID, newPrimary, RolePrimary, m.cfg.FailoverTimeout); err != nil {
			m.logger.Error().Err(err).
				Int("shard_id", shardID).
				Str("new_primary", newPrimary).
				Msg("Failed to assign new primary")
			m.completeFailover(shardID, newPrimary, false)
			return
		}

		// Wait for assignment to propagate
		select {
		case <-ctx.Done():
			m.logger.Error().
				Int("shard_id", shardID).
				Msg("Failover timeout")
			m.completeFailover(shardID, newPrimary, false)
			return
		case <-time.After(500 * time.Millisecond):
			// Brief wait for propagation
		}
	}

	m.logger.Info().
		Int("shard_id", shardID).
		Str("new_primary", newPrimary).
		Msg("Failover completed successfully")

	m.completeFailover(shardID, newPrimary, true)
}

// selectNewPrimary selects the best replica to promote.
func (m *FailoverManager) selectNewPrimary(shardID int, excludeNodeID string) string {
	group := m.cfg.ShardMap.GetShardGroup(shardID)
	if group == nil {
		return ""
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	// Find the healthiest replica with the least lag
	var bestReplica string
	for _, replica := range group.Replicas {
		if replica.ID == excludeNodeID {
			continue
		}

		// Check if replica is healthy
		if state, exists := m.nodeHealth[replica.ID]; exists && state.isHealthy {
			// For now, just pick the first healthy replica
			// In a more sophisticated implementation, we'd consider replication lag
			bestReplica = replica.ID
			break
		}
	}

	return bestReplica
}

// completeFailover marks failover as complete.
func (m *FailoverManager) completeFailover(shardID int, newPrimary string, success bool) {
	m.mu.Lock()
	if state, exists := m.shardHealth[shardID]; exists {
		state.failoverInProg = false
		if success {
			state.primaryID = newPrimary
			state.isPrimaryUp = true
		}
	}
	callback := m.onFailoverComplete
	m.mu.Unlock()

	if callback != nil {
		callback(shardID, newPrimary, success)
	}
}

// TriggerManualFailover triggers a manual failover for a shard.
func (m *FailoverManager) TriggerManualFailover(shardID int) error {
	m.mu.Lock()
	state, exists := m.shardHealth[shardID]
	if !exists {
		state = &shardHealthState{
			shardID: shardID,
		}
		m.shardHealth[shardID] = state
	}

	if state.failoverInProg {
		m.mu.Unlock()
		return fmt.Errorf("failover already in progress for shard %d", shardID)
	}

	primary := m.cfg.ShardMap.GetPrimary(shardID)
	if primary == nil {
		m.mu.Unlock()
		return fmt.Errorf("no primary for shard %d", shardID)
	}

	state.primaryID = primary.ID
	state.failoverInProg = true
	m.mu.Unlock()

	m.logger.Info().
		Int("shard_id", shardID).
		Str("current_primary", primary.ID).
		Msg("Manual failover initiated")

	go func() {
		m.executeFailover(shardID, primary.ID)
	}()

	return nil
}

// GetShardHealthStatus returns the health status of a shard.
func (m *FailoverManager) GetShardHealthStatus(shardID int) map[string]interface{} {
	m.mu.RLock()
	defer m.mu.RUnlock()

	state, exists := m.shardHealth[shardID]
	if !exists {
		return nil
	}

	return map[string]interface{}{
		"shard_id":          state.shardID,
		"primary_id":        state.primaryID,
		"is_primary_up":     state.isPrimaryUp,
		"failover_in_prog":  state.failoverInProg,
		"last_check":        state.lastCheck.Format(time.RFC3339),
	}
}

// GetNodeHealthStatus returns the health status of a node.
func (m *FailoverManager) GetNodeHealthStatus(nodeID string) map[string]interface{} {
	m.mu.RLock()
	defer m.mu.RUnlock()

	state, exists := m.nodeHealth[nodeID]
	if !exists {
		return nil
	}

	return map[string]interface{}{
		"node_id":          state.nodeID,
		"is_healthy":       state.isHealthy,
		"consecutive_fail": state.consecutiveFail,
		"last_check":       state.lastCheck.Format(time.RFC3339),
		"last_success":     state.lastSuccess.Format(time.RFC3339),
	}
}

// Stats returns overall failover manager statistics.
func (m *FailoverManager) Stats() map[string]interface{} {
	m.mu.RLock()
	defer m.mu.RUnlock()

	nodeStats := make(map[string]map[string]interface{})
	for nodeID, state := range m.nodeHealth {
		nodeStats[nodeID] = map[string]interface{}{
			"is_healthy":       state.isHealthy,
			"consecutive_fail": state.consecutiveFail,
		}
	}

	shardStats := make(map[int]map[string]interface{})
	for shardID, state := range m.shardHealth {
		shardStats[shardID] = map[string]interface{}{
			"primary_id":       state.primaryID,
			"is_primary_up":    state.isPrimaryUp,
			"failover_in_prog": state.failoverInProg,
		}
	}

	return map[string]interface{}{
		"running":               m.running.Load(),
		"health_check_interval": m.cfg.HealthCheckInterval.String(),
		"unhealthy_threshold":   m.cfg.UnhealthyThreshold,
		"node_count":            len(m.nodeHealth),
		"shard_count":           len(m.shardHealth),
		"nodes":                 nodeStats,
		"shards":                shardStats,
	}
}
