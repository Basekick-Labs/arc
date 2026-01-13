package sharding

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/basekick-labs/arc/internal/cluster"
	"github.com/hashicorp/raft"
	"github.com/rs/zerolog"
)

// MetaCluster coordinates shard assignments across the cluster.
// It uses Raft consensus to maintain a consistent view of the shard map.
type MetaCluster struct {
	raftNode *raft.Raft
	fsm      *MetaFSM
	config   *MetaClusterConfig
	logger   zerolog.Logger

	mu sync.RWMutex
}

// MetaClusterConfig holds configuration for the meta cluster.
type MetaClusterConfig struct {
	NumShards         int           // Total number of shards
	ReplicationFactor int           // Target number of replicas per shard
	LocalNode         *cluster.Node // Local node reference
	Logger            zerolog.Logger
}

// NewMetaCluster creates a new meta cluster coordinator.
// The raftNode must be started separately.
func NewMetaCluster(cfg *MetaClusterConfig, raftNode *raft.Raft) *MetaCluster {
	fsm := NewMetaFSM(cfg.NumShards, cfg.Logger)

	return &MetaCluster{
		raftNode: raftNode,
		fsm:      fsm,
		config:   cfg,
		logger:   cfg.Logger.With().Str("component", "meta-cluster").Logger(),
	}
}

// GetFSM returns the meta FSM for use with Raft.
func (m *MetaCluster) GetFSM() *MetaFSM {
	return m.fsm
}

// IsLeader returns true if this node is the meta cluster leader.
func (m *MetaCluster) IsLeader() bool {
	if m.raftNode == nil {
		return false
	}
	return m.raftNode.State() == raft.Leader
}

// LeaderAddr returns the address of the current leader.
func (m *MetaCluster) LeaderAddr() string {
	if m.raftNode == nil {
		return ""
	}
	addr, _ := m.raftNode.LeaderWithID()
	return string(addr)
}

// AssignShard assigns a shard to a node with the specified role.
// Must be called on the leader.
func (m *MetaCluster) AssignShard(shardID int, nodeID string, role Role, timeout time.Duration) error {
	if !m.IsLeader() {
		return ErrNotLeader
	}

	payload, err := json.Marshal(AssignShardPayload{
		ShardID: shardID,
		NodeID:  nodeID,
		Role:    role,
	})
	if err != nil {
		return err
	}

	cmd, err := json.Marshal(MetaCommand{
		Type:    CmdAssignShard,
		Payload: payload,
	})
	if err != nil {
		return err
	}

	future := m.raftNode.Apply(cmd, timeout)
	if err := future.Error(); err != nil {
		return fmt.Errorf("raft apply failed: %w", err)
	}

	if resp := future.Response(); resp != nil {
		if err, ok := resp.(error); ok {
			return err
		}
	}

	return nil
}

// UnassignShard removes a shard assignment from a node.
// Must be called on the leader.
func (m *MetaCluster) UnassignShard(shardID int, nodeID string, timeout time.Duration) error {
	if !m.IsLeader() {
		return ErrNotLeader
	}

	payload, err := json.Marshal(UnassignShardPayload{
		ShardID: shardID,
		NodeID:  nodeID,
	})
	if err != nil {
		return err
	}

	cmd, err := json.Marshal(MetaCommand{
		Type:    CmdUnassignShard,
		Payload: payload,
	})
	if err != nil {
		return err
	}

	future := m.raftNode.Apply(cmd, timeout)
	if err := future.Error(); err != nil {
		return fmt.Errorf("raft apply failed: %w", err)
	}

	return nil
}

// AddNode registers a node with the meta cluster.
// Must be called on the leader.
func (m *MetaCluster) AddNode(node *NodeInfo, timeout time.Duration) error {
	if !m.IsLeader() {
		return ErrNotLeader
	}

	payload, err := json.Marshal(AddNodePayload{
		Node: *node,
	})
	if err != nil {
		return err
	}

	cmd, err := json.Marshal(MetaCommand{
		Type:    CmdAddNode,
		Payload: payload,
	})
	if err != nil {
		return err
	}

	future := m.raftNode.Apply(cmd, timeout)
	if err := future.Error(); err != nil {
		return fmt.Errorf("raft apply failed: %w", err)
	}

	return nil
}

// RemoveNode removes a node from the meta cluster.
// This also unassigns all shards from the node.
// Must be called on the leader.
func (m *MetaCluster) RemoveNode(nodeID string, timeout time.Duration) error {
	if !m.IsLeader() {
		return ErrNotLeader
	}

	payload, err := json.Marshal(RemoveNodePayload{
		NodeID: nodeID,
	})
	if err != nil {
		return err
	}

	cmd, err := json.Marshal(MetaCommand{
		Type:    CmdRemoveNode,
		Payload: payload,
	})
	if err != nil {
		return err
	}

	future := m.raftNode.Apply(cmd, timeout)
	if err := future.Error(); err != nil {
		return fmt.Errorf("raft apply failed: %w", err)
	}

	return nil
}

// UpdateNode updates a node's state in the meta cluster.
// Must be called on the leader.
func (m *MetaCluster) UpdateNode(nodeID string, state string, timeout time.Duration) error {
	if !m.IsLeader() {
		return ErrNotLeader
	}

	payload, err := json.Marshal(UpdateNodePayload{
		NodeID: nodeID,
		State:  state,
	})
	if err != nil {
		return err
	}

	cmd, err := json.Marshal(MetaCommand{
		Type:    CmdUpdateNode,
		Payload: payload,
	})
	if err != nil {
		return err
	}

	future := m.raftNode.Apply(cmd, timeout)
	if err := future.Error(); err != nil {
		return fmt.Errorf("raft apply failed: %w", err)
	}

	return nil
}

// GetShardMap returns the current shard map.
func (m *MetaCluster) GetShardMap() *ShardMap {
	return m.fsm.GetShardMap()
}

// GetNodes returns all known nodes.
func (m *MetaCluster) GetNodes() map[string]*NodeInfo {
	return m.fsm.GetNodes()
}

// GetNode returns a specific node by ID.
func (m *MetaCluster) GetNode(nodeID string) (*NodeInfo, bool) {
	return m.fsm.GetNode(nodeID)
}

// OnNodeJoin handles a new node joining the cluster.
// It automatically assigns shards to balance the cluster.
func (m *MetaCluster) OnNodeJoin(node *NodeInfo) error {
	if !m.IsLeader() {
		return ErrNotLeader
	}

	// First, add the node
	if err := m.AddNode(node, 5*time.Second); err != nil {
		return fmt.Errorf("failed to add node: %w", err)
	}

	// Auto-assign shards to maintain replication factor
	return m.rebalanceShards()
}

// OnNodeLeave handles a node leaving the cluster.
// It reassigns shards to maintain replication factor.
func (m *MetaCluster) OnNodeLeave(nodeID string) error {
	if !m.IsLeader() {
		return ErrNotLeader
	}

	// Get shards that this node was responsible for
	shardMap := m.GetShardMap()
	affectedShards := shardMap.GetNodeShards(nodeID)

	// Remove the node
	if err := m.RemoveNode(nodeID, 5*time.Second); err != nil {
		return fmt.Errorf("failed to remove node: %w", err)
	}

	// If the node was a primary for any shard, we need to elect a new primary
	for _, sr := range affectedShards {
		if sr.Role == RolePrimary {
			// Promote a replica to primary
			if err := m.promoteReplicaToPrimary(sr.ShardID); err != nil {
				m.logger.Error().Err(err).Int("shard_id", sr.ShardID).Msg("Failed to promote replica")
			}
		}
	}

	// Rebalance to maintain replication factor
	return m.rebalanceShards()
}

// promoteReplicaToPrimary promotes a healthy replica to primary for a shard.
func (m *MetaCluster) promoteReplicaToPrimary(shardID int) error {
	shardMap := m.GetShardMap()
	group := shardMap.GetShardGroup(shardID)
	if group == nil {
		return fmt.Errorf("shard %d not found", shardID)
	}

	// Find a healthy replica
	for _, replica := range group.Replicas {
		if replica.IsHealthy() {
			m.logger.Info().
				Int("shard_id", shardID).
				Str("new_primary", replica.ID).
				Msg("Promoting replica to primary")

			// First, assign as primary (this replaces any existing primary assignment)
			if err := m.AssignShard(shardID, replica.ID, RolePrimary, 5*time.Second); err != nil {
				return err
			}

			// Remove from replicas list
			return m.UnassignShard(shardID, replica.ID, 5*time.Second)
		}
	}

	m.logger.Warn().Int("shard_id", shardID).Msg("No healthy replica to promote")
	return nil
}

// rebalanceShards redistributes shards to maintain the target replication factor.
func (m *MetaCluster) rebalanceShards() error {
	shardMap := m.GetShardMap()
	nodes := m.GetNodes()
	numShards := shardMap.NumShards()
	targetRF := m.config.ReplicationFactor

	// Get healthy nodes
	healthyNodes := make([]*NodeInfo, 0)
	for _, node := range nodes {
		if node.State == string(cluster.StateHealthy) {
			healthyNodes = append(healthyNodes, node)
		}
	}

	if len(healthyNodes) == 0 {
		m.logger.Warn().Msg("No healthy nodes available for shard assignment")
		return nil
	}

	m.logger.Debug().
		Int("healthy_nodes", len(healthyNodes)).
		Int("num_shards", numShards).
		Int("target_rf", targetRF).
		Msg("Rebalancing shards")

	// For each shard, ensure we have a primary and enough replicas
	for shardID := 0; shardID < numShards; shardID++ {
		group := shardMap.GetShardGroup(shardID)

		// Assign primary if missing
		if group.Primary == nil {
			nodeID := m.selectNodeForShard(shardID, healthyNodes, group)
			if nodeID != "" {
				if err := m.AssignShard(shardID, nodeID, RolePrimary, 5*time.Second); err != nil {
					m.logger.Error().Err(err).Int("shard_id", shardID).Msg("Failed to assign primary")
				}
			}
		}

		// Add replicas up to target RF
		currentReplicas := len(group.Replicas)
		if group.Primary != nil {
			currentReplicas++ // Count primary as part of replication
		}

		for currentReplicas < targetRF && currentReplicas < len(healthyNodes) {
			nodeID := m.selectNodeForShard(shardID, healthyNodes, group)
			if nodeID == "" {
				break // No more suitable nodes
			}

			if err := m.AssignShard(shardID, nodeID, RoleReplica, 5*time.Second); err != nil {
				m.logger.Error().Err(err).Int("shard_id", shardID).Msg("Failed to assign replica")
				break
			}
			currentReplicas++

			// Refresh group to get updated replicas
			group = shardMap.GetShardGroup(shardID)
		}
	}

	return nil
}

// selectNodeForShard selects a node that doesn't already have this shard.
func (m *MetaCluster) selectNodeForShard(shardID int, nodes []*NodeInfo, group *ShardGroup) string {
	// Get existing node IDs for this shard
	existingNodes := make(map[string]bool)
	if group.Primary != nil {
		existingNodes[group.Primary.ID] = true
	}
	for _, r := range group.Replicas {
		existingNodes[r.ID] = true
	}

	// Find a node that doesn't have this shard yet
	// Prefer nodes with fewer shards for better distribution
	var bestNode *NodeInfo
	bestShardCount := -1

	shardMap := m.GetShardMap()
	for _, node := range nodes {
		if existingNodes[node.ID] {
			continue // Already has this shard
		}

		shardCount := len(shardMap.GetNodeShards(node.ID))
		if bestNode == nil || shardCount < bestShardCount {
			bestNode = node
			bestShardCount = shardCount
		}
	}

	if bestNode != nil {
		return bestNode.ID
	}
	return ""
}

// Status returns the current status of the meta cluster.
func (m *MetaCluster) Status() map[string]interface{} {
	shardMap := m.GetShardMap()
	nodes := m.GetNodes()

	shardStatus := make([]map[string]interface{}, 0, shardMap.NumShards())
	for i := 0; i < shardMap.NumShards(); i++ {
		group := shardMap.GetShardGroup(i)
		ss := map[string]interface{}{
			"shard_id":     i,
			"primary":      nil,
			"replicas":     []string{},
			"replica_count": len(group.Replicas),
		}
		if group.Primary != nil {
			ss["primary"] = group.Primary.ID
		}
		replicaIDs := make([]string, 0, len(group.Replicas))
		for _, r := range group.Replicas {
			replicaIDs = append(replicaIDs, r.ID)
		}
		ss["replicas"] = replicaIDs
		shardStatus = append(shardStatus, ss)
	}

	return map[string]interface{}{
		"is_leader":          m.IsLeader(),
		"leader_addr":        m.LeaderAddr(),
		"num_shards":         shardMap.NumShards(),
		"num_nodes":          len(nodes),
		"replication_factor": m.config.ReplicationFactor,
		"shard_map":          shardStatus,
		"shard_map_version":  shardMap.Version(),
	}
}

// Errors for the meta cluster.
var (
	ErrNotLeader = fmt.Errorf("not the meta cluster leader")
)
