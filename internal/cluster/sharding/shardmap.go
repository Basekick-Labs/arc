package sharding

import (
	"hash/fnv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/basekick-labs/arc/internal/cluster"
)

// ShardMap maintains the mapping of shards to nodes.
// It is the central registry for shard assignments in the cluster.
type ShardMap struct {
	numShards   int
	assignments map[int]*ShardGroup
	version     atomic.Uint64
	mu          sync.RWMutex
}

// ShardGroup represents a group of nodes responsible for a single shard.
// Each shard has one primary (accepts writes) and zero or more replicas.
type ShardGroup struct {
	ShardID        int
	Primary        *cluster.Node
	Replicas       []*cluster.Node
	ReplicationLag map[string]time.Duration // nodeID -> lag
}

// NewShardMap creates a new shard map with the specified number of shards.
func NewShardMap(numShards int) *ShardMap {
	if numShards <= 0 {
		numShards = 1
	}

	sm := &ShardMap{
		numShards:   numShards,
		assignments: make(map[int]*ShardGroup, numShards),
	}

	// Initialize empty shard groups
	for i := 0; i < numShards; i++ {
		sm.assignments[i] = &ShardGroup{
			ShardID:        i,
			Replicas:       make([]*cluster.Node, 0),
			ReplicationLag: make(map[string]time.Duration),
		}
	}

	return sm
}

// GetShard returns the shard ID for a given database name using FNV-1a hash.
func (sm *ShardMap) GetShard(database string) int {
	h := fnv.New32a()
	h.Write([]byte(database))
	return int(h.Sum32() % uint32(sm.numShards))
}

// GetPrimary returns the primary node for a shard, or nil if not assigned.
func (sm *ShardMap) GetPrimary(shardID int) *cluster.Node {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	if group, ok := sm.assignments[shardID]; ok {
		return group.Primary
	}
	return nil
}

// GetShardGroup returns the full shard group for a shard ID.
func (sm *ShardMap) GetShardGroup(shardID int) *ShardGroup {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	if group, ok := sm.assignments[shardID]; ok {
		// Return a copy to prevent external modification
		return &ShardGroup{
			ShardID:        group.ShardID,
			Primary:        group.Primary,
			Replicas:       append([]*cluster.Node{}, group.Replicas...),
			ReplicationLag: copyLagMap(group.ReplicationLag),
		}
	}
	return nil
}

// SetPrimary sets the primary node for a shard.
func (sm *ShardMap) SetPrimary(shardID int, node *cluster.Node) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if group, ok := sm.assignments[shardID]; ok {
		group.Primary = node
		sm.version.Add(1)
	}
}

// AddReplica adds a replica node to a shard group.
func (sm *ShardMap) AddReplica(shardID int, node *cluster.Node) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if group, ok := sm.assignments[shardID]; ok {
		// Check if already a replica
		for _, r := range group.Replicas {
			if r.ID == node.ID {
				return
			}
		}
		group.Replicas = append(group.Replicas, node)
		sm.version.Add(1)
	}
}

// RemoveReplica removes a replica node from a shard group.
func (sm *ShardMap) RemoveReplica(shardID int, nodeID string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if group, ok := sm.assignments[shardID]; ok {
		for i, r := range group.Replicas {
			if r.ID == nodeID {
				group.Replicas = append(group.Replicas[:i], group.Replicas[i+1:]...)
				delete(group.ReplicationLag, nodeID)
				sm.version.Add(1)
				return
			}
		}
	}
}

// UpdateReplicationLag updates the replication lag for a replica.
func (sm *ShardMap) UpdateReplicationLag(shardID int, nodeID string, lag time.Duration) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if group, ok := sm.assignments[shardID]; ok {
		group.ReplicationLag[nodeID] = lag
	}
}

// SelectNode selects a node from the shard group for reading.
// It can return either the primary or any healthy replica.
func (sm *ShardMap) SelectNode(shardID int) *cluster.Node {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	group, ok := sm.assignments[shardID]
	if !ok {
		return nil
	}

	// If we have healthy replicas, prefer them for reads
	for _, replica := range group.Replicas {
		if replica.IsHealthy() {
			return replica
		}
	}

	// Fall back to primary
	return group.Primary
}

// GetAllNodes returns all nodes involved in a shard (primary + replicas).
func (sm *ShardMap) GetAllNodes(shardID int) []*cluster.Node {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	group, ok := sm.assignments[shardID]
	if !ok {
		return nil
	}

	nodes := make([]*cluster.Node, 0, len(group.Replicas)+1)
	if group.Primary != nil {
		nodes = append(nodes, group.Primary)
	}
	nodes = append(nodes, group.Replicas...)
	return nodes
}

// NumShards returns the total number of shards.
func (sm *ShardMap) NumShards() int {
	return sm.numShards
}

// Version returns the current version of the shard map.
// Incremented on every modification.
func (sm *ShardMap) Version() uint64 {
	return sm.version.Load()
}

// GetNodeShards returns all shards where a node is primary or replica.
func (sm *ShardMap) GetNodeShards(nodeID string) []ShardRole {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	var roles []ShardRole
	for shardID, group := range sm.assignments {
		if group.Primary != nil && group.Primary.ID == nodeID {
			roles = append(roles, ShardRole{ShardID: shardID, Role: RolePrimary})
		}
		for _, replica := range group.Replicas {
			if replica.ID == nodeID {
				roles = append(roles, ShardRole{ShardID: shardID, Role: RoleReplica})
			}
		}
	}
	return roles
}

// RemoveNode removes a node from all shard groups.
// Returns list of shards where the node was primary (need failover).
func (sm *ShardMap) RemoveNode(nodeID string) []int {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	var primaryShards []int

	for shardID, group := range sm.assignments {
		// Check if primary
		if group.Primary != nil && group.Primary.ID == nodeID {
			group.Primary = nil
			primaryShards = append(primaryShards, shardID)
		}

		// Remove from replicas
		for i, r := range group.Replicas {
			if r.ID == nodeID {
				group.Replicas = append(group.Replicas[:i], group.Replicas[i+1:]...)
				delete(group.ReplicationLag, nodeID)
				break
			}
		}
	}

	if len(primaryShards) > 0 {
		sm.version.Add(1)
	}

	return primaryShards
}

// Stats returns statistics about the shard map.
func (sm *ShardMap) Stats() map[string]interface{} {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	primaryCount := 0
	replicaCount := 0
	unassignedCount := 0

	for _, group := range sm.assignments {
		if group.Primary != nil {
			primaryCount++
		} else {
			unassignedCount++
		}
		replicaCount += len(group.Replicas)
	}

	return map[string]interface{}{
		"num_shards":       sm.numShards,
		"assigned_shards":  primaryCount,
		"unassigned":       unassignedCount,
		"total_replicas":   replicaCount,
		"version":          sm.version.Load(),
	}
}

// ShardRole represents a node's role in a shard.
type ShardRole struct {
	ShardID int
	Role    Role
}

// Role defines a node's role within a shard group.
type Role string

const (
	RolePrimary Role = "primary"
	RoleReplica Role = "replica"
)

func copyLagMap(m map[string]time.Duration) map[string]time.Duration {
	result := make(map[string]time.Duration, len(m))
	for k, v := range m {
		result[k] = v
	}
	return result
}
