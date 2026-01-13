package sharding

import (
	"encoding/json"
	"io"
	"sync"

	"github.com/basekick-labs/arc/internal/cluster"
	"github.com/hashicorp/raft"
	"github.com/rs/zerolog"
)

// MetaFSM is the finite state machine for the meta cluster.
// It manages the authoritative shard map across the cluster.
type MetaFSM struct {
	shardMap *ShardMap
	nodes    map[string]*NodeInfo // All known nodes
	mu       sync.RWMutex
	logger   zerolog.Logger

	// Callbacks for state changes
	onShardAssigned   func(shardID int, nodeID string, role Role)
	onShardUnassigned func(shardID int, nodeID string)
	onNodeAdded       func(node *NodeInfo)
	onNodeRemoved     func(nodeID string)
}

// NodeInfo represents node information stored in the meta FSM.
type NodeInfo struct {
	ID         string `json:"id"`
	Name       string `json:"name"`
	APIAddress string `json:"api_address"`
	CoordAddr  string `json:"coord_addr"`
	State      string `json:"state"`
	Version    string `json:"version"`
}

// Command types for the meta FSM.
const (
	CmdAssignShard   = "assign_shard"
	CmdUnassignShard = "unassign_shard"
	CmdAddNode       = "add_node"
	CmdRemoveNode    = "remove_node"
	CmdUpdateNode    = "update_node"
)

// MetaCommand represents a command to the meta FSM.
type MetaCommand struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

// AssignShardPayload is the payload for shard assignment commands.
type AssignShardPayload struct {
	ShardID int    `json:"shard_id"`
	NodeID  string `json:"node_id"`
	Role    Role   `json:"role"` // "primary" or "replica"
}

// UnassignShardPayload is the payload for shard unassignment commands.
type UnassignShardPayload struct {
	ShardID int    `json:"shard_id"`
	NodeID  string `json:"node_id"`
}

// AddNodePayload is the payload for adding a node.
type AddNodePayload struct {
	Node NodeInfo `json:"node"`
}

// RemoveNodePayload is the payload for removing a node.
type RemoveNodePayload struct {
	NodeID string `json:"node_id"`
}

// UpdateNodePayload is the payload for updating a node.
type UpdateNodePayload struct {
	NodeID string `json:"node_id"`
	State  string `json:"state,omitempty"`
}

// NewMetaFSM creates a new meta FSM.
func NewMetaFSM(numShards int, logger zerolog.Logger) *MetaFSM {
	return &MetaFSM{
		shardMap: NewShardMap(numShards),
		nodes:    make(map[string]*NodeInfo),
		logger:   logger.With().Str("component", "meta-fsm").Logger(),
	}
}

// SetCallbacks sets the callback functions for state changes.
func (f *MetaFSM) SetCallbacks(
	onShardAssigned func(shardID int, nodeID string, role Role),
	onShardUnassigned func(shardID int, nodeID string),
	onNodeAdded func(node *NodeInfo),
	onNodeRemoved func(nodeID string),
) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.onShardAssigned = onShardAssigned
	f.onShardUnassigned = onShardUnassigned
	f.onNodeAdded = onNodeAdded
	f.onNodeRemoved = onNodeRemoved
}

// Apply implements raft.FSM.
func (f *MetaFSM) Apply(log *raft.Log) interface{} {
	var cmd MetaCommand
	if err := json.Unmarshal(log.Data, &cmd); err != nil {
		f.logger.Error().Err(err).Msg("Failed to unmarshal meta command")
		return err
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	switch cmd.Type {
	case CmdAssignShard:
		return f.applyAssignShard(cmd.Payload)
	case CmdUnassignShard:
		return f.applyUnassignShard(cmd.Payload)
	case CmdAddNode:
		return f.applyAddNode(cmd.Payload)
	case CmdRemoveNode:
		return f.applyRemoveNode(cmd.Payload)
	case CmdUpdateNode:
		return f.applyUpdateNode(cmd.Payload)
	default:
		f.logger.Warn().Str("type", cmd.Type).Msg("Unknown command type")
		return nil
	}
}

func (f *MetaFSM) applyAssignShard(payload json.RawMessage) interface{} {
	var p AssignShardPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return err
	}

	// Get or create node from our registry
	nodeInfo, exists := f.nodes[p.NodeID]
	if !exists {
		f.logger.Warn().Str("node_id", p.NodeID).Msg("Node not found for shard assignment")
		return nil
	}

	// Create a cluster.Node for the shard map
	node := &cluster.Node{
		ID:         nodeInfo.ID,
		Name:       nodeInfo.Name,
		APIAddress: nodeInfo.APIAddress,
		Address:    nodeInfo.CoordAddr,
	}
	node.UpdateState(cluster.NodeState(nodeInfo.State))

	if p.Role == RolePrimary {
		f.shardMap.SetPrimary(p.ShardID, node)
	} else {
		f.shardMap.AddReplica(p.ShardID, node)
	}

	f.logger.Info().
		Int("shard_id", p.ShardID).
		Str("node_id", p.NodeID).
		Str("role", string(p.Role)).
		Msg("Shard assigned")

	if f.onShardAssigned != nil {
		go f.onShardAssigned(p.ShardID, p.NodeID, p.Role)
	}

	return nil
}

func (f *MetaFSM) applyUnassignShard(payload json.RawMessage) interface{} {
	var p UnassignShardPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return err
	}

	// Check if this node is the primary
	primary := f.shardMap.GetPrimary(p.ShardID)
	if primary != nil && primary.ID == p.NodeID {
		f.shardMap.SetPrimary(p.ShardID, nil)
	} else {
		f.shardMap.RemoveReplica(p.ShardID, p.NodeID)
	}

	f.logger.Info().
		Int("shard_id", p.ShardID).
		Str("node_id", p.NodeID).
		Msg("Shard unassigned")

	if f.onShardUnassigned != nil {
		go f.onShardUnassigned(p.ShardID, p.NodeID)
	}

	return nil
}

func (f *MetaFSM) applyAddNode(payload json.RawMessage) interface{} {
	var p AddNodePayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return err
	}

	f.nodes[p.Node.ID] = &p.Node

	f.logger.Info().
		Str("node_id", p.Node.ID).
		Str("api_address", p.Node.APIAddress).
		Msg("Node added to meta cluster")

	if f.onNodeAdded != nil {
		go f.onNodeAdded(&p.Node)
	}

	return nil
}

func (f *MetaFSM) applyRemoveNode(payload json.RawMessage) interface{} {
	var p RemoveNodePayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return err
	}

	delete(f.nodes, p.NodeID)

	// Also remove from all shard groups
	f.shardMap.RemoveNode(p.NodeID)

	f.logger.Info().
		Str("node_id", p.NodeID).
		Msg("Node removed from meta cluster")

	if f.onNodeRemoved != nil {
		go f.onNodeRemoved(p.NodeID)
	}

	return nil
}

func (f *MetaFSM) applyUpdateNode(payload json.RawMessage) interface{} {
	var p UpdateNodePayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return err
	}

	if nodeInfo, exists := f.nodes[p.NodeID]; exists {
		if p.State != "" {
			nodeInfo.State = p.State
		}
	}

	return nil
}

// Snapshot implements raft.FSM.
func (f *MetaFSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	// Create a snapshot of the current state
	snapshot := &MetaFSMSnapshot{
		NumShards: f.shardMap.NumShards(),
		Nodes:     make(map[string]*NodeInfo),
		Shards:    make(map[int]*ShardSnapshot),
	}

	// Copy nodes
	for id, node := range f.nodes {
		snapshot.Nodes[id] = &NodeInfo{
			ID:         node.ID,
			Name:       node.Name,
			APIAddress: node.APIAddress,
			CoordAddr:  node.CoordAddr,
			State:      node.State,
			Version:    node.Version,
		}
	}

	// Copy shard assignments
	for i := 0; i < f.shardMap.NumShards(); i++ {
		group := f.shardMap.GetShardGroup(i)
		if group == nil {
			continue
		}

		ss := &ShardSnapshot{
			ShardID:  i,
			Replicas: make([]string, 0, len(group.Replicas)),
		}
		if group.Primary != nil {
			ss.PrimaryID = group.Primary.ID
		}
		for _, r := range group.Replicas {
			ss.Replicas = append(ss.Replicas, r.ID)
		}
		snapshot.Shards[i] = ss
	}

	return snapshot, nil
}

// Restore implements raft.FSM.
func (f *MetaFSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()

	var snapshot MetaFSMSnapshot
	if err := json.NewDecoder(rc).Decode(&snapshot); err != nil {
		return err
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	// Restore nodes
	f.nodes = snapshot.Nodes

	// Recreate shard map
	f.shardMap = NewShardMap(snapshot.NumShards)

	// Restore shard assignments
	for _, ss := range snapshot.Shards {
		if ss.PrimaryID != "" {
			if nodeInfo, exists := f.nodes[ss.PrimaryID]; exists {
				node := &cluster.Node{
					ID:         nodeInfo.ID,
					Name:       nodeInfo.Name,
					APIAddress: nodeInfo.APIAddress,
					Address:    nodeInfo.CoordAddr,
				}
				node.UpdateState(cluster.NodeState(nodeInfo.State))
				f.shardMap.SetPrimary(ss.ShardID, node)
			}
		}
		for _, replicaID := range ss.Replicas {
			if nodeInfo, exists := f.nodes[replicaID]; exists {
				node := &cluster.Node{
					ID:         nodeInfo.ID,
					Name:       nodeInfo.Name,
					APIAddress: nodeInfo.APIAddress,
					Address:    nodeInfo.CoordAddr,
				}
				node.UpdateState(cluster.NodeState(nodeInfo.State))
				f.shardMap.AddReplica(ss.ShardID, node)
			}
		}
	}

	f.logger.Info().
		Int("num_shards", snapshot.NumShards).
		Int("num_nodes", len(snapshot.Nodes)).
		Msg("Meta FSM restored from snapshot")

	return nil
}

// GetShardMap returns the current shard map (read-only).
func (f *MetaFSM) GetShardMap() *ShardMap {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.shardMap
}

// GetNodes returns all known nodes.
func (f *MetaFSM) GetNodes() map[string]*NodeInfo {
	f.mu.RLock()
	defer f.mu.RUnlock()

	result := make(map[string]*NodeInfo, len(f.nodes))
	for id, node := range f.nodes {
		result[id] = &NodeInfo{
			ID:         node.ID,
			Name:       node.Name,
			APIAddress: node.APIAddress,
			CoordAddr:  node.CoordAddr,
			State:      node.State,
			Version:    node.Version,
		}
	}
	return result
}

// GetNode returns a specific node by ID.
func (f *MetaFSM) GetNode(nodeID string) (*NodeInfo, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	node, exists := f.nodes[nodeID]
	if !exists {
		return nil, false
	}
	return &NodeInfo{
		ID:         node.ID,
		Name:       node.Name,
		APIAddress: node.APIAddress,
		CoordAddr:  node.CoordAddr,
		State:      node.State,
		Version:    node.Version,
	}, true
}

// MetaFSMSnapshot is a snapshot of the meta FSM state.
type MetaFSMSnapshot struct {
	NumShards int                    `json:"num_shards"`
	Nodes     map[string]*NodeInfo   `json:"nodes"`
	Shards    map[int]*ShardSnapshot `json:"shards"`
}

// ShardSnapshot represents a shard's assignment state.
type ShardSnapshot struct {
	ShardID   int      `json:"shard_id"`
	PrimaryID string   `json:"primary_id,omitempty"`
	Replicas  []string `json:"replicas,omitempty"`
}

// Persist implements raft.FSMSnapshot.
func (s *MetaFSMSnapshot) Persist(sink raft.SnapshotSink) error {
	err := json.NewEncoder(sink).Encode(s)
	if err != nil {
		sink.Cancel()
		return err
	}
	return sink.Close()
}

// Release implements raft.FSMSnapshot.
func (s *MetaFSMSnapshot) Release() {}
