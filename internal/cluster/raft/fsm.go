package raft

import (
	"encoding/json"
	"fmt"
	"io"
	"sync"

	"github.com/hashicorp/raft"
	"github.com/rs/zerolog"
)

// CommandType represents the type of FSM command.
type CommandType uint8

const (
	// CommandAddNode adds a node to the cluster.
	CommandAddNode CommandType = iota + 1
	// CommandRemoveNode removes a node from the cluster.
	CommandRemoveNode
	// CommandUpdateNode updates node information.
	CommandUpdateNode
	// CommandUpdateNodeState updates a node's state.
	CommandUpdateNodeState
	// CommandPromoteWriter promotes a writer node to primary.
	CommandPromoteWriter
	// CommandDemoteWriter demotes a writer node to standby.
	CommandDemoteWriter
)

// Command represents a command to be applied to the FSM.
type Command struct {
	Type    CommandType `json:"type"`
	Payload []byte      `json:"payload"`
}

// NodeInfo represents node information stored in the FSM.
type NodeInfo struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Role        string `json:"role"`
	ClusterName string `json:"cluster_name"`
	Address     string `json:"address"`
	APIAddress  string `json:"api_address"`
	State       string `json:"state"`
	Version     string `json:"version"`
	WriterState string `json:"writer_state,omitempty"` // "primary", "standby", or "" for non-writers
	CoreCount   int    `json:"core_count"`             // Number of CPU cores on this node
}

// AddNodePayload is the payload for CommandAddNode.
type AddNodePayload struct {
	Node NodeInfo `json:"node"`
}

// RemoveNodePayload is the payload for CommandRemoveNode.
type RemoveNodePayload struct {
	NodeID string `json:"node_id"`
}

// UpdateNodePayload is the payload for CommandUpdateNode.
type UpdateNodePayload struct {
	Node NodeInfo `json:"node"`
}

// UpdateNodeStatePayload is the payload for CommandUpdateNodeState.
type UpdateNodeStatePayload struct {
	NodeID   string `json:"node_id"`
	NewState string `json:"new_state"`
}

// PromoteWriterPayload is the payload for CommandPromoteWriter.
type PromoteWriterPayload struct {
	NodeID       string `json:"node_id"`        // Node to promote to primary
	OldPrimaryID string `json:"old_primary_id"` // Previous primary (to demote)
}

// DemoteWriterPayload is the payload for CommandDemoteWriter.
type DemoteWriterPayload struct {
	NodeID string `json:"node_id"` // Node to demote to standby
}

// FSMSnapshot represents a snapshot of the FSM state.
type FSMSnapshot struct {
	Nodes           map[string]*NodeInfo `json:"nodes"`
	PrimaryWriterID string               `json:"primary_writer_id,omitempty"`
}

// ClusterFSM implements the raft.FSM interface for cluster state management.
// It maintains the authoritative state of nodes in the cluster.
type ClusterFSM struct {
	mu              sync.RWMutex
	nodes           map[string]*NodeInfo
	primaryWriterID string // ID of the current primary writer node
	logger          zerolog.Logger

	// Callbacks for state changes
	onNodeAdded      func(*NodeInfo)
	onNodeRemoved    func(string)
	onNodeUpdated    func(*NodeInfo)
	onWriterPromoted func(newPrimaryID, oldPrimaryID string)
}

// NewClusterFSM creates a new cluster FSM.
func NewClusterFSM(logger zerolog.Logger) *ClusterFSM {
	return &ClusterFSM{
		nodes:  make(map[string]*NodeInfo),
		logger: logger.With().Str("component", "cluster-fsm").Logger(),
	}
}

// SetCallbacks sets the FSM callbacks for state changes.
func (f *ClusterFSM) SetCallbacks(onAdded func(*NodeInfo), onRemoved func(string), onUpdated func(*NodeInfo)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.onNodeAdded = onAdded
	f.onNodeRemoved = onRemoved
	f.onNodeUpdated = onUpdated
}

// SetWriterPromotedCallback sets the callback for writer promotion events.
func (f *ClusterFSM) SetWriterPromotedCallback(cb func(newPrimaryID, oldPrimaryID string)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.onWriterPromoted = cb
}

// GetPrimaryWriterID returns the current primary writer node ID.
func (f *ClusterFSM) GetPrimaryWriterID() string {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.primaryWriterID
}

// Apply applies a Raft log entry to the FSM.
// This is called by Raft when a log entry is committed.
func (f *ClusterFSM) Apply(log *raft.Log) interface{} {
	var cmd Command
	if err := json.Unmarshal(log.Data, &cmd); err != nil {
		f.logger.Error().Err(err).Msg("Failed to unmarshal command")
		return fmt.Errorf("failed to unmarshal command: %w", err)
	}

	switch cmd.Type {
	case CommandAddNode:
		return f.applyAddNode(cmd.Payload)
	case CommandRemoveNode:
		return f.applyRemoveNode(cmd.Payload)
	case CommandUpdateNode:
		return f.applyUpdateNode(cmd.Payload)
	case CommandUpdateNodeState:
		return f.applyUpdateNodeState(cmd.Payload)
	case CommandPromoteWriter:
		return f.applyPromoteWriter(cmd.Payload)
	case CommandDemoteWriter:
		return f.applyDemoteWriter(cmd.Payload)
	default:
		return fmt.Errorf("unknown command type: %d", cmd.Type)
	}
}

func (f *ClusterFSM) applyAddNode(payload []byte) interface{} {
	var p AddNodePayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("failed to unmarshal add node payload: %w", err)
	}

	f.mu.Lock()
	f.nodes[p.Node.ID] = &p.Node
	callback := f.onNodeAdded
	f.mu.Unlock()

	f.logger.Info().
		Str("node_id", p.Node.ID).
		Str("role", p.Node.Role).
		Msg("Node added to cluster state")

	if callback != nil {
		callback(&p.Node)
	}

	return nil
}

func (f *ClusterFSM) applyRemoveNode(payload []byte) interface{} {
	var p RemoveNodePayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("failed to unmarshal remove node payload: %w", err)
	}

	f.mu.Lock()
	delete(f.nodes, p.NodeID)
	callback := f.onNodeRemoved
	f.mu.Unlock()

	f.logger.Info().
		Str("node_id", p.NodeID).
		Msg("Node removed from cluster state")

	if callback != nil {
		callback(p.NodeID)
	}

	return nil
}

func (f *ClusterFSM) applyUpdateNode(payload []byte) interface{} {
	var p UpdateNodePayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("failed to unmarshal update node payload: %w", err)
	}

	f.mu.Lock()
	f.nodes[p.Node.ID] = &p.Node
	callback := f.onNodeUpdated
	f.mu.Unlock()

	f.logger.Debug().
		Str("node_id", p.Node.ID).
		Msg("Node updated in cluster state")

	if callback != nil {
		callback(&p.Node)
	}

	return nil
}

func (f *ClusterFSM) applyUpdateNodeState(payload []byte) interface{} {
	var p UpdateNodeStatePayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("failed to unmarshal update node state payload: %w", err)
	}

	f.mu.Lock()
	node, exists := f.nodes[p.NodeID]
	if exists {
		node.State = p.NewState
	}
	callback := f.onNodeUpdated
	f.mu.Unlock()

	if !exists {
		return fmt.Errorf("node %s not found", p.NodeID)
	}

	f.logger.Debug().
		Str("node_id", p.NodeID).
		Str("new_state", p.NewState).
		Msg("Node state updated")

	if callback != nil {
		callback(node)
	}

	return nil
}

func (f *ClusterFSM) applyPromoteWriter(payload []byte) interface{} {
	var p PromoteWriterPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("failed to unmarshal promote writer payload: %w", err)
	}

	if p.NodeID == "" {
		return fmt.Errorf("promote writer: node_id is required")
	}

	f.mu.Lock()
	// Validate the node exists and is a writer
	if node, exists := f.nodes[p.NodeID]; exists && node.Role != "writer" {
		f.mu.Unlock()
		return fmt.Errorf("promote writer: node %s has role %s, expected writer", p.NodeID, node.Role)
	}

	// Warn if OldPrimaryID doesn't match actual primary (informational only — FSM uses its own tracking)
	oldPrimaryID := f.primaryWriterID
	if p.OldPrimaryID != "" && oldPrimaryID != "" && p.OldPrimaryID != oldPrimaryID {
		f.logger.Warn().
			Str("expected_old_primary", p.OldPrimaryID).
			Str("actual_old_primary", oldPrimaryID).
			Msg("OldPrimaryID mismatch during promotion")
	}
	if oldPrimaryID != "" && oldPrimaryID != p.NodeID {
		if oldNode, exists := f.nodes[oldPrimaryID]; exists {
			oldNode.WriterState = "standby"
		}
	}

	// Promote new primary
	newNode, exists := f.nodes[p.NodeID]
	if exists {
		newNode.WriterState = "primary"
	}
	f.primaryWriterID = p.NodeID
	callback := f.onWriterPromoted
	f.mu.Unlock()

	if !exists {
		return fmt.Errorf("node %s not found", p.NodeID)
	}

	f.logger.Info().
		Str("new_primary", p.NodeID).
		Str("old_primary", oldPrimaryID).
		Msg("Writer promoted to primary")

	if callback != nil {
		callback(p.NodeID, oldPrimaryID)
	}

	return nil
}

func (f *ClusterFSM) applyDemoteWriter(payload []byte) interface{} {
	var p DemoteWriterPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("failed to unmarshal demote writer payload: %w", err)
	}

	if p.NodeID == "" {
		return fmt.Errorf("demote writer: node_id is required")
	}

	f.mu.Lock()
	node, exists := f.nodes[p.NodeID]
	if exists {
		node.WriterState = "standby"
	}
	if f.primaryWriterID == p.NodeID {
		f.primaryWriterID = ""
	}
	f.mu.Unlock()

	if !exists {
		return fmt.Errorf("node %s not found", p.NodeID)
	}

	f.logger.Info().
		Str("node_id", p.NodeID).
		Msg("Writer demoted to standby")

	return nil
}

// Snapshot returns a snapshot of the FSM state.
func (f *ClusterFSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	// Deep copy nodes
	nodes := make(map[string]*NodeInfo, len(f.nodes))
	for id, node := range f.nodes {
		nodeCopy := *node
		nodes[id] = &nodeCopy
	}

	return &fsmSnapshot{nodes: nodes, primaryWriterID: f.primaryWriterID}, nil
}

// Restore restores the FSM from a snapshot.
func (f *ClusterFSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()

	var snapshot FSMSnapshot
	if err := json.NewDecoder(rc).Decode(&snapshot); err != nil {
		return fmt.Errorf("failed to decode snapshot: %w", err)
	}

	f.mu.Lock()
	f.nodes = snapshot.Nodes
	f.primaryWriterID = snapshot.PrimaryWriterID
	f.mu.Unlock()

	f.logger.Info().
		Int("node_count", len(snapshot.Nodes)).
		Str("primary_writer", snapshot.PrimaryWriterID).
		Msg("FSM restored from snapshot")

	return nil
}

// GetNode returns a copy of the node info for the given ID.
func (f *ClusterFSM) GetNode(nodeID string) (*NodeInfo, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	node, exists := f.nodes[nodeID]
	if !exists {
		return nil, false
	}
	nodeCopy := *node
	return &nodeCopy, true
}

// GetAllNodes returns copies of all nodes.
func (f *ClusterFSM) GetAllNodes() []*NodeInfo {
	f.mu.RLock()
	defer f.mu.RUnlock()
	nodes := make([]*NodeInfo, 0, len(f.nodes))
	for _, node := range f.nodes {
		nodeCopy := *node
		nodes = append(nodes, &nodeCopy)
	}
	return nodes
}

// GetNodesByRole returns copies of all nodes with the given role.
func (f *ClusterFSM) GetNodesByRole(role string) []*NodeInfo {
	f.mu.RLock()
	defer f.mu.RUnlock()
	var nodes []*NodeInfo
	for _, node := range f.nodes {
		if node.Role == role {
			nodeCopy := *node
			nodes = append(nodes, &nodeCopy)
		}
	}
	return nodes
}

// NodeCount returns the number of nodes in the FSM.
func (f *ClusterFSM) NodeCount() int {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return len(f.nodes)
}

// TotalCores returns the sum of CoreCount across all nodes in the FSM.
func (f *ClusterFSM) TotalCores() int {
	f.mu.RLock()
	defer f.mu.RUnlock()
	total := 0
	for _, node := range f.nodes {
		total += node.CoreCount
	}
	return total
}

// fsmSnapshot implements raft.FSMSnapshot.
type fsmSnapshot struct {
	nodes           map[string]*NodeInfo
	primaryWriterID string
}

// Persist writes the snapshot to the given sink.
func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	snapshot := FSMSnapshot{Nodes: s.nodes, PrimaryWriterID: s.primaryWriterID}

	data, err := json.Marshal(snapshot)
	if err != nil {
		sink.Cancel()
		return fmt.Errorf("failed to marshal snapshot: %w", err)
	}

	if _, err := sink.Write(data); err != nil {
		sink.Cancel()
		return fmt.Errorf("failed to write snapshot: %w", err)
	}

	return sink.Close()
}

// Release is called when the snapshot is no longer needed.
func (s *fsmSnapshot) Release() {}
