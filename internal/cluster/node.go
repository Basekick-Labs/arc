package cluster

import (
	"sync"
	"time"
)

// NodeState represents the health state of a node in the cluster.
type NodeState string

const (
	// StateUnknown is the initial state before health is determined.
	StateUnknown NodeState = "unknown"

	// StateHealthy indicates the node is operating normally.
	StateHealthy NodeState = "healthy"

	// StateUnhealthy indicates the node has failed health checks but may recover.
	StateUnhealthy NodeState = "unhealthy"

	// StateDead indicates the node has been unreachable for an extended period.
	StateDead NodeState = "dead"

	// StateJoining indicates the node is joining the cluster.
	StateJoining NodeState = "joining"

	// StateLeaving indicates the node is gracefully leaving the cluster.
	StateLeaving NodeState = "leaving"
)

// String returns the string representation of the state.
func (s NodeState) String() string {
	return string(s)
}

// WriterState represents whether a writer node is primary or standby.
type WriterState string

const (
	// WriterStatePrimary indicates this writer is the active primary accepting writes.
	WriterStatePrimary WriterState = "primary"

	// WriterStateStandby indicates this writer is a hot standby ready for promotion.
	WriterStateStandby WriterState = "standby"

	// WriterStateNone indicates the node is not a writer or has no assigned writer state.
	WriterStateNone WriterState = ""
)

// NodeStats contains runtime statistics for a node.
// These are updated periodically via heartbeat.
type NodeStats struct {
	CPUUsage       float64 `json:"cpu_usage"`       // CPU usage percentage (0-100)
	MemoryUsage    float64 `json:"memory_usage"`    // Memory usage percentage (0-100)
	IngestRate     int64   `json:"ingest_rate"`     // Records ingested per second
	QueryRate      int64   `json:"query_rate"`      // Queries executed per second
	StorageUsed    int64   `json:"storage_used"`    // Bytes used in storage
	Connections    int     `json:"connections"`     // Active HTTP connections
	ActiveQueries  int     `json:"active_queries"`  // Currently running queries
	CompactionJobs int     `json:"compaction_jobs"` // Active compaction jobs (for compactor role)
}

// Node represents a node in the Arc cluster.
type Node struct {
	// Identity
	ID          string   `json:"id"`           // Unique node identifier
	Name        string   `json:"name"`         // Human-readable name
	Role        NodeRole `json:"role"`         // Node role (writer, reader, compactor, standalone)
	ClusterName string   `json:"cluster_name"` // Name of the cluster this node belongs to

	// Network
	Address    string `json:"address"`     // Coordinator address (host:port for inter-node communication)
	APIAddress string `json:"api_address"` // HTTP API address (host:port for client requests)

	// State
	State NodeState `json:"state"` // Current health state

	// Health tracking
	LastHeartbeat time.Time `json:"last_heartbeat"` // Time of last successful heartbeat
	LastHealthy   time.Time `json:"last_healthy"`   // Time node was last known healthy
	FailedChecks  int       `json:"failed_checks"`  // Consecutive failed health checks

	// Metadata
	Version   string    `json:"version"`    // Arc version running on this node
	StartedAt time.Time `json:"started_at"` // When the node process started
	JoinedAt  time.Time `json:"joined_at"`  // When the node joined the cluster

	// Writer state (primary/standby) - only relevant for writer nodes
	WriterSt WriterState `json:"writer_state,omitempty"`

	// Runtime stats (updated via heartbeat)
	Stats NodeStats `json:"stats"`

	mu sync.RWMutex
}

// NewNode creates a new Node with the given parameters.
func NewNode(id, name string, role NodeRole, clusterName string) *Node {
	now := time.Now()
	return &Node{
		ID:          id,
		Name:        name,
		Role:        role,
		ClusterName: clusterName,
		State:       StateUnknown,
		StartedAt:   now,
	}
}

// IsHealthy returns true if the node is in a healthy state.
func (n *Node) IsHealthy() bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.State == StateHealthy
}

// IsAvailable returns true if the node can accept requests.
// A node is available if it's healthy or just joining (warming up).
func (n *Node) IsAvailable() bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.State == StateHealthy || n.State == StateJoining
}

// GetState returns the current node state.
func (n *Node) GetState() NodeState {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.State
}

// UpdateState updates the node state.
func (n *Node) UpdateState(state NodeState) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.State = state
	if state == StateHealthy {
		n.LastHealthy = time.Now()
	}
}

// RecordHeartbeat records a successful heartbeat from the node.
func (n *Node) RecordHeartbeat(stats NodeStats) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.LastHeartbeat = time.Now()
	n.Stats = stats
	n.FailedChecks = 0
	if n.State == StateHealthy {
		n.LastHealthy = time.Now()
	}
}

// RecordFailedCheck records a failed health check.
// Returns the new count of consecutive failed checks.
func (n *Node) RecordFailedCheck() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.FailedChecks++
	return n.FailedChecks
}

// GetFailedChecks returns the number of consecutive failed health checks.
func (n *Node) GetFailedChecks() int {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.FailedChecks
}

// GetLastHeartbeat returns the time of the last successful heartbeat.
func (n *Node) GetLastHeartbeat() time.Time {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.LastHeartbeat
}

// GetStats returns a copy of the node's current stats.
func (n *Node) GetStats() NodeStats {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.Stats
}

// SetAddresses sets the node's network addresses.
func (n *Node) SetAddresses(coordinatorAddr, apiAddr string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.Address = coordinatorAddr
	n.APIAddress = apiAddr
}

// SetVersion sets the node's Arc version.
func (n *Node) SetVersion(version string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.Version = version
}

// MarkJoined marks the node as having joined the cluster.
func (n *Node) MarkJoined() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.JoinedAt = time.Now()
	n.State = StateHealthy
	n.LastHealthy = time.Now()
}

// GetWriterState returns the writer state of this node.
func (n *Node) GetWriterState() WriterState {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.WriterSt
}

// SetWriterState sets the writer state of this node.
func (n *Node) SetWriterState(state WriterState) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.WriterSt = state
}

// IsPrimaryWriter returns true if this node is the primary writer.
func (n *Node) IsPrimaryWriter() bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.Role == RoleWriter && n.WriterSt == WriterStatePrimary
}

// GetCapabilities returns the capabilities for this node's role.
func (n *Node) GetCapabilities() RoleCapabilities {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.Role.GetCapabilities()
}

// Clone returns a deep copy of the node (without the mutex).
func (n *Node) Clone() *Node {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return &Node{
		ID:            n.ID,
		Name:          n.Name,
		Role:          n.Role,
		ClusterName:   n.ClusterName,
		Address:       n.Address,
		APIAddress:    n.APIAddress,
		State:         n.State,
		WriterSt:      n.WriterSt,
		LastHeartbeat: n.LastHeartbeat,
		LastHealthy:   n.LastHealthy,
		FailedChecks:  n.FailedChecks,
		Version:       n.Version,
		StartedAt:     n.StartedAt,
		JoinedAt:      n.JoinedAt,
		Stats:         n.Stats,
	}
}

// StateTransition represents a node state change.
type StateTransition struct {
	OldState     NodeState
	NewState     NodeState
	FailedChecks int
}

// ProcessHealthCheckResult atomically processes a health check result and returns
// any state transition that occurred. This prevents TOCTOU race conditions by
// performing the check and update under a single lock.
func (n *Node) ProcessHealthCheckResult(healthy bool, unhealthyThreshold, deadThreshold int) *StateTransition {
	n.mu.Lock()
	defer n.mu.Unlock()

	oldState := n.State

	if healthy {
		// Reset failed checks and mark healthy
		n.FailedChecks = 0
		n.LastHeartbeat = time.Now()

		if oldState != StateHealthy && oldState != StateJoining {
			n.State = StateHealthy
			n.LastHealthy = time.Now()
			return &StateTransition{OldState: oldState, NewState: StateHealthy, FailedChecks: 0}
		}
		return nil
	}

	// Record failed check
	n.FailedChecks++

	// Check if we should transition to unhealthy
	if n.FailedChecks >= unhealthyThreshold && oldState == StateHealthy {
		n.State = StateUnhealthy
		return &StateTransition{OldState: oldState, NewState: StateUnhealthy, FailedChecks: n.FailedChecks}
	}

	// Check if we should transition to dead
	if n.FailedChecks >= deadThreshold && oldState != StateDead {
		n.State = StateDead
		return &StateTransition{OldState: oldState, NewState: StateDead, FailedChecks: n.FailedChecks}
	}

	return nil
}
