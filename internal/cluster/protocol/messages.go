package protocol

import "time"

// MessageType identifies the type of cluster protocol message.
type MessageType uint8

const (
	// MsgJoinRequest is sent by a new node wanting to join the cluster.
	MsgJoinRequest MessageType = iota + 1
	// MsgJoinResponse is sent by the leader after processing a join request.
	MsgJoinResponse
	// MsgLeaderInfo is sent when a non-leader receives a join request.
	MsgLeaderInfo
	// MsgHeartbeat is sent periodically to maintain connection.
	MsgHeartbeat
	// MsgHeartbeatAck acknowledges a heartbeat.
	MsgHeartbeatAck
	// MsgLeaveNotify is sent when a node is gracefully leaving.
	MsgLeaveNotify

	// WAL Replication messages (Phase 3.3) - start at 0x10 to avoid conflicts
	// MsgReplicateSync is sent by a reader to request replication sync.
	MsgReplicateSync MessageType = 0x10
	// MsgReplicateSyncAck is sent by the writer in response to sync request.
	MsgReplicateSyncAck MessageType = 0x11
)

// String returns the string representation of a message type.
func (m MessageType) String() string {
	switch m {
	case MsgJoinRequest:
		return "JoinRequest"
	case MsgJoinResponse:
		return "JoinResponse"
	case MsgLeaderInfo:
		return "LeaderInfo"
	case MsgHeartbeat:
		return "Heartbeat"
	case MsgHeartbeatAck:
		return "HeartbeatAck"
	case MsgLeaveNotify:
		return "LeaveNotify"
	case MsgReplicateSync:
		return "ReplicateSync"
	case MsgReplicateSyncAck:
		return "ReplicateSyncAck"
	default:
		return "Unknown"
	}
}

// ReplicateSync is sent by a reader node to request WAL replication.
type ReplicateSync struct {
	ReaderID          string `json:"reader_id"`
	LastKnownSequence uint64 `json:"last_known_seq"`
}

// ReplicateSyncAck is sent by the writer in response to a sync request.
type ReplicateSyncAck struct {
	CurrentSequence uint64 `json:"current_seq"`
	CanResume       bool   `json:"can_resume"`
	Error           string `json:"error,omitempty"`
}

// JoinRequest is sent by a new node to join the cluster.
type JoinRequest struct {
	NodeID      string `json:"node_id"`
	NodeName    string `json:"node_name"`
	Role        string `json:"role"`
	ClusterName string `json:"cluster_name"`
	RaftAddr    string `json:"raft_addr"`    // Raft transport address for AddVoter
	APIAddr     string `json:"api_addr"`     // HTTP API address
	CoordAddr   string `json:"coord_addr"`   // Coordinator address (for peer communication)
	Version     string `json:"version"`
	CoreCount   int    `json:"core_count"`   // Number of CPU cores (GOMAXPROCS) on this node
}

// NodeInfo represents a node in the cluster (used in JoinResponse).
type NodeInfo struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	Role      string `json:"role"`
	State     string `json:"state"`
	RaftAddr  string `json:"raft_addr"`
	APIAddr   string `json:"api_addr"`
	CoordAddr string `json:"coord_addr"`
	CoreCount int    `json:"core_count"` // Number of CPU cores on this node
}

// JoinResponse is sent by the leader after processing a join request.
type JoinResponse struct {
	Success    bool       `json:"success"`
	LeaderID   string     `json:"leader_id"`
	LeaderAddr string     `json:"leader_addr"` // Leader's coordinator address
	RaftLeader string     `json:"raft_leader"` // Leader's Raft address
	Nodes      []NodeInfo `json:"nodes"`       // Current cluster members
	Error      string     `json:"error,omitempty"`
}

// LeaderInfo is sent when a non-leader receives a join request,
// redirecting the requester to the current leader.
type LeaderInfo struct {
	LeaderID        string `json:"leader_id"`
	LeaderCoordAddr string `json:"leader_coord_addr"` // Leader's coordinator address
	LeaderRaftAddr  string `json:"leader_raft_addr"`  // Leader's Raft address
}

// Heartbeat is sent periodically to maintain connection and share state.
type Heartbeat struct {
	NodeID    string    `json:"node_id"`
	State     string    `json:"state"`
	IsLeader  bool      `json:"is_leader"`
	Timestamp time.Time `json:"timestamp"`
}

// HeartbeatAck acknowledges a heartbeat.
type HeartbeatAck struct {
	NodeID    string    `json:"node_id"`
	Timestamp time.Time `json:"timestamp"`
}

// LeaveNotify is sent when a node is gracefully leaving the cluster.
type LeaveNotify struct {
	NodeID string `json:"node_id"`
	Reason string `json:"reason,omitempty"`
}
