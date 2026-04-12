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

	// Peer file replication messages (Enterprise Phase 2) - start at 0x20
	// to leave room for future WAL replication messages.
	//
	// MsgFetchFile is sent by a puller to request a Parquet file from a peer.
	// The request payload includes the path and HMAC auth headers. The peer
	// responds with a MsgFetchFileAck header, immediately followed by the raw
	// file bytes (length = ack.SizeBytes) on the same TCP connection.
	MsgFetchFile MessageType = 0x20
	// MsgFetchFileAck is the response header for a file fetch. After the
	// header the origin streams raw body bytes directly on the connection —
	// the body is NOT framed as another protocol message.
	MsgFetchFileAck MessageType = 0x21

	// Leader-forwarding messages (Enterprise Phase 4) - start at 0x30 to
	// leave room for additional file-replication messages.
	//
	// MsgForwardApply is sent by a non-leader node that needs to apply a
	// raft.Command (RegisterFile, DeleteFile, etc.) to the cluster FSM.
	// The recipient unmarshals the embedded command and runs it through
	// hraft.Raft.Apply locally — which only works if the recipient IS
	// the current Raft leader. The recipient validates HMAC and returns
	// MsgForwardApplyAck with success or error.
	//
	// This closes the Phase 1 silent-skip blind spot at coordinator.go's
	// RegisterFileInManifest where non-leader writers would silently lose
	// file registrations, AND unblocks Phase 4's compactor (which is
	// typically NOT the leader in deployments where the writer
	// bootstraps Raft).
	MsgForwardApply MessageType = 0x30
	// MsgForwardApplyAck is the response from the leader after attempting
	// to apply the forwarded command. Includes a success/error status so
	// the caller can distinguish "applied" from "leader rejected".
	MsgForwardApplyAck MessageType = 0x31
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
	case MsgFetchFile:
		return "FetchFile"
	case MsgFetchFileAck:
		return "FetchFileAck"
	case MsgForwardApply:
		return "ForwardApply"
	case MsgForwardApplyAck:
		return "ForwardApplyAck"
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
	RaftAddr    string `json:"raft_addr"`  // Raft transport address for AddVoter
	APIAddr     string `json:"api_addr"`   // HTTP API address
	CoordAddr   string `json:"coord_addr"` // Coordinator address (for peer communication)
	Version     string `json:"version"`
	CoreCount   int    `json:"core_count"` // Number of CPU cores (GOMAXPROCS) on this node
	// Authentication (present when shared_secret is configured)
	AuthNonce     string `json:"auth_nonce,omitempty"`
	AuthTimestamp int64  `json:"auth_timestamp,omitempty"`
	AuthHMAC      string `json:"auth_hmac,omitempty"`
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
	// Authentication (present when shared_secret configured)
	AuthNonce     string `json:"auth_nonce,omitempty"`
	AuthTimestamp int64  `json:"auth_timestamp,omitempty"`
	AuthHMAC      string `json:"auth_hmac,omitempty"`
}

// FetchFileRequest is sent by a puller to request a Parquet file from a peer.
// HMAC auth headers are embedded in the payload rather than using a separate
// handshake because the fetch is a one-shot operation on a freshly dialed
// connection. The validator is security.ValidateFetchHMAC with a ±5 minute
// freshness tolerance. The path is bound into the signed payload so a stolen
// MAC cannot be replayed to fetch a different file within the window.
type FetchFileRequest struct {
	Path      string `json:"path"`
	NodeID    string `json:"node_id"`
	Nonce     string `json:"nonce"`
	Timestamp int64  `json:"timestamp"`
	HMAC      string `json:"hmac"`
}

// Phase 2 fetch-ack error reasons — human-readable strings the Phase 2
// server emits in FetchFileAckHeader.Error. Phase 3 added the typed Code
// field above, but these strings remain the compatibility contract: a
// Phase 3 puller talking to a Phase 2 peer (no Code field) falls back to
// exact-match on these constants to decide whether a negative ack should
// trigger multi-peer fallback. Changing these strings is a wire-protocol
// break against Phase 2 peers — add new codes instead.
const (
	ErrMsgFileNotInManifest = "file not in manifest"
	ErrMsgFileNotFound      = "file not found on local backend"
)

// AckErrorCode is a machine-readable error category on FetchFileAckHeader,
// added in Phase 3 so the puller can implement multi-peer fallback without
// brittle substring matching on human-readable error text. An empty code
// means either Status == "ok" or the remote is a Phase 2 peer that predates
// this field — the fetch client falls back to exact-match on Error in that
// case.
type AckErrorCode string

const (
	// AckCodeNotFound: the peer does not have the requested file in its
	// local backend. The puller treats this as a fallback trigger and tries
	// the next candidate peer within the same attempt.
	AckCodeNotFound AckErrorCode = "not_found"
	// AckCodeManifest: the peer does not have an entry for the requested
	// path in its Raft FSM manifest. Also triggers fallback.
	AckCodeManifest AckErrorCode = "manifest"
	// AckCodeAuth: HMAC validation failed. Does NOT trigger fallback — a
	// cluster-wide shared-secret mismatch would silently fall through every
	// peer and hide the real misconfiguration.
	AckCodeAuth AckErrorCode = "auth"
	// AckCodeBackend: the peer's local storage backend returned an error
	// (Exists check failed, backend not configured, etc.). Does not fall
	// through — surfaces as an attempt failure so operators see the problem.
	AckCodeBackend AckErrorCode = "backend"
	// AckCodeRaft: the peer's Raft node is unavailable (no FSM, not yet
	// bootstrapped). Does not fall through.
	AckCodeRaft AckErrorCode = "raft"
	// AckCodeInvalidPath: path sanitization rejected the request (null
	// byte, absolute path, traversal). Does not fall through.
	AckCodeInvalidPath AckErrorCode = "invalid_path"
)

// FetchFileAckHeader is the response header for a fetch request. If Status is
// "ok" the origin will immediately write SizeBytes of raw body content directly
// to the TCP connection (NOT wrapped in another protocol envelope — the body is
// read with io.CopyN on the raw conn). If Status is "error" no body follows.
type FetchFileAckHeader struct {
	Status    string       `json:"status"`          // "ok" or "error"
	Code      AckErrorCode `json:"code,omitempty"`  // machine-readable error category (Phase 3)
	Error     string       `json:"error,omitempty"` // populated when Status == "error"
	SizeBytes int64        `json:"size_bytes"`
	SHA256    string       `json:"sha256"` // hex-encoded; must match manifest SHA256
}

// ForwardApplyRequest is sent by a non-leader node to ask the Raft leader
// to apply a Command on its behalf. CommandJSON is the already-serialized
// raft.Command (Type + Payload) that hraft.Raft.Apply expects — keeping it
// as a raw json.RawMessage avoids re-marshalling and lets the protocol
// package stay free of any raft.* imports.
//
// HMAC auth headers use the same {nonce, nodeID, clusterName, timestamp}
// signature as the join handshake (security.ComputeHMAC, NOT the
// path-bound ComputeFetchHMAC). The forwarded command's content is
// trusted because cluster peers are mutually authenticated; the HMAC's
// only job is to prove "I am a known peer in this cluster".
//
// Phase 4 only sends this for RegisterFile and DeleteFile commands, but
// the wire format accepts any Command type so future Phase 5+ commands
// (compactor election, etc.) reuse the same path.
type ForwardApplyRequest struct {
	CommandJSON []byte `json:"command_json"` // already-marshalled raft.Command
	NodeID      string `json:"node_id"`
	Nonce       string `json:"nonce"`
	Timestamp   int64  `json:"timestamp"`
	HMAC        string `json:"hmac"`
}

// ForwardApplyAck is the leader's response. Status="ok" means the Raft
// Apply succeeded and the command is durably committed. Status="error"
// means the leader rejected (auth, no longer leader, apply failed, etc.).
//
// Code is machine-readable for the same reason FetchFileAckHeader.Code is:
// the caller (CompactionBridge or file_registrar) needs to distinguish
// "the recipient is no longer the leader, retry with the new leader" from
// "auth failed, give up" without substring-matching the error text.
type ForwardApplyAck struct {
	Status string             `json:"status"`          // "ok" or "error"
	Code   ForwardApplyCode   `json:"code,omitempty"`  // machine-readable error category
	Error  string             `json:"error,omitempty"` // human-readable detail
}

// ForwardApplyCode is the typed error category for ForwardApplyAck. Same
// design as AckErrorCode for fetch — the bridge consumes Code, not Error.
type ForwardApplyCode string

const (
	// ForwardCodeNotLeader: the recipient is not the Raft leader. The
	// caller should re-resolve the leader and retry. This can happen
	// during leader-flap if the caller's view of who-is-leader is stale.
	ForwardCodeNotLeader ForwardApplyCode = "not_leader"
	// ForwardCodeAuth: HMAC validation failed.
	ForwardCodeAuth ForwardApplyCode = "auth"
	// ForwardCodeInvalidCommand: the embedded CommandJSON failed to
	// unmarshal as a raft.Command. Indicates a wire-format bug or a
	// version mismatch — caller should NOT retry.
	ForwardCodeInvalidCommand ForwardApplyCode = "invalid_command"
	// ForwardCodeApplyFailed: the leader's hraft.Raft.Apply call returned
	// an error. The error message is in the Error field for diagnosis.
	// Callers MAY retry — the failure could be transient (Raft timeout,
	// FSM apply panic recovery, etc.).
	ForwardCodeApplyFailed ForwardApplyCode = "apply_failed"
	// ForwardCodeRaftUnavailable: the recipient's Raft node is nil or
	// not running. Caller should not retry against this peer.
	ForwardCodeRaftUnavailable ForwardApplyCode = "raft_unavailable"
)
