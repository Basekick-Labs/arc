package protocol

import (
	"encoding/json"
	"time"
)

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

	// WAL replication handshake (Phase 3.3). The stream is gone (#888,
	// #1181): a reader of a release before 27.01.1 still sends
	// MsgReplicateSync until it is upgraded, and a writer answers it with a
	// refusing MsgReplicateSyncAck. Reserved: never reuse 0x10-0x1F while
	// such readers can exist.
	MsgReplicateSync    MessageType = 0x10
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

	// Origin stream (#1207): a standby writer dials the primary and receives
	// every ingest batch the primary commits, to keep a copy until the
	// batch's files are registered. MsgOriginStreamHello opens it; the
	// primary answers MsgOriginStreamHelloAck and then owns the connection
	// (internal/cluster/originstream frames follow).
	MsgOriginStreamHello    MessageType = 0x40
	MsgOriginStreamHelloAck MessageType = 0x41

	// Pending registrations (#1214): before the orphan-storage sweep deletes
	// unregistered objects on a shared object store, the reconciler asks
	// every node that can ingest which files it flushed and has not
	// registered yet. A node before #1214 closes the connection on the
	// unknown type, which the asker treats as no answer.
	MsgPendingRegistrationsQuery MessageType = 0x50
	MsgPendingRegistrationsReply MessageType = 0x51

	// Flush scope (#1265): in shared storage every writer takes writes, so
	// before a DELETE scans, the primary asks every node to flush the rows
	// of its scope it has buffered and to name the files it flushed that
	// wait for registration. A node before #1265 closes the connection on
	// the unknown type, which the asker treats as no answer.
	MsgFlushScopeRequest MessageType = 0x52
	MsgFlushScopeReply   MessageType = 0x53

	// Local policies (#1324): before the cluster takes over a policy kind,
	// the leader asks every node for the rows of that kind in its own
	// SQLite. Sent only once cluster policies are enabled, so every node
	// knows the type.
	MsgLocalPoliciesQuery MessageType = 0x54
	MsgLocalPoliciesReply MessageType = 0x55
)

// LocalPoliciesQuery asks a node for its own rows of a policy kind, once its
// FSM has applied the collect at Epoch (a log index).
// HMAC: security.ComputeLocalPoliciesQueryHMAC.
type LocalPoliciesQuery struct {
	NodeID      string `json:"node_id"`
	ClusterName string `json:"cluster_name"`
	Kind        string `json:"kind"`
	Epoch       uint64 `json:"epoch"`
	Nonce       string `json:"nonce"`
	Timestamp   int64  `json:"timestamp"`
	HMAC        string `json:"hmac"`
}

// Local policies reply statuses.
const (
	// LocalPoliciesOK: Policies is every row of the kind on the node.
	LocalPoliciesOK = "ok"
	// LocalPoliciesFailed: the node could not read its rows, or had not
	// applied the collect in time.
	LocalPoliciesFailed = "failed"
	// LocalPoliciesRefused: the query failed authentication or validation.
	LocalPoliciesRefused = "refused"
)

// LocalPoliciesReply answers a LocalPoliciesQuery, bound to its nonce, kind
// and epoch, over a digest of Policies: security.ComputeLocalPoliciesReplyHMAC.
// Policies is the kind's rows as JSON.
type LocalPoliciesReply struct {
	NodeID    string          `json:"node_id"`
	Status    string          `json:"status"`
	Policies  json.RawMessage `json:"policies,omitempty"`
	Timestamp int64           `json:"timestamp"`
	HMAC      string          `json:"hmac"`
}

// PendingRegistrationsQuery asks a node for the files it flushed that the
// cluster manifest does not list yet, whenever they were flushed.
// HMAC: security.ComputePendingRegistrationsQueryHMAC.
type PendingRegistrationsQuery struct {
	NodeID      string `json:"node_id"`
	ClusterName string `json:"cluster_name"`
	Nonce       string `json:"nonce"`
	Timestamp   int64  `json:"timestamp"`
	HMAC        string `json:"hmac"`
}

// Pending registrations reply statuses.
const (
	// PendingStatusOK: Paths is every file pending registration on the node.
	PendingStatusOK = "ok"
	// PendingStatusTooMany: more such files than one reply can carry.
	PendingStatusTooMany = "too_many"
	// PendingStatusUnknown: the node ingests but has no registration journal
	// (it could not be opened), so it cannot tell.
	PendingStatusUnknown = "unknown"
	// PendingStatusRefused: the query failed authentication or validation.
	PendingStatusRefused = "refused"
)

// PendingRegistrationsReply answers a query. The HMAC binds the reply to the
// query's nonce and covers a digest of Paths, so neither a recorded reply nor
// one with paths stripped is accepted:
// security.ComputePendingRegistrationsReplyHMAC.
type PendingRegistrationsReply struct {
	NodeID    string   `json:"node_id"`
	Status    string   `json:"status"`
	Paths     []string `json:"paths,omitempty"`
	Timestamp int64    `json:"timestamp"`
	HMAC      string   `json:"hmac"`
}

// FlushScopeRequest asks a node to flush its buffered rows of Database
// (Measurement "" = all of them). HMAC:
// security.ComputeFlushScopeRequestHMAC.
type FlushScopeRequest struct {
	NodeID      string `json:"node_id"`
	ClusterName string `json:"cluster_name"`
	Database    string `json:"database"`
	Measurement string `json:"measurement,omitempty"`
	Nonce       string `json:"nonce"`
	Timestamp   int64  `json:"timestamp"`
	HMAC        string `json:"hmac"`
}

// Flush scope reply statuses.
const (
	// FlushStatusOK: flushed; Paths is every file of the scope still
	// waiting to register on the node.
	FlushStatusOK = "ok"
	// FlushStatusUntracked: flushed, but the node has no registration
	// journal, so it cannot name the files waiting to register.
	FlushStatusUntracked = "untracked"
	// FlushStatusFailed: the flush failed or timed out, or an earlier one
	// failed and its rows wait for WAL recovery.
	FlushStatusFailed = "flush_failed"
	// FlushStatusTooMany: more pending files than one reply can carry.
	FlushStatusTooMany = "too_many"
	// FlushStatusRefused: the request failed authentication or validation.
	FlushStatusRefused = "refused"
)

// FlushScopeReply answers a FlushScopeRequest, bound to its nonce and
// scope, over a digest of Paths: security.ComputeFlushScopeReplyHMAC.
type FlushScopeReply struct {
	NodeID    string   `json:"node_id"`
	Status    string   `json:"status"`
	Paths     []string `json:"paths,omitempty"`
	Timestamp int64    `json:"timestamp"`
	HMAC      string   `json:"hmac"`
}

// OriginStreamHello is sent by a standby writer to open the origin stream.
// HMAC: security.ComputeOriginStreamHelloHMAC.
type OriginStreamHello struct {
	// Version is the origin stream protocol the standby speaks; covered by
	// the HMAC. The primary refuses one it does not speak.
	Version     int    `json:"version"`
	NodeID      string `json:"node_id"`
	ClusterName string `json:"cluster_name"`
	Nonce       string `json:"nonce"`
	Timestamp   int64  `json:"timestamp"`
	HMAC        string `json:"hmac"`
}

// OriginStreamHelloAck answers a hello. Error is set on refusal. Instance is
// the primary's current WAL writer instance (hex). The HMAC binds the reply
// to the hello's nonce: security.ComputeOriginStreamHelloAckHMAC.
type OriginStreamHelloAck struct {
	OriginNodeID string `json:"origin_node_id"`
	Instance     string `json:"instance,omitempty"`
	Error        string `json:"error,omitempty"`
	Timestamp    int64  `json:"timestamp"`
	HMAC         string `json:"hmac"`
}

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
	case MsgOriginStreamHello:
		return "OriginStreamHello"
	case MsgOriginStreamHelloAck:
		return "OriginStreamHelloAck"
	case MsgPendingRegistrationsQuery:
		return "PendingRegistrationsQuery"
	case MsgPendingRegistrationsReply:
		return "PendingRegistrationsReply"
	case MsgFlushScopeRequest:
		return "FlushScopeRequest"
	case MsgFlushScopeReply:
		return "FlushScopeReply"
	case MsgLocalPoliciesQuery:
		return "LocalPoliciesQuery"
	case MsgLocalPoliciesReply:
		return "LocalPoliciesReply"
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

// ReplicateSync is what a reader of a release before 27.01.1 sends to request
// the WAL replication stream. It is decoded only to be refused (#1181): every
// request gets the same answer, so its HMAC fields (GHSA-wfgr-8x84-22q7) are
// not checked.
type ReplicateSync struct {
	ReaderID          string `json:"reader_id"`
	LastKnownSequence uint64 `json:"last_known_seq"`

	// Authentication fields of the old handshake (GHSA-wfgr-8x84-22q7).
	Nonce       string `json:"nonce,omitempty"`
	ClusterName string `json:"cluster_name,omitempty"`
	Timestamp   int64  `json:"timestamp,omitempty"`
	HMAC        string `json:"hmac,omitempty"`

	// SupportsBinaryEntries is what an old reader sent to ask for binary
	// entry frames (#698); decoded and ignored.
	SupportsBinaryEntries bool `json:"bin_entries,omitempty"`
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
	RaftAddr    string `json:"raft_addr"`  // Raft transport address; the leader adds the node as a voter or a non-voter depending on its role (#862)
	APIAddr     string `json:"api_addr"`   // HTTP API address
	CoordAddr   string `json:"coord_addr"` // Coordinator address (for peer communication)
	Version     string `json:"version"`
	CoreCount   int    `json:"core_count"` // Number of CPU cores (GOMAXPROCS) on this node
	// Authentication (present when shared_secret is configured)
	AuthNonce     string `json:"auth_nonce,omitempty"`
	AuthTimestamp int64  `json:"auth_timestamp,omitempty"`
	AuthHMAC      string `json:"auth_hmac,omitempty"`

	// Capabilities this node's build supports (#1207), e.g. stream_claims_v1.
	// Outside AuthHMAC on purpose: an older leader validates AuthHMAC over the
	// fields it knows, so adding one there would refuse every upgraded node
	// during a rolling upgrade. AuthHMAC2 covers the same fields plus
	// Capabilities; a leader that reads Capabilities requires it. Stripping
	// both only makes the node look like an older build.
	Capabilities []string `json:"capabilities,omitempty"`
	AuthHMAC2    string   `json:"auth_hmac2,omitempty"`
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
	// Authentication (present when shared_secret is configured). Signed by
	// the responder over the REQUEST's nonce plus every field above, so the
	// requester validates with the nonce it generated and needs no nonce
	// cache of its own. See security/handshake_auth.go.
	AuthTimestamp int64  `json:"auth_timestamp,omitempty"`
	AuthHMAC      string `json:"auth_hmac,omitempty"`
}

// LeaderInfo is sent when a non-leader receives a join request,
// redirecting the requester to the current leader.
type LeaderInfo struct {
	LeaderID        string `json:"leader_id"`
	LeaderCoordAddr string `json:"leader_coord_addr"` // Leader's coordinator address
	LeaderRaftAddr  string `json:"leader_raft_addr"`  // Leader's Raft address
	// Authentication (present when shared_secret is configured). Signed by
	// the responder over the REQUEST's nonce plus every field above, so the
	// requester validates with the nonce it generated and needs no nonce
	// cache of its own. See security/handshake_auth.go.
	AuthTimestamp int64  `json:"auth_timestamp,omitempty"`
	AuthHMAC      string `json:"auth_hmac,omitempty"`
}

// Heartbeat is sent periodically to maintain connection and share state.
type Heartbeat struct {
	NodeID    string    `json:"node_id"`
	State     string    `json:"state"`
	IsLeader  bool      `json:"is_leader"`
	Timestamp time.Time `json:"timestamp"`
	// Authentication (present when shared_secret configured). A heartbeat
	// mutates the sender's recorded liveness/state in the registry, so it is
	// authenticated like join/leave to prevent a network attacker from
	// spoofing a node's health (GHSA-p378-jp5r-gpgw).
	AuthNonce     string `json:"auth_nonce,omitempty"`
	AuthTimestamp int64  `json:"auth_timestamp,omitempty"`
	AuthHMAC      string `json:"auth_hmac,omitempty"`
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
	// ByteOffset is the byte position to resume from. Zero means start from the
	// beginning (full fetch). The HMAC does NOT bind ByteOffset — binding it
	// would invalidate the MAC on resume since the path is already bound.
	ByteOffset int64 `json:"byte_offset,omitempty"`
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
	// AckCodeBadOffset: the requested byte offset is invalid (negative,
	// >= file size, or the backend does not support seeking). The puller
	// should delete any partial file and retry from zero — not fall through
	// to another peer.
	AckCodeBadOffset AckErrorCode = "bad_offset"
)

// FetchFileAckHeader is the response header for a fetch request. If Status is
// "ok" the origin will immediately write SizeBytes of raw body content directly
// to the TCP connection (NOT wrapped in another protocol envelope — the body is
// read with io.CopyN on the raw conn). If Status is "error" no body follows.
type FetchFileAckHeader struct {
	Status    string       `json:"status"`          // "ok" or "error"
	Code      AckErrorCode `json:"code,omitempty"`  // machine-readable error category (Phase 3)
	Error     string       `json:"error,omitempty"` // populated when Status == "error"
	SizeBytes int64        `json:"size_bytes"`      // tail bytes being sent (total - ByteOffset)
	SHA256    string       `json:"sha256"`          // hex-encoded whole-file hash from manifest
	// ByteOffset echoes the byte offset the server will serve from. The puller
	// validates this matches the requested offset before streaming the body.
	ByteOffset int64 `json:"byte_offset,omitempty"`
}

// ForwardApplyRequest is sent by a non-leader node to ask the Raft leader
// to apply a Command on its behalf. CommandJSON is the already-serialized
// raft.Command (Type + Payload) that hraft.Raft.Apply expects — keeping it
// as a raw json.RawMessage avoids re-marshalling and lets the protocol
// package stay free of any raft.* imports.
//
// HMAC auth headers are payload-bound: security.ComputeForwardHMAC signs
// {nonce, nodeID, clusterName, sha256(CommandJSON), timestamp}, so the
// forwarded command cannot be altered in flight by a peer that merely
// knows the shared secret's MAC for some other command. The ack is
// likewise signed over this request's nonce (26.09.2) — see
// security/handshake_auth.go.
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
	// WantIndex asks the leader for the applied command's log index in the
	// ack, so the requester can wait until its own FSM has applied it
	// (#1324). Sent only once cluster policies are enabled, which means every
	// node understands it. Not covered by the request MAC: flipping it only
	// makes the ack fail validation, which an on-path attacker can cause by
	// dropping the ack anyway.
	WantIndex bool `json:"want_index,omitempty"`
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
	Status string           `json:"status"`          // "ok" or "error"
	Code   ForwardApplyCode `json:"code,omitempty"`  // machine-readable error category
	Error  string           `json:"error,omitempty"` // human-readable detail
	// Index is the applied command's log index, on a success ack to a
	// request with WantIndex (#1324). Signed when present.
	Index uint64 `json:"index,omitempty"`
	// Authentication (present when shared_secret is configured). Signed by
	// the responder over the REQUEST's nonce plus every field above, so the
	// requester validates with the nonce it generated and needs no nonce
	// cache of its own. See security/handshake_auth.go.
	AuthTimestamp int64  `json:"auth_timestamp,omitempty"`
	AuthHMAC      string `json:"auth_hmac,omitempty"`
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
	// ForwardCodeClaimConflict: the FSM refused registrations of rows another
	// writer has claimed (#1207). Terminal: retrying cannot succeed. The
	// message names the refused op indexes.
	ForwardCodeClaimConflict ForwardApplyCode = "claim_conflict"
	// ForwardCodeRaftUnavailable: the recipient's Raft node is nil or
	// not running. Caller should not retry against this peer.
	ForwardCodeRaftUnavailable ForwardApplyCode = "raft_unavailable"
)

// GetNodeID and GetCapabilities let the leader's stream claims join gate
// read a join request without importing this package's types into raft.
func (r *JoinRequest) GetNodeID() string         { return r.NodeID }
func (r *JoinRequest) GetCapabilities() []string { return r.Capabilities }
