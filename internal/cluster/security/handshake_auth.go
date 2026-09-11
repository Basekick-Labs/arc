package security

import (
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"time"
)

// Coordinator-handshake authentication.
//
// Every message on the coordinator TCP protocol's handshake family — the join,
// heartbeat and leave requests, and the join, leader-redirect and
// forward-apply-ack responses — is authenticated here, and the MAC covers
// EVERY non-auth field of the message.
//
// That total-coverage rule is the point. The previous scheme (ComputeHMAC,
// removed in 26.09.2) signed only {msgType, nonce, nodeID, clusterName,
// timestamp} while the handlers went on to consume Role, the advertised Raft /
// API / coordinator addresses, Version and CoreCount from the same message. An
// attacker on a plaintext interconnect could rewrite any of those and the MAC
// still verified, because none of them was in it. Binding "the fields the
// handler happens to read today" would leave the same trap for the next field
// somebody adds, so the invariant is the whole message.
//
// Canonicalization is length-prefixed (canonicalSyncInput), not NUL-delimited.
// See that function's doc for why NUL is not sufficient on this transport: it
// is raw TCP carrying JSON, which passes NUL through happily, so an attacker
// who controls a field's contents can otherwise manufacture delimiters and
// re-partition the signed input. The message-type label is the first field,
// preserving the per-message-type domain separation from #504.

// JoinAuthFields is every non-auth field of protocol.JoinRequest.
type JoinAuthFields struct {
	NodeID      string
	NodeName    string
	Role        string
	ClusterName string
	RaftAddr    string
	APIAddr     string
	CoordAddr   string
	Version     string
	CoreCount   int
}

// HeartbeatAuthFields is every non-auth field of protocol.Heartbeat, plus the
// cluster name (which the wire message does not carry — both sides supply it
// from their own config, so a heartbeat cannot be replayed into another
// cluster that happens to share a secret).
//
// TimestampUnixNano is protocol.Heartbeat.Timestamp. The receiver does not
// read that field today; it is bound anyway, per the total-coverage rule.
type HeartbeatAuthFields struct {
	NodeID            string
	ClusterName       string
	State             string
	IsLeader          bool
	TimestampUnixNano int64
}

// LeaveAuthFields is every non-auth field of protocol.LeaveNotify, plus the
// cluster name (see HeartbeatAuthFields).
type LeaveAuthFields struct {
	NodeID      string
	ClusterName string
	Reason      string
}

// NodeAuthFields mirrors protocol.NodeInfo field-for-field, in the same order.
//
// It exists so this package does not import internal/cluster/protocol.
// security is imported by fourteen files including internal/api and
// internal/edgesync; none of them should transitively acquire the coordinator
// wire protocol. The caller converts; keeping the field order identical makes
// that conversion auditable at a glance.
type NodeAuthFields struct {
	ID        string
	Name      string
	Role      string
	State     string
	RaftAddr  string
	APIAddr   string
	CoordAddr string
	CoreCount int
}

// JoinResponseAuthFields is every non-auth field of protocol.JoinResponse.
type JoinResponseAuthFields struct {
	Success    bool
	LeaderID   string
	LeaderAddr string
	RaftLeader string
	Error      string
	Nodes      []NodeAuthFields
}

// LeaderInfoAuthFields is every non-auth field of protocol.LeaderInfo.
type LeaderInfoAuthFields struct {
	LeaderID        string
	LeaderCoordAddr string
	LeaderRaftAddr  string
}

// ForwardAckAuthFields is every non-auth field of protocol.ForwardApplyAck.
//
// Code is bound because callers branch on it: ForwardCodeNotLeader is mapped
// to a transient retry, so an unsigned Code would let an on-path attacker
// choose between "retry forever" and "fail permanently".
type ForwardAckAuthFields struct {
	Status string
	Code   string
	Error  string
}

// Handshake message-type labels for the response direction. The request
// labels are MsgTypeJoin / MsgTypeHeartbeat / MsgTypeLeave, shared with the
// pre-26.09.2 scheme.
const (
	MsgTypeJoinResp   MsgType = "join-resp"
	MsgTypeLeaderInfo MsgType = "leader-info"
	MsgTypeForwardAck MsgType = "forward-ack"
)

// ErrHandshakeReplay is returned when a handshake message's nonce has already
// been used inside the freshness window.
//
// Distinguished from a MAC failure because it is not a forgery: the message is
// authentic, which is what makes it worth alerting on. A well-behaved peer
// never reuses a nonce, so this means either a broken client or someone
// replaying captured traffic.
var ErrHandshakeReplay = errors.New("security: handshake nonce already used")

// computeHandshakeMACRaw is the one signing path for the whole family.
//
// label MUST be a handshake label (join, heartbeat, leave, join-resp,
// leader-info, forward-ack). MsgTypeRaftAuth / MsgTypeRaftAuthResp are NOT
// handshake labels — the Raft transport handshake has its own helper in
// raft_auth.go using the older NUL format, and passing one here would compile
// but produce a MAC no validator computes.
func computeHandshakeMACRaw(sharedSecret string, label MsgType, fields ...string) []byte {
	all := make([]string, 0, len(fields)+1)
	all = append(all, string(label))
	all = append(all, fields...)
	return computeRawHMAC(sharedSecret, canonicalSyncInput(all...))
}

// validateHandshakeMAC checks freshness then the MAC, in constant time.
func validateHandshakeMAC(sharedSecret string, label MsgType, timestamp int64, tolerance time.Duration, receivedMAC string, fields ...string) error {
	if err := checkHandshakeFreshness(timestamp, tolerance); err != nil {
		return err
	}
	expected := computeHandshakeMACRaw(sharedSecret, label, fields...)
	if !constantTimeHexEqual(expected, receivedMAC) {
		return errors.New("HMAC validation failed: shared secret mismatch or malformed MAC")
	}
	return nil
}

// checkHandshakeFreshness enforces the symmetric timestamp window. Kept
// separate so every handshake validator shares one definition of "fresh" —
// and so NewNonceCache's doc can reason about the exact same inequality.
func checkHandshakeFreshness(timestamp int64, tolerance time.Duration) error {
	// Timestamps are second-granularity, so a sub-second tolerance truncates
	// to zero and rejects everything — fail-closed, but a confusing footgun
	// for a future caller who passes 500ms. Refuse it explicitly, matching
	// checkSyncFreshness.
	if tolerance < time.Second {
		return fmt.Errorf("security: handshake auth tolerance %v is below the one-second timestamp granularity", tolerance)
	}

	now := time.Now().Unix()
	drift := now - timestamp
	if drift < 0 {
		drift = -drift
	}
	if drift > int64(tolerance.Seconds()) {
		return errors.New("auth timestamp expired")
	}
	return nil
}

func joinFields(nonce string, ts int64, f JoinAuthFields) []string {
	return []string{
		nonce, f.NodeID, f.NodeName, f.Role, f.ClusterName,
		f.RaftAddr, f.APIAddr, f.CoordAddr, f.Version,
		strconv.Itoa(f.CoreCount), strconv.FormatInt(ts, 10),
	}
}

// ComputeJoinHMAC signs every field of a join request.
func ComputeJoinHMAC(sharedSecret, nonce string, timestamp int64, f JoinAuthFields) string {
	return hex.EncodeToString(computeHandshakeMACRaw(sharedSecret, MsgTypeJoin, joinFields(nonce, timestamp, f)...))
}

// ValidateJoinHMAC verifies a join request's MAC and freshness.
//
// Prefer ValidateJoinHMACWithReplay: freshness alone is not replay protection.
func ValidateJoinHMAC(sharedSecret, nonce string, timestamp int64, f JoinAuthFields, receivedMAC string, tolerance time.Duration) error {
	return validateHandshakeMAC(sharedSecret, MsgTypeJoin, timestamp, tolerance, receivedMAC, joinFields(nonce, timestamp, f)...)
}

func heartbeatFields(nonce string, ts int64, f HeartbeatAuthFields) []string {
	return []string{
		nonce, f.NodeID, f.ClusterName, f.State,
		strconv.FormatBool(f.IsLeader), strconv.FormatInt(f.TimestampUnixNano, 10),
		strconv.FormatInt(ts, 10),
	}
}

// ComputeHeartbeatHMAC signs every field of a heartbeat.
func ComputeHeartbeatHMAC(sharedSecret, nonce string, timestamp int64, f HeartbeatAuthFields) string {
	return hex.EncodeToString(computeHandshakeMACRaw(sharedSecret, MsgTypeHeartbeat, heartbeatFields(nonce, timestamp, f)...))
}

// ValidateHeartbeatHMAC verifies a heartbeat's MAC and freshness.
//
// Prefer ValidateHeartbeatHMACWithReplay.
func ValidateHeartbeatHMAC(sharedSecret, nonce string, timestamp int64, f HeartbeatAuthFields, receivedMAC string, tolerance time.Duration) error {
	return validateHandshakeMAC(sharedSecret, MsgTypeHeartbeat, timestamp, tolerance, receivedMAC, heartbeatFields(nonce, timestamp, f)...)
}

func leaveFields(nonce string, ts int64, f LeaveAuthFields) []string {
	return []string{nonce, f.NodeID, f.ClusterName, f.Reason, strconv.FormatInt(ts, 10)}
}

// ComputeLeaveHMAC signs every field of a leave notification.
func ComputeLeaveHMAC(sharedSecret, nonce string, timestamp int64, f LeaveAuthFields) string {
	return hex.EncodeToString(computeHandshakeMACRaw(sharedSecret, MsgTypeLeave, leaveFields(nonce, timestamp, f)...))
}

// ValidateLeaveHMAC verifies a leave notification's MAC and freshness.
//
// Prefer ValidateLeaveHMACWithReplay.
func ValidateLeaveHMAC(sharedSecret, nonce string, timestamp int64, f LeaveAuthFields, receivedMAC string, tolerance time.Duration) error {
	return validateHandshakeMAC(sharedSecret, MsgTypeLeave, timestamp, tolerance, receivedMAC, leaveFields(nonce, timestamp, f)...)
}

// validateWithReplay is the fail-closed validate-then-consume path shared by
// the three request validators.
//
// The nonce is consumed only AFTER the MAC verifies, so a forged message
// cannot burn a cache slot and lock out the legitimate nonce. A nil guard is
// an error rather than a skipped check: a validator that returns nil without
// consuming the nonce leaves the message replayable for the whole freshness
// window with every test still passing, which is exactly how this gets
// forgotten. Callers that legitimately have no guard (no shared secret
// configured) must not reach here at all.
func validateWithReplay(guard ReplayGuard, nodeID, nonce string, validate func() error) error {
	// Both nil shapes must be caught: a nil interface, and a non-nil
	// interface holding a nil *NonceCache (which a caller passing a nil
	// struct pointer produces, and which `guard == nil` does NOT match).
	// NonceCache.Track is additionally nil-safe, so the one production
	// implementation fails closed rather than panicking even if it reaches
	// Track. A different ReplayGuard implementation whose Track is not
	// nil-safe would still panic on a typed nil — there is exactly one today,
	// and a new one must keep that property.
	if guard == nil {
		return errors.New("security: handshake replay guard is required")
	}
	if nc, ok := guard.(*NonceCache); ok && nc == nil {
		return errors.New("security: handshake replay guard is required")
	}
	if err := validate(); err != nil {
		return err
	}
	if !guard.Track(nodeID, nonce) {
		return ErrHandshakeReplay
	}
	return nil
}

// ValidateJoinHMACWithReplay validates a join request's MAC and consumes its
// nonce. Returns ErrHandshakeReplay if the nonce was already used.
func ValidateJoinHMACWithReplay(guard ReplayGuard, sharedSecret, nonce string, timestamp int64, f JoinAuthFields, receivedMAC string, tolerance time.Duration) error {
	return validateWithReplay(guard, f.NodeID, nonce, func() error {
		return ValidateJoinHMAC(sharedSecret, nonce, timestamp, f, receivedMAC, tolerance)
	})
}

// ValidateHeartbeatHMACWithReplay validates a heartbeat's MAC and consumes its
// nonce. Returns ErrHandshakeReplay if the nonce was already used.
func ValidateHeartbeatHMACWithReplay(guard ReplayGuard, sharedSecret, nonce string, timestamp int64, f HeartbeatAuthFields, receivedMAC string, tolerance time.Duration) error {
	return validateWithReplay(guard, f.NodeID, nonce, func() error {
		return ValidateHeartbeatHMAC(sharedSecret, nonce, timestamp, f, receivedMAC, tolerance)
	})
}

// ValidateLeaveHMACWithReplay validates a leave notification's MAC and
// consumes its nonce. Returns ErrHandshakeReplay if the nonce was already used.
func ValidateLeaveHMACWithReplay(guard ReplayGuard, sharedSecret, nonce string, timestamp int64, f LeaveAuthFields, receivedMAC string, tolerance time.Duration) error {
	return validateWithReplay(guard, f.NodeID, nonce, func() error {
		return ValidateLeaveHMAC(sharedSecret, nonce, timestamp, f, receivedMAC, tolerance)
	})
}

// Response direction.
//
// A response is signed over the REQUEST's nonce plus every field of the
// response. The requester validates with the nonce it generated, so it needs
// no nonce cache of its own: a response captured from an earlier exchange
// carries a MAC over a different nonce and simply fails to verify. Each
// request has exactly one outstanding response (join dials per attempt;
// forward-apply holds forwardMu across send and receive), so there is no
// legitimate case where a response arrives bearing a nonce other than the one
// the caller is waiting on.
//
// Pre-auth error responses are signed too, over whatever nonce the request
// carried — possibly the empty string, if an unauthenticated peer sent one.
// That is not an oracle: the label is the first length-prefixed field and the
// timestamp the last, every other field of an error response is chosen by the
// responder, and every other MAC family in this package ends its canonical
// input with a raw nonce or \x00-joined timestamp, so no arrangement of an
// attacker-chosen nonce can collide with a MAC from another family.

func joinResponseFields(reqNonce string, ts int64, f JoinResponseAuthFields) []string {
	out := make([]string, 0, 7+len(f.Nodes)*8)
	out = append(out,
		reqNonce,
		strconv.FormatBool(f.Success),
		f.LeaderID, f.LeaderAddr, f.RaftLeader, f.Error,
		strconv.Itoa(len(f.Nodes)),
	)
	for _, n := range f.Nodes {
		out = append(out, n.ID, n.Name, n.Role, n.State, n.RaftAddr, n.APIAddr, n.CoordAddr, strconv.Itoa(n.CoreCount))
	}
	out = append(out, strconv.FormatInt(ts, 10))
	return out
}

// ComputeJoinResponseHMAC signs a join response over the request's nonce.
func ComputeJoinResponseHMAC(sharedSecret, reqNonce string, timestamp int64, f JoinResponseAuthFields) string {
	return hex.EncodeToString(computeHandshakeMACRaw(sharedSecret, MsgTypeJoinResp, joinResponseFields(reqNonce, timestamp, f)...))
}

// ValidateJoinResponseHMAC verifies a join response against the nonce the
// caller sent in its request.
func ValidateJoinResponseHMAC(sharedSecret, reqNonce string, timestamp int64, f JoinResponseAuthFields, receivedMAC string, tolerance time.Duration) error {
	return validateHandshakeMAC(sharedSecret, MsgTypeJoinResp, timestamp, tolerance, receivedMAC, joinResponseFields(reqNonce, timestamp, f)...)
}

func leaderInfoFields(reqNonce string, ts int64, f LeaderInfoAuthFields) []string {
	return []string{reqNonce, f.LeaderID, f.LeaderCoordAddr, f.LeaderRaftAddr, strconv.FormatInt(ts, 10)}
}

// ComputeLeaderInfoHMAC signs a leader redirect over the request's nonce.
func ComputeLeaderInfoHMAC(sharedSecret, reqNonce string, timestamp int64, f LeaderInfoAuthFields) string {
	return hex.EncodeToString(computeHandshakeMACRaw(sharedSecret, MsgTypeLeaderInfo, leaderInfoFields(reqNonce, timestamp, f)...))
}

// ValidateLeaderInfoHMAC verifies a leader redirect against the nonce the
// caller sent in its request. A redirect names the address the joiner will
// dial next, so an unauthenticated one hands an attacker the joiner's next
// signed join request.
func ValidateLeaderInfoHMAC(sharedSecret, reqNonce string, timestamp int64, f LeaderInfoAuthFields, receivedMAC string, tolerance time.Duration) error {
	return validateHandshakeMAC(sharedSecret, MsgTypeLeaderInfo, timestamp, tolerance, receivedMAC, leaderInfoFields(reqNonce, timestamp, f)...)
}

func forwardAckFields(reqNonce string, ts int64, f ForwardAckAuthFields) []string {
	return []string{reqNonce, f.Status, f.Code, f.Error, strconv.FormatInt(ts, 10)}
}

// ComputeForwardAckHMAC signs a forward-apply ack over the request's nonce.
func ComputeForwardAckHMAC(sharedSecret, reqNonce string, timestamp int64, f ForwardAckAuthFields) string {
	return hex.EncodeToString(computeHandshakeMACRaw(sharedSecret, MsgTypeForwardAck, forwardAckFields(reqNonce, timestamp, f)...))
}

// ValidateForwardAckHMAC verifies a forward-apply ack against the nonce the
// caller sent in its request.
func ValidateForwardAckHMAC(sharedSecret, reqNonce string, timestamp int64, f ForwardAckAuthFields, receivedMAC string, tolerance time.Duration) error {
	return validateHandshakeMAC(sharedSecret, MsgTypeForwardAck, timestamp, tolerance, receivedMAC, forwardAckFields(reqNonce, timestamp, f)...)
}
