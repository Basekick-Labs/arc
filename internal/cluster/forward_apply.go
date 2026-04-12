package cluster

// Phase 4 leader forwarding: when a non-leader node needs to apply a Raft
// command (RegisterFile, DeleteFile), it forwards the command over the
// existing peer-protocol TCP connection to the current Raft leader, which
// applies it locally and returns success/error.
//
// This file holds the client-side (forwardApplyToLeader, called from any
// non-leader coordinator method that needs to write to Raft) and the
// server-side (handleForwardApply, dispatched from handlePeerConnection).
// Both sides use the existing security.ComputeHMAC / security.ValidateHMAC
// helpers — leader forwarding is a "trusted-peer" operation, not a
// per-resource one, so the path-bound HMAC variant isn't needed.
//
// Background and rationale: Phase 1 introduced the manifest with a
// silent-skip on non-leader writers (coordinator.go's RegisterFileInManifest
// would return nil rather than error if the writer wasn't currently the
// leader). That's a latent data-loss bug — the writer's flush succeeds,
// the file lands in storage, but no Raft entry is appended and no peer
// learns about it. Phase 4 surfaced the same bug from a different angle
// because the compactor is typically NOT the Raft leader, so its
// CompactionBridge would always silently skip and the watcher would
// retry forever.
//
// The fix is leader forwarding — uniform across all callers. Both
// RegisterFileInManifest and DeleteFileFromManifest now call
// forwardApplyToLeader on non-leader nodes instead of silently dropping
// the command. The CompactionBridge uses these methods unchanged, so the
// fix is transparent to it.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/protocol"
	clusterraft "github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/cluster/security"
)

// ErrNoLeaderKnown is returned by forwardApplyToLeader when the local
// Raft node hasn't observed a leader yet (e.g. during election or right
// after startup before the first heartbeat). Callers should treat this
// as a transient retry condition.
var ErrNoLeaderKnown = errors.New("forward apply: no leader currently known")

// ErrLeaderUnreachable is returned when the leader's ID is known but the
// registry doesn't have a coordinator address for it (e.g. the leader
// just left the registry mid-flight). Caller should retry.
var ErrLeaderUnreachable = errors.New("forward apply: leader address not in registry")

// forwardApplyTimeout bounds a single dial+send+recv round-trip to the
// leader. 5 seconds is generous enough for slow networks and a Raft
// quorum-commit on the leader side, while short enough that a stuck
// leader doesn't block the caller's whole shutdown sequence.
const forwardApplyTimeout = 5 * time.Second

// forwardApplyToLeader serializes a Raft Command and ships it to the
// current Raft leader for application. Returns nil if the leader applied
// successfully, or an error wrapping the protocol-level rejection.
//
// This is the universal "I'm not the leader, please apply this for me"
// path. Callers should already have checked IsLeader() and decided to
// forward; this function does NOT re-check leadership locally because
// that would create a TOCTOU window between the check and the dial.
//
// Special errors:
//   - ErrNoLeaderKnown: no leader observed yet, retry
//   - ErrLeaderUnreachable: leader ID known but not in registry, retry
//   - any other error: protocol-level rejection (auth, apply failed, etc.)
func (c *Coordinator) forwardApplyToLeader(ctx context.Context, cmd *clusterraft.Command) error {
	if c.raftNode == nil {
		return fmt.Errorf("forward apply: raft not initialized")
	}
	if c.cfg.SharedSecret == "" {
		// Forward apply requires authenticated transport. The startup
		// validation already enforces shared_secret when ReplicationEnabled
		// is true, but we double-check defensively.
		return fmt.Errorf("forward apply: shared_secret not configured")
	}

	// Resolve the current leader. LeaderID returns empty when no leader
	// is currently known (no recent heartbeat / mid-election).
	leaderID := c.raftNode.LeaderID()
	if leaderID == "" {
		return ErrNoLeaderKnown
	}
	if leaderID == c.localNode.ID {
		// We thought we weren't the leader but we are — caller's IsLeader
		// check raced with a recent election. Apply locally instead of
		// dialing ourselves. This is the safe fallback.
		return c.raftNode.Apply(cmd, forwardApplyTimeout)
	}

	leaderNode, ok := c.registry.Get(leaderID)
	if !ok || leaderNode.Address == "" {
		return fmt.Errorf("%w: leader_id=%s", ErrLeaderUnreachable, leaderID)
	}

	// Marshal the command exactly as Node.Apply would. Embedding the raw
	// JSON in the request lets the leader call Node.Apply unchanged on
	// the receiving side — no double-marshaling, no Command struct
	// duplication.
	cmdJSON, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("forward apply: marshal command: %w", err)
	}

	// Build the HMAC-authenticated request. Same {nonce, nodeID,
	// clusterName, timestamp} payload as the join handshake — leader
	// forwarding is a trusted-peer operation, not a per-resource one,
	// so the path-bound fetch HMAC variant isn't appropriate here.
	nonce, err := security.GenerateNonce()
	if err != nil {
		return fmt.Errorf("forward apply: generate nonce: %w", err)
	}
	ts := time.Now().Unix()
	mac := security.ComputeHMAC(c.cfg.SharedSecret, nonce, c.localNode.ID, c.cfg.ClusterName, ts)

	req := &protocol.ForwardApplyRequest{
		CommandJSON: cmdJSON,
		NodeID:      c.localNode.ID,
		Nonce:       nonce,
		Timestamp:   ts,
		HMAC:        mac,
	}

	// Dial the leader. security.Dial reuses the cluster TLS config when
	// configured, falling back to plain TCP otherwise.
	dialTimeout := forwardApplyTimeout
	if deadline, ok := ctx.Deadline(); ok {
		if remaining := time.Until(deadline); remaining < dialTimeout {
			dialTimeout = remaining
		}
	}
	conn, err := security.Dial("tcp", leaderNode.Address, dialTimeout, c.tlsConfig)
	if err != nil {
		return fmt.Errorf("forward apply: dial leader %s (%s): %w", leaderID, leaderNode.Address, err)
	}
	defer conn.Close()

	// Bound the whole round-trip by the caller's context if it has a
	// deadline, otherwise use the default forward-apply timeout. This
	// applies to both Send and Receive — a stuck leader can't pin our
	// goroutine forever.
	roundTripDeadline := time.Now().Add(forwardApplyTimeout)
	if deadline, ok := ctx.Deadline(); ok && deadline.Before(roundTripDeadline) {
		roundTripDeadline = deadline
	}
	_ = conn.SetDeadline(roundTripDeadline)

	if err := protocol.SendMessage(conn, &protocol.Message{
		Type:    protocol.MsgForwardApply,
		Payload: req,
	}, forwardApplyTimeout); err != nil {
		return fmt.Errorf("forward apply: send: %w", err)
	}

	ackMsg, err := protocol.ReceiveMessage(conn, forwardApplyTimeout)
	if err != nil {
		return fmt.Errorf("forward apply: receive ack: %w", err)
	}
	if ackMsg.Type != protocol.MsgForwardApplyAck {
		return fmt.Errorf("forward apply: unexpected ack type: %v", ackMsg.Type)
	}
	ack, ok := ackMsg.Payload.(*protocol.ForwardApplyAck)
	if !ok {
		return fmt.Errorf("forward apply: ack payload has wrong type: %T", ackMsg.Payload)
	}
	if ack.Status != "ok" {
		return fmt.Errorf("forward apply: leader rejected (code=%s): %s", ack.Code, ack.Error)
	}
	return nil
}

// handleForwardApply is the server side of leader forwarding. Dispatched
// from handlePeerConnection when MsgForwardApply arrives. Validates HMAC,
// confirms we're still the leader, applies the command via Node.Apply,
// and sends an ack on the same connection.
//
// Connection is owned by this function — it must close before returning.
func (c *Coordinator) handleForwardApply(conn net.Conn, req *protocol.ForwardApplyRequest) {
	defer conn.Close()
	remoteAddr := conn.RemoteAddr().String()

	// HMAC validation. Uses the join-handshake variant (no path binding)
	// because the embedded command's contents are trusted between cluster
	// peers — the HMAC's job is "this is a known peer" not "this peer is
	// allowed to apply this specific command".
	if c.cfg.SharedSecret == "" {
		c.sendForwardApplyError(conn, protocol.ForwardCodeAuth, "leader has no shared_secret configured")
		return
	}
	if err := security.ValidateHMAC(
		c.cfg.SharedSecret, req.Nonce, req.NodeID, c.cfg.ClusterName,
		req.Timestamp, req.HMAC, 5*time.Minute,
	); err != nil {
		c.logger.Warn().
			Err(err).
			Str("peer", remoteAddr).
			Str("requesting_node", req.NodeID).
			Msg("ForwardApply rejected: HMAC validation failed")
		c.sendForwardApplyError(conn, protocol.ForwardCodeAuth, "authentication failed")
		return
	}

	if c.raftNode == nil {
		c.sendForwardApplyError(conn, protocol.ForwardCodeRaftUnavailable, "raft not initialized")
		return
	}

	// Confirm we're STILL the leader. The caller forwarded to us because
	// their LeaderID() check said we're leader, but leadership can flap
	// in the milliseconds between their check and our handler running.
	// On false, return ForwardCodeNotLeader so the caller can re-resolve
	// and retry (typically against the new leader).
	if !c.raftNode.IsLeader() {
		c.logger.Debug().
			Str("requesting_node", req.NodeID).
			Msg("ForwardApply rejected: no longer leader")
		c.sendForwardApplyError(conn, protocol.ForwardCodeNotLeader, "not the current leader")
		return
	}

	// Unmarshal the embedded command. Wire-format mismatch (caller and
	// recipient on different Arc versions with incompatible Command
	// shapes) returns InvalidCommand — caller should NOT retry, this is
	// a deployment bug.
	var cmd clusterraft.Command
	if err := json.Unmarshal(req.CommandJSON, &cmd); err != nil {
		c.logger.Warn().
			Err(err).
			Str("requesting_node", req.NodeID).
			Msg("ForwardApply rejected: invalid command JSON")
		c.sendForwardApplyError(conn, protocol.ForwardCodeInvalidCommand, "invalid command")
		return
	}

	// Security: allowlist the command types that may be forwarded. Only
	// file-manifest operations (RegisterFile, DeleteFile) are expected
	// via leader forwarding. Topology-mutating commands (AddNode,
	// RemoveNode, PromoteWriter, etc.) must go through their dedicated
	// join/leave handlers which have their own auth flow. Rejecting
	// unexpected types prevents a compromised peer from escalating a
	// shared-secret credential into topology mutations.
	if cmd.Type != clusterraft.CommandRegisterFile && cmd.Type != clusterraft.CommandDeleteFile {
		c.logger.Warn().
			Str("requesting_node", req.NodeID).
			Int("cmd_type", int(cmd.Type)).
			Msg("ForwardApply rejected: command type not allowed via forwarding")
		c.sendForwardApplyError(conn, protocol.ForwardCodeInvalidCommand, "command type not allowed via forwarding")
		return
	}

	// Apply via Node.Apply — this is the same code path local applies
	// take, so the FSM handler doesn't need to know whether the command
	// originated locally or via forwarding.
	if err := c.raftNode.Apply(&cmd, forwardApplyTimeout); err != nil {
		c.logger.Warn().
			Err(err).
			Str("requesting_node", req.NodeID).
			Int("cmd_type", int(cmd.Type)).
			Msg("ForwardApply: Raft Apply failed")
		c.sendForwardApplyError(conn, protocol.ForwardCodeApplyFailed, "raft apply failed")
		return
	}

	// Success ack.
	if err := protocol.SendMessage(conn, &protocol.Message{
		Type:    protocol.MsgForwardApplyAck,
		Payload: &protocol.ForwardApplyAck{Status: "ok"},
	}, forwardApplyTimeout); err != nil {
		c.logger.Debug().Err(err).Msg("ForwardApply: failed to send success ack")
		return
	}
	c.logger.Debug().
		Str("requesting_node", req.NodeID).
		Int("cmd_type", int(cmd.Type)).
		Msg("ForwardApply: applied successfully")
}

// sendForwardApplyError is a small helper to send an error ack. Best-effort:
// any write error is logged at debug because the connection is about to
// close anyway via the caller's defer.
func (c *Coordinator) sendForwardApplyError(conn net.Conn, code protocol.ForwardApplyCode, reason string) {
	ack := &protocol.ForwardApplyAck{
		Status: "error",
		Code:   code,
		Error:  reason,
	}
	if err := protocol.SendMessage(conn, &protocol.Message{
		Type:    protocol.MsgForwardApplyAck,
		Payload: ack,
	}, forwardApplyTimeout); err != nil {
		c.logger.Debug().Err(err).Msg("ForwardApply: failed to send error ack")
	}
}
