package cluster

// Phase 4 leader forwarding: when a non-leader node needs to apply a Raft
// command (RegisterFile, DeleteFile), it forwards the command over the
// existing peer-protocol TCP connection to the current Raft leader, which
// applies it locally and returns success/error.
//
// This file holds the client-side (forwardApplyToLeader, called from any
// non-leader coordinator method that needs to write to Raft) and the
// server-side (handleForwardApply, dispatched from handlePeerConnection).
// Both sides use security.ComputeForwardHMAC / security.ValidateForwardHMAC
// which bind the command payload into the signed material, preventing
// on-wire command swapping even without TLS.
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

	// Build the HMAC-authenticated request. The HMAC covers the command
	// payload (via SHA-256 digest) so an on-wire attacker cannot swap
	// the command body while keeping the same MAC. This uses
	// ComputeForwardHMAC which binds {nonce, nodeID, clusterName,
	// sha256(cmdJSON), timestamp} into the signed material.
	nonce, err := security.GenerateNonce()
	if err != nil {
		return fmt.Errorf("forward apply: generate nonce: %w", err)
	}
	ts := time.Now().Unix()
	mac := security.ComputeForwardHMAC(c.cfg.SharedSecret, nonce, c.localNode.ID, c.cfg.ClusterName, cmdJSON, ts)

	req := &protocol.ForwardApplyRequest{
		CommandJSON: cmdJSON,
		NodeID:      c.localNode.ID,
		Nonce:       nonce,
		Timestamp:   ts,
		HMAC:        mac,
	}

	// Acquire or reuse a cached TCP connection to the leader. The
	// connection is kept alive across forwarded commands to amortize
	// dial + TLS handshake costs on the flush hot path. On any
	// send/receive error, the connection is closed and the next call
	// dials fresh (lazy reconnect).
	conn, err := c.getOrDialLeader(ctx, leaderID, leaderNode.Address)
	if err != nil {
		return fmt.Errorf("forward apply: %w", err)
	}

	// Set a per-call deadline on the shared connection.
	roundTripDeadline := time.Now().Add(forwardApplyTimeout)
	if deadline, ok := ctx.Deadline(); ok && deadline.Before(roundTripDeadline) {
		roundTripDeadline = deadline
	}
	_ = conn.SetDeadline(roundTripDeadline)

	if err := protocol.SendMessage(conn, &protocol.Message{
		Type:    protocol.MsgForwardApply,
		Payload: req,
	}, forwardApplyTimeout); err != nil {
		c.closeForwardConn() // stale — next call redials
		return fmt.Errorf("forward apply: send: %w", err)
	}

	ackMsg, err := protocol.ReceiveMessage(conn, forwardApplyTimeout)
	if err != nil {
		c.closeForwardConn()
		return fmt.Errorf("forward apply: receive ack: %w", err)
	}
	if ackMsg.Type != protocol.MsgForwardApplyAck {
		c.closeForwardConn()
		return fmt.Errorf("forward apply: unexpected ack type: %v", ackMsg.Type)
	}
	ack, ok := ackMsg.Payload.(*protocol.ForwardApplyAck)
	if !ok {
		c.closeForwardConn()
		return fmt.Errorf("forward apply: ack payload has wrong type: %T", ackMsg.Payload)
	}
	if ack.Status != "ok" {
		// Protocol-level rejection — connection is still valid, don't close.
		return fmt.Errorf("forward apply: leader rejected (code=%s): %s", ack.Code, ack.Error)
	}
	return nil
}

// getOrDialLeader returns the cached leader connection if it's still open
// and pointed at the right leader, or dials a new one.
func (c *Coordinator) getOrDialLeader(ctx context.Context, leaderID, leaderAddr string) (net.Conn, error) {
	c.forwardConnMu.Lock()
	defer c.forwardConnMu.Unlock()

	// Reuse existing connection if it's to the same leader.
	if c.forwardConn != nil && c.forwardConnLeader == leaderID {
		return c.forwardConn, nil
	}

	// Leader changed or no connection — close stale and dial fresh.
	if c.forwardConn != nil {
		c.forwardConn.Close()
		c.forwardConn = nil
		c.forwardConnLeader = ""
	}

	dialTimeout := forwardApplyTimeout
	if deadline, ok := ctx.Deadline(); ok {
		if remaining := time.Until(deadline); remaining < dialTimeout {
			dialTimeout = remaining
		}
	}
	conn, err := security.Dial("tcp", leaderAddr, dialTimeout, c.tlsConfig)
	if err != nil {
		return nil, fmt.Errorf("dial leader %s (%s): %w", leaderID, leaderAddr, err)
	}

	c.forwardConn = conn
	c.forwardConnLeader = leaderID
	return conn, nil
}

// closeForwardConn closes the cached leader connection (if any) so the
// next call to getOrDialLeader redials. Safe to call even if no connection
// is cached.
func (c *Coordinator) closeForwardConn() {
	c.forwardConnMu.Lock()
	defer c.forwardConnMu.Unlock()
	if c.forwardConn != nil {
		c.forwardConn.Close()
		c.forwardConn = nil
		c.forwardConnLeader = ""
	}
}

// handleForwardApply is the server side of leader forwarding. Dispatched
// from handlePeerConnection when MsgForwardApply arrives. Validates HMAC,
// confirms we're still the leader, applies the command via Node.Apply,
// and sends an ack on the same connection.
//
// The connection is NOT owned by this function — the caller
// (handleForwardApplyLoop) manages the connection lifetime to support
// multiple forwarded commands on the same TCP connection.
func (c *Coordinator) handleForwardApply(conn net.Conn, req *protocol.ForwardApplyRequest) {
	remoteAddr := conn.RemoteAddr().String()

	// HMAC validation. Uses the payload-bound variant (ComputeForwardHMAC)
	// that includes SHA-256(CommandJSON) in the signed material. This
	// prevents an on-wire attacker from swapping the command payload while
	// keeping the same MAC, even without TLS.
	if c.cfg.SharedSecret == "" {
		c.sendForwardApplyError(conn, protocol.ForwardCodeAuth, "leader has no shared_secret configured")
		return
	}
	if err := security.ValidateForwardHMAC(
		c.cfg.SharedSecret, req.Nonce, req.NodeID, c.cfg.ClusterName,
		req.CommandJSON, req.Timestamp, req.HMAC, 5*time.Minute,
	); err != nil {
		c.logger.Warn().
			Err(err).
			Str("peer", remoteAddr).
			Str("requesting_node", req.NodeID).
			Msg("ForwardApply rejected: HMAC validation failed")
		c.sendForwardApplyError(conn, protocol.ForwardCodeAuth, "authentication failed")
		return
	}

	// Replay protection: reject duplicate (nodeID, nonce) pairs within
	// the TTL window. The nonce cache is initialized in Start() and
	// shared across all handleForwardApply invocations.
	if c.nonceCache != nil && !c.nonceCache.Track(req.NodeID, req.Nonce) {
		c.logger.Warn().
			Str("peer", remoteAddr).
			Str("requesting_node", req.NodeID).
			Msg("ForwardApply rejected: nonce replay detected")
		c.sendForwardApplyError(conn, protocol.ForwardCodeAuth, "nonce replay")
		return
	}

	if c.raftNode == nil {
		c.sendForwardApplyError(conn, protocol.ForwardCodeRaftUnavailable, "raft not initialized")
		return
	}

	// Authorization: ensure the requesting node's role is allowed to
	// mutate the file manifest. Only writers (CanIngest — flush path)
	// and compactors (CanCompact — compaction bridge) legitimately
	// forward RegisterFile/DeleteFile commands. A compromised reader
	// node should not be able to use its shared-secret credential to
	// delete files from the cluster manifest.
	if peerNode, ok := c.registry.Get(req.NodeID); ok {
		caps := peerNode.Role.GetCapabilities()
		if !caps.CanIngest && !caps.CanCompact {
			c.logger.Warn().
				Str("peer", remoteAddr).
				Str("requesting_node", req.NodeID).
				Str("role", string(peerNode.Role)).
				Msg("ForwardApply rejected: node role not authorized for manifest mutations")
			c.sendForwardApplyError(conn, protocol.ForwardCodeAuth, "unauthorized role")
			return
		}
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

// handleForwardApplyLoop owns the connection for a persistent forwarding
// session. It handles the first request (already parsed by the dispatcher),
// then loops reading additional MsgForwardApply messages until the client
// closes the connection or sends a different message type. This supports
// the client-side connection caching: one dial, many commands.
func (c *Coordinator) handleForwardApplyLoop(conn net.Conn, firstReq *protocol.ForwardApplyRequest) {
	defer conn.Close()

	// Handle the first request that was already parsed by the dispatcher.
	c.handleForwardApply(conn, firstReq)

	// Read subsequent messages on the same connection. The client sends
	// one MsgForwardApply per forwarded command, reusing the connection.
	// A read timeout (30s idle) prevents leaked connections if the client
	// disappears without closing cleanly.
	for {
		_ = conn.SetReadDeadline(time.Now().Add(30 * time.Second))
		msg, err := protocol.ReceiveMessage(conn, 30*time.Second)
		if err != nil {
			// EOF or timeout — client closed or went idle. Normal lifecycle.
			return
		}
		if msg.Type != protocol.MsgForwardApply {
			c.logger.Warn().
				Str("type", msg.Type.String()).
				Msg("ForwardApplyLoop: unexpected message type on persistent connection")
			return
		}
		req, ok := msg.Payload.(*protocol.ForwardApplyRequest)
		if !ok {
			c.logger.Warn().Msg("ForwardApplyLoop: payload type mismatch")
			return
		}
		c.handleForwardApply(conn, req)
	}
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
