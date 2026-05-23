package cluster

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/basekick-labs/arc/internal/auth"
	"github.com/basekick-labs/arc/internal/cluster/raft"
)

// CoordinatorAuthProposer adapts *Coordinator into the auth.RaftProposer
// interface that AuthManager consults on every write. Construct via
// NewCoordinatorAuthProposer once at startup and pass to
// authManager.SetRaftProposer(...). Phase A: Cluster Auth Convergence.
//
// The shim is a thin wrapper: it builds the cluster.raft.Command envelope
// (Type + JSON-marshalled payload), then either calls raftNode.Apply
// directly (when this node is the leader) or forwards via the existing
// forwardApplyToLeader machinery (when it isn't). Same shape as
// Coordinator.RegisterFileInManifest at coordinator.go:2870.
type CoordinatorAuthProposer struct {
	coord *Coordinator
}

// NewCoordinatorAuthProposer returns a proposer wired to the given
// coordinator. Returns nil if the coordinator is nil or if its raftNode
// is nil — both indicate OSS / standalone mode where the proposer
// should not be installed. Callers in cmd/arc/main.go should check for
// nil before calling authManager.SetRaftProposer.
func NewCoordinatorAuthProposer(c *Coordinator) auth.RaftProposer {
	if c == nil || c.raftNode == nil {
		return nil
	}
	return &CoordinatorAuthProposer{coord: c}
}

// GetRaftFSM exposes the cluster FSM for callers that need to wire
// callbacks (e.g. cmd/arc/main.go installs auth-state callbacks via
// raftFSM.SetAuthCallbacks). Returns nil in OSS / standalone mode.
// Phase A: Cluster Auth Convergence.
func (c *Coordinator) GetRaftFSM() *raft.ClusterFSM {
	if c == nil {
		return nil
	}
	return c.raftFSM
}

// Propose implements auth.RaftProposer.
//
// The auth package owns the payload type definition (CreateTokenPayload,
// UpdateTokenPayload, etc.) and JSON-marshals it before calling Propose;
// we just wrap those bytes in a raft.Command{Type, Payload} envelope and
// hand it to the Raft machinery. The applier-side validation lives in
// internal/cluster/raft/fsm.go's applyCreateToken/etc. functions.
func (p *CoordinatorAuthProposer) Propose(ctx context.Context, cmdType uint8, payload []byte, timeout time.Duration) error {
	if p == nil || p.coord == nil || p.coord.raftNode == nil {
		return auth.ErrLeaderUnknown // defensive — should never happen if constructed via NewCoordinatorAuthProposer
	}

	cmd := &raft.Command{
		Type:    raft.CommandType(cmdType),
		Payload: payload,
	}

	// Leader: apply directly. Apply blocks until the FSM has run + the
	// entry is committed to quorum (hashicorp/raft semantics — by the
	// time Apply returns, the FSM apply has executed AND the FSM's
	// onTokenXxx callback has fired on this node, so the local SQLite
	// materialise is done).
	if p.coord.raftNode.IsLeader() {
		if err := p.coord.raftNode.Apply(cmd, timeout); err != nil {
			return wrapApplyError(err)
		}
		return nil
	}

	// Follower: forward to the current leader. The forward path is the
	// same as for CommandRegisterFile (see coordinator.go:2890).
	forwardCtx, cancel := context.WithTimeout(p.coord.ctxOrBackground(), timeout)
	defer cancel()
	// Caller's context wins if it cancels first; we merge via select.
	mergedCtx := mergeContexts(ctx, forwardCtx)
	if err := p.coord.forwardApplyToLeader(mergedCtx, cmd); err != nil {
		return wrapApplyError(err)
	}
	return nil
}

// wrapApplyError translates the cluster-internal error shapes into the
// auth-side sentinel errors so AuthManager / API handlers can branch
// uniformly. Apply failures from the FSM's applier-side validation
// (e.g. "token name already exists") come back wrapped — preserve them
// via errors.Is for the AuthManager's ensureFirstToken caller.
func wrapApplyError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, ErrNoLeaderKnown) {
		return auth.ErrLeaderUnknown
	}
	// Otherwise it's an FSM apply failure (validation, idempotency,
	// timeout). Wrap with ErrApplyFailed but preserve the original
	// message so the auth-side "already exists" detection in
	// ensureFirstToken keeps working.
	return fmt.Errorf("%w: %w", auth.ErrApplyFailed, err)
}

// mergeContexts returns a context that cancels when EITHER input
// cancels. Lightweight goroutine; the returned context's parent is
// context.Background so it doesn't accidentally inherit deadlines
// from either side.
func mergeContexts(a, b context.Context) context.Context {
	merged, cancel := context.WithCancel(context.Background())
	go func() {
		select {
		case <-a.Done():
		case <-b.Done():
		case <-merged.Done():
			return
		}
		cancel()
	}()
	return merged
}

// ToAuthTokenEntry converts a cluster.raft.TokenEntry (the FSM-side
// shape) into the auth.ClusterTokenEntry shape that AuthManager's
// Apply* methods expect. The two structs are field-for-field
// equivalent — the duplication exists to break the cluster→auth
// import cycle (auth cannot import cluster.raft).
//
// Used by cmd/arc/main.go's SetAuthCallbacks closures.
func ToAuthTokenEntry(e *raft.TokenEntry) auth.ClusterTokenEntry {
	if e == nil {
		return auth.ClusterTokenEntry{}
	}
	return auth.ClusterTokenEntry{
		ID:                e.ID,
		Name:              e.Name,
		Description:       e.Description,
		Permissions:       e.Permissions,
		TokenHash:         e.TokenHash,
		TokenPrefix:       e.TokenPrefix,
		CreatedAtUnixNano: e.CreatedAtUnixNano,
		ExpiresAtUnixNano: e.ExpiresAtUnixNano,
		Enabled:           e.Enabled,
		LSN:               e.LSN,
	}
}
