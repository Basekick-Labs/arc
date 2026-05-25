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

// IsLeader implements auth.RaftProposer. Returns false defensively when
// the proposer or its underlying coordinator/raft node is nil — those
// paths only happen in OSS / standalone mode where the proposer should
// have been nil at construction (NewCoordinatorAuthProposer returns nil
// for those cases), but the extra checks make the method safe to call
// even from a stale reference. Phase A.1: Cluster Auth Convergence.
func (p *CoordinatorAuthProposer) IsLeader() bool {
	if p == nil || p.coord == nil || p.coord.raftNode == nil {
		return false
	}
	return p.coord.raftNode.IsLeader()
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
	// mergedCancel must be deferred so the watcher goroutine inside
	// mergeContexts exits as soon as forwardApplyToLeader returns,
	// rather than waiting up to forwardCtx's full timeout (Gemini
	// #451 round-4).
	mergedCtx, mergedCancel := mergeContexts(ctx, forwardCtx)
	defer mergedCancel()
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
// cancels, plus a CancelFunc the caller MUST defer.
//
// The returned context inherits values from `a` (the caller-supplied
// request context, which carries trace IDs, loggers, and other
// request-scoped metadata) and additionally cancels when `b` (the
// deadline-bounded forward-apply context) cancels.
//
// **Always defer the returned cancel.** Without it, the watcher
// goroutine keeps running until `b`'s 5-second timeout, leaking one
// goroutine per call even when forwardApplyToLeader returns
// immediately. Gemini #451 round-4 review.
//
// Earlier versions: parented on context.Background (round 1 → dropped
// values; fixed in round 1); leaked the watcher goroutine on early
// return (round 4 → returns CancelFunc; fixed here).
func mergeContexts(a, b context.Context) (context.Context, context.CancelFunc) {
	merged, cancel := context.WithCancel(a)
	go func() {
		select {
		case <-b.Done():
			cancel()
		case <-merged.Done():
		}
	}()
	return merged, cancel
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

// Phase A.1: ToAuth<X>Entry helpers — same pattern as ToAuthTokenEntry
// but for the 5 RBAC entry types. Used by cmd/arc/main.go's
// SetRBACCallbacks closures.

// ToAuthOrganizationEntry converts cluster.raft.OrganizationEntry into
// auth.ClusterOrganizationEntry.
func ToAuthOrganizationEntry(e *raft.OrganizationEntry) auth.ClusterOrganizationEntry {
	if e == nil {
		return auth.ClusterOrganizationEntry{}
	}
	return auth.ClusterOrganizationEntry{
		ID:                e.ID,
		Name:              e.Name,
		Description:       e.Description,
		CreatedAtUnixNano: e.CreatedAtUnixNano,
		UpdatedAtUnixNano: e.UpdatedAtUnixNano,
		Enabled:           e.Enabled,
		LSN:               e.LSN,
	}
}

// ToAuthTeamEntry converts cluster.raft.TeamEntry into auth.ClusterTeamEntry.
func ToAuthTeamEntry(e *raft.TeamEntry) auth.ClusterTeamEntry {
	if e == nil {
		return auth.ClusterTeamEntry{}
	}
	return auth.ClusterTeamEntry{
		ID:                e.ID,
		OrganizationID:    e.OrganizationID,
		Name:              e.Name,
		Description:       e.Description,
		CreatedAtUnixNano: e.CreatedAtUnixNano,
		UpdatedAtUnixNano: e.UpdatedAtUnixNano,
		Enabled:           e.Enabled,
		LSN:               e.LSN,
	}
}

// ToAuthRoleEntry converts cluster.raft.RoleEntry into auth.ClusterRoleEntry.
func ToAuthRoleEntry(e *raft.RoleEntry) auth.ClusterRoleEntry {
	if e == nil {
		return auth.ClusterRoleEntry{}
	}
	return auth.ClusterRoleEntry{
		ID:                e.ID,
		TeamID:            e.TeamID,
		DatabasePattern:   e.DatabasePattern,
		Permissions:       e.Permissions,
		CreatedAtUnixNano: e.CreatedAtUnixNano,
		LSN:               e.LSN,
	}
}

// ToAuthMeasurementPermissionEntry converts
// cluster.raft.MeasurementPermissionEntry into
// auth.ClusterMeasurementPermissionEntry.
func ToAuthMeasurementPermissionEntry(e *raft.MeasurementPermissionEntry) auth.ClusterMeasurementPermissionEntry {
	if e == nil {
		return auth.ClusterMeasurementPermissionEntry{}
	}
	return auth.ClusterMeasurementPermissionEntry{
		ID:                 e.ID,
		RoleID:             e.RoleID,
		MeasurementPattern: e.MeasurementPattern,
		Permissions:        e.Permissions,
		CreatedAtUnixNano:  e.CreatedAtUnixNano,
		LSN:                e.LSN,
	}
}

// ToAuthTokenMembershipEntry converts cluster.raft.TokenMembershipEntry
// into auth.ClusterTokenMembershipEntry.
func ToAuthTokenMembershipEntry(e *raft.TokenMembershipEntry) auth.ClusterTokenMembershipEntry {
	if e == nil {
		return auth.ClusterTokenMembershipEntry{}
	}
	return auth.ClusterTokenMembershipEntry{
		ID:                e.ID,
		TokenID:           e.TokenID,
		TeamID:            e.TeamID,
		CreatedAtUnixNano: e.CreatedAtUnixNano,
		LSN:               e.LSN,
	}
}
