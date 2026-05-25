package auth

import (
	"context"
	"errors"
	"time"
)

// RaftProposer is the seam between AuthManager and the cluster's Raft
// FSM. AuthManager calls Propose for every write (CreateToken, RevokeToken,
// etc.); the proposer implementation either applies the command directly
// (when this node is the Raft leader) or forwards it to whichever node IS
// the leader. The cluster package supplies a concrete implementation
// (CoordinatorAuthProposer) that wraps a *cluster.Coordinator.
//
// In OSS / standalone deployments the proposer is nil — AuthManager
// detects nil and falls back to the existing direct-SQLite-write path.
// Same shape as the existing cluster.RegisterFileInManifest fallback at
// internal/cluster/coordinator.go (the "if c.raftNode == nil { return
// nil }" pattern).
type RaftProposer interface {
	// Propose submits a command to the Raft FSM. The implementation is
	// responsible for:
	//   - If this node is the leader, calling raftNode.Apply directly.
	//   - Otherwise, forwarding the command to the current leader via
	//     internal/cluster/forward_apply.go.
	//
	// commandType is the cluster.raft.CommandType value (uint8). Auth
	// passes ProposalCommandCreateToken / ProposalCommandUpdateToken / etc.
	// — the values are defined as constants in this package to avoid an
	// import of internal/cluster/raft (which would create a cycle through
	// the cluster package's import of internal/auth).
	//
	// payload is the JSON-encoded command payload (e.g. CreateTokenPayload).
	// The cluster proposer wraps it in a raft.Command envelope itself.
	//
	// Returns nil on a successful apply (the FSM apply on the leader has
	// run; followers will catch up via Raft log replication within the
	// usual <50ms window). Returns ErrLeaderUnknown if the cluster has
	// no current leader; ErrApplyFailed if the FSM rejected the command
	// (e.g. validation failure on applier side); a network error if
	// forwarding to the leader failed.
	Propose(ctx context.Context, commandType uint8, payload []byte, timeout time.Duration) error

	// IsLeader reports whether this node currently holds the Raft
	// leadership. Used by RBACManager.SeedRBACFromLocalSQLite to gate
	// the leader-only upgrade-seed flow — followers skip the seed
	// because their local rows will arrive via Raft replication of the
	// leader's apply log. Defensive against transient elections: if a
	// follower briefly observes itself as leader during a partition,
	// the worst case is a duplicate Create proposal that the FSM-side
	// UNIQUE check rejects as "already exists" — counted as a (harmless)
	// rejection in the metrics.
	//
	// Token bootstrap (ensureFirstToken in auth.go) does NOT call this
	// — it deliberately proposes from every node and relies on FSM-
	// side UNIQUE rejection to ensure only the leader's proposal lands.
	// The seed path is different because there's no "first writer wins"
	// semantics; we'd duplicate every existing local row on every
	// follower and pollute the rejected_total counter.
	IsLeader() bool
}

// Cluster raft command-type constants mirrored into the auth package.
// These MUST match the values in internal/cluster/raft/fsm.go's
// CommandType enum (CommandCreateToken=12, etc.). The mirror is
// load-bearing: the cluster proposer uses these to construct the raft.
// Command envelope, and a desync would result in commands being routed
// to the wrong apply function or rejected as unknown.
//
// We accept the duplication (over an import) because:
//   - cluster imports auth, so auth importing cluster.raft would cycle.
//   - The values are a wire-format contract; they shouldn't drift, and
//     a single test in internal/cluster/raft pins them (see
//     fsm_proposal_pin_test.go's TestProposalCommandTypesMatchFSM).
//
// If a new CommandXxxToken is added to fsm.go, mirror it here and add
// the pinning-test entry.
const (
	ProposalCommandCreateToken uint8 = 12
	ProposalCommandUpdateToken uint8 = 13
	ProposalCommandRevokeToken uint8 = 14
	ProposalCommandDeleteToken uint8 = 15
	ProposalCommandRotateToken uint8 = 16

	// Phase A.1: Cluster Auth Convergence (RBAC). Mirror of fsm.go
	// CommandCreateOrganization (17) through CommandRemoveTokenFromTeam
	// (29). See TestProposalCommandTypesMatchFSM in fsm_proposal_pin_test.go for the
	// wire-format pin covering all 18 values.
	ProposalCommandCreateOrganization          uint8 = 17
	ProposalCommandUpdateOrganization          uint8 = 18
	ProposalCommandDeleteOrganization          uint8 = 19
	ProposalCommandCreateTeam                  uint8 = 20
	ProposalCommandUpdateTeam                  uint8 = 21
	ProposalCommandDeleteTeam                  uint8 = 22
	ProposalCommandCreateRole                  uint8 = 23
	ProposalCommandUpdateRole                  uint8 = 24
	ProposalCommandDeleteRole                  uint8 = 25
	ProposalCommandCreateMeasurementPermission uint8 = 26
	ProposalCommandDeleteMeasurementPermission uint8 = 27
	ProposalCommandAddTokenToTeam              uint8 = 28
	ProposalCommandRemoveTokenFromTeam         uint8 = 29
)

// Sentinel errors that the proposer implementation should return for
// the AuthManager's write path to handle uniformly. The AuthManager
// surfaces these to API handlers as 5xx responses (cluster issue) so
// the operator can distinguish them from 4xx validation rejections.
var (
	// ErrLeaderUnknown means the cluster has no current Raft leader.
	// Typically transient (during election); the caller should retry.
	ErrLeaderUnknown = errors.New("cluster has no current leader")
	// ErrApplyFailed means the FSM apply returned an error — most often
	// applier-side validation (e.g. token name already exists, malformed
	// permissions). The wrapped error contains the specific reason.
	ErrApplyFailed = errors.New("cluster apply failed")
)
