package raft

import (
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/hashicorp/raft"
	"github.com/rs/zerolog"
)

// CommandType represents the type of FSM command.
type CommandType uint8

const (
	// CommandAddNode adds a node to the cluster.
	CommandAddNode CommandType = iota + 1
	// CommandRemoveNode removes a node from the cluster.
	CommandRemoveNode
	// CommandUpdateNode updates node information.
	CommandUpdateNode
	// CommandUpdateNodeState updates a node's state.
	CommandUpdateNodeState
	// CommandPromoteWriter promotes a writer node to primary.
	CommandPromoteWriter
	// CommandDemoteWriter demotes a writer node to standby.
	CommandDemoteWriter
	// CommandRegisterFile announces a newly written file to the cluster manifest.
	CommandRegisterFile
	// CommandDeleteFile removes a file from the cluster manifest.
	CommandDeleteFile
	// CommandAssignCompactor designates a node as the active compactor.
	// Used by the CompactorFailoverManager for automatic failover.
	CommandAssignCompactor
	// CommandBatchFileOps groups multiple RegisterFile and DeleteFile operations
	// into a single Raft log entry — one apply per compaction manifest instead of O(N).
	CommandBatchFileOps
	// CommandUpdateFile replaces an existing file's metadata in the cluster manifest.
	// Used after partial rewrites that change size/checksum but keep the same path.
	CommandUpdateFile
	// CommandCreateToken inserts a new API token into the cluster-wide auth state.
	// The payload carries the bcrypt hash and prefix only — plaintext token values
	// are NEVER written to the Raft log; the proposer returns the plaintext to its
	// caller out-of-band. See Phase A: Cluster Auth Convergence.
	CommandCreateToken
	// CommandUpdateToken updates mutable metadata (name, description, permissions,
	// expires_at) on an existing token. Does NOT change the token hash — use
	// CommandRotateToken for that.
	CommandUpdateToken
	// CommandRevokeToken flips enabled=0 on a token, blocking future Verify calls
	// without removing the row. Reversible.
	CommandRevokeToken
	// CommandDeleteToken hard-deletes a token row from the cluster-wide auth state.
	// Irreversible.
	CommandDeleteToken
	// CommandRotateToken replaces a token's hash + prefix with a fresh pair, while
	// preserving the row's identity (ID, name, permissions, etc.). Same plaintext-
	// secrecy invariant as CommandCreateToken: only the hash + prefix go through
	// Raft.
	CommandRotateToken

	// Phase A.1: Cluster Auth Convergence (RBAC)
	//
	// The RBAC command set extends Phase A's per-token replication to the five
	// RBAC tables: organizations, teams, roles, measurement_permissions, and
	// token_memberships. Each entity gets Create/Update/Delete applies (where
	// applicable) under the same FSM seam used by tokens. Delete commands
	// cascade in-apply across the in-memory maps under a single Raft log
	// entry — mirroring SQLite ON DELETE CASCADE semantics at the FSM level,
	// while the local SQLite materialise relies on its own FK cascade.

	// CommandCreateOrganization inserts a new RBAC organization into the
	// cluster-wide auth state. UNIQUE(name) is enforced applier-side via
	// the organizationsByName index.
	CommandCreateOrganization
	// CommandUpdateOrganization updates mutable metadata (name, description,
	// enabled) on an existing organization.
	CommandUpdateOrganization
	// CommandDeleteOrganization hard-deletes an organization. Cascades to
	// all child teams, roles, measurement_permissions, and token_memberships
	// under a single log entry. Irreversible.
	CommandDeleteOrganization
	// CommandCreateTeam inserts a new team scoped to an organization.
	// UNIQUE(organization_id, name) is enforced applier-side via the
	// teamsByOrg nested index.
	CommandCreateTeam
	// CommandUpdateTeam updates mutable metadata (name, description, enabled)
	// on an existing team.
	CommandUpdateTeam
	// CommandDeleteTeam hard-deletes a team. Cascades to child roles,
	// measurement_permissions, and token_memberships under a single log
	// entry. Irreversible.
	CommandDeleteTeam
	// CommandCreateRole inserts a new role bound to a team. No UNIQUE
	// constraint on (team_id, database_pattern) — same pattern in multiple
	// roles is allowed by design.
	CommandCreateRole
	// CommandUpdateRole updates a role's database_pattern and/or permissions.
	CommandUpdateRole
	// CommandDeleteRole hard-deletes a role. Cascades to child
	// measurement_permissions under a single log entry. Irreversible.
	CommandDeleteRole
	// CommandCreateMeasurementPermission inserts a measurement-level
	// permission scoped to a role. No UNIQUE constraint; multiple patterns
	// per role are allowed.
	CommandCreateMeasurementPermission
	// CommandDeleteMeasurementPermission hard-deletes a measurement
	// permission. Leaf entity — no further cascade.
	CommandDeleteMeasurementPermission
	// CommandAddTokenToTeam grants a token membership in a team.
	// UNIQUE(token_id, team_id) is enforced applier-side via the
	// tokenMembershipsByPair nested index.
	CommandAddTokenToTeam
	// CommandRemoveTokenFromTeam revokes a token's membership in a team.
	CommandRemoveTokenFromTeam
)

// Command represents a command to be applied to the FSM.
type Command struct {
	Type    CommandType `json:"type"`
	Payload []byte      `json:"payload"`
}

// NodeInfo represents node information stored in the FSM.
type NodeInfo struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Role        string `json:"role"`
	ClusterName string `json:"cluster_name"`
	Address     string `json:"address"`
	APIAddress  string `json:"api_address"`
	State       string `json:"state"`
	Version     string `json:"version"`
	WriterState string `json:"writer_state,omitempty"` // "primary", "standby", or "" for non-writers
	CoreCount   int    `json:"core_count"`             // Number of CPU cores on this node
}

// AddNodePayload is the payload for CommandAddNode.
type AddNodePayload struct {
	Node NodeInfo `json:"node"`
}

// RemoveNodePayload is the payload for CommandRemoveNode.
type RemoveNodePayload struct {
	NodeID string `json:"node_id"`
}

// UpdateNodePayload is the payload for CommandUpdateNode.
type UpdateNodePayload struct {
	Node NodeInfo `json:"node"`
}

// UpdateNodeStatePayload is the payload for CommandUpdateNodeState.
type UpdateNodeStatePayload struct {
	NodeID   string `json:"node_id"`
	NewState string `json:"new_state"`
}

// PromoteWriterPayload is the payload for CommandPromoteWriter.
type PromoteWriterPayload struct {
	NodeID       string `json:"node_id"`        // Node to promote to primary
	OldPrimaryID string `json:"old_primary_id"` // Previous primary (to demote)
}

// DemoteWriterPayload is the payload for CommandDemoteWriter.
type DemoteWriterPayload struct {
	NodeID string `json:"node_id"` // Node to demote to standby
}

// FileEntry represents a single Parquet file in the cluster-wide manifest.
// This is the authoritative record of a file's existence, used by peer
// replication to decide what to pull from other nodes.
type FileEntry struct {
	Path          string    `json:"path"`           // Relative storage path (e.g. "db/measurement/2026/04/11/14/file.parquet")
	SHA256        string    `json:"sha256"`         // Content checksum for verification
	SizeBytes     int64     `json:"size_bytes"`     // File size
	Database      string    `json:"database"`       // Arc database name
	Measurement   string    `json:"measurement"`    // Arc measurement name
	PartitionTime time.Time `json:"partition_time"` // Partition time (for hot/cold routing)
	OriginNodeID  string    `json:"origin_node_id"` // Node that first wrote the file
	Tier          string    `json:"tier"`           // "hot" or "cold"
	CreatedAt     time.Time `json:"created_at"`     // When the file was first registered
	LSN           uint64    `json:"lsn"`            // Raft log index at registration (for ordering)
}

// RegisterFilePayload is the payload for CommandRegisterFile.
type RegisterFilePayload struct {
	File FileEntry `json:"file"`
}

// DeleteFilePayload is the payload for CommandDeleteFile.
type DeleteFilePayload struct {
	Path   string `json:"path"`
	Reason string `json:"reason,omitempty"` // "retention", "compaction", "manual"
}

// UpdateFilePayload is the payload for CommandUpdateFile.
type UpdateFilePayload struct {
	File FileEntry `json:"file"`
}

// AssignCompactorPayload is the payload for CommandAssignCompactor.
type AssignCompactorPayload struct {
	NodeID         string `json:"node_id"`
	OldCompactorID string `json:"old_compactor_id,omitempty"`
}

// BatchFileOp is a single operation within a CommandBatchFileOps command.
// Type must be CommandRegisterFile, CommandDeleteFile, or CommandUpdateFile;
// any other value causes the FSM to return an error when the batch is applied.
type BatchFileOp struct {
	Type    CommandType `json:"type"`    // CommandRegisterFile, CommandDeleteFile, or CommandUpdateFile
	Payload []byte      `json:"payload"` // Same payload shape as the corresponding single command
}

// BatchFileOpsPayload is the payload for CommandBatchFileOps.
type BatchFileOpsPayload struct {
	Ops []BatchFileOp `json:"ops"`
}

// TokenEntry represents a single API token in the cluster-wide auth state.
// This is the authoritative record of a token's existence, used by every node's
// AuthManager to materialise its local SQLite cache via the FSM apply callbacks.
//
// Plaintext secrecy invariant: only TokenHash (bcrypt) and TokenPrefix (SHA256
// prefix used for the indexed lookup) are stored. The plaintext token value
// never lands in the Raft log — the proposer returns it directly to its caller
// before the command is marshalled. Same posture as today's CreateToken.
type TokenEntry struct {
	ID                int64  `json:"id"`                             // Raft log index at create time (deterministic across nodes)
	Name              string `json:"name"`                           // Human-readable name; UNIQUE in api_tokens
	Description       string `json:"description,omitempty"`          // Optional free-text
	Permissions       string `json:"permissions"`                    // Comma-separated: "read,write,delete,admin"
	TokenHash         string `json:"token_hash"`                     // bcrypt hash of the plaintext token
	TokenPrefix       string `json:"token_prefix"`                   // SHA256(token)[:16] for indexed lookup
	CreatedAtUnixNano int64  `json:"created_at_unix_nano"`           // Proposer-set; deterministic across log replay
	ExpiresAtUnixNano int64  `json:"expires_at_unix_nano,omitempty"` // 0 = no expiry
	Enabled           bool   `json:"enabled"`                        // false = revoked
	LSN               uint64 `json:"lsn"`                            // Raft log index at last mutation
}

// CreateTokenPayload is the payload for CommandCreateToken.
type CreateTokenPayload struct {
	Token TokenEntry `json:"token"`
}

// UpdateTokenPayload is the payload for CommandUpdateToken. Optional fields
// (nil-able pointers in the request) are encoded as zero values; the apply
// path treats empty strings as "leave unchanged" so that selective updates
// don't require sending the whole record. Only Permissions and ExpiresAt
// follow that rule because Name has a UNIQUE constraint and clobbering it
// from "" to "" is a no-op anyway. ChangedFields lists which fields the
// proposer intends to mutate so apply can distinguish "blank because empty"
// from "blank because not changing."
type UpdateTokenPayload struct {
	ID                int64    `json:"id"`
	Name              string   `json:"name,omitempty"`
	Description       string   `json:"description,omitempty"`
	Permissions       string   `json:"permissions,omitempty"`
	ExpiresAtUnixNano int64    `json:"expires_at_unix_nano,omitempty"`
	ChangedFields     []string `json:"changed_fields"` // subset of {"name","description","permissions","expires_at"}
}

// RevokeTokenPayload is the payload for CommandRevokeToken.
type RevokeTokenPayload struct {
	ID int64 `json:"id"`
}

// DeleteTokenPayload is the payload for CommandDeleteToken.
type DeleteTokenPayload struct {
	ID int64 `json:"id"`
}

// RotateTokenPayload is the payload for CommandRotateToken. The new plaintext
// is generated by the proposer and returned to the caller out of band; only
// the new hash + prefix go through Raft.
type RotateTokenPayload struct {
	ID        int64  `json:"id"`
	NewHash   string `json:"new_hash"`
	NewPrefix string `json:"new_prefix"`
}

// FSMSnapshot represents a snapshot of the FSM state.
//
// Backwards-compatibility note: every map field carries `omitempty`. A
// snapshot taken by an older binary that lacks one of the maps decodes into
// a nil map on the newer binary, and the Restore loop simply iterates zero
// entries — no version field required. The reverse (newer snapshot decoded
// by older binary) silently drops the unknown fields per encoding/json.
type FSMSnapshot struct {
	Nodes             map[string]*NodeInfo  `json:"nodes"`
	PrimaryWriterID   string                `json:"primary_writer_id,omitempty"`
	ActiveCompactorID string                `json:"active_compactor_id,omitempty"`
	Files             map[string]*FileEntry `json:"files,omitempty"`  // File manifest (peer replication)
	Tokens            map[int64]*TokenEntry `json:"tokens,omitempty"` // Cluster-wide auth tokens (Phase A)
	// Phase A.1: Cluster Auth Convergence (RBAC). Secondary indices
	// (organizationsByName, teamsByOrg, etc.) are intentionally NOT
	// persisted — Restore rebuilds them from the primary maps after the
	// JSON decode. This keeps the snapshot format index-agnostic so future
	// index additions don't break old-snapshot reads.
	Organizations          map[int64]*OrganizationEntry          `json:"organizations,omitempty"`
	Teams                  map[int64]*TeamEntry                  `json:"teams,omitempty"`
	Roles                  map[int64]*RoleEntry                  `json:"roles,omitempty"`
	MeasurementPermissions map[int64]*MeasurementPermissionEntry `json:"measurement_permissions,omitempty"`
	TokenMemberships       map[int64]*TokenMembershipEntry       `json:"token_memberships,omitempty"`
}

// ClusterFSM implements the raft.FSM interface for cluster state management.
// It maintains the authoritative state of nodes in the cluster.
type ClusterFSM struct {
	mu                sync.RWMutex
	nodes             map[string]*NodeInfo
	primaryWriterID   string                // ID of the current primary writer node
	activeCompactorID string                // ID of the node currently holding the compactor lease
	files             map[string]*FileEntry // File manifest (path → entry) for peer replication
	// Secondary index: database → set of file paths. Maintained alongside
	// the primary `files` map on register/delete to avoid O(N) scans when
	// filtering by database.
	filesByDB map[string]map[string]struct{}
	// tokens holds the cluster-wide API-token state (Phase A: Cluster Auth
	// Convergence). The map is the source of truth in memory; each node's
	// local SQLite `api_tokens` is a materialised cache rebuilt from
	// CommandCreateToken/Update/etc. callbacks. Keyed by the token's
	// cluster-wide ID (stamped from the Raft log index on create).
	tokens map[int64]*TokenEntry
	// tokensByPrefix is the secondary index used by AuthManager.VerifyToken
	// for O(1) lookup of "tokens with this SHA256(token)[:16] prefix". A
	// prefix collision is astronomically unlikely but the slice form keeps
	// the FSM robust against it. Maintained alongside `tokens`.
	tokensByPrefix map[string][]int64
	// tokensByName is the secondary index used to enforce the cluster-wide
	// UNIQUE(name) constraint that mirrors the SQLite api_tokens schema.
	// Without this index, applyCreateToken/applyUpdateToken would have to
	// scan f.tokens linearly on every apply — a problem because the FSM
	// apply path is single-threaded and blocks the Raft commit loop. Keyed
	// by Name → ID; the inverse mapping back to *TokenEntry goes through
	// f.tokens. Maintained alongside `tokens` and `tokensByPrefix`.
	// Phase A: Cluster Auth Convergence.
	tokensByName map[string]int64

	// -------------------------------------------------------------------
	// Phase A.1: Cluster Auth Convergence (RBAC).
	//
	// Five primary maps mirror the rbac_organizations, rbac_teams,
	// rbac_roles, rbac_measurement_permissions, and rbac_token_memberships
	// SQLite tables. Each map is the in-memory source of truth; the local
	// SQLite tables are materialised caches rebuilt from FSM apply
	// callbacks (same shape as Phase A tokens).
	//
	// Secondary indices serve two roles:
	//   - UNIQUE-constraint enforcement on the single-threaded apply
	//     path. Without them, applyCreateOrganization (and siblings)
	//     would have to scan the primary map linearly on every apply,
	//     blocking the Raft commit loop. Same reasoning as tokensByName.
	//   - Cascade traversal for ON DELETE CASCADE semantics at the FSM
	//     layer. applyDeleteOrganization walks teamsByOrg → rolesByTeam
	//     → measurementPermsByRole → tokenMembershipsByTeam under a
	//     single log entry. SQLite-side cascade fires naturally via FKs
	//     in the local materialise.
	// -------------------------------------------------------------------

	// organizations holds all RBAC organizations, keyed by Raft-stamped ID.
	organizations map[int64]*OrganizationEntry
	// organizationsByName enforces UNIQUE(name) cluster-wide. Keyed by
	// Name → ID; resolve back to *OrganizationEntry via organizations.
	organizationsByName map[string]int64

	// teams holds all RBAC teams, keyed by Raft-stamped ID.
	teams map[int64]*TeamEntry
	// teamsByOrg is the folded UNIQUE-and-traversal index: orgID → name →
	// teamID. Serves both UNIQUE(organization_id, name) enforcement and
	// the "all teams under this org" cascade query. Folded into one map
	// (vs. two separate maps) because the natural traversal key IS the
	// name — saves a map and prevents drift between two indices.
	teamsByOrg map[int64]map[string]int64

	// roles holds all RBAC roles, keyed by Raft-stamped ID.
	roles map[int64]*RoleEntry
	// rolesByTeam is the traversal-only index: teamID → set of roleIDs.
	// Used by applyDeleteTeam / applyDeleteOrganization to cascade.
	rolesByTeam map[int64]map[int64]struct{}

	// measurementPermissions holds all RBAC measurement permissions,
	// keyed by Raft-stamped ID.
	measurementPermissions map[int64]*MeasurementPermissionEntry
	// measurementPermsByRole is the traversal-only index: roleID → set
	// of measurement-permission IDs. Used by applyDeleteRole /
	// applyDeleteTeam / applyDeleteOrganization to cascade.
	measurementPermsByRole map[int64]map[int64]struct{}

	// tokenMemberships holds all RBAC token-team memberships, keyed by
	// Raft-stamped surrogate ID. The (token_id, team_id) pair is the
	// natural business key; the surrogate ID is retained for API parity.
	tokenMemberships map[int64]*TokenMembershipEntry
	// tokenMembershipsByPair enforces UNIQUE(token_id, team_id)
	// cluster-wide. Nested map: tokenID → teamID → membershipID. The
	// inner-map lookup is what every applyAddTokenToTeam does on the
	// UNIQUE-check fast path.
	tokenMembershipsByPair map[int64]map[int64]int64
	// tokenMembershipsByToken is the traversal-only index used by
	// applyDeleteToken to cascade memberships out of FSM state when a
	// token row is hard-deleted (mirrors SQLite FK rbac_token_memberships.
	// token_id REFERENCES api_tokens(id) ON DELETE CASCADE).
	tokenMembershipsByToken map[int64]map[int64]struct{}
	// tokenMembershipsByTeam is the traversal-only index used by
	// applyDeleteTeam / applyDeleteOrganization to cascade memberships
	// when a team is removed.
	tokenMembershipsByTeam map[int64]map[int64]struct{}

	logger zerolog.Logger

	// rejectedPaths counts manifest entries refused by path validation
	// across every code path that mutates the manifest: applyRegisterFile,
	// applyUpdateFile (steady-state runtime AND log replay), batch
	// pre-validation, and Restore (snapshot boot). A non-zero value —
	// or, more usefully, non-zero growth — is the load-bearing operator
	// signal that somebody proposed a path this FSM refused.
	//
	// Surfaced three ways:
	//   - RejectedPathsCount() Go-level getter (used by Node.Start's
	//     end-of-boot summary log).
	//   - The per-entry "manifest path validation failed" Error log lines.
	//   - The process-wide metrics package counter
	//     arc_cluster_manifest_rejected_paths_total, exported via
	//     /metrics (Prometheus) and /api/v1/metrics (JSON). Every
	//     rejectedPaths.Add(1) site also calls
	//     metrics.Get().IncClusterManifestRejectedPaths() so the two
	//     counters stay in sync.
	//
	// Atomic because /metrics scrapes read the (process-wide) sibling
	// counter concurrently with FSM Apply on the runFSM goroutine. See
	// GHSA-f85q-mvg8-qf37.
	rejectedPaths atomic.Int64

	// rejectedTokens counts token commands refused by applier-side
	// validation (empty name, missing hash, malformed permissions, etc.).
	// Same shape as rejectedPaths — atomic for /metrics scrape safety,
	// monotonic, accessed via RejectedTokensCount(). Phase A: Cluster
	// Auth Convergence.
	rejectedTokens atomic.Int64

	// rejectedRBAC counts RBAC commands refused by applier-side validation
	// across all 13 RBAC command types (validation failure, missing
	// parent FK target, UNIQUE constraint violation, etc.). Single
	// counter rather than per-command to keep the metric surface
	// manageable; the per-entry Error log line carries the specific
	// reason. Phase A.1: Cluster Auth Convergence (RBAC).
	rejectedRBAC atomic.Int64

	// Callbacks for state changes
	onNodeAdded         func(*NodeInfo)
	onNodeRemoved       func(string)
	onNodeUpdated       func(*NodeInfo)
	onWriterPromoted    func(newPrimaryID, oldPrimaryID string)
	onCompactorAssigned func(newCompactorID, oldCompactorID string)
	onFileRegistered    func(*FileEntry)
	onFileDeleted       func(path string, reason string)
	// Auth-state callbacks: invoked from every node's apply path so the
	// node's local AuthManager can materialise the change into its
	// SQLite cache (and invalidate the in-memory verify-token cache).
	// All five are wired together via SetAuthCallbacks; nil callbacks
	// are skipped, mirroring the file-manifest pattern.
	onTokenCreated func(*TokenEntry)
	onTokenUpdated func(*TokenEntry)
	onTokenRevoked func(id int64)
	onTokenDeleted func(id int64)
	onTokenRotated func(id int64, newHash, newPrefix string, lsn uint64)

	// RBAC-state callbacks: invoked from every node's apply path so the
	// node's local RBACManager can materialise the change into its SQLite
	// cache (and invalidate the relevant permission caches). All wired
	// together via SetRBACCallbacks; nil callbacks are skipped, mirroring
	// the auth-state pattern. Phase A.1: Cluster Auth Convergence.
	//
	// For Delete callbacks the cascade has already happened in-FSM under
	// the same log entry; the callback is fired ONCE for the top-level
	// deleted entity. Local SQLite cascade fires via FK ON DELETE CASCADE
	// inside the materialise — neither layer needs to enumerate the
	// affected children.
	onOrganizationCreated          func(*OrganizationEntry)
	onOrganizationUpdated          func(*OrganizationEntry)
	onOrganizationDeleted          func(id int64)
	onTeamCreated                  func(*TeamEntry)
	onTeamUpdated                  func(*TeamEntry)
	onTeamDeleted                  func(id int64)
	onRoleCreated                  func(*RoleEntry)
	onRoleUpdated                  func(*RoleEntry)
	onRoleDeleted                  func(id int64)
	onMeasurementPermissionCreated func(*MeasurementPermissionEntry)
	onMeasurementPermissionDeleted func(id int64)
	onTokenMembershipAdded         func(*TokenMembershipEntry)
	onTokenMembershipRemoved       func(tokenID, teamID int64)
}

// NewClusterFSM creates a new cluster FSM.
func NewClusterFSM(logger zerolog.Logger) *ClusterFSM {
	return &ClusterFSM{
		nodes:          make(map[string]*NodeInfo),
		files:          make(map[string]*FileEntry),
		filesByDB:      make(map[string]map[string]struct{}),
		tokens:         make(map[int64]*TokenEntry),
		tokensByPrefix: make(map[string][]int64),
		tokensByName:   make(map[string]int64),

		// Phase A.1: RBAC primary maps + secondary indices.
		organizations:           make(map[int64]*OrganizationEntry),
		organizationsByName:     make(map[string]int64),
		teams:                   make(map[int64]*TeamEntry),
		teamsByOrg:              make(map[int64]map[string]int64),
		roles:                   make(map[int64]*RoleEntry),
		rolesByTeam:             make(map[int64]map[int64]struct{}),
		measurementPermissions:  make(map[int64]*MeasurementPermissionEntry),
		measurementPermsByRole:  make(map[int64]map[int64]struct{}),
		tokenMemberships:        make(map[int64]*TokenMembershipEntry),
		tokenMembershipsByPair:  make(map[int64]map[int64]int64),
		tokenMembershipsByToken: make(map[int64]map[int64]struct{}),
		tokenMembershipsByTeam:  make(map[int64]map[int64]struct{}),

		logger: logger.With().Str("component", "cluster-fsm").Logger(),
	}
}

// rejectManifestPath centralizes the FSM-side response to a path that
// failed ValidateManifestPath. Used by applyRegisterFile and
// applyUpdateFile.
//
// In every case: log at Error, increment the rejectedPaths counter,
// and return a wrapped validation error. The downstream behaviour
// then depends on whether Apply is being called by a proposer or by
// log replay — and crucially, the *security property* (entry does
// NOT land in f.files) holds in both cases because we return BEFORE
// any state mutation in the caller.
//
//   - Proposer path (Arc's Node.RegisterFile / .UpdateFile etc.):
//     hashicorp/raft delivers the error via future.Response(), and
//     Arc's node.go propagates it to the caller. The leader's
//     Register/Update API call returns the validation error.
//
//   - Log replay (hashicorp/raft runFSM goroutine processing committed
//     entries past the snapshot): the error is silently swallowed by
//     runFSM (req.future == nil for replayed entries) — boot
//     continues, the entry doesn't land, the counter is incremented,
//     and operators see the Error log line.
//
// Note: hashicorp/raft commits a proposed entry to the leader's log
// AND replicates it to followers BEFORE Apply runs. So a malicious
// path is briefly in every node's Raft log; the security property is
// that no node's FSM puts it in f.files. The Raft log is bounded by
// the snapshot rotation interval — eventually the malicious entry is
// garbage-collected when a snapshot is taken (Snapshot drains
// f.files, which never contained the malicious entry).
//
// op is "register" or "update" for log disambiguation. errors.Is(err,
// ErrPath*) gives the rejection reason. See GHSA-f85q-mvg8-qf37.
func (f *ClusterFSM) rejectManifestPath(op, path string, logIndex uint64, err error) error {
	f.incRejectedPaths()
	f.logger.Error().
		Err(err).
		Str("op", op).
		Str("path", path).
		Uint64("log_index", logIndex).
		Msg("manifest path validation failed — entry refused, not added to f.files")
	return fmt.Errorf("%s file: %w", op, err)
}

// incRejectedPaths increments both the per-FSM rejectedPaths counter
// (consumed by Node.Start's end-of-boot summary log + tests) and the
// process-wide metrics-package counter (exposed via /metrics and
// /api/v1/metrics as arc_cluster_manifest_rejected_paths_total).
// Every site that refuses a manifest entry — runtime apply, batch
// pre-pass, snapshot Restore — calls through here so the two
// counters stay in sync. See GHSA-f85q-mvg8-qf37.
func (f *ClusterFSM) incRejectedPaths() {
	f.rejectedPaths.Add(1)
	metrics.Get().IncClusterManifestRejectedPaths()
}

// RejectedPathsCount returns the count of manifest entries refused by
// path validation across every FSM code path (snapshot Restore +
// runtime Apply + log replay Apply). The counter is monotonic. A
// non-zero growth rate is the load-bearing operator signal that
// somebody — a peer, a stored snapshot, or a pre-validation Raft log
// entry — proposed a path this FSM refused. Scrape and alert on it.
// See GHSA-f85q-mvg8-qf37.
func (f *ClusterFSM) RejectedPathsCount() int64 {
	return f.rejectedPaths.Load()
}

// SetCallbacks sets the FSM callbacks for state changes.
func (f *ClusterFSM) SetCallbacks(onAdded func(*NodeInfo), onRemoved func(string), onUpdated func(*NodeInfo)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.onNodeAdded = onAdded
	f.onNodeRemoved = onRemoved
	f.onNodeUpdated = onUpdated
}

// SetWriterPromotedCallback sets the callback for writer promotion events.
func (f *ClusterFSM) SetWriterPromotedCallback(cb func(newPrimaryID, oldPrimaryID string)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.onWriterPromoted = cb
}

// SetCompactorAssignedCallback sets the callback for compactor assignment events.
func (f *ClusterFSM) SetCompactorAssignedCallback(cb func(newCompactorID, oldCompactorID string)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.onCompactorAssigned = cb
}

// GetActiveCompactorID returns the node ID currently holding the compactor lease.
func (f *ClusterFSM) GetActiveCompactorID() string {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.activeCompactorID
}

// SetFileCallbacks sets the callbacks for file manifest events (peer replication).
func (f *ClusterFSM) SetFileCallbacks(onRegistered func(*FileEntry), onDeleted func(path, reason string)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.onFileRegistered = onRegistered
	f.onFileDeleted = onDeleted
}

// SetAuthCallbacks wires the cluster FSM into the local AuthManager so that
// every node's SQLite api_tokens table stays consistent with the Raft-
// replicated auth state. Callbacks fire after the FSM's in-memory tokens
// map has been mutated, on the runFSM goroutine. Each is allowed to take
// its time (the AuthManager methods do a single SQLite write + cache
// invalidate — sub-millisecond at typical token counts).
//
// Nil callbacks are tolerated; non-cluster (OSS) builds simply never
// register handlers and the FSM's apply path is the source of truth in
// memory without any SQLite mirror. Phase A: Cluster Auth Convergence.
func (f *ClusterFSM) SetAuthCallbacks(
	onCreated func(*TokenEntry),
	onUpdated func(*TokenEntry),
	onRevoked func(id int64),
	onDeleted func(id int64),
	onRotated func(id int64, newHash, newPrefix string, lsn uint64),
) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.onTokenCreated = onCreated
	f.onTokenUpdated = onUpdated
	f.onTokenRevoked = onRevoked
	f.onTokenDeleted = onDeleted
	f.onTokenRotated = onRotated
}

// RejectedTokensCount returns the count of token commands refused by
// applier-side validation (empty name, missing hash, malformed permissions,
// etc.). Same shape as RejectedPathsCount — monotonic, atomic, scraped by
// the test suite and the operator alerting playbook.
func (f *ClusterFSM) RejectedTokensCount() int64 {
	return f.rejectedTokens.Load()
}

// incRejectedTokens increments the local rejection counter. Mirrors
// incRejectedPaths in shape; if/when a process-wide metrics counter
// for auth rejections is added, this is the single site to wire it.
func (f *ClusterFSM) incRejectedTokens() {
	f.rejectedTokens.Add(1)
	metrics.Get().IncClusterAuthRejected()
}

// SetRBACCallbacks wires the cluster FSM into the local RBACManager so that
// every node's SQLite RBAC tables stay consistent with the Raft-replicated
// RBAC state. Same shape as SetAuthCallbacks but with 13 callback slots
// (matching the 13 RBAC command types). Each callback fires after the FSM's
// in-memory map mutation, on the runFSM goroutine.
//
// For Delete callbacks the FSM-side cascade has already happened under the
// same log entry; the callback is fired ONCE per top-level deleted entity
// and the local SQLite cascade fires via ON DELETE CASCADE inside the
// materialise. Phase A.1: Cluster Auth Convergence (RBAC).
func (f *ClusterFSM) SetRBACCallbacks(
	onOrgCreated func(*OrganizationEntry),
	onOrgUpdated func(*OrganizationEntry),
	onOrgDeleted func(id int64),
	onTeamCreated func(*TeamEntry),
	onTeamUpdated func(*TeamEntry),
	onTeamDeleted func(id int64),
	onRoleCreated func(*RoleEntry),
	onRoleUpdated func(*RoleEntry),
	onRoleDeleted func(id int64),
	onMeasurementPermissionCreated func(*MeasurementPermissionEntry),
	onMeasurementPermissionDeleted func(id int64),
	onTokenMembershipAdded func(*TokenMembershipEntry),
	onTokenMembershipRemoved func(tokenID, teamID int64),
) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.onOrganizationCreated = onOrgCreated
	f.onOrganizationUpdated = onOrgUpdated
	f.onOrganizationDeleted = onOrgDeleted
	f.onTeamCreated = onTeamCreated
	f.onTeamUpdated = onTeamUpdated
	f.onTeamDeleted = onTeamDeleted
	f.onRoleCreated = onRoleCreated
	f.onRoleUpdated = onRoleUpdated
	f.onRoleDeleted = onRoleDeleted
	f.onMeasurementPermissionCreated = onMeasurementPermissionCreated
	f.onMeasurementPermissionDeleted = onMeasurementPermissionDeleted
	f.onTokenMembershipAdded = onTokenMembershipAdded
	f.onTokenMembershipRemoved = onTokenMembershipRemoved
}

// RejectedRBACCount returns the count of RBAC commands refused by
// applier-side validation across all 13 RBAC command types. Single
// monotonic counter, atomic for /metrics scrape safety, mirrors
// RejectedTokensCount. Phase A.1: Cluster Auth Convergence (RBAC).
func (f *ClusterFSM) RejectedRBACCount() int64 {
	return f.rejectedRBAC.Load()
}

// GetTokenByID returns a copy of the token entry with the given ID, or
// nil if no such token exists in the FSM. Used by tests + by the
// AuthManager when a local SQLite materialise needs to reconcile with
// the cluster-authoritative state (rare path: a node booting from a
// stale local DB).
func (f *ClusterFSM) GetTokenByID(id int64) *TokenEntry {
	f.mu.RLock()
	defer f.mu.RUnlock()
	entry, ok := f.tokens[id]
	if !ok {
		return nil
	}
	copy := *entry
	return &copy
}

// TokenCount returns the number of tokens currently in the FSM.
func (f *ClusterFSM) TokenCount() int {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return len(f.tokens)
}

// GetPrimaryWriterID returns the current primary writer node ID.
func (f *ClusterFSM) GetPrimaryWriterID() string {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.primaryWriterID
}

// Apply applies a Raft log entry to the FSM.
// This is called by Raft when a log entry is committed.
func (f *ClusterFSM) Apply(log *raft.Log) interface{} {
	var cmd Command
	if err := json.Unmarshal(log.Data, &cmd); err != nil {
		f.logger.Error().Err(err).Msg("Failed to unmarshal command")
		return fmt.Errorf("failed to unmarshal command: %w", err)
	}

	switch cmd.Type {
	case CommandAddNode:
		return f.applyAddNode(cmd.Payload)
	case CommandRemoveNode:
		return f.applyRemoveNode(cmd.Payload)
	case CommandUpdateNode:
		return f.applyUpdateNode(cmd.Payload)
	case CommandUpdateNodeState:
		return f.applyUpdateNodeState(cmd.Payload)
	case CommandPromoteWriter:
		return f.applyPromoteWriter(cmd.Payload)
	case CommandDemoteWriter:
		return f.applyDemoteWriter(cmd.Payload)
	case CommandRegisterFile:
		return f.applyRegisterFile(cmd.Payload, log.Index)
	case CommandDeleteFile:
		return f.applyDeleteFile(cmd.Payload)
	case CommandAssignCompactor:
		return f.applyAssignCompactor(cmd.Payload)
	case CommandBatchFileOps:
		return f.applyBatchFileOps(cmd.Payload, log.Index)
	case CommandUpdateFile:
		return f.applyUpdateFile(cmd.Payload, log.Index)
	case CommandCreateToken:
		return f.applyCreateToken(cmd.Payload, log.Index)
	case CommandUpdateToken:
		return f.applyUpdateToken(cmd.Payload, log.Index)
	case CommandRevokeToken:
		return f.applyRevokeToken(cmd.Payload, log.Index)
	case CommandDeleteToken:
		return f.applyDeleteToken(cmd.Payload, log.Index)
	case CommandRotateToken:
		return f.applyRotateToken(cmd.Payload, log.Index)

	// Phase A.1: RBAC dispatch (13 cases). All apply functions live in
	// fsm_rbac.go; the dispatch is here to keep the wire-format contract
	// (CommandType ↔ apply function) in one place.
	case CommandCreateOrganization:
		return f.applyCreateOrganization(cmd.Payload, log.Index)
	case CommandUpdateOrganization:
		return f.applyUpdateOrganization(cmd.Payload, log.Index)
	case CommandDeleteOrganization:
		return f.applyDeleteOrganization(cmd.Payload, log.Index)
	case CommandCreateTeam:
		return f.applyCreateTeam(cmd.Payload, log.Index)
	case CommandUpdateTeam:
		return f.applyUpdateTeam(cmd.Payload, log.Index)
	case CommandDeleteTeam:
		return f.applyDeleteTeam(cmd.Payload, log.Index)
	case CommandCreateRole:
		return f.applyCreateRole(cmd.Payload, log.Index)
	case CommandUpdateRole:
		return f.applyUpdateRole(cmd.Payload, log.Index)
	case CommandDeleteRole:
		return f.applyDeleteRole(cmd.Payload, log.Index)
	case CommandCreateMeasurementPermission:
		return f.applyCreateMeasurementPermission(cmd.Payload, log.Index)
	case CommandDeleteMeasurementPermission:
		return f.applyDeleteMeasurementPermission(cmd.Payload, log.Index)
	case CommandAddTokenToTeam:
		return f.applyAddTokenToTeam(cmd.Payload, log.Index)
	case CommandRemoveTokenFromTeam:
		return f.applyRemoveTokenFromTeam(cmd.Payload, log.Index)

	default:
		return fmt.Errorf("unknown command type: %d", cmd.Type)
	}
}

func (f *ClusterFSM) applyAddNode(payload []byte) interface{} {
	var p AddNodePayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("failed to unmarshal add node payload: %w", err)
	}

	f.mu.Lock()
	f.nodes[p.Node.ID] = &p.Node
	callback := f.onNodeAdded
	f.mu.Unlock()

	f.logger.Info().
		Str("node_id", p.Node.ID).
		Str("role", p.Node.Role).
		Msg("Node added to cluster state")

	if callback != nil {
		callback(&p.Node)
	}

	return nil
}

func (f *ClusterFSM) applyRemoveNode(payload []byte) interface{} {
	var p RemoveNodePayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("failed to unmarshal remove node payload: %w", err)
	}

	f.mu.Lock()
	delete(f.nodes, p.NodeID)
	callback := f.onNodeRemoved
	f.mu.Unlock()

	f.logger.Info().
		Str("node_id", p.NodeID).
		Msg("Node removed from cluster state")

	if callback != nil {
		callback(p.NodeID)
	}

	return nil
}

func (f *ClusterFSM) applyUpdateNode(payload []byte) interface{} {
	var p UpdateNodePayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("failed to unmarshal update node payload: %w", err)
	}

	f.mu.Lock()
	f.nodes[p.Node.ID] = &p.Node
	callback := f.onNodeUpdated
	f.mu.Unlock()

	f.logger.Debug().
		Str("node_id", p.Node.ID).
		Msg("Node updated in cluster state")

	if callback != nil {
		callback(&p.Node)
	}

	return nil
}

func (f *ClusterFSM) applyUpdateNodeState(payload []byte) interface{} {
	var p UpdateNodeStatePayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("failed to unmarshal update node state payload: %w", err)
	}

	f.mu.Lock()
	node, exists := f.nodes[p.NodeID]
	if exists {
		node.State = p.NewState
	}
	callback := f.onNodeUpdated
	f.mu.Unlock()

	if !exists {
		return fmt.Errorf("node %s not found", p.NodeID)
	}

	f.logger.Debug().
		Str("node_id", p.NodeID).
		Str("new_state", p.NewState).
		Msg("Node state updated")

	if callback != nil {
		callback(node)
	}

	return nil
}

func (f *ClusterFSM) applyPromoteWriter(payload []byte) interface{} {
	var p PromoteWriterPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("failed to unmarshal promote writer payload: %w", err)
	}

	if p.NodeID == "" {
		return fmt.Errorf("promote writer: node_id is required")
	}

	f.mu.Lock()
	// Validate the node exists and is a writer
	if node, exists := f.nodes[p.NodeID]; exists && node.Role != "writer" {
		f.mu.Unlock()
		return fmt.Errorf("promote writer: node %s has role %s, expected writer", p.NodeID, node.Role)
	}

	// Warn if OldPrimaryID doesn't match actual primary (informational only — FSM uses its own tracking)
	oldPrimaryID := f.primaryWriterID
	if p.OldPrimaryID != "" && oldPrimaryID != "" && p.OldPrimaryID != oldPrimaryID {
		f.logger.Warn().
			Str("expected_old_primary", p.OldPrimaryID).
			Str("actual_old_primary", oldPrimaryID).
			Msg("OldPrimaryID mismatch during promotion")
	}
	if oldPrimaryID != "" && oldPrimaryID != p.NodeID {
		if oldNode, exists := f.nodes[oldPrimaryID]; exists {
			oldNode.WriterState = "standby"
		}
	}

	// Promote new primary
	newNode, exists := f.nodes[p.NodeID]
	if exists {
		newNode.WriterState = "primary"
	}
	f.primaryWriterID = p.NodeID
	callback := f.onWriterPromoted
	f.mu.Unlock()

	if !exists {
		return fmt.Errorf("node %s not found", p.NodeID)
	}

	f.logger.Info().
		Str("new_primary", p.NodeID).
		Str("old_primary", oldPrimaryID).
		Msg("Writer promoted to primary")

	if callback != nil {
		callback(p.NodeID, oldPrimaryID)
	}

	return nil
}

func (f *ClusterFSM) applyDemoteWriter(payload []byte) interface{} {
	var p DemoteWriterPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("failed to unmarshal demote writer payload: %w", err)
	}

	if p.NodeID == "" {
		return fmt.Errorf("demote writer: node_id is required")
	}

	f.mu.Lock()
	node, exists := f.nodes[p.NodeID]
	if exists {
		node.WriterState = "standby"
	}
	if f.primaryWriterID == p.NodeID {
		f.primaryWriterID = ""
	}
	f.mu.Unlock()

	if !exists {
		return fmt.Errorf("node %s not found", p.NodeID)
	}

	f.logger.Info().
		Str("node_id", p.NodeID).
		Msg("Writer demoted to standby")

	return nil
}

func (f *ClusterFSM) applyRegisterFile(payload []byte, logIndex uint64) interface{} {
	var p RegisterFilePayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("failed to unmarshal register file payload: %w", err)
	}
	return f.applyRegisterFileStruct(p, logIndex)
}

// applyRegisterFileStruct is the struct-taking variant of applyRegisterFile.
// applyBatchFileOps calls this directly after unmarshalling the payload
// once in its pre-validation pass, avoiding a second unmarshal in the
// apply loop. Non-batch callers go through applyRegisterFile, which
// unmarshals + dispatches here.
func (f *ClusterFSM) applyRegisterFileStruct(p RegisterFilePayload, logIndex uint64) interface{} {
	// Validate the path BEFORE any state mutation. See GHSA-f85q-mvg8-qf37:
	// historically the only check was empty-string, which let an attacker
	// register arbitrary paths (/etc/passwd, s3://attacker-bucket/..., etc.)
	// into the authoritative manifest. ValidateManifestPath rejects every
	// malicious shape from the audit (scheme, absolute, parent-traversal,
	// NUL, oversize) plus the historical empty-string case.
	if err := ValidateManifestPath(p.File.Path); err != nil {
		return f.rejectManifestPath("register", p.File.Path, logIndex, err)
	}
	if p.File.CreatedAt.IsZero() {
		// The creator is responsible for setting CreatedAt before appending
		// to Raft. We do NOT fill it in here because time.Now() inside Apply
		// is non-deterministic — log replay on other nodes would produce
		// different values and diverge FSM state.
		return fmt.Errorf("register file: created_at is required")
	}

	// Stamp the LSN from the Raft log index (deterministic across all nodes)
	p.File.LSN = logIndex

	f.mu.Lock()
	entry := p.File
	// If the file was already registered under a different database (unlikely
	// but possible if an operator moves a file across databases), remove the
	// old index entry first to keep filesByDB consistent.
	if old, existed := f.files[entry.Path]; existed && old.Database != entry.Database {
		if oldIdx, ok := f.filesByDB[old.Database]; ok {
			delete(oldIdx, old.Path)
			if len(oldIdx) == 0 {
				delete(f.filesByDB, old.Database)
			}
		}
	}
	f.files[entry.Path] = &entry
	// Maintain the database → files secondary index
	idx, ok := f.filesByDB[entry.Database]
	if !ok {
		idx = make(map[string]struct{})
		f.filesByDB[entry.Database] = idx
	}
	idx[entry.Path] = struct{}{}
	callback := f.onFileRegistered
	f.mu.Unlock()

	f.logger.Debug().
		Str("path", entry.Path).
		Str("database", entry.Database).
		Str("measurement", entry.Measurement).
		Str("origin", entry.OriginNodeID).
		Int64("size_bytes", entry.SizeBytes).
		Uint64("lsn", entry.LSN).
		Msg("File registered in cluster manifest")

	if callback != nil {
		entryCopy := entry
		callback(&entryCopy)
	}

	return nil
}

func (f *ClusterFSM) applyDeleteFile(payload []byte) interface{} {
	var p DeleteFilePayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("failed to unmarshal delete file payload: %w", err)
	}
	return f.applyDeleteFileStruct(p)
}

// applyDeleteFileStruct is the struct-taking variant of applyDeleteFile.
// applyBatchFileOps calls this directly after unmarshalling once during
// pre-validation, avoiding a second unmarshal in the apply loop.
func (f *ClusterFSM) applyDeleteFileStruct(p DeleteFilePayload) interface{} {
	if p.Path == "" {
		return fmt.Errorf("delete file: path is required")
	}

	f.mu.Lock()
	existing, existed := f.files[p.Path]
	delete(f.files, p.Path)
	if existed {
		// Remove from the database → files secondary index
		if idx, ok := f.filesByDB[existing.Database]; ok {
			delete(idx, p.Path)
			if len(idx) == 0 {
				delete(f.filesByDB, existing.Database)
			}
		}
	}
	callback := f.onFileDeleted
	f.mu.Unlock()

	if !existed {
		// Idempotent — deletion of a non-existent file is a no-op
		return nil
	}

	f.logger.Debug().
		Str("path", p.Path).
		Str("reason", p.Reason).
		Msg("File removed from cluster manifest")

	if callback != nil {
		callback(p.Path, p.Reason)
	}

	return nil
}

func (f *ClusterFSM) applyUpdateFile(payload []byte, logIndex uint64) interface{} {
	var p UpdateFilePayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("failed to unmarshal update file payload: %w", err)
	}
	return f.applyUpdateFileStruct(p, logIndex)
}

// applyUpdateFileStruct is the struct-taking variant of applyUpdateFile.
// applyBatchFileOps calls this directly after unmarshalling once during
// pre-validation, avoiding a second unmarshal in the apply loop.
func (f *ClusterFSM) applyUpdateFileStruct(p UpdateFilePayload, logIndex uint64) interface{} {
	// Same validation as applyRegisterFile — an attacker who can submit
	// Update commands could otherwise insert a new manifest entry at a
	// malicious path (Update writes f.files[entry.Path] = &entry, so a
	// payload with Path="/etc/passwd" would create a fresh entry at
	// that path rather than mutating any pre-existing one). The end
	// risk to operators is the same as in applyRegisterFile: a hostile
	// path in f.files. See GHSA-f85q-mvg8-qf37.
	if err := ValidateManifestPath(p.File.Path); err != nil {
		return f.rejectManifestPath("update", p.File.Path, logIndex, err)
	}
	if p.File.CreatedAt.IsZero() {
		// Same reasoning as applyRegisterFileStruct: the caller is
		// responsible for setting CreatedAt before appending to Raft.
		// Apply MUST NOT stamp it from time.Now() because that would
		// be non-deterministic across nodes during log replay. An
		// Update payload with a zero CreatedAt is a caller bug — it
		// would clobber the original entry's creation timestamp with
		// the JSON zero value.
		return fmt.Errorf("update file: created_at is required")
	}

	// Stamp the LSN from the current Raft log index so consumers that
	// watch f.files for "did this entry change" can detect the update.
	// Without this, an Update that mutates an existing entry would
	// leave the LSN at its registration-time value, and downstream
	// consumers (e.g. compaction watchers) couldn't distinguish a
	// fresh state from a stale one.
	p.File.LSN = logIndex

	f.mu.Lock()
	entry := p.File
	// If the database changed (defensive), remove the old secondary index entry first.
	if old, existed := f.files[entry.Path]; existed && old.Database != entry.Database {
		if oldIdx, ok := f.filesByDB[old.Database]; ok {
			delete(oldIdx, old.Path)
			if len(oldIdx) == 0 {
				delete(f.filesByDB, old.Database)
			}
		}
	}
	f.files[entry.Path] = &entry
	if entry.Database != "" {
		idx, ok := f.filesByDB[entry.Database]
		if !ok {
			idx = make(map[string]struct{})
			f.filesByDB[entry.Database] = idx
		}
		idx[entry.Path] = struct{}{}
	}
	callback := f.onFileRegistered
	f.mu.Unlock()

	f.logger.Debug().
		Str("path", entry.Path).
		Int64("size_bytes", entry.SizeBytes).
		Str("sha256", entry.SHA256).
		Msg("File updated in cluster manifest")

	// Trigger onFileRegistered so reader nodes detect the content change and
	// pull the updated file from the writer. The file path is the same but the
	// content (and checksum) changed, so readers must re-fetch it.
	if callback != nil {
		entryCopy := entry
		callback(&entryCopy)
	}

	return nil
}

func (f *ClusterFSM) applyAssignCompactor(payload []byte) interface{} {
	var p AssignCompactorPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("failed to unmarshal assign compactor payload: %w", err)
	}

	if p.NodeID == "" {
		return fmt.Errorf("assign compactor: node_id is required")
	}

	f.mu.Lock()
	oldCompactorID := f.activeCompactorID
	f.activeCompactorID = p.NodeID
	callback := f.onCompactorAssigned
	f.mu.Unlock()

	f.logger.Info().
		Str("new_compactor", p.NodeID).
		Str("old_compactor", oldCompactorID).
		Msg("Compactor lease assigned")

	if callback != nil {
		callback(p.NodeID, oldCompactorID)
	}

	return nil
}

func (f *ClusterFSM) applyBatchFileOps(payload []byte, logIndex uint64) interface{} {
	var p BatchFileOpsPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("failed to unmarshal batch file ops payload: %w", err)
	}

	// Pre-validate every Register/Update path BEFORE any state
	// mutation so the batch is atomic with respect to path validation:
	// if any op carries a malicious path, the whole batch is refused
	// and NO ops have side-effects. Without this pass, an attacker
	// could place a legitimate op first and a malicious op second —
	// the legitimate op would land before the loop hit the malicious
	// one. See GHSA-f85q-mvg8-qf37 review notes.
	//
	// Delete ops are not validated here because applyDeleteFile only
	// uses the path as a map key (delete of a non-existent key is a
	// no-op); validating delete paths would change existing semantics
	// for callers that legitimately race delete-then-register.
	//
	// We store each decoded Register/Update payload in parallel slots
	// (decoded[i]) so the apply loop below can call the *Struct apply
	// variants directly without re-unmarshalling. Each payload is
	// decoded exactly once; Delete ops keep nil decoded slots.
	decoded := make([]any, len(p.Ops))
	for i, op := range p.Ops {
		switch op.Type {
		case CommandRegisterFile:
			var rp RegisterFilePayload
			if err := json.Unmarshal(op.Payload, &rp); err != nil {
				return fmt.Errorf("batch file ops: op[%d] unmarshal: %w", i, err)
			}
			if err := ValidateManifestPath(rp.File.Path); err != nil {
				f.incRejectedPaths()
				f.logger.Error().
					Err(err).
					Str("op", "batch-prevalidate").
					Str("path", rp.File.Path).
					Uint64("log_index", logIndex).
					Int("batch_index", i).
					Msg("manifest path validation failed during batch pre-check — entire batch refused")
				return fmt.Errorf("batch file ops: op[%d]: %w", i, err)
			}
			// Mirror applyRegisterFileStruct's CreatedAt requirement
			// in the pre-pass so a batch that would fail mid-apply on
			// this check is refused atomically up front.
			if rp.File.CreatedAt.IsZero() {
				return fmt.Errorf("batch file ops: op[%d]: register file: created_at is required", i)
			}
			decoded[i] = rp
		case CommandUpdateFile:
			var up UpdateFilePayload
			if err := json.Unmarshal(op.Payload, &up); err != nil {
				return fmt.Errorf("batch file ops: op[%d] unmarshal: %w", i, err)
			}
			if err := ValidateManifestPath(up.File.Path); err != nil {
				f.incRejectedPaths()
				f.logger.Error().
					Err(err).
					Str("op", "batch-prevalidate").
					Str("path", up.File.Path).
					Uint64("log_index", logIndex).
					Int("batch_index", i).
					Msg("manifest path validation failed during batch pre-check — entire batch refused")
				return fmt.Errorf("batch file ops: op[%d]: %w", i, err)
			}
			// Mirror applyUpdateFileStruct's CreatedAt requirement.
			if up.File.CreatedAt.IsZero() {
				return fmt.Errorf("batch file ops: op[%d]: update file: created_at is required", i)
			}
			decoded[i] = up
		case CommandDeleteFile:
			// Delete paths intentionally bypass ValidateManifestPath
			// (applyDeleteFile only uses the path as a map key — delete
			// of a non-existent key is a no-op). BUT we still pre-check
			// the structural invariants that applyDeleteFile enforces:
			// unmarshal success and non-empty path. Without this, a
			// malformed Delete payload would pass the pre-pass and fail
			// mid-apply, violating the batch atomicity invariant. The
			// decoded payload is stored in decoded[i] so the apply loop
			// reuses it (one unmarshal per Delete op instead of two).
			var dp DeleteFilePayload
			if err := json.Unmarshal(op.Payload, &dp); err != nil {
				return fmt.Errorf("batch file ops: op[%d] unmarshal: %w", i, err)
			}
			if dp.Path == "" {
				return fmt.Errorf("batch file ops: op[%d]: delete file: path is required", i)
			}
			decoded[i] = dp
		default:
			return fmt.Errorf("batch file ops: op[%d] unsupported type: %d", i, op.Type)
		}
	}

	// Apply loop. Each *Struct call re-validates path + CreatedAt that
	// the pre-pass above already passed for the same payload — that's
	// intentional defense-in-depth (two cheap checks vs. one JSON
	// unmarshal), and it lets the *Struct functions stand on their
	// own when called outside the batch path (single-op Register /
	// Update from internal/cluster/raft/node.go).
	for i, op := range p.Ops {
		var result interface{}
		switch op.Type {
		case CommandRegisterFile:
			result = f.applyRegisterFileStruct(decoded[i].(RegisterFilePayload), logIndex)
		case CommandDeleteFile:
			result = f.applyDeleteFileStruct(decoded[i].(DeleteFilePayload))
		case CommandUpdateFile:
			result = f.applyUpdateFileStruct(decoded[i].(UpdateFilePayload), logIndex)
		default:
			// Unreachable: pre-pass at lines above rejects unsupported
			// op types. Kept for type-completeness so the switch isn't
			// missing the default arm.
			return fmt.Errorf("batch file ops: op[%d] unsupported type: %d", i, op.Type)
		}
		// apply*FileStruct return nil on success or an error on failure
		// — they never return a non-nil non-error value. The type-assert
		// is defensive: if any handler is ever refactored to return
		// something unexpected, we propagate it as an error rather than
		// silently ignoring it.
		if result != nil {
			if err, ok := result.(error); ok {
				return fmt.Errorf("batch file ops: op[%d] (type=%d): %w", i, op.Type, err)
			}
			return fmt.Errorf("batch file ops: op[%d] (type=%d): unexpected non-error result: %v", i, op.Type, result)
		}
	}
	return nil
}

// rejectToken centralises FSM-side rejection of a malformed token command.
// Same shape as rejectManifestPath: log at Error, bump counters, return a
// wrapped validation error. The security property — entry does NOT land in
// f.tokens — holds because callers return BEFORE any state mutation.
//
// op is one of "create", "update", "revoke", "delete", "rotate" for log
// disambiguation. Phase A: Cluster Auth Convergence.
func (f *ClusterFSM) rejectToken(op string, id int64, logIndex uint64, err error) error {
	f.incRejectedTokens()
	f.logger.Error().
		Err(err).
		Str("op", op).
		Int64("token_id", id).
		Uint64("log_index", logIndex).
		Msg("token command validation failed — entry refused, not added to f.tokens")
	return fmt.Errorf("%s token: %w", op, err)
}

// validateTokenEntry performs applier-side validation of a TokenEntry. Same
// design as ValidateManifestPath: every node runs this on apply, so a
// malformed entry never lands in f.tokens regardless of which node proposed
// it. Returns nil on success; any other return triggers rejectToken.
//
// Checks (cheapest first):
//   - Name non-empty and length-bounded (avoid a DOS via 1GB names).
//   - TokenHash non-empty (we never accept tokens without a bcrypt hash —
//     the plaintext path is proposer-side only).
//   - TokenPrefix non-empty (required for the indexed lookup).
//   - Permissions contains only the allowed verbs.
func validateTokenEntry(entry *TokenEntry) error {
	if entry == nil {
		return fmt.Errorf("token entry is nil")
	}
	if entry.Name == "" {
		return fmt.Errorf("token name is required")
	}
	if len(entry.Name) > 256 {
		return fmt.Errorf("token name too long: %d > 256", len(entry.Name))
	}
	if entry.TokenHash == "" {
		return fmt.Errorf("token hash is required")
	}
	if entry.TokenPrefix == "" {
		return fmt.Errorf("token prefix is required")
	}
	if err := validatePermissionString(entry.Permissions); err != nil {
		return err
	}
	return nil
}

// validatePermissionString rejects anything outside the documented verb set.
// The allowed verbs are "read", "write", "delete", "admin"; empty is allowed
// (means "no permissions"). Comma-separated. Tolerates ASCII whitespace
// around each verb (defensive normalisation via splitCSV) so a typo in the
// proposer's normalise step doesn't surface as a confusing " write"-is-not-
// allowed error.
func validatePermissionString(perms string) error {
	if perms == "" {
		return nil
	}
	allowed := map[string]bool{"read": true, "write": true, "delete": true, "admin": true}
	for _, p := range splitCSV(perms) {
		if !allowed[p] {
			return fmt.Errorf("invalid permission %q (allowed: read, write, delete, admin)", p)
		}
	}
	return nil
}

// splitCSV is a comma splitter that trims surrounding ASCII whitespace
// from each token. The proposer is expected to normalise input but we
// trim defensively here so a forgotten normalise step at the proposer
// surfaces as a clean validation error (the verb itself was unknown)
// rather than a confusing one (" write" vs "write"). Gemini #451
// round-2 review.
func splitCSV(s string) []string {
	if s == "" {
		return nil
	}
	out := make([]string, 0, 4)
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == ',' {
			out = append(out, trimASCIISpace(s[start:i]))
			start = i + 1
		}
	}
	out = append(out, trimASCIISpace(s[start:]))
	return out
}

// trimASCIISpace strips leading/trailing spaces and tabs from s without
// allocating when there's nothing to trim. Cheaper than strings.TrimSpace
// (which handles full Unicode whitespace) for the permission-string use
// case where only ASCII space/tab can appear in legitimate input.
func trimASCIISpace(s string) string {
	lo, hi := 0, len(s)
	for lo < hi && (s[lo] == ' ' || s[lo] == '\t') {
		lo++
	}
	for hi > lo && (s[hi-1] == ' ' || s[hi-1] == '\t') {
		hi--
	}
	if lo == 0 && hi == len(s) {
		return s
	}
	return s[lo:hi]
}

func (f *ClusterFSM) applyCreateToken(payload []byte, logIndex uint64) interface{} {
	var p CreateTokenPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("failed to unmarshal create token payload: %w", err)
	}
	if err := validateTokenEntry(&p.Token); err != nil {
		return f.rejectToken("create", p.Token.ID, logIndex, err)
	}
	if p.Token.CreatedAtUnixNano == 0 {
		// Same reasoning as applyRegisterFileStruct: the proposer must set
		// CreatedAt before appending to Raft. Apply MUST NOT fill it in
		// from time.Now() because that would be non-deterministic during
		// log replay.
		return f.rejectToken("create", p.Token.ID, logIndex, fmt.Errorf("created_at is required"))
	}

	// Stamp the ID and LSN from the Raft log index (deterministic across
	// all nodes). The proposer leaves both at 0; apply assigns them.
	entry := p.Token
	entry.ID = int64(logIndex)
	entry.LSN = logIndex
	if !entry.Enabled {
		// New tokens default to enabled. The proposer should also set this
		// but apply enforces the invariant for older payloads / fuzz.
		entry.Enabled = true
	}

	f.mu.Lock()
	// Reject if a token with the same name already exists (mirrors the
	// SQLite UNIQUE constraint on api_tokens.name; without this check, two
	// concurrent proposers could each commit a CreateToken with the same
	// name and the slower node's SQLite write would fail with UNIQUE
	// violation while the FSM map happily holds both). O(1) via the
	// tokensByName secondary index — the apply path is single-threaded
	// and blocks the Raft commit loop, so a linear scan over all tokens
	// would bottleneck the cluster as the token count grows.
	if _, exists := f.tokensByName[entry.Name]; exists {
		f.mu.Unlock()
		return f.rejectToken("create", entry.ID, logIndex, fmt.Errorf("token name %q already exists", entry.Name))
	}
	f.tokens[entry.ID] = &entry
	f.tokensByPrefix[entry.TokenPrefix] = append(f.tokensByPrefix[entry.TokenPrefix], entry.ID)
	f.tokensByName[entry.Name] = entry.ID
	callback := f.onTokenCreated
	f.mu.Unlock()

	metrics.Get().IncClusterAuthApplyCreate()
	f.logger.Debug().
		Int64("token_id", entry.ID).
		Str("name", entry.Name).
		Str("permissions", entry.Permissions).
		Uint64("lsn", entry.LSN).
		Msg("Token created in cluster auth state")

	if callback != nil {
		entryCopy := entry
		callback(&entryCopy)
	}
	return nil
}

func (f *ClusterFSM) applyUpdateToken(payload []byte, logIndex uint64) interface{} {
	var p UpdateTokenPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("failed to unmarshal update token payload: %w", err)
	}
	if p.ID == 0 {
		return f.rejectToken("update", 0, logIndex, fmt.Errorf("token id is required"))
	}
	// Validate any permission string we're about to write so a malformed
	// payload doesn't land in f.tokens. Other fields are free-form.
	for _, field := range p.ChangedFields {
		if field == "permissions" {
			if err := validatePermissionString(p.Permissions); err != nil {
				return f.rejectToken("update", p.ID, logIndex, err)
			}
		}
	}

	f.mu.Lock()
	entry, ok := f.tokens[p.ID]
	if !ok {
		f.mu.Unlock()
		// Idempotent: an update of a deleted token is silently dropped on
		// log replay (otherwise replaying a snapshot after a delete would
		// break). Counter still bumps so operators see the divergence.
		f.incRejectedTokens()
		f.logger.Warn().Int64("token_id", p.ID).Uint64("log_index", logIndex).Msg("Update of unknown token; dropping")
		return nil
	}
	// Apply each changed field. Empty Name/Permissions in p are taken as
	// "leave unchanged" UNLESS the field is listed in ChangedFields.
	changed := make(map[string]bool, len(p.ChangedFields))
	for _, field := range p.ChangedFields {
		changed[field] = true
	}
	if changed["name"] {
		// Enforce cluster-wide name uniqueness for the new name. O(1) via
		// the tokensByName secondary index (Gemini #451 review feedback —
		// the apply path is single-threaded and blocks the Raft commit
		// loop, so a linear scan was a future scaling bottleneck).
		if otherID, exists := f.tokensByName[p.Name]; exists && otherID != p.ID {
			f.mu.Unlock()
			return f.rejectToken("update", p.ID, logIndex, fmt.Errorf("token name %q already exists", p.Name))
		}
		// Maintain the name index: drop the old binding, add the new one.
		delete(f.tokensByName, entry.Name)
		f.tokensByName[p.Name] = entry.ID
		entry.Name = p.Name
	}
	if changed["description"] {
		entry.Description = p.Description
	}
	if changed["permissions"] {
		entry.Permissions = p.Permissions
	}
	if changed["expires_at"] {
		entry.ExpiresAtUnixNano = p.ExpiresAtUnixNano
	}
	entry.LSN = logIndex
	updated := *entry
	callback := f.onTokenUpdated
	f.mu.Unlock()

	metrics.Get().IncClusterAuthApplyUpdate()
	f.logger.Debug().
		Int64("token_id", p.ID).
		Strs("changed_fields", p.ChangedFields).
		Uint64("lsn", logIndex).
		Msg("Token updated in cluster auth state")

	if callback != nil {
		callback(&updated)
	}
	return nil
}

func (f *ClusterFSM) applyRevokeToken(payload []byte, logIndex uint64) interface{} {
	var p RevokeTokenPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("failed to unmarshal revoke token payload: %w", err)
	}
	if p.ID == 0 {
		return f.rejectToken("revoke", 0, logIndex, fmt.Errorf("token id is required"))
	}

	f.mu.Lock()
	entry, ok := f.tokens[p.ID]
	if !ok {
		f.mu.Unlock()
		// Idempotent on log replay.
		return nil
	}
	entry.Enabled = false
	entry.LSN = logIndex
	callback := f.onTokenRevoked
	f.mu.Unlock()

	metrics.Get().IncClusterAuthApplyRevoke()
	f.logger.Info().Int64("token_id", p.ID).Uint64("lsn", logIndex).Msg("Token revoked in cluster auth state")
	if callback != nil {
		callback(p.ID)
	}
	return nil
}

func (f *ClusterFSM) applyDeleteToken(payload []byte, logIndex uint64) interface{} {
	var p DeleteTokenPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("failed to unmarshal delete token payload: %w", err)
	}
	if p.ID == 0 {
		return f.rejectToken("delete", 0, logIndex, fmt.Errorf("id is required"))
	}

	f.mu.Lock()
	entry, ok := f.tokens[p.ID]
	if !ok {
		f.mu.Unlock()
		// Idempotent — deletion of a non-existent token is a no-op.
		return nil
	}
	delete(f.tokens, p.ID)
	// Remove from the prefix index.
	if ids, ok := f.tokensByPrefix[entry.TokenPrefix]; ok {
		out := ids[:0]
		for _, id := range ids {
			if id != p.ID {
				out = append(out, id)
			}
		}
		if len(out) == 0 {
			delete(f.tokensByPrefix, entry.TokenPrefix)
		} else {
			f.tokensByPrefix[entry.TokenPrefix] = out
		}
	}
	// Remove from the name index so a future create with the same name
	// is no longer rejected as a duplicate.
	delete(f.tokensByName, entry.Name)

	// Phase A.1 extension: cascade out of FSM state for any RBAC
	// memberships this token held. Mirrors the SQLite FK ON DELETE
	// CASCADE on rbac_token_memberships.token_id REFERENCES
	// api_tokens(id). Without this the FSM membership map would hold
	// orphans pointing at a non-existent token row, diverging from
	// the local SQLite materialise.
	cascadedMemberships := 0
	if tokenSet, ok := f.tokenMembershipsByToken[p.ID]; ok {
		for membershipID := range tokenSet {
			mem, ok := f.tokenMemberships[membershipID]
			if !ok {
				continue
			}
			// Remove from byPair.
			if pairMap, ok := f.tokenMembershipsByPair[mem.TokenID]; ok {
				delete(pairMap, mem.TeamID)
				if len(pairMap) == 0 {
					delete(f.tokenMembershipsByPair, mem.TokenID)
				}
			}
			// Remove from byTeam.
			if teamSet, ok := f.tokenMembershipsByTeam[mem.TeamID]; ok {
				delete(teamSet, membershipID)
				if len(teamSet) == 0 {
					delete(f.tokenMembershipsByTeam, mem.TeamID)
				}
			}
			delete(f.tokenMemberships, membershipID)
			cascadedMemberships++
		}
		delete(f.tokenMembershipsByToken, p.ID)
	}

	callback := f.onTokenDeleted
	f.mu.Unlock()

	metrics.Get().IncClusterAuthApplyDelete()
	logEvt := f.logger.Info().Int64("token_id", p.ID)
	if cascadedMemberships > 0 {
		logEvt = logEvt.Int("cascaded_memberships", cascadedMemberships)
	}
	logEvt.Msg("Token deleted from cluster auth state")
	if callback != nil {
		callback(p.ID)
	}
	return nil
}

func (f *ClusterFSM) applyRotateToken(payload []byte, logIndex uint64) interface{} {
	var p RotateTokenPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("failed to unmarshal rotate token payload: %w", err)
	}
	if p.ID == 0 {
		return f.rejectToken("rotate", 0, logIndex, fmt.Errorf("token id is required"))
	}
	if p.NewHash == "" || p.NewPrefix == "" {
		return f.rejectToken("rotate", p.ID, logIndex, fmt.Errorf("new_hash and new_prefix are required"))
	}

	f.mu.Lock()
	entry, ok := f.tokens[p.ID]
	if !ok {
		f.mu.Unlock()
		return nil // idempotent
	}
	oldPrefix := entry.TokenPrefix
	entry.TokenHash = p.NewHash
	entry.TokenPrefix = p.NewPrefix
	entry.LSN = logIndex
	// Maintain the prefix index: remove from old slot, add to new.
	if oldPrefix != p.NewPrefix {
		if ids, ok := f.tokensByPrefix[oldPrefix]; ok {
			out := ids[:0]
			for _, id := range ids {
				if id != p.ID {
					out = append(out, id)
				}
			}
			if len(out) == 0 {
				delete(f.tokensByPrefix, oldPrefix)
			} else {
				f.tokensByPrefix[oldPrefix] = out
			}
		}
		f.tokensByPrefix[p.NewPrefix] = append(f.tokensByPrefix[p.NewPrefix], p.ID)
	}
	callback := f.onTokenRotated
	f.mu.Unlock()

	metrics.Get().IncClusterAuthApplyRotate()
	f.logger.Info().Int64("token_id", p.ID).Uint64("lsn", logIndex).Msg("Token rotated in cluster auth state")
	if callback != nil {
		callback(p.ID, p.NewHash, p.NewPrefix, logIndex)
	}
	return nil
}

// Snapshot returns a snapshot of the FSM state.
func (f *ClusterFSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	// Deep copy nodes
	nodes := make(map[string]*NodeInfo, len(f.nodes))
	for id, node := range f.nodes {
		nodeCopy := *node
		nodes[id] = &nodeCopy
	}

	// Deep copy files
	files := make(map[string]*FileEntry, len(f.files))
	for path, file := range f.files {
		fileCopy := *file
		files[path] = &fileCopy
	}

	// Deep copy tokens
	tokens := make(map[int64]*TokenEntry, len(f.tokens))
	for id, t := range f.tokens {
		tokenCopy := *t
		tokens[id] = &tokenCopy
	}

	// Phase A.1: Deep copy RBAC primary maps. Secondary indices are NOT
	// persisted — Restore rebuilds them from these primaries.
	organizations := make(map[int64]*OrganizationEntry, len(f.organizations))
	for id, e := range f.organizations {
		entryCopy := *e
		organizations[id] = &entryCopy
	}
	teams := make(map[int64]*TeamEntry, len(f.teams))
	for id, e := range f.teams {
		entryCopy := *e
		teams[id] = &entryCopy
	}
	roles := make(map[int64]*RoleEntry, len(f.roles))
	for id, e := range f.roles {
		entryCopy := *e
		roles[id] = &entryCopy
	}
	measurementPermissions := make(map[int64]*MeasurementPermissionEntry, len(f.measurementPermissions))
	for id, e := range f.measurementPermissions {
		entryCopy := *e
		measurementPermissions[id] = &entryCopy
	}
	tokenMemberships := make(map[int64]*TokenMembershipEntry, len(f.tokenMemberships))
	for id, e := range f.tokenMemberships {
		entryCopy := *e
		tokenMemberships[id] = &entryCopy
	}

	return &fsmSnapshot{
		nodes:                  nodes,
		primaryWriterID:        f.primaryWriterID,
		activeCompactorID:      f.activeCompactorID,
		files:                  files,
		tokens:                 tokens,
		organizations:          organizations,
		teams:                  teams,
		roles:                  roles,
		measurementPermissions: measurementPermissions,
		tokenMemberships:       tokenMemberships,
	}, nil
}

// Restore restores the FSM from a snapshot.
func (f *ClusterFSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()

	var snapshot FSMSnapshot
	if err := json.NewDecoder(rc).Decode(&snapshot); err != nil {
		return fmt.Errorf("failed to decode snapshot: %w", err)
	}

	// Validate every restored manifest path. Entries that fail
	// validation are LOGGED AT ERROR + SKIPPED (NOT added to f.files)
	// and counted into rejectedPaths so operators can alert on a
	// non-zero growth rate. See GHSA-f85q-mvg8-qf37. We deliberately
	// continue restoring the rest of the snapshot rather than failing
	// hard — a cluster boot blocked by a single malicious entry in a
	// historical snapshot would be unrecoverable.
	//
	// Rebuild f.files in a fresh map rather than mutating
	// snapshot.Files in place; this keeps the snapshot struct
	// untouched and makes the filter step impossible to skip on
	// subsequent reads.
	restoredFiles := make(map[string]*FileEntry, len(snapshot.Files))
	for path, entry := range snapshot.Files {
		// Defensive nil-check: f.files is map[string]*FileEntry, so a
		// corrupted or attacker-crafted snapshot with `null` values
		// would deserialize to nil pointers here. Without this skip,
		// the secondary-index rebuild below (entry.Database) would
		// panic — bricking the node's boot. Treat as a quarantine.
		// See Gemini PR #446 r6.
		if entry == nil {
			f.incRejectedPaths()
			f.logger.Error().
				Str("path", path).
				Str("source", "snapshot").
				Msg("manifest entry is nil during snapshot restore — entry refused, not added to f.files")
			continue
		}
		if err := ValidateManifestPath(path); err != nil {
			f.incRejectedPaths()
			f.logger.Error().
				Err(err).
				Str("path", path).
				Str("source", "snapshot").
				Msg("manifest path validation failed during snapshot restore — entry refused, not added to f.files")
			continue
		}
		restoredFiles[path] = entry
	}

	// Validate every restored token. Same quarantine policy as files: a
	// snapshot entry that fails validateTokenEntry is skipped + counted
	// into rejectedTokens. Operators alert on growth. The historical
	// reason for choosing skip-not-fail is the same: a single bad entry
	// from an attacker or a corrupted snapshot should not block boot.
	restoredTokens := make(map[int64]*TokenEntry, len(snapshot.Tokens))
	for id, entry := range snapshot.Tokens {
		if entry == nil {
			f.incRejectedTokens()
			f.logger.Error().Int64("token_id", id).Str("source", "snapshot").Msg("token entry is nil during snapshot restore — entry refused")
			continue
		}
		if err := validateTokenEntry(entry); err != nil {
			f.incRejectedTokens()
			f.logger.Error().Err(err).Int64("token_id", id).Str("source", "snapshot").Msg("token validation failed during snapshot restore — entry refused")
			continue
		}
		restoredTokens[id] = entry
	}

	// Phase A.1: same quarantine policy for the 5 RBAC entry types.
	// Each map is validated independently; a bad entry in one map does
	// NOT affect the others. rejectedRBAC counts all skipped entries
	// across all 5 maps.
	restoredOrgs := make(map[int64]*OrganizationEntry, len(snapshot.Organizations))
	// Track names seen so far to enforce UNIQUE(name) during restore.
	// A corrupted snapshot with two orgs sharing a name would
	// otherwise rebuild organizationsByName with the second write
	// silently overwriting the first — the loser stays in the primary
	// map but is unreachable via the name index, the FSM is
	// permanently inconsistent. Quarantine duplicates instead.
	// Gemini PR #458 round 7 G25.
	restoredOrgNames := make(map[string]int64, len(snapshot.Organizations))
	for id, entry := range snapshot.Organizations {
		if entry == nil {
			f.incRejectedRBAC()
			f.logger.Error().Int64("organization_id", id).Str("source", "snapshot").Msg("organization entry is nil during snapshot restore — entry refused")
			continue
		}
		if err := validateOrganizationEntry(entry); err != nil {
			f.incRejectedRBAC()
			f.logger.Error().Err(err).Int64("organization_id", id).Str("source", "snapshot").Msg("organization validation failed during snapshot restore — entry refused")
			continue
		}
		if dupID, exists := restoredOrgNames[entry.Name]; exists {
			f.incRejectedRBAC()
			f.logger.Error().
				Int64("organization_id", id).
				Int64("duplicate_of_id", dupID).
				Str("name", entry.Name).
				Str("source", "snapshot").
				Msg("duplicate organization name during snapshot restore — entry refused")
			continue
		}
		restoredOrgs[id] = entry
		restoredOrgNames[entry.Name] = id
	}
	restoredTeams := make(map[int64]*TeamEntry, len(snapshot.Teams))
	// Track (orgID, name) seen so far to enforce UNIQUE(org_id, name)
	// during restore. Same defence as the org-name check above —
	// symmetric to applyCreateTeam's UNIQUE enforcement. Gemini PR #458
	// round 7 G25 (extended to the composite-key case).
	restoredTeamsByOrgAndName := make(map[int64]map[string]int64)
	for id, entry := range snapshot.Teams {
		if entry == nil {
			f.incRejectedRBAC()
			f.logger.Error().Int64("team_id", id).Str("source", "snapshot").Msg("team entry is nil during snapshot restore — entry refused")
			continue
		}
		if err := validateTeamEntry(entry); err != nil {
			f.incRejectedRBAC()
			f.logger.Error().Err(err).Int64("team_id", id).Str("source", "snapshot").Msg("team validation failed during snapshot restore — entry refused")
			continue
		}
		// Orphan check: team's parent org must have survived restore.
		// Otherwise we'd have a stranded team in the FSM with no UNIQUE
		// scope to enforce against. Skip + count.
		if _, ok := restoredOrgs[entry.OrganizationID]; !ok {
			f.incRejectedRBAC()
			f.logger.Error().Int64("team_id", id).Int64("organization_id", entry.OrganizationID).Str("source", "snapshot").Msg("team references unknown organization during snapshot restore — entry refused")
			continue
		}
		// Duplicate (org_id, name) check.
		orgNames := restoredTeamsByOrgAndName[entry.OrganizationID]
		if dupID, exists := orgNames[entry.Name]; exists {
			f.incRejectedRBAC()
			f.logger.Error().
				Int64("team_id", id).
				Int64("duplicate_of_id", dupID).
				Int64("organization_id", entry.OrganizationID).
				Str("name", entry.Name).
				Str("source", "snapshot").
				Msg("duplicate team name within organization during snapshot restore — entry refused")
			continue
		}
		if orgNames == nil {
			orgNames = make(map[string]int64)
			restoredTeamsByOrgAndName[entry.OrganizationID] = orgNames
		}
		orgNames[entry.Name] = id
		restoredTeams[id] = entry
	}
	restoredRoles := make(map[int64]*RoleEntry, len(snapshot.Roles))
	for id, entry := range snapshot.Roles {
		if entry == nil {
			f.incRejectedRBAC()
			f.logger.Error().Int64("role_id", id).Str("source", "snapshot").Msg("role entry is nil during snapshot restore — entry refused")
			continue
		}
		if err := validateRoleEntry(entry); err != nil {
			f.incRejectedRBAC()
			f.logger.Error().Err(err).Int64("role_id", id).Str("source", "snapshot").Msg("role validation failed during snapshot restore — entry refused")
			continue
		}
		if _, ok := restoredTeams[entry.TeamID]; !ok {
			f.incRejectedRBAC()
			f.logger.Error().Int64("role_id", id).Int64("team_id", entry.TeamID).Str("source", "snapshot").Msg("role references unknown team during snapshot restore — entry refused")
			continue
		}
		restoredRoles[id] = entry
	}
	restoredMPerms := make(map[int64]*MeasurementPermissionEntry, len(snapshot.MeasurementPermissions))
	for id, entry := range snapshot.MeasurementPermissions {
		if entry == nil {
			f.incRejectedRBAC()
			f.logger.Error().Int64("measurement_permission_id", id).Str("source", "snapshot").Msg("measurement_permission entry is nil during snapshot restore — entry refused")
			continue
		}
		if err := validateMeasurementPermissionEntry(entry); err != nil {
			f.incRejectedRBAC()
			f.logger.Error().Err(err).Int64("measurement_permission_id", id).Str("source", "snapshot").Msg("measurement_permission validation failed during snapshot restore — entry refused")
			continue
		}
		if _, ok := restoredRoles[entry.RoleID]; !ok {
			f.incRejectedRBAC()
			f.logger.Error().Int64("measurement_permission_id", id).Int64("role_id", entry.RoleID).Str("source", "snapshot").Msg("measurement_permission references unknown role during snapshot restore — entry refused")
			continue
		}
		restoredMPerms[id] = entry
	}
	restoredMemberships := make(map[int64]*TokenMembershipEntry, len(snapshot.TokenMemberships))
	// Track (token_id, team_id) seen so far to enforce UNIQUE(token_id,
	// team_id) during restore. Symmetric to applyAddTokenToTeam's
	// UNIQUE enforcement and the org/team duplicate-name checks
	// above. Gemini PR #458 round 7 G25 (extended).
	restoredMembershipPairs := make(map[int64]map[int64]int64)
	for id, entry := range snapshot.TokenMemberships {
		if entry == nil {
			f.incRejectedRBAC()
			f.logger.Error().Int64("membership_id", id).Str("source", "snapshot").Msg("token_membership entry is nil during snapshot restore — entry refused")
			continue
		}
		if err := validateTokenMembershipEntry(entry); err != nil {
			f.incRejectedRBAC()
			f.logger.Error().Err(err).Int64("membership_id", id).Str("source", "snapshot").Msg("token_membership validation failed during snapshot restore — entry refused")
			continue
		}
		if _, ok := restoredTokens[entry.TokenID]; !ok {
			f.incRejectedRBAC()
			f.logger.Error().Int64("membership_id", id).Int64("token_id", entry.TokenID).Str("source", "snapshot").Msg("token_membership references unknown token during snapshot restore — entry refused")
			continue
		}
		if _, ok := restoredTeams[entry.TeamID]; !ok {
			f.incRejectedRBAC()
			f.logger.Error().Int64("membership_id", id).Int64("team_id", entry.TeamID).Str("source", "snapshot").Msg("token_membership references unknown team during snapshot restore — entry refused")
			continue
		}
		// Duplicate (token_id, team_id) check.
		tokenPairs := restoredMembershipPairs[entry.TokenID]
		if dupID, exists := tokenPairs[entry.TeamID]; exists {
			f.incRejectedRBAC()
			f.logger.Error().
				Int64("membership_id", id).
				Int64("duplicate_of_id", dupID).
				Int64("token_id", entry.TokenID).
				Int64("team_id", entry.TeamID).
				Str("source", "snapshot").
				Msg("duplicate token-team membership during snapshot restore — entry refused")
			continue
		}
		if tokenPairs == nil {
			tokenPairs = make(map[int64]int64)
			restoredMembershipPairs[entry.TokenID] = tokenPairs
		}
		tokenPairs[entry.TeamID] = id
		restoredMemberships[id] = entry
	}

	f.mu.Lock()
	f.nodes = snapshot.Nodes
	f.primaryWriterID = snapshot.PrimaryWriterID
	f.activeCompactorID = snapshot.ActiveCompactorID
	f.files = restoredFiles
	// Rebuild the database → files secondary index from the restored files
	f.filesByDB = make(map[string]map[string]struct{}, len(f.files))
	for path, entry := range f.files {
		idx, ok := f.filesByDB[entry.Database]
		if !ok {
			idx = make(map[string]struct{})
			f.filesByDB[entry.Database] = idx
		}
		idx[path] = struct{}{}
	}
	// Rebuild tokens + both secondary indices. The prefix index supports
	// AuthManager.VerifyToken lookups; the name index supports O(1)
	// uniqueness checks in applyCreateToken/applyUpdateToken (Gemini #451
	// review feedback).
	f.tokens = restoredTokens
	f.tokensByPrefix = make(map[string][]int64, len(f.tokens))
	f.tokensByName = make(map[string]int64, len(f.tokens))
	for id, entry := range f.tokens {
		f.tokensByPrefix[entry.TokenPrefix] = append(f.tokensByPrefix[entry.TokenPrefix], id)
		f.tokensByName[entry.Name] = id
	}

	// Phase A.1: install RBAC primary maps + rebuild all secondary
	// indices from scratch (we never persist them — they're derived).
	f.organizations = restoredOrgs
	f.organizationsByName = make(map[string]int64, len(f.organizations))
	for id, entry := range f.organizations {
		f.organizationsByName[entry.Name] = id
	}
	f.teams = restoredTeams
	f.teamsByOrg = make(map[int64]map[string]int64)
	for id, entry := range f.teams {
		orgTeams, ok := f.teamsByOrg[entry.OrganizationID]
		if !ok {
			orgTeams = make(map[string]int64)
			f.teamsByOrg[entry.OrganizationID] = orgTeams
		}
		orgTeams[entry.Name] = id
	}
	f.roles = restoredRoles
	f.rolesByTeam = make(map[int64]map[int64]struct{})
	for id, entry := range f.roles {
		teamRoles, ok := f.rolesByTeam[entry.TeamID]
		if !ok {
			teamRoles = make(map[int64]struct{})
			f.rolesByTeam[entry.TeamID] = teamRoles
		}
		teamRoles[id] = struct{}{}
	}
	f.measurementPermissions = restoredMPerms
	f.measurementPermsByRole = make(map[int64]map[int64]struct{})
	for id, entry := range f.measurementPermissions {
		roleMPerms, ok := f.measurementPermsByRole[entry.RoleID]
		if !ok {
			roleMPerms = make(map[int64]struct{})
			f.measurementPermsByRole[entry.RoleID] = roleMPerms
		}
		roleMPerms[id] = struct{}{}
	}
	f.tokenMemberships = restoredMemberships
	f.tokenMembershipsByPair = make(map[int64]map[int64]int64)
	f.tokenMembershipsByToken = make(map[int64]map[int64]struct{})
	f.tokenMembershipsByTeam = make(map[int64]map[int64]struct{})
	for id, entry := range f.tokenMemberships {
		pairMap, ok := f.tokenMembershipsByPair[entry.TokenID]
		if !ok {
			pairMap = make(map[int64]int64)
			f.tokenMembershipsByPair[entry.TokenID] = pairMap
		}
		pairMap[entry.TeamID] = id
		tokenSet, ok := f.tokenMembershipsByToken[entry.TokenID]
		if !ok {
			tokenSet = make(map[int64]struct{})
			f.tokenMembershipsByToken[entry.TokenID] = tokenSet
		}
		tokenSet[id] = struct{}{}
		teamSet, ok := f.tokenMembershipsByTeam[entry.TeamID]
		if !ok {
			teamSet = make(map[int64]struct{})
			f.tokenMembershipsByTeam[entry.TeamID] = teamSet
		}
		teamSet[id] = struct{}{}
	}
	f.mu.Unlock()

	f.logger.Info().
		Int("node_count", len(snapshot.Nodes)).
		Int("file_count", len(snapshot.Files)).
		Int("token_count", len(snapshot.Tokens)).
		Int("organization_count", len(restoredOrgs)).
		Int("team_count", len(restoredTeams)).
		Int("role_count", len(restoredRoles)).
		Int("measurement_permission_count", len(restoredMPerms)).
		Int("token_membership_count", len(restoredMemberships)).
		Str("primary_writer", snapshot.PrimaryWriterID).
		Str("active_compactor", snapshot.ActiveCompactorID).
		Msg("FSM restored from snapshot")

	return nil
}

// GetNode returns a copy of the node info for the given ID.
func (f *ClusterFSM) GetNode(nodeID string) (*NodeInfo, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	node, exists := f.nodes[nodeID]
	if !exists {
		return nil, false
	}
	nodeCopy := *node
	return &nodeCopy, true
}

// GetAllNodes returns copies of all nodes.
func (f *ClusterFSM) GetAllNodes() []*NodeInfo {
	f.mu.RLock()
	defer f.mu.RUnlock()
	nodes := make([]*NodeInfo, 0, len(f.nodes))
	for _, node := range f.nodes {
		nodeCopy := *node
		nodes = append(nodes, &nodeCopy)
	}
	return nodes
}

// GetNodesByRole returns copies of all nodes with the given role.
func (f *ClusterFSM) GetNodesByRole(role string) []*NodeInfo {
	f.mu.RLock()
	defer f.mu.RUnlock()
	var nodes []*NodeInfo
	for _, node := range f.nodes {
		if node.Role == role {
			nodeCopy := *node
			nodes = append(nodes, &nodeCopy)
		}
	}
	return nodes
}

// NodeCount returns the number of nodes in the FSM.
func (f *ClusterFSM) NodeCount() int {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return len(f.nodes)
}

// TotalCores returns the sum of CoreCount across all nodes in the FSM.
func (f *ClusterFSM) TotalCores() int {
	f.mu.RLock()
	defer f.mu.RUnlock()
	total := 0
	for _, node := range f.nodes {
		total += node.CoreCount
	}
	return total
}

// GetFile returns a copy of the file entry for the given path, or nil if not found.
func (f *ClusterFSM) GetFile(path string) (*FileEntry, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	entry, exists := f.files[path]
	if !exists {
		return nil, false
	}
	entryCopy := *entry
	return &entryCopy, true
}

// GetAllFiles returns copies of all files in the manifest.
//
// WARNING: This is O(N) in the number of files and allocates a full copy.
// For large clusters with millions of files this can cause memory pressure
// and GC pauses. It is suitable for small-to-medium clusters (< ~100k files)
// and administrative/debugging use. A future phase will add a paginated or
// streaming variant for the API handler when scale demands it — tracked as
// a follow-up to Phase 1.
func (f *ClusterFSM) GetAllFiles() []*FileEntry {
	f.mu.RLock()
	defer f.mu.RUnlock()
	files := make([]*FileEntry, 0, len(f.files))
	for _, file := range f.files {
		fileCopy := *file
		files = append(files, &fileCopy)
	}
	return files
}

// GetFilesByDatabase returns copies of all files for the given database.
// Uses the filesByDB secondary index for O(k) lookup where k is the number
// of files for the database — no longer scans the full manifest.
func (f *ClusterFSM) GetFilesByDatabase(database string) []*FileEntry {
	f.mu.RLock()
	defer f.mu.RUnlock()
	idx, ok := f.filesByDB[database]
	if !ok {
		return nil
	}
	files := make([]*FileEntry, 0, len(idx))
	for path := range idx {
		if entry, exists := f.files[path]; exists {
			entryCopy := *entry
			files = append(files, &entryCopy)
		}
	}
	return files
}

// FileCount returns the number of files in the manifest.
func (f *ClusterFSM) FileCount() int {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return len(f.files)
}

// fsmSnapshot implements raft.FSMSnapshot.
type fsmSnapshot struct {
	nodes             map[string]*NodeInfo
	primaryWriterID   string
	activeCompactorID string
	files             map[string]*FileEntry
	tokens            map[int64]*TokenEntry

	// Phase A.1: RBAC primary maps (no secondary indices persisted).
	organizations          map[int64]*OrganizationEntry
	teams                  map[int64]*TeamEntry
	roles                  map[int64]*RoleEntry
	measurementPermissions map[int64]*MeasurementPermissionEntry
	tokenMemberships       map[int64]*TokenMembershipEntry
}

// Persist writes the snapshot to the given sink.
func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	snapshot := FSMSnapshot{
		Nodes:                  s.nodes,
		PrimaryWriterID:        s.primaryWriterID,
		ActiveCompactorID:      s.activeCompactorID,
		Files:                  s.files,
		Tokens:                 s.tokens,
		Organizations:          s.organizations,
		Teams:                  s.teams,
		Roles:                  s.roles,
		MeasurementPermissions: s.measurementPermissions,
		TokenMemberships:       s.tokenMemberships,
	}

	data, err := json.Marshal(snapshot)
	if err != nil {
		sink.Cancel()
		return fmt.Errorf("failed to marshal snapshot: %w", err)
	}

	if _, err := sink.Write(data); err != nil {
		sink.Cancel()
		return fmt.Errorf("failed to write snapshot: %w", err)
	}

	return sink.Close()
}

// Release is called when the snapshot is no longer needed.
func (s *fsmSnapshot) Release() {}
