package raft

import (
	"encoding/json"
	"fmt"

	"github.com/basekick-labs/arc/internal/metrics"
)

// Phase A.1: Cluster Auth Convergence (RBAC).
//
// This file holds the FSM-side entry and payload struct definitions for the
// five RBAC tables — organizations, teams, roles, measurement_permissions,
// and token_memberships. The apply logic (mutation, cascade, secondary-index
// maintenance) lives alongside in this file; the dispatch arms are wired
// into Apply()'s switch statement in fsm.go.
//
// Design choices, locked in the Phase A.1 plan:
//
//   - IDs are stamped from the Raft log index at create time, identical to
//     TokenEntry.ID. This gives every node the same ID regardless of which
//     node proposed the command, with no extra coordination.
//   - Timestamps are int64 unix-nano fields (not time.Time) so JSON encoding
//     is deterministic across nodes and across Go runtime versions. The
//     proposer stamps CreatedAt/UpdatedAt before marshalling; apply does
//     NOT regenerate them.
//   - Delete commands cascade in-apply across the affected maps under a
//     single Raft log entry. SQLite-side cascade fires naturally via the
//     ON DELETE CASCADE foreign keys in the local materialise; the FSM
//     cascade mirrors that at the in-memory layer. Two independent
//     mechanisms, each correct in its own layer.
//   - No plaintext secrets transit the FSM for RBAC. The shape concern
//     from Phase A (bcrypt hash + prefix only, never plaintext) does not
//     apply here.

// -----------------------------------------------------------------------------
// Entry types — the in-memory record shapes held by ClusterFSM.
// -----------------------------------------------------------------------------

// OrganizationEntry mirrors rbac_organizations row. UNIQUE(name) is enforced
// applier-side via the organizationsByName index in ClusterFSM.
type OrganizationEntry struct {
	ID                int64  `json:"id"`                    // Raft log index at create time (deterministic across nodes)
	Name              string `json:"name"`                  // UNIQUE; validated against nameValidationRegex
	Description       string `json:"description,omitempty"` // Optional free-text
	CreatedAtUnixNano int64  `json:"created_at_unix_nano"`  // Proposer-set; deterministic across log replay
	UpdatedAtUnixNano int64  `json:"updated_at_unix_nano"`  // Bumped by apply on Update; equals CreatedAt on Create
	Enabled           bool   `json:"enabled"`               // false = soft-disable; row still present
	LSN               uint64 `json:"lsn"`                   // Raft log index at last mutation
}

// TeamEntry mirrors rbac_teams row. UNIQUE(organization_id, name) is enforced
// applier-side via the teamsByOrg nested index (orgID → name → teamID).
type TeamEntry struct {
	ID                int64  `json:"id"`
	OrganizationID    int64  `json:"organization_id"`
	Name              string `json:"name"`
	Description       string `json:"description,omitempty"`
	CreatedAtUnixNano int64  `json:"created_at_unix_nano"`
	UpdatedAtUnixNano int64  `json:"updated_at_unix_nano"`
	Enabled           bool   `json:"enabled"`
	LSN               uint64 `json:"lsn"`
}

// RoleEntry mirrors rbac_roles row. No UNIQUE constraint on database_pattern
// — multiple roles per team may target the same pattern by design.
// Permissions is encoded as a comma-separated string for parity with the
// SQLite TEXT column; the materialiser splits/joins at the SQLite boundary.
type RoleEntry struct {
	ID                int64  `json:"id"`
	TeamID            int64  `json:"team_id"`
	DatabasePattern   string `json:"database_pattern"`     // validated against patternValidationRegex
	Permissions       string `json:"permissions"`          // CSV: "read,write,delete,admin"
	CreatedAtUnixNano int64  `json:"created_at_unix_nano"` // Roles are immutable except via UpdateRole
	LSN               uint64 `json:"lsn"`
}

// MeasurementPermissionEntry mirrors rbac_measurement_permissions row. No
// UNIQUE constraint; multiple patterns per role are allowed and additive.
type MeasurementPermissionEntry struct {
	ID                 int64  `json:"id"`
	RoleID             int64  `json:"role_id"`
	MeasurementPattern string `json:"measurement_pattern"`  // validated against patternValidationRegex
	Permissions        string `json:"permissions"`          // CSV
	CreatedAtUnixNano  int64  `json:"created_at_unix_nano"` // Leaf entity; no Update command
	LSN                uint64 `json:"lsn"`
}

// TokenMembershipEntry mirrors rbac_token_memberships row. UNIQUE(token_id,
// team_id) is enforced applier-side via the tokenMembershipsByPair nested
// index. The surrogate ID is retained for parity with the API surface
// (handlers return TokenMembership.ID to callers) even though the (token_id,
// team_id) pair is the natural business key.
type TokenMembershipEntry struct {
	ID                int64  `json:"id"`
	TokenID           int64  `json:"token_id"`
	TeamID            int64  `json:"team_id"`
	CreatedAtUnixNano int64  `json:"created_at_unix_nano"`
	LSN               uint64 `json:"lsn"`
}

// -----------------------------------------------------------------------------
// Payloads — the wire-format shapes carried inside Command.Payload.
// -----------------------------------------------------------------------------

// CreateOrganizationPayload is the payload for CommandCreateOrganization.
// The proposer sets CreatedAtUnixNano; apply stamps ID from the Raft log
// index, copies CreatedAt to UpdatedAt, and forces Enabled=true.
type CreateOrganizationPayload struct {
	Organization OrganizationEntry `json:"organization"`
}

// UpdateOrganizationPayload is the payload for CommandUpdateOrganization.
// Empty Name is treated as "no change" by apply because UNIQUE(name) is a
// real constraint. Empty Description is treated literally (clearing the
// field). ChangedFields disambiguates "blank because empty" from "blank
// because not changing". Mirrors Phase A's UpdateTokenPayload pattern.
type UpdateOrganizationPayload struct {
	ID                int64    `json:"id"`
	Name              string   `json:"name,omitempty"`
	Description       string   `json:"description,omitempty"`
	Enabled           bool     `json:"enabled,omitempty"`
	UpdatedAtUnixNano int64    `json:"updated_at_unix_nano"`
	ChangedFields     []string `json:"changed_fields"` // subset of {"name","description","enabled"}
}

// DeleteOrganizationPayload is the payload for CommandDeleteOrganization.
// Apply cascades to teams, roles, measurement_permissions, and
// token_memberships under this single log entry.
type DeleteOrganizationPayload struct {
	ID int64 `json:"id"`
}

// CreateTeamPayload is the payload for CommandCreateTeam. The proposer sets
// OrganizationID + Name + Description + CreatedAt. Apply stamps ID,
// validates the parent organization exists, enforces UNIQUE(org_id, name).
type CreateTeamPayload struct {
	Team TeamEntry `json:"team"`
}

// UpdateTeamPayload is the payload for CommandUpdateTeam. Same ChangedFields
// pattern as UpdateOrganizationPayload.
type UpdateTeamPayload struct {
	ID                int64    `json:"id"`
	Name              string   `json:"name,omitempty"`
	Description       string   `json:"description,omitempty"`
	Enabled           bool     `json:"enabled,omitempty"`
	UpdatedAtUnixNano int64    `json:"updated_at_unix_nano"`
	ChangedFields     []string `json:"changed_fields"` // subset of {"name","description","enabled"}
}

// DeleteTeamPayload is the payload for CommandDeleteTeam. Apply cascades to
// roles, measurement_permissions, and token_memberships under this log entry.
type DeleteTeamPayload struct {
	ID int64 `json:"id"`
}

// CreateRolePayload is the payload for CommandCreateRole. The proposer sets
// TeamID + DatabasePattern + Permissions + CreatedAt. Apply stamps ID and
// validates the parent team exists.
type CreateRolePayload struct {
	Role RoleEntry `json:"role"`
}

// UpdateRolePayload is the payload for CommandUpdateRole. Roles have a much
// smaller mutable surface than orgs/teams — database_pattern and
// permissions only.
type UpdateRolePayload struct {
	ID              int64    `json:"id"`
	DatabasePattern string   `json:"database_pattern,omitempty"`
	Permissions     string   `json:"permissions,omitempty"`
	ChangedFields   []string `json:"changed_fields"` // subset of {"database_pattern","permissions"}
}

// DeleteRolePayload is the payload for CommandDeleteRole. Apply cascades to
// measurement_permissions under this log entry.
type DeleteRolePayload struct {
	ID int64 `json:"id"`
}

// CreateMeasurementPermissionPayload is the payload for
// CommandCreateMeasurementPermission. Leaf entity — no further cascade on
// delete. The proposer sets RoleID + MeasurementPattern + Permissions +
// CreatedAt.
type CreateMeasurementPermissionPayload struct {
	MeasurementPermission MeasurementPermissionEntry `json:"measurement_permission"`
}

// DeleteMeasurementPermissionPayload is the payload for
// CommandDeleteMeasurementPermission. Leaf delete; no cascade.
type DeleteMeasurementPermissionPayload struct {
	ID int64 `json:"id"`
}

// AddTokenToTeamPayload is the payload for CommandAddTokenToTeam. The
// proposer sets TokenID + TeamID + CreatedAt. Apply stamps ID, validates
// the team exists, and enforces UNIQUE(token_id, team_id).
type AddTokenToTeamPayload struct {
	Membership TokenMembershipEntry `json:"membership"`
}

// RemoveTokenFromTeamPayload is the payload for CommandRemoveTokenFromTeam.
// Identifies the membership by its (token_id, team_id) pair — the natural
// business key — rather than by surrogate ID, because API handlers
// operate on the pair.
type RemoveTokenFromTeamPayload struct {
	TokenID int64 `json:"token_id"`
	TeamID  int64 `json:"team_id"`
}

// -----------------------------------------------------------------------------
// Validation helpers — applier-side checks that run on every node.
//
// Same design principle as validateTokenEntry / ValidateManifestPath: a
// malformed entry must NOT land in the FSM state regardless of which node
// proposed the command. Validators are pure functions; they take an entry
// (or its constituent fields) and return nil on success or a wrapped error
// describing the rejection. Callers pair the error with rejectRBAC() so the
// counter, log line, and security envelope all stay consistent across the
// 13 apply functions.
// -----------------------------------------------------------------------------

// rbacNameMaxLen bounds the length of organization/team Name fields. The
// SQLite schema doesn't enforce a length but a 256-byte cap matches the
// regex used by RBACManager (nameValidationRegex: max 64 chars) with
// substantial slack to avoid a DOS via a multi-megabyte name string in
// the Raft log. Same posture as validateTokenEntry's 256 cap.
const rbacNameMaxLen = 256

// rbacPatternMaxLen bounds database/measurement patterns. Real patterns
// are short ("production", "metrics_*", "*") but the FSM accepts anything
// the proposer-side regex allowed. 256 bytes is generous.
const rbacPatternMaxLen = 256

// rbacDescriptionMaxLen bounds free-text description fields.
const rbacDescriptionMaxLen = 1024

// validateOrganizationEntry checks that an OrganizationEntry is shaped for
// safe insertion into the cluster FSM. Validates only the fields the FSM
// owns; the SQLite-side cache enforces its own UNIQUE constraint as a
// belt-and-braces.
func validateOrganizationEntry(entry *OrganizationEntry) error {
	if entry == nil {
		return fmt.Errorf("organization entry is nil")
	}
	if entry.Name == "" {
		return fmt.Errorf("organization name is required")
	}
	if len(entry.Name) > rbacNameMaxLen {
		return fmt.Errorf("organization name too long: %d > %d", len(entry.Name), rbacNameMaxLen)
	}
	if len(entry.Description) > rbacDescriptionMaxLen {
		return fmt.Errorf("organization description too long: %d > %d", len(entry.Description), rbacDescriptionMaxLen)
	}
	return nil
}

// validateTeamEntry checks a TeamEntry. OrganizationID parent-existence is
// checked on the apply path (under the FSM lock) — not here — because
// pure-function validators must not touch f.organizations.
func validateTeamEntry(entry *TeamEntry) error {
	if entry == nil {
		return fmt.Errorf("team entry is nil")
	}
	if entry.OrganizationID <= 0 {
		return fmt.Errorf("team organization_id is required")
	}
	if entry.Name == "" {
		return fmt.Errorf("team name is required")
	}
	if len(entry.Name) > rbacNameMaxLen {
		return fmt.Errorf("team name too long: %d > %d", len(entry.Name), rbacNameMaxLen)
	}
	if len(entry.Description) > rbacDescriptionMaxLen {
		return fmt.Errorf("team description too long: %d > %d", len(entry.Description), rbacDescriptionMaxLen)
	}
	return nil
}

// validateRoleEntry checks a RoleEntry. TeamID parent-existence is
// checked on the apply path under the FSM lock.
func validateRoleEntry(entry *RoleEntry) error {
	if entry == nil {
		return fmt.Errorf("role entry is nil")
	}
	if entry.TeamID <= 0 {
		return fmt.Errorf("role team_id is required")
	}
	if entry.DatabasePattern == "" {
		return fmt.Errorf("role database_pattern is required")
	}
	if len(entry.DatabasePattern) > rbacPatternMaxLen {
		return fmt.Errorf("role database_pattern too long: %d > %d", len(entry.DatabasePattern), rbacPatternMaxLen)
	}
	if err := validatePermissionString(entry.Permissions); err != nil {
		return err
	}
	return nil
}

// validateMeasurementPermissionEntry checks a MeasurementPermissionEntry.
// RoleID parent-existence is checked on the apply path.
func validateMeasurementPermissionEntry(entry *MeasurementPermissionEntry) error {
	if entry == nil {
		return fmt.Errorf("measurement_permission entry is nil")
	}
	if entry.RoleID <= 0 {
		return fmt.Errorf("measurement_permission role_id is required")
	}
	if entry.MeasurementPattern == "" {
		return fmt.Errorf("measurement_permission measurement_pattern is required")
	}
	if len(entry.MeasurementPattern) > rbacPatternMaxLen {
		return fmt.Errorf("measurement_permission measurement_pattern too long: %d > %d", len(entry.MeasurementPattern), rbacPatternMaxLen)
	}
	if err := validatePermissionString(entry.Permissions); err != nil {
		return err
	}
	return nil
}

// validateTokenMembershipEntry checks a TokenMembershipEntry. TokenID
// existence in f.tokens AND TeamID existence in f.teams are checked on
// the apply path under the FSM lock (they're FSM-state queries, not pure
// validation).
func validateTokenMembershipEntry(entry *TokenMembershipEntry) error {
	if entry == nil {
		return fmt.Errorf("token_membership entry is nil")
	}
	if entry.TokenID <= 0 {
		return fmt.Errorf("token_membership token_id is required")
	}
	if entry.TeamID <= 0 {
		return fmt.Errorf("token_membership team_id is required")
	}
	return nil
}

// rejectRBAC centralises FSM-side rejection of a malformed RBAC command.
// Same shape as rejectToken: log at Error, bump the (single) RBAC reject
// counter + the process-wide Prometheus counter, return a wrapped
// validation error. The security property — entry does NOT land in any
// RBAC map — holds because callers return BEFORE any state mutation.
//
// op is one of {"create_organization", "update_organization",
// "delete_organization", ... "remove_token_from_team"} for log
// disambiguation across all 13 command types. id is the affected entity's
// ID (0 if not yet stamped). Phase A.1: Cluster Auth Convergence (RBAC).
func (f *ClusterFSM) rejectRBAC(op string, id int64, logIndex uint64, err error) error {
	f.incRejectedRBAC()
	f.logger.Error().
		Err(err).
		Str("op", op).
		Int64("entity_id", id).
		Uint64("log_index", logIndex).
		Msg("RBAC command validation failed — entry refused, not added to FSM state")
	return fmt.Errorf("%s: %w", op, err)
}

// incRejectedRBAC increments the local rejection counter and the
// process-wide Prometheus counter. Mirrors incRejectedTokens.
func (f *ClusterFSM) incRejectedRBAC() {
	f.rejectedRBAC.Add(1)
	metrics.Get().IncClusterRBACRejected()
}

// -----------------------------------------------------------------------------
// Organization apply handlers (Create, Update, Delete with 4-level cascade).
// -----------------------------------------------------------------------------

// applyCreateOrganization stamps an OrganizationEntry into FSM state.
// Enforces UNIQUE(name) via the organizationsByName index. Mirrors
// applyCreateToken (fsm.go).
func (f *ClusterFSM) applyCreateOrganization(payload []byte, logIndex uint64) interface{} {
	var p CreateOrganizationPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("failed to unmarshal create organization payload: %w", err)
	}
	if err := validateOrganizationEntry(&p.Organization); err != nil {
		return f.rejectRBAC("create_organization", p.Organization.ID, logIndex, err)
	}
	if p.Organization.CreatedAtUnixNano == 0 {
		// Same reasoning as applyCreateToken: the proposer must set
		// CreatedAt before appending to Raft. Apply MUST NOT fill it in
		// from time.Now() because that would be non-deterministic during
		// log replay.
		return f.rejectRBAC("create_organization", p.Organization.ID, logIndex, fmt.Errorf("created_at is required"))
	}

	entry := p.Organization
	entry.ID = int64(logIndex)
	entry.LSN = logIndex
	if entry.UpdatedAtUnixNano == 0 {
		entry.UpdatedAtUnixNano = entry.CreatedAtUnixNano
	}
	entry.Enabled = true

	f.mu.Lock()
	if _, exists := f.organizationsByName[entry.Name]; exists {
		f.mu.Unlock()
		return f.rejectRBAC("create_organization", entry.ID, logIndex, fmt.Errorf("organization name %q already exists", entry.Name))
	}
	f.organizations[entry.ID] = &entry
	f.organizationsByName[entry.Name] = entry.ID
	callback := f.onOrganizationCreated
	f.mu.Unlock()

	metrics.Get().IncClusterRBACApplyCreateOrganization()
	f.logger.Debug().
		Int64("organization_id", entry.ID).
		Str("name", entry.Name).
		Uint64("lsn", entry.LSN).
		Msg("Organization created in cluster RBAC state")

	if callback != nil {
		entryCopy := entry
		callback(&entryCopy)
	}
	return nil
}

// applyUpdateOrganization mutates name/description/enabled on an existing
// organization. ChangedFields disambiguates "blank because empty" from
// "blank because not changing". UNIQUE(name) is re-enforced if name is
// changing. Mirrors applyUpdateToken.
func (f *ClusterFSM) applyUpdateOrganization(payload []byte, logIndex uint64) interface{} {
	var p UpdateOrganizationPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("failed to unmarshal update organization payload: %w", err)
	}
	if p.ID == 0 {
		return f.rejectRBAC("update_organization", 0, logIndex, fmt.Errorf("organization id is required"))
	}
	// Pre-validate any name we're about to write.
	for _, field := range p.ChangedFields {
		if field == "name" {
			if p.Name == "" {
				return f.rejectRBAC("update_organization", p.ID, logIndex, fmt.Errorf("organization name cannot be cleared"))
			}
			if len(p.Name) > rbacNameMaxLen {
				return f.rejectRBAC("update_organization", p.ID, logIndex, fmt.Errorf("organization name too long: %d > %d", len(p.Name), rbacNameMaxLen))
			}
		}
		if field == "description" && len(p.Description) > rbacDescriptionMaxLen {
			return f.rejectRBAC("update_organization", p.ID, logIndex, fmt.Errorf("organization description too long: %d > %d", len(p.Description), rbacDescriptionMaxLen))
		}
	}

	f.mu.Lock()
	existing, ok := f.organizations[p.ID]
	if !ok {
		f.mu.Unlock()
		return f.rejectRBAC("update_organization", p.ID, logIndex, fmt.Errorf("organization %d not found", p.ID))
	}
	// Make a copy to avoid mutating the existing pointer until we know all
	// validation passes. Then atomically swap.
	updated := *existing
	for _, field := range p.ChangedFields {
		switch field {
		case "name":
			if p.Name != existing.Name {
				if _, exists := f.organizationsByName[p.Name]; exists {
					f.mu.Unlock()
					return f.rejectRBAC("update_organization", p.ID, logIndex, fmt.Errorf("organization name %q already exists", p.Name))
				}
				// Rewire the name index: remove old, add new.
				delete(f.organizationsByName, existing.Name)
				f.organizationsByName[p.Name] = p.ID
				updated.Name = p.Name
			}
		case "description":
			updated.Description = p.Description
		case "enabled":
			updated.Enabled = p.Enabled
		}
	}
	if p.UpdatedAtUnixNano != 0 {
		updated.UpdatedAtUnixNano = p.UpdatedAtUnixNano
	}
	updated.LSN = logIndex
	f.organizations[p.ID] = &updated
	callback := f.onOrganizationUpdated
	f.mu.Unlock()

	metrics.Get().IncClusterRBACApplyUpdateOrganization()
	f.logger.Debug().
		Int64("organization_id", p.ID).
		Strs("changed_fields", p.ChangedFields).
		Uint64("lsn", updated.LSN).
		Msg("Organization updated in cluster RBAC state")

	if callback != nil {
		entryCopy := updated
		callback(&entryCopy)
	}
	return nil
}

// applyDeleteOrganization removes an organization from FSM state AND
// cascades the delete across teams → roles → measurement_permissions →
// token_memberships under a single Raft log entry. Mirrors SQLite ON
// DELETE CASCADE semantics at the FSM layer (SQLite-side cascade fires
// naturally via FK in the local materialise).
//
// The cascade enumerates affected children via the traversal indices —
// O(degree of subtree) walks, not O(total entries). All map mutations
// happen under f.mu.Lock(); concurrent CreateTeam under the same org is
// serialised by the lock and sees post-cascade state.
func (f *ClusterFSM) applyDeleteOrganization(payload []byte, logIndex uint64) interface{} {
	var p DeleteOrganizationPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("failed to unmarshal delete organization payload: %w", err)
	}
	if p.ID == 0 {
		return f.rejectRBAC("delete_organization", 0, logIndex, fmt.Errorf("organization id is required"))
	}

	f.mu.Lock()
	existing, ok := f.organizations[p.ID]
	if !ok {
		// Idempotent on re-apply: deleting a non-existent org is a no-op
		// rather than a hard error. Same posture as applyDeleteToken at
		// the SQLite "RowsAffected == 0" boundary.
		f.mu.Unlock()
		f.logger.Warn().
			Int64("organization_id", p.ID).
			Uint64("log_index", logIndex).
			Msg("DeleteOrganization: organization not found in FSM state (post-state matches; no-op)")
		metrics.Get().IncClusterRBACApplyDeleteOrganization()
		return nil
	}

	// Cascade. Enumerate descendants under the lock, then delete.
	orgName := existing.Name
	cascadedTeams, cascadedRoles, cascadedMPerms, cascadedMemberships := f.cascadeDeleteOrgLocked(p.ID, logIndex)

	delete(f.organizations, p.ID)
	delete(f.organizationsByName, orgName)
	// Clear the orgID entry in teamsByOrg (it may still have a stale-empty
	// map otherwise after cascadeDeleteOrgLocked drained it).
	delete(f.teamsByOrg, p.ID)

	callback := f.onOrganizationDeleted
	f.mu.Unlock()

	metrics.Get().IncClusterRBACApplyDeleteOrganization()
	f.logger.Info().
		Int64("organization_id", p.ID).
		Str("name", orgName).
		Int("cascaded_teams", cascadedTeams).
		Int("cascaded_roles", cascadedRoles).
		Int("cascaded_measurement_permissions", cascadedMPerms).
		Int("cascaded_memberships", cascadedMemberships).
		Uint64("log_index", logIndex).
		Msg("Organization deleted in cluster RBAC state (with cascade)")

	if callback != nil {
		callback(p.ID)
	}
	return nil
}

// cascadeDeleteOrgLocked removes every descendant of orgID from FSM state.
// Caller MUST hold f.mu.Lock(). Returns counts for logging. Does NOT
// remove the org itself — caller does that.
func (f *ClusterFSM) cascadeDeleteOrgLocked(orgID int64, _ uint64) (teams, roles, mperms, memberships int) {
	teamMap := f.teamsByOrg[orgID]
	for _, teamID := range teamMap {
		// For each team under this org, cascade into roles + memberships.
		r, m, mb := f.cascadeDeleteTeamLocked(teamID)
		roles += r
		mperms += m
		memberships += mb
		delete(f.teams, teamID)
		teams++
	}
	return
}

// cascadeDeleteTeamLocked removes every descendant of teamID. Caller MUST
// hold f.mu.Lock(). Removes roles + their measurement_permissions +
// memberships. Does NOT remove the team itself.
func (f *ClusterFSM) cascadeDeleteTeamLocked(teamID int64) (roles, mperms, memberships int) {
	// Roles → measurement_permissions.
	roleSet := f.rolesByTeam[teamID]
	for roleID := range roleSet {
		m := f.cascadeDeleteRoleLocked(roleID)
		mperms += m
		delete(f.roles, roleID)
		roles++
	}
	delete(f.rolesByTeam, teamID)

	// Memberships under this team.
	memberSet := f.tokenMembershipsByTeam[teamID]
	for membershipID := range memberSet {
		mem, ok := f.tokenMemberships[membershipID]
		if !ok {
			continue
		}
		// Remove from byPair (tokenID → teamID → membershipID).
		if pairMap, ok := f.tokenMembershipsByPair[mem.TokenID]; ok {
			delete(pairMap, mem.TeamID)
			if len(pairMap) == 0 {
				delete(f.tokenMembershipsByPair, mem.TokenID)
			}
		}
		// Remove from byToken.
		if tokenSet, ok := f.tokenMembershipsByToken[mem.TokenID]; ok {
			delete(tokenSet, membershipID)
			if len(tokenSet) == 0 {
				delete(f.tokenMembershipsByToken, mem.TokenID)
			}
		}
		delete(f.tokenMemberships, membershipID)
		memberships++
	}
	delete(f.tokenMembershipsByTeam, teamID)
	return
}

// cascadeDeleteRoleLocked removes every measurement_permission under
// roleID. Caller MUST hold f.mu.Lock().
func (f *ClusterFSM) cascadeDeleteRoleLocked(roleID int64) (mperms int) {
	mpermSet := f.measurementPermsByRole[roleID]
	for mpermID := range mpermSet {
		delete(f.measurementPermissions, mpermID)
		mperms++
	}
	delete(f.measurementPermsByRole, roleID)
	return
}

// -----------------------------------------------------------------------------
// Team apply handlers (Create, Update, Delete with 3-level cascade).
// -----------------------------------------------------------------------------

// applyCreateTeam stamps a TeamEntry into FSM state. Validates the parent
// organization exists and enforces UNIQUE(organization_id, name) via the
// teamsByOrg nested index.
func (f *ClusterFSM) applyCreateTeam(payload []byte, logIndex uint64) interface{} {
	var p CreateTeamPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("failed to unmarshal create team payload: %w", err)
	}
	if err := validateTeamEntry(&p.Team); err != nil {
		return f.rejectRBAC("create_team", p.Team.ID, logIndex, err)
	}
	if p.Team.CreatedAtUnixNano == 0 {
		return f.rejectRBAC("create_team", p.Team.ID, logIndex, fmt.Errorf("created_at is required"))
	}

	entry := p.Team
	entry.ID = int64(logIndex)
	entry.LSN = logIndex
	if entry.UpdatedAtUnixNano == 0 {
		entry.UpdatedAtUnixNano = entry.CreatedAtUnixNano
	}
	entry.Enabled = true

	f.mu.Lock()
	// Parent organization must exist.
	if _, ok := f.organizations[entry.OrganizationID]; !ok {
		f.mu.Unlock()
		return f.rejectRBAC("create_team", entry.ID, logIndex, fmt.Errorf("organization %d not found", entry.OrganizationID))
	}
	// UNIQUE(org_id, name) check via teamsByOrg. Defensive
	// init-on-demand: both the missing-outer-key case AND a stored-nil
	// inner-map case (which shouldn't happen but is symmetric with the
	// guard in applyUpdateTeam — see internal review round 2 / Gemini
	// round 2 G9). Writing to a nil map panics in Go, so the guard is
	// belt-and-braces against any future code path that could leave the
	// nested map in an inconsistent state.
	orgTeams, ok := f.teamsByOrg[entry.OrganizationID]
	if !ok || orgTeams == nil {
		orgTeams = make(map[string]int64)
		f.teamsByOrg[entry.OrganizationID] = orgTeams
	}
	if _, exists := orgTeams[entry.Name]; exists {
		f.mu.Unlock()
		return f.rejectRBAC("create_team", entry.ID, logIndex, fmt.Errorf("team name %q already exists in organization %d", entry.Name, entry.OrganizationID))
	}
	f.teams[entry.ID] = &entry
	orgTeams[entry.Name] = entry.ID
	callback := f.onTeamCreated
	f.mu.Unlock()

	metrics.Get().IncClusterRBACApplyCreateTeam()
	f.logger.Debug().
		Int64("team_id", entry.ID).
		Int64("organization_id", entry.OrganizationID).
		Str("name", entry.Name).
		Uint64("lsn", entry.LSN).
		Msg("Team created in cluster RBAC state")

	if callback != nil {
		entryCopy := entry
		callback(&entryCopy)
	}
	return nil
}

// applyUpdateTeam mutates name/description/enabled on an existing team.
// UNIQUE(org_id, name) is re-enforced if name is changing.
func (f *ClusterFSM) applyUpdateTeam(payload []byte, logIndex uint64) interface{} {
	var p UpdateTeamPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("failed to unmarshal update team payload: %w", err)
	}
	if p.ID == 0 {
		return f.rejectRBAC("update_team", 0, logIndex, fmt.Errorf("team id is required"))
	}
	for _, field := range p.ChangedFields {
		if field == "name" {
			if p.Name == "" {
				return f.rejectRBAC("update_team", p.ID, logIndex, fmt.Errorf("team name cannot be cleared"))
			}
			if len(p.Name) > rbacNameMaxLen {
				return f.rejectRBAC("update_team", p.ID, logIndex, fmt.Errorf("team name too long: %d > %d", len(p.Name), rbacNameMaxLen))
			}
		}
		if field == "description" && len(p.Description) > rbacDescriptionMaxLen {
			return f.rejectRBAC("update_team", p.ID, logIndex, fmt.Errorf("team description too long: %d > %d", len(p.Description), rbacDescriptionMaxLen))
		}
	}

	f.mu.Lock()
	existing, ok := f.teams[p.ID]
	if !ok {
		f.mu.Unlock()
		return f.rejectRBAC("update_team", p.ID, logIndex, fmt.Errorf("team %d not found", p.ID))
	}
	updated := *existing
	for _, field := range p.ChangedFields {
		switch field {
		case "name":
			if p.Name != existing.Name {
				// Defensive: applyCreateTeam always initialises
				// teamsByOrg[orgID] when it stores a team, so this
				// should always be non-nil — but a corrupted snapshot
				// restore or a buggy future code path could leave the
				// nested map missing. Initialise on-demand so the write
				// at the bottom of this block cannot panic on a nil
				// map. Gemini PR #458 round 2.
				orgTeams, ok := f.teamsByOrg[existing.OrganizationID]
				if !ok || orgTeams == nil {
					orgTeams = make(map[string]int64)
					f.teamsByOrg[existing.OrganizationID] = orgTeams
				}
				if _, exists := orgTeams[p.Name]; exists {
					f.mu.Unlock()
					return f.rejectRBAC("update_team", p.ID, logIndex, fmt.Errorf("team name %q already exists in organization %d", p.Name, existing.OrganizationID))
				}
				delete(orgTeams, existing.Name)
				orgTeams[p.Name] = p.ID
				updated.Name = p.Name
			}
		case "description":
			updated.Description = p.Description
		case "enabled":
			updated.Enabled = p.Enabled
		}
	}
	if p.UpdatedAtUnixNano != 0 {
		updated.UpdatedAtUnixNano = p.UpdatedAtUnixNano
	}
	updated.LSN = logIndex
	f.teams[p.ID] = &updated
	callback := f.onTeamUpdated
	f.mu.Unlock()

	metrics.Get().IncClusterRBACApplyUpdateTeam()
	f.logger.Debug().
		Int64("team_id", p.ID).
		Strs("changed_fields", p.ChangedFields).
		Uint64("lsn", updated.LSN).
		Msg("Team updated in cluster RBAC state")

	if callback != nil {
		entryCopy := updated
		callback(&entryCopy)
	}
	return nil
}

// applyDeleteTeam removes a team and cascades to roles +
// measurement_permissions + memberships. SQLite ON DELETE CASCADE fires
// naturally in the local materialise.
func (f *ClusterFSM) applyDeleteTeam(payload []byte, logIndex uint64) interface{} {
	var p DeleteTeamPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("failed to unmarshal delete team payload: %w", err)
	}
	if p.ID == 0 {
		return f.rejectRBAC("delete_team", 0, logIndex, fmt.Errorf("team id is required"))
	}

	f.mu.Lock()
	existing, ok := f.teams[p.ID]
	if !ok {
		f.mu.Unlock()
		f.logger.Warn().
			Int64("team_id", p.ID).
			Uint64("log_index", logIndex).
			Msg("DeleteTeam: team not found in FSM state (post-state matches; no-op)")
		metrics.Get().IncClusterRBACApplyDeleteTeam()
		return nil
	}
	teamName := existing.Name
	orgID := existing.OrganizationID

	cascadedRoles, cascadedMPerms, cascadedMemberships := f.cascadeDeleteTeamLocked(p.ID)
	delete(f.teams, p.ID)
	if orgTeams, ok := f.teamsByOrg[orgID]; ok {
		delete(orgTeams, teamName)
		if len(orgTeams) == 0 {
			delete(f.teamsByOrg, orgID)
		}
	}

	callback := f.onTeamDeleted
	f.mu.Unlock()

	metrics.Get().IncClusterRBACApplyDeleteTeam()
	f.logger.Info().
		Int64("team_id", p.ID).
		Int64("organization_id", orgID).
		Str("name", teamName).
		Int("cascaded_roles", cascadedRoles).
		Int("cascaded_measurement_permissions", cascadedMPerms).
		Int("cascaded_memberships", cascadedMemberships).
		Uint64("log_index", logIndex).
		Msg("Team deleted in cluster RBAC state (with cascade)")

	if callback != nil {
		callback(p.ID)
	}
	return nil
}

// -----------------------------------------------------------------------------
// Role apply handlers (Create, Update, Delete with 1-level cascade).
// -----------------------------------------------------------------------------

// applyCreateRole stamps a RoleEntry into FSM state. Validates the parent
// team exists. No UNIQUE constraint on (team_id, database_pattern) — same
// pattern across multiple roles is allowed by design.
func (f *ClusterFSM) applyCreateRole(payload []byte, logIndex uint64) interface{} {
	var p CreateRolePayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("failed to unmarshal create role payload: %w", err)
	}
	if err := validateRoleEntry(&p.Role); err != nil {
		return f.rejectRBAC("create_role", p.Role.ID, logIndex, err)
	}
	if p.Role.CreatedAtUnixNano == 0 {
		return f.rejectRBAC("create_role", p.Role.ID, logIndex, fmt.Errorf("created_at is required"))
	}

	entry := p.Role
	entry.ID = int64(logIndex)
	entry.LSN = logIndex

	f.mu.Lock()
	if _, ok := f.teams[entry.TeamID]; !ok {
		f.mu.Unlock()
		return f.rejectRBAC("create_role", entry.ID, logIndex, fmt.Errorf("team %d not found", entry.TeamID))
	}
	// Defensive init-on-demand (symmetric with the other Create* apply
	// functions — see comment in applyCreateTeam). Internal review round 2.
	teamRoles, ok := f.rolesByTeam[entry.TeamID]
	if !ok || teamRoles == nil {
		teamRoles = make(map[int64]struct{})
		f.rolesByTeam[entry.TeamID] = teamRoles
	}
	f.roles[entry.ID] = &entry
	teamRoles[entry.ID] = struct{}{}
	callback := f.onRoleCreated
	f.mu.Unlock()

	metrics.Get().IncClusterRBACApplyCreateRole()
	f.logger.Debug().
		Int64("role_id", entry.ID).
		Int64("team_id", entry.TeamID).
		Str("database_pattern", entry.DatabasePattern).
		Str("permissions", entry.Permissions).
		Uint64("lsn", entry.LSN).
		Msg("Role created in cluster RBAC state")

	if callback != nil {
		entryCopy := entry
		callback(&entryCopy)
	}
	return nil
}

// applyUpdateRole mutates database_pattern and/or permissions on an
// existing role.
func (f *ClusterFSM) applyUpdateRole(payload []byte, logIndex uint64) interface{} {
	var p UpdateRolePayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("failed to unmarshal update role payload: %w", err)
	}
	if p.ID == 0 {
		return f.rejectRBAC("update_role", 0, logIndex, fmt.Errorf("role id is required"))
	}
	for _, field := range p.ChangedFields {
		switch field {
		case "database_pattern":
			if p.DatabasePattern == "" {
				return f.rejectRBAC("update_role", p.ID, logIndex, fmt.Errorf("role database_pattern cannot be cleared"))
			}
			if len(p.DatabasePattern) > rbacPatternMaxLen {
				return f.rejectRBAC("update_role", p.ID, logIndex, fmt.Errorf("role database_pattern too long: %d > %d", len(p.DatabasePattern), rbacPatternMaxLen))
			}
		case "permissions":
			if err := validatePermissionString(p.Permissions); err != nil {
				return f.rejectRBAC("update_role", p.ID, logIndex, err)
			}
		}
	}

	f.mu.Lock()
	existing, ok := f.roles[p.ID]
	if !ok {
		f.mu.Unlock()
		return f.rejectRBAC("update_role", p.ID, logIndex, fmt.Errorf("role %d not found", p.ID))
	}
	updated := *existing
	for _, field := range p.ChangedFields {
		switch field {
		case "database_pattern":
			updated.DatabasePattern = p.DatabasePattern
		case "permissions":
			updated.Permissions = p.Permissions
		}
	}
	updated.LSN = logIndex
	f.roles[p.ID] = &updated
	callback := f.onRoleUpdated
	f.mu.Unlock()

	metrics.Get().IncClusterRBACApplyUpdateRole()
	f.logger.Debug().
		Int64("role_id", p.ID).
		Strs("changed_fields", p.ChangedFields).
		Uint64("lsn", updated.LSN).
		Msg("Role updated in cluster RBAC state")

	if callback != nil {
		entryCopy := updated
		callback(&entryCopy)
	}
	return nil
}

// applyDeleteRole removes a role and cascades to measurement_permissions.
func (f *ClusterFSM) applyDeleteRole(payload []byte, logIndex uint64) interface{} {
	var p DeleteRolePayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("failed to unmarshal delete role payload: %w", err)
	}
	if p.ID == 0 {
		return f.rejectRBAC("delete_role", 0, logIndex, fmt.Errorf("role id is required"))
	}

	f.mu.Lock()
	existing, ok := f.roles[p.ID]
	if !ok {
		f.mu.Unlock()
		f.logger.Warn().
			Int64("role_id", p.ID).
			Uint64("log_index", logIndex).
			Msg("DeleteRole: role not found in FSM state (post-state matches; no-op)")
		metrics.Get().IncClusterRBACApplyDeleteRole()
		return nil
	}
	teamID := existing.TeamID

	cascadedMPerms := f.cascadeDeleteRoleLocked(p.ID)
	delete(f.roles, p.ID)
	if teamRoles, ok := f.rolesByTeam[teamID]; ok {
		delete(teamRoles, p.ID)
		if len(teamRoles) == 0 {
			delete(f.rolesByTeam, teamID)
		}
	}

	callback := f.onRoleDeleted
	f.mu.Unlock()

	metrics.Get().IncClusterRBACApplyDeleteRole()
	f.logger.Info().
		Int64("role_id", p.ID).
		Int64("team_id", teamID).
		Int("cascaded_measurement_permissions", cascadedMPerms).
		Uint64("log_index", logIndex).
		Msg("Role deleted in cluster RBAC state (with cascade)")

	if callback != nil {
		callback(p.ID)
	}
	return nil
}

// -----------------------------------------------------------------------------
// MeasurementPermission apply handlers (Create, Delete; no Update, no cascade).
// -----------------------------------------------------------------------------

// applyCreateMeasurementPermission stamps a MeasurementPermissionEntry into
// FSM state. Validates parent role exists. Leaf entity — no further
// cascade considerations.
func (f *ClusterFSM) applyCreateMeasurementPermission(payload []byte, logIndex uint64) interface{} {
	var p CreateMeasurementPermissionPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("failed to unmarshal create measurement_permission payload: %w", err)
	}
	if err := validateMeasurementPermissionEntry(&p.MeasurementPermission); err != nil {
		return f.rejectRBAC("create_measurement_permission", p.MeasurementPermission.ID, logIndex, err)
	}
	if p.MeasurementPermission.CreatedAtUnixNano == 0 {
		return f.rejectRBAC("create_measurement_permission", p.MeasurementPermission.ID, logIndex, fmt.Errorf("created_at is required"))
	}

	entry := p.MeasurementPermission
	entry.ID = int64(logIndex)
	entry.LSN = logIndex

	f.mu.Lock()
	if _, ok := f.roles[entry.RoleID]; !ok {
		f.mu.Unlock()
		return f.rejectRBAC("create_measurement_permission", entry.ID, logIndex, fmt.Errorf("role %d not found", entry.RoleID))
	}
	// Defensive init-on-demand (symmetric — see applyCreateTeam comment).
	// Internal review round 2.
	roleMPerms, ok := f.measurementPermsByRole[entry.RoleID]
	if !ok || roleMPerms == nil {
		roleMPerms = make(map[int64]struct{})
		f.measurementPermsByRole[entry.RoleID] = roleMPerms
	}
	f.measurementPermissions[entry.ID] = &entry
	roleMPerms[entry.ID] = struct{}{}
	callback := f.onMeasurementPermissionCreated
	f.mu.Unlock()

	metrics.Get().IncClusterRBACApplyCreateMeasurementPermission()
	f.logger.Debug().
		Int64("measurement_permission_id", entry.ID).
		Int64("role_id", entry.RoleID).
		Str("measurement_pattern", entry.MeasurementPattern).
		Str("permissions", entry.Permissions).
		Uint64("lsn", entry.LSN).
		Msg("MeasurementPermission created in cluster RBAC state")

	if callback != nil {
		entryCopy := entry
		callback(&entryCopy)
	}
	return nil
}

// applyDeleteMeasurementPermission removes a measurement_permission. Leaf
// delete — no cascade.
func (f *ClusterFSM) applyDeleteMeasurementPermission(payload []byte, logIndex uint64) interface{} {
	var p DeleteMeasurementPermissionPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("failed to unmarshal delete measurement_permission payload: %w", err)
	}
	if p.ID == 0 {
		return f.rejectRBAC("delete_measurement_permission", 0, logIndex, fmt.Errorf("measurement_permission id is required"))
	}

	f.mu.Lock()
	existing, ok := f.measurementPermissions[p.ID]
	if !ok {
		f.mu.Unlock()
		f.logger.Warn().
			Int64("measurement_permission_id", p.ID).
			Uint64("log_index", logIndex).
			Msg("DeleteMeasurementPermission: not found in FSM state (post-state matches; no-op)")
		metrics.Get().IncClusterRBACApplyDeleteMeasurementPermission()
		return nil
	}
	roleID := existing.RoleID
	delete(f.measurementPermissions, p.ID)
	if roleMPerms, ok := f.measurementPermsByRole[roleID]; ok {
		delete(roleMPerms, p.ID)
		if len(roleMPerms) == 0 {
			delete(f.measurementPermsByRole, roleID)
		}
	}
	callback := f.onMeasurementPermissionDeleted
	f.mu.Unlock()

	metrics.Get().IncClusterRBACApplyDeleteMeasurementPermission()
	f.logger.Debug().
		Int64("measurement_permission_id", p.ID).
		Int64("role_id", roleID).
		Uint64("log_index", logIndex).
		Msg("MeasurementPermission deleted in cluster RBAC state")

	if callback != nil {
		callback(p.ID)
	}
	return nil
}

// -----------------------------------------------------------------------------
// TokenMembership apply handlers (Add, Remove; no Update, no cascade).
// -----------------------------------------------------------------------------

// applyAddTokenToTeam grants a token a membership in a team. Validates
// both the token and team exist (in FSM state), and enforces UNIQUE(token_id,
// team_id) via the tokenMembershipsByPair nested index.
func (f *ClusterFSM) applyAddTokenToTeam(payload []byte, logIndex uint64) interface{} {
	var p AddTokenToTeamPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("failed to unmarshal add_token_to_team payload: %w", err)
	}
	if err := validateTokenMembershipEntry(&p.Membership); err != nil {
		return f.rejectRBAC("add_token_to_team", p.Membership.ID, logIndex, err)
	}
	if p.Membership.CreatedAtUnixNano == 0 {
		return f.rejectRBAC("add_token_to_team", p.Membership.ID, logIndex, fmt.Errorf("created_at is required"))
	}

	entry := p.Membership
	entry.ID = int64(logIndex)
	entry.LSN = logIndex

	f.mu.Lock()
	if _, ok := f.tokens[entry.TokenID]; !ok {
		f.mu.Unlock()
		return f.rejectRBAC("add_token_to_team", entry.ID, logIndex, fmt.Errorf("token %d not found", entry.TokenID))
	}
	if _, ok := f.teams[entry.TeamID]; !ok {
		f.mu.Unlock()
		return f.rejectRBAC("add_token_to_team", entry.ID, logIndex, fmt.Errorf("team %d not found", entry.TeamID))
	}
	// UNIQUE(token_id, team_id) via tokenMembershipsByPair.
	// Defensive init-on-demand for all three nested maps (symmetric —
	// see applyCreateTeam comment). Internal review round 2.
	tokenPairs, ok := f.tokenMembershipsByPair[entry.TokenID]
	if !ok || tokenPairs == nil {
		tokenPairs = make(map[int64]int64)
		f.tokenMembershipsByPair[entry.TokenID] = tokenPairs
	}
	if _, exists := tokenPairs[entry.TeamID]; exists {
		f.mu.Unlock()
		return f.rejectRBAC("add_token_to_team", entry.ID, logIndex, fmt.Errorf("token %d is already a member of team %d", entry.TokenID, entry.TeamID))
	}
	f.tokenMemberships[entry.ID] = &entry
	tokenPairs[entry.TeamID] = entry.ID
	// byToken and byTeam traversal indices.
	tokenSet, ok := f.tokenMembershipsByToken[entry.TokenID]
	if !ok || tokenSet == nil {
		tokenSet = make(map[int64]struct{})
		f.tokenMembershipsByToken[entry.TokenID] = tokenSet
	}
	tokenSet[entry.ID] = struct{}{}
	teamSet, ok := f.tokenMembershipsByTeam[entry.TeamID]
	if !ok || teamSet == nil {
		teamSet = make(map[int64]struct{})
		f.tokenMembershipsByTeam[entry.TeamID] = teamSet
	}
	teamSet[entry.ID] = struct{}{}
	callback := f.onTokenMembershipAdded
	f.mu.Unlock()

	metrics.Get().IncClusterRBACApplyAddTokenToTeam()
	f.logger.Debug().
		Int64("membership_id", entry.ID).
		Int64("token_id", entry.TokenID).
		Int64("team_id", entry.TeamID).
		Uint64("lsn", entry.LSN).
		Msg("TokenMembership added in cluster RBAC state")

	if callback != nil {
		entryCopy := entry
		callback(&entryCopy)
	}
	return nil
}

// applyRemoveTokenFromTeam revokes a token's membership in a team.
// Identifies the membership by its (token_id, team_id) pair.
func (f *ClusterFSM) applyRemoveTokenFromTeam(payload []byte, logIndex uint64) interface{} {
	var p RemoveTokenFromTeamPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("failed to unmarshal remove_token_from_team payload: %w", err)
	}
	if p.TokenID == 0 || p.TeamID == 0 {
		return f.rejectRBAC("remove_token_from_team", 0, logIndex, fmt.Errorf("token_id and team_id are required"))
	}

	f.mu.Lock()
	tokenPairs, ok := f.tokenMembershipsByPair[p.TokenID]
	if !ok {
		f.mu.Unlock()
		f.logger.Warn().
			Int64("token_id", p.TokenID).
			Int64("team_id", p.TeamID).
			Uint64("log_index", logIndex).
			Msg("RemoveTokenFromTeam: token has no memberships (post-state matches; no-op)")
		metrics.Get().IncClusterRBACApplyRemoveTokenFromTeam()
		return nil
	}
	membershipID, ok := tokenPairs[p.TeamID]
	if !ok {
		f.mu.Unlock()
		f.logger.Warn().
			Int64("token_id", p.TokenID).
			Int64("team_id", p.TeamID).
			Uint64("log_index", logIndex).
			Msg("RemoveTokenFromTeam: membership not found (post-state matches; no-op)")
		metrics.Get().IncClusterRBACApplyRemoveTokenFromTeam()
		return nil
	}
	delete(tokenPairs, p.TeamID)
	if len(tokenPairs) == 0 {
		delete(f.tokenMembershipsByPair, p.TokenID)
	}
	if tokenSet, ok := f.tokenMembershipsByToken[p.TokenID]; ok {
		delete(tokenSet, membershipID)
		if len(tokenSet) == 0 {
			delete(f.tokenMembershipsByToken, p.TokenID)
		}
	}
	if teamSet, ok := f.tokenMembershipsByTeam[p.TeamID]; ok {
		delete(teamSet, membershipID)
		if len(teamSet) == 0 {
			delete(f.tokenMembershipsByTeam, p.TeamID)
		}
	}
	delete(f.tokenMemberships, membershipID)
	callback := f.onTokenMembershipRemoved
	f.mu.Unlock()

	metrics.Get().IncClusterRBACApplyRemoveTokenFromTeam()
	f.logger.Debug().
		Int64("membership_id", membershipID).
		Int64("token_id", p.TokenID).
		Int64("team_id", p.TeamID).
		Uint64("log_index", logIndex).
		Msg("TokenMembership removed from cluster RBAC state")

	if callback != nil {
		callback(p.TokenID, p.TeamID)
	}
	return nil
}
