package auth

// Phase A.1: Cluster Auth Convergence (RBAC).
//
// This file is the RBAC equivalent of cluster_apply.go: it owns the auth-
// side mirror types (Cluster<X>Entry), the wire shapes used to JSON-marshal
// payloads to the proposer (cluster<X>EntryWire), and the 13 Apply<X>
// methods on *RBACManager that the FSM apply path calls back into to
// materialise cluster-wide state into local SQLite.
//
// All five Cluster<X>Entry shapes mirror cluster/raft's *Entry types
// field-for-field; the duplication is necessary because the auth package
// cannot import cluster/raft (would cycle via cluster → auth). The wire
// types and the cluster/raft entry types share JSON tags so a marshal
// from one decodes losslessly into the other; the wire-format pin in
// fsm_proposal_pin_test.go covers the contract.

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// -----------------------------------------------------------------------------
// Proposer plumbing (RBAC-side mirror of AuthManager.SetRaftProposer /
// getProposer / proposeCommand in cluster_apply.go).
// -----------------------------------------------------------------------------

// SetRaftProposer wires the cluster's Raft FSM into RBACManager. Calling
// this with a non-nil proposer flips every subsequent Create/Update/Delete
// /Add/Remove on RBAC entities from direct-SQLite to Raft-proposed. Nil
// flips back to direct-SQLite. Safe to call concurrently with active
// writes — the proposer is read under proposerMu.
func (rm *RBACManager) SetRaftProposer(p RaftProposer) {
	rm.proposerMu.Lock()
	rm.proposer = p
	rm.proposerMu.Unlock()
}

// getProposer returns the current proposer (or nil) under the read lock.
func (rm *RBACManager) getProposer() RaftProposer {
	rm.proposerMu.RLock()
	defer rm.proposerMu.RUnlock()
	return rm.proposer
}

// proposeRBACCommand marshals a payload and submits via the configured
// proposer. Returns nil on a successful FSM apply on the leader; the
// onXxx callback has already fired by the time this returns. Same shape
// as AuthManager.proposeCommand.
func (rm *RBACManager) proposeRBACCommand(ctx context.Context, cmdType uint8, payload interface{}) error {
	p := rm.getProposer()
	if p == nil {
		return fmt.Errorf("rbac: proposer not configured (cluster mode required)")
	}
	bytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("rbac: marshal payload: %w", err)
	}
	if err := p.Propose(ctx, cmdType, bytes, proposeTimeout); err != nil {
		return err
	}
	return nil
}

// -----------------------------------------------------------------------------
// Mirror types — field-for-field copies of the cluster/raft *Entry shapes.
// -----------------------------------------------------------------------------

// ClusterOrganizationEntry mirrors cluster/raft.OrganizationEntry. Used by
// the FSM apply callback to hand the materialise method an entry without
// importing cluster/raft.
type ClusterOrganizationEntry struct {
	ID                int64
	Name              string
	Description       string
	CreatedAtUnixNano int64
	UpdatedAtUnixNano int64
	Enabled           bool
	LSN               uint64
}

// ClusterTeamEntry mirrors cluster/raft.TeamEntry.
type ClusterTeamEntry struct {
	ID                int64
	OrganizationID    int64
	Name              string
	Description       string
	CreatedAtUnixNano int64
	UpdatedAtUnixNano int64
	Enabled           bool
	LSN               uint64
}

// ClusterRoleEntry mirrors cluster/raft.RoleEntry.
type ClusterRoleEntry struct {
	ID                int64
	TeamID            int64
	DatabasePattern   string
	Permissions       string
	CreatedAtUnixNano int64
	LSN               uint64
}

// ClusterMeasurementPermissionEntry mirrors
// cluster/raft.MeasurementPermissionEntry.
type ClusterMeasurementPermissionEntry struct {
	ID                 int64
	RoleID             int64
	MeasurementPattern string
	Permissions        string
	CreatedAtUnixNano  int64
	LSN                uint64
}

// ClusterTokenMembershipEntry mirrors cluster/raft.TokenMembershipEntry.
type ClusterTokenMembershipEntry struct {
	ID                int64
	TokenID           int64
	TeamID            int64
	CreatedAtUnixNano int64
	LSN               uint64
}

// -----------------------------------------------------------------------------
// Wire shapes — JSON-tagged forms used to marshal payloads to the proposer.
// Tags MUST match the cluster/raft *Entry tags exactly.
// -----------------------------------------------------------------------------

type clusterOrganizationEntryWire struct {
	ID                int64  `json:"id"`
	Name              string `json:"name"`
	Description       string `json:"description,omitempty"`
	CreatedAtUnixNano int64  `json:"created_at_unix_nano"`
	UpdatedAtUnixNano int64  `json:"updated_at_unix_nano"`
	Enabled           bool   `json:"enabled"`
	LSN               uint64 `json:"lsn,omitempty"`
}

type clusterTeamEntryWire struct {
	ID                int64  `json:"id"`
	OrganizationID    int64  `json:"organization_id"`
	Name              string `json:"name"`
	Description       string `json:"description,omitempty"`
	CreatedAtUnixNano int64  `json:"created_at_unix_nano"`
	UpdatedAtUnixNano int64  `json:"updated_at_unix_nano"`
	Enabled           bool   `json:"enabled"`
	LSN               uint64 `json:"lsn,omitempty"`
}

type clusterRoleEntryWire struct {
	ID                int64  `json:"id"`
	TeamID            int64  `json:"team_id"`
	DatabasePattern   string `json:"database_pattern"`
	Permissions       string `json:"permissions"`
	CreatedAtUnixNano int64  `json:"created_at_unix_nano"`
	LSN               uint64 `json:"lsn,omitempty"`
}

type clusterMeasurementPermissionEntryWire struct {
	ID                 int64  `json:"id"`
	RoleID             int64  `json:"role_id"`
	MeasurementPattern string `json:"measurement_pattern"`
	Permissions        string `json:"permissions"`
	CreatedAtUnixNano  int64  `json:"created_at_unix_nano"`
	LSN                uint64 `json:"lsn,omitempty"`
}

type clusterTokenMembershipEntryWire struct {
	ID                int64  `json:"id"`
	TokenID           int64  `json:"token_id"`
	TeamID            int64  `json:"team_id"`
	CreatedAtUnixNano int64  `json:"created_at_unix_nano"`
	LSN               uint64 `json:"lsn,omitempty"`
}

// -----------------------------------------------------------------------------
// Proposer-side payload wrappers — mirror the cluster/raft *Payload shapes.
// -----------------------------------------------------------------------------

type createOrganizationPayloadWire struct {
	Organization clusterOrganizationEntryWire `json:"organization"`
}

type updateOrganizationPayloadWire struct {
	ID                int64    `json:"id"`
	Name              string   `json:"name,omitempty"`
	Description       string   `json:"description,omitempty"`
	Enabled           bool     `json:"enabled,omitempty"`
	UpdatedAtUnixNano int64    `json:"updated_at_unix_nano"`
	ChangedFields     []string `json:"changed_fields"`
}

type deleteOrganizationPayloadWire struct {
	ID int64 `json:"id"`
}

type createTeamPayloadWire struct {
	Team clusterTeamEntryWire `json:"team"`
}

type updateTeamPayloadWire struct {
	ID                int64    `json:"id"`
	Name              string   `json:"name,omitempty"`
	Description       string   `json:"description,omitempty"`
	Enabled           bool     `json:"enabled,omitempty"`
	UpdatedAtUnixNano int64    `json:"updated_at_unix_nano"`
	ChangedFields     []string `json:"changed_fields"`
}

type deleteTeamPayloadWire struct {
	ID int64 `json:"id"`
}

type createRolePayloadWire struct {
	Role clusterRoleEntryWire `json:"role"`
}

type updateRolePayloadWire struct {
	ID              int64    `json:"id"`
	DatabasePattern string   `json:"database_pattern,omitempty"`
	Permissions     string   `json:"permissions,omitempty"`
	ChangedFields   []string `json:"changed_fields"`
}

type deleteRolePayloadWire struct {
	ID int64 `json:"id"`
}

type createMeasurementPermissionPayloadWire struct {
	MeasurementPermission clusterMeasurementPermissionEntryWire `json:"measurement_permission"`
}

type deleteMeasurementPermissionPayloadWire struct {
	ID int64 `json:"id"`
}

type addTokenToTeamPayloadWire struct {
	Membership clusterTokenMembershipEntryWire `json:"membership"`
}

type removeTokenFromTeamPayloadWire struct {
	TokenID int64 `json:"token_id"`
	TeamID  int64 `json:"team_id"`
}

// -----------------------------------------------------------------------------
// Apply* methods — called by the FSM apply callback on every node, including
// the proposer's own apply (the apply path is the single source of truth
// for the SQLite write).
//
// Each method:
//   - INSERT/UPDATE/DELETE the corresponding rbac_* row
//   - Check RowsAffected to detect cluster<->local divergence
//   - Invalidate the relevant cache (InvalidateAllCache or
//     InvalidateTokenCache(tokenID))
//
// Idempotency vs. divergence: log replay can re-apply the same command,
// so identical-content writes are a no-op. But a row with different
// content at the same ID surfaces loudly (mirrors ApplyCreateToken's
// SELECT-first pattern in cluster_apply.go).
// -----------------------------------------------------------------------------

// ApplyCreateOrganization materialises a CreateOrganization Raft apply into
// local SQLite. Two-stage divergence detection:
//
//  1. SELECT by id: identical name → idempotent (log replay); different
//     name at the same id → loud error (cluster<->local divergence at
//     the surrogate-ID layer).
//  2. After INSERT, if SQLite reports UNIQUE-on-name collision: a
//     pre-A.1 AUTOINCREMENT row with the same name already exists at a
//     DIFFERENT id. This is the legitimate upgrade path — the seed
//     proposes a Create for every pre-existing local row, and the
//     proposer's local materialise must accept that the row is already
//     present under its old surrogate ID. Treat as idempotent and log
//     at Info so the operator can see the seed working.
//
// Phase A.1: Cluster Auth Convergence (RBAC).
func (rm *RBACManager) ApplyCreateOrganization(entry ClusterOrganizationEntry) error {
	if entry.ID == 0 {
		return fmt.Errorf("ApplyCreateOrganization: id required")
	}
	createdAt := time.Unix(0, entry.CreatedAtUnixNano).UTC()
	updatedAt := time.Unix(0, entry.UpdatedAtUnixNano).UTC()
	enabled := 0
	if entry.Enabled {
		enabled = 1
	}

	var existingName string
	queryErr := rm.db.QueryRow(
		`SELECT name FROM rbac_organizations WHERE id = ?`,
		entry.ID,
	).Scan(&existingName)
	switch {
	case queryErr == nil:
		if existingName == entry.Name {
			rm.InvalidateAllCache()
			return nil
		}
		return fmt.Errorf("ApplyCreateOrganization: id %d already exists locally with different name (cluster<->local divergence; see upgrade notes)", entry.ID)
	case errors.Is(queryErr, sql.ErrNoRows):
		// fall through to INSERT
	default:
		return fmt.Errorf("ApplyCreateOrganization: pre-insert lookup: %w", queryErr)
	}

	if _, insertErr := rm.db.Exec(`
		INSERT INTO rbac_organizations
			(id, name, description, created_at, updated_at, enabled)
		VALUES (?, ?, ?, ?, ?, ?)
	`, entry.ID, entry.Name, entry.Description, createdAt, updatedAt, enabled); insertErr != nil {
		if strings.Contains(insertErr.Error(), "UNIQUE constraint failed") {
			// Upgrade-seed path: the local SQLite already holds a row
			// with this name under a different (pre-A.1 AUTOINCREMENT)
			// id. The cluster FSM is the source of truth, so the local
			// row's id is the divergent one. If we left the local row
			// in place, every subsequent CreateTeam under the new
			// FSM-stamped org id would fail the local FK check
			// (rbac_teams.organization_id REFERENCES rbac_organizations
			// ON DELETE CASCADE doesn't help — the parent id doesn't
			// exist locally yet). Realign by deleting the old row +
			// inserting under the new id, in a single transaction. The
			// DELETE cascades to any local pre-A.1 child rows
			// (teams, roles, measurement_permissions, token_memberships)
			// — operators have already been told via the seed Warn that
			// child rows are not auto-replicated and must be re-issued
			// post-upgrade, so cascading them out is the intended
			// outcome. Gemini PR #458 review.
			tx, txErr := rm.db.Begin()
			if txErr != nil {
				return fmt.Errorf("ApplyCreateOrganization: upgrade-seed tx begin: %w", txErr)
			}
			// Best-effort rollback on any error path; explicit Commit
			// below succeeds the happy path.
			defer func() { _ = tx.Rollback() }()

			if _, delErr := tx.Exec(`DELETE FROM rbac_organizations WHERE name = ?`, entry.Name); delErr != nil {
				return fmt.Errorf("ApplyCreateOrganization: upgrade-seed delete old row: %w", delErr)
			}
			if _, insErr := tx.Exec(`
				INSERT INTO rbac_organizations
					(id, name, description, created_at, updated_at, enabled)
				VALUES (?, ?, ?, ?, ?, ?)
			`, entry.ID, entry.Name, entry.Description, createdAt, updatedAt, enabled); insErr != nil {
				return fmt.Errorf("ApplyCreateOrganization: upgrade-seed re-insert: %w", insErr)
			}
			if commitErr := tx.Commit(); commitErr != nil {
				return fmt.Errorf("ApplyCreateOrganization: upgrade-seed commit: %w", commitErr)
			}

			rm.logger.Info().
				Int64("cluster_id", entry.ID).
				Str("name", entry.Name).
				Msg("ApplyCreateOrganization: upgrade-seed re-aligned local row to FSM-stamped id (any pre-A.1 child rows cascade-deleted; re-issue via API)")
			rm.InvalidateAllCache()
			return nil
		}
		return fmt.Errorf("ApplyCreateOrganization: insert: %w", insertErr)
	}
	rm.InvalidateAllCache()
	return nil
}

// ApplyUpdateOrganization materialises an UpdateOrganization apply. The FSM
// has already merged the partial-update semantics — every field on entry
// holds the post-update authoritative value. UPDATE unconditionally and
// detect divergence via RowsAffected==0.
func (rm *RBACManager) ApplyUpdateOrganization(entry ClusterOrganizationEntry) error {
	if entry.ID == 0 {
		return fmt.Errorf("ApplyUpdateOrganization: id required")
	}
	updatedAt := time.Unix(0, entry.UpdatedAtUnixNano).UTC()
	enabled := 0
	if entry.Enabled {
		enabled = 1
	}
	result, err := rm.db.Exec(`
		UPDATE rbac_organizations
		SET name = ?, description = ?, updated_at = ?, enabled = ?
		WHERE id = ?
	`, entry.Name, entry.Description, updatedAt, enabled, entry.ID)
	if err != nil {
		return fmt.Errorf("ApplyUpdateOrganization: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("ApplyUpdateOrganization: rows affected: %w", err)
	}
	if rows == 0 {
		// Divergence: cluster FSM holds this row but our local SQLite
		// doesn't. Don't invalidate cache on this branch — the cache
		// has no entry for this row anyway, and invalidating would
		// force re-reads of stale state. The Error log + returned
		// error surface the divergence to main.go's callback wrapper
		// so the operator sees it. Phase A.1: design clarified after
		// internal review.
		rm.logger.Error().
			Int64("organization_id", entry.ID).
			Str("name", entry.Name).
			Msg("ApplyUpdateOrganization: local SQLite row missing — cluster<->local divergence")
		return fmt.Errorf("ApplyUpdateOrganization: id %d not present in local SQLite cache (cluster<->local divergence)", entry.ID)
	}
	rm.InvalidateAllCache()
	return nil
}

// ApplyDeleteOrganization materialises a DeleteOrganization apply. SQLite
// ON DELETE CASCADE handles teams/roles/measurement_permissions/
// token_memberships under the local FK. A missing row is logged at WARN
// (post-state matches) but does NOT return error — the FSM is the
// authoritative state and we don't want a missing local row to block
// Raft progress.
func (rm *RBACManager) ApplyDeleteOrganization(id int64) error {
	if id == 0 {
		return fmt.Errorf("ApplyDeleteOrganization: id required")
	}
	result, err := rm.db.Exec(`DELETE FROM rbac_organizations WHERE id = ?`, id)
	if err != nil {
		return fmt.Errorf("ApplyDeleteOrganization: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("ApplyDeleteOrganization: rows affected: %w", err)
	}
	if rows == 0 {
		rm.logger.Warn().
			Int64("organization_id", id).
			Msg("ApplyDeleteOrganization: local SQLite row missing (post-state matches; no-op)")
	}
	rm.InvalidateAllCache()
	return nil
}

// ApplyCreateTeam materialises a CreateTeam apply. SELECT-first divergence
// detection on the composite UNIQUE(organization_id, name) key.
func (rm *RBACManager) ApplyCreateTeam(entry ClusterTeamEntry) error {
	if entry.ID == 0 {
		return fmt.Errorf("ApplyCreateTeam: id required")
	}
	createdAt := time.Unix(0, entry.CreatedAtUnixNano).UTC()
	updatedAt := time.Unix(0, entry.UpdatedAtUnixNano).UTC()
	enabled := 0
	if entry.Enabled {
		enabled = 1
	}

	var existingOrg int64
	var existingName string
	queryErr := rm.db.QueryRow(
		`SELECT organization_id, name FROM rbac_teams WHERE id = ?`,
		entry.ID,
	).Scan(&existingOrg, &existingName)
	switch {
	case queryErr == nil:
		if existingOrg == entry.OrganizationID && existingName == entry.Name {
			rm.InvalidateAllCache()
			return nil
		}
		return fmt.Errorf("ApplyCreateTeam: id %d already exists locally with different content (cluster<->local divergence)", entry.ID)
	case errors.Is(queryErr, sql.ErrNoRows):
	default:
		return fmt.Errorf("ApplyCreateTeam: pre-insert lookup: %w", queryErr)
	}

	if _, insertErr := rm.db.Exec(`
		INSERT INTO rbac_teams
			(id, organization_id, name, description, created_at, updated_at, enabled)
		VALUES (?, ?, ?, ?, ?, ?, ?)
	`, entry.ID, entry.OrganizationID, entry.Name, entry.Description, createdAt, updatedAt, enabled); insertErr != nil {
		return fmt.Errorf("ApplyCreateTeam: insert: %w", insertErr)
	}
	rm.InvalidateAllCache()
	return nil
}

// ApplyUpdateTeam materialises an UpdateTeam apply.
func (rm *RBACManager) ApplyUpdateTeam(entry ClusterTeamEntry) error {
	if entry.ID == 0 {
		return fmt.Errorf("ApplyUpdateTeam: id required")
	}
	updatedAt := time.Unix(0, entry.UpdatedAtUnixNano).UTC()
	enabled := 0
	if entry.Enabled {
		enabled = 1
	}
	result, err := rm.db.Exec(`
		UPDATE rbac_teams
		SET name = ?, description = ?, updated_at = ?, enabled = ?
		WHERE id = ?
	`, entry.Name, entry.Description, updatedAt, enabled, entry.ID)
	if err != nil {
		return fmt.Errorf("ApplyUpdateTeam: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("ApplyUpdateTeam: rows affected: %w", err)
	}
	if rows == 0 {
		// Divergence path: do not invalidate cache (no entry to bust)
		// — see ApplyUpdateOrganization comment above for rationale.
		rm.logger.Error().
			Int64("team_id", entry.ID).
			Str("name", entry.Name).
			Msg("ApplyUpdateTeam: local SQLite row missing — cluster<->local divergence")
		return fmt.Errorf("ApplyUpdateTeam: id %d not present in local SQLite cache (cluster<->local divergence)", entry.ID)
	}
	rm.InvalidateAllCache()
	return nil
}

// ApplyDeleteTeam materialises a DeleteTeam apply.
func (rm *RBACManager) ApplyDeleteTeam(id int64) error {
	if id == 0 {
		return fmt.Errorf("ApplyDeleteTeam: id required")
	}
	result, err := rm.db.Exec(`DELETE FROM rbac_teams WHERE id = ?`, id)
	if err != nil {
		return fmt.Errorf("ApplyDeleteTeam: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("ApplyDeleteTeam: rows affected: %w", err)
	}
	if rows == 0 {
		rm.logger.Warn().
			Int64("team_id", id).
			Msg("ApplyDeleteTeam: local SQLite row missing (post-state matches; no-op)")
	}
	rm.InvalidateAllCache()
	return nil
}

// ApplyCreateRole materialises a CreateRole apply. SELECT-first divergence
// on TeamID + DatabasePattern (no DB-side UNIQUE; just an upgrade-safety
// check).
func (rm *RBACManager) ApplyCreateRole(entry ClusterRoleEntry) error {
	if entry.ID == 0 {
		return fmt.Errorf("ApplyCreateRole: id required")
	}
	createdAt := time.Unix(0, entry.CreatedAtUnixNano).UTC()

	var existingTeam int64
	var existingPattern string
	queryErr := rm.db.QueryRow(
		`SELECT team_id, database_pattern FROM rbac_roles WHERE id = ?`,
		entry.ID,
	).Scan(&existingTeam, &existingPattern)
	switch {
	case queryErr == nil:
		if existingTeam == entry.TeamID && existingPattern == entry.DatabasePattern {
			rm.InvalidateAllCache()
			return nil
		}
		return fmt.Errorf("ApplyCreateRole: id %d already exists locally with different content (cluster<->local divergence)", entry.ID)
	case errors.Is(queryErr, sql.ErrNoRows):
	default:
		return fmt.Errorf("ApplyCreateRole: pre-insert lookup: %w", queryErr)
	}

	if _, insertErr := rm.db.Exec(`
		INSERT INTO rbac_roles
			(id, team_id, database_pattern, permissions, created_at)
		VALUES (?, ?, ?, ?, ?)
	`, entry.ID, entry.TeamID, entry.DatabasePattern, entry.Permissions, createdAt); insertErr != nil {
		return fmt.Errorf("ApplyCreateRole: insert: %w", insertErr)
	}
	rm.InvalidateAllCache()
	return nil
}

// ApplyUpdateRole materialises an UpdateRole apply.
func (rm *RBACManager) ApplyUpdateRole(entry ClusterRoleEntry) error {
	if entry.ID == 0 {
		return fmt.Errorf("ApplyUpdateRole: id required")
	}
	result, err := rm.db.Exec(`
		UPDATE rbac_roles
		SET database_pattern = ?, permissions = ?
		WHERE id = ?
	`, entry.DatabasePattern, entry.Permissions, entry.ID)
	if err != nil {
		return fmt.Errorf("ApplyUpdateRole: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("ApplyUpdateRole: rows affected: %w", err)
	}
	if rows == 0 {
		// Divergence path: do not invalidate cache (no entry to bust)
		// — see ApplyUpdateOrganization comment above for rationale.
		rm.logger.Error().
			Int64("role_id", entry.ID).
			Msg("ApplyUpdateRole: local SQLite row missing — cluster<->local divergence")
		return fmt.Errorf("ApplyUpdateRole: id %d not present in local SQLite cache (cluster<->local divergence)", entry.ID)
	}
	rm.InvalidateAllCache()
	return nil
}

// ApplyDeleteRole materialises a DeleteRole apply.
func (rm *RBACManager) ApplyDeleteRole(id int64) error {
	if id == 0 {
		return fmt.Errorf("ApplyDeleteRole: id required")
	}
	result, err := rm.db.Exec(`DELETE FROM rbac_roles WHERE id = ?`, id)
	if err != nil {
		return fmt.Errorf("ApplyDeleteRole: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("ApplyDeleteRole: rows affected: %w", err)
	}
	if rows == 0 {
		rm.logger.Warn().
			Int64("role_id", id).
			Msg("ApplyDeleteRole: local SQLite row missing (post-state matches; no-op)")
	}
	rm.InvalidateAllCache()
	return nil
}

// ApplyCreateMeasurementPermission materialises a
// CreateMeasurementPermission apply.
func (rm *RBACManager) ApplyCreateMeasurementPermission(entry ClusterMeasurementPermissionEntry) error {
	if entry.ID == 0 {
		return fmt.Errorf("ApplyCreateMeasurementPermission: id required")
	}
	createdAt := time.Unix(0, entry.CreatedAtUnixNano).UTC()

	var existingRole int64
	var existingPattern string
	queryErr := rm.db.QueryRow(
		`SELECT role_id, measurement_pattern FROM rbac_measurement_permissions WHERE id = ?`,
		entry.ID,
	).Scan(&existingRole, &existingPattern)
	switch {
	case queryErr == nil:
		if existingRole == entry.RoleID && existingPattern == entry.MeasurementPattern {
			rm.InvalidateAllCache()
			return nil
		}
		return fmt.Errorf("ApplyCreateMeasurementPermission: id %d already exists locally with different content (cluster<->local divergence)", entry.ID)
	case errors.Is(queryErr, sql.ErrNoRows):
	default:
		return fmt.Errorf("ApplyCreateMeasurementPermission: pre-insert lookup: %w", queryErr)
	}

	if _, insertErr := rm.db.Exec(`
		INSERT INTO rbac_measurement_permissions
			(id, role_id, measurement_pattern, permissions, created_at)
		VALUES (?, ?, ?, ?, ?)
	`, entry.ID, entry.RoleID, entry.MeasurementPattern, entry.Permissions, createdAt); insertErr != nil {
		return fmt.Errorf("ApplyCreateMeasurementPermission: insert: %w", insertErr)
	}
	rm.InvalidateAllCache()
	return nil
}

// ApplyDeleteMeasurementPermission materialises a
// DeleteMeasurementPermission apply.
func (rm *RBACManager) ApplyDeleteMeasurementPermission(id int64) error {
	if id == 0 {
		return fmt.Errorf("ApplyDeleteMeasurementPermission: id required")
	}
	result, err := rm.db.Exec(`DELETE FROM rbac_measurement_permissions WHERE id = ?`, id)
	if err != nil {
		return fmt.Errorf("ApplyDeleteMeasurementPermission: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("ApplyDeleteMeasurementPermission: rows affected: %w", err)
	}
	if rows == 0 {
		rm.logger.Warn().
			Int64("measurement_permission_id", id).
			Msg("ApplyDeleteMeasurementPermission: local SQLite row missing (post-state matches; no-op)")
	}
	rm.InvalidateAllCache()
	return nil
}

// ApplyAddTokenToTeam materialises an AddTokenToTeam apply. Invalidates
// only this token's cache (per-token granularity, no need to bust
// permCache for other tokens).
func (rm *RBACManager) ApplyAddTokenToTeam(entry ClusterTokenMembershipEntry) error {
	if entry.ID == 0 {
		return fmt.Errorf("ApplyAddTokenToTeam: id required")
	}
	createdAt := time.Unix(0, entry.CreatedAtUnixNano).UTC()

	var existingTokenID, existingTeamID int64
	queryErr := rm.db.QueryRow(
		`SELECT token_id, team_id FROM rbac_token_memberships WHERE id = ?`,
		entry.ID,
	).Scan(&existingTokenID, &existingTeamID)
	switch {
	case queryErr == nil:
		if existingTokenID == entry.TokenID && existingTeamID == entry.TeamID {
			rm.InvalidateTokenCache(entry.TokenID)
			return nil
		}
		return fmt.Errorf("ApplyAddTokenToTeam: id %d already exists locally with different content (cluster<->local divergence)", entry.ID)
	case errors.Is(queryErr, sql.ErrNoRows):
	default:
		return fmt.Errorf("ApplyAddTokenToTeam: pre-insert lookup: %w", queryErr)
	}

	if _, insertErr := rm.db.Exec(`
		INSERT INTO rbac_token_memberships
			(id, token_id, team_id, created_at)
		VALUES (?, ?, ?, ?)
	`, entry.ID, entry.TokenID, entry.TeamID, createdAt); insertErr != nil {
		// A SQLite UNIQUE(token_id, team_id) collision means a pre-A.1
		// AUTOINCREMENT row had the same pair under a different ID —
		// classic upgrade-divergence shape. Treat as a divergence error.
		if strings.Contains(insertErr.Error(), "UNIQUE constraint failed") {
			return fmt.Errorf("ApplyAddTokenToTeam: (token %d, team %d) already exists locally with different id (cluster<->local divergence)", entry.TokenID, entry.TeamID)
		}
		return fmt.Errorf("ApplyAddTokenToTeam: insert: %w", insertErr)
	}
	rm.InvalidateTokenCache(entry.TokenID)
	return nil
}

// ApplyRemoveTokenFromTeam materialises a RemoveTokenFromTeam apply.
// Identified by the (token_id, team_id) pair.
func (rm *RBACManager) ApplyRemoveTokenFromTeam(tokenID, teamID int64) error {
	if tokenID == 0 || teamID == 0 {
		return fmt.Errorf("ApplyRemoveTokenFromTeam: token_id and team_id required")
	}
	result, err := rm.db.Exec(`
		DELETE FROM rbac_token_memberships WHERE token_id = ? AND team_id = ?
	`, tokenID, teamID)
	if err != nil {
		return fmt.Errorf("ApplyRemoveTokenFromTeam: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("ApplyRemoveTokenFromTeam: rows affected: %w", err)
	}
	if rows == 0 {
		rm.logger.Warn().
			Int64("token_id", tokenID).
			Int64("team_id", teamID).
			Msg("ApplyRemoveTokenFromTeam: local SQLite row missing (post-state matches; no-op)")
	}
	rm.InvalidateTokenCache(tokenID)
	return nil
}
