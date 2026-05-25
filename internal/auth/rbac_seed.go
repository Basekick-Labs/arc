package auth

// Phase A.1: leader-only "seed FSM from local SQLite" upgrade path.
//
// Pre-26.06.1 builds wrote RBAC rows directly to per-node SQLite. After
// the operator upgrades to 26.06.1, the FSM in-memory state is empty
// (the cluster has never observed those rows) while the leader's local
// SQLite still holds them. Without an explicit seed, those rows would
// be stranded on the leader forever — they'd never replicate to
// followers and any post-upgrade Create with the same name would
// collide cluster-wide.
//
// What the seed handles:
//
//   - rbac_organizations — top-level entity, no FK dependency. Safe to
//     propose: the cluster FSM stamps a new log-index ID, the local
//     materialise accepts the existing pre-A.1 AUTOINCREMENT row as an
//     upgrade no-op via the UNIQUE(name)-collision-as-idempotency
//     branch in ApplyCreateOrganization. Follower nodes get the row at
//     the cluster's new ID; the leader keeps the row at its
//     pre-A.1 ID. The two IDs map to the same logical organization
//     because UNIQUE(name) is enforced cluster-wide.
//
// What the seed does NOT handle (deferred to operator re-issue):
//
//   - Teams, roles, measurement_permissions, token_memberships — these
//     reference parent entities by surrogate ID. The upgrade-divergence
//     between pre-A.1 local AUTOINCREMENT IDs and cluster-stamped
//     log-index IDs makes the FK references ambiguous (the seed reads
//     local team.organization_id=1, but the cluster FSM has the org at
//     id=42). Re-issuing these rows through the API post-upgrade is
//     the documented path. If local SQLite contains any teams / roles
//     / measurement_permissions / token_memberships at seed time, the
//     seed logs a Warn telling the operator to re-issue.
//
// Runs only on the Raft leader. Followers skip silently — their rows
// will arrive via Raft replication of the leader's apply log. The
// caller (cmd/arc/main.go) typically gates this on
// proposer.IsLeader() AND a successful WaitForLeader(30s).

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

// SeedRBACFromLocalSQLite proposes a Create<X> for every existing RBAC
// row in local SQLite. Idempotent — safe to re-run.
//
// Returns the first error encountered AFTER attempting every row;
// per-row errors are logged but don't abort the seed (one bad row
// shouldn't strand the others). Callers should treat a non-nil return
// as informational rather than fatal — the cluster can still operate
// correctly with a partial seed (the missing rows just won't replicate
// until an operator re-issues them).
func (rm *RBACManager) SeedRBACFromLocalSQLite(ctx context.Context) error {
	p := rm.getProposer()
	if p == nil {
		// Not in cluster mode — nothing to seed.
		return nil
	}
	if !p.IsLeader() {
		rm.logger.Debug().Msg("SeedRBACFromLocalSQLite: not leader; skipping")
		return nil
	}

	// Fast-exit when there is nothing local to seed. Avoids five Query
	// scans + their per-row allocations on the common "fresh cluster"
	// path (or "cluster that was already migrated"). Errors are logged
	// at Warn and treated as "state may exist" so we still attempt the
	// seed pass; better to do redundant work than to silently skip.
	if has, err := rm.HasLocalRBACState(); err != nil {
		rm.logger.Warn().Err(err).Msg("SeedRBACFromLocalSQLite: HasLocalRBACState failed; proceeding with full seed scan")
	} else if !has {
		rm.logger.Info().Msg("SeedRBACFromLocalSQLite: no local RBAC rows; nothing to seed")
		return nil
	}

	rm.logger.Info().Msg("SeedRBACFromLocalSQLite: starting (leader-only, idempotent)")

	var firstErr error
	var counts struct {
		orgs     int
		orgsSkip int
	}

	// 1. Organizations — top of the FK chain.
	type orgRow struct {
		id          int64
		name        string
		description string
		createdAt   time.Time
		updatedAt   time.Time
		enabled     bool
	}
	orgRows, err := func() ([]orgRow, error) {
		// Inner scope so defer rows.Close() fires before the outer
		// SeedRBACFromLocalSQLite continues to its COUNT(*) checks,
		// keeping the rows handle short-lived. Gemini PR #458 round 2.
		rows, qerr := rm.db.Query(`
			SELECT id, name, description, created_at, updated_at, enabled
			FROM rbac_organizations ORDER BY id
		`)
		if qerr != nil {
			return nil, fmt.Errorf("seed: query organizations: %w", qerr)
		}
		defer rows.Close()

		var out []orgRow
		for rows.Next() {
			var r orgRow
			if scanErr := rows.Scan(&r.id, &r.name, &r.description, &r.createdAt, &r.updatedAt, &r.enabled); scanErr != nil {
				return nil, fmt.Errorf("seed: scan organization: %w", scanErr)
			}
			out = append(out, r)
		}
		// Surface iteration errors (connection loss, mid-stream
		// corruption) loudly rather than silently proceeding with
		// partial data.
		if iterErr := rows.Err(); iterErr != nil {
			return nil, fmt.Errorf("seed: organizations iteration error: %w", iterErr)
		}
		return out, nil
	}()
	if err != nil {
		return err
	}
	for _, r := range orgRows {
		payload := createOrganizationPayloadWire{
			Organization: clusterOrganizationEntryWire{
				Name:              r.name,
				Description:       r.description,
				CreatedAtUnixNano: r.createdAt.UnixNano(),
				UpdatedAtUnixNano: r.updatedAt.UnixNano(),
				Enabled:           r.enabled,
			},
		}
		if err := rm.proposeRBACCommand(ctx, ProposalCommandCreateOrganization, payload); err != nil {
			if isDuplicateErr(err) {
				counts.orgsSkip++
				continue
			}
			rm.logger.Error().Err(err).Int64("organization_id", r.id).Str("name", r.name).Msg("seed: organization propose failed")
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		counts.orgs++
	}

	// 2-5. Teams, roles, measurement_permissions, token_memberships are
	// NOT auto-seeded — they reference parent entities by surrogate ID,
	// and the pre-A.1 AUTOINCREMENT IDs in local SQLite generally do
	// not match the cluster FSM's log-index-stamped IDs after the org
	// seed runs. Cross-node FK references would mismatch.
	//
	// Instead, count pre-existing rows in each child table and emit a
	// Warn telling the operator to re-issue them via the API. The
	// existing local rows remain readable on this node (the FK chain
	// in SQLite is intact); they just won't replicate to followers
	// until re-created. This trade-off is documented in the v26.06.1
	// release notes.
	var teamsPresent, rolesPresent, mpermsPresent, memPresent int
	// COUNT(*) errors here are surfaced at Warn rather than silently
	// swallowed — a future schema migration breaking one of these
	// tables should not be invisible at startup.
	if err := rm.db.QueryRow(`SELECT COUNT(*) FROM rbac_teams`).Scan(&teamsPresent); err != nil {
		rm.logger.Warn().Err(err).Msg("SeedRBACFromLocalSQLite: COUNT rbac_teams failed (treating as 0)")
	}
	if err := rm.db.QueryRow(`SELECT COUNT(*) FROM rbac_roles`).Scan(&rolesPresent); err != nil {
		rm.logger.Warn().Err(err).Msg("SeedRBACFromLocalSQLite: COUNT rbac_roles failed (treating as 0)")
	}
	if err := rm.db.QueryRow(`SELECT COUNT(*) FROM rbac_measurement_permissions`).Scan(&mpermsPresent); err != nil {
		rm.logger.Warn().Err(err).Msg("SeedRBACFromLocalSQLite: COUNT rbac_measurement_permissions failed (treating as 0)")
	}
	if err := rm.db.QueryRow(`SELECT COUNT(*) FROM rbac_token_memberships`).Scan(&memPresent); err != nil {
		rm.logger.Warn().Err(err).Msg("SeedRBACFromLocalSQLite: COUNT rbac_token_memberships failed (treating as 0)")
	}
	if teamsPresent+rolesPresent+mpermsPresent+memPresent > 0 {
		rm.logger.Warn().
			Int("teams_local", teamsPresent).
			Int("roles_local", rolesPresent).
			Int("measurement_permissions_local", mpermsPresent).
			Int("token_memberships_local", memPresent).
			Msg("SeedRBACFromLocalSQLite: child RBAC rows present in local SQLite are NOT auto-seeded (FK-ID rebase ambiguity); re-issue them via the API post-upgrade for cluster-wide replication")
	}

	rm.logger.Info().
		Int("organizations_seeded", counts.orgs).
		Int("organizations_skipped", counts.orgsSkip).
		Int("teams_local_unseeded", teamsPresent).
		Int("roles_local_unseeded", rolesPresent).
		Int("measurement_permissions_local_unseeded", mpermsPresent).
		Int("token_memberships_local_unseeded", memPresent).
		Msg("SeedRBACFromLocalSQLite: done")

	return firstErr
}

// isDuplicateErr returns true if the proposer error indicates the row is
// already present in cluster FSM state. Used to make the seed idempotent
// on re-run.
func isDuplicateErr(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "already exists") ||
		strings.Contains(msg, "is already a member")
}

// HasLocalRBACState reports whether the leader's local SQLite holds any
// RBAC rows. Useful as a cheap "do we even need to seed" check before
// firing 5 individual table scans.
func (rm *RBACManager) HasLocalRBACState() (bool, error) {
	var count int
	err := rm.db.QueryRow(`
		SELECT (
			SELECT COUNT(*) FROM rbac_organizations
		) + (
			SELECT COUNT(*) FROM rbac_teams
		) + (
			SELECT COUNT(*) FROM rbac_roles
		) + (
			SELECT COUNT(*) FROM rbac_measurement_permissions
		) + (
			SELECT COUNT(*) FROM rbac_token_memberships
		)
	`).Scan(&count)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, fmt.Errorf("HasLocalRBACState: %w", err)
	}
	return count > 0, nil
}
