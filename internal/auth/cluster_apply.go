package auth

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// proposeTimeout caps how long a single AuthManager write will wait for
// a Raft apply (whether direct on the leader or forwarded to it).
// Matches the timeout used by internal/cluster/file_registrar.go for
// CommandRegisterFile.
const proposeTimeout = 5 * time.Second

// SetRaftProposer wires the cluster's Raft FSM into AuthManager. Calling
// this with a non-nil proposer flips every subsequent CreateToken /
// UpdateToken / RevokeToken / DeleteToken / RotateToken from direct-
// SQLite to a Raft-proposed apply. Calling it with nil flips back to
// direct-SQLite (used by tests and by graceful shutdown). Safe to call
// concurrently with active writes; the proposer is read under proposerMu.
func (am *AuthManager) SetRaftProposer(p RaftProposer) {
	am.proposerMu.Lock()
	am.proposer = p
	am.proposerMu.Unlock()
}

// getProposer returns the current proposer (or nil) under the read lock.
// Hot-path writes call this once and operate on the snapshot — if a
// concurrent SetRaftProposer flips it, the in-flight write completes
// against whichever proposer was active when it started, which is the
// behaviour we want (avoids torn writes).
func (am *AuthManager) getProposer() RaftProposer {
	am.proposerMu.RLock()
	defer am.proposerMu.RUnlock()
	return am.proposer
}

// proposeCommand marshals the payload, wraps it in the proposer's
// expected envelope (commandType + bytes), and submits via the
// proposer. Returns nil on a successful FSM apply on the leader (the
// apply has already mutated the in-memory FSM map AND triggered the
// onTokenXxx callback on the leader; follower applies happen
// asynchronously within the usual <50ms Raft replication window).
//
// On the proposer node itself (whether leader or follower), the
// callback has already fired by the time this returns — so the local
// SQLite materialise is done. The caller can rely on a subsequent
// VerifyToken on this node seeing the new state.
func (am *AuthManager) proposeCommand(ctx context.Context, cmdType uint8, payload interface{}) error {
	p := am.getProposer()
	if p == nil {
		return fmt.Errorf("auth: proposer not configured (cluster mode required)")
	}
	bytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("auth: marshal payload: %w", err)
	}
	if err := p.Propose(ctx, cmdType, bytes, proposeTimeout); err != nil {
		return err
	}
	return nil
}

// ClusterTokenEntry is the FSM-side projection of an api_tokens row.
// The cluster package's TokenEntry mirrors this shape; we duplicate the
// fields here so the FSM (internal/cluster/raft) can call back into the
// AuthManager without auth importing the cluster.raft package (which
// would cycle via internal/cluster → internal/auth).
//
// Field names match cluster.raft.TokenEntry exactly so the cluster shim
// can copy the struct field by field without translation.
type ClusterTokenEntry struct {
	ID                int64
	Name              string
	Description       string
	Permissions       string
	TokenHash         string
	TokenPrefix       string
	CreatedAtUnixNano int64
	ExpiresAtUnixNano int64
	Enabled           bool
	LSN               uint64
}

// clusterTokenEntryWire is the JSON-tagged wire form of ClusterTokenEntry,
// used when AuthManager marshals a CreateTokenPayload / UpdateTokenPayload
// for the proposer. The JSON tags MUST match the cluster.raft.TokenEntry
// field tags exactly — drift here means the FSM apply on the leader
// sees zero-valued fields and the apply silently writes a broken row.
//
// Mirror is pinned by a test in Step 7 (internal/cluster/raft/
// fsm_test.go's TestProposalWirePinning) that round-trips a
// clusterTokenEntryWire through json.Marshal into a TokenEntry and
// asserts every field survives. Any future field addition needs both
// sides updated AND the pinning test extended.
type clusterTokenEntryWire struct {
	ID                int64  `json:"id"`
	Name              string `json:"name"`
	Description       string `json:"description,omitempty"`
	Permissions       string `json:"permissions"`
	TokenHash         string `json:"token_hash"`
	TokenPrefix       string `json:"token_prefix"`
	CreatedAtUnixNano int64  `json:"created_at_unix_nano"`
	ExpiresAtUnixNano int64  `json:"expires_at_unix_nano,omitempty"`
	Enabled           bool   `json:"enabled"`
	LSN               uint64 `json:"lsn,omitempty"`
}

// ApplyCreateToken materialises a CreateToken Raft apply into local
// SQLite. Called from every node's FSM apply callback (including the
// proposer's own apply, because the apply path is the single source of
// truth for the SQLite write — see the plan's "FSM is source of truth"
// decision).
//
// Idempotency vs. divergence detection: log replay can re-apply the
// same CommandCreateToken (FSM snapshot older than last applied index),
// so we must accept "row already exists with identical fields" as a
// no-op. But a row at the same ID with a DIFFERENT hash means an
// upgrade-in-place left a pre-26.06.1 AUTOINCREMENT row colliding
// with the Raft-stamped log-index ID space — surface that loudly
// rather than silently dropping the cluster-authoritative apply.
//
// Cache invalidate happens unconditionally so concurrent VerifyToken
// calls on this node pick up the new row on next check.
func (am *AuthManager) ApplyCreateToken(entry ClusterTokenEntry) error {
	if entry.ID == 0 {
		return fmt.Errorf("ApplyCreateToken: id required")
	}
	// .UTC() on both: SQLite text-encodes time.Time using the value's
	// location, so a future `WHERE created_at = ?` or `WHERE expires_at = ?`
	// readback can only round-trip cleanly if writes are in a stable
	// timezone. Matches the RBAC apply path's UTC convention (#459).
	// Issue #460.
	createdAt := time.Unix(0, entry.CreatedAtUnixNano).UTC()
	var expiresAt interface{}
	if entry.ExpiresAtUnixNano != 0 {
		expiresAt = time.Unix(0, entry.ExpiresAtUnixNano).UTC()
	}
	enabled := 0
	if entry.Enabled {
		enabled = 1
	}

	// Phase A: detect existing-row-with-different-content collisions
	// (the upgrade-in-place divergence shape) before INSERTing. The
	// FSM is the cluster-authoritative state; SQLite is the materialised
	// cache. If a pre-26.06.1 AUTOINCREMENT row at this ID exists and
	// carries DIFFERENT content than the Raft apply, silently
	// overwriting would hide the divergence and silently no-oping
	// would diverge the cache from the FSM. Surface it loudly so the
	// operator can decide.
	var existingHash, existingName string
	queryErr := am.db.QueryRow(
		`SELECT token_hash, name FROM api_tokens WHERE id = ?`,
		entry.ID,
	).Scan(&existingHash, &existingName)
	switch {
	case queryErr == nil:
		// Row exists. Idempotent replay: identical hash + name → no-op.
		// (Log replay re-applies the same CommandCreateToken when the
		// FSM snapshot is older than the last applied index.)
		if existingHash == entry.TokenHash && existingName == entry.Name {
			am.InvalidateCache()
			return nil
		}
		// Divergence: pre-existing local row at this ID has different
		// content. Refusing to overwrite preserves the operator's
		// ability to log in via the old token; surfacing the error
		// makes the upgrade hazard visible. Operator action: drop
		// the diverging local auth.db rows before re-joining the
		// cluster, or accept the local-only behaviour (the cluster's
		// authoritative state stays in the FSM in-memory map).
		return fmt.Errorf("ApplyCreateToken: id %d already exists locally with different token (cluster<->local divergence; see upgrade notes for pre-26.06.1 tokens)", entry.ID)
	case errors.Is(queryErr, sql.ErrNoRows):
		// Expected fresh-row path; fall through to INSERT.
	default:
		return fmt.Errorf("ApplyCreateToken: pre-insert lookup: %w", queryErr)
	}

	if _, insertErr := am.db.Exec(`
		INSERT INTO api_tokens
			(id, name, token_hash, token_prefix, description, permissions, created_at, expires_at, enabled)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, entry.ID, entry.Name, entry.TokenHash, entry.TokenPrefix, entry.Description, entry.Permissions, createdAt, expiresAt, enabled); insertErr != nil {
		// A name-collision (UNIQUE(name) constraint) here would mean
		// the cluster applied a Raft-stamped row whose name conflicts
		// with a pre-existing AUTOINCREMENT row — same upgrade hazard
		// from the opposite direction. Surface it.
		return fmt.Errorf("ApplyCreateToken: insert: %w", insertErr)
	}
	am.InvalidateCache()
	return nil
}

// ApplyUpdateToken materialises an UpdateToken Raft apply into local
// SQLite. Each field is updated unconditionally with the new value
// from the FSM — the FSM already merged the partial-update semantics
// (ChangedFields gating), so by the time we get here every field on
// the entry holds the post-update authoritative value.
func (am *AuthManager) ApplyUpdateToken(entry ClusterTokenEntry) error {
	if entry.ID == 0 {
		return fmt.Errorf("ApplyUpdateToken: id required")
	}
	var expiresAt interface{}
	if entry.ExpiresAtUnixNano != 0 {
		// .UTC() per the cluster-apply UTC convention; see ApplyCreateToken
		// for the rationale. Issue #460.
		expiresAt = time.Unix(0, entry.ExpiresAtUnixNano).UTC()
	}
	result, err := am.db.Exec(`
		UPDATE api_tokens
		SET name = ?, description = ?, permissions = ?, expires_at = ?
		WHERE id = ?
	`, entry.Name, entry.Description, entry.Permissions, expiresAt, entry.ID)
	if err != nil {
		return fmt.Errorf("ApplyUpdateToken: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("ApplyUpdateToken: rows affected: %w", err)
	}
	if rows == 0 {
		// Cluster FSM holds this token but our local SQLite doesn't. Most
		// likely cause: the local row was manually deleted, or the FSM
		// applied a CreateToken on this node that failed materialise and
		// the upstream divergence wasn't repaired before this Update
		// landed. Surface loudly so the operator can re-sync the local
		// cache. The next CreateToken / restart re-applies the FSM
		// snapshot and the row reappears. Gemini #451 round-3 review.
		am.logger.Error().
			Int64("token_id", entry.ID).
			Str("name", entry.Name).
			Msg("ApplyUpdateToken: local SQLite row missing — cluster FSM<->local cache divergence")
		am.InvalidateCache()
		return fmt.Errorf("ApplyUpdateToken: id %d not present in local SQLite cache (cluster<->local divergence)", entry.ID)
	}
	am.InvalidateCache()
	return nil
}

// ApplyRevokeToken materialises a RevokeToken Raft apply.
//
// Checks RowsAffected() to detect local cache divergence: if the cluster
// FSM holds this token enabled but our SQLite doesn't have the row at
// all, the revoke would silently no-op and the token would keep
// authenticating against the local stale cache (a security regression).
// Gemini #451 round-3 review.
func (am *AuthManager) ApplyRevokeToken(id int64) error {
	if id == 0 {
		return fmt.Errorf("ApplyRevokeToken: id required")
	}
	result, err := am.db.Exec("UPDATE api_tokens SET enabled = 0 WHERE id = ?", id)
	if err != nil {
		return fmt.Errorf("ApplyRevokeToken: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("ApplyRevokeToken: rows affected: %w", err)
	}
	if rows == 0 {
		am.logger.Error().
			Int64("token_id", id).
			Msg("ApplyRevokeToken: local SQLite row missing — cluster<->local divergence; the token may still authenticate against this node's stale cache")
		am.InvalidateCache()
		return fmt.Errorf("ApplyRevokeToken: id %d not present in local SQLite cache (cluster<->local divergence)", id)
	}
	am.InvalidateCache()
	return nil
}

// ApplyDeleteToken materialises a DeleteToken Raft apply.
//
// A zero RowsAffected() count is technically idempotent (deletion of a
// non-existent row is a SQL no-op), but in this context it signals that
// the local materialised cache was already missing an entry the cluster
// FSM thought existed. Logged loudly so divergence is observable, but
// NOT returned as an error — the desired post-state (no row) is the
// same either way. Gemini #451 round-3 review.
func (am *AuthManager) ApplyDeleteToken(id int64) error {
	if id == 0 {
		return fmt.Errorf("ApplyDeleteToken: id required")
	}
	result, err := am.db.Exec("DELETE FROM api_tokens WHERE id = ?", id)
	if err != nil {
		return fmt.Errorf("ApplyDeleteToken: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("ApplyDeleteToken: rows affected: %w", err)
	}
	if rows == 0 {
		am.logger.Warn().
			Int64("token_id", id).
			Msg("ApplyDeleteToken: local SQLite row already missing — cluster<->local divergence (post-state matches, no error)")
	}
	am.InvalidateCache()
	return nil
}

// ApplyRotateToken materialises a RotateToken Raft apply.
//
// Particularly important to detect divergence here: a missing local row
// means the rotate silently no-ops and the token KEEPS authenticating
// against the OLD plaintext (via the OLD hash that's still in the local
// cache) until the cache TTL expires. Gemini #451 round-3 review.
func (am *AuthManager) ApplyRotateToken(id int64, newHash, newPrefix string) error {
	if id == 0 {
		return fmt.Errorf("ApplyRotateToken: id required")
	}
	if newHash == "" || newPrefix == "" {
		return fmt.Errorf("ApplyRotateToken: new_hash and new_prefix required")
	}
	result, err := am.db.Exec("UPDATE api_tokens SET token_hash = ?, token_prefix = ? WHERE id = ?", newHash, newPrefix, id)
	if err != nil {
		return fmt.Errorf("ApplyRotateToken: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("ApplyRotateToken: rows affected: %w", err)
	}
	if rows == 0 {
		am.logger.Error().
			Int64("token_id", id).
			Msg("ApplyRotateToken: local SQLite row missing — cluster<->local divergence; the OLD token may keep authenticating against this node until its cache entry expires")
		am.InvalidateCache()
		return fmt.Errorf("ApplyRotateToken: id %d not present in local SQLite cache (cluster<->local divergence)", id)
	}
	am.InvalidateCache()
	return nil
}
