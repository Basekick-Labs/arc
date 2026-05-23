package auth

import (
	"fmt"
	"time"
)

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

// ApplyCreateToken materialises a CreateToken Raft apply into local
// SQLite. Called from every node's FSM apply callback (including the
// proposer's own apply, because the apply path is the single source of
// truth for the SQLite write — see the plan's "FSM is source of truth"
// decision).
//
// Idempotent: if a row with this ID already exists (log replay after a
// crash), the INSERT OR IGNORE is a no-op. The FSM's own validation
// already happened before this is called, so we don't re-validate here.
//
// Cache invalidate happens unconditionally so concurrent VerifyToken
// calls on this node pick up the new row on next check.
func (am *AuthManager) ApplyCreateToken(entry ClusterTokenEntry) error {
	if entry.ID == 0 {
		return fmt.Errorf("ApplyCreateToken: id required")
	}
	createdAt := time.Unix(0, entry.CreatedAtUnixNano)
	var expiresAt interface{}
	if entry.ExpiresAtUnixNano != 0 {
		expiresAt = time.Unix(0, entry.ExpiresAtUnixNano)
	}
	enabled := 0
	if entry.Enabled {
		enabled = 1
	}
	// INSERT OR IGNORE so log replay (which can re-apply the same
	// CommandCreateToken when the FSM's snapshot is older than the
	// last applied index) doesn't duplicate the row. The FSM is the
	// authoritative state; SQLite is the materialised cache.
	_, err := am.db.Exec(`
		INSERT OR IGNORE INTO api_tokens
			(id, name, token_hash, token_prefix, description, permissions, created_at, expires_at, enabled)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, entry.ID, entry.Name, entry.TokenHash, entry.TokenPrefix, entry.Description, entry.Permissions, createdAt, expiresAt, enabled)
	if err != nil {
		return fmt.Errorf("ApplyCreateToken: insert: %w", err)
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
		expiresAt = time.Unix(0, entry.ExpiresAtUnixNano)
	}
	_, err := am.db.Exec(`
		UPDATE api_tokens
		SET name = ?, description = ?, permissions = ?, expires_at = ?
		WHERE id = ?
	`, entry.Name, entry.Description, entry.Permissions, expiresAt, entry.ID)
	if err != nil {
		return fmt.Errorf("ApplyUpdateToken: %w", err)
	}
	am.InvalidateCache()
	return nil
}

// ApplyRevokeToken materialises a RevokeToken Raft apply.
func (am *AuthManager) ApplyRevokeToken(id int64) error {
	if id == 0 {
		return fmt.Errorf("ApplyRevokeToken: id required")
	}
	_, err := am.db.Exec("UPDATE api_tokens SET enabled = 0 WHERE id = ?", id)
	if err != nil {
		return fmt.Errorf("ApplyRevokeToken: %w", err)
	}
	am.InvalidateCache()
	return nil
}

// ApplyDeleteToken materialises a DeleteToken Raft apply.
func (am *AuthManager) ApplyDeleteToken(id int64) error {
	if id == 0 {
		return fmt.Errorf("ApplyDeleteToken: id required")
	}
	_, err := am.db.Exec("DELETE FROM api_tokens WHERE id = ?", id)
	if err != nil {
		return fmt.Errorf("ApplyDeleteToken: %w", err)
	}
	am.InvalidateCache()
	return nil
}

// ApplyRotateToken materialises a RotateToken Raft apply.
func (am *AuthManager) ApplyRotateToken(id int64, newHash, newPrefix string) error {
	if id == 0 {
		return fmt.Errorf("ApplyRotateToken: id required")
	}
	if newHash == "" || newPrefix == "" {
		return fmt.Errorf("ApplyRotateToken: new_hash and new_prefix required")
	}
	_, err := am.db.Exec("UPDATE api_tokens SET token_hash = ?, token_prefix = ? WHERE id = ?", newHash, newPrefix, id)
	if err != nil {
		return fmt.Errorf("ApplyRotateToken: %w", err)
	}
	am.InvalidateCache()
	return nil
}
