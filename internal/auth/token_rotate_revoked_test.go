package auth

import (
	"context"
	"errors"
	"testing"
)

// #1625: a revoked token is not rotated. Revocation is permanent (no route
// re-enables a token), so the new secret could never authenticate; the rotate
// is refused with ErrTokenRevoked and the stored hash is left alone.

func tokenHashOf(t *testing.T, am *AuthManager, id int64) (hash, prefix string) {
	t.Helper()
	if err := am.db.QueryRow(`SELECT token_hash, token_prefix FROM api_tokens WHERE id = ?`, id).Scan(&hash, &prefix); err != nil {
		t.Fatalf("read token %d: %v", id, err)
	}
	return hash, prefix
}

func TestRotateRevokedTokenStandalone(t *testing.T) {
	am := newTokenWriteTestManager(t)
	ctx := context.Background()
	if _, err := am.CreateToken(ctx, "svc", "", "read", nil); err != nil {
		t.Fatalf("CreateToken: %v", err)
	}
	id := tokenIDByName(t, am, "svc")
	if err := am.RevokeToken(ctx, id); err != nil {
		t.Fatalf("RevokeToken: %v", err)
	}
	hash, prefix := tokenHashOf(t, am, id)

	value, err := am.RotateToken(ctx, id)
	if !errors.Is(err, ErrTokenRevoked) || value != "" {
		t.Fatalf("rotate of a revoked token: value %q, err %v, want ErrTokenRevoked", value, err)
	}
	if errors.Is(err, ErrNotFound) {
		t.Errorf("rotate of a revoked token is also ErrNotFound: %v", err)
	}
	if h, p := tokenHashOf(t, am, id); h != hash || p != prefix {
		t.Error("the refused rotate changed the stored hash or prefix")
	}
	// Revoking it again is still fine (idempotent).
	if err := am.RevokeToken(ctx, id); err != nil {
		t.Errorf("second revoke: %v", err)
	}
}

func TestRotateRevokedTokenClustered(t *testing.T) {
	am := newTokenWriteTestManager(t)
	ctx := context.Background()
	// The create and the revoke as the FSM's apply callbacks write them on
	// this node, so the row is cluster-materialised and enabled=0 is really
	// stored; the fake proposer applies nothing.
	const id = 7
	if err := am.ApplyCreateToken(ClusterTokenEntry{
		ID: id, Name: "svc", Permissions: "read",
		TokenHash: "$2a$10$svcfakebcrypthash.............................", TokenPrefix: "svcprefix0123",
		CreatedAtUnixNano: 1_700_000_000_000_000_000, Enabled: true, LSN: id,
	}); err != nil {
		t.Fatalf("ApplyCreateToken: %v", err)
	}
	if err := am.ApplyRevokeToken(id); err != nil {
		t.Fatalf("ApplyRevokeToken: %v", err)
	}
	prop := &fsmLikeTokenProposer{}
	am.SetRaftProposer(prop)

	value, err := am.RotateToken(ctx, id)
	if !errors.Is(err, ErrTokenRevoked) || value != "" {
		t.Fatalf("rotate of a revoked token: value %q, err %v, want ErrTokenRevoked", value, err)
	}
	if got := prop.proposals(); len(got) != 0 {
		t.Errorf("proposed %v for a revoked token, want nothing", got)
	}
}
