package auth

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
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

// TestClusterRotateReReadFailureKeepsTheRotate: once the rotate has committed,
// a failed re-read of the local row must not turn it into an error, which
// would discard a valid new secret. The value is returned and the failure
// logged at Warn.
func TestClusterRotateReReadFailureKeepsTheRotate(t *testing.T) {
	logs := &syncBuffer{}
	am, err := NewAuthManager(filepath.Join(t.TempDir(), "auth.db"), time.Minute, 100, zerolog.New(logs))
	if err != nil {
		t.Fatalf("NewAuthManager: %v", err)
	}
	t.Cleanup(func() {
		am.SetRaftProposer(nil)
		_ = am.Close()
	})
	ctx := context.Background()
	if _, err := am.CreateToken(ctx, "svc", "", "read", nil); err != nil {
		t.Fatalf("CreateToken: %v", err)
	}
	id := tokenIDByName(t, am, "svc")
	prop := &fsmLikeTokenProposer{onPropose: func(cmdType uint8) {
		if cmdType == ProposalCommandRotateToken {
			// Every later read of the row fails.
			if _, err := am.db.Exec(`ALTER TABLE api_tokens RENAME TO api_tokens_gone`); err != nil {
				t.Errorf("break the table: %v", err)
			}
		}
	}}
	am.SetRaftProposer(prop)

	value, err := am.RotateToken(ctx, id)
	if err != nil || value == "" {
		t.Fatalf("rotate whose re-read fails: value %q, err %v, want the new value and no error", value, err)
	}
	if out := logs.String(); !strings.Contains(out, `"level":"warn"`) || !strings.Contains(out, "could not re-read") {
		t.Errorf("no Warn for the failed re-read; logs:\n%s", out)
	}
}

// TestWriteRotatedTokenGuardsRevoked pins the standalone rotate's write on its
// own: RotateToken's pre-check refuses a revoked token before this write is
// reached, so only a revoke landing while the value is hashed gets here, and
// the write itself must refuse it.
func TestWriteRotatedTokenGuardsRevoked(t *testing.T) {
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

	if err := am.writeRotatedToken(id, "new-hash", "new-prefix"); !errors.Is(err, ErrTokenRevoked) {
		t.Errorf("write to a revoked token: %v, want ErrTokenRevoked", err)
	}
	if h, p := tokenHashOf(t, am, id); h != hash || p != prefix {
		t.Error("the write replaced a revoked token's hash or prefix")
	}
	if err := am.writeRotatedToken(4242, "new-hash", "new-prefix"); !errors.Is(err, ErrNotFound) {
		t.Errorf("write to an unknown ID: %v, want ErrNotFound", err)
	}
}
