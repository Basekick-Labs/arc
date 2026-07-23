package auth

import (
	"errors"
	"testing"
)

// Pins the error contract for the token-membership methods.
//
// AddTokenToTeam and RemoveTokenFromTeam are consumed by
// internal/api/auth_routes.go, which maps their errors to status codes:
// "team not found" / "already a member" → 400, "membership not found" →
// 404. That mapping used to compare err.Error() against exact strings, so
// rewording a message silently changed a status code. It now matches the
// sentinels asserted here.
//
// Both properties matter and are asserted together:
//
//   - errors.Is finds the sentinel (so the handlers classify correctly)
//   - Error() is byte-identical to the historical text (so the message
//     stays a stable part of the API response body)
//
// These endpoints are license-gated inline, so an HTTP-level test would
// return 403 before reaching the mapping; the contract is pinned here at
// the manager layer instead.
func TestTokenMembershipErrorContract(t *testing.T) {
	rm, am, cleanup := setupTestRBACManager(t)
	defer cleanup()

	ctx := t.Context()

	org, err := rm.CreateOrganization(ctx, &CreateOrganizationRequest{Name: "tm-org"})
	if err != nil {
		t.Fatalf("seed org: %v", err)
	}
	team, err := rm.CreateTeam(ctx, org.ID, &CreateTeamRequest{Name: "tm-team"})
	if err != nil {
		t.Fatalf("seed team: %v", err)
	}

	// CreateToken returns the plaintext value; look the row up to get its ID.
	if _, err := am.CreateToken(ctx, "tm-token", "membership test", "read", nil); err != nil {
		t.Fatalf("seed token: %v", err)
	}
	tokens, err := am.ListTokens()
	if err != nil {
		t.Fatalf("list tokens: %v", err)
	}
	var tokenID int64
	for _, tk := range tokens {
		if tk.Name == "tm-token" {
			tokenID = tk.ID
			break
		}
	}
	if tokenID == 0 {
		t.Fatal("seeded token not found in ListTokens")
	}

	t.Run("missing team is ErrNotFound", func(t *testing.T) {
		_, err := rm.AddTokenToTeam(ctx, tokenID, 99999)
		if err == nil {
			t.Fatal("expected an error for a nonexistent team")
		}
		if !errors.Is(err, ErrNotFound) {
			t.Errorf("errors.Is(err, ErrNotFound) = false for %q", err)
		}
		if err.Error() != "team not found" {
			t.Errorf("message drifted: got %q, want %q", err, "team not found")
		}
	})

	// A nonexistent token is NOT reported as "token not found" in OSS mode:
	// that check lives in the cluster (proposer) branch, and the direct
	// SQLite path trips a FOREIGN KEY constraint first, which surfaces as a
	// generic failure. Pre-existing behavior, asserted here so the
	// difference is documented rather than assumed to be a sentinel case.
	t.Run("missing token is generic in OSS mode", func(t *testing.T) {
		_, err := rm.AddTokenToTeam(ctx, 999999, team.ID)
		if err == nil {
			t.Fatal("expected an error for a nonexistent token")
		}
		if errors.Is(err, ErrNotFound) {
			t.Errorf("OSS path should not tag ErrNotFound here, got %q", err)
		}
	})

	t.Run("duplicate membership is ErrConflict", func(t *testing.T) {
		if _, err := rm.AddTokenToTeam(ctx, tokenID, team.ID); err != nil {
			t.Fatalf("first AddTokenToTeam: %v", err)
		}
		_, err := rm.AddTokenToTeam(ctx, tokenID, team.ID)
		if err == nil {
			t.Fatal("expected an error for a duplicate membership")
		}
		if !errors.Is(err, ErrConflict) {
			t.Errorf("errors.Is(err, ErrConflict) = false for %q", err)
		}
		if err.Error() != "token is already a member of this team" {
			t.Errorf("message drifted: got %q, want %q", err, "token is already a member of this team")
		}
		// Must NOT be a name collision — that would map to 409, not 400.
		if errors.Is(err, ErrNameConflict) {
			t.Error("duplicate membership must not match ErrNameConflict")
		}
	})

	t.Run("missing membership on remove is ErrNotFound", func(t *testing.T) {
		err := rm.RemoveTokenFromTeam(ctx, tokenID, 99999)
		if err == nil {
			t.Fatal("expected an error removing a nonexistent membership")
		}
		if !errors.Is(err, ErrNotFound) {
			t.Errorf("errors.Is(err, ErrNotFound) = false for %q", err)
		}
		if err.Error() != "token membership not found" {
			t.Errorf("message drifted: got %q, want %q", err, "token membership not found")
		}
	})
}
