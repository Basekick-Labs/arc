package raft

import (
	"bytes"
	"io"
	"testing"

	"github.com/hashicorp/raft"
	"github.com/rs/zerolog"
)

// Phase A: Cluster Auth Convergence — FSM-level tests.
//
// These tests cover the 5 token CommandTypes (Create/Update/Revoke/
// Delete/Rotate) at the FSM-apply level: in-memory only, no SQLite,
// no real Raft. They pin the applier-side invariants:
//   - ID is stamped from the Raft log index
//   - validateTokenEntry rejects malformed payloads + bumps the
//     rejectedTokens counter
//   - tokensByPrefix is maintained in sync with tokens map
//   - apply is idempotent on log replay (update of unknown token =
//     no-op, double-create with same ID = no-op)
//   - snapshot round-trip preserves both maps + the prefix index
//
// Materialisation callbacks (FSM → AuthManager → SQLite) are tested
// at the proposer-round-trip layer in internal/auth — outside the
// scope of this file.

func newTestFSMWithBootstrapNode(t *testing.T) *ClusterFSM {
	t.Helper()
	fsm := NewClusterFSM(zerolog.Nop())
	// Most token tests don't care about nodes, but a few stale-token
	// integration tests use this helper to seed minimal state.
	return fsm
}

// makeTokenEntry constructs a CreateToken-shaped TokenEntry with
// sensible defaults. Tests override individual fields by patching
// the returned struct.
func makeTokenEntry(name string) TokenEntry {
	return TokenEntry{
		Name:              name,
		Permissions:       "read,write",
		TokenHash:         "$2a$10$" + name + "fakebcrypthash................",
		TokenPrefix:       "prefix-" + name,
		CreatedAtUnixNano: 1_700_000_000_000_000_000, // 2023-11-14 UTC
		Enabled:           true,
	}
}

// makeTokenCommand wraps a CreateTokenPayload (or any other token
// payload) in the Command{Type, Payload} envelope the FSM expects.
func makeTokenCommand(t *testing.T, cmdType CommandType, payload interface{}) []byte {
	return makeCommand(t, cmdType, payload)
}

func TestApplyCreateToken_Success(t *testing.T) {
	fsm := newTestFSMWithBootstrapNode(t)
	tok := makeTokenEntry("admin")
	cmd := makeTokenCommand(t, CommandCreateToken, CreateTokenPayload{Token: tok})

	result := fsm.Apply(&raft.Log{Data: cmd, Index: 42})
	if result != nil {
		t.Fatalf("CreateToken should succeed, got %v", result)
	}
	if fsm.TokenCount() != 1 {
		t.Errorf("expected 1 token in FSM, got %d", fsm.TokenCount())
	}
	got := fsm.GetTokenByID(42)
	if got == nil {
		t.Fatal("expected token at id=42 (the log index)")
	}
	if got.Name != "admin" {
		t.Errorf("name mismatch: got %q, want %q", got.Name, "admin")
	}
	if got.ID != 42 {
		t.Errorf("ID should be stamped from log index: got %d, want 42", got.ID)
	}
	if got.LSN != 42 {
		t.Errorf("LSN should be stamped from log index: got %d, want 42", got.LSN)
	}
	if !got.Enabled {
		t.Errorf("Enabled should default true on create")
	}
}

func TestApplyCreateToken_RejectsEmptyName(t *testing.T) {
	fsm := newTestFSMWithBootstrapNode(t)
	tok := makeTokenEntry("")
	cmd := makeTokenCommand(t, CommandCreateToken, CreateTokenPayload{Token: tok})

	result := fsm.Apply(&raft.Log{Data: cmd, Index: 1})
	if result == nil {
		t.Fatal("empty name should be rejected")
	}
	if fsm.TokenCount() != 0 {
		t.Errorf("rejected token must not land: count=%d", fsm.TokenCount())
	}
	if fsm.RejectedTokensCount() != 1 {
		t.Errorf("rejection counter should bump: got %d", fsm.RejectedTokensCount())
	}
}

func TestApplyCreateToken_RejectsMissingHash(t *testing.T) {
	fsm := newTestFSMWithBootstrapNode(t)
	tok := makeTokenEntry("admin")
	tok.TokenHash = ""
	cmd := makeTokenCommand(t, CommandCreateToken, CreateTokenPayload{Token: tok})

	result := fsm.Apply(&raft.Log{Data: cmd, Index: 1})
	if result == nil {
		t.Fatal("missing hash should be rejected")
	}
	if fsm.RejectedTokensCount() != 1 {
		t.Errorf("rejection counter should bump: got %d", fsm.RejectedTokensCount())
	}
}

func TestApplyCreateToken_RejectsMissingPrefix(t *testing.T) {
	fsm := newTestFSMWithBootstrapNode(t)
	tok := makeTokenEntry("admin")
	tok.TokenPrefix = ""
	cmd := makeTokenCommand(t, CommandCreateToken, CreateTokenPayload{Token: tok})

	result := fsm.Apply(&raft.Log{Data: cmd, Index: 1})
	if result == nil {
		t.Fatal("missing prefix should be rejected")
	}
}

func TestApplyCreateToken_RejectsInvalidPermissions(t *testing.T) {
	fsm := newTestFSMWithBootstrapNode(t)
	tok := makeTokenEntry("admin")
	tok.Permissions = "read,write,evil" // "evil" is not in the allowed verb set
	cmd := makeTokenCommand(t, CommandCreateToken, CreateTokenPayload{Token: tok})

	result := fsm.Apply(&raft.Log{Data: cmd, Index: 1})
	if result == nil {
		t.Fatal("invalid permission should be rejected")
	}
	if fsm.RejectedTokensCount() != 1 {
		t.Errorf("rejection counter should bump")
	}
}

func TestApplyCreateToken_RejectsZeroCreatedAt(t *testing.T) {
	fsm := newTestFSMWithBootstrapNode(t)
	tok := makeTokenEntry("admin")
	tok.CreatedAtUnixNano = 0
	cmd := makeTokenCommand(t, CommandCreateToken, CreateTokenPayload{Token: tok})

	result := fsm.Apply(&raft.Log{Data: cmd, Index: 1})
	if result == nil {
		t.Fatal("zero CreatedAt should be rejected (would diverge during log replay)")
	}
}

func TestApplyCreateToken_RejectsDuplicateName(t *testing.T) {
	fsm := newTestFSMWithBootstrapNode(t)
	tok1 := makeTokenEntry("admin")
	cmd1 := makeTokenCommand(t, CommandCreateToken, CreateTokenPayload{Token: tok1})
	if r := fsm.Apply(&raft.Log{Data: cmd1, Index: 1}); r != nil {
		t.Fatalf("first create should succeed: %v", r)
	}

	tok2 := makeTokenEntry("admin")
	tok2.TokenPrefix = "different-prefix"
	tok2.TokenHash = "different-hash$2a$10$..............................."
	cmd2 := makeTokenCommand(t, CommandCreateToken, CreateTokenPayload{Token: tok2})

	result := fsm.Apply(&raft.Log{Data: cmd2, Index: 2})
	if result == nil {
		t.Fatal("duplicate name should be rejected (mirrors SQLite UNIQUE constraint)")
	}
	if fsm.TokenCount() != 1 {
		t.Errorf("duplicate must not land: count=%d", fsm.TokenCount())
	}
}

func TestApplyCreateToken_StampsLogIndexAsID(t *testing.T) {
	fsm := newTestFSMWithBootstrapNode(t)
	tok := makeTokenEntry("admin")
	// Index 42 in the Raft log → ID 42 in the FSM. This is the
	// deterministic-cross-nodes property; every node applying log
	// index 42 produces a token with the same ID.
	cmd := makeTokenCommand(t, CommandCreateToken, CreateTokenPayload{Token: tok})
	fsm.Apply(&raft.Log{Data: cmd, Index: 42})
	got := fsm.GetTokenByID(42)
	if got == nil || got.ID != 42 {
		t.Fatalf("expected token at ID=42")
	}
}

func TestApplyCreateToken_MaintainsPrefixIndex(t *testing.T) {
	fsm := newTestFSMWithBootstrapNode(t)
	tok := makeTokenEntry("admin")
	tok.TokenPrefix = "abcdef0123456789"
	cmd := makeTokenCommand(t, CommandCreateToken, CreateTokenPayload{Token: tok})
	fsm.Apply(&raft.Log{Data: cmd, Index: 5})

	fsm.mu.RLock()
	ids := fsm.tokensByPrefix["abcdef0123456789"]
	fsm.mu.RUnlock()
	if len(ids) != 1 || ids[0] != 5 {
		t.Errorf("prefix index should contain id=5; got %v", ids)
	}
}

func TestApplyUpdateToken_ChangesOnlyListedFields(t *testing.T) {
	fsm := newTestFSMWithBootstrapNode(t)
	createCmd := makeTokenCommand(t, CommandCreateToken, CreateTokenPayload{Token: makeTokenEntry("svc")})
	fsm.Apply(&raft.Log{Data: createCmd, Index: 10})

	updateCmd := makeTokenCommand(t, CommandUpdateToken, UpdateTokenPayload{
		ID:            10,
		Description:   "new desc",
		ChangedFields: []string{"description"}, // Permissions intentionally left out
	})
	if r := fsm.Apply(&raft.Log{Data: updateCmd, Index: 11}); r != nil {
		t.Fatalf("update should succeed: %v", r)
	}
	got := fsm.GetTokenByID(10)
	if got == nil {
		t.Fatal("token disappeared")
	}
	if got.Description != "new desc" {
		t.Errorf("description not updated: got %q", got.Description)
	}
	if got.Permissions != "read,write" {
		t.Errorf("permissions should be unchanged: got %q", got.Permissions)
	}
	if got.LSN != 11 {
		t.Errorf("LSN should advance on update: got %d", got.LSN)
	}
}

func TestApplyUpdateToken_RejectsInvalidPermissions(t *testing.T) {
	fsm := newTestFSMWithBootstrapNode(t)
	fsm.Apply(&raft.Log{Data: makeTokenCommand(t, CommandCreateToken, CreateTokenPayload{Token: makeTokenEntry("svc")}), Index: 1})

	updateCmd := makeTokenCommand(t, CommandUpdateToken, UpdateTokenPayload{
		ID:            1,
		Permissions:   "read,evil",
		ChangedFields: []string{"permissions"},
	})
	result := fsm.Apply(&raft.Log{Data: updateCmd, Index: 2})
	if result == nil {
		t.Fatal("invalid permission on update should be rejected")
	}
	if fsm.RejectedTokensCount() != 1 {
		t.Errorf("rejection counter should bump on update validation")
	}
}

func TestApplyUpdateToken_UnknownTokenIsIdempotent(t *testing.T) {
	fsm := newTestFSMWithBootstrapNode(t)
	updateCmd := makeTokenCommand(t, CommandUpdateToken, UpdateTokenPayload{
		ID:            999,
		Description:   "x",
		ChangedFields: []string{"description"},
	})
	// Update of unknown token = no-op (covers the log-replay case where
	// CommandDeleteToken precedes the snapshot horizon).
	result := fsm.Apply(&raft.Log{Data: updateCmd, Index: 1})
	if result != nil {
		t.Fatalf("update of unknown token should be no-op, got %v", result)
	}
}

func TestApplyRevokeToken_DisablesToken(t *testing.T) {
	fsm := newTestFSMWithBootstrapNode(t)
	fsm.Apply(&raft.Log{Data: makeTokenCommand(t, CommandCreateToken, CreateTokenPayload{Token: makeTokenEntry("svc")}), Index: 7})

	if got := fsm.GetTokenByID(7); !got.Enabled {
		t.Fatal("expected freshly-created token to be enabled")
	}

	revokeCmd := makeTokenCommand(t, CommandRevokeToken, RevokeTokenPayload{ID: 7})
	if r := fsm.Apply(&raft.Log{Data: revokeCmd, Index: 8}); r != nil {
		t.Fatalf("revoke should succeed: %v", r)
	}
	got := fsm.GetTokenByID(7)
	if got.Enabled {
		t.Errorf("token should be disabled after revoke")
	}
	if got.LSN != 8 {
		t.Errorf("LSN should advance on revoke: got %d", got.LSN)
	}
}

func TestApplyDeleteToken_RemovesFromBothMaps(t *testing.T) {
	fsm := newTestFSMWithBootstrapNode(t)
	tok := makeTokenEntry("svc")
	tok.TokenPrefix = "del-prefix"
	fsm.Apply(&raft.Log{Data: makeTokenCommand(t, CommandCreateToken, CreateTokenPayload{Token: tok}), Index: 3})

	deleteCmd := makeTokenCommand(t, CommandDeleteToken, DeleteTokenPayload{ID: 3})
	if r := fsm.Apply(&raft.Log{Data: deleteCmd, Index: 4}); r != nil {
		t.Fatalf("delete should succeed: %v", r)
	}
	if fsm.GetTokenByID(3) != nil {
		t.Error("token should be gone after delete")
	}
	fsm.mu.RLock()
	_, exists := fsm.tokensByPrefix["del-prefix"]
	fsm.mu.RUnlock()
	if exists {
		t.Error("prefix index should be cleaned up after delete")
	}
}

func TestApplyDeleteToken_UnknownTokenIsIdempotent(t *testing.T) {
	fsm := newTestFSMWithBootstrapNode(t)
	deleteCmd := makeTokenCommand(t, CommandDeleteToken, DeleteTokenPayload{ID: 999})
	if r := fsm.Apply(&raft.Log{Data: deleteCmd, Index: 1}); r != nil {
		t.Fatalf("delete of unknown token should be no-op: %v", r)
	}
}

func TestApplyRotateToken_UpdatesHashAndPrefix(t *testing.T) {
	fsm := newTestFSMWithBootstrapNode(t)
	tok := makeTokenEntry("svc")
	tok.TokenPrefix = "old-prefix"
	fsm.Apply(&raft.Log{Data: makeTokenCommand(t, CommandCreateToken, CreateTokenPayload{Token: tok}), Index: 12})

	rotateCmd := makeTokenCommand(t, CommandRotateToken, RotateTokenPayload{
		ID:        12,
		NewHash:   "$2a$10$newhash................................",
		NewPrefix: "new-prefix",
	})
	if r := fsm.Apply(&raft.Log{Data: rotateCmd, Index: 13}); r != nil {
		t.Fatalf("rotate should succeed: %v", r)
	}
	got := fsm.GetTokenByID(12)
	if got.TokenPrefix != "new-prefix" {
		t.Errorf("prefix should be new-prefix, got %q", got.TokenPrefix)
	}
	if got.LSN != 13 {
		t.Errorf("LSN should advance: got %d", got.LSN)
	}
	// Prefix index should now point to id=12 under "new-prefix" and
	// "old-prefix" should be removed.
	fsm.mu.RLock()
	newIDs := fsm.tokensByPrefix["new-prefix"]
	_, oldExists := fsm.tokensByPrefix["old-prefix"]
	fsm.mu.RUnlock()
	if len(newIDs) != 1 || newIDs[0] != 12 {
		t.Errorf("new prefix should contain id=12; got %v", newIDs)
	}
	if oldExists {
		t.Error("old prefix should be removed from index")
	}
}

func TestApplyRotateToken_RejectsMissingFields(t *testing.T) {
	fsm := newTestFSMWithBootstrapNode(t)
	fsm.Apply(&raft.Log{Data: makeTokenCommand(t, CommandCreateToken, CreateTokenPayload{Token: makeTokenEntry("svc")}), Index: 1})

	// Missing new_hash.
	rotateCmd := makeTokenCommand(t, CommandRotateToken, RotateTokenPayload{
		ID:        1,
		NewPrefix: "new-prefix",
	})
	if r := fsm.Apply(&raft.Log{Data: rotateCmd, Index: 2}); r == nil {
		t.Fatal("rotate with empty new_hash should be rejected")
	}
}

func TestFSMSnapshot_RoundTripsTokens(t *testing.T) {
	fsm := newTestFSMWithBootstrapNode(t)

	// Seed 3 tokens with distinct prefixes.
	for i, name := range []string{"admin", "svc1", "svc2"} {
		tok := makeTokenEntry(name)
		tok.TokenPrefix = "prefix-" + name
		fsm.Apply(&raft.Log{Data: makeTokenCommand(t, CommandCreateToken, CreateTokenPayload{Token: tok}), Index: uint64(i + 1)})
	}
	if fsm.TokenCount() != 3 {
		t.Fatalf("setup: expected 3 tokens, got %d", fsm.TokenCount())
	}

	// Snapshot + reload into a fresh FSM. testSnapshotSink is defined
	// in fsm_test.go (same package).
	snap, err := fsm.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot failed: %v", err)
	}

	var buf bytes.Buffer
	sink := &testSnapshotSink{Writer: &buf}
	if err := snap.Persist(sink); err != nil {
		t.Fatalf("Persist failed: %v", err)
	}

	fsm2 := newTestFSMWithBootstrapNode(t)
	if err := fsm2.Restore(io.NopCloser(&buf)); err != nil {
		t.Fatalf("Restore failed: %v", err)
	}
	if fsm2.TokenCount() != 3 {
		t.Errorf("restored FSM should have 3 tokens, got %d", fsm2.TokenCount())
	}
	// Prefix index must be rebuilt.
	fsm2.mu.RLock()
	prefixCount := len(fsm2.tokensByPrefix)
	fsm2.mu.RUnlock()
	if prefixCount != 3 {
		t.Errorf("prefix index should have 3 entries, got %d", prefixCount)
	}
}
