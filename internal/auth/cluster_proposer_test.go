package auth

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// Phase A: Cluster Auth Convergence — proposer round-trip tests.
//
// These tests pin the seam between AuthManager (which routes writes
// through the proposer) and a downstream FSM-equivalent. We don't
// import internal/cluster/raft here (would cycle); instead we use a
// fakeProposer that simulates a leader-side Apply by directly invoking
// AuthManager's Apply* materialise methods. The result is a single-
// node round-trip that proves:
//
//   1. CreateToken via proposer → fakeProposer.Apply → ApplyCreateToken
//      → local SQLite → VerifyToken returns valid TokenInfo.
//   2. RevokeToken invalidates the cache so the next VerifyToken sees
//      the token disabled.
//   3. Nil proposer falls through to the existing direct-SQLite path
//      (OSS unchanged).
//   4. The wire format (JSON tags on clusterTokenEntryWire) round-trips
//      through json.Marshal/Unmarshal without field drift.

// fakeProposer simulates a Raft leader by deterministically assigning
// monotonic IDs and invoking the matching AuthManager.Apply* method.
// In production the equivalent happens on the FSM's runFSM goroutine
// after Raft commits; here we collapse the loop into the same call
// site so we can assert against the resulting SQLite state synchronously.
type fakeProposer struct {
	mu     sync.Mutex
	logIdx atomic.Int64
	am     *AuthManager // wired post-construction via setAuthManager
}

func newFakeProposer() *fakeProposer { return &fakeProposer{} }

func (f *fakeProposer) setAuthManager(am *AuthManager) { f.am = am }

func (f *fakeProposer) Propose(ctx context.Context, cmdType uint8, payload []byte, timeout time.Duration) error {
	if f.am == nil {
		return fmt.Errorf("fakeProposer: AuthManager not wired")
	}
	f.mu.Lock()
	defer f.mu.Unlock()

	idx := f.logIdx.Add(1)
	switch cmdType {
	case ProposalCommandCreateToken:
		var p struct {
			Token clusterTokenEntryWire `json:"token"`
		}
		if err := json.Unmarshal(payload, &p); err != nil {
			return err
		}
		entry := ClusterTokenEntry{
			ID:                idx, // simulate FSM stamping ID = log index
			Name:              p.Token.Name,
			Description:       p.Token.Description,
			Permissions:       p.Token.Permissions,
			TokenHash:         p.Token.TokenHash,
			TokenPrefix:       p.Token.TokenPrefix,
			CreatedAtUnixNano: p.Token.CreatedAtUnixNano,
			ExpiresAtUnixNano: p.Token.ExpiresAtUnixNano,
			Enabled:           true,
			LSN:               uint64(idx),
		}
		// Reject duplicate names — mirror the FSM's UNIQUE check so the
		// proposer test exercises that path.
		if existing := f.findByName(entry.Name); existing != nil {
			return errors.New("token name already exists")
		}
		return f.am.ApplyCreateToken(entry)
	case ProposalCommandUpdateToken:
		var p struct {
			ID            int64    `json:"id"`
			Name          string   `json:"name,omitempty"`
			Description   string   `json:"description,omitempty"`
			Permissions   string   `json:"permissions,omitempty"`
			ChangedFields []string `json:"changed_fields"`
		}
		if err := json.Unmarshal(payload, &p); err != nil {
			return err
		}
		// Load current entry to apply partial-update semantics, the same
		// way the FSM does.
		row, err := f.am.GetTokenByID(p.ID)
		if err != nil {
			return err
		}
		changed := map[string]bool{}
		for _, c := range p.ChangedFields {
			changed[c] = true
		}
		if changed["name"] {
			row.Name = p.Name
		}
		if changed["description"] {
			row.Description = p.Description
		}
		if changed["permissions"] {
			row.Permissions = []string{p.Permissions} // simplified — the real apply joins on comma
		}
		entry := ClusterTokenEntry{
			ID:          row.ID,
			Name:        row.Name,
			Description: row.Description,
			Permissions: row.Permissions[0], // simplified
			Enabled:     row.Enabled,
		}
		return f.am.ApplyUpdateToken(entry)
	case ProposalCommandRevokeToken:
		var p struct {
			ID int64 `json:"id"`
		}
		if err := json.Unmarshal(payload, &p); err != nil {
			return err
		}
		return f.am.ApplyRevokeToken(p.ID)
	case ProposalCommandDeleteToken:
		var p struct {
			ID int64 `json:"id"`
		}
		if err := json.Unmarshal(payload, &p); err != nil {
			return err
		}
		return f.am.ApplyDeleteToken(p.ID)
	case ProposalCommandRotateToken:
		var p struct {
			ID        int64  `json:"id"`
			NewHash   string `json:"new_hash"`
			NewPrefix string `json:"new_prefix"`
		}
		if err := json.Unmarshal(payload, &p); err != nil {
			return err
		}
		return f.am.ApplyRotateToken(p.ID, p.NewHash, p.NewPrefix)
	default:
		return fmt.Errorf("fakeProposer: unknown command type %d", cmdType)
	}
}

func (f *fakeProposer) findByName(name string) *TokenInfo {
	tokens, err := f.am.ListTokens()
	if err != nil {
		return nil
	}
	for _, t := range tokens {
		if t.Name == name {
			t2 := t
			return &t2
		}
	}
	return nil
}

// setupClusteredAuthManager builds an AuthManager backed by a temp
// SQLite + a fakeProposer wired together. Returns a cleanup function.
func setupClusteredAuthManager(t *testing.T) (*AuthManager, *fakeProposer, func()) {
	t.Helper()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "auth.db")
	am, err := NewAuthManager(dbPath, 1*time.Second, 100, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewAuthManager: %v", err)
	}
	prop := newFakeProposer()
	prop.setAuthManager(am)
	am.SetRaftProposer(prop)
	return am, prop, func() {
		am.SetRaftProposer(nil) // restore OSS state before close
		_ = am.Close()
	}
}

func TestCreateToken_RoundTripsThroughProposer(t *testing.T) {
	am, _, cleanup := setupClusteredAuthManager(t)
	defer cleanup()

	plaintext, err := am.CreateToken("smoke", "round-trip test", "read,write", nil)
	if err != nil {
		t.Fatalf("CreateToken: %v", err)
	}
	if plaintext == "" {
		t.Fatal("expected non-empty plaintext returned to caller")
	}

	// The materialise hook should have written the row + invalidated
	// the cache, so VerifyToken sees it on first call (no cache hit).
	info := am.VerifyToken(plaintext)
	if info == nil {
		t.Fatal("VerifyToken returned nil for freshly-created token via proposer")
	}
	if info.Name != "smoke" {
		t.Errorf("name mismatch: got %q, want %q", info.Name, "smoke")
	}
}

func TestRevokeToken_InvalidatesCacheOnApply(t *testing.T) {
	am, _, cleanup := setupClusteredAuthManager(t)
	defer cleanup()

	plaintext, err := am.CreateToken("svc", "test", "read,write", nil)
	if err != nil {
		t.Fatalf("CreateToken: %v", err)
	}

	// Warm the cache.
	if info := am.VerifyToken(plaintext); info == nil {
		t.Fatal("token should validate before revoke")
	}

	// Get the token's ID via list so we can revoke.
	tokens, err := am.ListTokens()
	if err != nil {
		t.Fatalf("ListTokens: %v", err)
	}
	var id int64
	for _, tk := range tokens {
		if tk.Name == "svc" {
			id = tk.ID
		}
	}
	if id == 0 {
		t.Fatal("could not find newly created token")
	}

	if err := am.RevokeToken(id); err != nil {
		t.Fatalf("RevokeToken: %v", err)
	}

	// Cache should be invalidated — VerifyToken now sees enabled=0 and
	// returns nil. If the cache wasn't invalidated by ApplyRevokeToken,
	// the warm entry would survive and incorrectly return valid.
	if info := am.VerifyToken(plaintext); info != nil {
		t.Errorf("VerifyToken should reject revoked token, got %+v", info)
	}
}

func TestProposerNil_FallsThroughToDirectSQLite(t *testing.T) {
	// Construct an AuthManager WITHOUT a proposer (the OSS / standalone
	// configuration). All writes must hit SQLite directly.
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "auth.db")
	am, err := NewAuthManager(dbPath, 1*time.Second, 100, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewAuthManager: %v", err)
	}
	defer am.Close()
	if am.getProposer() != nil {
		t.Fatal("freshly-constructed AuthManager should have nil proposer")
	}

	plaintext, err := am.CreateToken("oss-mode", "fallback test", "read,write", nil)
	if err != nil {
		t.Fatalf("CreateToken (OSS path): %v", err)
	}
	if info := am.VerifyToken(plaintext); info == nil {
		t.Fatal("OSS path should still create + verify tokens directly")
	}
}

func TestEnsureFirstToken_ReturnsEmptyOnDuplicateName(t *testing.T) {
	am, _, cleanup := setupClusteredAuthManager(t)
	defer cleanup()

	// Pre-seed an admin token through the proposer.
	if _, err := am.EnsureInitialToken(); err != nil {
		t.Fatalf("first EnsureInitialToken: %v", err)
	}

	// Second call from a "follower" node should detect "already exists"
	// and return empty string + nil error (no banner on the non-winner).
	got, err := am.EnsureInitialToken()
	if err != nil {
		t.Fatalf("second EnsureInitialToken: %v", err)
	}
	if got != "" {
		t.Errorf("second EnsureInitialToken should return empty (no-op); got %q", got)
	}
}

func TestClusterTokenEntryWire_JSONRoundTrip(t *testing.T) {
	// Pin the wire format: a clusterTokenEntryWire marshalled to JSON
	// and unmarshalled back must preserve every field. If anyone adds a
	// field to one side without the other, this test breaks early.
	original := clusterTokenEntryWire{
		ID:                42,
		Name:              "smoke",
		Description:       "wire round trip",
		Permissions:       "read,write,delete",
		TokenHash:         "$2a$10$......................................",
		TokenPrefix:       "abcdef0123456789",
		CreatedAtUnixNano: 1_700_000_000_000_000_000,
		ExpiresAtUnixNano: 1_800_000_000_000_000_000,
		Enabled:           true,
		LSN:               99,
	}
	bytes, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var restored clusterTokenEntryWire
	if err := json.Unmarshal(bytes, &restored); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if restored != original {
		t.Errorf("wire round-trip lost fields\nbefore: %+v\nafter:  %+v", original, restored)
	}
}
