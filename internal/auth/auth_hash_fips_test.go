//go:build fips

package auth

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"
)

// In the FIPS build, legacy bcrypt and sha256 token hashes must FAIL CLOSED —
// verifying them would invoke a non-FIPS-approved algorithm. The operator is
// expected to rotate (recreate) such tokens, which stores a PBKDF2 hash.
func TestVerifyLegacyTokenHash_FIPSFailsClosed(t *testing.T) {
	am, cleanup := setupTestAuthManager(t)
	defer cleanup()

	t.Run("sha256 legacy hash rejected", func(t *testing.T) {
		token := "legacy-token"
		h := sha256.Sum256([]byte(token))
		legacy := hex.EncodeToString(h[:])
		if am.verifyTokenHash(token, legacy) {
			t.Error("FIPS build must reject a legacy SHA256 token hash (fail closed)")
		}
	})

	t.Run("bcrypt legacy hash rejected", func(t *testing.T) {
		// A real bcrypt hash of "x"; the FIPS build must not even attempt to
		// verify it (no bcrypt code is linked). Any $2 hash must be rejected.
		bcryptHash := "$2a$10$N9qo8uLOickgx2ZMRZoMyeIjZAgcfl7p92ldGxad68LJZdL17lhWy"
		if am.verifyTokenHash("x", bcryptHash) {
			t.Error("FIPS build must reject a legacy bcrypt token hash (fail closed)")
		}
	})

	// PBKDF2 hashes still verify in the FIPS build (the approved path).
	t.Run("pbkdf2 still verifies", func(t *testing.T) {
		token := "fips-token"
		hash, err := am.hashToken(token)
		if err != nil {
			t.Fatalf("hashToken: %v", err)
		}
		if !am.verifyTokenHash(token, hash) {
			t.Error("FIPS build must verify a PBKDF2 token hash")
		}
	})
}
