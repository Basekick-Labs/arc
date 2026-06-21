//go:build !fips

package auth

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"
)

// In the default (non-FIPS) build, legacy bcrypt and sha256 token hashes must
// still verify so an upgrade does not invalidate existing tokens.
func TestVerifyLegacyTokenHash_DefaultBuild(t *testing.T) {
	am, cleanup := setupTestAuthManager(t)
	defer cleanup()

	t.Run("sha256 legacy hash verifies", func(t *testing.T) {
		token := "legacy-token"
		h := sha256Sum(token)
		if !am.verifyTokenHash(token, h) {
			t.Error("default build should verify a valid legacy SHA256 hash")
		}
		if am.verifyTokenHash("wrong-token", h) {
			t.Error("legacy SHA256 verify should fail for wrong token")
		}
	})
}

// sha256Sum computes a SHA256 hex digest, matching the pre-v26 legacy token
// hash format. Only used by the default-build legacy verification test.
func sha256Sum(s string) string {
	h := sha256.Sum256([]byte(s))
	return hex.EncodeToString(h[:])
}
