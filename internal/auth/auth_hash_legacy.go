//go:build !fips

package auth

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"

	"golang.org/x/crypto/bcrypt"
)

// verifyLegacyTokenHash verifies pre-PBKDF2 token hashes in the default
// (non-FIPS) build: bcrypt ($2…) hashes minted before the PBKDF2 migration,
// and bare sha256 hashes from pre-v26 tokens. New tokens are always PBKDF2
// (see hashToken); this path exists purely for backward compatibility so an
// upgrade does not invalidate existing tokens.
//
// The fips build replaces this file (auth_hash_fips.go) with a fail-closed
// implementation, because bcrypt is not FIPS-approved and a bare unsalted
// sha256 is not an acceptable token KDF under a FIPS posture.
func (am *AuthManager) verifyLegacyTokenHash(token, hash string) bool {
	// Bcrypt hash (used for new tokens between v26 and the PBKDF2 migration).
	if strings.HasPrefix(hash, "$2") {
		return bcrypt.CompareHashAndPassword([]byte(hash), []byte(token)) == nil
	}
	// SHA256 hash (legacy compatibility for pre-v26 tokens).
	// #nosec G401 -- Legacy compatibility only, not used for new token storage.
	h := sha256.Sum256([]byte(token))
	return hash == hex.EncodeToString(h[:])
}
