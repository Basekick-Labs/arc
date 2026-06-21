//go:build fips

package auth

import "strings"

// verifyLegacyTokenHash fails closed in the FIPS build.
//
// Legacy token hashes are either bcrypt ($2…) — a non-FIPS-approved KDF that
// lives in golang.org/x/crypto (outside the module boundary and NOT rejected
// by GODEBUG=fips140=only) — or a bare unsalted SHA-256, which is not an
// acceptable token-hashing construction under a FIPS posture. The fips build
// therefore refuses to verify them and tells the operator to rotate the token
// (recreating it stores a PBKDF2 hash via hashToken).
//
// This file intentionally does NOT import golang.org/x/crypto/bcrypt, so the
// fips binary contains no Blowfish/bcrypt code at all — the CI symbol-absence
// check depends on this.
func (am *AuthManager) verifyLegacyTokenHash(token, hash string) bool {
	kind := "sha256"
	if strings.HasPrefix(hash, "$2") {
		kind = "bcrypt"
	}
	am.logger.Warn().
		Str("hash_kind", kind).
		Bool("fips_mode", true).
		Msg("rejecting legacy non-FIPS token hash; rotate this token (recreate it) to store a FIPS-approved PBKDF2 hash")
	return false
}
