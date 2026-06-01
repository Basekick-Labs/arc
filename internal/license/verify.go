package license

import (
	"crypto"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/rs/zerolog/log"
)

// noPublicKeyWarnOnce gates the "pinned key is nil" warning so a
// misconfigured build (placeholder PEM in pubkey.go) prints exactly
// one operator-visible line at first verify attempt, rather than
// spamming the logs on every 4-hour re-verification cycle.
var noPublicKeyWarnOnce sync.Once

// Verification errors. Exported so callers (and tests) can match on
// specific failure modes rather than parsing error strings.
var (
	// ErrNoLicenseFile is returned when the activation server response
	// did not include a license_file field (or it was empty).
	ErrNoLicenseFile = errors.New("license: response missing license_file")

	// ErrNoLicenseSignature is returned when license_file is present
	// but the separate license_signature field is missing or empty.
	// Older activation servers that don't emit detached signatures
	// trigger this; 26.06.2 onwards refuses to operate against such
	// servers.
	ErrNoLicenseSignature = errors.New("license: response missing license_signature")

	// ErrMalformedLicenseFile is returned when license_file is present
	// but not valid base64.
	ErrMalformedLicenseFile = errors.New("license: license_file is not valid base64")

	// ErrMalformedSignature is returned when license_signature is
	// present but not valid base64.
	ErrMalformedSignature = errors.New("license: license_signature is not valid base64")

	// ErrSignatureVerificationFailed is returned when the signature
	// is well-formed but does not verify against the pinned public
	// key over the license_file bytes. This is the failure mode
	// operators care about most: somebody tried to substitute a
	// forged license payload.
	ErrSignatureVerificationFailed = errors.New("license: signature verification failed")

	// ErrMalformedSignedLicense is returned when the decoded
	// license_file bytes are not parseable as a SignedLicense JSON
	// object. Only fires AFTER signature verification succeeds —
	// indicates a server-side bug (signed garbage) rather than an
	// attacker (an attacker who could produce a verifying signature
	// could just as easily produce verifying valid JSON).
	ErrMalformedSignedLicense = errors.New("license: decoded license_file is not a valid signed license")

	// ErrNoPublicKey is returned when the pinned public key was not
	// successfully parsed at package init. Indicates a build-time bug
	// (placeholder PEM left in pubkey.go, or malformed pinned key).
	ErrNoPublicKey = errors.New("license: pinned public key not loaded — build is misconfigured")
)

// VerifyLicenseFile validates a detached-signature license blob from
// the activation server and returns the parsed SignedLicense.
//
// The contract: licenseFileB64 is the base64-encoded canonical JSON
// payload the server signed, and signatureB64 is the base64-encoded
// RSA-PKCS1v15 SHA-256 signature over the DECODED licenseFileB64
// bytes (NOT over a re-marshaled struct). This means the bytes the
// server signed and the bytes the client verifies are bit-for-bit
// identical — server-side struct evolution (adding new fields) can
// never desynchronize the two sides.
//
// On success, returns a SignedLicense parsed from the verified bytes.
// Unknown fields in the JSON are silently dropped (Go json.Unmarshal
// default behavior) — that's the entire point of detached signatures:
// the bytes were signed, so we KNOW the server emitted them; parsing
// happens after, and any field the client doesn't know about is
// genuinely safe to ignore.
//
// On failure, returns one of the Err* sentinels (errors.Is-friendly).
func VerifyLicenseFile(licenseFileB64, signatureB64 string) (*SignedLicense, error) {
	if parsedEnterprisePublicKey == nil {
		noPublicKeyWarnOnce.Do(func() {
			log.Error().Msg("license: pinned enterprise public key did not load at startup — every license verification will fail; this is a build-time misconfiguration in internal/license/pubkey.go")
		})
		return nil, ErrNoPublicKey
	}
	if licenseFileB64 == "" {
		return nil, ErrNoLicenseFile
	}
	if signatureB64 == "" {
		return nil, ErrNoLicenseSignature
	}

	payload, err := base64.StdEncoding.DecodeString(licenseFileB64)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrMalformedLicenseFile, err)
	}

	signature, err := base64.StdEncoding.DecodeString(signatureB64)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrMalformedSignature, err)
	}

	// Verify the signature against the EXACT bytes we received. No
	// struct round-trip; nothing here parses the JSON first. This is
	// what makes the design forward-compatible.
	hash := sha256.Sum256(payload)
	if err := rsa.VerifyPKCS1v15(parsedEnterprisePublicKey, crypto.SHA256, hash[:], signature); err != nil {
		return nil, ErrSignatureVerificationFailed
	}

	// Signature is valid; now we can parse the payload. Any unknown
	// field the server added in a future version is dropped here,
	// but the signature already proved server intent over the
	// known + unknown fields together.
	signed, err := parseSignedLicense(payload)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrMalformedSignedLicense, err)
	}

	return signed, nil
}

// parseSignedLicense is a thin wrapper around json.Unmarshal so the
// error path in VerifyLicenseFile can wrap a known sentinel.
func parseSignedLicense(data []byte) (*SignedLicense, error) {
	var s SignedLicense
	if err := json.Unmarshal(data, &s); err != nil {
		return nil, err
	}
	return &s, nil
}
