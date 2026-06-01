package license

import (
	"crypto"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
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
	// did not include a license_file field (or it was empty). Older
	// activation servers that don't emit signed blobs trigger this;
	// 26.06.2 onwards refuses to operate against such servers.
	ErrNoLicenseFile = errors.New("license: response missing license_file")

	// ErrMalformedLicenseFile is returned when license_file is present
	// but not valid base64.
	ErrMalformedLicenseFile = errors.New("license: license_file is not valid base64")

	// ErrMalformedSignedLicense is returned when the decoded license
	// file is not parseable as a SignedLicense JSON object.
	ErrMalformedSignedLicense = errors.New("license: decoded license_file is not a valid signed license")

	// ErrMissingSignature is returned when the SignedLicense parses
	// but the signature field is empty.
	ErrMissingSignature = errors.New("license: signed license has no signature")

	// ErrMalformedSignature is returned when the signature field is
	// present but not valid base64.
	ErrMalformedSignature = errors.New("license: signature is not valid base64")

	// ErrSignatureVerificationFailed is returned when the signature
	// is well-formed but does not verify against the pinned public
	// key. This is the failure mode operators care about most:
	// somebody tried to substitute a forged license blob.
	ErrSignatureVerificationFailed = errors.New("license: signature verification failed")

	// ErrNoPublicKey is returned when the pinned public key was not
	// successfully parsed at package init. Indicates a build-time bug
	// (placeholder PEM left in pubkey.go, or malformed pinned key).
	ErrNoPublicKey = errors.New("license: pinned public key not loaded — build is misconfigured")
)

// VerifyLicenseFile decodes, parses, and signature-verifies the
// base64-encoded "license_file" field returned by the activation
// server. On success, returns the parsed SignedLicense whose fields
// are trustworthy. On any failure, returns one of the Err* sentinels
// above wrapped with context (or %w-wrapped).
//
// The signature is verified against the pinned public key in
// pubkey.go. Callers MUST NOT use a SignedLicense returned from any
// other code path for feature-gating decisions.
func VerifyLicenseFile(licenseFile string) (*SignedLicense, error) {
	if parsedEnterprisePublicKey == nil {
		// Loud-but-once operator warning: the pinned PEM in
		// pubkey.go failed to parse at package init, which is a
		// build-time misconfiguration (placeholder left in, key
		// pasted with whitespace damage, etc.). Print one
		// recognizable line so the operator can grep for it.
		noPublicKeyWarnOnce.Do(func() {
			log.Println("license: pinned enterprise public key did not load at startup — every license verification will fail; this is a build-time misconfiguration in internal/license/pubkey.go")
		})
		return nil, ErrNoPublicKey
	}
	if licenseFile == "" {
		return nil, ErrNoLicenseFile
	}

	rawJSON, err := base64.StdEncoding.DecodeString(licenseFile)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrMalformedLicenseFile, err)
	}

	signed, err := parseSignedLicense(rawJSON)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrMalformedSignedLicense, err)
	}
	if signed.Signature == "" {
		return nil, ErrMissingSignature
	}

	signature, err := base64.StdEncoding.DecodeString(signed.Signature)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrMalformedSignature, err)
	}

	// Re-compute the canonical bytes the server signed: same struct
	// with the Signature field zeroed, json.Marshal applied.
	canonical, err := signed.ToJSONForSigning()
	if err != nil {
		// Should never happen — encoding/json doesn't fail on this shape.
		return nil, fmt.Errorf("license: marshal canonical bytes: %w", err)
	}

	hash := sha256.Sum256(canonical)
	if err := rsa.VerifyPKCS1v15(parsedEnterprisePublicKey, crypto.SHA256, hash[:], signature); err != nil {
		return nil, ErrSignatureVerificationFailed
	}

	return signed, nil
}

// parseSignedLicense is a thin wrapper around json.Unmarshal so the
// error path in VerifyLicenseFile can wrap a known sentinel. Split
// into its own function for symmetry with the server's FromJSON.
func parseSignedLicense(data []byte) (*SignedLicense, error) {
	var s SignedLicense
	if err := json.Unmarshal(data, &s); err != nil {
		return nil, err
	}
	return &s, nil
}
