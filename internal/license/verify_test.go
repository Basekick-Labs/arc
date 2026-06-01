package license

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"strings"
	"testing"
	"time"
)

// testRSAKey is a fresh RSA-2048 keypair generated per test run. The
// public key is wired into `parsedEnterprisePublicKey` for the lifetime
// of each test via withTestPublicKey. Tests that need a different key
// (wrong-key reject) use rsaKey2.
//
// Generated at package-test init time so all tests share one keypair
// — RSA-2048 generation is ~50ms, acceptable as a one-time cost.
var (
	rsaKey1 *rsa.PrivateKey
	rsaKey2 *rsa.PrivateKey
)

func init() {
	var err error
	rsaKey1, err = rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		panic("test setup: failed to generate rsaKey1: " + err.Error())
	}
	rsaKey2, err = rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		panic("test setup: failed to generate rsaKey2: " + err.Error())
	}
}

// withTestPublicKey temporarily replaces parsedEnterprisePublicKey for
// the duration of a test. t.Cleanup ensures no test leaks its pinned
// key into subsequent tests.
func withTestPublicKey(t *testing.T, pubKey *rsa.PublicKey) {
	t.Helper()
	original := parsedEnterprisePublicKey
	parsedEnterprisePublicKey = pubKey
	t.Cleanup(func() {
		parsedEnterprisePublicKey = original
	})
}

// signDetached produces the (licenseFileB64, signatureB64) pair the
// activation server would have emitted for a given SignedLicense.
// Mirrors Signer.SignDetached on the server side.
func signDetached(t *testing.T, priv *rsa.PrivateKey, lic *SignedLicense) (licenseFileB64, signatureB64 string) {
	t.Helper()
	payload, err := json.Marshal(lic)
	if err != nil {
		t.Fatalf("marshal license: %v", err)
	}
	hash := sha256.Sum256(payload)
	signature, err := rsa.SignPKCS1v15(rand.Reader, priv, crypto.SHA256, hash[:])
	if err != nil {
		t.Fatalf("SignPKCS1v15: %v", err)
	}
	return base64.StdEncoding.EncodeToString(payload), base64.StdEncoding.EncodeToString(signature)
}

// signDetachedRaw signs an arbitrary byte slice (NOT necessarily a
// SignedLicense JSON). Used by the forward-compatibility test to
// simulate a future server adding fields the current client doesn't
// know about.
func signDetachedRaw(t *testing.T, priv *rsa.PrivateKey, payload []byte) (licenseFileB64, signatureB64 string) {
	t.Helper()
	hash := sha256.Sum256(payload)
	signature, err := rsa.SignPKCS1v15(rand.Reader, priv, crypto.SHA256, hash[:])
	if err != nil {
		t.Fatalf("SignPKCS1v15: %v", err)
	}
	return base64.StdEncoding.EncodeToString(payload), base64.StdEncoding.EncodeToString(signature)
}

func sampleLicense() *SignedLicense {
	return &SignedLicense{
		Version:       1,
		LicenseKey:    "ARC-ENT-TEST-0000",
		CustomerID:    "cust-test",
		CustomerName:  "Test Customer",
		CustomerEmail: "test@example.com",
		Tier:          "enterprise",
		MaxCores:      32,
		MaxMachines:   4,
		Features:      []string{"tiered_storage", "clustering", "audit_logging"},
		IssuedAt:      time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC),
		ExpiresAt:     time.Date(2027, 6, 1, 0, 0, 0, 0, time.UTC),
	}
}

// TestVerifyLicenseFile_GoldenPath pins the happy path: a license
// signed by the trusted key with the trusted public key pinned
// returns the SignedLicense intact and verifies the signature.
func TestVerifyLicenseFile_GoldenPath(t *testing.T) {
	withTestPublicKey(t, &rsaKey1.PublicKey)
	lic := sampleLicense()
	licenseFile, signature := signDetached(t, rsaKey1, lic)

	got, err := VerifyLicenseFile(licenseFile, signature)
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if got.LicenseKey != lic.LicenseKey {
		t.Errorf("LicenseKey = %q, want %q", got.LicenseKey, lic.LicenseKey)
	}
	if got.Tier != lic.Tier {
		t.Errorf("Tier = %q, want %q", got.Tier, lic.Tier)
	}
	if got.MaxCores != lic.MaxCores {
		t.Errorf("MaxCores = %d, want %d", got.MaxCores, lic.MaxCores)
	}
	if len(got.Features) != len(lic.Features) {
		t.Errorf("Features len = %d, want %d", len(got.Features), len(lic.Features))
	}
}

// TestVerifyLicenseFile_TamperedPayload pins detection of payload
// modification: the signature was valid for the original payload
// bytes, but a byte has been flipped after signing, so verification
// must fail.
func TestVerifyLicenseFile_TamperedPayload(t *testing.T) {
	withTestPublicKey(t, &rsaKey1.PublicKey)
	lic := sampleLicense()
	licenseFile, signature := signDetached(t, rsaKey1, lic)

	// Mutate the encoded payload (flip a byte in the decoded bytes,
	// then re-encode to base64).
	payload, _ := base64.StdEncoding.DecodeString(licenseFile)
	payload[len(payload)/2] ^= 0xFF
	tamperedFile := base64.StdEncoding.EncodeToString(payload)

	_, err := VerifyLicenseFile(tamperedFile, signature)
	if err == nil {
		t.Fatal("expected verification failure, got nil")
	}
	if !errors.Is(err, ErrSignatureVerificationFailed) {
		t.Errorf("expected ErrSignatureVerificationFailed, got %v", err)
	}
}

// TestVerifyLicenseFile_TamperedSignature pins detection of signature
// modification: payload is intact but the signature bytes have been
// flipped. The signature is well-formed base64 but won't verify.
func TestVerifyLicenseFile_TamperedSignature(t *testing.T) {
	withTestPublicKey(t, &rsaKey1.PublicKey)
	lic := sampleLicense()
	licenseFile, signature := signDetached(t, rsaKey1, lic)

	sigBytes, _ := base64.StdEncoding.DecodeString(signature)
	sigBytes[0] ^= 0xFF
	tamperedSig := base64.StdEncoding.EncodeToString(sigBytes)

	_, err := VerifyLicenseFile(licenseFile, tamperedSig)
	if err == nil {
		t.Fatal("expected verification failure, got nil")
	}
	if !errors.Is(err, ErrSignatureVerificationFailed) {
		t.Errorf("expected ErrSignatureVerificationFailed, got %v", err)
	}
}

// TestVerifyLicenseFile_WrongKey pins detection of signature-from-
// another-key attempts: the blob is signed by rsaKey2 but the pinned
// trust root is rsaKey1.
func TestVerifyLicenseFile_WrongKey(t *testing.T) {
	withTestPublicKey(t, &rsaKey1.PublicKey)
	lic := sampleLicense()
	licenseFile, signature := signDetached(t, rsaKey2, lic) // signed by rsaKey2!

	_, err := VerifyLicenseFile(licenseFile, signature)
	if err == nil {
		t.Fatal("expected verification failure, got nil")
	}
	if !errors.Is(err, ErrSignatureVerificationFailed) {
		t.Errorf("expected ErrSignatureVerificationFailed, got %v", err)
	}
}

// TestVerifyLicenseFile_MissingLicenseFile pins fail-closed on the
// empty-license-file case (older server, or stripped response).
func TestVerifyLicenseFile_MissingLicenseFile(t *testing.T) {
	withTestPublicKey(t, &rsaKey1.PublicKey)
	_, err := VerifyLicenseFile("", "some-sig")
	if !errors.Is(err, ErrNoLicenseFile) {
		t.Errorf("expected ErrNoLicenseFile, got %v", err)
	}
}

// TestVerifyLicenseFile_MissingSignature pins fail-closed when
// license_file is present but license_signature is missing.
func TestVerifyLicenseFile_MissingSignature(t *testing.T) {
	withTestPublicKey(t, &rsaKey1.PublicKey)
	lic := sampleLicense()
	licenseFile, _ := signDetached(t, rsaKey1, lic)
	_, err := VerifyLicenseFile(licenseFile, "")
	if !errors.Is(err, ErrNoLicenseSignature) {
		t.Errorf("expected ErrNoLicenseSignature, got %v", err)
	}
}

// TestVerifyLicenseFile_MalformedBase64File pins fail-closed on
// garbage in license_file.
func TestVerifyLicenseFile_MalformedBase64File(t *testing.T) {
	withTestPublicKey(t, &rsaKey1.PublicKey)
	_, err := VerifyLicenseFile("!!!notbase64!!!", "validplaceholderbase64==")
	if !errors.Is(err, ErrMalformedLicenseFile) {
		t.Errorf("expected ErrMalformedLicenseFile, got %v", err)
	}
}

// TestVerifyLicenseFile_MalformedBase64Signature pins fail-closed on
// garbage in license_signature.
func TestVerifyLicenseFile_MalformedBase64Signature(t *testing.T) {
	withTestPublicKey(t, &rsaKey1.PublicKey)
	lic := sampleLicense()
	licenseFile, _ := signDetached(t, rsaKey1, lic)
	_, err := VerifyLicenseFile(licenseFile, "!!!notbase64!!!")
	if !errors.Is(err, ErrMalformedSignature) {
		t.Errorf("expected ErrMalformedSignature, got %v", err)
	}
}

// TestVerifyLicenseFile_MalformedSignedLicense pins fail-closed when
// the signature verifies but the payload isn't valid SignedLicense
// JSON. This is the "server-side bug signed garbage" case — an
// attacker who could produce a verifying signature could also
// produce valid JSON, so this isn't a security boundary, just a
// safety net.
func TestVerifyLicenseFile_MalformedSignedLicense(t *testing.T) {
	withTestPublicKey(t, &rsaKey1.PublicKey)
	// Sign arbitrary bytes that aren't a JSON object.
	licenseFile, signature := signDetachedRaw(t, rsaKey1, []byte("this is not json"))

	_, err := VerifyLicenseFile(licenseFile, signature)
	if !errors.Is(err, ErrMalformedSignedLicense) {
		t.Errorf("expected ErrMalformedSignedLicense, got %v", err)
	}
}

// TestVerifyLicenseFile_NoPublicKey pins fail-closed when the build
// has a placeholder/malformed pinned key (parsedEnterprisePublicKey
// is nil). This is the build-time misconfiguration safety net.
func TestVerifyLicenseFile_NoPublicKey(t *testing.T) {
	original := parsedEnterprisePublicKey
	parsedEnterprisePublicKey = nil
	t.Cleanup(func() { parsedEnterprisePublicKey = original })

	lic := sampleLicense()
	licenseFile, signature := signDetached(t, rsaKey1, lic)
	_, err := VerifyLicenseFile(licenseFile, signature)
	if !errors.Is(err, ErrNoPublicKey) {
		t.Errorf("expected ErrNoPublicKey, got %v", err)
	}
}

// TestVerifyLicenseFile_FutureServerAddsFields_StillVerifies is the
// flagship test for the JWS-style detached-signature design. It
// simulates the scenario Gemini flagged in PR #473 as a critical
// flaw against the previous embedded-signature design:
//
//  1. A future activation server adds new fields to the License
//     struct (here: "new_feature_x" and "max_widgets") that the
//     current arc client doesn't know about.
//  2. The future server signs the full payload INCLUDING the new
//     fields.
//  3. The current arc client receives the payload + signature.
//  4. Verification MUST succeed: the signature is over the raw
//     bytes the client received, and the verifier never tries to
//     re-marshal a parsed struct.
//  5. After verification, the client parses what it knows; the
//     unknown fields are silently dropped by json.Unmarshal (Go's
//     default behavior). The known fields come through correctly.
//
// This test passing is the entire reason for the detached-signature
// refactor: it lets the server evolve the License struct without
// breaking every deployed older arc binary.
func TestVerifyLicenseFile_FutureServerAddsFields_StillVerifies(t *testing.T) {
	withTestPublicKey(t, &rsaKey1.PublicKey)

	// Future server's payload: includes fields the current client
	// doesn't know about. Sign the EXACT bytes the future server
	// would emit — that's the contract.
	futureLicensePayload := []byte(`{
		"version": 2,
		"license_key": "ARC-ENT-FUTURE-0000",
		"customer_id": "cust-future",
		"customer_name": "Future Customer",
		"tier": "enterprise",
		"max_cores": 64,
		"max_machines": 8,
		"features": ["tiered_storage", "clustering"],
		"new_feature_x": true,
		"max_widgets": 99,
		"issued_at": "2027-06-01T00:00:00Z",
		"expires_at": "2028-06-01T00:00:00Z"
	}`)
	licenseFile, signature := signDetachedRaw(t, rsaKey1, futureLicensePayload)

	got, err := VerifyLicenseFile(licenseFile, signature)
	if err != nil {
		t.Fatalf("verification should succeed against future-server payload, got %v", err)
	}

	// Known fields come through correctly.
	if got.LicenseKey != "ARC-ENT-FUTURE-0000" {
		t.Errorf("LicenseKey = %q, want ARC-ENT-FUTURE-0000", got.LicenseKey)
	}
	if got.Tier != "enterprise" {
		t.Errorf("Tier = %q, want enterprise", got.Tier)
	}
	if got.MaxCores != 64 {
		t.Errorf("MaxCores = %d, want 64", got.MaxCores)
	}
	if got.Version != 2 {
		t.Errorf("Version = %d, want 2", got.Version)
	}
}

// TestVerifyLicenseFile_PayloadWhitespaceMatters confirms that the
// signature is over the EXACT bytes received, not over some
// normalized form. If a proxy reformats the JSON (re-indents,
// changes field order), the signature will not verify. This is the
// correct behavior: the server signed specific bytes, and any
// modification — even semantically-equivalent whitespace changes —
// must invalidate the signature.
func TestVerifyLicenseFile_PayloadWhitespaceMatters(t *testing.T) {
	withTestPublicKey(t, &rsaKey1.PublicKey)
	// Sign one specific byte sequence; the original encoded payload
	// is intentionally discarded — this test verifies that a
	// whitespace-different (semantically-identical) payload paired
	// with the original signature fails verification.
	original := []byte(`{"version":1,"license_key":"X"}`)
	_, signature := signDetachedRaw(t, rsaKey1, original)

	// Re-encode with different whitespace — semantically identical,
	// byte-different.
	reformatted := base64.StdEncoding.EncodeToString([]byte(`{ "version": 1, "license_key": "X" }`))

	_, err := VerifyLicenseFile(reformatted, signature)
	if !errors.Is(err, ErrSignatureVerificationFailed) {
		t.Errorf("expected ErrSignatureVerificationFailed for whitespace-altered payload, got %v", err)
	}
}

// TestEnterprisePublicKey_PinnedAtBuildTime confirms the production
// public key embedded in pubkey.go parses correctly at package init.
// If this test fails the binary is misconfigured and every Enterprise
// activation will fail with ErrNoPublicKey at runtime.
func TestEnterprisePublicKey_PinnedAtBuildTime(t *testing.T) {
	if parsedEnterprisePublicKey == nil {
		t.Fatal("parsedEnterprisePublicKey is nil — pubkey.go contains a placeholder or malformed PEM; production builds will reject every license")
	}
	bits := parsedEnterprisePublicKey.N.BitLen()
	if bits != 2048 {
		t.Errorf("pinned key bit-size = %d, want 2048", bits)
	}
}

// TestParseRSAPublicKey_ValidPEM confirms the PEM parsing path works
// against a freshly-generated key.
func TestParseRSAPublicKey_ValidPEM(t *testing.T) {
	pubKeyBytes, err := x509.MarshalPKIXPublicKey(&rsaKey1.PublicKey)
	if err != nil {
		t.Fatalf("marshal pub: %v", err)
	}
	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: pubKeyBytes})

	parsed, err := parseRSAPublicKey(pemBytes)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if parsed.N.Cmp(rsaKey1.PublicKey.N) != 0 {
		t.Error("parsed N does not match original")
	}
}

func TestParseRSAPublicKey_MalformedPEM(t *testing.T) {
	cases := []struct {
		name string
		pem  string
	}{
		{"empty", ""},
		{"not_pem", "this is not a PEM block"},
		{"truncated", "-----BEGIN PUBLIC KEY-----\n"},
		{"wrong_type", "-----BEGIN PRIVATE KEY-----\nMIIEvwIBAD\n-----END PRIVATE KEY-----"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parseRSAPublicKey([]byte(tc.pem))
			if err == nil {
				t.Errorf("expected error for %q, got nil", tc.name)
			}
		})
	}
}

// TestSignedLicense_ToRuntimeLicense_DerivesStatusFromSignedExpiry
// pins the trust-boundary contract: Status is derived from the signed
// ExpiresAt, not trusted from the unsigned envelope. An attacker
// replaying a yesterday-signed blob alongside a "status: active"
// envelope today must NOT result in an active runtime License.
func TestSignedLicense_ToRuntimeLicense_DerivesStatusFromSignedExpiry(t *testing.T) {
	lic := &SignedLicense{
		ExpiresAt: time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC),
		Tier:      "enterprise",
	}
	now := time.Date(2026, 6, 2, 0, 0, 0, 0, time.UTC) // one day after expiry

	runtime := lic.ToRuntimeLicense(now)
	if runtime.Status != "expired" {
		t.Errorf("Status = %q, want 'expired' (signed ExpiresAt is in the past)", runtime.Status)
	}
	if runtime.DaysRemaining != 0 {
		t.Errorf("DaysRemaining = %d, want 0", runtime.DaysRemaining)
	}
}

func TestSignedLicense_ToRuntimeLicense_ActiveWhenNotExpired(t *testing.T) {
	lic := &SignedLicense{
		ExpiresAt: time.Date(2027, 6, 1, 0, 0, 0, 0, time.UTC),
		Tier:      "enterprise",
	}
	now := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)

	runtime := lic.ToRuntimeLicense(now)
	if runtime.Status != "active" {
		t.Errorf("Status = %q, want 'active'", runtime.Status)
	}
	if runtime.DaysRemaining < 360 || runtime.DaysRemaining > 366 {
		t.Errorf("DaysRemaining = %d, want ~365", runtime.DaysRemaining)
	}
}

// TestBindFingerprint_AllowsEmptyFingerprintInSignedBlob pins the
// "signed blob has no fingerprint" path: some sign flows omit it
// (e.g. offline licenses). The client must not reject those — only
// non-empty mismatches.
func TestBindFingerprint_AllowsEmptyFingerprintInSignedBlob(t *testing.T) {
	c := &Client{fingerprint: "sha256:abc"}
	signed := &SignedLicense{MachineFingerprint: ""}
	if err := c.bindFingerprint(signed); err != nil {
		t.Errorf("expected nil error for empty fingerprint, got %v", err)
	}
}

func TestBindFingerprint_AcceptsMatchingFingerprint(t *testing.T) {
	c := &Client{fingerprint: "sha256:abc123"}
	signed := &SignedLicense{MachineFingerprint: "sha256:abc123"}
	if err := c.bindFingerprint(signed); err != nil {
		t.Errorf("expected nil error for matching fingerprint, got %v", err)
	}
}

// TestBindFingerprint_RejectsMismatch pins the replay-defense: a
// signed blob bound to a different machine's fingerprint must be
// rejected even though the RSA signature verifies. Closes the
// "replay customer-A's blob on customer-B's machine" attack.
func TestBindFingerprint_RejectsMismatch(t *testing.T) {
	c := &Client{fingerprint: "sha256:thismachine"}
	signed := &SignedLicense{MachineFingerprint: "sha256:differentmachine"}
	err := c.bindFingerprint(signed)
	if err == nil {
		t.Fatal("expected error for fingerprint mismatch, got nil")
	}
	if !strings.Contains(err.Error(), "does not match this machine") {
		t.Errorf("error %q does not mention fingerprint mismatch", err)
	}
}
