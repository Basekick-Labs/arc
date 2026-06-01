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
// the duration of a test. Returns a cleanup function via t.Cleanup so
// no test can leak its pinned key into a subsequent test.
func withTestPublicKey(t *testing.T, pubKey *rsa.PublicKey) {
	t.Helper()
	original := parsedEnterprisePublicKey
	parsedEnterprisePublicKey = pubKey
	t.Cleanup(func() {
		parsedEnterprisePublicKey = original
	})
}

// signLicense produces a license_file string the server would have
// emitted: builds the SignedLicense, signs the canonical bytes, sets
// the Signature field, marshals + base64-encodes.
func signLicense(t *testing.T, priv *rsa.PrivateKey, lic *SignedLicense) string {
	t.Helper()
	// Capture the canonical bytes (signature-cleared) the server signs.
	canonical, err := lic.ToJSONForSigning()
	if err != nil {
		t.Fatalf("ToJSONForSigning: %v", err)
	}
	hash := sha256.Sum256(canonical)
	signature, err := rsa.SignPKCS1v15(rand.Reader, priv, crypto.SHA256, hash[:])
	if err != nil {
		t.Fatalf("SignPKCS1v15: %v", err)
	}
	lic.Signature = base64.StdEncoding.EncodeToString(signature)

	signedJSON, err := json.Marshal(lic)
	if err != nil {
		t.Fatalf("marshal signed: %v", err)
	}
	return base64.StdEncoding.EncodeToString(signedJSON)
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
	licenseFile := signLicense(t, rsaKey1, lic)

	got, err := VerifyLicenseFile(licenseFile)
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
// modification: the signature was valid for the original blob, but a
// byte of the blob has been flipped after signing, so verification
// must fail. This is the "attacker upgrades their tier from starter
// to enterprise without re-signing" attack.
func TestVerifyLicenseFile_TamperedPayload(t *testing.T) {
	withTestPublicKey(t, &rsaKey1.PublicKey)
	lic := sampleLicense()
	licenseFile := signLicense(t, rsaKey1, lic)

	// Decode, mutate the License, re-encode WITHOUT re-signing.
	rawJSON, err := base64.StdEncoding.DecodeString(licenseFile)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	var s SignedLicense
	if err := json.Unmarshal(rawJSON, &s); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	// Upgrade ourselves from enterprise to unlimited tier.
	s.Tier = "unlimited"
	tamperedJSON, _ := json.Marshal(s)
	tamperedFile := base64.StdEncoding.EncodeToString(tamperedJSON)

	_, err = VerifyLicenseFile(tamperedFile)
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
	licenseFile := signLicense(t, rsaKey1, lic)

	rawJSON, _ := base64.StdEncoding.DecodeString(licenseFile)
	var s SignedLicense
	_ = json.Unmarshal(rawJSON, &s)
	// Flip a byte of the signature (decode → mutate → re-encode).
	sigBytes, _ := base64.StdEncoding.DecodeString(s.Signature)
	sigBytes[0] ^= 0xFF
	s.Signature = base64.StdEncoding.EncodeToString(sigBytes)
	tamperedJSON, _ := json.Marshal(s)
	tamperedFile := base64.StdEncoding.EncodeToString(tamperedJSON)

	_, err := VerifyLicenseFile(tamperedFile)
	if err == nil {
		t.Fatal("expected verification failure, got nil")
	}
	if !errors.Is(err, ErrSignatureVerificationFailed) {
		t.Errorf("expected ErrSignatureVerificationFailed, got %v", err)
	}
}

// TestVerifyLicenseFile_WrongKey pins detection of signature-from-
// another-key attempts: the blob is signed by rsaKey2 but the pinned
// trust root is rsaKey1. The attacker has full signing capability
// with their own key but can't forge against the pinned one.
func TestVerifyLicenseFile_WrongKey(t *testing.T) {
	withTestPublicKey(t, &rsaKey1.PublicKey)
	lic := sampleLicense()
	licenseFile := signLicense(t, rsaKey2, lic) // signed by rsaKey2!

	_, err := VerifyLicenseFile(licenseFile)
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
	_, err := VerifyLicenseFile("")
	if !errors.Is(err, ErrNoLicenseFile) {
		t.Errorf("expected ErrNoLicenseFile, got %v", err)
	}
}

// TestVerifyLicenseFile_MalformedBase64 pins fail-closed on garbage
// in license_file. An attacker substituting `!!!notbase64!!!` should
// not panic or trip a CPU loop — just fail cleanly.
func TestVerifyLicenseFile_MalformedBase64(t *testing.T) {
	withTestPublicKey(t, &rsaKey1.PublicKey)
	_, err := VerifyLicenseFile("!!!notbase64!!!")
	if !errors.Is(err, ErrMalformedLicenseFile) {
		t.Errorf("expected ErrMalformedLicenseFile, got %v", err)
	}
}

// TestVerifyLicenseFile_MalformedSignedLicense pins fail-closed on
// valid-base64-but-not-a-SignedLicense JSON. Could happen if the
// server's struct shape drifts and we decode an older arc against a
// newer-shaped response.
func TestVerifyLicenseFile_MalformedSignedLicense(t *testing.T) {
	withTestPublicKey(t, &rsaKey1.PublicKey)
	junk := base64.StdEncoding.EncodeToString([]byte("this is valid base64 but not json"))
	_, err := VerifyLicenseFile(junk)
	if !errors.Is(err, ErrMalformedSignedLicense) {
		t.Errorf("expected ErrMalformedSignedLicense, got %v", err)
	}
}

// TestVerifyLicenseFile_MissingSignature pins fail-closed when the
// SignedLicense JSON is valid but has no signature field. The server
// should never emit this, but if a bug or middleware strips it, we
// fail closed rather than accepting an unsigned payload.
func TestVerifyLicenseFile_MissingSignature(t *testing.T) {
	withTestPublicKey(t, &rsaKey1.PublicKey)
	lic := sampleLicense()
	// Build the blob WITHOUT signing — Signature stays empty.
	signedJSON, _ := json.Marshal(lic)
	noSigFile := base64.StdEncoding.EncodeToString(signedJSON)

	_, err := VerifyLicenseFile(noSigFile)
	if !errors.Is(err, ErrMissingSignature) {
		t.Errorf("expected ErrMissingSignature, got %v", err)
	}
}

// TestBindFingerprint_AllowsEmptyFingerprintInSignedBlob pins the
// "signed blob has no fingerprint" path: some sign flows omit it
// (e.g. offline licenses). The client must not reject those — only
// non-empty mismatches.
func TestBindFingerprint_AllowsEmptyFingerprintInSignedBlob(t *testing.T) {
	c := &Client{fingerprint: "sha256:abc"}
	signed := &SignedLicense{MachineFingerprint: ""} // omitted
	if err := c.bindFingerprint(signed); err != nil {
		t.Errorf("expected nil error for empty fingerprint, got %v", err)
	}
}

// TestBindFingerprint_AcceptsMatchingFingerprint covers the happy path.
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
// "MitM intercepts customer-A's blob and replays it on customer-B's
// machine" attack that the original matrix didn't cover.
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

// TestEnterprisePublicKey_PinnedAtBuildTime confirms the production
// public key embedded in pubkey.go parses correctly at package init.
// If this test fails the binary is misconfigured and every Enterprise
// activation will fail with ErrNoPublicKey at runtime — this catches
// that at build time instead.
func TestEnterprisePublicKey_PinnedAtBuildTime(t *testing.T) {
	if parsedEnterprisePublicKey == nil {
		t.Fatal("parsedEnterprisePublicKey is nil — pubkey.go contains a placeholder or malformed PEM; production builds will reject every license")
	}
	// Sanity-check the key size — anything not RSA-2048 indicates
	// the wrong key was pasted (e.g. a 1024-bit test key).
	bits := parsedEnterprisePublicKey.N.BitLen()
	if bits != 2048 {
		t.Errorf("pinned key bit-size = %d, want 2048 (RSA-2048 is the contract with the activation server)", bits)
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
	licenseFile := signLicense(t, rsaKey1, lic)
	_, err := VerifyLicenseFile(licenseFile)
	if !errors.Is(err, ErrNoPublicKey) {
		t.Errorf("expected ErrNoPublicKey, got %v", err)
	}
}

// TestParseRSAPublicKey_ValidPEM confirms the PEM parsing path works
// against a freshly-generated key. Guards against future PEM-format
// regressions if x509 conventions change.
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
	// Signed yesterday; expired yesterday (per the signed blob).
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
	now := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC) // a year before expiry

	runtime := lic.ToRuntimeLicense(now)
	if runtime.Status != "active" {
		t.Errorf("Status = %q, want 'active'", runtime.Status)
	}
	if runtime.DaysRemaining < 360 || runtime.DaysRemaining > 366 {
		t.Errorf("DaysRemaining = %d, want ~365", runtime.DaysRemaining)
	}
}

// TestVerifyLicenseFile_TolerantToUnknownFields documents (and pins)
// that unknown JSON fields injected into the license_file are silently
// ignored by Go's default json.Unmarshal — they don't reach the
// SignedLicense struct, they're not part of the canonical bytes we
// recompute, and the signature still verifies. This is the correct
// behavior for forward compatibility:
//
//   - The server can add new fields in a future version without
//     breaking existing arc clients (the future fields are dropped on
//     unmarshal; canonical bytes match the older arc's struct shape).
//   - An attacker injecting fields gains nothing: the verifier never
//     reads those fields, so they're not a vector for tier-elevation
//     or feature-flag tampering. Only modifications to fields the
//     verifier actually decodes can affect runtime gating, and those
//     break the signature.
//
// This test exists to document the trust boundary, not to test a
// failure case. If a future change tightens this (DisallowUnknownFields,
// say), this test will start failing and the contract should be
// re-thought before relaxing it back.
func TestVerifyLicenseFile_TolerantToUnknownFields(t *testing.T) {
	withTestPublicKey(t, &rsaKey1.PublicKey)

	lic := sampleLicense()
	licenseFile := signLicense(t, rsaKey1, lic)

	// Inject an unknown field into the JSON without re-signing.
	rawJSON, _ := base64.StdEncoding.DecodeString(licenseFile)
	withExtra := strings.Replace(string(rawJSON), `"signature":`, `"future_field":"new_value","signature":`, 1)
	tamperedFile := base64.StdEncoding.EncodeToString([]byte(withExtra))

	got, err := VerifyLicenseFile(tamperedFile)
	if err != nil {
		t.Errorf("expected verification to succeed (unknown fields are dropped, canonical bytes match), got %v", err)
	}
	// Confirm the injected field had no effect on the decoded license.
	if got.LicenseKey != lic.LicenseKey {
		t.Errorf("LicenseKey changed: got %q, want %q", got.LicenseKey, lic.LicenseKey)
	}
	if got.Tier != lic.Tier {
		t.Errorf("Tier changed: got %q, want %q", got.Tier, lic.Tier)
	}
}
