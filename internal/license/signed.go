package license

import (
	"encoding/json"
	"fmt"
	"time"
)

// timeNow is the clock used for license-status derivation. Production
// callers should pass timeNow() to ToRuntimeLicense; tests overwrite
// this var to drive the expiry math deterministically (e.g. to verify
// that a SignedLicense with ExpiresAt in the past produces Status =
// "expired" regardless of what the unsigned envelope claimed).
var timeNow = func() time.Time { return time.Now().UTC() }

// SignedLicense is the wire-compatible mirror of the activation
// server's License struct (defined in arc-enterprise-activation-server's
// internal/license/license.go).
//
// The field order, JSON tags, omitempty markers, and types MUST match
// the server's struct exactly, because the signature is computed over
// the result of `json.Marshal(SignedLicense{Signature:""})` — any
// deviation produces different canonical bytes and the signature
// will not verify.
//
// This struct is intentionally separate from the runtime-facing
// License type in license.go: License represents what arc *uses*
// internally (computed fields like DaysRemaining, simplified subset);
// SignedLicense represents what the server *signs and sends* (exact
// wire shape including IssuedAt, MachineFingerprint, etc.). The
// verification path unwraps SignedLicense → License after checking
// the signature.
type SignedLicense struct {
	Version            int       `json:"version"`
	LicenseKey         string    `json:"license_key"`
	CustomerID         string    `json:"customer_id"`
	CustomerName       string    `json:"customer_name"`
	CustomerEmail      string    `json:"customer_email,omitempty"`
	Tier               Tier      `json:"tier"`
	MaxCores           int       `json:"max_cores"`
	MaxMachines        int       `json:"max_machines"`
	Features           []string  `json:"features"`
	MachineFingerprint string    `json:"machine_fingerprint,omitempty"`
	IssuedAt           time.Time `json:"issued_at"`
	ExpiresAt          time.Time `json:"expires_at"`
	Signature          string    `json:"signature,omitempty"`
}

// ToJSONForSigning returns the canonical JSON bytes of this
// SignedLicense with the Signature field zeroed out. These are the
// bytes the activation server signed and that we hash before
// verifying.
//
// Must match `(*License).ToJSONForSigning()` in the activation server
// byte-for-byte. The implementation deliberately mirrors the server:
// shallow copy, clear Signature, json.Marshal.
func (s *SignedLicense) ToJSONForSigning() ([]byte, error) {
	if s == nil {
		return nil, fmt.Errorf("signed license is nil")
	}
	copy := *s
	copy.Signature = ""
	return json.Marshal(copy)
}

// ToRuntimeLicense converts a verified SignedLicense into the runtime
// License type used by the rest of arc. Caller must have already
// verified the signature; this function does NOT re-verify.
//
// Status and DaysRemaining are derived locally from the signed
// ExpiresAt rather than trusted from the unsigned envelope: an
// attacker controlling the response could otherwise replay a
// previously-signed blob (legitimately expired today) alongside an
// envelope claiming "active". The signed ExpiresAt is the only
// trustworthy expiry signal.
//
// `now` is passed in so tests can drive the computation; callers
// outside tests pass time.Now().UTC().
func (s *SignedLicense) ToRuntimeLicense(now time.Time) *License {
	status, daysRemaining := s.deriveStatus(now)
	return &License{
		LicenseKey:    s.LicenseKey,
		CustomerID:    s.CustomerID,
		CustomerName:  s.CustomerName,
		Tier:          s.Tier,
		MaxCores:      s.MaxCores,
		MaxMachines:   s.MaxMachines,
		Features:      s.Features,
		ExpiresAt:     s.ExpiresAt,
		Status:        status,
		DaysRemaining: daysRemaining,
	}
}

// deriveStatus computes Status + DaysRemaining from the signed
// ExpiresAt + the current time. Matches the server-side mapping:
//   - expired_at in the past         → "expired"
//   - expired_at in the past 30 days → "grace_period"  (deprecated; server now returns "expired" here too)
//   - expired_at in the future       → "active"
//
// We deliberately don't implement a grace period client-side: if the
// signed ExpiresAt is in the past, the license is expired. The server
// can still grant a different status in its unsigned envelope (e.g.
// for billing-grace UX) but Arc's feature gates use the strict signed
// expiry to avoid trust escalation through the unsigned envelope.
func (s *SignedLicense) deriveStatus(now time.Time) (string, int) {
	remaining := int(s.ExpiresAt.Sub(now).Hours() / 24)
	if !s.ExpiresAt.After(now) {
		return "expired", 0
	}
	return "active", remaining
}
