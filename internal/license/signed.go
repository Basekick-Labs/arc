package license

import (
	"time"
)

// SignedLicense is the parsed form of the canonical JSON payload the
// activation server signs and Arc verifies via a detached signature.
//
// The field order, JSON tags, and types are kept aligned with the
// activation server's License struct (in arc-enterprise-activation-
// server's internal/license/license.go) — but unlike the previous
// embedded-signature design, divergence between the two sides no
// longer breaks signature verification. Signature verification is
// done against the RAW received bytes; this struct is only used
// AFTER verification, for parsing the known fields. Unknown fields
// the server may have added in a future version are silently
// ignored by json.Unmarshal.
//
// The Signature field has been REMOVED in this version: the
// signature now travels in a separate envelope field
// (response.license_signature), not embedded in the payload.
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
// ExpiresAt + the current time:
//   - ExpiresAt in the past   → "expired"
//   - ExpiresAt in the future → "active"
//
// We deliberately don't implement a grace period client-side: if the
// signed ExpiresAt is in the past, the license is expired. The server
// can grant a different status in its unsigned envelope (e.g. for
// billing-grace UX) but Arc's feature gates use the strict signed
// expiry to prevent trust escalation through the unsigned envelope.
func (s *SignedLicense) deriveStatus(now time.Time) (string, int) {
	if !s.ExpiresAt.After(now) {
		return "expired", 0
	}
	remaining := int(s.ExpiresAt.Sub(now).Hours() / 24)
	return "active", remaining
}
