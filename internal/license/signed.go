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
	Policy             *ExpiryPolicy `json:"expiry_policy,omitempty"`
	Version            int           `json:"version"`
	LicenseKey         string        `json:"license_key"`
	CustomerID         string        `json:"customer_id"`
	CustomerName       string        `json:"customer_name"`
	CustomerEmail      string        `json:"customer_email,omitempty"`
	Tier               Tier          `json:"tier"`
	MaxCores           int           `json:"max_cores"`
	MaxMachines        int           `json:"max_machines"`
	Features           []string      `json:"features"`
	MachineFingerprint string        `json:"machine_fingerprint,omitempty"`
	IssuedAt           time.Time     `json:"issued_at"`
	ExpiresAt          time.Time     `json:"expires_at"`
}

// ToRuntimeLicense converts a verified SignedLicense into the runtime
// License type used by the rest of arc. Caller must have already
// verified the signature; this function does NOT re-verify.
//
// Status and DaysRemaining are derived from the signed policy at the supplied
// evaluation time, never from unsigned envelope status. Online callers supply
// the verified authority clock; explicit offline files retain separate local
// time provenance. Legacy files without a policy keep their exact signed cutoff.
func (s *SignedLicense) ToRuntimeLicense(now time.Time) *License {
	status, daysRemaining := s.deriveStatus(now)
	return &License{
		Policy:        s.Policy,
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

// deriveStatus never infers grace from a legacy expires_at. Only an explicit,
// signature-verified policy can authorize the paid calendar grace period.
func (s *SignedLicense) deriveStatus(now time.Time) (string, int) {
	if s.Policy != nil {
		state := s.Policy.state(now)
		days := 0
		if now.Before(s.Policy.TermEndsAt) {
			days = int(s.Policy.TermEndsAt.Sub(now) / (24 * time.Hour))
		}
		return state.Status, days
	}
	return expiryStatus(s.ExpiresAt, now)
}
