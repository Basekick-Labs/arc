package license

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/rs/zerolog"
)

// Offline (air-gapped) license file support — deliverable B of the reviewed
// plan behind #608's boot resilience. The file is the SAME detached pair the
// online endpoints emit ({license_file, license_signature}; extra fields such
// as the human-readable "summary" are ignored), downloaded from the activation
// server admin and pointed at via license.file_path / ARC_LICENSE_FILE_PATH.
//
// Source-context rules (deliberately different from the boot cache):
//   - The offline file MAY be unbound (empty fingerprint): it is a site
//     license by design — one file activates any machine until expiry,
//     acknowledged at mint time and audit-logged server-side.
//   - A BOUND pair in the file slot still binds: a non-empty signed
//     fingerprint must match this machine (someone pointing file_path at a
//     copied online response gets the online rules, not a bypass).
//   - The boot CACHE, by contrast, requires a non-empty matching fingerprint
//     (cache.go) — so an offline file dropped into the cache slot is rejected
//     there, and the two paths cannot be confused into weakening each other.
//
// Offline mode is network-free, full stop: no activation, no verification
// calls, no periodic re-validation, no cache. Expiry is enforced AT LOAD —
// a file that expires mid-process keeps its features until the next restart
// rejects it (consistent with the boot-only enforcement philosophy of the
// resilience work; do not claim runtime expiry enforcement in docs).
// Fail-closed: any error → OSS.

// offlineFile is the accepted file shape; unknown fields (summary) ignored.
type offlineFile struct {
	LicenseFile      string `json:"license_file"`
	LicenseSignature string `json:"license_signature"`
}

// NewOfflineClient builds a license client whose license comes entirely from
// the file at path. The returned client never contacts the license server:
// StartPeriodicValidation on it is a no-op, and there is no cache.
func NewOfflineClient(path string, logger zerolog.Logger) (*Client, error) {
	c := &Client{
		licenseKey: "", // no key in file mode; the signed payload is authoritative
		offline:    true,
		stopCh:     make(chan struct{}),
		logger:     logger.With().Str("component", "license-offline").Logger(),
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("offline license file unreadable: %w", err)
	}
	var f offlineFile
	if err := json.Unmarshal(data, &f); err != nil {
		return nil, fmt.Errorf("offline license file malformed: %w", err)
	}
	if f.LicenseFile == "" || f.LicenseSignature == "" {
		return nil, fmt.Errorf("offline license file missing license_file/license_signature")
	}
	signed, err := VerifyLicenseFile(f.LicenseFile, f.LicenseSignature)
	if err != nil {
		return nil, fmt.Errorf("offline license verification failed: %w", err)
	}
	// Bound files still bind; empty = unbound site license (allowed HERE, and
	// only here — see the source-context rules above). The fingerprint is
	// generated LAZILY, only for bound files: an unbound site license must not
	// fail on a box where fingerprinting fails — exotic air-gapped hardware is
	// exactly this feature's audience (#B review MED-4).
	if signed.MachineFingerprint != "" {
		fingerprint, err := GenerateMachineFingerprint()
		if err != nil {
			return nil, fmt.Errorf("offline license is machine-bound but fingerprinting failed: %w", err)
		}
		c.fingerprint = fingerprint
		if err := c.bindFingerprint(signed); err != nil {
			return nil, fmt.Errorf("offline license verification failed: %w", err)
		}
	}
	now := time.Now().UTC()
	if !now.Before(signed.ExpiresAt) {
		return nil, fmt.Errorf("offline license expired at %s", signed.ExpiresAt.UTC().Format(time.RFC3339))
	}

	lic := signed.ToRuntimeLicense(now)
	c.mu.Lock()
	c.license = lic
	c.source = SourceFile
	c.mu.Unlock()

	c.logger.Info().
		Str("tier", string(lic.Tier)).
		Time("expires_at", lic.ExpiresAt).
		Bool("site_license", signed.MachineFingerprint == "").
		Msg("offline license loaded and verified")
	return c, nil
}
