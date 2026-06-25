package license

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/rs/zerolog"
)

// maxOfflineLicenseBytes caps the on-disk license file we'll read. A signed
// RSA-2048 license_file is ~800 bytes after base64; 1 MiB is generous headroom
// while still refusing to slurp a hostile multi-GB file into memory.
const maxOfflineLicenseBytes = 1 << 20

// OfflineLicenseFile is the on-disk format for air-gapped activation. It is the
// same detached-signature pair the enterprise server returns over HTTP — the
// base64 canonical SignedLicense JSON plus its base64 RSA-PKCS1v15 SHA-256
// signature — so an operator can pre-mint it with the signing key and hand it
// to an air-gapped customer out-of-band (USB, secure portal). No network is
// involved on the customer side.
type OfflineLicenseFile struct {
	LicenseFile      string `json:"license_file"`      // base64 canonical SignedLicense JSON (the signed bytes)
	LicenseSignature string `json:"license_signature"` // base64 RSA-PKCS1v15 SHA-256 over the decoded LicenseFile bytes
}

// LoadOfflineLicense builds a license Client from a signed license file on disk,
// performing NO network calls. It verifies the detached signature against the
// pinned enterprise public key (the same VerifyLicenseFile path the online
// activation uses) and, on success, returns a Client whose license is the
// verified file contents.
//
// Air-gapped / site-license semantics: the machine fingerprint in the signed
// license is NOT enforced here — offline files are issued unbound (one signed
// file activates any node for the customer until expiry). Expiry IS enforced via
// the runtime status the same way as online licenses (deriveStatus): an expired
// offline file yields an "expired" status and the feature gates fall closed.
func LoadOfflineLicense(path string, logger zerolog.Logger) (*Client, error) {
	componentLogger := logger.With().Str("component", "license-offline").Logger()

	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open offline license file %q: %w", path, err)
	}
	defer f.Close()

	// Bound the read so a malformed/hostile file can't OOM startup. Read
	// limit+1 to detect overflow, matching the import handlers' pattern.
	data, err := io.ReadAll(io.LimitReader(f, maxOfflineLicenseBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read offline license file %q: %w", path, err)
	}
	if int64(len(data)) > maxOfflineLicenseBytes {
		return nil, fmt.Errorf("offline license file %q exceeds %d bytes", path, maxOfflineLicenseBytes)
	}

	var lf OfflineLicenseFile
	if err := json.Unmarshal(data, &lf); err != nil {
		return nil, fmt.Errorf("parse offline license file %q: %w", path, err)
	}
	if lf.LicenseFile == "" || lf.LicenseSignature == "" {
		return nil, fmt.Errorf("offline license file %q is missing license_file or license_signature", path)
	}

	// Reuse the exact verification path used for online activation: the
	// signature is checked against the raw decoded license_file bytes using the
	// pinned RSA-2048 public key. No struct round-trip, no network.
	signed, err := VerifyLicenseFile(lf.LicenseFile, lf.LicenseSignature)
	if err != nil {
		return nil, fmt.Errorf("offline license signature verification failed: %w", err)
	}

	license := signed.ToRuntimeLicense(time.Now().UTC())

	c := &Client{
		// No serverURL / httpClient: an offline client never reaches the network.
		licenseKey: signed.LicenseKey, // sourced from the signed file, not config
		license:    license,
		stopCh:     make(chan struct{}),
		logger:     componentLogger,
		offline:    true,
	}

	componentLogger.Info().
		Str("tier", string(license.Tier)).
		Str("status", license.Status).
		Int("days_remaining", license.DaysRemaining).
		Int("max_cores", license.MaxCores).
		Time("expires_at", license.ExpiresAt).
		Str("path", path).
		Msg("Offline license loaded and verified")

	return c, nil
}
