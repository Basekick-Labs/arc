package license

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"time"
)

// License cache + resilient boot (#license-boot-resilience).
//
// Field incident: every pod boot made one blocking ActivateOrVerify with no
// retry and no cache; a deploy-window ingress 404 on the license server
// crash-looped a customer's cluster (27 restarts) because OSS fallback is
// FATAL for enterprise-required configs (shared-storage multi-writer).
//
// The cache is the exact signed server response pair — tamper-evident by
// signature, nothing secret beyond what the license already is. Loading it is
// gated HARD (see LoadCachedLicense): without the fingerprint requirement, a
// copied cache file would license any number of machines until expiry
// (VerifyLicenseFile alone checks signature+parse only, and bindFingerprint
// accepts an EMPTY fingerprint as unbound — a legitimate cache can never be
// unbound, because Arc always sends a non-empty fingerprint for the server to
// sign).

// cacheFileName sits in the auth-DB directory (per-pod volume in the Helm
// chart — no cross-pod file races).
const cacheFileName = "license_cache.json"

// Boot retry budget. ONE phase deadline bounds the whole retry loop —
// per-attempt arithmetic lies when the server is black-holed (three full
// 30s HTTP timeouts = 103s, past the ~60s liveness kill line). The Helm
// chart's startupProbe (shipped with this change) covers the phase.
// Vars, not consts, purely so tests can compress time; production never
// mutates them.
var (
	bootRetryPhaseBudget = 45 * time.Second
	bootRetryAttempts    = 3
	bootRetryBackoffUnit = time.Second
)

// bootRetryBackoff returns the pause before attempt i (1-based, no pause
// before the first), with ±20% jitter so a restart storm doesn't synchronize
// against the server's rate limiter (100 req/min/IP, and cluster egress is
// typically NAT'd to one IP).
func bootRetryBackoff(attempt int) time.Duration {
	if attempt <= 1 {
		return 0
	}
	base := bootRetryBackoffUnit << (attempt - 1) // 2s, 4s at the 1s unit
	jitter := time.Duration(rand.Int63n(int64(base)/2)) - time.Duration(int64(base)/4)
	return base + jitter
}

// cachedPair is the on-disk shape: byte-identical to the server's activation/
// verify response payload, so one verifier serves the online response, this
// cache, and (deliverable B) the offline license file.
type cachedPair struct {
	LicenseFile      string `json:"license_file"`
	LicenseSignature string `json:"license_signature"`
}

// cachePath returns "" when caching is disabled (no dir configured — tests,
// or library callers).
func (c *Client) cachePath() string {
	if c.cacheDir == "" {
		return ""
	}
	return filepath.Join(c.cacheDir, cacheFileName)
}

// saveCache persists the verified pair after a successful Activate/Verify.
// Best-effort: a cache write failure never fails the activation that produced
// it. 0600 — it is a bearer of enterprise entitlements for THIS machine.
func (c *Client) saveCache(licenseFile, licenseSignature string) {
	path := c.cachePath()
	if path == "" {
		return
	}
	// Never persist an UNBOUND pair: LoadCachedLicense's gate 3 could never
	// load it, so it would only overwrite a previously usable bound cache.
	// Server-bug-only today (the server always signs the fingerprint Arc
	// sent), and it keeps deliverable B's unbound site files structurally out
	// of the cache slot.
	if signed, err := VerifyLicenseFile(licenseFile, licenseSignature); err != nil || signed.MachineFingerprint == "" {
		c.logger.Warn().Msg("refusing to cache an unbound or unverifiable license pair")
		return
	}
	data, err := json.Marshal(cachedPair{LicenseFile: licenseFile, LicenseSignature: licenseSignature})
	if err != nil {
		c.logger.Warn().Err(err).Msg("license cache marshal failed")
		return
	}
	// The license phase runs BEFORE main.go creates the auth-DB directory, so
	// a first-ever boot would otherwise fail its first cache write and only
	// persist at the next periodic verify (4h later) — leaving a restart
	// inside that window unprotected. 0700 matches the auth-DB dir's own mode.
	if err := os.MkdirAll(c.cacheDir, 0o700); err != nil {
		c.logger.Warn().Err(err).Msg("license cache dir create failed")
		return
	}
	// Fixed tmp name is safe: cache writers are strictly serialized (boot
	// completes before StartPeriodicValidation; no other Verify callers).
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		c.logger.Warn().Err(err).Msg("license cache write failed")
		return
	}
	if err := os.Rename(tmp, path); err != nil {
		c.logger.Warn().Err(err).Msg("license cache rename failed")
		return
	}
	c.logger.Debug().Str("path", path).Msg("license cache updated")
}

// deleteCache removes the cache (best-effort), used when its contents cannot
// belong to the configured key.
func (c *Client) deleteCache() {
	if path := c.cachePath(); path != "" {
		_ = os.Remove(path)
	}
}

// LoadCachedLicense loads and re-verifies the cached pair. ALL of the
// following must hold, or the cache is rejected (and on a key mismatch,
// deleted):
//
//  1. signature verifies against the pinned public key;
//  2. the signed license key equals the CONFIGURED key — a stale cache from a
//     replaced/downgraded license must not resurrect old entitlements;
//  3. the signed fingerprint is NON-EMPTY and equals this machine's — see the
//     file comment; empty-accepting bindFingerprint is NOT sufficient here;
//  4. not expired (the cache is honored until the license's OWN expires_at,
//     never longer — no new grace is introduced).
func (c *Client) LoadCachedLicense() (*License, error) {
	path := c.cachePath()
	if path == "" {
		return nil, fmt.Errorf("license cache disabled")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("license cache unavailable: %w", err)
	}
	var pair cachedPair
	if err := json.Unmarshal(data, &pair); err != nil {
		return nil, fmt.Errorf("license cache corrupt: %w", err)
	}
	signed, err := VerifyLicenseFile(pair.LicenseFile, pair.LicenseSignature)
	if err != nil {
		return nil, fmt.Errorf("license cache failed verification: %w", err)
	}
	if signed.LicenseKey != c.licenseKey {
		c.deleteCache()
		return nil, fmt.Errorf("license cache is for a different license key; deleted")
	}
	if signed.MachineFingerprint == "" || signed.MachineFingerprint != c.fingerprint {
		return nil, fmt.Errorf("license cache is not bound to this machine")
	}
	now := time.Now().UTC()
	if !now.Before(signed.ExpiresAt) {
		return nil, fmt.Errorf("cached license expired at %s", signed.ExpiresAt.UTC().Format(time.RFC3339))
	}

	lic := signed.ToRuntimeLicense(now)
	c.mu.Lock()
	c.license = lic
	c.mu.Unlock()
	return lic, nil
}

// LicenseSource says where the boot license came from, for operator-facing
// logging.
type LicenseSource string

const (
	SourceServer LicenseSource = "server"
	SourceCache  LicenseSource = "cache"
)

// ActivateOrVerifyResilient is the boot entry point: bounded retries against
// the server, then — for TRANSIENT failures only — the verified cache.
//
//   - The whole retry phase shares one deadline (bootRetryPhaseBudget); a
//     black-holed server cannot stretch it to attempts × HTTP-timeout.
//   - A DEFINITIVE rejection (the server spoke the protocol and said no)
//     returns immediately and never consults the cache: revocation stays
//     effective whenever the server is reachable.
//   - Cache fallback proceeds FULLY LICENSED until the license's own
//     expires_at; the caller logs it loudly and the periodic re-verify keeps
//     trying the server (first success rewrites the cache).
func (c *Client) ActivateOrVerifyResilient(ctx context.Context) (*License, LicenseSource, error) {
	phaseCtx, cancel := context.WithTimeout(ctx, bootRetryPhaseBudget)
	defer cancel()

	var lastErr error
	for attempt := 1; attempt <= bootRetryAttempts; attempt++ {
		if attempt > 1 {
			select {
			case <-phaseCtx.Done():
			case <-time.After(bootRetryBackoff(attempt)):
			}
			if phaseCtx.Err() != nil {
				break // phase budget exhausted
			}
		}
		lic, err := c.ActivateOrVerify(phaseCtx)
		if err == nil {
			return lic, SourceServer, nil
		}
		lastErr = err
		if FailureClassOf(err) == ClassDefinitive {
			return nil, SourceServer, err
		}
		c.logger.Warn().Err(err).Int("attempt", attempt).Int("max_attempts", bootRetryAttempts).
			Msg("license server unreachable or answered non-definitively; will retry")
	}

	lic, cacheErr := c.LoadCachedLicense()
	if cacheErr != nil {
		c.logger.Warn().Err(cacheErr).Msg("license cache not usable")
		return nil, SourceServer, fmt.Errorf("license server unreachable and no usable cache: %w", lastErr)
	}
	return lic, SourceCache, nil
}
