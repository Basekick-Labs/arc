package license

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"
)

// maxLicenseResponseBytes caps the response-body size we'll read on
// the activation/verify path. A signed RSA-2048 license_file is
// ~800 bytes after base64; 1 MiB is generous headroom that still
// shuts down a hostile proxy attempting to OOM us by streaming an
// unbounded body.
const maxLicenseResponseBytes = 1 << 20

const (
	// LicenseServerURL is the hardcoded enterprise license server URL
	LicenseServerURL = "https://enterprise.basekick.net"

	// ValidationInterval is how often the license is re-validated (4 hours)
	ValidationInterval = 4 * time.Hour
)

// ClientConfig holds configuration for the license client
type ClientConfig struct {
	LicenseKey string
	// CacheDir, when non-empty, enables the on-disk license cache
	// (license_cache.json) used for boot resilience when the license server
	// is unreachable. See cache.go.
	CacheDir string
	Logger   zerolog.Logger
}

// Client handles license validation with the enterprise server
type Client struct {
	serverURL   string
	licenseKey  string
	fingerprint string
	cacheDir    string
	// offline marks a file-mode client (NewOfflineClient): network-free, no
	// periodic validation, no cache.
	offline bool
	// source says where the CURRENT license came from ("server", "cache",
	// "file") for /health; guarded by mu. A cache-boot flips to "server" on
	// the first successful background verify.
	source     LicenseSource
	httpClient *http.Client
	license    *License
	mu         sync.RWMutex
	stopCh     chan struct{}
	logger     zerolog.Logger
}

// VerifyRequest represents a license verification request
type VerifyRequest struct {
	LicenseKey         string `json:"license_key"`
	MachineFingerprint string `json:"machine_fingerprint"`
}

// VerifyResponse represents the response from the license server.
//
// LicenseFile + LicenseSignature form a JWS-style detached signature
// pair. As of 26.06.2 this is the only material used for feature
// gating; the other fields below are decoded for backward-compat /
// debug logging only and MUST NOT be relied upon. Use
// VerifyLicenseFile(LicenseFile, LicenseSignature) to derive a
// verified License.
type VerifyResponse struct {
	Valid            bool   `json:"valid"`
	LicenseFile      string `json:"license_file,omitempty"`      // Base64 canonical License JSON (the SIGNED bytes)
	LicenseSignature string `json:"license_signature,omitempty"` // Base64 RSA-PKCS1v15 SHA-256 signature over LicenseFile's decoded bytes
	Tier             string `json:"tier,omitempty"`
	// No omitempty: a value time.Time is never omitted by encoding/json, so the
	// tag was a no-op; drop it to keep the contract honest (#546). This field is
	// decode-side/debug-only — the authoritative expiry is the signed License.
	ExpiresAt     time.Time `json:"expires_at"`
	DaysRemaining int       `json:"days_remaining,omitempty"`
	Status        string    `json:"status,omitempty"` // active, grace_period, read_only, expired
	Error         string    `json:"error,omitempty"`
	MaxCores      int       `json:"max_cores,omitempty"`
	MaxMachines   int       `json:"max_machines,omitempty"`
	Features      []string  `json:"features,omitempty"`
	CustomerID    string    `json:"customer_id,omitempty"`
	CustomerName  string    `json:"customer_name,omitempty"`
}

// NewClient creates a new license client
func NewClient(cfg *ClientConfig) (*Client, error) {
	if cfg.LicenseKey == "" {
		return nil, fmt.Errorf("license key is required")
	}

	// Generate machine fingerprint
	fingerprint, err := GenerateMachineFingerprint()
	if err != nil {
		return nil, fmt.Errorf("failed to generate machine fingerprint: %w", err)
	}

	c := &Client{
		serverURL:   LicenseServerURL,
		licenseKey:  cfg.LicenseKey,
		cacheDir:    cfg.CacheDir,
		fingerprint: fingerprint,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
		stopCh: make(chan struct{}),
		logger: cfg.Logger.With().Str("component", "license-client").Logger(),
	}

	c.logger.Debug().
		Str("server_url", LicenseServerURL).
		Str("fingerprint", fingerprint).
		Msg("License client initialized")

	return c, nil
}

// ActivateRequest represents a license activation request
type ActivateRequest struct {
	LicenseKey         string `json:"license_key"`
	MachineFingerprint string `json:"machine_fingerprint"`
	Hostname           string `json:"hostname"`
	Cores              int    `json:"cores"`
}

// ActivateResponse represents the response from license activation.
//
// LicenseFile + LicenseSignature form a JWS-style detached signature
// pair (matches VerifyResponse — see that struct for the rationale).
// Decoded LicenseFile bytes ARE what was signed; LicenseSignature is
// RSA-PKCS1v15 SHA-256 over those exact bytes.
type ActivateResponse struct {
	Success          bool   `json:"success"`
	LicenseFile      string `json:"license_file,omitempty"`      // Base64 canonical License JSON (the SIGNED bytes)
	LicenseSignature string `json:"license_signature,omitempty"` // Base64 RSA-PKCS1v15 SHA-256 signature over LicenseFile's decoded bytes
	// No omitempty: a value time.Time is never omitted (#546); tag was a no-op.
	ExpiresAt     time.Time `json:"expires_at"`
	Tier          string    `json:"tier,omitempty"`
	MaxCores      int       `json:"max_cores,omitempty"`
	Error         string    `json:"error,omitempty"`
	CustomerID    string    `json:"customer_id,omitempty"`
	CustomerName  string    `json:"customer_name,omitempty"`
	Features      []string  `json:"features,omitempty"`
	DaysRemaining int       `json:"days_remaining,omitempty"`
	Status        string    `json:"status,omitempty"`
}

// Activate registers this machine with the license server
func (c *Client) Activate(ctx context.Context) (*License, error) {
	if c.offline {
		return nil, fmt.Errorf("license client is in offline file mode; network operations are disabled")
	}
	url := fmt.Sprintf("%s/api/v1/activate", c.serverURL)

	hostname, _ := os.Hostname()
	cores := runtime.NumCPU()

	req := &ActivateRequest{
		LicenseKey:         c.licenseKey,
		MachineFingerprint: c.fingerprint,
		Hostname:           hostname,
		Cores:              cores,
	}

	jsonData, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	c.logger.Info().
		Str("license_key", c.licenseKey[:min(12, len(c.licenseKey))]+"...").
		Str("fingerprint", c.fingerprint[:min(16, len(c.fingerprint))]+"...").
		Str("hostname", hostname).
		Int("cores", cores).
		Msg("Activating license")

	httpReq, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(jsonData))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		c.logger.Error().Err(err).Msg("License activation request failed")
		return nil, transientErr("license activation request failed: %w", err)
	}
	defer resp.Body.Close()

	if err := checkHTTPStatus(resp); err != nil {
		return nil, fmt.Errorf("license activation failed: %w", err)
	}
	var activateResp ActivateResponse
	if err := readBoundedJSON(resp, &activateResp); err != nil {
		// Malformed 200 = proxy default backend, not a protocol answer.
		return nil, transientErr("failed to decode activation response: %w", err)
	}

	if !activateResp.Success {
		errMsg := activateResp.Error
		if errMsg == "" {
			errMsg = "license activation failed"
		}
		c.logger.Warn().Str("error", errMsg).Msg("License activation failed")
		// 200 envelope rejection: the server spoke the protocol and said no.
		return nil, definitiveErr("license activation failed: %s", errMsg)
	}

	// Verify the detached signature against the raw license_file
	// bytes (NOT against a re-marshaled struct — that's the forward-
	// compat trap). The unsigned envelope fields (Tier/Features/
	// MaxCores/...) are discarded; the verified SignedLicense is the
	// sole source of truth for every feature gate.
	signed, err := VerifyLicenseFile(activateResp.LicenseFile, activateResp.LicenseSignature)
	if err != nil {
		c.logger.Warn().Err(err).Msg("License activation: signature verification failed")
		// Protocol violation (or MITM): transient — the cache re-verifies
		// independently, so preferring it cannot be worse than this response.
		return nil, transientErr("license activation failed: %w", err)
	}
	if err := c.bindFingerprint(signed); err != nil {
		c.logger.Warn().Err(err).Msg("License activation: fingerprint binding mismatch")
		return nil, transientErr("license activation failed: %w", err)
	}
	c.logger.Debug().Msg("signature verified")

	license := signed.ToRuntimeLicense(time.Now().UTC())

	// Store the license
	c.mu.Lock()
	c.license = license
	c.source = SourceServer
	c.mu.Unlock()

	// Persist the verified pair for boot resilience (see cache.go).
	c.saveCache(activateResp.LicenseFile, activateResp.LicenseSignature)

	c.logger.Info().
		Str("tier", string(license.Tier)).
		Str("status", license.Status).
		Int("days_remaining", license.DaysRemaining).
		Int("max_cores", license.MaxCores).
		Time("expires_at", license.ExpiresAt).
		Msg("License activated successfully")

	return license, nil
}

// Verify validates the license with the enterprise server
func (c *Client) Verify(ctx context.Context) (*License, error) {
	if c.offline {
		return nil, fmt.Errorf("license client is in offline file mode; network operations are disabled")
	}
	url := fmt.Sprintf("%s/api/v1/verify", c.serverURL)

	c.logger.Debug().
		Str("license_key", c.licenseKey[:min(12, len(c.licenseKey))]+"...").
		Msg("Verifying license")

	// Use query parameters for GET-style verify
	httpReq, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	q := httpReq.URL.Query()
	q.Add("license_key", c.licenseKey)
	q.Add("machine_fingerprint", c.fingerprint)
	httpReq.URL.RawQuery = q.Encode()

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		c.logger.Error().Err(err).Msg("License verification request failed")
		return nil, transientErr("license verification request failed: %w", err)
	}
	defer resp.Body.Close()

	if err := checkHTTPStatus(resp); err != nil {
		return nil, fmt.Errorf("license validation failed: %w", err)
	}
	var verifyResp VerifyResponse
	if err := readBoundedJSON(resp, &verifyResp); err != nil {
		return nil, transientErr("failed to decode verify response: %w", err)
	}

	if !verifyResp.Valid {
		errMsg := verifyResp.Error
		if errMsg == "" {
			errMsg = "license validation failed"
		}
		c.logger.Warn().Str("error", errMsg).Msg("License validation failed")
		// Two envelope rejections are NOT terminal (activation is the intended
		// recovery); matched EXACTLY against the server's strings here — where
		// the raw envelope is in hand — rather than substring-scanned on a
		// wrapped message that also embeds HTTP body previews. The server side
		// carries a frozen-contract comment on these strings.
		if errMsg == "machine not activated for this license" || errMsg == "activation has been revoked" {
			return nil, fmt.Errorf("license validation failed: %w: %s", errActivationMissing, errMsg)
		}
		// Everything else = definitive (unknown strings included: the server
		// spoke the protocol and said no).
		return nil, definitiveErr("license validation failed: %s", errMsg)
	}

	// Same detached-signature verification path as Activate.
	signed, err := VerifyLicenseFile(verifyResp.LicenseFile, verifyResp.LicenseSignature)
	if err != nil {
		c.logger.Warn().Err(err).Msg("License verification: signature verification failed")
		return nil, transientErr("license validation failed: %w", err)
	}
	if err := c.bindFingerprint(signed); err != nil {
		c.logger.Warn().Err(err).Msg("License verification: fingerprint binding mismatch")
		return nil, transientErr("license validation failed: %w", err)
	}
	c.logger.Debug().Msg("signature verified")

	license := signed.ToRuntimeLicense(time.Now().UTC())

	// Store the license
	c.mu.Lock()
	c.license = license
	c.source = SourceServer
	c.mu.Unlock()

	// Persist the verified pair for boot resilience (see cache.go).
	c.saveCache(verifyResp.LicenseFile, verifyResp.LicenseSignature)

	c.logger.Info().
		Str("tier", string(license.Tier)).
		Str("status", license.Status).
		Int("days_remaining", license.DaysRemaining).
		Int("max_cores", license.MaxCores).
		Time("expires_at", license.ExpiresAt).
		Msg("License verified successfully")

	return license, nil
}

// ActivateOrVerify tries to verify first, and if the machine is not activated, activates it
func (c *Client) ActivateOrVerify(ctx context.Context) (*License, error) {
	// Try to verify first
	license, err := c.Verify(ctx)
	if err == nil {
		return license, nil
	}

	// errActivationMissing covers the two NOT-terminal verify rejections —
	// activation is the server's intended recovery path for both:
	//   - "machine not activated": first boot on this fingerprint.
	//   - "activation has been revoked": the server's stale-activation reaper
	//     revokes activations without a heartbeat for 72h, and Arc has never
	//     sent heartbeats — so this is a ROUTINE condition for any
	//     stable-fingerprint machine, not an operator decision. Re-activating
	//     re-creates the activation record (the server supports it); a
	//     deliberately revoked LICENSE still rejects the re-activation
	//     definitively ("license is not active").
	if errors.Is(err, errActivationMissing) {
		c.logger.Info().Msg("No live activation for this machine, attempting activation")
		return c.Activate(ctx)
	}

	// Other error, return it (carrying its failure class).
	return nil, err
}

// StartPeriodicValidation starts background license validation
func (c *Client) StartPeriodicValidation(interval time.Duration) {
	if c.offline {
		c.logger.Debug().Msg("offline license mode: periodic validation disabled")
		return
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				// ActivateOrVerify, not bare Verify: an activation reaped
				// during a long outage (>72h without a liveness signal)
				// self-heals on the next tick instead of warning every 4h
				// until restart — and each success refreshes the cache.
				_, err := c.ActivateOrVerify(ctx)
				if err != nil {
					c.logger.Warn().Err(err).Msg("Periodic license validation failed")
				}
				cancel()
			case <-c.stopCh:
				c.logger.Debug().Msg("Stopping periodic license validation")
				return
			}
		}
	}()

	c.logger.Info().
		Dur("interval", interval).
		Msg("Started periodic license validation")
}

// Stop stops the license client and any background tasks
func (c *Client) Stop() {
	close(c.stopCh)
}

// GetLicense returns the current cached license
func (c *Client) GetLicense() *License {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.license
}

// IsValid returns true if there's a valid license
func (c *Client) IsValid() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.license != nil && c.license.IsValid()
}

// IsEnterprise returns true if the license is enterprise tier or higher
func (c *Client) IsEnterprise() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.license != nil && c.license.IsEnterprise()
}

// CanUseCQScheduler returns true if CQ scheduling is allowed
func (c *Client) CanUseCQScheduler() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.license != nil && c.license.CanUseCQScheduler()
}

// CanUseRetentionScheduler returns true if retention scheduling is allowed
func (c *Client) CanUseRetentionScheduler() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.license != nil && c.license.CanUseRetentionScheduler()
}

// CanUseTieredStorage returns true if tiered storage is allowed
func (c *Client) CanUseTieredStorage() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.license != nil && c.license.CanUseTieredStorage()
}

// CanUseClustering returns true if multi-node clustering is allowed.
// Used by the reconciliation API middleware to re-validate the
// license on every request so a license expiry mid-process kicks
// in without restart.
func (c *Client) CanUseClustering() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.license != nil && c.license.CanUseClustering()
}

// CanUseAuditLogging returns true if audit logging is allowed
func (c *Client) CanUseAuditLogging() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.license != nil && c.license.CanUseAuditLogging()
}

// CanUseWriterFailover returns true if automatic writer failover is allowed
func (c *Client) CanUseWriterFailover() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.license != nil && c.license.CanUseWriterFailover()
}

// CanUseQueryGovernance returns true if query governance is allowed
func (c *Client) CanUseQueryGovernance() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.license != nil && c.license.CanUseQueryGovernance()
}

// CanUseQueryManagement returns true if query management is allowed
func (c *Client) CanUseQueryManagement() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.license != nil && c.license.CanUseQueryManagement()
}

// CanUseArcx returns true if loading the proprietary arcx DuckDB
// extension is allowed. Read at process startup before issuing `LOAD`;
// **license expiry mid-process does NOT unload arcx by design** —
// the extension lives in DuckDB's process memory after LOAD, and
// there is no symmetric `UNLOAD` we can issue from re-validation
// middleware. An operator who needs to revoke arcx must restart Arc.
// The runtime perimeter is binary distribution (see arcx repo README).
func (c *Client) CanUseArcx() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.license != nil && c.license.CanUseArcx()
}

// CanUseSharedStorageMultiWriter returns true if Pattern 2 multi-writer
// (N RoleWriter nodes sharing one object-storage backend) is allowed.
// Read at process startup before opening the ingest gate; if false +
// cluster.shared_storage_mode=true, Arc refuses to start with a clear
// error pointing operators at the license documentation.
func (c *Client) CanUseSharedStorageMultiWriter() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.license != nil && c.license.CanUseSharedStorageMultiWriter()
}

// GetFingerprint returns the machine fingerprint
func (c *Client) GetFingerprint() string {
	return c.fingerprint
}

// min returns the minimum of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// bindFingerprint enforces that, when the signed License is bound to
// a specific machine, that machine is THIS machine. Closes the
// "MitM captures customer A's signed blob and replays it to customer
// B's machine" attack: the activation server already binds the
// MachineFingerprint at sign time; arc now verifies the binding at
// the receiver.
//
// An empty MachineFingerprint in the signed blob is permitted (some
// signing flows omit it intentionally, e.g. offline licenses) and
// returns nil. A non-empty fingerprint that doesn't match the
// running machine is rejected.
//
// The constant-time check isn't strictly necessary (this is a public-
// readable identifier, not a secret) but the fingerprints have fixed
// "sha256:" prefix + 64 hex chars and bytewise equality is fine
// either way.
func (c *Client) bindFingerprint(signed *SignedLicense) error {
	if signed == nil || signed.MachineFingerprint == "" {
		return nil
	}
	if signed.MachineFingerprint != c.fingerprint {
		return fmt.Errorf("license: signed fingerprint %q does not match this machine %q",
			signed.MachineFingerprint[:min(16, len(signed.MachineFingerprint))]+"...",
			c.fingerprint[:min(16, len(c.fingerprint))]+"...")
	}
	return nil
}

// checkHTTPStatus returns a descriptive error if the response is not
// a 2xx. Lets callers surface "HTTP 502 from license server" to
// operators instead of "failed to decode response: invalid character
// 'H' looking for beginning of value" — much easier to triage.
//
// Doesn't close resp.Body — caller's defer handles that.
// checkHTTPStatus returns nil for 2xx and a CLASSIFIED error otherwise: 5xx
// and 429 are transient; a 4xx is definitive only when its body is the
// protocol's own JSON error shape — a bare/HTML 4xx is an ingress artifact
// (the field incident: deploy-window 404s crash-looped a cluster). The message
// embeds a bounded, rune-safe preview of the body for diagnosis.
func checkHTTPStatus(resp *http.Response) error {
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}
	// Drain a small bounded prefix of the body so the message can include
	// the server's error text. Anything more than 4 KiB is almost
	// certainly an HTML error page from an upstream proxy; truncate.
	preview, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
	previewStr := strings.TrimSpace(string(preview))
	// Rune-safe truncation: a byte-slice cut can split a multi-byte
	// UTF-8 sequence and leave the error string invalid (turns the
	// log line into mojibake). Counting runes preserves character
	// boundaries on localized error pages.
	if runes := []rune(previewStr); len(runes) > 256 {
		previewStr = string(runes[:256]) + "...[truncated]"
	}
	definitive := resp.StatusCode >= 400 && resp.StatusCode < 500 &&
		resp.StatusCode != 429 && isProtocolJSONBody(previewStr)
	mk := transientErr
	if definitive {
		mk = definitiveErr
	}
	if previewStr == "" {
		return mk("HTTP %d from license server", resp.StatusCode)
	}
	return mk("HTTP %d from license server: %s", resp.StatusCode, previewStr)
}

// readBoundedJSON decodes a JSON response body into `v`, refusing to
// read more than maxLicenseResponseBytes. Defends against a hostile
// proxy returning an unbounded stream — the threat model explicitly
// contemplates a substituted activation server, and DoSing the arc
// process via a huge body is the lowest-effort attack against it.
//
// Doesn't close resp.Body — caller's defer handles that.
func readBoundedJSON(resp *http.Response, v any) error {
	bounded := io.LimitReader(resp.Body, maxLicenseResponseBytes)
	return json.NewDecoder(bounded).Decode(v)
}
