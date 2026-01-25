package license

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"
)

const (
	// LicenseServerURL is the hardcoded enterprise license server URL
	LicenseServerURL = "https://enterprise.basekick.net"

	// ValidationInterval is how often the license is re-validated (4 hours)
	ValidationInterval = 4 * time.Hour
)

// ClientConfig holds configuration for the license client
type ClientConfig struct {
	LicenseKey string
	Logger     zerolog.Logger
}

// Client handles license validation with the enterprise server
type Client struct {
	serverURL   string
	licenseKey  string
	fingerprint string
	httpClient  *http.Client
	license     *License
	mu          sync.RWMutex
	stopCh      chan struct{}
	logger      zerolog.Logger
}

// VerifyRequest represents a license verification request
type VerifyRequest struct {
	LicenseKey         string `json:"license_key"`
	MachineFingerprint string `json:"machine_fingerprint"`
}

// VerifyResponse represents the response from the license server
type VerifyResponse struct {
	Valid         bool      `json:"valid"`
	Tier          string    `json:"tier,omitempty"`
	ExpiresAt     time.Time `json:"expires_at,omitempty"`
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

// ActivateResponse represents the response from license activation
type ActivateResponse struct {
	Success       bool      `json:"success"`
	LicenseFile   string    `json:"license_file,omitempty"` // Base64 encoded signed license
	ExpiresAt     time.Time `json:"expires_at,omitempty"`
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
		return nil, fmt.Errorf("license activation request failed: %w", err)
	}
	defer resp.Body.Close()

	var activateResp ActivateResponse
	if err := json.NewDecoder(resp.Body).Decode(&activateResp); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	if !activateResp.Success {
		errMsg := activateResp.Error
		if errMsg == "" {
			errMsg = "license activation failed"
		}
		c.logger.Warn().Str("error", errMsg).Msg("License activation failed")
		return nil, fmt.Errorf("license activation failed: %s", errMsg)
	}

	license := &License{
		LicenseKey:    c.licenseKey,
		CustomerID:    activateResp.CustomerID,
		CustomerName:  activateResp.CustomerName,
		Tier:          TierFromString(activateResp.Tier),
		MaxCores:      activateResp.MaxCores,
		Features:      activateResp.Features,
		ExpiresAt:     activateResp.ExpiresAt,
		Status:        activateResp.Status,
		DaysRemaining: activateResp.DaysRemaining,
	}

	// Store the license
	c.mu.Lock()
	c.license = license
	c.mu.Unlock()

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
		return nil, fmt.Errorf("license verification request failed: %w", err)
	}
	defer resp.Body.Close()

	var verifyResp VerifyResponse
	if err := json.NewDecoder(resp.Body).Decode(&verifyResp); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	if !verifyResp.Valid {
		errMsg := verifyResp.Error
		if errMsg == "" {
			errMsg = "license validation failed"
		}
		c.logger.Warn().Str("error", errMsg).Msg("License validation failed")
		return nil, fmt.Errorf("license validation failed: %s", errMsg)
	}

	license := &License{
		LicenseKey:    c.licenseKey,
		CustomerID:    verifyResp.CustomerID,
		CustomerName:  verifyResp.CustomerName,
		Tier:          TierFromString(verifyResp.Tier),
		MaxCores:      verifyResp.MaxCores,
		MaxMachines:   verifyResp.MaxMachines,
		Features:      verifyResp.Features,
		ExpiresAt:     verifyResp.ExpiresAt,
		Status:        verifyResp.Status,
		DaysRemaining: verifyResp.DaysRemaining,
	}

	// Store the license
	c.mu.Lock()
	c.license = license
	c.mu.Unlock()

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

	// If verification failed with "machine not activated", try to activate
	if strings.Contains(err.Error(), "machine not activated") {
		c.logger.Info().Msg("Machine not activated, attempting activation")
		return c.Activate(ctx)
	}

	// Other error, return it
	return nil, err
}

// StartPeriodicValidation starts background license validation
func (c *Client) StartPeriodicValidation(interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				_, err := c.Verify(ctx)
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
