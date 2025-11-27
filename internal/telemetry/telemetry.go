package telemetry

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"github.com/rs/zerolog"
)

const (
	// DefaultEndpoint is the telemetry collection endpoint
	DefaultEndpoint = "https://telemetry.basekick.net/api/v1/telemetry"

	// DefaultInterval is the telemetry reporting interval
	DefaultInterval = 24 * time.Hour

	// instanceIDFile stores the persistent instance ID (matching Python's .instance_id)
	instanceIDFile = ".instance_id"
)

// Config holds telemetry configuration
type Config struct {
	Enabled  bool          // Enable telemetry (default: true)
	Endpoint string        // Telemetry endpoint URL
	Interval time.Duration // Reporting interval (default: 24h)
	DataDir  string        // Directory for instance ID file
}

// DefaultConfig returns the default telemetry configuration
func DefaultConfig() *Config {
	return &Config{
		Enabled:  true,
		Endpoint: DefaultEndpoint,
		Interval: DefaultInterval,
		DataDir:  "./data",
	}
}

// Collector collects and sends telemetry data
type Collector struct {
	config     *Config
	instanceID string
	version    string
	startTime  time.Time

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	client *http.Client
	logger zerolog.Logger
}

// TelemetryPayload represents the data sent to the telemetry endpoint
// Matches Python format from collector.py
type TelemetryPayload struct {
	InstanceID string    `json:"instance_id"`
	Timestamp  string    `json:"timestamp"`
	ArcVersion string    `json:"arc_version"`
	OS         OSInfo    `json:"os"`
	CPU        CPUInfo   `json:"cpu"`
	Memory     MemInfo   `json:"memory"`
}

// OSInfo contains operating system information
type OSInfo struct {
	Name         string `json:"name"`
	Version      string `json:"version"`
	Architecture string `json:"architecture"`
	Platform     string `json:"platform"`
}

// CPUInfo contains CPU information
type CPUInfo struct {
	PhysicalCores *int `json:"physical_cores"`
	LogicalCores  *int `json:"logical_cores"`
	FrequencyMHz  *int `json:"frequency_mhz"`
}

// MemInfo contains memory information
type MemInfo struct {
	TotalGB *float64 `json:"total_gb"`
}

// New creates a new telemetry collector
func New(cfg *Config, version string, logger zerolog.Logger) (*Collector, error) {
	if cfg == nil {
		cfg = DefaultConfig()
	}

	// Load or generate instance ID
	instanceID, err := loadOrCreateInstanceID(cfg.DataDir)
	if err != nil {
		return nil, fmt.Errorf("failed to get instance ID: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	c := &Collector{
		config:     cfg,
		instanceID: instanceID,
		version:    version,
		startTime:  time.Now(),
		ctx:        ctx,
		cancel:     cancel,
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
		logger: logger.With().Str("component", "telemetry").Logger(),
	}

	return c, nil
}

// Start begins periodic telemetry collection
func (c *Collector) Start() {
	if !c.config.Enabled {
		c.logger.Info().Msg("Telemetry is disabled")
		return
	}

	c.logger.Info().
		Str("instance_id", c.instanceID).
		Dur("interval", c.config.Interval).
		Str("endpoint", c.config.Endpoint).
		Msg("Telemetry collector started")

	c.wg.Add(1)
	go c.run()
}

// Stop stops the telemetry collector
func (c *Collector) Stop() {
	c.cancel()
	c.wg.Wait()
	c.logger.Info().Msg("Telemetry collector stopped")
}

// GetInstanceID returns the instance ID
func (c *Collector) GetInstanceID() string {
	return c.instanceID
}

func (c *Collector) run() {
	defer c.wg.Done()

	// Send initial telemetry after a small delay (like Python's 60s delay)
	// to ensure Arc is fully started
	c.logger.Info().Msg("Sending initial telemetry beacon")
	c.sendTelemetry()

	// Then send periodically
	ticker := time.NewTicker(c.config.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.sendTelemetry()
		case <-c.ctx.Done():
			return
		}
	}
}

func (c *Collector) sendTelemetry() {
	payload := c.collectPayload()

	data, err := json.Marshal(payload)
	if err != nil {
		c.logger.Error().Err(err).Msg("Failed to marshal telemetry payload")
		return
	}

	c.logger.Info().
		Str("endpoint", c.config.Endpoint).
		Str("instance_id", c.instanceID).
		Msg("Sending telemetry")

	req, err := http.NewRequestWithContext(c.ctx, "POST", c.config.Endpoint, bytes.NewReader(data))
	if err != nil {
		c.logger.Error().Err(err).Msg("Failed to create telemetry request")
		return
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", fmt.Sprintf("Arc/%s", c.version))

	resp, err := c.client.Do(req)
	if err != nil {
		c.logger.Warn().Err(err).Str("endpoint", c.config.Endpoint).Msg("Failed to send telemetry")
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		c.logger.Warn().Int("status", resp.StatusCode).Msg("Telemetry endpoint returned error")
		return
	}

	c.logger.Info().Int("status", resp.StatusCode).Msg("Telemetry sent successfully")
}

func (c *Collector) collectPayload() *TelemetryPayload {
	// Get CPU info
	numCPU := runtime.NumCPU()

	// Get memory info (total system memory via runtime)
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
	totalGB := float64(memStats.Sys) / (1024 * 1024 * 1024)

	// Build OS platform string like Python's platform.platform()
	platform := fmt.Sprintf("%s-%s-%s", runtime.GOOS, runtime.Version(), runtime.GOARCH)

	return &TelemetryPayload{
		InstanceID: c.instanceID,
		Timestamp:  time.Now().UTC().Format("2006-01-02T15:04:05.000000") + "Z",
		ArcVersion: c.version,
		OS: OSInfo{
			Name:         runtime.GOOS,
			Version:      runtime.Version(), // Go version as OS version proxy
			Architecture: runtime.GOARCH,
			Platform:     platform,
		},
		CPU: CPUInfo{
			PhysicalCores: &numCPU, // Go doesn't distinguish physical/logical easily
			LogicalCores:  &numCPU,
			FrequencyMHz:  nil, // Not easily available in Go without cgo
		},
		Memory: MemInfo{
			TotalGB: &totalGB,
		},
	}
}

func loadOrCreateInstanceID(dataDir string) (string, error) {
	// Ensure data directory exists
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return "", fmt.Errorf("failed to create data directory: %w", err)
	}

	idPath := filepath.Join(dataDir, instanceIDFile)

	// Try to load existing ID
	data, err := os.ReadFile(idPath)
	if err == nil && len(data) > 0 {
		return string(bytes.TrimSpace(data)), nil
	}

	// Generate new ID as UUID format (matching Python's uuid.uuid4())
	id, err := generateUUID()
	if err != nil {
		return "", fmt.Errorf("failed to generate instance ID: %w", err)
	}

	// Save ID
	if err := os.WriteFile(idPath, []byte(id), 0644); err != nil {
		return "", fmt.Errorf("failed to save instance ID: %w", err)
	}

	return id, nil
}

func generateUUID() (string, error) {
	// Generate UUID v4 format: xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}

	// Set version (4) and variant bits
	b[6] = (b[6] & 0x0f) | 0x40 // Version 4
	b[8] = (b[8] & 0x3f) | 0x80 // Variant is 10

	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		b[0:4], b[4:6], b[6:8], b[8:10], b[10:16]), nil
}
