package cluster

import (
	"context"
	"sync"
	"time"

	"github.com/rs/zerolog"
)

// HealthChecker performs periodic health checks on cluster nodes.
// It monitors node heartbeats and updates node state based on check results.
type HealthChecker struct {
	registry           *Registry
	checkInterval      time.Duration
	checkTimeout       time.Duration
	unhealthyThreshold int

	running bool
	stopCh  chan struct{}
	mu      sync.Mutex

	logger zerolog.Logger
}

// HealthCheckerConfig holds configuration for the health checker.
type HealthCheckerConfig struct {
	Registry           *Registry
	CheckInterval      time.Duration // How often to check nodes (default: 5s)
	CheckTimeout       time.Duration // Timeout for each check (default: 3s)
	UnhealthyThreshold int           // Failed checks before marking unhealthy (default: 3)
	Logger             zerolog.Logger
}

// NewHealthChecker creates a new health checker.
func NewHealthChecker(cfg *HealthCheckerConfig) *HealthChecker {
	// Set defaults
	checkInterval := cfg.CheckInterval
	if checkInterval == 0 {
		checkInterval = 5 * time.Second
	}

	checkTimeout := cfg.CheckTimeout
	if checkTimeout == 0 {
		checkTimeout = 3 * time.Second
	}

	unhealthyThreshold := cfg.UnhealthyThreshold
	if unhealthyThreshold == 0 {
		unhealthyThreshold = 3
	}

	return &HealthChecker{
		registry:           cfg.Registry,
		checkInterval:      checkInterval,
		checkTimeout:       checkTimeout,
		unhealthyThreshold: unhealthyThreshold,
		stopCh:             make(chan struct{}),
		logger:             cfg.Logger.With().Str("component", "health-checker").Logger(),
	}
}

// Start starts the health checker background loop.
func (h *HealthChecker) Start() {
	h.mu.Lock()
	if h.running {
		h.mu.Unlock()
		return
	}
	h.running = true
	h.mu.Unlock()

	go h.checkLoop()

	h.logger.Info().
		Dur("interval", h.checkInterval).
		Dur("timeout", h.checkTimeout).
		Int("unhealthy_threshold", h.unhealthyThreshold).
		Msg("Health checker started")
}

// Stop stops the health checker.
func (h *HealthChecker) Stop() {
	h.mu.Lock()
	if !h.running {
		h.mu.Unlock()
		return
	}
	h.running = false
	h.mu.Unlock()

	close(h.stopCh)
	h.logger.Info().Msg("Health checker stopped")
}

// IsRunning returns true if the health checker is running.
func (h *HealthChecker) IsRunning() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.running
}

// checkLoop performs periodic health checks on all nodes.
func (h *HealthChecker) checkLoop() {
	ticker := time.NewTicker(h.checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			h.checkAllNodes()
		case <-h.stopCh:
			return
		}
	}
}

// checkAllNodes checks the health of all registered nodes.
func (h *HealthChecker) checkAllNodes() {
	nodes := h.registry.GetAll()
	local := h.registry.Local()

	for _, node := range nodes {
		// Skip local node (always healthy if we're running)
		if local != nil && node.ID == local.ID {
			continue
		}

		// Check each remote node concurrently
		go h.checkNode(node)
	}
}

// checkNode checks the health of a single node.
// Uses atomic state transition to prevent TOCTOU race conditions.
func (h *HealthChecker) checkNode(node *Node) {
	ctx, cancel := context.WithTimeout(context.Background(), h.checkTimeout)
	defer cancel()

	healthy := h.performHealthCheck(ctx, node)

	// Use atomic state transition to prevent race conditions
	deadThreshold := h.unhealthyThreshold * 2
	transition := node.ProcessHealthCheckResult(healthy, h.unhealthyThreshold, deadThreshold)

	// Handle state transitions
	if transition != nil {
		switch transition.NewState {
		case StateHealthy:
			h.logger.Info().
				Str("node_id", node.ID).
				Str("role", string(node.Role)).
				Str("previous_state", string(transition.OldState)).
				Msg("Node became healthy")
			h.registry.NotifyHealthy(node)

		case StateUnhealthy:
			h.logger.Warn().
				Str("node_id", node.ID).
				Str("role", string(node.Role)).
				Int("failed_checks", transition.FailedChecks).
				Msg("Node became unhealthy")
			h.registry.NotifyUnhealthy(node)

		case StateDead:
			h.logger.Error().
				Str("node_id", node.ID).
				Str("role", string(node.Role)).
				Int("failed_checks", transition.FailedChecks).
				Msg("Node marked as dead")
		}
	}
}

// performHealthCheck performs the actual health check on a node.
// For Phase 2, this checks heartbeat freshness.
// Phase 3 will add active HTTP health endpoint checks.
func (h *HealthChecker) performHealthCheck(ctx context.Context, node *Node) bool {
	// Check if context is already cancelled
	select {
	case <-ctx.Done():
		return false
	default:
	}

	// Check heartbeat freshness
	// Consider healthy if heartbeat within 3x check interval
	lastHeartbeat := node.GetLastHeartbeat()
	threshold := 3 * h.checkInterval

	if time.Since(lastHeartbeat) < threshold {
		return true
	}

	// TODO (Phase 3): Add active health check
	// - HTTP GET to node's /health endpoint
	// - TCP connection test to coordinator port
	// - gRPC health check if using gRPC

	return false
}

// CheckNow performs an immediate health check on a specific node.
// This is useful for on-demand checks outside the regular interval.
func (h *HealthChecker) CheckNow(nodeID string) bool {
	node, exists := h.registry.Get(nodeID)
	if !exists {
		return false
	}

	ctx, cancel := context.WithTimeout(context.Background(), h.checkTimeout)
	defer cancel()

	return h.performHealthCheck(ctx, node)
}

// GetCheckInterval returns the configured check interval.
func (h *HealthChecker) GetCheckInterval() time.Duration {
	return h.checkInterval
}

// GetUnhealthyThreshold returns the configured unhealthy threshold.
func (h *HealthChecker) GetUnhealthyThreshold() int {
	return h.unhealthyThreshold
}

// Status returns the health checker status for monitoring.
func (h *HealthChecker) Status() map[string]interface{} {
	h.mu.Lock()
	running := h.running
	h.mu.Unlock()

	return map[string]interface{}{
		"running":             running,
		"check_interval_ms":   h.checkInterval.Milliseconds(),
		"check_timeout_ms":    h.checkTimeout.Milliseconds(),
		"unhealthy_threshold": h.unhealthyThreshold,
	}
}
