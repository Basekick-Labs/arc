package api

import (
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"

	"github.com/basekick-labs/arc/internal/auth"
	"github.com/basekick-labs/arc/internal/mqtt"
)

// MQTTHandler handles MQTT stats and health endpoints
type MQTTHandler struct {
	manager     mqtt.Manager
	authManager *auth.AuthManager
	logger      zerolog.Logger
}

// NewMQTTHandler creates a new MQTT handler
func NewMQTTHandler(manager mqtt.Manager, authManager *auth.AuthManager, logger zerolog.Logger) *MQTTHandler {
	return &MQTTHandler{
		manager:     manager,
		authManager: authManager,
		logger:      logger.With().Str("component", "mqtt-api").Logger(),
	}
}

// RegisterRoutes registers the MQTT stats/health API routes
func (h *MQTTHandler) RegisterRoutes(app *fiber.App) {
	mqttGroup := app.Group("/api/v1/mqtt")

	// Stats and health endpoints (read-only, less restrictive auth)
	mqttGroup.Get("/stats", h.handleStats)
	mqttGroup.Get("/health", h.handleHealth)
}

// handleStats returns statistics for all MQTT subscriptions
func (h *MQTTHandler) handleStats(c *fiber.Ctx) error {
	stats, err := h.manager.GetAllStats(c.Context())
	if err != nil {
		h.logger.Error().Err(err).Msg("Failed to get MQTT stats")
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"success": false,
			"error":   "Failed to get MQTT stats",
		})
	}

	// Calculate totals
	var totalMessages, totalFailed, totalBytes int64
	var runningCount, stoppedCount, errorCount int

	for _, s := range stats {
		totalMessages += s.MessagesReceived
		totalFailed += s.MessagesFailed
		totalBytes += s.BytesReceived

		switch s.Status {
		case "running":
			runningCount++
		case "stopped", "paused":
			stoppedCount++
		case "error":
			errorCount++
		}
	}

	return c.JSON(fiber.Map{
		"success": true,
		"stats": fiber.Map{
			"subscriptions": stats,
			"summary": fiber.Map{
				"total_subscriptions": len(stats),
				"running":             runningCount,
				"stopped":             stoppedCount,
				"error":               errorCount,
				"total_messages":      totalMessages,
				"total_failed":        totalFailed,
				"total_bytes":         totalBytes,
			},
		},
	})
}

// handleHealth returns MQTT subsystem health status
func (h *MQTTHandler) handleHealth(c *fiber.Ctx) error {
	stats, err := h.manager.GetAllStats(c.Context())
	if err != nil {
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
			"status":  "unhealthy",
			"error":   "Failed to get MQTT stats",
			"healthy": false,
		})
	}

	// Count statuses
	var running, stopped, errors int
	for _, s := range stats {
		switch s.Status {
		case "running":
			running++
		case "stopped", "paused":
			stopped++
		case "error":
			errors++
		}
	}

	// Determine overall health
	status := "healthy"
	if errors > 0 {
		status = "degraded"
	}
	if running == 0 && len(stats) > 0 {
		status = "unhealthy"
	}

	return c.JSON(fiber.Map{
		"status":  status,
		"healthy": status == "healthy",
		"subscriptions": fiber.Map{
			"total":   len(stats),
			"running": running,
			"stopped": stopped,
			"errors":  errors,
		},
	})
}
