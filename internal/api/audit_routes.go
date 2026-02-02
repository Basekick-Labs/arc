package api

import (
	"strconv"
	"time"

	"github.com/basekick-labs/arc/internal/audit"
	"github.com/basekick-labs/arc/internal/auth"
	"github.com/basekick-labs/arc/internal/license"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// AuditHandler handles audit log query endpoints
type AuditHandler struct {
	auditLogger   *audit.Logger
	authManager   *auth.AuthManager
	licenseClient *license.Client
	logger        zerolog.Logger
}

// NewAuditHandler creates a new audit handler
func NewAuditHandler(auditLogger *audit.Logger, authManager *auth.AuthManager, licenseClient *license.Client, logger zerolog.Logger) *AuditHandler {
	return &AuditHandler{
		auditLogger:   auditLogger,
		authManager:   authManager,
		licenseClient: licenseClient,
		logger:        logger.With().Str("component", "audit-handler").Logger(),
	}
}

// RegisterRoutes registers audit log query endpoints
func (h *AuditHandler) RegisterRoutes(app *fiber.App) {
	auditGroup := app.Group("/api/v1/audit")
	auditGroup.Use(auth.RequireAdmin(h.authManager))
	auditGroup.Use(h.requireAuditLicense)

	auditGroup.Get("/logs", h.queryLogs)
	auditGroup.Get("/stats", h.getStats)
}

func (h *AuditHandler) requireAuditLicense(c *fiber.Ctx) error {
	if h.licenseClient == nil || !h.licenseClient.CanUseAuditLogging() {
		return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
			"success": false,
			"error":   "Audit logging requires an enterprise license with the 'audit_logging' feature enabled",
		})
	}
	return c.Next()
}

func (h *AuditHandler) queryLogs(c *fiber.Ctx) error {
	filter := &audit.QueryFilter{
		EventType: c.Query("event_type"),
		Actor:     c.Query("actor"),
		Database:  c.Query("database"),
	}

	if since := c.Query("since"); since != "" {
		t, err := time.Parse(time.RFC3339, since)
		if err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"success": false,
				"error":   "Invalid 'since' format, use RFC3339 (e.g., 2026-01-01T00:00:00Z)",
			})
		}
		filter.Since = t
	}

	if until := c.Query("until"); until != "" {
		t, err := time.Parse(time.RFC3339, until)
		if err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"success": false,
				"error":   "Invalid 'until' format, use RFC3339 (e.g., 2026-01-02T00:00:00Z)",
			})
		}
		filter.Until = t
	}

	if limit := c.Query("limit"); limit != "" {
		n, err := strconv.Atoi(limit)
		if err != nil || n < 1 {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"success": false,
				"error":   "Invalid 'limit' parameter",
			})
		}
		filter.Limit = n
	}

	if offset := c.Query("offset"); offset != "" {
		n, err := strconv.Atoi(offset)
		if err != nil || n < 0 {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"success": false,
				"error":   "Invalid 'offset' parameter",
			})
		}
		filter.Offset = n
	}

	entries, err := h.auditLogger.Query(c.Context(), filter)
	if err != nil {
		h.logger.Error().Err(err).Msg("Failed to query audit logs")
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"success": false,
			"error":   "Failed to query audit logs",
		})
	}

	return c.JSON(fiber.Map{
		"success": true,
		"data":    entries,
		"count":   len(entries),
	})
}

func (h *AuditHandler) getStats(c *fiber.Ctx) error {
	var since time.Time
	if sinceStr := c.Query("since"); sinceStr != "" {
		t, err := time.Parse(time.RFC3339, sinceStr)
		if err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"success": false,
				"error":   "Invalid 'since' format, use RFC3339",
			})
		}
		since = t
	} else {
		// Default: last 24 hours
		since = time.Now().UTC().Add(-24 * time.Hour)
	}

	stats, err := h.auditLogger.Stats(c.Context(), since)
	if err != nil {
		h.logger.Error().Err(err).Msg("Failed to get audit stats")
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"success": false,
			"error":   "Failed to get audit stats",
		})
	}

	return c.JSON(fiber.Map{
		"success": true,
		"data":    stats,
		"since":   since.Format(time.RFC3339),
	})
}
