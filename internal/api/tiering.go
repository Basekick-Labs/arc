package api

import (
	"context"
	"time"

	"github.com/basekick-labs/arc/internal/tiering"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// TieringHandler handles tiered storage API operations
type TieringHandler struct {
	manager *tiering.Manager
	logger  zerolog.Logger
}

// NewTieringHandler creates a new tiering handler
func NewTieringHandler(manager *tiering.Manager, logger zerolog.Logger) *TieringHandler {
	return &TieringHandler{
		manager: manager,
		logger:  logger.With().Str("component", "tiering-api").Logger(),
	}
}

// GetStatus returns the current tiering status
// GET /api/v1/tiering/status
func (h *TieringHandler) GetStatus(c *fiber.Ctx) error {
	ctx, cancel := context.WithTimeout(c.Context(), 30*time.Second)
	defer cancel()

	status, err := h.manager.GetStatus(ctx)
	if err != nil {
		h.logger.Error().Err(err).Msg("Failed to get tiering status")
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "Failed to get tiering status",
		})
	}

	return c.JSON(status)
}

// GetFiles returns files by tier
// GET /api/v1/tiering/files
// Query params: tier (optional), database (optional), limit (optional)
func (h *TieringHandler) GetFiles(c *fiber.Ctx) error {
	ctx, cancel := context.WithTimeout(c.Context(), 30*time.Second)
	defer cancel()

	tierParam := c.Query("tier")
	database := c.Query("database")
	limit := c.QueryInt("limit", 100)

	var files []tiering.FileMetadata
	var err error

	if database != "" {
		files, err = h.manager.GetMetadata().GetFilesByDatabase(ctx, database)
	} else if tierParam != "" {
		tier := tiering.TierFromString(tierParam)
		files, err = h.manager.GetMetadata().GetFilesInTier(ctx, tier)
	} else {
		// Get all files - query each tier (2-tier system: hot and cold)
		for _, t := range []tiering.Tier{tiering.TierHot, tiering.TierCold} {
			tierFiles, tierErr := h.manager.GetMetadata().GetFilesInTier(ctx, t)
			if tierErr != nil {
				err = tierErr
				break
			}
			files = append(files, tierFiles...)
		}
	}

	if err != nil {
		h.logger.Error().Err(err).Msg("Failed to get tiering files")
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "Failed to get tiering files",
		})
	}

	// Apply limit
	if len(files) > limit {
		files = files[:limit]
	}

	return c.JSON(fiber.Map{
		"files": files,
		"count": len(files),
	})
}

// TriggerMigrationRequest represents a manual migration request
type TriggerMigrationRequest struct {
	FromTier    string `json:"from_tier,omitempty"`    // Optional: "hot" (2-tier system)
	ToTier      string `json:"to_tier,omitempty"`      // Optional: "cold" (2-tier system)
	Database    string `json:"database,omitempty"`     // Optional: filter by database
	Measurement string `json:"measurement,omitempty"`  // Optional: filter by measurement
	DryRun      bool   `json:"dry_run,omitempty"`      // If true, only report what would be migrated
}

// TriggerMigration triggers a manual migration
// POST /api/v1/tiering/migrate
func (h *TieringHandler) TriggerMigration(c *fiber.Ctx) error {
	ctx, cancel := context.WithTimeout(c.Context(), 2*time.Hour)
	defer cancel()

	var req TriggerMigrationRequest
	if err := c.BodyParser(&req); err != nil {
		// Empty body is OK - run full migration cycle
	}

	// For now, just run a full migration cycle
	// TODO: Support filtered migrations based on request params

	h.logger.Info().
		Str("from_tier", req.FromTier).
		Str("to_tier", req.ToTier).
		Str("database", req.Database).
		Bool("dry_run", req.DryRun).
		Msg("Manual migration triggered")

	if err := h.manager.TriggerMigration(ctx); err != nil {
		h.logger.Error().Err(err).Msg("Failed to trigger migration")
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "Failed to trigger migration",
		})
	}

	return c.JSON(fiber.Map{
		"message": "Migration completed successfully",
		"status":  "completed",
	})
}

// GetStats returns migration statistics
// GET /api/v1/tiering/stats
func (h *TieringHandler) GetStats(c *fiber.Ctx) error {
	ctx, cancel := context.WithTimeout(c.Context(), 30*time.Second)
	defer cancel()

	// Get tier stats
	tierStats, err := h.manager.GetMetadata().GetTierStats(ctx)
	if err != nil {
		h.logger.Error().Err(err).Msg("Failed to get tier stats")
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "Failed to get tier stats",
		})
	}

	// Get recent migrations
	recentMigrations, err := h.manager.GetMetadata().GetRecentMigrations(ctx, 10)
	if err != nil {
		h.logger.Error().Err(err).Msg("Failed to get recent migrations")
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "Failed to get recent migrations",
		})
	}

	return c.JSON(fiber.Map{
		"tier_stats":        tierStats,
		"recent_migrations": recentMigrations,
	})
}

// ScanFiles scans the hot tier storage and registers all existing files
// POST /api/v1/tiering/scan
func (h *TieringHandler) ScanFiles(c *fiber.Ctx) error {
	ctx, cancel := context.WithTimeout(c.Context(), 30*time.Minute)
	defer cancel()

	h.logger.Info().Msg("Starting file scan via API")

	result, err := h.manager.ScanAndRegisterFiles(ctx)
	if err != nil {
		h.logger.Error().Err(err).Msg("Failed to scan files")
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": err.Error(),
		})
	}

	return c.JSON(result)
}

// RegisterRoutes registers tiering API routes
func (h *TieringHandler) RegisterRoutes(app fiber.Router) {
	tiering := app.Group("/api/v1/tiering")

	tiering.Get("/status", h.GetStatus)
	tiering.Get("/files", h.GetFiles)
	tiering.Post("/migrate", h.TriggerMigration)
	tiering.Get("/stats", h.GetStats)
	tiering.Post("/scan", h.ScanFiles)
}
