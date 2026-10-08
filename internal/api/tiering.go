package api

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/basekick-labs/arc/internal/auth"
	"github.com/basekick-labs/arc/internal/license"
	"github.com/basekick-labs/arc/internal/tiering"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// TieringHandler handles tiered storage API operations
type TieringHandler struct {
	manager       *tiering.Manager
	authManager   *auth.AuthManager
	licenseClient *license.Client
	logger        zerolog.Logger
	getFiles      func(context.Context, string, string) ([]tiering.FileMetadata, error)
	// scanTiers and scanBudget are indirected for the same reason getFiles
	// is: the response classification below is worth testing without
	// standing up a Manager, a SQLite file and a licence client.
	scanTiers  func(context.Context) (*tiering.ScanResult, error)
	scanBudget func() time.Duration
}

// NewTieringHandler creates a new tiering handler
func NewTieringHandler(manager *tiering.Manager, authManager *auth.AuthManager, licenseClient *license.Client, logger zerolog.Logger) *TieringHandler {
	return &TieringHandler{
		manager:       manager,
		authManager:   authManager,
		licenseClient: licenseClient,
		logger:        logger.With().Str("component", "tiering-api").Logger(),
		scanTiers:     manager.ScanTiers,
		scanBudget:    manager.ScanBudget,
		getFiles: func(ctx context.Context, tierParam, database string) ([]tiering.FileMetadata, error) {
			var files []tiering.FileMetadata
			if database != "" {
				return manager.GetMetadata().GetFilesByDatabase(ctx, database)
			}
			if tierParam != "" {
				tier := tiering.TierFromString(tierParam)
				return manager.GetMetadata().GetFilesInTier(ctx, tier)
			}
			for _, t := range []tiering.Tier{tiering.TierHot, tiering.TierCold} {
				tierFiles, err := manager.GetMetadata().GetFilesInTier(ctx, t)
				if err != nil {
					return nil, err
				}
				files = append(files, tierFiles...)
			}
			return files, nil
		},
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
	limit := 100
	if raw := c.Query("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 0 {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid limit"})
		}
		limit = n
	}

	files, err := h.getFiles(ctx, tierParam, database)

	if err != nil {
		h.logger.Error().Err(err).Msg("Failed to get tiering files")
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "Failed to get tiering files",
		})
	}

	// limit=0 deliberately means an empty result. Positive limits are
	// clamped here as a second line of defence even if boundary validation changes.
	if limit == 0 {
		files = files[:0]
	} else if limit > 0 && len(files) > limit {
		files = files[:limit]
	}

	return c.JSON(fiber.Map{
		"files": files,
		"count": len(files),
	})
}

// TriggerMigrationRequest represents a manual migration request
type TriggerMigrationRequest struct {
	FromTier    string `json:"from_tier,omitempty"`   // Optional: "hot" (2-tier system)
	ToTier      string `json:"to_tier,omitempty"`     // Optional: "cold" (2-tier system)
	Database    string `json:"database,omitempty"`    // Optional: filter by database
	Measurement string `json:"measurement,omitempty"` // Optional: filter by measurement
	DryRun      bool   `json:"dry_run,omitempty"`     // If true, only report what would be migrated
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
		if errors.Is(err, tiering.ErrMigrationRoleGated) {
			_, role := h.manager.MigrationGate()
			h.logger.Info().Str("role", role).Msg("Manual migration rejected: node is not the primary writer")
			status, body := migrationRoleGatedResponse(role)
			return c.Status(status).JSON(body)
		}
		if errors.Is(err, tiering.ErrMigrationCycleRunning) {
			return c.Status(fiber.StatusConflict).JSON(fiber.Map{
				"error":   "Migration cycle already running",
				"message": "A migration cycle is already in progress on this node. Wait for it to complete.",
			})
		}
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

// migrationRoleGatedResponse is the answer to a manual migration on a node
// the cluster gate excludes. 409: the request conflicts with the node's
// current role rather than being malformed or unauthorized, and the same
// request succeeds on the primary writer.
func migrationRoleGatedResponse(role string) (int, fiber.Map) {
	return fiber.StatusConflict, fiber.Map{
		"error":   "Migration runs on the primary writer only; this node is not the primary writer",
		"role":    role,
		"message": "Retry against the primary writer, or wait for the scheduled cycle: this node keeps its tier metadata in sync on every tick.",
	}
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

// ScanFiles registers every file in hot storage and, on a node with a
// cluster gate, first learns which files other nodes moved to cold.
// POST /api/v1/tiering/scan
//
// Bounded by tiered_storage.scan_timeout, the same budget the startup scan and
// the pre-migration scan use (#1154). The budget is read from the manager
// rather than carried on the handler so it cannot be a zero here.
//
// Synchronous on purpose. c.Context() is the right parent because it is what
// makes a server shutdown cancel an in-flight scan, and the handler blocks
// until the scan returns, so the context never outlives the request. Note it
// is NOT cancelled when the client disconnects: fasthttp closes
// RequestCtx.Done only on shutdown, which is why a second scan is refused
// below rather than left to pile up behind an abandoned curl.
func (h *TieringHandler) ScanFiles(c *fiber.Ctx) error {
	budget := h.scanBudget()
	ctx, cancel := context.WithTimeout(c.Context(), budget)
	defer cancel()

	h.logger.Info().Dur("scan_timeout", budget).Msg("Starting file scan via API")

	result, err := h.scanTiers(ctx)
	switch {
	case errors.Is(err, tiering.ErrScanRunning):
		h.logger.Warn().Msg("File scan via API refused: a scan is already running on this node")
		return c.Status(fiber.StatusConflict).JSON(fiber.Map{
			"error":   "A tier scan is already running on this node",
			"message": "Wait for it to finish and read its result from GET /api/v1/tiering/status, which reports the last scan.",
		})

	case errors.Is(err, context.DeadlineExceeded):
		// The scan ran out of budget. The rows it wrote are real, so the
		// partial counts go back with the error rather than being
		// discarded: a bare 500 told the operator nothing about how far it
		// got, or that no stale hot row was retired.
		h.logger.Warn().Dur("scan_timeout", budget).
			Int("scanned", result.FilesScanned).
			Msg("File scan via API ran out of budget and stopped part way")
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
			"error":   "The tier scan ran out of its budget and stopped part way",
			"message": "The rows below were written, but the scan is incomplete and no stale hot rows were retired. Raise tiered_storage.scan_timeout above the time a full scan takes on this node.",
			"result":  result,
		})

	case errors.Is(err, context.Canceled):
		// The ONLY producer of Canceled here is the server shutting down:
		// fasthttp closes RequestCtx.Done on shutdown and never on a client
		// disconnect. Saying "raise your budget" would be the wrong advice.
		h.logger.Warn().Msg("File scan via API cancelled: the node is shutting down")
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
			"error":   "The tier scan was cancelled because this node is shutting down",
			"message": "Retry after the node restarts; the startup scan may have already covered it.",
			"result":  result,
		})

	case err != nil:
		h.logger.Error().Err(err).Msg("Failed to scan files")
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error":  err.Error(),
			"result": result,
		})
	}

	// A truncation in the retirement tail returns no error of its own, so it
	// arrives here with err nil. Answering 200 would report a scan that left
	// stale hot rows behind as a clean one.
	if result.Truncated {
		h.logger.Warn().Dur("scan_timeout", budget).
			Int("scanned", result.FilesScanned).
			Msg("File scan via API stopped part way")
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
			"error":   "The tier scan stopped part way through",
			"message": "The rows below were written, but the scan is incomplete and no stale hot rows were retired. Raise tiered_storage.scan_timeout above the time a full scan takes on this node.",
			"result":  result,
		})
	}

	return c.JSON(result)
}

// requireTieringLicense checks that the license still includes the tiering feature.
func (h *TieringHandler) requireTieringLicense(c *fiber.Ctx) error {
	if h.licenseClient == nil || !h.licenseClient.CanUseTieredStorage() {
		return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
			"success": false,
			"error":   "Tiered storage requires an enterprise license with the 'tiering' feature enabled",
		})
	}
	return c.Next()
}

// RegisterRoutes registers tiering API routes
func (h *TieringHandler) RegisterRoutes(app fiber.Router) {
	tiering := app.Group("/api/v1/tiering")
	if h.authManager != nil {
		tiering.Use(auth.RequireAdmin(h.authManager))
	}
	tiering.Use(h.requireTieringLicense)

	tiering.Get("/status", h.GetStatus)
	tiering.Get("/files", h.GetFiles)
	tiering.Post("/migrate", h.TriggerMigration)
	tiering.Get("/stats", h.GetStats)
	tiering.Post("/scan", h.ScanFiles)
}
