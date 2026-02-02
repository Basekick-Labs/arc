package api

import (
	"context"
	"regexp"
	"time"

	"github.com/basekick-labs/arc/internal/auth"
	"github.com/basekick-labs/arc/internal/backup"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// validBackupID matches the format produced by generateBackupID.
var validBackupID = regexp.MustCompile(`^backup-\d{8}-\d{6}-[a-f0-9]{8}$`)

// BackupHandler handles backup and restore API operations.
type BackupHandler struct {
	manager     *backup.Manager
	authManager *auth.AuthManager
	logger      zerolog.Logger
}

// NewBackupHandler creates a new backup handler.
func NewBackupHandler(manager *backup.Manager, authManager *auth.AuthManager, logger zerolog.Logger) *BackupHandler {
	return &BackupHandler{
		manager:     manager,
		authManager: authManager,
		logger:      logger.With().Str("component", "backup-api").Logger(),
	}
}

// RegisterRoutes registers backup and restore API routes.
func (h *BackupHandler) RegisterRoutes(app fiber.Router) {
	group := app.Group("/api/v1/backup")
	if h.authManager != nil {
		group.Use(auth.RequireAdmin(h.authManager))
	}

	group.Post("/", h.CreateBackup)
	group.Get("/", h.ListBackups)
	group.Get("/status", h.GetStatus)
	group.Get("/:id", h.GetBackup)
	group.Delete("/:id", h.DeleteBackup)
	group.Post("/restore", h.RestoreBackup)
}

// CreateBackupRequest is the request body for POST /api/v1/backup.
type CreateBackupRequest struct {
	IncludeMetadata *bool `json:"include_metadata"` // default: true
	IncludeConfig   *bool `json:"include_config"`   // default: true
}

// CreateBackup triggers a new backup.
// POST /api/v1/backup
func (h *BackupHandler) CreateBackup(c *fiber.Ctx) error {
	// Check if a backup is already running
	if p := h.manager.GetProgress(); p != nil && p.Status == "running" {
		return c.Status(fiber.StatusConflict).JSON(fiber.Map{
			"error":   "A backup or restore operation is already in progress",
			"status":  p.Status,
			"operation": p.Operation,
		})
	}

	var req CreateBackupRequest
	if err := c.BodyParser(&req); err != nil {
		// Empty body is fine — use defaults
	}

	opts := backup.BackupOptions{
		IncludeMetadata: true,
		IncludeConfig:   true,
	}
	if req.IncludeMetadata != nil {
		opts.IncludeMetadata = *req.IncludeMetadata
	}
	if req.IncludeConfig != nil {
		opts.IncludeConfig = *req.IncludeConfig
	}

	// Run backup asynchronously — Fiber recycles c.Context() after the handler
	// returns, so we must use a detached context.
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Hour)
		defer cancel()
		if _, err := h.manager.CreateBackup(ctx, opts); err != nil {
			h.logger.Error().Err(err).Msg("Backup failed")
		}
	}()

	// Return immediately — client polls /status for progress
	return c.Status(fiber.StatusAccepted).JSON(fiber.Map{
		"message": "Backup started",
		"status":  "running",
	})
}

// ListBackups returns all available backups.
// GET /api/v1/backup
func (h *BackupHandler) ListBackups(c *fiber.Ctx) error {
	ctx, cancel := context.WithTimeout(c.Context(), 30*time.Second)
	defer cancel()

	summaries, err := h.manager.ListBackups(ctx)
	if err != nil {
		h.logger.Error().Err(err).Msg("Failed to list backups")
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "Failed to list backups",
		})
	}

	if summaries == nil {
		summaries = []backup.BackupSummary{}
	}

	return c.JSON(fiber.Map{
		"backups": summaries,
		"count":   len(summaries),
	})
}

// GetStatus returns the progress of the current active operation.
// GET /api/v1/backup/status
func (h *BackupHandler) GetStatus(c *fiber.Ctx) error {
	p := h.manager.GetProgress()
	if p == nil {
		return c.JSON(fiber.Map{
			"status": "idle",
		})
	}
	return c.JSON(p)
}

// GetBackup returns the manifest for a specific backup.
// GET /api/v1/backup/:id
func (h *BackupHandler) GetBackup(c *fiber.Ctx) error {
	id := c.Params("id")
	if !validBackupID.MatchString(id) {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "Invalid backup ID format",
		})
	}

	ctx, cancel := context.WithTimeout(c.Context(), 30*time.Second)
	defer cancel()

	manifest, err := h.manager.GetBackup(ctx, id)
	if err != nil {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
			"error": "Backup not found",
		})
	}

	return c.JSON(manifest)
}

// DeleteBackup removes a backup.
// DELETE /api/v1/backup/:id
func (h *BackupHandler) DeleteBackup(c *fiber.Ctx) error {
	id := c.Params("id")
	if !validBackupID.MatchString(id) {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "Invalid backup ID format",
		})
	}

	ctx, cancel := context.WithTimeout(c.Context(), 30*time.Second)
	defer cancel()

	if err := h.manager.DeleteBackup(ctx, id); err != nil {
		h.logger.Error().Err(err).Str("backup_id", id).Msg("Failed to delete backup")
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "Failed to delete backup",
		})
	}

	return c.JSON(fiber.Map{
		"message":   "Backup deleted",
		"backup_id": id,
	})
}

// RestoreRequest is the request body for POST /api/v1/backup/restore.
type RestoreRequest struct {
	BackupID        string `json:"backup_id"`
	RestoreData     *bool  `json:"restore_data"`     // default: true
	RestoreMetadata *bool  `json:"restore_metadata"` // default: true
	RestoreConfig   *bool  `json:"restore_config"`   // default: false
	Confirm         bool   `json:"confirm"`          // must be true
}

// RestoreBackup triggers a restore from a backup.
// POST /api/v1/backup/restore
func (h *BackupHandler) RestoreBackup(c *fiber.Ctx) error {
	// Check if an operation is already running
	if p := h.manager.GetProgress(); p != nil && p.Status == "running" {
		return c.Status(fiber.StatusConflict).JSON(fiber.Map{
			"error":     "A backup or restore operation is already in progress",
			"status":    p.Status,
			"operation": p.Operation,
		})
	}

	var req RestoreRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "Invalid request body",
		})
	}

	if !validBackupID.MatchString(req.BackupID) {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "Invalid or missing backup_id",
		})
	}

	if !req.Confirm {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "Restore is a destructive operation. Set confirm: true to proceed.",
		})
	}

	opts := backup.RestoreOptions{
		BackupID:        req.BackupID,
		RestoreData:     true,
		RestoreMetadata: true,
		RestoreConfig:   false,
	}
	if req.RestoreData != nil {
		opts.RestoreData = *req.RestoreData
	}
	if req.RestoreMetadata != nil {
		opts.RestoreMetadata = *req.RestoreMetadata
	}
	if req.RestoreConfig != nil {
		opts.RestoreConfig = *req.RestoreConfig
	}

	// Run restore asynchronously — detached context (Fiber recycles c.Context()).
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Hour)
		defer cancel()
		if _, err := h.manager.RestoreBackup(ctx, opts); err != nil {
			h.logger.Error().Err(err).Msg("Restore failed")
		}
	}()

	return c.Status(fiber.StatusAccepted).JSON(fiber.Map{
		"message":   "Restore started",
		"backup_id": req.BackupID,
		"status":    "running",
	})
}
