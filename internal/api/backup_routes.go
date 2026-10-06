package api

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sync/atomic"
	"time"

	"github.com/basekick-labs/arc/internal/auth"
	"github.com/basekick-labs/arc/internal/backup"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// validBackupID matches the format produced by generateBackupID.
var validBackupID = regexp.MustCompile(`^backup-\d{8}-\d{6}-[a-f0-9]{8}$`)

// BackupCoordinator is the minimal cluster interface the backup handler needs
// (#1083): which node may run a backup or a restore. nil = standalone mode,
// no gate. Same shape as DeleteCoordinator and RetentionCoordinator.
type BackupCoordinator interface {
	// IsPrimaryWriter reports whether this node may execute writer-only
	// mutations. A restore writes data files and (on a cluster) manifest
	// entries; a backup reads a listing that only the primary writer is
	// guaranteed to hold in full, and only one node should hold the slot.
	IsPrimaryWriter() bool
	// Role returns a human-readable role string for the rejection message.
	Role() string
}

// BackupHandler handles backup and restore API operations.
type BackupHandler struct {
	manager         *backup.Manager
	authManager     *auth.AuthManager
	coordinator     BackupCoordinator // nil in standalone mode
	logger          zerolog.Logger
	activeOperation atomic.Pointer[string]
}

// NewBackupHandler creates a new backup handler.
func NewBackupHandler(manager *backup.Manager, authManager *auth.AuthManager, logger zerolog.Logger) *BackupHandler {
	return &BackupHandler{
		manager:     manager,
		authManager: authManager,
		logger:      logger.With().Str("component", "backup-api").Logger(),
	}
}

// SetCoordinator wires the cluster coordinator for the node gate. Callers
// pass it only when they hold a non-nil coordinator: an interface holding a
// typed nil pointer is not == nil (#713), and the gate would then call
// methods on a nil receiver. A nil interface is ignored here.
func (h *BackupHandler) SetCoordinator(c BackupCoordinator) {
	if c == nil {
		return
	}
	h.coordinator = c
}

// rejectUnlessPrimaryWriter answers 503 when this node is a cluster member
// that is not the primary writer, and reports whether it did. Evaluated per
// request, as the retention and CQ schedulers do, so a promotion or demotion
// takes effect without a restart. 503 rather than 409: on this API 409 means
// "an operation is in progress" and arcli maps it; the delete API uses the
// same 503 for the same condition. Standby writers, readers and the compactor
// are all refused: a backup from a node that is not the primary may lack
// files the primary holds, and a restore from one would write and register
// files from a node the cluster does not treat as its writer.
func (h *BackupHandler) rejectUnlessPrimaryWriter(c *fiber.Ctx, operation string) bool {
	if h.coordinator == nil || h.coordinator.IsPrimaryWriter() {
		return false
	}
	role := h.coordinator.Role()
	h.logger.Warn().Str("operation", operation).Str("role", role).Msg("Backup API request rejected: this node is not the primary writer")
	_ = c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
		"error": fmt.Sprintf("%s rejected: node role %q is not primary writer; route to the primary writer", operation, role),
		"role":  role,
	})
	return true
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
	var req CreateBackupRequest
	if err := c.BodyParser(&req); err != nil {
		// Empty body is fine — use defaults
	}

	// Node gate first (#1083): a node that may not run the backup does no
	// work and takes no slot.
	if h.rejectUnlessPrimaryWriter(c, "backup") {
		return nil
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

	acquired, err := h.acquireOperation(c, "backup")
	if !acquired {
		return err
	}

	// Run backup asynchronously — Fiber recycles c.Context() after the handler
	// returns, so we must use a detached context.
	go func() {
		defer h.activeOperation.Store(nil)
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

	// Deletion shares the backup/restore admission slot (#626): deleting the
	// backup a restore is reading tears files out from under it, and deleting
	// one mid-write leaves a half-written directory. Unlike backup/restore the
	// delete is synchronous, so the handler holds the slot for its duration.
	acquired, err := h.acquireOperation(c, "delete")
	if !acquired {
		return err
	}
	defer h.activeOperation.Store(nil)

	ctx, cancel := context.WithTimeout(c.Context(), 30*time.Second)
	defer cancel()

	if err := h.manager.DeleteBackup(ctx, id); err != nil {
		// Belt for operations started outside this handler: the manager
		// refuses to delete while its own mutex is held.
		if errors.Is(err, backup.ErrOperationInProgress) {
			operation := "unknown"
			if p := h.manager.GetProgress(); p != nil && p.Status == "running" {
				operation = p.Operation
			}
			return h.operationConflict(c, operation)
		}
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
	RestoreMetadata *bool  `json:"restore_metadata"` // default: true standalone, false on a cluster node (where true is refused)
	RestoreConfig   *bool  `json:"restore_config"`   // default: false (refused on a cluster node)
	Confirm         bool   `json:"confirm"`          // must be true
	// Mode is "merge" (default: additive, resurrects files deleted since the
	// backup) or "replace" (cluster nodes only: the current files of each
	// restored database are removed through the cluster manifest first).
	Mode string `json:"mode"`
}

// RestoreBackup triggers a restore from a backup.
// POST /api/v1/backup/restore
func (h *BackupHandler) RestoreBackup(c *fiber.Ctx) error {
	var req RestoreRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "Invalid request body",
		})
	}

	// Node gate first (#1083), before any validation: a node that may not
	// run the restore does no work and takes no slot.
	if h.rejectUnlessPrimaryWriter(c, "restore") {
		return nil
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

	mode, err := backup.NormalizeRestoreMode(req.Mode)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": err.Error(),
		})
	}

	// On a cluster node the SQLite database and arc.toml are per-node state
	// (#1083): the database holds Raft-replicated tokens, the tier rows of
	// THIS node (#1062) and the audit log, and arc.toml holds cluster.node_id,
	// the role, the seeds, raft_bootstrap and the shared secret. A copy taken
	// on another node, or on this node at another time, would boot this node
	// with another node identity or desynchronise it from the manifest. So on
	// a cluster node restore_metadata defaults to false and an explicit true
	// is refused, as is restore_config. An FSM-aware metadata restore is a
	// design item of its own.
	clustered := h.coordinator != nil
	opts := backup.RestoreOptions{
		BackupID:        req.BackupID,
		RestoreData:     true,
		RestoreMetadata: !clustered,
		RestoreConfig:   false,
		Mode:            mode,
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
	if clustered && opts.RestoreMetadata {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "restore_metadata is not available on a cluster node: the SQLite database holds Raft-replicated tokens, the tier rows of this node and the audit log, so a copy from a backup would diverge this node from the cluster; restore data only (restore_metadata: false)",
		})
	}
	if clustered && opts.RestoreConfig {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "restore_config is not available on a cluster node: arc.toml holds this node identity (cluster.node_id, role, seeds, raft_bootstrap, shared secret), and a config taken on another node would boot this one as that node",
		})
	}
	if !clustered && mode == backup.RestoreModeReplace {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "mode \"replace\" is only available on a cluster node, where the current files are removed through the cluster manifest; a standalone restore is additive (mode \"merge\")",
		})
	}

	acquired, err := h.acquireOperation(c, "restore")
	if !acquired {
		return err
	}

	// Run restore asynchronously — detached context (Fiber recycles c.Context()).
	go func() {
		defer h.activeOperation.Store(nil)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Hour)
		defer cancel()
		if _, err := h.manager.RestoreBackup(ctx, opts); err != nil {
			h.logger.Error().Err(err).Msg("Restore failed")
		}
	}()

	resp := fiber.Map{
		"message":   "Restore started",
		"backup_id": req.BackupID,
		"status":    "running",
		"mode":      mode,
	}
	// Restored databases are STAGED and applied at the next boot (#635), and
	// a restored config only takes effect on reload — both need a server
	// restart to take effect.
	if opts.RestoreMetadata || opts.RestoreConfig {
		resp["restart_required"] = true
	}
	if opts.RestoreMetadata {
		resp["staged"] = true
	}
	return c.Status(fiber.StatusAccepted).JSON(resp)
}

// acquireOperation atomically reserves the handler's shared backup/restore slot.
func (h *BackupHandler) acquireOperation(c *fiber.Ctx, operation string) (bool, error) {
	if p := h.manager.GetProgress(); p != nil && p.Status == "running" {
		return false, h.operationConflict(c, p.Operation)
	}
	if !h.activeOperation.CompareAndSwap(nil, &operation) {
		// The holder may release between the failed CAS and this Load; report
		// "unknown" rather than an empty operation in that sliver (#622 review).
		held := "unknown"
		if active := h.activeOperation.Load(); active != nil {
			held = *active
		}
		return false, h.operationConflict(c, held)
	}
	return true, nil
}

func (h *BackupHandler) operationConflict(c *fiber.Ctx, operation string) error {
	return c.Status(fiber.StatusConflict).JSON(fiber.Map{
		"error":     "A backup or restore operation is already in progress",
		"status":    "running",
		"operation": operation,
	})
}
