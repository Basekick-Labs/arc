package api

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"sync/atomic"
	"time"

	"github.com/basekick-labs/arc/internal/auth"
	"github.com/basekick-labs/arc/internal/backup"
	"github.com/basekick-labs/arc/internal/storage"
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
	IncludeMetadata *bool `json:"include_metadata"` // default: true; false and refused when scoped
	IncludeConfig   *bool `json:"include_config"`   // default: true; false when scoped
	// Databases scopes the backup to these databases (#1084): storage-root
	// segments, so an edge-sync spoke is named as the spoke. Empty or absent
	// is a whole-instance backup. At most maxScopeDatabases names, each a
	// safe storage path segment that names an existing database.
	Databases []string `json:"databases"`
}

// maxScopeDatabases bounds how many databases one backup may be scoped to:
// each is checked synchronously, with bounded work, inside this request.
const maxScopeDatabases = 256

// scopedMetadataRefusal is the 400 for include_metadata: true on a scoped
// backup. No apostrophe on purpose: the log masker truncates error text at
// one.
const scopedMetadataRefusal = "include_metadata is not available on a scoped backup: the SQLite database holds the tier rows of every database, the tokens, the continuous queries and the audit log, so it cannot ride along with one database; take an unscoped backup for it"

// normalizeBackupScope validates the databases of a scoped backup request and
// returns them sorted and de-duplicated, or an error naming the offending
// value. Names are matched exactly (storage segments are case-sensitive).
// Each must pass isSafeStoragePathSegment, the rule for a name that NAMES
// something existing in the storage root (not the create-time rule, see its
// doc comment), because the manager turns each into a ListObjects prefix and
// a PrefixProber prefix; and none may be a reserved root (_schema,
// _compaction_state), which hold Arc's own state and not a database, and
// which the hot-prefix probe would otherwise call known.
func normalizeBackupScope(names []string) ([]string, error) {
	if len(names) == 0 {
		return nil, nil
	}
	if len(names) > maxScopeDatabases {
		return nil, fmt.Errorf("databases lists %d names; a backup can be scoped to at most %d", len(names), maxScopeDatabases)
	}
	seen := make(map[string]struct{}, len(names))
	out := make([]string, 0, len(names))
	for _, name := range names {
		if name == "" {
			return nil, errors.New("databases contains an empty name")
		}
		if !isSafeStoragePathSegment(name) {
			return nil, fmt.Errorf("databases contains %q, which is not a valid database name: a name is one storage path segment (no separators, no backslash, no NUL, not . or .., not dot-prefixed, at most %d bytes)", name, storage.MaxUsableKeySegmentLen)
		}
		if storage.IsReservedRootDir(name) {
			return nil, fmt.Errorf("databases contains %q, which is a reserved storage root, not a database", name)
		}
		if _, dup := seen[name]; dup {
			return nil, fmt.Errorf("databases lists %q more than once", name)
		}
		seen[name] = struct{}{}
		out = append(out, name)
	}
	sort.Strings(out)
	return out, nil
}

// CreateBackup triggers a new backup.
// POST /api/v1/backup
func (h *BackupHandler) CreateBackup(c *fiber.Ctx) error {
	var req CreateBackupRequest
	// An empty body means the defaults. A non-empty body must be JSON and
	// must parse: for a scoping feature the worst outcome is a `databases`
	// that is silently dropped and becomes a whole-instance backup, which is
	// exactly what a form-typed body does (curl -d defaults to
	// application/x-www-form-urlencoded, and Fiber form-parses that into an
	// empty request with every unknown key ignored).
	if len(c.Body()) > 0 {
		if !c.Is("json") {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": "Invalid request body: send JSON (Content-Type: application/json)",
			})
		}
		if err := c.BodyParser(&req); err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": "Invalid request body",
			})
		}
	}

	// Node gate first (#1083): a node that may not run the backup does no
	// work and takes no slot.
	if h.rejectUnlessPrimaryWriter(c, "backup") {
		return nil
	}

	// Scope (#1084): validated before any work and before the slot is taken.
	databases, err := normalizeBackupScope(req.Databases)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": err.Error(),
		})
	}
	scoped := len(databases) > 0

	// Defaults: a whole-instance backup carries the SQLite metadata and the
	// config; a scoped one carries neither unless asked, and metadata cannot
	// be asked for (it is every database's state, not one database's).
	opts := backup.BackupOptions{
		IncludeMetadata: !scoped,
		IncludeConfig:   !scoped,
		Databases:       databases,
	}
	if req.IncludeMetadata != nil {
		opts.IncludeMetadata = *req.IncludeMetadata
	}
	if req.IncludeConfig != nil {
		opts.IncludeConfig = *req.IncludeConfig
	}
	if scoped && opts.IncludeMetadata {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": scopedMetadataRefusal,
		})
	}

	// Known-database check, synchronous and bounded per name (one indexed
	// tier-metadata query, then two prefix probes, and only when all three
	// say no an enumeration of the hidden keys under <name>/, which then
	// holds nothing listable; never a listing of a real database), so a typo
	// is a 400 now rather than a failed run later. A probe that cannot be
	// answered is a 500, not a guess. See Manager.CheckDatabasesKnown.
	if scoped {
		ctx, cancel := context.WithTimeout(c.Context(), 30*time.Second)
		defer cancel()
		if err := h.manager.CheckDatabasesKnown(ctx, databases); err != nil {
			var unknown *backup.UnknownDatabasesError
			if errors.As(err, &unknown) {
				return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
					"error":             err.Error(),
					"unknown_databases": unknown.Names,
				})
			}
			h.logger.Error().Err(err).Strs("databases", databases).Msg("Could not check whether the scoped databases exist")
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"error": "Could not check whether the requested databases exist: " + err.Error(),
			})
		}
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
	resp := fiber.Map{
		"message": "Backup started",
		"status":  "running",
	}
	if scoped {
		resp["databases"] = databases
	}
	return c.Status(fiber.StatusAccepted).JSON(resp)
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
	// Mode is "merge" (additive, resurrects files deleted since the backup)
	// or "replace" (cluster nodes only: the current files of each restored
	// database are removed through the cluster manifest first). Absent, it
	// is merge, except that a scoped backup (#1084) restored on a cluster
	// node defaults to replace; the response echoes the effective mode.
	// Replace of a scoped backup that holds no data file for one of its
	// databases is refused (400), implicit or explicit: it would only delete.
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
	// Replace is keyed on the Raft manifest being wired, which is what the
	// manager runs from, not on the coordinator: a cluster node without
	// cluster.raft_data_dir has the coordinator and not the manifest, and the
	// manager would refuse the mode anyway.
	if !h.manager.ClusterManifestWired() && mode == backup.RestoreModeReplace {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "mode \"replace\" is only available on a cluster node, where the current files are removed through the cluster manifest; a standalone restore is additive (mode \"merge\")",
		})
	}

	// Effective mode (#1084): a request that named none restores a scoped
	// backup in replace mode on a cluster node. Resolved here from the
	// manifest's scope and from whether the manager has the Raft manifest
	// wired, with the same function the manager applies after it reads the
	// manifest, so the echo is what will run. A manifest that cannot be read
	// here (unknown id, or a transient backup-storage error) resolves as
	// unscoped, is echoed that way and is admitted as before; the manager
	// then reads the manifest itself and fails the run with "backup not
	// found", or runs from what it read.
	var manifest *backup.Manifest
	{
		ctx, cancel := context.WithTimeout(c.Context(), 30*time.Second)
		mf, err := h.manager.GetBackup(ctx, req.BackupID)
		cancel()
		if err != nil {
			h.logger.Debug().Err(err).Str("backup_id", req.BackupID).Msg("Could not read the backup manifest before the restore; resolving the mode as for an unscoped backup")
		} else {
			manifest = mf
		}
	}
	var scope []string
	if manifest != nil {
		scope = manifest.Scope
	}
	mode, err = backup.ResolveRestoreMode(req.Mode, scope, h.manager.ClusterManifestWired())
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": err.Error(),
		})
	}
	// A replace of a scoped backup that holds no data file for one of its
	// databases would only delete (#1084); refused here with the manager's
	// text, whether the mode was defaulted or named.
	if manifest != nil {
		if err := backup.CheckScopedReplace(mode, manifest); err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": err.Error(),
			})
		}
	}
	opts.Mode = mode

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
	// restart to take effect. Only when the backup holds them: a scoped
	// backup never carries the metadata, so a default standalone restore of
	// one stages nothing. When the manifest could not be read the flags
	// follow the request, as before.
	stagesMetadata := opts.RestoreMetadata && (manifest == nil || manifest.HasMetadata)
	restoresConfig := opts.RestoreConfig && (manifest == nil || manifest.HasConfig)
	if stagesMetadata || restoresConfig {
		resp["restart_required"] = true
	}
	if stagesMetadata {
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
