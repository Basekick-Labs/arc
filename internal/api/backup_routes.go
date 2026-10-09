package api

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/basekick-labs/arc/internal/auth"
	"github.com/basekick-labs/arc/internal/backup"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// BackupCoordinator is the minimal cluster interface the backup handler needs
// (#1083, #1134): which node may create, restore or delete a backup.
// nil = standalone mode, no gate. Same shape as DeleteCoordinator and
// RetentionCoordinator.
type BackupCoordinator interface {
	// IsPrimaryWriter reports whether this node may execute writer-only
	// mutations. A restore writes data files and (on a cluster) manifest
	// entries; a backup reads a listing that only the primary writer is
	// guaranteed to hold in full, and only one node should hold the slot.
	// Deletion sweeps the configured backup targets of the authoritative node.
	IsPrimaryWriter() bool
	// Role returns a human-readable role string for the rejection message.
	Role() string
}

// BackupHandler handles backup and restore API operations.
type BackupHandler struct {
	manager          *backup.Manager
	authManager      *auth.AuthManager
	coordinator      BackupCoordinator // nil in standalone mode
	operationTimeout time.Duration
	logger           zerolog.Logger
	activeOperation  atomic.Pointer[string]
}

// NewBackupHandler creates a new backup handler.
//
// operationTimeout bounds one backup or one restore run
// (backup.operation_timeout, default 2h). It is a constructor parameter rather
// than a settable field because both routes detach from the request context —
// Fiber recycles it — so this is the only thing that eventually releases the
// single-operation slot, and a zero value would expire the context before the
// run started. config.Load refuses a non-positive value; a caller that builds
// one by hand and leaves it zero is corrected here to the historical 2h rather
// than cancelling instantly.
func NewBackupHandler(manager *backup.Manager, authManager *auth.AuthManager, operationTimeout time.Duration, logger zerolog.Logger) *BackupHandler {
	if operationTimeout <= 0 {
		operationTimeout = defaultBackupOperationTimeout
	}
	return &BackupHandler{
		manager:          manager,
		authManager:      authManager,
		operationTimeout: operationTimeout,
		logger:           logger.With().Str("component", "backup-api").Logger(),
	}
}

// defaultBackupOperationTimeout is the value both routes were hardcoded to
// before backup.operation_timeout existed. It is the fallback for a
// hand-built handler only; the configured default lives in
// config.setDefaults so operators can see it.
//
// The manager's constant, not a second copy: the LISTING reads the same figure
// to decide whether a run whose manifests have not landed may still be in
// flight (#1085 stage B2b-2), and two 2h constants would drift.
const defaultBackupOperationTimeout = backup.DefaultOperationTimeout

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
// files from a node the cluster does not treat as its writer. Deletion must
// use the authoritative node's configured target set as well (#1134).
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

// describeTarget names the backup destination in an operator-facing error.
// The manager's own backupTarget.describe says the same thing for log lines;
// this is the HTTP-response half, kept here because the response must not
// carry the manager's wrapped SDK error.
func describeTarget(name string) string {
	if name == "" {
		return "the backup destination"
	}
	return fmt.Sprintf("backup target %q", name)
}

// describeTargets names every configured destination (#1085 stage B2b-2).
//
// With several targets, naming only the DEFAULT points an operator at the
// wrong store: the request may have failed against a routed one, and the
// manager's own error says which. This is for the messages that cannot know,
// where the honest answer is the whole set.
func describeTargets(names []string) string {
	switch len(names) {
	case 0:
		return "the backup destination"
	case 1:
		return describeTarget(names[0])
	}
	quoted := make([]string, 0, len(names))
	for _, name := range names {
		quoted = append(quoted, fmt.Sprintf("%q", name))
	}
	return "backup targets " + strings.Join(quoted, ", ")
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
	// IncludeConfig default: true, EXCEPT false when the backup is scoped and
	// false when the destination is a remote target, whose own credentials
	// arc.toml carries (#1085 stage B2b-1). Setting it true against a remote
	// target is honoured and warned about.
	IncludeConfig *bool `json:"include_config"`
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
	// IncludeConfig is additionally false when the destination is a REMOTE
	// target (#1085 stage B2b-1): arc.toml carries that target's own
	// credentials, so copying it there puts the keys that unlock the backup
	// store inside the backups it holds. A request may still ask for it
	// explicitly, and the manager warns when it does.
	// IncludeConfig is false when ANY configured target is remote (#1085 stage
	// B2b-2), not only the default one: arc.toml carries every target's
	// credentials, so a local default plus one remote routed target still
	// means copying it puts the keys to that store inside a backup it holds.
	opts := backup.BackupOptions{
		IncludeMetadata: !scoped,
		IncludeConfig:   !scoped && !h.manager.TargetIsRemote(),
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
		ctx, cancel := context.WithTimeout(context.Background(), h.operationTimeout)
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
//
// With several configured targets (#1085 stage B2b-2) the listing fans out
// over all of them and unions by backup ID. A target that cannot provide a
// complete listing, including one with an unreadable manifest, is NAMED in
// unreachable_targets and the response is still 200: one failed store must
// not hide the backups on the others. 503 is returned when no target can
// provide a complete listing; one unreadable destination is not a trustworthy
// answer for a single-target setup.
func (h *BackupHandler) ListBackups(c *fiber.Ctx) error {
	ctx, cancel := context.WithTimeout(c.Context(), 30*time.Second)
	defer cancel()

	// include_foreign=true also returns backups another Arc instance wrote to
	// the same destination, each marked foreign_owner with its
	// owner_instance_id (#1085 stage B2b-1). The default leaves them out,
	// because two instances sharing a bucket and prefix merging into one
	// listing is the hazard the owner field exists for; the opt-in exists
	// because after a restore onto fresh hardware every backup reads as
	// foreign, and an operator who could not list them could not find the id
	// of the next one to restore.
	includeForeign := c.QueryBool("include_foreign", false)

	listing, err := h.manager.ListBackupsDetailed(ctx, includeForeign)
	if err != nil {
		targets := h.manager.TargetNames()
		h.logger.Error().Err(err).Strs("targets", targets).Msg("Failed to list backups")
		// 503, not 500: once a destination can be remote this is a transient
		// far more often than a defect, and the answer has to name the
		// destinations or an operator with more than one configured store
		// cannot tell which is down. Error text rather than err.Error()
		// because installErrSanitizer masks quoted spans in logged errors and
		// the response should not carry the SDK's own wording either.
		//
		// Reached only when EVERY target failed; a partial failure is a 200
		// naming the unreachable ones.
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
			"error":   fmt.Sprintf("could not list backups: %s is unreachable or unreadable; retry, or check the destination configuration", describeTargets(targets)),
			"target":  h.manager.TargetName(),
			"targets": targets,
		})
	}

	summaries := listing.Backups
	if summaries == nil {
		summaries = []backup.BackupSummary{}
	}

	resp := fiber.Map{
		"backups": summaries,
		"count":   len(summaries),
	}
	// The filter has to announce itself (#1085 stage B2b-1). On fresh hardware
	// every backup at the destination belongs to the instance being replaced,
	// so recovery — the case backups exist for — would otherwise be answered
	// with an empty array and no sign that anything had been withheld. The
	// count, and the hint beside it, are what make include_foreign=true
	// discoverable from the response rather than only from arc.toml.
	if listing.FilteredForeign > 0 {
		resp["filtered_foreign"] = listing.FilteredForeign
		resp["hint"] = fmt.Sprintf("%d backup(s) at this destination were written by a different Arc instance and are not listed; add ?include_foreign=true to see them, with the owner id of each", listing.FilteredForeign)
	}
	// Both fields are omitted when empty, so a single-destination response is
	// exactly the one it was.
	if len(listing.UnreachableTargets) > 0 {
		resp["unreachable_targets"] = listing.UnreachableTargets
		// Two distinct consequences, and an operator needs both: a backup held
		// ONLY there is absent from the listing, and one that spans targets is
		// present with its counts summed over the legs that answered, marked
		// partial_view with an incomplete_runs entry naming what is missing.
		resp["warning"] = fmt.Sprintf("%d backup target(s) could not be reached: a backup held only there is missing from this listing, and one that spans targets is marked partial_view with counts from the targets that answered: %s",
			len(listing.UnreachableTargets), strings.Join(listing.UnreachableTargets, ", "))
	}
	// A separate field, never mixed into backups: a client that renders that
	// array must not grow phantom entries (#1085 stage B2b-2).
	if len(listing.IncompleteRuns) > 0 {
		resp["incomplete_runs"] = listing.IncompleteRuns
	}
	return c.JSON(resp)
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
	if !backup.IsValidBackupID(id) {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "Invalid backup ID format",
		})
	}

	ctx, cancel := context.WithTimeout(c.Context(), 30*time.Second)
	defer cancel()

	// The MERGED run view at the top level — where a one-manifest reader
	// already looks for it — plus the per-target slices beside it (#1085 stage
	// B2b-2). arcli and every other existing client keep working.
	detail, err := h.manager.GetBackupDetail(ctx, id)
	if err != nil {
		// An unknown id and an unreadable destination are different answers
		// (#1085 stage B2b-1). They used to be one 404, which cost nothing
		// when the destination was a local directory and lies once it is
		// remote: a transient told the operator their backup was gone.
		if errors.Is(err, backup.ErrBackupNotFound) {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
				"error": "Backup not found",
			})
		}
		targets := h.manager.TargetNames()
		h.logger.Error().Err(err).Str("backup_id", id).Strs("targets", targets).
			Msg("Failed to read the backup manifest")
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
			"error":   fmt.Sprintf("could not read backup %s: %s is unreachable or unreadable; retry, or check the destination configuration", id, describeTargets(targets)),
			"target":  h.manager.TargetName(),
			"targets": targets,
		})
	}

	return c.JSON(detail)
}

// DeleteBackup removes a backup.
// DELETE /api/v1/backup/:id
func (h *BackupHandler) DeleteBackup(c *fiber.Ctx) error {
	if h.rejectUnlessPrimaryWriter(c, "delete") {
		return nil
	}

	id := c.Params("id")
	if !backup.IsValidBackupID(id) {
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
		// The manager's error now names the destination (#1085 stage B2b-1), and
		// the target is a field as well so an operator with more than one
		// configured store can tell which one failed. The status code is
		// deliberately left at 500: splitting it into 404/503 the way GET
		// /:id does is a separate change this stage did not scope, and no
		// behaviour here depends on the distinction.
		h.logger.Error().Err(err).Str("backup_id", id).Strs("targets", h.manager.TargetNames()).
			Msg("Failed to delete backup")
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

	if !backup.IsValidBackupID(req.BackupID) {
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
	// manifest, so the echo is what will run.
	//
	// A manifest that cannot be READ is an ERROR here, not a fall-through to
	// "as for an unscoped backup" (#1085 stage B2b-1). The manager was always
	// the authority and still is — it fails hard on a manifest read error,
	// re-resolves the mode from the manifest it read and re-runs
	// CheckScopedReplace — so the restore never actually ran with the wrong
	// mode. What the fall-through produced was a 202 that LIED: an operator
	// told "mode: merge" while the manager would run "replace", or the
	// reverse. With a local destination the read could hardly fail; with a
	// remote one it is an ordinary transient, and an operator who reads
	// "merge" and stops worrying is the hazard. 503 rather than 500 for the
	// same reason rejectUnlessPrimaryWriter uses it: the honest advice is to
	// retry.
	//
	// A manifest that is ABSENT is a different answer and keeps the behaviour
	// it has: admitted, and failed asynchronously by the manager with "backup
	// not found". The echoed mode is not a lie in that case because no
	// restore runs at all, the progress record carries the failure, and the
	// asynchronous not-found failure is a tested contract this change has no
	// reason to break. Only "the manifest may exist and I could not reach it"
	// gets the 503.
	var manifest *backup.Manifest
	{
		ctx, cancel := context.WithTimeout(c.Context(), 30*time.Second)
		mf, err := h.manager.GetBackup(ctx, req.BackupID)
		cancel()
		switch {
		case errors.Is(err, backup.ErrBackupNotFound):
			h.logger.Info().Str("backup_id", req.BackupID).Strs("targets", h.manager.TargetNames()).
				Msg("No manifest for the requested backup; admitting the restore, which will fail with backup not found")
		case err != nil:
			targets := h.manager.TargetNames()
			h.logger.Error().Err(err).Str("backup_id", req.BackupID).Strs("targets", targets).
				Msg("Could not read the backup manifest before the restore")
			return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
				"error":   fmt.Sprintf("could not read backup %s before the restore: %s is unreachable or unreadable; retry, or check the destination configuration", req.BackupID, describeTargets(targets)),
				"target":  h.manager.TargetName(),
				"targets": targets,
			})
		default:
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
	// text, whether the mode was defaulted or named. Still guarded: an
	// unreadable manifest is answered above, but an ABSENT one is admitted and
	// fails in the manager, so manifest can be nil here.
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
		ctx, cancel := context.WithTimeout(context.Background(), h.operationTimeout)
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
	// one stages nothing. A manifest that could not be READ is answered with a
	// 503 above rather than letting these flags follow the request; a manifest
	// that is ABSENT still reaches here, and the flags follow the request as
	// before, because the restore it describes fails in the manager anyway.
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
