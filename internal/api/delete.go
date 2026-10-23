package api

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	sqlutil "github.com/basekick-labs/arc/internal/sql"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/basekick-labs/arc/internal/auth"
	"github.com/basekick-labs/arc/internal/cluster"
	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/database"
	"github.com/basekick-labs/arc/internal/reconciliation"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/basekick-labs/arc/internal/throttle"
	"github.com/basekick-labs/arc/internal/tiering"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// DeleteCoordinator is the minimal cluster interface the delete handler needs
// to gate execution to the primary writer and propagate file manifest changes.
// nil = standalone mode (no gate, manifest not updated).
type DeleteCoordinator interface {
	BatchFileOpsInManifest(ops []raft.BatchFileOp) error
	GetFileEntry(path string) (*raft.FileEntry, bool)
	IsPrimaryWriter() bool
	Role() string
	// LocalNodeID names this node in the manifest. A rewrite stamps it as the
	// origin of the new bytes (#976).
	LocalNodeID() string
}

// errManifestFailure is returned when a Raft manifest update fails.
// This is non-transient (e.g. Raft quorum loss) and aborts the delete operation.
var errManifestFailure = errors.New("cluster manifest update failed")

// Why a DELETE left a file alone for tiering, in its response.
const (
	movingWhy        = "are being moved to cold storage and were left as they are (a DELETE does not reach files in cold storage, so their rows stay once the move completes)"
	coldUncheckedWhy = "were left as they are because cold storage could not be checked for a copy of them"
)

// errNotRegisteredYet reports a file on a shared object store that the cluster
// manifest does not list and this node is not registering (#1214): another
// writer's file waiting for its registration, as a rule. The DELETE leaves it
// as it is and answers 503; a retry finds it registered.
var errNotRegisteredYet = errors.New("file is not registered in the cluster manifest yet (another writer may still be registering it)")

// onlyNotRegistered reports a DELETE whose only failed files were left because
// the manifest does not list them yet (#1214): retryable, so 503. Any other
// failure is the more general answer, 207.
func onlyNotRegistered(failed, notRegistered int) bool {
	return failed == notRegistered
}

// mayBePendingElsewhere reports an unregistered object younger than the
// reconciler's grace floor, or of unknown age: one a peer may still be
// registering (#1214).
func (h *DeleteHandler) mayBePendingElsewhere(ctx context.Context, path string) bool {
	age, known := objectAge(ctx, h.storage, path)
	return !known || age < reconciliation.MinGraceWindow
}

// objectAge is how long ago the object at path was written: from the store's
// listing, or else from the flush time in an Arc file name (a rewrite's name,
// <logical>_rewrite_<nanos>.parquet, does not match and is of unknown age).
func objectAge(ctx context.Context, store storage.Backend, path string) (time.Duration, bool) {
	if ol, ok := store.(storage.ObjectLister); ok {
		if objs, err := ol.ListObjects(ctx, path); err == nil {
			for _, o := range objs {
				if o.Path == path && !o.LastModified.IsZero() {
					return time.Since(o.LastModified), true
				}
			}
		}
	}
	m := flushedFileName.FindStringSubmatch(filepath.Base(path))
	if m == nil {
		return 0, false
	}
	flushed, err := time.Parse("20060102150405", m[1]+m[2])
	if err != nil {
		return 0, false
	}
	return time.Since(flushed), true
}

// unregisteredFreshAge is how young an unregistered object on a shared store
// is left alone by a DELETE (#1265): a writer's registration outbox skips
// its storage check for a file it flushed within 30s, so a file the DELETE
// rewrote or removed that soon would be registered after it is gone, a
// manifest entry with no object. Twice that and more, for clock skew between
// the store and the writer.
var unregisteredFreshAge = 2 * time.Minute

// freshUnregistered reports an object no manifest entry covers that a
// writer may be about to register without looking at it again. Of unknown
// age it is not: an object store always dates its objects, so that is a
// file of a local backend no other writer shares.
func (h *DeleteHandler) freshUnregistered(ctx context.Context, path string) bool {
	age, known := objectAge(ctx, h.storage, path)
	return known && age < unregisteredFreshAge
}

// flushedFileName matches a flushed file's name,
// <measurement>_<YYYYMMDD>_<HHMMSS>_<nanos>.parquet, its time in UTC
// (ArrowBuffer.generateStoragePath).
var flushedFileName = regexp.MustCompile(`_(\d{8})_(\d{6})_\d{9}\.parquet$`)

// sharedManifest reports a cluster coordinator with a Raft file manifest on a
// shared object store, whatever compaction commit says.
func sharedManifest(coord any) bool {
	c, ok := coord.(interface {
		HasRaft() bool
		SharedObjectStore() bool
	})
	return ok && c.HasRaft() && c.SharedObjectStore()
}

// errSourceRetired reports that the file a rewrite was about to read is gone because a concurrent
// DELETE retired it and republished its surviving rows under a new path. The request is not wrong,
// it is stale: re-running it rebuilds the affected-file list and succeeds. Phrased without an
// apostrophe because the log masker truncates a line at the first quote.
var errSourceRetired = errors.New("source file was retired by a concurrent delete, retry the request")

// parquetRowGroupSize is the row group size used for Parquet rewrites during delete operations.
// Matches compaction's row group size to limit DuckDB's internal write buffer per group.
const parquetRowGroupSize = 122880

// fileMetadata returns the byte size and hex-encoded SHA-256 of the file at path.
func fileMetadata(path string) (sizeBytes int64, sha256hex string, err error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, "", err
	}
	defer f.Close()
	h := sha256.New()
	n, err := io.Copy(h, f)
	if err != nil {
		return 0, "", err
	}
	return n, fmt.Sprintf("%x", h.Sum(nil)), nil
}

// deleteFreeOSMemoryDebounce throttles freeOSMemoryThrottled to once per 30s, process-wide.
var deleteFreeOSMemoryDebounce = throttle.New(30 * time.Second)

// freeOSMemoryThrottled fires debug.FreeOSMemory in a goroutine at most once every 30 seconds.
// This prevents GC storms when multiple concurrent delete/retention requests complete together.
func freeOSMemoryThrottled() {
	if deleteFreeOSMemoryDebounce.TryAcquire() {
		go debug.FreeOSMemory()
	}
}

// DeleteHandler handles delete operations using file rewrite strategy
type DeleteHandler struct {
	// onFilesRemoved invalidates the query caches (and the cluster's) after
	// files were removed or rewritten: see SetOnFilesRemoved.
	onFilesRemoved func()
	db             *database.DuckDB
	storage        storage.Backend
	config         *config.DeleteConfig
	authManager    *auth.AuthManager
	coordinator    DeleteCoordinator // nil in standalone mode
	// pending is this node's file registration outbox (#1209): a file it
	// flushed may not be in the manifest yet. nil off ingest nodes.
	pending        PendingRegistrations
	tieringManager *tiering.Manager
	// moveClaims keeps the tiering migrator off a file this DELETE rewrites
	// (the tiering manager, when tiering is on).
	moveClaims moveClaimer
	// tempDir is the absolute, sandbox-allowlisted directory used by
	// rewriteS3File to stage the COPY-rewritten parquet locally before
	// uploading. MUST match one of the prefixes added to DuckDB's
	// allowed_directories in cmd/arc/main.go, otherwise the COPY ... TO
	// fails with a permission error and DELETE on S3 backends silently
	// breaks. main.go passes the same path as the import handler's upload
	// dir; the two flows have identical sandbox requirements.
	tempDir   string
	rewriteMu sync.Mutex
	// reads (standalone) leaves replaced files out of reads until they are
	// gone (retired_reads.go); nil on a cluster.
	reads *RetiredReads
	// retiredSeen: when this node first saw each journal record (the
	// ambiguity delay runs on this node's clock, delete_leftovers.go).
	retiredSeenMu sync.Mutex
	retiredSeen   map[string]time.Time
	logger        zerolog.Logger
	// flusher is the ingest buffer: a DELETE flushes the rows acknowledged
	// before it first (nil on nodes that do not ingest).
	flusher DeleteFlusher
	// recordID is the DELETE record of the rewrite running (under
	// rewriteMu), 0 without one.
	recordID uint64
}

// DeleteRequest represents a delete operation request
type DeleteRequest struct {
	Database    string `json:"database"`
	Measurement string `json:"measurement"`
	Where       string `json:"where"`
	DryRun      bool   `json:"dry_run"`
	Confirm     bool   `json:"confirm"`
	// AllowUnreachable runs the DELETE although files the cluster manifest
	// lists are not on this node (stream takeover): their rows are not
	// deleted, and the response names them.
	AllowUnreachable bool `json:"allow_unreachable,omitempty"`
}

// DeleteResponse represents a delete operation response
type DeleteResponse struct {
	Success         bool     `json:"success"`
	DeletedCount    int64    `json:"deleted_count"`
	AffectedFiles   int      `json:"affected_files"`
	RewrittenFiles  int      `json:"rewritten_files"`
	ExecutionTimeMs float64  `json:"execution_time_ms"`
	DryRun          bool     `json:"dry_run"`
	FilesProcessed  []string `json:"files_processed"`
	FailedFiles     []string `json:"failed_files,omitempty"`
	// UnreachableFiles are manifest files this node did not have: with
	// allow_unreachable, skipped; without it, why the DELETE was refused.
	UnreachableFiles []string `json:"unreachable_files,omitempty"`
	// UnflushedNodes are other nodes whose buffered rows of the scope could
	// not be flushed first (shared storage, #1265): with allow_unreachable,
	// the rows they hold are left; without it, why the DELETE was refused.
	UnflushedNodes []string `json:"unflushed_nodes,omitempty"`
	Error          string   `json:"error,omitempty"`
}

// DeleteConfigResponse represents delete configuration info
type DeleteConfigResponse struct {
	Enabled               bool              `json:"enabled"`
	ConfirmationThreshold int               `json:"confirmation_threshold"`
	MaxRowsPerDelete      int               `json:"max_rows_per_delete"`
	Implementation        string            `json:"implementation"`
	PerformanceImpact     map[string]string `json:"performance_impact"`
}

// affectedFile holds info about a file that has matching rows
type affectedFile struct {
	path         string
	matchCount   int64
	relativePath string
	// sha is the file's manifest checksum when it was scanned, with
	// compaction commit on (#1207 d2); "" for a file pending registration.
	sha string
}

// Dangerous punctuation patterns for WHERE clause validation (exact substring match)
var dangerousPunctuationPatterns = []string{
	";",  // Statement terminator
	"--", // SQL comment
	"/*", // Multi-line comment
}

// Dangerous keyword patterns for WHERE clause validation (word-boundary match)
// Uses \b word boundaries to avoid false positives on column names like "offset", "payload", "dataset"
var dangerousKeywordPattern = regexp.MustCompile(`(?i)\b(DROP|DELETE|INSERT|UPDATE|EXEC|EXECUTE|UNION|SELECT|CREATE|ALTER|COPY|ATTACH|DETACH|LOAD|INSTALL|PRAGMA|CALL|SET)\b`)

// dangerousIOFunctionPattern rejects DuckDB's filesystem-I/O table-function
// family in a DELETE WHERE clause.
//
// The keyword list above blocks SELECT, which kills the scalar-subquery vector
// that made GHSA-wmjj-g8xc-6hwr exploitable on the query endpoint — but these
// readers do not need a subquery to run. The WHERE clause is interpolated into
// `SELECT ... FROM read_parquet(...) WHERE <where>`, so a scalar expression like
// `1=1 OR list_contains(glob('/data/**'), 'x')` reaches DuckDB with no SELECT of
// its own and turns the row count into a cross-tenant existence oracle. DELETE is
// admin-gated, but an admin token is not meant to be a filesystem read primitive.
//
// Matches the function NAME anywhere in the clause, not anchored to a FROM/JOIN
// position — these can sit in any scalar expression. Kept as a separate pattern
// from the keyword list so the error message can name the offending function.
var dangerousIOFunctionPattern = regexp.MustCompile(`(?i)\b(` + strings.Join([]string{
	"read_parquet", "parquet_scan", "read_csv", "read_csv_auto",
	"read_json", "read_json_auto", "read_ndjson", "read_ndjson_auto",
	"read_text", "read_blob", "read_xlsx", "glob",
	"parquet_metadata", "parquet_schema", "parquet_file_metadata",
	"parquet_kv_metadata", "parquet_bloom_probe",
	"delta_scan", "iceberg_scan", "iceberg_metadata", "iceberg_snapshots",
	"arc_partition_agg",
}, "|") + `)\s*\(`)

// NewDeleteHandler creates a new delete handler. tempDir MUST be the same
// path cmd/arc/main.go added to the DuckDB sandbox's allowed_directories,
// otherwise the COPY ... TO inside rewriteS3File fails on S3-backed
// deployments. Logs a Warn on empty tempDir so misconfigured deployments
// surface the issue at startup rather than at the first DELETE.
func NewDeleteHandler(db *database.DuckDB, storage storage.Backend, cfg *config.DeleteConfig, authManager *auth.AuthManager, tempDir string, logger zerolog.Logger) *DeleteHandler {
	componentLogger := logger.With().Str("component", "delete-handler").Logger()
	if tempDir == "" {
		componentLogger.Warn().Msg("NewDeleteHandler called with empty tempDir — DELETE on S3 backends will fail under the DuckDB sandbox; cmd/arc/main.go should pass the sandbox-allowlisted upload directory")
	}
	return &DeleteHandler{
		db:          db,
		storage:     storage,
		config:      cfg,
		authManager: authManager,
		tempDir:     tempDir,
		logger:      componentLogger,
	}
}

// SetRetiredReads wires the standalone read source for replaced files.
func (h *DeleteHandler) SetRetiredReads(r *RetiredReads) { h.reads = r }

// SetCoordinator wires the cluster coordinator for manifest updates and role
// gating. Called after construction when cluster mode is enabled.
func (h *DeleteHandler) SetCoordinator(c DeleteCoordinator) {
	h.coordinator = c
}

// PendingRegistrations is the slice of the registration outbox a DELETE
// needs: cluster.RegistrationOutbox.
type PendingRegistrations interface {
	ReplacePending(ctx context.Context, oldPath, newPath string, sizeBytes int64, sha256 string) (cluster.ReplaceOutcome, raft.FileEntry, error)
	Withdraw(ctx context.Context, path string) (bool, cluster.ReplaceOutcome, error)
}

// SetPendingRegistrations wires the registration outbox, so a rewrite of a
// file still pending registration is announced in its place (#1210).
func (h *DeleteHandler) SetPendingRegistrations(p PendingRegistrations) {
	h.pending = p
}

// SetTieringManager wires rewrite/delete metadata updates into tiering.
func (h *DeleteHandler) SetTieringManager(tm *tiering.Manager) {
	h.tieringManager = tm
	if tm != nil {
		h.moveClaims = tm
	}
}

// moveClaimer is tiering.Manager.ClaimForDelete.
type moveClaimer interface {
	ClaimForDelete(ctx context.Context, path string) (release func(), err error)
}

// RegisterRoutes registers delete endpoints
func (h *DeleteHandler) RegisterRoutes(app *fiber.App) {
	group := app.Group("/api/v1/delete")
	if h.authManager != nil {
		group.Use(auth.RequireAdmin(h.authManager))
	}
	group.Post("/", h.handleDelete)
	group.Get("/config", h.handleGetConfig)
}

// handleGetConfig returns the current delete configuration
func (h *DeleteHandler) handleGetConfig(c *fiber.Ctx) error {
	return c.JSON(DeleteConfigResponse{
		Enabled:               h.config.Enabled,
		ConfirmationThreshold: h.config.ConfirmationThreshold,
		MaxRowsPerDelete:      h.config.MaxRowsPerDelete,
		Implementation:        "rewrite-based",
		PerformanceImpact: map[string]string{
			"writes":  "zero overhead",
			"queries": "zero overhead",
			"deletes": "expensive (file rewrites)",
		},
	})
}

// handleDelete processes delete requests
func (h *DeleteHandler) handleDelete(c *fiber.Ctx) error {
	start := time.Now()

	// Check if delete operations are enabled
	if !h.config.Enabled {
		return c.Status(fiber.StatusForbidden).JSON(DeleteResponse{
			Success: false,
			Error:   "Delete operations are disabled. Set delete.enabled=true in arc.toml to enable.",
		})
	}

	// Parse request
	var req DeleteRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(DeleteResponse{
			Success: false,
			Error:   "Invalid request body: " + err.Error(),
		})
	}

	// Validate required fields
	if req.Database == "" {
		return c.Status(fiber.StatusBadRequest).JSON(DeleteResponse{
			Success: false,
			Error:   "database is required",
		})
	}
	if req.Measurement == "" {
		return c.Status(fiber.StatusBadRequest).JSON(DeleteResponse{
			Success: false,
			Error:   "measurement is required",
		})
	}

	// These values are concatenated into storage prefixes and DuckDB paths, so
	// they must be usable as one path segment. isSafeStoragePathSegment is the
	// same rule the database endpoints apply, rather than a fourth local
	// spelling of it (#746): the previous check here rejected any ".."
	// SUBSTRING, so it refused "a..b", which every other layer accepts and
	// which names exactly one directory. That is the raw-substring shape #737
	// and #741 were each fixed for.
	//
	// ValidateGlobSafe is applied alongside it because these names end up in a
	// DuckDB read_parquet() path, where "*" and friends are pattern operators
	// rather than characters. Without it the request is accepted here and dies
	// several layers down at path resolution, and the query endpoints reject
	// the same name cleanly: one name, two verdicts at two depths.
	if !isSafeStoragePathSegment(req.Database) || storage.ValidateGlobSafe(req.Database) != nil {
		return c.Status(fiber.StatusBadRequest).JSON(DeleteResponse{
			Success: false,
			Error:   "database name contains invalid characters",
		})
	}
	// Arc's own state (_schema anchors, _compaction_state), not a database (#1012).
	if storage.IsReservedRootDir(req.Database) {
		return c.Status(fiber.StatusBadRequest).JSON(DeleteResponse{
			Success: false,
			Error:   "database name is reserved for Arc's own storage",
		})
	}
	if !isSafeStoragePathSegment(req.Measurement) || storage.ValidateGlobSafe(req.Measurement) != nil {
		return c.Status(fiber.StatusBadRequest).JSON(DeleteResponse{
			Success: false,
			Error:   "measurement name contains invalid characters",
		})
	}

	// In cluster mode, only the primary writer may execute deletes. Check before
	// findAffectedFiles to avoid running expensive DuckDB scans on reader nodes.
	if !req.DryRun && h.coordinator != nil && !h.coordinator.IsPrimaryWriter() {
		return c.Status(fiber.StatusServiceUnavailable).JSON(DeleteResponse{
			Success: false,
			Error:   fmt.Sprintf("delete rejected: node role %q is not primary writer", h.coordinator.Role()),
		})
	}

	// Validate WHERE clause
	isFullTableDelete, err := h.validateWhereClause(req.Where)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(DeleteResponse{
			Success: false,
			Error:   err.Error(),
		})
	}

	// Check if confirmation is required for full table delete
	if isFullTableDelete && !req.Confirm {
		return c.Status(fiber.StatusBadRequest).JSON(DeleteResponse{
			Success: false,
			Error:   "Full table delete detected (WHERE 1=1). Set confirm=true to proceed.",
		})
	}

	// Require confirmation for non-dry-run operations
	if !req.DryRun && !req.Confirm {
		return c.Status(fiber.StatusBadRequest).JSON(DeleteResponse{
			Success: false,
			Error:   "Confirmation required for delete operation. Set confirm=true or use dry_run=true to preview.",
		})
	}

	h.logger.Info().
		Str("database", req.Database).
		Str("measurement", req.Measurement).
		Str("where", sqlutil.ForLog(req.Where)).
		Bool("dry_run", req.DryRun).
		Msg("Processing delete request")

	ctx := c.Context()
	// With stream takeover on, the DELETE is recorded in the cluster and
	// applied again to rows acknowledged before it that reach a file later
	// (#1207 d1): its time functions are fixed to now.
	var rec deleteRecorder
	var marks map[string]uint64
	var unflushed []string
	where := req.Where
	if !req.DryRun {
		if rec = recorderOf(h.coordinator); rec != nil {
			if where, err = pinDeleteTime(req.Where, deleteNow()); err != nil {
				return c.Status(fiber.StatusBadRequest).JSON(DeleteResponse{Success: false, Error: err.Error()})
			}
		}
		if marks, err = h.flushBeforeDelete(ctx, req.Database, req.Measurement); err != nil {
			return respondDeleteUnavailable(c, DeleteResponse{Error: "Delete rejected: " + err.Error() + "; retry shortly"})
		}
		if unflushed, err = flushPeersBeforeDelete(ctx, h.coordinator, req.Database, req.Measurement, req.AllowUnreachable); err != nil {
			return respondDeleteUnavailable(c, DeleteResponse{Error: "Delete rejected: " + err.Error(), UnflushedNodes: unflushed})
		}
	}
	var unreachable []string
	// With compaction commit on, the scan set is the manifest's (#1207 d2):
	// a file it lists that is not here yet (a compaction output on its way)
	// is waited for.
	scan := scanOpts{wait: !req.DryRun, allowUnreachable: req.AllowUnreachable}
	if rec != nil {
		if unreachable, err = h.waitLocalFiles(ctx, rec, req.Database, req.Measurement, req.AllowUnreachable); err != nil {
			return respondDeleteUnavailable(c, DeleteResponse{Error: "Delete rejected: " + err.Error() + "; retry, or set allow_unreachable=true to skip them", UnreachableFiles: unreachable})
		}
	}

	// Find affected files
	affected, unlisted, leftovers, err := h.scanFiles(ctx, req.Database, req.Measurement, where, scan)
	if casActive(h.coordinator) {
		// The scan checked every manifest file of the measurement: what it
		// could not find is what is unreachable now.
		unreachable = unlisted
	}
	if errors.Is(err, errManifestNotSynced) || isContextErr(err) {
		return respondDeleteUnavailable(c, DeleteResponse{Error: "Delete rejected: " + err.Error() + "; retry shortly"})
	}
	if errors.Is(err, errUnreachableFiles) {
		return respondDeleteUnavailable(c, DeleteResponse{Error: "Delete rejected: " + err.Error() + "; retry, or set allow_unreachable=true to skip them", UnreachableFiles: unreachable})
	}
	if err != nil {
		h.logger.Error().Err(err).Msg("Failed to find affected files")
		return c.Status(fiber.StatusInternalServerError).JSON(DeleteResponse{
			Success:         false,
			Error:           "Failed to find affected files: " + err.Error(),
			ExecutionTimeMs: float64(time.Since(start).Milliseconds()),
		})
	}

	if len(affected) == 0 && rec == nil {
		if !req.DryRun && leftovers > 0 {
			// Nothing to rewrite, but a leftover the scan left out (it may
			// hold the only rows that matched) is still removed.
			h.rewriteMu.Lock()
			if h.removeLeftovers(ctx, req.Database, req.Measurement) > 0 {
				h.filesRemoved()
			}
			h.rewriteMu.Unlock()
		}
		// findAffectedFiles scanned parquet files via read_parquet — clear the cache even though
		// no rows matched, to avoid accumulating metadata for files that may later be deleted.
		h.db.ClearHTTPCache()
		freeOSMemoryThrottled()
		return c.JSON(DeleteResponse{
			Success:          true,
			DeletedCount:     0,
			AffectedFiles:    0,
			RewrittenFiles:   0,
			ExecutionTimeMs:  float64(time.Since(start).Milliseconds()),
			DryRun:           req.DryRun,
			FilesProcessed:   []string{},
			UnreachableFiles: unreachable,
			UnflushedNodes:   unflushed,
		})
	}

	// Calculate total rows to delete
	var totalToDelete int64
	for _, f := range affected {
		totalToDelete += f.matchCount
	}

	// Check max rows limit
	if totalToDelete > int64(h.config.MaxRowsPerDelete) {
		return c.Status(fiber.StatusBadRequest).JSON(DeleteResponse{
			Success: false,
			Error: fmt.Sprintf("Delete would affect %d rows, exceeding maximum of %d. "+
				"Adjust delete.max_rows_per_delete in arc.toml or refine WHERE clause.",
				totalToDelete, h.config.MaxRowsPerDelete),
		})
	}

	// Check confirmation threshold
	if totalToDelete > int64(h.config.ConfirmationThreshold) && !req.Confirm {
		return c.Status(fiber.StatusBadRequest).JSON(DeleteResponse{
			Success: false,
			Error: fmt.Sprintf("Delete would affect %d rows (threshold: %d). Set confirm=true to proceed.",
				totalToDelete, h.config.ConfirmationThreshold),
		})
	}

	h.logger.Info().
		Int("affected_files", len(affected)).
		Int64("total_rows", totalToDelete).
		Msg("Found affected files")

	// If dry run, just return stats
	if req.DryRun {
		fileNames := make([]string, len(affected))
		for i, f := range affected {
			fileNames[i] = filepath.Base(f.path)
		}

		return c.JSON(DeleteResponse{
			Success:          true,
			DeletedCount:     totalToDelete,
			AffectedFiles:    len(affected),
			RewrittenFiles:   0,
			ExecutionTimeMs:  float64(time.Since(start).Milliseconds()),
			DryRun:           true,
			FilesProcessed:   fileNames,
			UnreachableFiles: unreachable,
			UnflushedNodes:   unflushed,
		})
	}

	// Execute actual deletion (rewrite files). Partial rewrites publish a new
	// immutable path and retire the old one, so overlapping requests for the
	// same source must not both read and publish from the same pre-delete
	// snapshot. DELETE is already dominated by Parquet rewrite I/O; serialize
	// the rewrite phase rather than letting one request multiply another.
	// Held from the record on: the outbox applies records to pending files
	// under it too, and must not rewrite one between the scan and its use.
	h.rewriteMu.Lock()
	defer h.rewriteMu.Unlock()
	h.recordID = 0
	// Remove what earlier DELETEs replaced and could not remove, before
	// rewriting anything (the scan left them out; removeLeftovers says
	// which, per mode).
	if leftovers > 0 {
		h.removeLeftovers(ctx, req.Database, req.Measurement)
	}
	if rec != nil {
		// Recorded before the rewrite: rows of its scope that reach a file
		// from now on are rewritten without the matching ones. Then the
		// files registered before it are scanned again, here.
		id, err := rec.RecordDelete(ctx, req.Database, req.Measurement, where, marks)
		if err != nil {
			return respondDeleteUnavailable(c, DeleteResponse{Error: "Delete not run: recording it in the cluster failed or could not be confirmed (" + err.Error() + "); it may already apply to rows replayed later; retry to run it"})
		}
		h.recordID = id
		if unreachable, err = h.waitLocalFiles(ctx, rec, req.Database, req.Measurement, req.AllowUnreachable); err != nil {
			return respondDeleteUnavailable(c, DeleteResponse{Error: "Delete recorded, not run: " + err.Error() + "; rows the cluster replays later are already filtered; retry, or set allow_unreachable=true to skip them", UnreachableFiles: unreachable})
		}
		affected, unlisted, err = h.findAffectedFiles(ctx, req.Database, req.Measurement, where, scan)
		if casActive(h.coordinator) {
			unreachable = unlisted
		}
		if errors.Is(err, errManifestNotSynced) || isContextErr(err) {
			return respondDeleteUnavailable(c, DeleteResponse{Error: "Delete recorded, not run: " + err.Error() + "; retry shortly"})
		} else if errors.Is(err, errUnreachableFiles) {
			return respondDeleteUnavailable(c, DeleteResponse{Error: "Delete recorded, not run: " + err.Error() + "; rows the cluster replays later are already filtered; retry, or set allow_unreachable=true to skip them", UnreachableFiles: unreachable})
		} else if err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(DeleteResponse{
				Success: false, Error: "Delete recorded, not run: failed to find affected files: " + err.Error(),
				ExecutionTimeMs: float64(time.Since(start).Milliseconds()),
			})
		}
		var again int64
		for _, f := range affected {
			again += f.matchCount
		}
		if again > totalToDelete {
			// Files that arrived since the first count; the DELETE is
			// recorded already, so it runs on them too.
			h.logger.Warn().Int64("counted", totalToDelete).Int64("now", again).
				Msg("More rows match the delete than when it was checked against its limits; files arrived since")
		}
	}

	h.logger.Info().
		Int("file_count", len(affected)).
		Int64("rows", totalToDelete).
		Msg("Rewriting files to remove rows")

	var totalDeleted int64
	var rewrittenCount int
	processedFiles := make([]string, 0, len(affected))
	failedFiles := make([]string, 0, len(affected))
	// Across the scans after files changed under the DELETE (#1207 d2): every
	// file it looked at, and those that failed (not retried by a later scan).
	seen := make(map[string]bool, len(affected))
	failed := map[string]bool{}
	// Files left alone because the manifest does not list them yet (#1214):
	// retryable, so the answer is 503 rather than 207.
	var notRegistered []string
	var moving []string
	var coldUnchecked []string

	for round := 0; ; round++ {
		var changed []string
		for _, f := range affected {
			if failed[f.relativePath] {
				continue
			}
			seen[f.relativePath] = true
			deleted, err := h.rewriteFileWithoutDeletedRows(ctx, f.path, f.relativePath, where, f.sha)
			if err != nil && casActive(h.coordinator) && (errors.Is(err, errFileChanged) || errors.Is(err, errSourceRetired)) {
				// A compaction replaced the file while this DELETE ran (#1207
				// d2): its output holds the rows now. Scanned again below.
				changed = append(changed, f.relativePath)
				continue
			}
			if err != nil {
				// Post-rewrite bookkeeping/storage failures still mean the rows were
				// removed from the newly published file. rewriteFileWithoutDeletedRows
				// returns zero only when the manifest was not committed and the old
				// file remains authoritative.
				totalDeleted += deleted
				if errors.Is(err, tiering.ErrFileMoving) || errors.Is(err, tiering.ErrColdCheckFailed) {
					// Expected while a migration runs; the response says so.
					h.logger.Warn().Err(err).Str("file", f.path).Msg("File left as it is: it is being moved to cold storage")
				} else {
					h.logger.Error().Err(err).Str("file", f.path).Msg("Failed to rewrite file")
				}
				// Manifest failures are non-transient (Raft quorum loss) — abort the
				// entire operation to avoid deleting files from storage without a
				// corresponding manifest record.
				if errors.Is(err, errManifestFailure) {
					h.db.ClearHTTPCache()
					h.filesRemoved()
					freeOSMemoryThrottled()
					return c.Status(fiber.StatusInternalServerError).JSON(DeleteResponse{
						Success:         false,
						Error:           "Delete aborted: " + err.Error(),
						DeletedCount:    totalDeleted,
						AffectedFiles:   len(seen),
						RewrittenFiles:  rewrittenCount,
						FilesProcessed:  processedFiles,
						FailedFiles:     failedFiles,
						ExecutionTimeMs: float64(time.Since(start).Milliseconds()),
					})
				}
				failed[f.relativePath] = true
				failedFiles = append(failedFiles, filepath.Base(f.path))
				if errors.Is(err, errNotRegisteredYet) {
					notRegistered = append(notRegistered, f.relativePath)
				}
				if errors.Is(err, tiering.ErrFileMoving) {
					moving = append(moving, f.relativePath)
				}
				if errors.Is(err, tiering.ErrColdCheckFailed) {
					coldUnchecked = append(coldUnchecked, f.relativePath)
				}
				continue
			}
			totalDeleted += deleted

			rewrittenCount++
			processedFiles = append(processedFiles, filepath.Base(f.path))

			h.logger.Debug().
				Str("file", filepath.Base(f.path)).
				Int64("deleted", deleted).
				Msg("Processed file")
		}
		if len(changed) == 0 {
			break
		}
		incomplete := func(why string) error {
			h.db.ClearHTTPCache()
			h.filesRemoved()
			freeOSMemoryThrottled()
			return respondDeleteUnavailable(c, DeleteResponse{
				Error:            "Delete incomplete: " + why + "; the rows of the other files are deleted; retry to finish",
				DeletedCount:     totalDeleted,
				AffectedFiles:    len(seen),
				RewrittenFiles:   rewrittenCount,
				FilesProcessed:   processedFiles,
				FailedFiles:      failedFiles,
				UnreachableFiles: unreachable,
				UnflushedNodes:   unflushed,
				ExecutionTimeMs:  float64(time.Since(start).Milliseconds()),
			})
		}
		if round+1 >= deleteChangedRounds {
			return incomplete(fmt.Sprintf("%d files kept changing under it (compaction)", len(changed)))
		}
		h.logger.Info().Int("files", len(changed)).Int("round", round+1).
			Msg("Files changed under the delete (compaction replaced them); scanning again")
		var missing []string
		affected, missing, err = h.rescanAfterChange(ctx, req.Database, req.Measurement, where, scan)
		unreachable = missing
		if err != nil {
			return incomplete(err.Error())
		}
	}

	// Clear DuckDB parquet metadata/data cache and release memory back to OS.
	h.db.ClearHTTPCache()
	h.filesRemoved()
	freeOSMemoryThrottled()

	executionTime := float64(time.Since(start).Milliseconds())

	h.logger.Info().
		Int64("deleted_count", totalDeleted).
		Int("rewritten_files", rewrittenCount).
		Int("failed_files", len(failedFiles)).
		Float64("execution_time_ms", executionTime).
		Msg("Delete operation completed")

	resp := DeleteResponse{
		Success:          len(failedFiles) == 0,
		DeletedCount:     totalDeleted,
		AffectedFiles:    len(seen),
		RewrittenFiles:   rewrittenCount,
		ExecutionTimeMs:  executionTime,
		DryRun:           false,
		FilesProcessed:   processedFiles,
		FailedFiles:      failedFiles,
		UnreachableFiles: unreachable,
		UnflushedNodes:   unflushed,
	}

	// A file left for tiering's move is not "retry later": once moved it is in
	// cold storage, which a DELETE does not reach. It answers 207 with the
	// reason, like any other file that failed.
	if !onlyNotRegistered(len(failedFiles), len(notRegistered)+len(coldUnchecked)) {
		// A failure that is not only "retry later" is the more general one:
		// 207, naming both.
		resp.Error = fmt.Sprintf("%d of %d files failed to process", len(failedFiles), len(seen))
		if len(notRegistered) > 0 {
			resp.Error += fmt.Sprintf(" (%d of them are not registered in the cluster manifest yet and were left as they are; retry for those)", len(notRegistered))
		}
		if len(moving) > 0 {
			resp.Error += fmt.Sprintf(" (%d of them %s)", len(moving), movingWhy)
		}
		if len(coldUnchecked) > 0 {
			resp.Error += fmt.Sprintf(" (%d of them %s; retry for those)", len(coldUnchecked), coldUncheckedWhy)
		}
		return c.Status(fiber.StatusMultiStatus).JSON(resp)
	}
	if len(notRegistered) > 0 || len(coldUnchecked) > 0 {
		var why []string
		if len(notRegistered) > 0 {
			why = append(why, fmt.Sprintf("%d of %d files are not registered in the cluster manifest yet (another writer may still be registering them) and were left as they are", len(notRegistered), len(seen)))
		}
		if len(coldUnchecked) > 0 {
			why = append(why, fmt.Sprintf("%d of %d files %s", len(coldUnchecked), len(seen), coldUncheckedWhy))
		}
		resp.Error = "Delete incomplete: " + strings.Join(why, "; ") + "; the rows of the other files are deleted; retry to finish"
		return respondDeleteUnavailable(c, resp)
	}

	return c.JSON(resp)
}

// validateWhereClause validates the WHERE clause and returns true if it's a full table delete
func (h *DeleteHandler) validateWhereClause(where string) (bool, error) {
	if where == "" || strings.TrimSpace(where) == "" {
		return false, fmt.Errorf("WHERE clause is required. To delete all data, use WHERE clause '1=1' with confirm=true")
	}

	// Validation scans run on the MASKED clause: a forbidden keyword or ';'
	// inside a string literal is data, not SQL (#834). The raw clause is what
	// gets interpolated into the DuckDB statement, so the unmatched-quote and
	// unmatched-parenthesis checks below stay on it. Backtick identifiers are
	// normalised to double quotes first, as the query path's validator does:
	// the masker does not know backticks, so a quote inside one would
	// otherwise open a spurious literal that hides whatever follows it.
	maskInput := backticksToDoubleQuotes(where)
	maskedWhere, masks := sqlutil.MaskStringLiterals(maskInput, sqlutil.HasQuotes(maskInput))
	// DuckDB reads non-ASCII spaces as token separators; the keyword and
	// function scans below only see ASCII ones (read_statements.go).
	maskedWhere = foldUnicodeSpace(maskedWhere)
	whereUpper := strings.ToUpper(strings.TrimSpace(maskedWhere))

	// Remove "WHERE" prefix if present
	if strings.HasPrefix(whereUpper, "WHERE ") {
		whereUpper = strings.TrimSpace(whereUpper[6:])
	}

	// Check for dangerous punctuation patterns (exact substring match)
	for _, pattern := range dangerousPunctuationPatterns {
		if strings.Contains(whereUpper, pattern) {
			return false, fmt.Errorf("WHERE clause contains forbidden pattern: %s", pattern)
		}
	}

	// Check for dangerous SQL keywords using word boundaries to avoid false positives
	// on column names like "offset" (contains SET), "payload" (contains LOAD), "dataset" (contains SET)
	// Match DuckDB's token boundaries when a keyword is glued to a number or a
	// masked string literal (#1080), as the query validator does.
	keywordCheck := numberGluedToWord.ReplaceAllString(strings.ToLower(maskedWhere), "$1$2 $3")
	keywordCheck = maskPlaceholder.ReplaceAllString(keywordCheck, " ")
	if match := dangerousKeywordPattern.FindString(keywordCheck); match != "" {
		return false, fmt.Errorf("WHERE clause contains forbidden keyword: %s", strings.ToUpper(match))
	}

	// Reject filesystem-I/O table functions (see dangerousIOFunctionPattern).
	// Matched against the identifier-quote-stripped form so the quoted spelling
	// `"glob"(...)`, which DuckDB executes identically, cannot slip past.
	ioCheck := maskedWhere
	for _, mask := range masks {
		if mask.Identifier {
			// Keep quoted identifiers visible to the function-name check while
			// leaving string-literal contents masked.
			ioCheck = strings.ReplaceAll(ioCheck, mask.Placeholder, mask.Original)
		}
	}
	ioCheck = strings.NewReplacer(`"`, "", "`", "").Replace(ioCheck)
	if m := dangerousIOFunctionPattern.FindStringSubmatch(ioCheck); m != nil {
		return false, fmt.Errorf("WHERE clause contains forbidden file I/O function: %s()", m[1])
	}
	if name := sideEffectFunction(ioCheck); name != "" {
		return false, fmt.Errorf("WHERE clause contains forbidden function: %s", name)
	}

	// SECURITY: reject a path literal standing in table position inside the
	// fragment. The keyword and I/O-function scans above cannot see this
	// class: a replacement scan has no function name to match, and the
	// keyword list blocks SELECT and UNION but not the other spellings
	// DuckDB accepts for introducing a relation — `EXISTS (FROM '<glob>')`
	// and `EXISTS (TABLE '<glob>')` carry neither.
	//
	// That matters here because the fragment is interpolated straight into
	// `SELECT ... FROM read_parquet(...) WHERE <fragment>` (findAffectedFiles,
	// and the per-file count below it), so a literal standing in table
	// position there is resolved by DuckDB rather than treated as a value —
	// and the response carries row and file counts derived from it, dry-run
	// included.
	//
	// Reuses the query path's guard rather than growing a third keyword list
	// in this file: that guard is fed by the same normalisation and already
	// knows every relation-introducing keyword DuckDB has (FROM, JOIN, TABLE,
	// SUMMARIZE, DESCRIBE, PIVOT, UNPIVOT), so this inherits additions to it
	// instead of drifting from them.
	//
	// It is applied to the FRAGMENT, not to the assembled statement: by the
	// time the statement exists the fragment sits inside Arc's own
	// read_parquet(...), which would self-trip the I/O denylist — the same
	// ordering constraint queryMeasurement documents. A fragment that
	// introduces a relation carries its own keyword, so the scanner arms
	// without needing a synthetic FROM clause around it.
	features := scanSQLFeatures(maskInput)
	normalised, _ := sqlutil.MaskStringLiterals(maskInput, features.hasQuotes)
	normalised = stripSQLComments(normalised, features.hasDashComment || features.hasBlockComment)
	if stringLiteralInTablePosition(normalised) {
		return false, fmt.Errorf("WHERE clause may not put a string literal in table position (replacement scans are disabled)")
	}

	// Check for unmatched quotes
	if strings.Count(where, "'")%2 != 0 {
		return false, fmt.Errorf("WHERE clause has unmatched quotes")
	}

	// Check for unmatched parentheses
	if strings.Count(where, "(") != strings.Count(where, ")") {
		return false, fmt.Errorf("WHERE clause has unmatched parentheses")
	}

	// Check for dangerous full table delete patterns
	dangerousPatterns := []string{"1=1", "TRUE", "1"}
	for _, pattern := range dangerousPatterns {
		if whereUpper == pattern {
			return true, nil // Full table delete
		}
	}

	return false, nil
}

// findAffectedFiles finds all Parquet files that contain rows matching the WHERE clause
func (h *DeleteHandler) findAffectedFiles(ctx context.Context, database, measurement, whereClause string, opts scanOpts) ([]affectedFile, []string, error) {
	affected, unreachable, _, err := h.scanFiles(ctx, database, measurement, whereClause, opts)
	return affected, unreachable, err
}

// scanFiles is findAffectedFiles, also returning how many leftovers of
// earlier DELETEs it left out (delete_leftovers.go).
func (h *DeleteHandler) scanFiles(ctx context.Context, database, measurement, whereClause string, opts scanOpts) ([]affectedFile, []string, int, error) {
	var affected []affectedFile

	// Use storage backend's List method to find parquet files
	prefix := fmt.Sprintf("%s/%s/", database, measurement)
	files, err := h.storage.List(ctx, prefix)
	if err != nil {
		return nil, nil, 0, fmt.Errorf("failed to list files: %w", err)
	}

	// Filter for parquet files and convert to query paths
	var parquetFiles []fileInfo
	for _, f := range files {
		if !strings.HasSuffix(strings.ToLower(f), ".parquet") {
			continue
		}
		queryPath, err := readParquetPath(h.storage, f)
		if err != nil {
			// A delete must not silently leave matching rows behind, so an
			// unusable key aborts rather than being skipped: the caller reports
			// how many rows it removed, and skipping would make that a lie.
			return nil, nil, 0, fmt.Errorf("listed file %q has no usable storage path: %w", f, err)
		}
		parquetFiles = append(parquetFiles, fileInfo{
			queryPath:    queryPath,
			relativePath: f,
		})
	}

	// Files an earlier DELETE replaced and could not remove are never read
	// into a new file (delete_leftovers.go).
	// With compaction commit on, scanSet does it right after its own sync.
	leftovers := 0
	if !casActive(h.coordinator) {
		var left []leftover
		var err error
		if parquetFiles, left, err = h.leftoverFiles(ctx, parquetFiles); err != nil {
			return nil, nil, 0, err
		}
		h.logLeftovers(left)
		leftovers = len(left)
	}

	// With compaction commit on (#1207 d2), the files the cluster lists, or
	// this node is still registering, are the DELETE's (scanSet): anything
	// else in storage is not data yet (a compaction output before its commit)
	// or no longer (an input after it), and rewriting it would orphan the
	// rewrite.
	var shas map[string]string
	var unreachable []string
	if casActive(h.coordinator) {
		if parquetFiles, shas, unreachable, err = h.scanSet(ctx, database, measurement, parquetFiles, opts, &leftovers); err != nil {
			return nil, unreachable, 0, err
		}
	}

	if len(parquetFiles) == 0 {
		h.logger.Warn().Str("prefix", prefix).Msg("No parquet files found for measurement")
		return affected, unreachable, leftovers, nil
	}

	h.logger.Info().Int("file_count", len(parquetFiles)).Msg("Scanning parquet files for matching rows")

	// Optimized: Use batch query to count matching rows across all files in one query
	// This replaces N individual queries with a single query using read_parquet with file list
	affected, err = h.countMatchingRowsInFiles(ctx, parquetFiles, whereClause)
	if err != nil {
		// Fallback to individual queries if batch fails (e.g., schema mismatch)
		h.logger.Warn().Err(err).Msg("Batch count failed, falling back to individual queries")
		if affected, err = h.countMatchingRowsIndividually(ctx, parquetFiles, whereClause); err != nil {
			return affected, unreachable, leftovers, err
		}
	}
	for i := range affected {
		affected[i].sha = shas[affected[i].relativePath]
	}
	return affected, unreachable, leftovers, nil
}

// fileInfo holds query and relative paths for a parquet file
type fileInfo struct {
	queryPath    string
	relativePath string
}

// countMatchingRowsInFiles counts matching rows across multiple files in a single query
func (h *DeleteHandler) countMatchingRowsInFiles(ctx context.Context, files []fileInfo, whereClause string) ([]affectedFile, error) {
	if len(files) == 0 {
		return nil, nil
	}

	db := h.db.DB()

	// Build array of file paths for DuckDB, escaping single quotes in paths.
	var pathList strings.Builder
	pathList.WriteString("[")
	for i, f := range files {
		if i > 0 {
			pathList.WriteString(", ")
		}
		pathList.WriteString(sqlutil.QuoteStringLiteral(f.queryPath))
	}
	pathList.WriteString("]")

	// Single query to get counts per file using filename column
	query := fmt.Sprintf(`
		SELECT filename, COUNT(*) as match_count
		FROM %s
		WHERE %s
		GROUP BY filename
		HAVING COUNT(*) > 0`,
		sqlutil.ReadParquet(pathList.String(), "filename=true", "union_by_name=true"), whereClause)

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("batch count query failed: %w", err)
	}
	defer rows.Close()

	// Build map of query path -> relative path for lookup
	pathMap := make(map[string]string)
	for _, f := range files {
		pathMap[f.queryPath] = f.relativePath
	}

	var affected []affectedFile
	for rows.Next() {
		var filename string
		var count int64
		if err := rows.Scan(&filename, &count); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		relativePath, ok := pathMap[filename]
		if !ok {
			// Try to find by suffix match (DuckDB may return absolute paths)
			for qp, rp := range pathMap {
				if strings.HasSuffix(filename, filepath.Base(qp)) {
					relativePath = rp
					ok = true
					break
				}
			}
		}
		if !ok {
			h.logger.Warn().Str("filename", filename).Msg("Could not map filename to relative path")
			continue
		}

		affected = append(affected, affectedFile{
			path:         filename,
			matchCount:   count,
			relativePath: relativePath,
		})
		h.logger.Debug().Str("file", filepath.Base(relativePath)).Int64("matches", count).Msg("Found matching rows")
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	return affected, nil
}

// countMatchingRowsIndividually is the fallback when batch query fails
func (h *DeleteHandler) countMatchingRowsIndividually(ctx context.Context, files []fileInfo, whereClause string) ([]affectedFile, error) {
	var affected []affectedFile
	db := h.db.DB()

	for _, f := range files {
		query := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s", sqlutil.ReadParquet(sqlutil.QuoteStringLiteral(f.queryPath)), whereClause)
		var count int64
		if err := db.QueryRowContext(ctx, query).Scan(&count); err != nil {
			h.logger.Warn().Err(err).Str("file", f.relativePath).Msg("Failed to count matching rows, skipping file")
			continue
		}

		if count > 0 {
			affected = append(affected, affectedFile{
				path:         f.queryPath,
				matchCount:   count,
				relativePath: f.relativePath,
			})
			h.logger.Debug().Str("file", filepath.Base(f.relativePath)).Int64("matches", count).Msg("Found matching rows")
		}
	}

	return affected, nil
}

// rewriteFileWithoutDeletedRows rewrites a Parquet file excluding rows that match the WHERE clause
// For local storage: uses atomic rename
// For S3: downloads, processes locally, then uploads
func (h *DeleteHandler) rewriteFileWithoutDeletedRows(ctx context.Context, queryPath, relativePath, whereClause, scanSHA string) (int64, error) {
	// The affected-file list was built before the rewrite mutex was acquired, so a delete that
	// won the lock first may already have retired this source and published its survivors under a
	// new immutable path. Detect that here rather than letting DuckDB report it: the engine
	// surfaces a missing source as "IO Error: No files found that match the pattern", which tells
	// an operator nothing about what to do. Both backends report an absent object as (-1, nil)
	// from StatFile, so this needs no error-string matching. The mutex is held for the whole
	// rewrite phase, so nothing can retire the file between this check and the read below.
	if size, statErr := h.storage.StatFile(ctx, relativePath); statErr == nil && size < 0 {
		return 0, fmt.Errorf("%w: %s", errSourceRetired, filepath.Base(relativePath))
	}
	// Kept off a file the tiering migrator is copying, or one with a cold
	// copy already: a rewrite here would leave that copy holding the rows
	// this DELETE removes, and the cold sync would serve them again for
	// good. Claimed for the whole rewrite, tier metadata included.
	if h.moveClaims != nil {
		release, err := h.moveClaims.ClaimForDelete(ctx, relativePath)
		if err != nil {
			return 0, err
		}
		defer release()
	}
	if scanSHA == unmanagedSHA && peerFlushOn(h.coordinator) && h.freshUnregistered(ctx, relativePath) {
		// A writer's flush still on its way to the manifest (#1265): left
		// as it is, and the DELETE retried, which finds it registered.
		return 0, fmt.Errorf("%w: %s", errNotRegisteredYet, filepath.Base(relativePath))
	}

	// Use the shared DuckDB connection to avoid memory retention from temporary connections
	db := h.db.DB()

	// Optimized: Single query to count both total rows and rows to keep
	// Uses COUNT(*) FILTER to get conditional count in one scan. A row the
	// clause cannot decide (NULL, e.g. host <> 'a' on a NULL host) is not
	// matched, as the scan counts it, so it is kept: NOT on its own would
	// drop it from the rewrite without counting it deleted.
	var rowsBefore, rowsAfter int64
	countQuery := fmt.Sprintf(`
		SELECT
			COUNT(*) as total,
			COUNT(*) FILTER (WHERE NOT COALESCE((%s), false)) as remaining
		FROM %s`,
		whereClause, sqlutil.ReadParquet(sqlutil.QuoteStringLiteral(queryPath)))

	if err := db.QueryRowContext(ctx, countQuery).Scan(&rowsBefore, &rowsAfter); err != nil {
		return 0, fmt.Errorf("failed to count rows: %w", err)
	}

	deleted := rowsBefore - rowsAfter
	if deleted == 0 {
		// Nothing matches now (a clause on now() that matched at the scan):
		// a rewrite would be the same rows under another name.
		return 0, nil
	}

	// If all rows would be deleted, remove the file entirely.
	if rowsAfter == 0 {
		h.logger.Info().Str("file", filepath.Base(relativePath)).Int64("deleted", deleted).Msg("All rows deleted, removing file")

		// A file still pending registration must not be registered after it is
		// gone (#1210). Taken out of the outbox first; if it was confirmed
		// meanwhile, the manifest delete below removes it.
		if h.pending != nil {
			withdraw := h.pending.Withdraw
			if w, ok := h.pending.(deleteWithdrawer); ok && h.recordID != 0 {
				// Batches of another writer process settle as deleted too.
				withdraw = func(ctx context.Context, path string) (bool, cluster.ReplaceOutcome, error) {
					return w.WithdrawForDelete(ctx, path, h.recordID)
				}
			}
			if _, _, err := withdraw(ctx, relativePath); err != nil {
				return 0, fmt.Errorf("withdraw pending registration for %s: %w", relativePath, err)
			}
		}

		// Manifest-before-storage: record the delete in the Raft manifest before
		// removing from storage. If the manifest update fails (Raft quorum loss),
		// abort — the file still exists in storage and the next delete will retry.
		if scanSHA == unmanagedSHA {
			// No manifest entry covered it when scanned (a shared object
			// store): one that does now is scanned again.
			if _, reg := h.coordinator.GetFileEntry(relativePath); reg {
				return 0, fmt.Errorf("%w: %s", errFileChanged, filepath.Base(relativePath))
			}
		} else if h.coordinator != nil {
			// With compaction commit on, the delete removes the content this
			// DELETE read, never a file a compaction replaced meanwhile (#1207 d2).
			expect := ""
			if casActive(h.coordinator) {
				expect = scanSHA
				if expect == "" {
					// Pending when scanned: confirmed since, or withdrawn above.
					if e, ok := h.coordinator.GetFileEntry(relativePath); ok {
						expect = e.SHA256
					}
				}
			}
			payload, err := json.Marshal(raft.DeleteFilePayload{Path: relativePath, Reason: deleteManifestReason, ExpectSHA: expect})
			if err != nil {
				return 0, fmt.Errorf("%w: marshal failed for %s: %v", errManifestFailure, relativePath, err)
			}
			if err := h.coordinator.BatchFileOpsInManifest([]raft.BatchFileOp{{Type: raft.CommandDeleteFile, Payload: payload}}); err != nil {
				if raft.IsManifestChanged(err) {
					return 0, fmt.Errorf("%w: %s", errFileChanged, filepath.Base(relativePath))
				}
				return 0, fmt.Errorf("%w: %v", errManifestFailure, err)
			}
		}

		if err := deleteReplaced(ctx, h.storage, relativePath); err != nil {
			// Manifest is already committed — storage failure is transient (network
			// blip, file already gone). Log as Warn and return the deleted count;
			// the Phase 5 reconciler at /api/v1/reconciliation cleans up the
			// orphaned manifest entry (manifest-references-missing-file is the
			// orphan-manifest case).
			h.logger.Warn().Err(err).Str("file", relativePath).Msg("Failed to delete file from storage after manifest update")
			return deleted, nil
		}
		h.dropFinalizingInputs(ctx, relativePath)
		return deleted, nil
	}

	// For remote backends (S3, Azure), write to a local temp file then upload.
	var rewroteDeleted int64
	var rewrittenPath string
	var s3Result *s3RewriteResult
	var rewriteErr error
	if h.isRemoteBackend() {
		rewroteDeleted, s3Result, rewriteErr = h.rewriteS3File(ctx, queryPath, relativePath, whereClause, rowsBefore, rowsAfter)
		if s3Result != nil {
			rewrittenPath = s3Result.newPath
		}
	} else {
		rewroteDeleted, rewrittenPath, rewriteErr = h.rewriteLocalFile(ctx, queryPath, relativePath, whereClause, rowsBefore, rowsAfter)
	}
	if rewriteErr != nil {
		return 0, rewriteErr
	}

	// Partial rewrites are immutable: publish the rewritten file under a new
	// path and delete the old manifest entry in the same Raft batch. Readers
	// therefore observe a register+delete instead of an in-place checksum/size
	// mutation, eliminating the same-size stale-file class (#975).
	if rewrittenPath == "" {
		return rewroteDeleted, nil
	}
	if h.coordinator != nil {
		if err := h.replaceManifestAfterRewrite(ctx, relativePath, rewrittenPath, s3Result, scanSHA); err != nil {
			if errors.Is(err, errManifestFailure) || errors.Is(err, errFileChanged) || errors.Is(err, errNotRegisteredYet) {
				return 0, err
			}
			return rewroteDeleted, err
		}
	} else {
		// The rewrite is in storage: finish even if the client goes away.
		fctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), rewriteFinishTimeout)
		defer cancel()
		// Reads leave the old file out from now (retired_reads.go), until it
		// is gone.
		h.reads.add(relativePath, rewrittenPath)
		// Recorded first: if the removal fails, or the process stops before
		// it, the drainer and every later DELETE know relativePath is
		// replaced (delete_leftovers.go).
		jerr := h.journalRetired(fctx, relativePath, rewrittenPath)
		if jerr != nil {
			h.logger.Warn().Err(jerr).Str("file", relativePath).Msg("Could not record a file this DELETE replaced before removing it")
		}
		if err := deleteReplaced(fctx, h.storage, relativePath); err != nil {
			h.logger.Error().Err(err).Str("file", relativePath).Str("new_file", rewrittenPath).Bool("recorded", jerr == nil).
				Msg("Rewrote the file but could not remove the old one; queries read its surviving rows twice until it is removed")
			return rewroteDeleted, fmt.Errorf("failed to delete superseded rewritten file %q: %w", relativePath, err)
		}
		h.reads.remove(relativePath)
		if jerr == nil {
			h.clearRetired(fctx, relativePath)
		}
	}
	if err := h.recordRewriteTiering(ctx, relativePath, rewrittenPath, s3Result); err != nil {
		return rewroteDeleted, err
	}

	return rewroteDeleted, nil
}

// rewriteLocalFile handles file rewrite for local storage by publishing a fresh immutable path
func (h *DeleteHandler) rewriteLocalFile(ctx context.Context, filePath, relativePath, whereClause string, rowsBefore, rowsAfter int64) (int64, string, error) {
	db := h.db.DB()
	deleted := rowsBefore - rowsAfter
	newRelativePath := rewritePath(relativePath)
	newFilePath := filepath.Join(filepath.Dir(filePath), filepath.Base(newRelativePath))

	// Create temp file for the rewritten data. Key it from the immutable output
	// name so concurrent rewrites cannot collide on one source's staging path.
	dir := filepath.Dir(filePath)
	tempDir := filepath.Join(dir, ".tmp")
	if err := os.MkdirAll(tempDir, 0700); err != nil {
		return 0, "", fmt.Errorf("failed to create temp directory: %w", err)
	}

	tempFile := filepath.Join(tempDir, filepath.Base(newRelativePath)+".new")

	// Write filtered data to temp file using DuckDB COPY. Run with
	// preserve_insertion_order forced on so the rewritten file keeps the
	// source's row order (typically time-sorted) even when the
	// database-wide setting is false.
	copyQuery := fmt.Sprintf(`
		COPY (
			SELECT * FROM %s WHERE NOT COALESCE((%s), false)
		) TO %s (
			FORMAT PARQUET,
			COMPRESSION ZSTD,
			COMPRESSION_LEVEL 3,
			ROW_GROUP_SIZE %d
		)`, sqlutil.ReadParquet(sqlutil.QuoteStringLiteral(filePath)), whereClause, sqlutil.QuoteStringLiteral(tempFile), parquetRowGroupSize)

	if err := database.ExecPreservingInsertionOrder(ctx, db, copyQuery); err != nil {
		os.Remove(tempFile)
		return 0, "", fmt.Errorf("failed to write filtered data: %w", err)
	}

	// Publish the rewritten bytes under a fresh immutable path. Keep the old
	// file intact until the manifest register+delete transaction commits.
	if err := os.Rename(tempFile, newFilePath); err != nil {
		os.Remove(tempFile)
		return 0, "", fmt.Errorf("failed to publish rewritten file: %w", err)
	}

	// Try to clean up temp directory if empty
	os.Remove(tempDir) // Ignore error, directory might not be empty

	h.logger.Info().
		Str("file", filepath.Base(filePath)).
		Int64("rows_before", rowsBefore).
		Int64("rows_after", rowsAfter).
		Int64("deleted", deleted).
		Msg("Rewrote file")

	return deleted, newRelativePath, nil
}

// s3RewriteResult holds the outcome of rewriteS3File for manifest replacement.
type s3RewriteResult struct {
	newPath   string
	sizeBytes int64
	sha256    string
}

// rewritePath returns a fresh path in the same partition directory. Keeping
// rewritten files immutable makes a content change visible as register+delete
// in the manifest instead of an in-place UpdateFile (#975).
func rewritePath(relativePath string) string {
	logicalPath := storage.StripRewriteSuffix(relativePath)
	base := strings.TrimSuffix(logicalPath, filepath.Ext(logicalPath))
	return fmt.Sprintf("%s_rewrite_%d.parquet", base, storage.NameTime(time.Now()).UnixNano())
}

// rewriteS3File handles file rewrite for S3 storage
// DuckDB can read from S3 directly, then we write to a temp file and upload
func (h *DeleteHandler) rewriteS3File(ctx context.Context, s3Path, relativePath, whereClause string, rowsBefore, rowsAfter int64) (int64, *s3RewriteResult, error) {
	// Fail-closed when the handler was constructed without a sandbox-
	// allowlisted tempDir. Without this guard, os.CreateTemp("", ...) would
	// fall back to os.TempDir() — outside the sandbox — and the subsequent
	// COPY ... TO would fail with a confusing DuckDB permission error. The
	// constructor Warn surfaces the misconfiguration at startup; this
	// fail-fast at request time provides a clearer signal in the response
	// body for the (rare) case where a constructor warning was missed.
	if h.tempDir == "" {
		return 0, nil, fmt.Errorf("delete handler is misconfigured: tempDir is empty; the DELETE-on-S3 staging directory must be allowlisted in the DuckDB sandbox (see cmd/arc/main.go uploadDir wiring)")
	}
	db := h.db.DB()
	deleted := rowsBefore - rowsAfter
	newRelativePath := rewritePath(relativePath)

	// Create temp file locally for the rewritten data. The destination
	// directory MUST be inside DuckDB's allowed_directories — main.go
	// passes the same sandbox-allowlisted path as the import-upload dir.
	tempFile, err := os.CreateTemp(h.tempDir, "arc-delete-*.parquet")
	if err != nil {
		return 0, nil, fmt.Errorf("failed to create temp file: %w", err)
	}
	// ToSlash so Windows backslashes from os.CreateTemp match the
	// forward-slash sandbox allowlist entry (h.tempDir is already
	// normalized by main.go). The COPY ... TO statement below
	// interpolates tempPath verbatim into SQL, so a backslash form
	// would mismatch allowed_directories and the rewrite would fail.
	tempPath := filepath.ToSlash(tempFile.Name())
	tempFile.Close()
	defer os.Remove(tempPath)

	// DuckDB reads from S3 and writes to local temp file. Run with
	// preserve_insertion_order forced on so the rewritten file keeps the
	// source's row order (typically time-sorted) even when the
	// database-wide setting is false.
	copyQuery := fmt.Sprintf(`
		COPY (
			SELECT * FROM %s WHERE NOT COALESCE((%s), false)
		) TO %s (
			FORMAT PARQUET,
			COMPRESSION ZSTD,
			COMPRESSION_LEVEL 3,
			ROW_GROUP_SIZE %d
		)`, sqlutil.ReadParquet(sqlutil.QuoteStringLiteral(s3Path)), whereClause, sqlutil.QuoteStringLiteral(tempPath), parquetRowGroupSize)

	if err := database.ExecPreservingInsertionOrder(ctx, db, copyQuery); err != nil {
		return 0, nil, fmt.Errorf("failed to write filtered data: %w", err)
	}

	// Compute SHA256 by full-reading the temp file, then Seek(0,0) and pass
	// the seekable *os.File to the storage backend. Two reads of the same
	// file, but the second hits OS page cache so disk is touched once.
	//
	// One pass with io.TeeReader would hand the backend a non-seekable body.
	// The S3 backend buffers such bodies itself now, but a seekable *os.File
	// lets it upload straight from the file (and rewind for retries) instead
	// of copying it into memory, so the two-pass read stays.
	uploadFile, err := os.Open(tempPath)
	if err != nil {
		return 0, nil, fmt.Errorf("failed to open temp file for upload: %w", err)
	}
	defer uploadFile.Close()

	info, err := uploadFile.Stat()
	if err != nil {
		return 0, nil, fmt.Errorf("failed to stat temp file: %w", err)
	}
	sizeBytes := info.Size()

	h256 := sha256.New()
	if _, err := io.Copy(h256, uploadFile); err != nil {
		return 0, nil, fmt.Errorf("failed to compute SHA256 of rewritten file: %w", err)
	}
	sha256hex := fmt.Sprintf("%x", h256.Sum(nil))

	if _, err := uploadFile.Seek(0, io.SeekStart); err != nil {
		return 0, nil, fmt.Errorf("failed to rewind rewritten file for upload: %w", err)
	}

	if err := h.storage.WriteReader(ctx, newRelativePath, uploadFile, sizeBytes); err != nil {
		return 0, nil, fmt.Errorf("failed to upload rewritten file to remote storage: %w", err)
	}

	h.logger.Info().
		Str("file", filepath.Base(relativePath)).
		Str("new_file", filepath.Base(newRelativePath)).
		Int64("rows_before", rowsBefore).
		Int64("rows_after", rowsAfter).
		Int64("deleted", deleted).
		Msg("Rewrote S3 file")

	return deleted, &s3RewriteResult{newPath: newRelativePath, sizeBytes: sizeBytes, sha256: sha256hex}, nil
}

// isRemoteBackend returns true if the storage backend requires a remote rewrite
// (download to local temp file, then re-upload). Covers S3 and Azure Blob.
func (h *DeleteHandler) isRemoteBackend() bool {
	switch h.storage.(type) {
	case *storage.S3Backend, *storage.AzureBlobBackend:
		return true
	}
	return false
}

func (h *DeleteHandler) recordRewriteTiering(ctx context.Context, oldPath, newPath string, s3 *s3RewriteResult) error {
	if h.tieringManager == nil {
		return nil
	}
	sizeBytes := int64(0)
	if s3 != nil {
		sizeBytes = s3.sizeBytes
	} else if lb, ok := h.storage.(*storage.LocalBackend); ok {
		info, err := os.Stat(filepath.Join(lb.GetBasePath(), newPath))
		if err != nil {
			return fmt.Errorf("stat rewritten file for tiering: %w", err)
		}
		sizeBytes = info.Size()
	}
	if err := h.tieringManager.RecordRewrittenFile(ctx, oldPath, newPath, sizeBytes); err != nil {
		return fmt.Errorf("update tier metadata for rewritten file: %w", err)
	}
	return nil
}

// rewrittenFileMetadata returns the rewritten file's size and SHA-256: read
// from disk on local storage, from the upload on object storage. Size -1 when
// neither applies.
func (h *DeleteHandler) rewrittenFileMetadata(newPath string, s3 *s3RewriteResult) (int64, string, error) {
	if lb, ok := h.storage.(*storage.LocalBackend); ok {
		basePath := lb.GetBasePath()
		fullPath := filepath.Join(basePath, newPath)
		if !strings.HasPrefix(fullPath, basePath+string(filepath.Separator)) {
			return 0, "", fmt.Errorf("path escapes storage root: %s", newPath)
		}
		size, sha, err := fileMetadata(fullPath)
		if err != nil {
			return 0, "", fmt.Errorf("failed to read rewritten file metadata: %w", err)
		}
		return size, sha, nil
	}
	if s3 != nil {
		return s3.sizeBytes, s3.sha256, nil
	}
	return -1, "", nil
}

// replaceManifestAfterRewrite publishes a partial rewrite as a new immutable
// file. Registering the new path and deleting the old path in one Raft command
// means followers never have to infer a content change from size/checksum on an
// existing path (#975).
func (h *DeleteHandler) replaceManifestAfterRewrite(reqCtx context.Context, oldPath, newPath string, s3 *s3RewriteResult, scanSHA string) error {
	// From here on the rewrite is in storage: what follows (the record, the
	// manifest, the removals) must finish even if the client goes away.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(reqCtx), rewriteFinishTimeout)
	defer cancel()
	existing, ok := h.coordinator.GetFileEntry(oldPath)
	if scanSHA == unmanagedSHA {
		// No manifest entry covered it when scanned (a shared object store,
		// #1207): the rewrite replaces it in storage only. One that covers
		// it now is scanned again.
		cleanup := func() {
			if err := h.storage.Delete(ctx, newPath); err != nil {
				h.logger.Warn().Err(err).Str("file", newPath).Msg("Failed to clean up a rewrite that was not announced")
			}
			h.clearRetired(ctx, oldPath)
		}
		if ok {
			cleanup()
			return fmt.Errorf("%w: %s", errFileChanged, filepath.Base(oldPath))
		}
		// The rewrite is registered, or it would be an orphan the
		// reconciler removes with the rows this DELETE kept.
		so, shared := sharedObjectStore(h.coordinator)
		size, sha, err := h.rewrittenFileMetadata(newPath, s3)
		if err == nil && (!shared || size < 0) {
			err = errors.New("no shared object store to register the rewrite in")
		}
		if err != nil {
			cleanup()
			return errors.Join(errManifestFailure, fmt.Errorf("register the rewrite of %s: %w", filepath.Base(oldPath), err))
		}
		// The intent, before the manifest changes (delete_leftovers.go).
		if err := h.settlePriorIntent(ctx, oldPath, newPath); err != nil {
			if cerr := h.storage.Delete(ctx, newPath); cerr != nil {
				h.logger.Warn().Err(cerr).Str("file", newPath).Msg("Failed to clean up a rewrite that was not announced")
			}
			return err
		}
		if err := h.journalIntent(ctx, oldPath, newPath); err != nil {
			cleanup()
			return errors.Join(errManifestFailure, err)
		}
		if _, err = so.AdoptRewrite(ctx, newPath, size, sha); err != nil {
			switch {
			case h.rewriteCommitted(ctx, oldPath, newPath, sha):
				// It applied after all.
			case raft.IsManifestChanged(err) || errors.Is(err, cluster.ErrNotAdoptable):
				cleanup()
				return errors.Join(errManifestFailure, fmt.Errorf("register the rewrite of %s: %w", filepath.Base(oldPath), err))
			default:
				return h.ambiguousRewrite(oldPath, newPath, err)
			}
		}
		if err := deleteReplaced(ctx, h.storage, oldPath); err != nil {
			h.logger.Error().Err(err).Str("file", oldPath).Str("new_file", newPath).
				Msg("Rewrite registered but failed to delete the superseded object; it may remain visible to glob queries until cleanup succeeds")
			return fmt.Errorf("rewrite registered but failed to delete superseded object %q: %w", oldPath, err)
		}
		h.clearRetired(ctx, oldPath)
		return nil
	}
	if !ok && h.pending != nil {
		// Not in the manifest yet may mean pending: flushed by this node and
		// not confirmed. Then the rewrite is what must be announced, and the
		// old file must not be (#1210).
		// On any error here the old file stays authoritative: remove the
		// rewrite, or this node reads its surviving rows twice.
		cleanupRewrite := func() {
			if err := h.storage.Delete(ctx, newPath); err != nil {
				h.logger.Warn().Err(err).Str("file", newPath).Msg("Failed to clean up a rewrite that was not announced")
			}
		}
		size, sha, err := h.rewrittenFileMetadata(newPath, s3)
		if err != nil {
			cleanupRewrite()
			return err
		}
		outcome, entry := cluster.ReplaceNotPending, raft.FileEntry{}
		if size >= 0 { // the outbox checks a pending file by its size
			outcome, entry, err = h.pending.ReplacePending(ctx, oldPath, newPath, size, sha)
		}
		if err != nil {
			cleanupRewrite()
			if ctx.Err() != nil {
				return err
			}
			return errors.Join(errManifestFailure, fmt.Errorf("replace pending registration: %w", err))
		}
		switch outcome {
		case cluster.ReplaceSwapped:
			if st, ok := h.pending.(deleteSeenStamper); ok && h.recordID != 0 {
				// Rewritten with this DELETE: no second pass for its record.
				st.SetDeleteSeen(newPath, h.recordID)
			}
			if err := deleteReplaced(ctx, h.storage, oldPath); err != nil {
				return fmt.Errorf("failed to delete superseded pending rewrite %q: %w", oldPath, err)
			}
			return nil
		case cluster.ReplaceRegistered:
			// Confirmed while we looked; this node's FSM may not show it yet.
			existing, ok = &entry, true
		case cluster.ReplaceParked:
			// Refused and parked while we looked (#1207): the rewrite must
			// not be announced in its place.
			cleanupRewrite()
			return errors.Join(errManifestFailure, cluster.ErrRegistrationParked)
		}
	}
	if !ok && casActive(h.coordinator) {
		// Gone from the manifest since the scan (a compaction replaced it):
		// its rows live in the compaction's output, which a scan finds. The
		// file itself is not this DELETE's to remove.
		if err := h.storage.Delete(ctx, newPath); err != nil {
			h.logger.Warn().Err(err).Str("file", newPath).Msg("Failed to clean up a rewrite that was not announced")
		}
		return fmt.Errorf("%w: %s", errFileChanged, filepath.Base(oldPath))
	}
	if !ok && sharedManifest(h.coordinator) && h.mayBePendingElsewhere(ctx, oldPath) {
		// A shared object store with a manifest, compaction commit off (a
		// cluster mid-upgrade): every writer's files are in one bucket, so a
		// recent file the manifest does not list and this node is not
		// registering is most likely another writer's, still pending its
		// registration (#1214). Replacing it here would leave the rewrite
		// unregistered, and the reconciler would remove it with the rows
		// this DELETE kept, while the writer's registration of the original
		// settles as gone. The original stays authoritative; the DELETE is
		// retried. An older one is an orphan to the reconciler too, and is
		// rewritten as before.
		if err := h.storage.Delete(ctx, newPath); err != nil {
			h.logger.Warn().Err(err).Str("file", newPath).Msg("Failed to clean up a rewrite that was not announced")
		}
		return fmt.Errorf("%w: %s", errNotRegisteredYet, filepath.Base(oldPath))
	}
	if !ok {
		// Absent from this node's manifest may only mean behind: catch up
		// before treating the file as an orphan, or a registered file is
		// removed with its rewrite left unregistered.
		if s, isSyncer := h.coordinator.(manifestSyncer); isSyncer {
			if err := s.SyncManifest(ctx); err != nil {
				if cerr := h.storage.Delete(ctx, newPath); cerr != nil {
					h.logger.Warn().Err(cerr).Str("file", newPath).Msg("Failed to clean up a rewrite that was not announced")
				}
				return fmt.Errorf("%w: %s (could not catch up with the cluster manifest: %v)", errNotRegisteredYet, filepath.Base(oldPath), err)
			}
			existing, ok = h.coordinator.GetFileEntry(oldPath)
		}
	}
	if !ok {
		// A true orphan: no manifest lists it. Its rewrite shares its fate
		// (the reconciler, when it acts, removes either).
		h.logger.Warn().
			Str("file", oldPath).
			Str("new_file", newPath).
			Msg("Rewritten source is absent from the manifest; new file is not registered and will not replicate; deleting the superseded storage object (reconciliation may later remove the orphan)")
		if err := deleteReplaced(ctx, h.storage, oldPath); err != nil {
			return fmt.Errorf("failed to delete unmanifested superseded rewrite %q: %w", oldPath, err)
		}
		return nil
	}

	entry := *existing
	entry.Path = newPath
	entry.OriginNodeID = h.coordinator.LocalNodeID()

	if size, sha, err := h.rewrittenFileMetadata(newPath, s3); err != nil {
		return err
	} else if size >= 0 {
		entry.SizeBytes = size
		entry.SHA256 = sha
	}

	// The rewrite holds the old file's rows minus the deleted ones: it takes
	// over the old file's batch coverage (#1207 S4b).
	entry.Coverage = nil
	// With compaction commit on, both ops apply only to the content this
	// DELETE read: a compaction that replaced the file meanwhile wins, and
	// the DELETE scans again (#1207 d2).
	expect := ""
	if casActive(h.coordinator) {
		expect = scanSHA
		if expect == "" {
			expect = existing.SHA256
		}
	}
	registerPayload, err := json.Marshal(raft.RegisterFilePayload{File: entry, InheritFrom: oldPath, InheritSHA: expect})
	if err != nil {
		return fmt.Errorf("%w: marshal replacement register: %v", errManifestFailure, err)
	}
	deletePayload, err := json.Marshal(raft.DeleteFilePayload{Path: oldPath, Reason: deleteRewriteManifestReason, ExpectSHA: expect})
	if err != nil {
		return fmt.Errorf("%w: marshal replacement delete: %v", errManifestFailure, err)
	}

	// The intent, before the manifest changes (delete_leftovers.go): an
	// earlier rewrite of the same file still settling is settled first, or
	// this one waits.
	cleanupNew := func() {
		if cerr := h.storage.Delete(ctx, newPath); cerr != nil {
			h.logger.Warn().Err(cerr).Str("file", newPath).Msg("Failed to clean up unregistered rewrite")
		}
	}
	if err := h.settlePriorIntent(ctx, oldPath, newPath); err != nil {
		cleanupNew()
		return err
	}
	if err := h.journalIntent(ctx, oldPath, newPath); err != nil {
		cleanupNew()
		return errors.Join(errManifestFailure, err)
	}
	if err := h.coordinator.BatchFileOpsInManifest([]raft.BatchFileOp{
		{Type: raft.CommandRegisterFile, Payload: registerPayload},
		{Type: raft.CommandDeleteFile, Payload: deletePayload},
	}); err != nil {
		switch {
		case raft.IsManifestChanged(err):
			// Refused: the old entry is authoritative, the rewrite an
			// orphan. The cleanup is best-effort.
			if cleanupErr := h.storage.Delete(ctx, newPath); cleanupErr != nil {
				h.logger.Warn().Err(cleanupErr).Str("file", newPath).Msg("Failed to clean up unregistered rewrite")
			}
			h.clearRetired(ctx, oldPath)
			return fmt.Errorf("%w: %s", errFileChanged, filepath.Base(oldPath))
		case h.rewriteCommitted(ctx, oldPath, newPath, entry.SHA256):
			// The batch applied although its caller saw an error (a forward
			// that timed out, leadership lost after the proposal).
		default:
			return h.ambiguousRewrite(oldPath, newPath, err)
		}
	}

	// The rewritten file is now authoritative. Delete the superseded object
	// directly on the node that performed the rewrite; this is required for
	// standalone mode and for clusters with replication disabled, where no
	// manifest-delete worker is running.
	if err := deleteReplaced(ctx, h.storage, oldPath); err != nil {
		h.logger.Error().Err(err).Str("file", oldPath).Str("new_file", newPath).
			Msg("Manifest committed but failed to delete superseded rewrite; the old object may remain visible to glob queries until cleanup succeeds")
		return fmt.Errorf("manifest committed but failed to delete superseded rewrite %q: %w", oldPath, err)
	}
	h.clearRetired(ctx, oldPath)
	h.dropFinalizingInputs(ctx, oldPath)
	return nil
}

// The manifest delete reasons of a row DELETE: a file it deleted whole, and
// the file a rewrite replaced. A compaction whose output either removes
// deletes its inputs too (raft.OutputGoneAccounted, #1207 d2a2b).
const (
	deleteManifestReason        = "delete"
	deleteRewriteManifestReason = "delete-rewrite"
)

// respondDeleteUnavailable answers 503 with a retry hint.
func respondDeleteUnavailable(c *fiber.Ctx, resp DeleteResponse) error {
	resp.Success = false
	c.Set(fiber.HeaderRetryAfter, "30")
	return c.Status(fiber.StatusServiceUnavailable).JSON(resp)
}

// SetOnFilesRemoved sets the function called after this handler removed or
// rewrote parquet files: the query caches hold pruned partition lists, and one
// naming a removed hour makes DuckDB report "No files found", which a query
// answers with a retryable 503 (classifyNoFiles) until the cache is cleared.
func (h *DeleteHandler) SetOnFilesRemoved(fn func()) {
	h.onFilesRemoved = fn
}

func (h *DeleteHandler) filesRemoved() {
	if h.onFilesRemoved != nil {
		h.onFilesRemoved()
	}
}
