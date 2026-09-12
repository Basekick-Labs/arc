package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/basekick-labs/arc/internal/auth"
	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/database"
	"github.com/basekick-labs/arc/internal/license"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/gofiber/fiber/v2"
	_ "github.com/mattn/go-sqlite3"
	"github.com/rs/zerolog"
)

// RetentionCoordinator is the minimal cluster interface retention needs to
// propagate file deletes to the Raft manifest and gate execution to the
// primary writer. Using a minimal interface avoids a compile-time dependency
// on the cluster package.
type RetentionCoordinator interface {
	BatchFileOpsInManifest(ops []raft.BatchFileOp) error
	// IsPrimaryWriter reports whether this node may execute writer-only mutations.
	// Returns true unconditionally for standalone (coordinator is nil).
	IsPrimaryWriter() bool
	// Role returns a human-readable role string for log messages.
	Role() string
}

// RetentionHandler handles retention policy operations
type RetentionHandler struct {
	storage storage.Backend
	config  *config.RetentionConfig
	db      *sql.DB // SQLite for policy metadata
	// ownsDB records whether this handler opened db itself. When retention
	// shares the auth database (the default), the handle is borrowed and Close
	// must leave it to its owner.
	ownsDB        bool
	duckdb        *database.DuckDB     // Shared DuckDB for parquet queries
	coordinator   RetentionCoordinator // nil in standalone mode
	licenseClient *license.Client      // nil when auth/licensing is disabled
	authManager   *auth.AuthManager
	logger        zerolog.Logger
}

// RetentionPolicy represents a retention policy
type RetentionPolicy struct {
	ID                  int64   `json:"id"`
	Name                string  `json:"name"`
	Database            string  `json:"database"`
	Measurement         *string `json:"measurement"`
	RetentionDays       int     `json:"retention_days"`
	BufferDays          int     `json:"buffer_days"`
	IsActive            bool    `json:"is_active"`
	LastExecutionTime   *string `json:"last_execution_time"`
	LastExecutionStatus *string `json:"last_execution_status"`
	LastDeletedCount    *int64  `json:"last_deleted_count"`
	CreatedAt           string  `json:"created_at"`
	UpdatedAt           string  `json:"updated_at"`
}

// RetentionPolicyRequest represents a request to create/update a policy
type RetentionPolicyRequest struct {
	Name          string  `json:"name"`
	Database      string  `json:"database"`
	Measurement   *string `json:"measurement"`
	RetentionDays int     `json:"retention_days"`
	BufferDays    int     `json:"buffer_days"`
	IsActive      bool    `json:"is_active"`
}

// ExecuteRetentionRequest represents a request to execute a policy
type ExecuteRetentionRequest struct {
	DryRun  bool `json:"dry_run"`
	Confirm bool `json:"confirm"`
}

// ExecuteRetentionResponse represents the result of executing a policy
type ExecuteRetentionResponse struct {
	PolicyID     int64  `json:"policy_id"`
	PolicyName   string `json:"policy_name"`
	DeletedCount int64  `json:"deleted_count"`
	FilesDeleted int    `json:"files_deleted"`
	// SkippedFiles counts files whose stored key could not be resolved to a
	// readable path, so they were neither examined nor deleted and never will
	// be. Omitted when zero, which is every healthy deployment.
	SkippedFiles         int      `json:"skipped_files,omitempty"`
	SkippedReason        string   `json:"skipped_reason,omitempty"`
	ExecutionTimeMs      float64  `json:"execution_time_ms"`
	DryRun               bool     `json:"dry_run"`
	CutoffDate           string   `json:"cutoff_date"`
	AffectedMeasurements []string `json:"affected_measurements"`
}

// RetentionExecution represents an execution history record
type RetentionExecution struct {
	ID                  int64   `json:"id"`
	PolicyID            int64   `json:"policy_id"`
	ExecutionTime       string  `json:"execution_time"`
	Status              string  `json:"status"`
	DeletedCount        int64   `json:"deleted_count"`
	CutoffDate          *string `json:"cutoff_date"`
	ExecutionDurationMs float64 `json:"execution_duration_ms"`
	ErrorMessage        *string `json:"error_message"`
}

// NewRetentionHandler creates a new retention handler
func NewRetentionHandler(storage storage.Backend, duckdb *database.DuckDB, cfg *config.RetentionConfig, licenseClient *license.Client, authManager *auth.AuthManager, logger zerolog.Logger) (*RetentionHandler, error) {
	// retention.db_path defaults to the auth database. When it resolves to the
	// same file, borrow the auth manager's handle rather than opening a second
	// connection pool against it — SQLite has a single writer, so independent
	// pools on one file only contend for the same lock. An operator who points
	// retention.db_path elsewhere still gets a genuinely separate database.
	var (
		db     *sql.DB
		ownsDB bool
	)
	if authManager.SharesDBPath(cfg.DBPath) {
		db = authManager.GetDB()
	} else {
		// Ensure directory exists
		dir := filepath.Dir(cfg.DBPath)
		if err := os.MkdirAll(dir, 0700); err != nil {
			return nil, fmt.Errorf("failed to create directory for retention DB: %w", err)
		}

		// Open SQLite database for policy metadata
		opened, err := sql.Open("sqlite3", cfg.DBPath)
		if err != nil {
			return nil, fmt.Errorf("failed to open retention database: %w", err)
		}
		db, ownsDB = opened, true
	}

	h := &RetentionHandler{
		storage:       storage,
		config:        cfg,
		db:            db,
		ownsDB:        ownsDB,
		duckdb:        duckdb,
		licenseClient: licenseClient,
		authManager:   authManager,
		logger:        logger.With().Str("component", "retention-handler").Logger(),
	}

	// Initialize tables
	if err := h.initTables(); err != nil {
		if ownsDB {
			db.Close()
		}
		return nil, fmt.Errorf("failed to initialize retention tables: %w", err)
	}

	h.warnUnusablePolicies()

	return h, nil
}

// warnUnusablePolicies reports stored policies whose database or measurement
// name cannot form a storage prefix (#741).
//
// Create and update validate both fields now, but rows written before that do
// not disappear, and the scheduler replays them forever. Without this the only
// signal is a listing error buried in a scheduled run hours later, attributed
// to storage rather than to the policy.
func (h *RetentionHandler) warnUnusablePolicies() {
	// Inactive rows are included: a disabled policy with a broken name is one
	// toggle away from failing, and the point is to report it before then.
	rows, err := h.db.Query(`SELECT name, database, measurement FROM retention_policies`)
	if err != nil {
		h.logger.Warn().Err(err).Msg("Could not check stored retention policies against the current name rules")
		return
	}
	defer rows.Close()

	for rows.Next() {
		var name, database string
		var measurement sql.NullString
		if err := rows.Scan(&name, &database, &measurement); err != nil {
			// Skip the row rather than returning: one unreadable row must not
			// hide every offender after it.
			h.logger.Warn().Err(err).Msg("Could not read a stored retention policy")
			continue
		}
		// Both fields are reported, not just the first: a policy can be broken
		// in both and fixing one would leave it still failing.
		if !isSafeStoragePathSegment(database) {
			h.logger.Warn().Str("policy", name).Str("database", database).
				Msg("Retention policy has an unusable database name and will fail every run; update or delete it")
		}
		if measurement.Valid && measurement.String != "" && !isSafeStoragePathSegment(measurement.String) {
			h.logger.Warn().Str("policy", name).Str("measurement", measurement.String).
				Msg("Retention policy has an unusable measurement name and will fail every run; update or delete it")
		}
	}
	if err := rows.Err(); err != nil {
		h.logger.Warn().Err(err).Msg("Could not finish checking stored retention policies")
	}
}

// SetCoordinator wires the cluster coordinator for manifest updates.
// Called after construction when cluster mode is enabled.
func (h *RetentionHandler) SetCoordinator(c RetentionCoordinator) {
	h.coordinator = c
}

// initTables creates the retention policy tables
func (h *RetentionHandler) initTables() error {
	// Retention policies table
	_, err := h.db.Exec(`
		CREATE TABLE IF NOT EXISTS retention_policies (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			name TEXT UNIQUE NOT NULL,
			database TEXT NOT NULL,
			measurement TEXT,
			retention_days INTEGER NOT NULL,
			buffer_days INTEGER DEFAULT 7,
			is_active BOOLEAN DEFAULT TRUE,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to create retention_policies table: %w", err)
	}

	// Retention execution history table
	_, err = h.db.Exec(`
		CREATE TABLE IF NOT EXISTS retention_executions (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			policy_id INTEGER NOT NULL,
			execution_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			status TEXT NOT NULL,
			deleted_count INTEGER DEFAULT 0,
			cutoff_date TIMESTAMP,
			execution_duration_ms FLOAT,
			error_message TEXT,
			FOREIGN KEY (policy_id) REFERENCES retention_policies (id)
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to create retention_executions table: %w", err)
	}

	h.logger.Info().Msg("Retention policy tables initialized")
	return nil
}

// Close releases the database handle if this handler owns it.
//
// When retention shares the auth database (the default), the handle is
// borrowed and its owner closes it later in the shutdown sequence.
func (h *RetentionHandler) Close() error {
	if !h.ownsDB {
		return nil
	}
	return h.db.Close()
}

// RegisterRoutes registers retention endpoints
func (h *RetentionHandler) RegisterRoutes(app *fiber.App) {
	group := app.Group("/api/v1/retention")

	// Read-only routes — any authenticated token
	group.Get("/", h.handleList)
	group.Get("/:id", h.handleGet)
	group.Get("/:id/executions", h.handleGetExecutions)

	// Admin routes — require admin permission for mutating operations
	if h.authManager != nil {
		group.Post("/", auth.RequireAdmin(h.authManager), h.handleCreate)
		group.Put("/:id", auth.RequireAdmin(h.authManager), h.handleUpdate)
		group.Delete("/:id", auth.RequireAdmin(h.authManager), h.handleDelete)
		group.Post("/:id/execute", auth.RequireAdmin(h.authManager), h.handleExecute)
	} else {
		group.Post("/", h.handleCreate)
		group.Put("/:id", h.handleUpdate)
		group.Delete("/:id", h.handleDelete)
		group.Post("/:id/execute", h.handleExecute)
	}
}

// handleCreate creates a new retention policy
func (h *RetentionHandler) handleCreate(c *fiber.Ctx) error {
	if !h.config.Enabled {
		return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
			"error": "Retention policies are disabled",
		})
	}

	var req RetentionPolicyRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "Invalid request body: " + err.Error(),
		})
	}

	// Validate
	if req.Name == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "name is required"})
	}
	if req.Database == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "database is required"})
	}
	// Both fields are concatenated into a storage prefix by
	// getMeasurementsToProcess and deleteOldFiles, and the row is replayed by
	// the scheduler forever, so an unvalidated one is a policy that fails on
	// every run long after the request that created it (#741).
	if !isSafeStoragePathSegment(req.Database) {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": fmt.Sprintf("invalid database name %q: may not be empty, contain a separator, or start with a dot", req.Database),
		})
	}
	if req.Measurement != nil && *req.Measurement != "" && !isSafeStoragePathSegment(*req.Measurement) {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": fmt.Sprintf("invalid measurement name %q: may not contain a separator or start with a dot", *req.Measurement),
		})
	}
	if req.RetentionDays <= 0 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "retention_days must be greater than 0"})
	}
	if req.RetentionDays <= req.BufferDays {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "retention_days must be greater than buffer_days"})
	}

	// Insert policy
	result, err := h.db.Exec(`
		INSERT INTO retention_policies (name, database, measurement, retention_days, buffer_days, is_active)
		VALUES (?, ?, ?, ?, ?, ?)
	`, req.Name, req.Database, req.Measurement, req.RetentionDays, req.BufferDays, req.IsActive)

	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE constraint") {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": fmt.Sprintf("Retention policy with name '%s' already exists", req.Name),
			})
		}
		h.logger.Error().Err(err).Msg("Failed to create retention policy")
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "Failed to create retention policy",
		})
	}

	policyID, _ := result.LastInsertId()
	h.logger.Info().Int64("policy_id", policyID).Str("name", req.Name).Msg("Created retention policy")

	// Return created policy
	policy, err := h.getPolicy(policyID)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "Failed to retrieve created policy",
		})
	}

	return c.Status(fiber.StatusCreated).JSON(policy)
}

// handleList returns all retention policies
func (h *RetentionHandler) handleList(c *fiber.Ctx) error {
	policies, err := h.getPolicies()
	if err != nil {
		h.logger.Error().Err(err).Msg("Failed to list retention policies")
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "Failed to list retention policies",
		})
	}

	return c.JSON(policies)
}

// handleGet returns a single retention policy
func (h *RetentionHandler) handleGet(c *fiber.Ctx) error {
	policyID, err := c.ParamsInt("id")
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "Invalid policy ID"})
	}

	policy, err := h.getPolicy(int64(policyID))
	if err != nil {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"error": "Retention policy not found"})
	}

	return c.JSON(policy)
}

// handleUpdate updates an existing retention policy
func (h *RetentionHandler) handleUpdate(c *fiber.Ctx) error {
	policyID, err := c.ParamsInt("id")
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "Invalid policy ID"})
	}

	var req RetentionPolicyRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "Invalid request body: " + err.Error(),
		})
	}

	// Validate. Update writes both name fields verbatim, so it can put a row
	// into exactly the state create now refuses (#741).
	if req.RetentionDays <= req.BufferDays {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "retention_days must be greater than buffer_days",
		})
	}
	if !isSafeStoragePathSegment(req.Database) {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": fmt.Sprintf("invalid database name %q: may not be empty, contain a separator, or start with a dot", req.Database),
		})
	}
	if req.Measurement != nil && *req.Measurement != "" && !isSafeStoragePathSegment(*req.Measurement) {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": fmt.Sprintf("invalid measurement name %q: may not contain a separator or start with a dot", *req.Measurement),
		})
	}

	result, err := h.db.Exec(`
		UPDATE retention_policies SET
			name = ?, database = ?, measurement = ?, retention_days = ?,
			buffer_days = ?, is_active = ?, updated_at = CURRENT_TIMESTAMP
		WHERE id = ?
	`, req.Name, req.Database, req.Measurement, req.RetentionDays, req.BufferDays, req.IsActive, policyID)

	if err != nil {
		h.logger.Error().Err(err).Msg("Failed to update retention policy")
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "Failed to update retention policy",
		})
	}

	rows, _ := result.RowsAffected()
	if rows == 0 {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"error": "Retention policy not found"})
	}

	h.logger.Info().Int("policy_id", policyID).Msg("Updated retention policy")

	policy, _ := h.getPolicy(int64(policyID))
	return c.JSON(policy)
}

// handleDelete deletes a retention policy
func (h *RetentionHandler) handleDelete(c *fiber.Ctx) error {
	policyID, err := c.ParamsInt("id")
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "Invalid policy ID"})
	}

	// Delete execution history first
	_, _ = h.db.Exec("DELETE FROM retention_executions WHERE policy_id = ?", policyID)

	// Delete policy
	result, err := h.db.Exec("DELETE FROM retention_policies WHERE id = ?", policyID)
	if err != nil {
		h.logger.Error().Err(err).Msg("Failed to delete retention policy")
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "Failed to delete retention policy",
		})
	}

	rows, _ := result.RowsAffected()
	if rows == 0 {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"error": "Retention policy not found"})
	}

	h.logger.Info().Int("policy_id", policyID).Msg("Deleted retention policy")

	return c.JSON(fiber.Map{"message": "Retention policy deleted successfully"})
}

// ExecutePolicy executes a retention policy by ID programmatically (used by scheduler)
// Returns the execution response and any error
func (h *RetentionHandler) ExecutePolicy(ctx context.Context, policyID int64) (*ExecuteRetentionResponse, error) {
	start := time.Now()

	// License check: guards direct programmatic calls that bypass the scheduler.
	if h.licenseClient != nil && !h.licenseClient.CanUseRetentionScheduler() {
		return nil, fmt.Errorf("valid enterprise license required for retention policy execution")
	}

	// Get policy
	policy, err := h.getPolicy(policyID)
	if err != nil {
		return nil, fmt.Errorf("retention policy not found: %w", err)
	}

	if !policy.IsActive {
		return nil, fmt.Errorf("retention policy is not active")
	}

	// Calculate cutoff date
	cutoffDate := time.Now().UTC().AddDate(0, 0, -(policy.RetentionDays + policy.BufferDays))

	h.logger.Info().
		Str("policy", policy.Name).
		Time("cutoff_date", cutoffDate).
		Msg("Executing scheduled retention policy")

	// Get measurements to process
	measurements, err := h.getMeasurementsToProcess(ctx, policy)
	if err != nil {
		return nil, fmt.Errorf("failed to discover measurements: %w", err)
	}

	h.logger.Info().Strs("measurements", measurements).Msg("Processing measurements")

	// Record execution start
	executionID := h.recordExecutionStart(policyID, cutoffDate)

	// Execute retention for each measurement
	var totalDeleted int64
	var totalFilesDeleted int
	var totalSkipped int

	for _, measurement := range measurements {
		deleted, filesDeleted, skipped, err := h.deleteOldFiles(ctx, policy.Database, measurement, cutoffDate, false, fmt.Sprintf("retention:%d", policyID))
		// Accumulate before error check: deleteOldFiles returns partial progress
		// on abort so the execution record reflects all completed work accurately.
		totalDeleted += deleted
		totalFilesDeleted += filesDeleted
		totalSkipped += skipped
		if err != nil {
			h.logger.Error().Err(err).Str("measurement", measurement).Msg("Failed to process measurement")
			// Abort on any error — manifest failures are non-transient (Raft quorum loss)
			// and continuing would produce orphaned manifest entries with no retry path.
			if executionID > 0 {
				h.recordExecutionComplete(executionID, "failed", totalDeleted, float64(time.Since(start).Milliseconds()), err.Error())
			}
			return nil, fmt.Errorf("retention aborted for policy %d: %w", policyID, err)
		}
	}

	// Clear DuckDB parquet metadata/data cache and release memory back to OS.
	h.duckdb.ClearHTTPCache()
	freeOSMemoryThrottled()

	executionTime := float64(time.Since(start).Milliseconds())

	// Record execution completion. A run that could not resolve some files did
	// not do what the policy asks, so it must not be recorded as a clean
	// "completed": the whole failure mode #746 fixed was retention reporting
	// success while deleting nothing.
	if executionID > 0 {
		status, detail := "completed", ""
		if totalSkipped > 0 {
			status = "completed_with_errors"
			detail = fmt.Sprintf("%d file(s) have a stored path that cannot be read and will never be deleted; see the log for the keys", totalSkipped)
			h.logger.Error().Int("skipped_files", totalSkipped).Str("database", policy.Database).
				Msg("Retention could not resolve some files; their data will never age out")
		}
		h.recordExecutionComplete(executionID, status, totalDeleted, executionTime, detail)
	}

	h.logger.Info().
		Int64("deleted_count", totalDeleted).
		Int("files_deleted", totalFilesDeleted).
		Float64("execution_time_ms", executionTime).
		Msg("Scheduled retention policy execution completed")

	return &ExecuteRetentionResponse{
		PolicyID:             policyID,
		PolicyName:           policy.Name,
		DeletedCount:         totalDeleted,
		FilesDeleted:         totalFilesDeleted,
		ExecutionTimeMs:      executionTime,
		DryRun:               false,
		CutoffDate:           cutoffDate.Format(time.RFC3339),
		AffectedMeasurements: measurements,
	}, nil
}

// GetActivePolicies returns all active retention policies (used by scheduler)
func (h *RetentionHandler) GetActivePolicies() ([]RetentionPolicy, error) {
	rows, err := h.db.Query(`
		SELECT
			rp.id, rp.name, rp.database, rp.measurement, rp.retention_days, rp.buffer_days, rp.is_active,
			rp.created_at, rp.updated_at,
			re.execution_time, re.status, re.deleted_count
		FROM retention_policies rp
		LEFT JOIN (
			SELECT policy_id, execution_time, status, deleted_count
			FROM retention_executions
			WHERE id IN (SELECT MAX(id) FROM retention_executions GROUP BY policy_id)
		) re ON rp.id = re.policy_id
		WHERE rp.is_active = TRUE
		ORDER BY rp.created_at DESC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var policies []RetentionPolicy
	for rows.Next() {
		var p RetentionPolicy
		if err := rows.Scan(
			&p.ID, &p.Name, &p.Database, &p.Measurement, &p.RetentionDays, &p.BufferDays, &p.IsActive,
			&p.CreatedAt, &p.UpdatedAt,
			&p.LastExecutionTime, &p.LastExecutionStatus, &p.LastDeletedCount,
		); err != nil {
			continue
		}
		policies = append(policies, p)
	}

	return policies, nil
}

// GetPolicy returns a retention policy by ID (used by scheduler)
func (h *RetentionHandler) GetPolicy(policyID int64) (*RetentionPolicy, error) {
	return h.getPolicy(policyID)
}

// handleExecute executes a retention policy
func (h *RetentionHandler) handleExecute(c *fiber.Ctx) error {
	start := time.Now()

	policyID, err := c.ParamsInt("id")
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "Invalid policy ID"})
	}

	var req ExecuteRetentionRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "Invalid request body: " + err.Error(),
		})
	}

	// Require confirmation for non-dry-run
	if !req.DryRun && !req.Confirm {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "Confirmation required for retention policy execution. Set confirm=true",
		})
	}

	// In cluster mode, only the primary writer may execute retention — reader
	// nodes must not race with the writer over shared or local storage.
	if !req.DryRun && h.coordinator != nil && !h.coordinator.IsPrimaryWriter() {
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
			"error": fmt.Sprintf("retention rejected: node role %q is not primary writer", h.coordinator.Role()),
		})
	}

	// Get policy
	policy, err := h.getPolicy(int64(policyID))
	if err != nil {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"error": "Retention policy not found"})
	}

	if !policy.IsActive {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "Retention policy is not active"})
	}

	// Calculate cutoff date
	cutoffDate := time.Now().UTC().AddDate(0, 0, -(policy.RetentionDays + policy.BufferDays))

	h.logger.Info().
		Str("policy", policy.Name).
		Time("cutoff_date", cutoffDate).
		Bool("dry_run", req.DryRun).
		Msg("Executing retention policy")

	// Get measurements to process
	measurements, err := h.getMeasurementsToProcess(c.Context(), policy)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "Failed to discover measurements: " + err.Error(),
		})
	}

	h.logger.Info().Strs("measurements", measurements).Msg("Processing measurements")

	// Record execution start
	var executionID int64
	if !req.DryRun {
		executionID = h.recordExecutionStart(int64(policyID), cutoffDate)
	}

	// Execute retention for each measurement
	var totalDeleted int64
	var totalFilesDeleted int
	var totalSkipped int

	for _, measurement := range measurements {
		deleted, filesDeleted, skipped, err := h.deleteOldFiles(c.Context(), policy.Database, measurement, cutoffDate, req.DryRun, fmt.Sprintf("retention:%d", policyID))
		totalSkipped += skipped
		totalDeleted += deleted
		totalFilesDeleted += filesDeleted
		if err != nil {
			h.logger.Error().Err(err).Str("measurement", measurement).Msg("Failed to process measurement")
			if !req.DryRun && executionID > 0 {
				h.recordExecutionComplete(executionID, "failed", totalDeleted, float64(time.Since(start).Milliseconds()), err.Error())
			}
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"error": fmt.Sprintf("retention aborted at measurement %q: %s", measurement, err.Error()),
			})
		}
	}

	// Clear DuckDB parquet metadata/data cache — dry runs also populate the cache via
	// read_parquet calls in getFileMaxTimeAndRowCount, so always clear regardless of dry run.
	h.duckdb.ClearHTTPCache()
	freeOSMemoryThrottled()

	executionTime := float64(time.Since(start).Milliseconds())

	// Record execution completion. See runPolicy: a run that could not resolve
	// some files is not a clean "completed".
	var skipDetail string
	if totalSkipped > 0 {
		skipDetail = fmt.Sprintf("%d file(s) have a stored path that cannot be read and will never be deleted; see the log for the keys", totalSkipped)
		h.logger.Error().Int("skipped_files", totalSkipped).Str("database", policy.Database).
			Msg("Retention could not resolve some files; their data will never age out")
	}
	if !req.DryRun && executionID > 0 {
		status := "completed"
		if totalSkipped > 0 {
			status = "completed_with_errors"
		}
		h.recordExecutionComplete(executionID, status, totalDeleted, executionTime, skipDetail)
	}

	h.logger.Info().
		Int64("deleted_count", totalDeleted).
		Int("files_deleted", totalFilesDeleted).
		Int("skipped_files", totalSkipped).
		Float64("execution_time_ms", executionTime).
		Msg("Retention policy execution completed")

	return c.JSON(ExecuteRetentionResponse{
		PolicyID:             int64(policyID),
		PolicyName:           policy.Name,
		DeletedCount:         totalDeleted,
		FilesDeleted:         totalFilesDeleted,
		SkippedFiles:         totalSkipped,
		SkippedReason:        skipDetail,
		ExecutionTimeMs:      executionTime,
		DryRun:               req.DryRun,
		CutoffDate:           cutoffDate.Format(time.RFC3339),
		AffectedMeasurements: measurements,
	})
}

// handleGetExecutions returns execution history for a policy
func (h *RetentionHandler) handleGetExecutions(c *fiber.Ctx) error {
	policyID, err := c.ParamsInt("id")
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "Invalid policy ID"})
	}

	limit := c.QueryInt("limit", 50)

	rows, err := h.db.Query(`
		SELECT id, policy_id, execution_time, status, deleted_count, cutoff_date, execution_duration_ms, error_message
		FROM retention_executions
		WHERE policy_id = ?
		ORDER BY execution_time DESC
		LIMIT ?
	`, policyID, limit)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "Failed to get executions",
		})
	}
	defer rows.Close()

	var executions []RetentionExecution
	for rows.Next() {
		var ex RetentionExecution
		if err := rows.Scan(&ex.ID, &ex.PolicyID, &ex.ExecutionTime, &ex.Status, &ex.DeletedCount, &ex.CutoffDate, &ex.ExecutionDurationMs, &ex.ErrorMessage); err != nil {
			continue
		}
		executions = append(executions, ex)
	}

	return c.JSON(fiber.Map{
		"policy_id":  policyID,
		"executions": executions,
	})
}

// getPolicy retrieves a single policy by ID
func (h *RetentionHandler) getPolicy(policyID int64) (*RetentionPolicy, error) {
	row := h.db.QueryRow(`
		SELECT
			rp.id, rp.name, rp.database, rp.measurement, rp.retention_days, rp.buffer_days, rp.is_active,
			rp.created_at, rp.updated_at,
			re.execution_time, re.status, re.deleted_count
		FROM retention_policies rp
		LEFT JOIN (
			SELECT policy_id, execution_time, status, deleted_count
			FROM retention_executions
			WHERE id IN (SELECT MAX(id) FROM retention_executions GROUP BY policy_id)
		) re ON rp.id = re.policy_id
		WHERE rp.id = ?
	`, policyID)

	var p RetentionPolicy
	err := row.Scan(
		&p.ID, &p.Name, &p.Database, &p.Measurement, &p.RetentionDays, &p.BufferDays, &p.IsActive,
		&p.CreatedAt, &p.UpdatedAt,
		&p.LastExecutionTime, &p.LastExecutionStatus, &p.LastDeletedCount,
	)
	if err != nil {
		return nil, err
	}

	return &p, nil
}

// getPolicies retrieves all policies
func (h *RetentionHandler) getPolicies() ([]RetentionPolicy, error) {
	rows, err := h.db.Query(`
		SELECT
			rp.id, rp.name, rp.database, rp.measurement, rp.retention_days, rp.buffer_days, rp.is_active,
			rp.created_at, rp.updated_at,
			re.execution_time, re.status, re.deleted_count
		FROM retention_policies rp
		LEFT JOIN (
			SELECT policy_id, execution_time, status, deleted_count
			FROM retention_executions
			WHERE id IN (SELECT MAX(id) FROM retention_executions GROUP BY policy_id)
		) re ON rp.id = re.policy_id
		ORDER BY rp.created_at DESC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var policies []RetentionPolicy
	for rows.Next() {
		var p RetentionPolicy
		if err := rows.Scan(
			&p.ID, &p.Name, &p.Database, &p.Measurement, &p.RetentionDays, &p.BufferDays, &p.IsActive,
			&p.CreatedAt, &p.UpdatedAt,
			&p.LastExecutionTime, &p.LastExecutionStatus, &p.LastDeletedCount,
		); err != nil {
			continue
		}
		policies = append(policies, p)
	}

	return policies, nil
}

// getMeasurementsToProcess gets measurements for a policy
// Supports all storage backends: local, S3, and Azure
func (h *RetentionHandler) getMeasurementsToProcess(ctx context.Context, policy *RetentionPolicy) ([]string, error) {
	if policy.Measurement != nil && *policy.Measurement != "" {
		return []string{*policy.Measurement}, nil
	}

	// Get all measurements in database by listing storage with database prefix
	prefix := policy.Database + "/"
	files, err := h.storage.List(ctx, prefix)
	if err != nil {
		return nil, fmt.Errorf("failed to list files: %w", err)
	}

	// Extract unique measurement names from file paths
	// Files are stored as: database/measurement/YYYY/MM/DD/HH/file.parquet
	measurementSet := make(map[string]struct{})
	for _, f := range files {
		// Remove database prefix
		relPath := strings.TrimPrefix(f, prefix)
		// Get first path component (measurement name)
		parts := strings.SplitN(relPath, "/", 2)
		if len(parts) > 0 && parts[0] != "" && !strings.HasPrefix(parts[0], ".") {
			measurementSet[parts[0]] = struct{}{}
		}
	}

	var measurements []string
	for m := range measurementSet {
		measurements = append(measurements, m)
	}

	return measurements, nil
}

// deleteOldFiles deletes Parquet files where ALL rows are older than cutoffDate
// Supports all storage backends: local, S3, and Azure
// deleteOldFiles removes files older than cutoffDate for one measurement.
//
// The third return is the number of files that had to be skipped because their
// stored key cannot be resolved to a readable path. It is reported separately
// from an error because the rest of the measurement still processed correctly,
// and separately from "deleted nothing" because a skip is permanent: the caller
// surfaces it so a policy that can never fully apply does not keep recording
// clean runs.
func (h *RetentionHandler) deleteOldFiles(ctx context.Context, database, measurement string, cutoffDate time.Time, dryRun bool, reason string) (int64, int, int, error) {
	// List all files for this measurement using storage backend
	prefix := database + "/" + measurement + "/"
	files, err := h.storage.List(ctx, prefix)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("failed to list files: %w", err)
	}

	// Filter to only parquet files
	var parquetFiles []string
	for _, f := range files {
		if strings.HasSuffix(strings.ToLower(f), ".parquet") {
			parquetFiles = append(parquetFiles, f)
		}
	}

	h.logger.Debug().Int("file_count", len(parquetFiles)).Str("measurement", measurement).Msg("Scanning files for old data")

	var deletedRows int64
	var deletedFiles int
	var skipped int            // files whose key cannot be turned into a readable path
	var eligiblePaths []string // paths eligible for deletion (used for manifest + storage ops)
	var eligibleRows []int64   // row counts parallel to eligiblePaths

	for _, relativePath := range parquetFiles {
		fullPath, err := readParquetPath(h.storage, relativePath)
		if err != nil {
			// Counted, not just logged. A file that can never be resolved is
			// skipped on every future run too, so its data never ages out; if
			// only a Warn recorded that, the policy would keep reporting
			// "completed, 0 deleted" forever, which is precisely the shape of
			// the bug this function was fixed for (#746).
			skipped++
			h.logger.Warn().Err(err).Str("file", relativePath).Msg("Unusable storage path; file can never be processed by retention")
			continue
		}

		maxTime, rowCount, err := h.getFileMaxTimeAndRowCount(ctx, fullPath)
		if err != nil {
			h.logger.Warn().Err(err).Str("file", relativePath).Msg("Failed to read file metadata")
			continue
		}

		if maxTime.Before(cutoffDate) {
			h.logger.Info().
				Str("file", filepath.Base(relativePath)).
				Time("max_time", maxTime).
				Int64("rows", rowCount).
				Bool("dry_run", dryRun).
				Msg("File eligible for deletion")

			if !dryRun {
				eligiblePaths = append(eligiblePaths, relativePath)
				eligibleRows = append(eligibleRows, rowCount)
			} else {
				deletedRows += rowCount
				deletedFiles++
			}
		}
	}

	if dryRun || len(eligiblePaths) == 0 {
		return deletedRows, deletedFiles, skipped, nil
	}

	// Process in chunks of 1000: update manifest first, then delete from storage.
	// Interleaving per-chunk limits orphan blast radius — a mid-run manifest
	// failure only affects the current chunk, not all remaining files.
	// Manifest-before-storage ordering ensures failures are retryable: if the
	// manifest update fails, the file still exists in storage and the next
	// retention run will pick it up. A storage delete failure after a successful
	// manifest update leaves a harmless ghost on disk (compaction will clean it).
	// On manifest failure we abort — a Raft quorum loss is not transient.
	// The Phase 5 reconciler at /api/v1/reconciliation cleans up any drift
	// from partial-failure scenarios on its periodic cron run.
	const manifestBatchSize = 1000
	var deletedFilePaths []string
	for i := 0; i < len(eligiblePaths); i += manifestBatchSize {
		end := i + manifestBatchSize
		if end > len(eligiblePaths) {
			end = len(eligiblePaths)
		}
		chunk := eligiblePaths[i:end]

		// subPaths/subRows track only the files that were successfully marshalled
		// into ops. The storage delete loop uses this subset so a marshal failure
		// never causes a file to be deleted from storage without a manifest entry.
		var ops []raft.BatchFileOp
		subPaths := chunk // default: all chunk paths (no coordinator)
		subRows := eligibleRows[i:end]
		if h.coordinator != nil {
			ops = make([]raft.BatchFileOp, 0, len(chunk))
			subPaths = make([]string, 0, len(chunk))
			subRows = make([]int64, 0, len(chunk))
			for j, p := range chunk {
				payload, err := json.Marshal(raft.DeleteFilePayload{Path: p, Reason: reason})
				if err != nil {
					h.logger.Warn().Err(err).Str("file", p).Msg("Failed to marshal manifest delete op; skipping file")
					continue
				}
				ops = append(ops, raft.BatchFileOp{Type: raft.CommandDeleteFile, Payload: payload})
				subPaths = append(subPaths, p)
				subRows = append(subRows, eligibleRows[i+j])
			}
			if len(ops) > 0 {
				if err := h.coordinator.BatchFileOpsInManifest(ops); err != nil {
					h.logger.Error().Err(err).Int("count", len(ops)).Int("chunk_start", i).
						Msg("Failed to update cluster manifest; aborting retention cycle")
					if len(deletedFilePaths) > 0 {
						h.cleanupEmptyDirectories(ctx, deletedFilePaths)
					}
					return deletedRows, deletedFiles, skipped, fmt.Errorf("failed to update cluster manifest: %w", err)
				}
			}
		}

		for j, relativePath := range subPaths {
			if err := h.storage.Delete(ctx, relativePath); err != nil {
				// Storage errors are transient (network blip, file already gone) —
				// log as Warn and continue so one bad file doesn't abort the cycle.
				h.logger.Warn().Err(err).Str("file", relativePath).Msg("Failed to delete file from storage; skipping")
				continue
			}
			deletedFilePaths = append(deletedFilePaths, relativePath)
			deletedRows += subRows[j]
			deletedFiles++
		}
	}

	if len(deletedFilePaths) > 0 {
		h.cleanupEmptyDirectories(ctx, deletedFilePaths)
	}

	return deletedRows, deletedFiles, skipped, nil
}

// readParquetPath resolves a storage key to a path that can be interpolated
// into DuckDB's read_parquet().
//
// Two rules, and they are separate on purpose. storage.ObjectURI applies the
// key contract, which is what makes the URI name the object the backend
// actually wrote (the prefix bug in #746). ValidateGlobSafe is applied HERE
// rather than inside ObjectURI because it is a property of this sink: DuckDB
// treats "*", "?", "[" and "{" in a path as pattern operators, so one file's
// key would silently expand to many, while the same key handed to a literal
// reader such as iceberg-go or os.Open is fine.
func readParquetPath(backend storage.Backend, key string) (string, error) {
	uri, err := storage.ObjectURI(backend, key)
	if err != nil {
		return "", err
	}
	if err := storage.ValidateGlobSafe(uri); err != nil {
		return "", err
	}
	return uri, nil
}

// getFileMaxTimeAndRowCount reads a Parquet file to get max time and row count
func (h *RetentionHandler) getFileMaxTimeAndRowCount(ctx context.Context, filePath string) (time.Time, int64, error) {
	// Use the shared DuckDB connection to avoid memory retention from temporary connections
	db := h.duckdb.DB()

	// read_parquet() does not support parameterized queries, so escape single
	// quotes in the path to prevent SQL injection via crafted file paths.
	safePath := strings.ReplaceAll(filePath, "'", "''")
	query := fmt.Sprintf("SELECT MAX(time) as max_time, COUNT(*) as cnt FROM read_parquet('%s')", safePath)
	row := db.QueryRowContext(ctx, query)

	var maxTime time.Time
	var rowCount int64
	if err := row.Scan(&maxTime, &rowCount); err != nil {
		return time.Time{}, 0, err
	}

	return maxTime.UTC(), rowCount, nil
}

// recordExecutionStart records the start of an execution
func (h *RetentionHandler) recordExecutionStart(policyID int64, cutoffDate time.Time) int64 {
	result, err := h.db.Exec(`
		INSERT INTO retention_executions (policy_id, execution_time, status, cutoff_date)
		VALUES (?, CURRENT_TIMESTAMP, 'running', ?)
	`, policyID, cutoffDate.Format(time.RFC3339))
	if err != nil {
		h.logger.Error().Err(err).Msg("Failed to record execution start")
		return 0
	}
	id, _ := result.LastInsertId()
	return id
}

// recordExecutionComplete records completion of an execution
func (h *RetentionHandler) recordExecutionComplete(executionID int64, status string, deletedCount int64, durationMs float64, errorMessage string) {
	_, err := h.db.Exec(`
		UPDATE retention_executions SET
			status = ?, deleted_count = ?, execution_duration_ms = ?, error_message = ?
		WHERE id = ?
	`, status, deletedCount, durationMs, errorMessage, executionID)
	if err != nil {
		h.logger.Error().Err(err).Msg("Failed to record execution complete")
	}
}

// cleanupEmptyDirectories attempts to remove empty directories after file deletion.
// Only works with storage backends that implement DirectoryRemover (e.g., LocalBackend).
func (h *RetentionHandler) cleanupEmptyDirectories(ctx context.Context, deletedFiles []string) {
	remover, ok := h.storage.(storage.DirectoryRemover)
	if !ok {
		h.logger.Debug().Msg("Storage backend does not support directory removal, skipping cleanup")
		return
	}

	// Collect unique directories from deleted files
	dirs := make(map[string]struct{})
	for _, filePath := range deletedFiles {
		dir := filepath.Dir(filePath)
		dirs[dir] = struct{}{}
	}

	if len(dirs) == 0 {
		return
	}

	var removed int
	for dir := range dirs {
		removed += h.removeDirectoryTree(ctx, remover, dir)
	}

	if removed > 0 {
		h.logger.Info().Int("directories_removed", removed).Msg("Cleaned up empty directories")
	}
}

// removeDirectoryTree attempts to remove a directory and its empty parents.
// Stops at the measurement level (database/measurement) to preserve structure.
func (h *RetentionHandler) removeDirectoryTree(ctx context.Context, remover storage.DirectoryRemover, dir string) int {
	parts := strings.Split(dir, "/")
	if len(parts) <= 2 {
		return 0 // Don't remove database or measurement directories
	}

	if err := remover.RemoveDirectory(ctx, dir); err != nil {
		h.logger.Debug().Err(err).Str("dir", dir).Msg("Could not remove directory (may not be empty)")
		return 0
	}

	h.logger.Debug().Str("dir", dir).Msg("Removed empty directory")

	parent := filepath.Dir(dir)
	return 1 + h.removeDirectoryTree(ctx, remover, parent)
}
