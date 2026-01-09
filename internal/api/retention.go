package api

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/database"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/gofiber/fiber/v2"
	_ "github.com/mattn/go-sqlite3"
	"github.com/rs/zerolog"
)

// RetentionHandler handles retention policy operations
type RetentionHandler struct {
	storage storage.Backend
	config  *config.RetentionConfig
	db      *sql.DB           // SQLite for policy metadata
	duckdb  *database.DuckDB  // Shared DuckDB for parquet queries
	logger  zerolog.Logger
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
	PolicyID             int64    `json:"policy_id"`
	PolicyName           string   `json:"policy_name"`
	DeletedCount         int64    `json:"deleted_count"`
	FilesDeleted         int      `json:"files_deleted"`
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
func NewRetentionHandler(storage storage.Backend, duckdb *database.DuckDB, cfg *config.RetentionConfig, logger zerolog.Logger) (*RetentionHandler, error) {
	// Ensure directory exists
	dir := filepath.Dir(cfg.DBPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory for retention DB: %w", err)
	}

	// Open SQLite database for policy metadata
	db, err := sql.Open("sqlite3", cfg.DBPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open retention database: %w", err)
	}

	h := &RetentionHandler{
		storage: storage,
		config:  cfg,
		db:      db,
		duckdb:  duckdb,
		logger:  logger.With().Str("component", "retention-handler").Logger(),
	}

	// Initialize tables
	if err := h.initTables(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to initialize retention tables: %w", err)
	}

	return h, nil
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

// Close closes the database connection
func (h *RetentionHandler) Close() error {
	return h.db.Close()
}

// RegisterRoutes registers retention endpoints
func (h *RetentionHandler) RegisterRoutes(app *fiber.App) {
	app.Post("/api/v1/retention", h.handleCreate)
	app.Get("/api/v1/retention", h.handleList)
	app.Get("/api/v1/retention/:id", h.handleGet)
	app.Put("/api/v1/retention/:id", h.handleUpdate)
	app.Delete("/api/v1/retention/:id", h.handleDelete)
	app.Post("/api/v1/retention/:id/execute", h.handleExecute)
	app.Get("/api/v1/retention/:id/executions", h.handleGetExecutions)
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

	// Validate
	if req.RetentionDays <= req.BufferDays {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "retention_days must be greater than buffer_days",
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
	measurements, err := h.getMeasurementsToProcess(policy)
	if err != nil {
		return nil, fmt.Errorf("failed to discover measurements: %w", err)
	}

	h.logger.Info().Strs("measurements", measurements).Msg("Processing measurements")

	// Record execution start
	executionID := h.recordExecutionStart(policyID, cutoffDate)

	// Execute retention for each measurement
	var totalDeleted int64
	var totalFilesDeleted int

	for _, measurement := range measurements {
		deleted, filesDeleted, err := h.deleteOldFiles(ctx, policy.Database, measurement, cutoffDate, false)
		if err != nil {
			h.logger.Error().Err(err).Str("measurement", measurement).Msg("Failed to process measurement")
			continue
		}
		totalDeleted += deleted
		totalFilesDeleted += filesDeleted
	}

	executionTime := float64(time.Since(start).Milliseconds())

	// Record execution completion
	if executionID > 0 {
		h.recordExecutionComplete(executionID, "completed", totalDeleted, executionTime, "")
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
	measurements, err := h.getMeasurementsToProcess(policy)
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

	for _, measurement := range measurements {
		deleted, filesDeleted, err := h.deleteOldFiles(context.Background(), policy.Database, measurement, cutoffDate, req.DryRun)
		if err != nil {
			h.logger.Error().Err(err).Str("measurement", measurement).Msg("Failed to process measurement")
			continue
		}
		totalDeleted += deleted
		totalFilesDeleted += filesDeleted
	}

	executionTime := float64(time.Since(start).Milliseconds())

	// Record execution completion
	if !req.DryRun && executionID > 0 {
		h.recordExecutionComplete(executionID, "completed", totalDeleted, executionTime, "")
	}

	h.logger.Info().
		Int64("deleted_count", totalDeleted).
		Int("files_deleted", totalFilesDeleted).
		Float64("execution_time_ms", executionTime).
		Msg("Retention policy execution completed")

	return c.JSON(ExecuteRetentionResponse{
		PolicyID:             int64(policyID),
		PolicyName:           policy.Name,
		DeletedCount:         totalDeleted,
		FilesDeleted:         totalFilesDeleted,
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
func (h *RetentionHandler) getMeasurementsToProcess(policy *RetentionPolicy) ([]string, error) {
	if policy.Measurement != nil && *policy.Measurement != "" {
		return []string{*policy.Measurement}, nil
	}

	// Get all measurements in database by scanning storage
	basePath := h.getStorageBasePath()
	if basePath == "" {
		return nil, fmt.Errorf("unable to determine storage base path")
	}

	dbPath := filepath.Join(basePath, policy.Database)
	entries, err := os.ReadDir(dbPath)
	if err != nil {
		if os.IsNotExist(err) {
			return []string{}, nil
		}
		return nil, err
	}

	var measurements []string
	for _, entry := range entries {
		if entry.IsDir() && !strings.HasPrefix(entry.Name(), ".") {
			measurements = append(measurements, entry.Name())
		}
	}

	return measurements, nil
}

// deleteOldFiles deletes Parquet files where ALL rows are older than cutoffDate
func (h *RetentionHandler) deleteOldFiles(ctx context.Context, database, measurement string, cutoffDate time.Time, dryRun bool) (int64, int, error) {
	basePath := h.getStorageBasePath()
	if basePath == "" {
		return 0, 0, fmt.Errorf("unable to determine storage base path")
	}

	measurementPath := filepath.Join(basePath, database, measurement)
	if _, err := os.Stat(measurementPath); os.IsNotExist(err) {
		return 0, 0, nil
	}

	// Find all parquet files
	var parquetFiles []string
	err := filepath.Walk(measurementPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if !info.IsDir() && strings.HasSuffix(strings.ToLower(info.Name()), ".parquet") {
			parquetFiles = append(parquetFiles, path)
		}
		return nil
	})
	if err != nil {
		return 0, 0, err
	}

	h.logger.Debug().Int("file_count", len(parquetFiles)).Str("measurement", measurement).Msg("Scanning files for old data")

	// Check each file
	var deletedRows int64
	var deletedFiles int

	for _, filePath := range parquetFiles {
		maxTime, rowCount, err := h.getFileMaxTimeAndRowCount(ctx, filePath)
		if err != nil {
			h.logger.Warn().Err(err).Str("file", filePath).Msg("Failed to read file metadata")
			continue
		}

		// If ALL rows in file are older than cutoff, delete the file
		if maxTime.Before(cutoffDate) {
			h.logger.Info().
				Str("file", filepath.Base(filePath)).
				Time("max_time", maxTime).
				Int64("rows", rowCount).
				Bool("dry_run", dryRun).
				Msg("File eligible for deletion")

			if !dryRun {
				if err := os.Remove(filePath); err != nil {
					h.logger.Error().Err(err).Str("file", filePath).Msg("Failed to delete file")
					continue
				}
			}

			deletedRows += rowCount
			deletedFiles++
		}
	}

	return deletedRows, deletedFiles, nil
}

// getFileMaxTimeAndRowCount reads a Parquet file to get max time and row count
func (h *RetentionHandler) getFileMaxTimeAndRowCount(ctx context.Context, filePath string) (time.Time, int64, error) {
	// Use the shared DuckDB connection to avoid memory retention from temporary connections
	db := h.duckdb.DB()

	// Get max time and count
	query := fmt.Sprintf("SELECT MAX(time) as max_time, COUNT(*) as cnt FROM read_parquet('%s')", filePath)
	row := db.QueryRowContext(ctx, query)

	var maxTimeMicros int64
	var rowCount int64
	if err := row.Scan(&maxTimeMicros, &rowCount); err != nil {
		return time.Time{}, 0, err
	}

	// Convert microseconds to time
	maxTime := time.UnixMicro(maxTimeMicros).UTC()

	return maxTime, rowCount, nil
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

// getStorageBasePath returns the base path for storage
func (h *RetentionHandler) getStorageBasePath() string {
	return storage.GetLocalBasePath(h.storage, &h.logger, "Retention", "./data/arc")
}
