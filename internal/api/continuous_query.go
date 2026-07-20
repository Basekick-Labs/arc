package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/basekick-labs/arc/internal/auth"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/database"
	"github.com/basekick-labs/arc/internal/ingest"
	sqlutil "github.com/basekick-labs/arc/internal/sql"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/basekick-labs/arc/pkg/models"
	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	_ "github.com/mattn/go-sqlite3"
	"github.com/rs/zerolog"
)

// CQSchedulerReloader is an interface for reloading CQ schedules after updates.
// This avoids a circular import between api and scheduler packages.
type CQSchedulerReloader interface {
	ReloadCQ(cqID int64) error
	// StartJobDirect schedules a job using data already in hand, avoiding a
	// redundant SQLite read that could race with the just-committed INSERT.
	StartJobDirect(cqID int64, name, interval string, isActive bool) error
}

// CQCoordinator is the minimal cluster interface the CQ handler needs to gate
// manual execute requests to the primary writer. nil = standalone mode (no gate).
type CQCoordinator interface {
	IsPrimaryWriter() bool
	Role() string
}

// ContinuousQueryHandler handles continuous query operations
type ContinuousQueryHandler struct {
	db          *database.DuckDB
	storage     storage.Backend
	arrowBuffer *ingest.ArrowBuffer
	config      *config.ContinuousQueryConfig
	sqliteDB    *sql.DB
	authManager *auth.AuthManager
	scheduler   CQSchedulerReloader
	coordinator CQCoordinator
	logger      zerolog.Logger
}

// SetScheduler sets the CQ scheduler for reloading after updates.
// Called after scheduler creation since it depends on the handler.
func (h *ContinuousQueryHandler) SetScheduler(s CQSchedulerReloader) {
	h.scheduler = s
}

// SetCoordinator sets the cluster coordinator for writer-gate checks.
// Called after coordinator creation since it depends on the handler.
func (h *ContinuousQueryHandler) SetCoordinator(c CQCoordinator) {
	h.coordinator = c
}

// ContinuousQuery represents a continuous query definition
type ContinuousQuery struct {
	ID                     int64   `json:"id"`
	Name                   string  `json:"name"`
	Description            *string `json:"description"`
	Database               string  `json:"database"`
	SourceMeasurement      string  `json:"source_measurement"`
	DestinationMeasurement string  `json:"destination_measurement"`
	Query                  string  `json:"query"`
	Interval               string  `json:"interval"`
	// TagColumns are the output columns that are grouping dimensions (not
	// aggregates). They are written as Parquet `arc:tags` metadata so that
	// compaction can dedup CQ output on (tags, time), making re-emitted windows
	// idempotent (#521). Empty means no dedup key — duplicate windows accumulate.
	TagColumns            []string `json:"tag_columns"`
	RetentionDays         *int     `json:"retention_days"`
	DeleteSourceAfterDays *int     `json:"delete_source_after_days"`
	IsActive              bool     `json:"is_active"`
	LastExecutionTime     *string  `json:"last_execution_time"`
	LastExecutionStatus   *string  `json:"last_execution_status"`
	LastProcessedTime     *string  `json:"last_processed_time"`
	LastRecordsWritten    *int64   `json:"last_records_written"`
	CreatedAt             string   `json:"created_at"`
	UpdatedAt             string   `json:"updated_at"`
}

// ContinuousQueryRequest represents a request to create/update a CQ
type ContinuousQueryRequest struct {
	Name                   string   `json:"name"`
	Description            *string  `json:"description"`
	Database               string   `json:"database"`
	SourceMeasurement      string   `json:"source_measurement"`
	DestinationMeasurement string   `json:"destination_measurement"`
	Query                  string   `json:"query"`
	Interval               string   `json:"interval"`
	TagColumns             []string `json:"tag_columns"`
	RetentionDays          *int     `json:"retention_days"`
	DeleteSourceAfterDays  *int     `json:"delete_source_after_days"`
	IsActive               bool     `json:"is_active"`
}

// ExecuteCQRequest represents a request to execute a CQ
type ExecuteCQRequest struct {
	StartTime *string `json:"start_time"`
	EndTime   *string `json:"end_time"`
	DryRun    bool    `json:"dry_run"`
}

// ExecuteCQResponse represents the result of executing a CQ
type ExecuteCQResponse struct {
	QueryID                int64   `json:"query_id"`
	QueryName              string  `json:"query_name"`
	ExecutionID            string  `json:"execution_id"`
	Status                 string  `json:"status"`
	StartTime              string  `json:"start_time"`
	EndTime                string  `json:"end_time"`
	RecordsRead            *int64  `json:"records_read"`
	RecordsWritten         int64   `json:"records_written"`
	ExecutionTimeSeconds   float64 `json:"execution_time_seconds"`
	DestinationMeasurement string  `json:"destination_measurement"`
	DryRun                 bool    `json:"dry_run"`
	ExecutedAt             string  `json:"executed_at"`
	ExecutedQuery          string  `json:"executed_query,omitempty"`
}

// CQExecution represents an execution history record
type CQExecution struct {
	ID                       int64   `json:"id"`
	QueryID                  int64   `json:"query_id"`
	ExecutionID              string  `json:"execution_id"`
	ExecutionTime            string  `json:"execution_time"`
	Status                   string  `json:"status"`
	StartTime                string  `json:"start_time"`
	EndTime                  string  `json:"end_time"`
	RecordsRead              *int64  `json:"records_read"`
	RecordsWritten           int64   `json:"records_written"`
	ExecutionDurationSeconds float64 `json:"execution_duration_seconds"`
	ErrorMessage             *string `json:"error_message"`
}

// NewContinuousQueryHandler creates a new continuous query handler
func NewContinuousQueryHandler(db *database.DuckDB, storage storage.Backend, arrowBuffer *ingest.ArrowBuffer, cfg *config.ContinuousQueryConfig, authManager *auth.AuthManager, logger zerolog.Logger) (*ContinuousQueryHandler, error) {
	// Ensure directory exists
	dir := filepath.Dir(cfg.DBPath)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, fmt.Errorf("failed to create directory for CQ DB: %w", err)
	}

	// Open SQLite database
	sqliteDB, err := sql.Open("sqlite3", cfg.DBPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open CQ database: %w", err)
	}

	h := &ContinuousQueryHandler{
		db:          db,
		storage:     storage,
		arrowBuffer: arrowBuffer,
		config:      cfg,
		sqliteDB:    sqliteDB,
		authManager: authManager,
		logger:      logger.With().Str("component", "cq-handler").Logger(),
	}

	// Initialize tables
	if err := h.initTables(); err != nil {
		sqliteDB.Close()
		return nil, fmt.Errorf("failed to initialize CQ tables: %w", err)
	}

	return h, nil
}

// initTables creates the continuous query tables
func (h *ContinuousQueryHandler) initTables() error {
	// Continuous queries table
	_, err := h.sqliteDB.Exec(`
		CREATE TABLE IF NOT EXISTS continuous_queries (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			name TEXT UNIQUE NOT NULL,
			description TEXT,
			database TEXT NOT NULL,
			source_measurement TEXT NOT NULL,
			destination_measurement TEXT NOT NULL,
			query TEXT NOT NULL,
			interval TEXT NOT NULL,
			tag_columns TEXT,
			retention_days INTEGER,
			delete_source_after_days INTEGER,
			is_active BOOLEAN DEFAULT TRUE,
			last_processed_time TIMESTAMP,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to create continuous_queries table: %w", err)
	}

	// Migration: add tag_columns to pre-existing databases (#521). The column
	// holds a JSON-encoded []string of grouping-dimension output columns, used
	// as the Parquet arc:tags dedup key. A duplicate-column error means an
	// already-migrated DB — ignore it; any other error is fatal.
	if _, err = h.sqliteDB.Exec(`ALTER TABLE continuous_queries ADD COLUMN tag_columns TEXT`); err != nil &&
		!strings.Contains(err.Error(), "duplicate column name") {
		return fmt.Errorf("failed to add tag_columns column: %w", err)
	}

	// Execution history table
	_, err = h.sqliteDB.Exec(`
		CREATE TABLE IF NOT EXISTS continuous_query_executions (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			query_id INTEGER NOT NULL,
			execution_id TEXT NOT NULL,
			execution_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			status TEXT NOT NULL,
			start_time TIMESTAMP NOT NULL,
			end_time TIMESTAMP NOT NULL,
			records_read INTEGER,
			records_written INTEGER DEFAULT 0,
			execution_duration_seconds FLOAT,
			error_message TEXT,
			FOREIGN KEY (query_id) REFERENCES continuous_queries (id)
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to create continuous_query_executions table: %w", err)
	}

	h.logger.Info().Msg("Continuous query tables initialized")
	return nil
}

// Close closes the database connection
func (h *ContinuousQueryHandler) Close() error {
	return h.sqliteDB.Close()
}

// RegisterRoutes registers continuous query endpoints
func (h *ContinuousQueryHandler) RegisterRoutes(app *fiber.App) {
	group := app.Group("/api/v1/continuous_queries")
	if h.authManager != nil {
		group.Use(auth.RequireAdmin(h.authManager))
	}
	group.Post("/", h.handleCreate)
	group.Get("/", h.handleList)
	group.Get("/:id", h.handleGet)
	group.Put("/:id", h.handleUpdate)
	group.Delete("/:id", h.handleDelete)
	group.Post("/:id/execute", h.handleExecute)
	group.Get("/:id/executions", h.handleGetExecutions)
}

// handleCreate creates a new continuous query
func (h *ContinuousQueryHandler) handleCreate(c *fiber.Ctx) error {
	if !h.config.Enabled {
		return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
			"error": "Continuous queries are disabled",
		})
	}

	var req ContinuousQueryRequest
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
	if req.SourceMeasurement == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "source_measurement is required"})
	}
	if req.DestinationMeasurement == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "destination_measurement is required"})
	}
	if !isValidMeasurementName(req.DestinationMeasurement) {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "invalid destination_measurement: must start with a letter and contain only alphanumeric characters, underscores, or hyphens",
		})
	}
	if req.Query == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "query is required"})
	}
	if req.Interval == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "interval is required"})
	}

	// Validate query has required placeholders
	if !strings.Contains(req.Query, "{start_time}") || !strings.Contains(req.Query, "{end_time}") {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "query must contain {start_time} and {end_time} placeholders",
		})
	}

	// Validate tag columns before storing — they end up in Parquet arc:tags
	// metadata and compaction's PARTITION BY, so unsafe names must be rejected
	// at the boundary (#521).
	if err := validateTagColumns(req.TagColumns); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
	}
	tagColumnsJSON, err := encodeTagColumns(req.TagColumns)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "failed to encode tag_columns"})
	}

	// Insert query
	result, err := h.sqliteDB.Exec(`
		INSERT INTO continuous_queries
		(name, description, database, source_measurement, destination_measurement, query, interval, tag_columns, retention_days, delete_source_after_days, is_active)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, req.Name, req.Description, req.Database, req.SourceMeasurement, req.DestinationMeasurement, req.Query, req.Interval, tagColumnsJSON, req.RetentionDays, req.DeleteSourceAfterDays, req.IsActive)

	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE constraint") {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": fmt.Sprintf("Continuous query with name '%s' already exists", req.Name),
			})
		}
		h.logger.Error().Err(err).Msg("Failed to create continuous query")
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "Failed to create continuous query",
		})
	}

	queryID, _ := result.LastInsertId()
	h.logger.Info().Int64("query_id", queryID).Str("name", req.Name).Msg("Created continuous query")

	// Kick the scheduler using the data we already have — avoids a redundant
	// SQLite read that could race with the just-committed INSERT on a
	// multi-connection pool.
	if h.scheduler != nil {
		if err := h.scheduler.StartJobDirect(queryID, req.Name, req.Interval, req.IsActive); err != nil {
			h.logger.Warn().Err(err).Int64("query_id", queryID).Msg("Failed to start CQ job after create")
		}
	}

	// Return created query
	cq, err := h.getQuery(queryID)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "Failed to retrieve created query",
		})
	}

	return c.Status(fiber.StatusCreated).JSON(cq)
}

// handleList returns all continuous queries
func (h *ContinuousQueryHandler) handleList(c *fiber.Ctx) error {
	database := c.Query("database")
	isActiveStr := c.Query("is_active")

	queries, err := h.getQueries(database, isActiveStr)
	if err != nil {
		h.logger.Error().Err(err).Msg("Failed to list continuous queries")
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "Failed to list continuous queries",
		})
	}

	return c.JSON(queries)
}

// handleGet returns a single continuous query
func (h *ContinuousQueryHandler) handleGet(c *fiber.Ctx) error {
	queryID, err := c.ParamsInt("id")
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "Invalid query ID"})
	}

	cq, err := h.getQuery(int64(queryID))
	if err != nil {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"error": "Continuous query not found"})
	}

	return c.JSON(cq)
}

// handleUpdate updates an existing continuous query
func (h *ContinuousQueryHandler) handleUpdate(c *fiber.Ctx) error {
	queryID, err := c.ParamsInt("id")
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "Invalid query ID"})
	}

	var req ContinuousQueryRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "Invalid request body: " + err.Error(),
		})
	}

	// Validate destination_measurement if provided
	if req.DestinationMeasurement != "" && !isValidMeasurementName(req.DestinationMeasurement) {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "invalid destination_measurement: must start with a letter and contain only alphanumeric characters, underscores, or hyphens",
		})
	}

	if err := validateTagColumns(req.TagColumns); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
	}
	tagColumnsJSON, err := encodeTagColumns(req.TagColumns)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "failed to encode tag_columns"})
	}

	result, err := h.sqliteDB.Exec(`
		UPDATE continuous_queries SET
			name = ?, description = ?, database = ?, source_measurement = ?,
			destination_measurement = ?, query = ?, interval = ?, tag_columns = ?, retention_days = ?,
			delete_source_after_days = ?, is_active = ?, updated_at = CURRENT_TIMESTAMP
		WHERE id = ?
	`, req.Name, req.Description, req.Database, req.SourceMeasurement, req.DestinationMeasurement, req.Query, req.Interval, tagColumnsJSON, req.RetentionDays, req.DeleteSourceAfterDays, req.IsActive, queryID)

	if err != nil {
		h.logger.Error().Err(err).Msg("Failed to update continuous query")
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "Failed to update continuous query",
		})
	}

	rows, _ := result.RowsAffected()
	if rows == 0 {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"error": "Continuous query not found"})
	}

	h.logger.Info().Int("query_id", queryID).Msg("Updated continuous query")

	// Reload the scheduler so it picks up the new definition
	if h.scheduler != nil {
		if err := h.scheduler.ReloadCQ(int64(queryID)); err != nil {
			h.logger.Warn().Err(err).Int("query_id", queryID).Msg("Failed to reload CQ schedule after update")
		}
	}

	cq, _ := h.getQuery(int64(queryID))
	return c.JSON(cq)
}

// handleDelete deletes a continuous query
func (h *ContinuousQueryHandler) handleDelete(c *fiber.Ctx) error {
	queryID, err := c.ParamsInt("id")
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "Invalid query ID"})
	}

	// Delete execution history first
	_, _ = h.sqliteDB.Exec("DELETE FROM continuous_query_executions WHERE query_id = ?", queryID)

	// Delete query
	result, err := h.sqliteDB.Exec("DELETE FROM continuous_queries WHERE id = ?", queryID)
	if err != nil {
		h.logger.Error().Err(err).Msg("Failed to delete continuous query")
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "Failed to delete continuous query",
		})
	}

	rows, _ := result.RowsAffected()
	if rows == 0 {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"error": "Continuous query not found"})
	}

	h.logger.Info().Int("query_id", queryID).Msg("Deleted continuous query")

	return c.JSON(fiber.Map{"message": "Continuous query deleted successfully"})
}

// ExecuteCQ executes a continuous query by ID programmatically (used by scheduler)
// Returns the execution response and any error
func (h *ContinuousQueryHandler) ExecuteCQ(ctx context.Context, queryID int64) (*ExecuteCQResponse, error) {
	start := time.Now()

	// Get query definition
	cq, err := h.getQuery(queryID)
	if err != nil {
		return nil, fmt.Errorf("continuous query not found: %w", err)
	}

	if !cq.IsActive {
		return nil, fmt.Errorf("continuous query is not active")
	}

	// Determine time range - use last processed time or default to 1 hour ago
	var startTime, endTime time.Time

	if cq.LastProcessedTime != nil {
		startTime, _ = time.Parse(time.RFC3339, *cq.LastProcessedTime)
		startTime = startTime.UTC()
	} else {
		startTime = time.Now().UTC().Add(-1 * time.Hour)
	}
	endTime = time.Now().UTC()

	if !startTime.Before(endTime) {
		return nil, fmt.Errorf("start_time must be before end_time")
	}

	// Replace placeholders in query
	executedQuery := strings.ReplaceAll(cq.Query, "{start_time}", fmt.Sprintf("'%s'", startTime.Format(time.RFC3339)))
	executedQuery = strings.ReplaceAll(executedQuery, "{end_time}", fmt.Sprintf("'%s'", endTime.Format(time.RFC3339)))

	executionID := fmt.Sprintf("cq-sched-%s", uuid.New().String()[:8])

	h.logger.Info().
		Str("query_name", cq.Name).
		Str("execution_id", executionID).
		Time("start_time", startTime).
		Time("end_time", endTime).
		Msg("Executing scheduled continuous query")

	// Execute the aggregation query using DuckDB
	recordsWritten, err := h.executeAggregation(ctx, cq, executedQuery, startTime, endTime)
	if err != nil {
		executionDuration := time.Since(start).Seconds()

		// Record failed execution
		h.recordExecution(queryID, executionID, "failed", startTime, endTime, nil, 0, executionDuration, err.Error())

		h.logger.Error().Err(err).Str("query_name", cq.Name).Msg("Scheduled continuous query execution failed")
		return nil, fmt.Errorf("execution failed: %w", err)
	}

	executionDuration := time.Since(start).Seconds()

	// Record successful execution and update last processed time atomically
	if err := h.recordExecutionAndUpdateTime(queryID, executionID, startTime, endTime, recordsWritten, executionDuration); err != nil {
		h.logger.Error().Err(err).Str("query_name", cq.Name).Msg("Failed to record execution (data was written)")
	}

	h.logger.Info().
		Str("query_name", cq.Name).
		Int64("records_written", recordsWritten).
		Float64("duration_seconds", executionDuration).
		Msg("Scheduled continuous query completed")

	return &ExecuteCQResponse{
		QueryID:                queryID,
		QueryName:              cq.Name,
		ExecutionID:            executionID,
		Status:                 "completed",
		StartTime:              startTime.Format(time.RFC3339),
		EndTime:                endTime.Format(time.RFC3339),
		RecordsRead:            nil,
		RecordsWritten:         recordsWritten,
		ExecutionTimeSeconds:   executionDuration,
		DestinationMeasurement: cq.DestinationMeasurement,
		DryRun:                 false,
		ExecutedAt:             time.Now().UTC().Format(time.RFC3339),
	}, nil
}

// GetActiveCQs returns all active continuous queries (used by scheduler)
func (h *ContinuousQueryHandler) GetActiveCQs() ([]ContinuousQuery, error) {
	return h.getQueries("", "true")
}

// GetCQ returns a continuous query by ID (used by scheduler)
func (h *ContinuousQueryHandler) GetCQ(queryID int64) (*ContinuousQuery, error) {
	return h.getQuery(queryID)
}

// handleExecute executes a continuous query
func (h *ContinuousQueryHandler) handleExecute(c *fiber.Ctx) error {
	start := time.Now()

	// Cluster gate: only the primary writer may execute CQs.
	if h.coordinator != nil && !h.coordinator.IsPrimaryWriter() {
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
			"error": fmt.Sprintf("CQ execution rejected: node role %q is not primary writer", h.coordinator.Role()),
		})
	}

	queryID, err := c.ParamsInt("id")
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "Invalid query ID"})
	}

	var req ExecuteCQRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "Invalid request body: " + err.Error(),
		})
	}

	// Get query definition
	cq, err := h.getQuery(int64(queryID))
	if err != nil {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"error": "Continuous query not found"})
	}

	if !cq.IsActive {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "Continuous query is not active"})
	}

	// Determine time range
	var startTime, endTime time.Time

	if req.StartTime != nil && *req.StartTime != "" {
		startTime, err = time.Parse(time.RFC3339, *req.StartTime)
		if err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "Invalid start_time format"})
		}
		startTime = startTime.UTC()
	} else if cq.LastProcessedTime != nil {
		startTime, _ = time.Parse(time.RFC3339, *cq.LastProcessedTime)
		startTime = startTime.UTC()
	} else {
		startTime = time.Now().UTC().Add(-1 * time.Hour)
	}

	if req.EndTime != nil && *req.EndTime != "" {
		endTime, err = time.Parse(time.RFC3339, *req.EndTime)
		if err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "Invalid end_time format"})
		}
		endTime = endTime.UTC()
	} else {
		endTime = time.Now().UTC()
	}

	if !startTime.Before(endTime) {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "start_time must be before end_time"})
	}

	// Replace placeholders in query
	executedQuery := strings.ReplaceAll(cq.Query, "{start_time}", fmt.Sprintf("'%s'", startTime.Format(time.RFC3339)))
	executedQuery = strings.ReplaceAll(executedQuery, "{end_time}", fmt.Sprintf("'%s'", endTime.Format(time.RFC3339)))

	executionID := fmt.Sprintf("cq-exec-%s", uuid.New().String()[:8])

	h.logger.Info().
		Str("query_name", cq.Name).
		Str("execution_id", executionID).
		Time("start_time", startTime).
		Time("end_time", endTime).
		Bool("dry_run", req.DryRun).
		Msg("Executing continuous query")

	// Dry run - just return the query
	if req.DryRun {
		executionDuration := time.Since(start).Seconds()

		return c.JSON(ExecuteCQResponse{
			QueryID:                int64(queryID),
			QueryName:              cq.Name,
			ExecutionID:            executionID,
			Status:                 "dry_run",
			StartTime:              startTime.Format(time.RFC3339),
			EndTime:                endTime.Format(time.RFC3339),
			RecordsRead:            nil,
			RecordsWritten:         0,
			ExecutionTimeSeconds:   executionDuration,
			DestinationMeasurement: cq.DestinationMeasurement,
			DryRun:                 true,
			ExecutedAt:             time.Now().UTC().Format(time.RFC3339),
			ExecutedQuery:          executedQuery,
		})
	}

	// Execute the aggregation query using DuckDB. Derive from the request context
	// (so a client disconnect cancels downstream context-aware work) and cap the
	// runtime with the same 10-minute deadline the scheduler path uses, so a
	// manual execute over a very wide range can't run unbounded.
	ctx, cancel := context.WithTimeout(c.Context(), 10*time.Minute)
	defer cancel()
	recordsWritten, err := h.executeAggregation(ctx, cq, executedQuery, startTime, endTime)
	if err != nil {
		executionDuration := time.Since(start).Seconds()

		// Record failed execution
		h.recordExecution(int64(queryID), executionID, "failed", startTime, endTime, nil, 0, executionDuration, err.Error())

		h.logger.Error().Err(err).Str("query_name", cq.Name).Msg("Continuous query execution failed")
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "Execution failed: " + err.Error(),
		})
	}

	executionDuration := time.Since(start).Seconds()

	// Record successful execution and update last processed time atomically
	if err := h.recordExecutionAndUpdateTime(int64(queryID), executionID, startTime, endTime, recordsWritten, executionDuration); err != nil {
		h.logger.Error().Err(err).Int("query_id", queryID).Msg("Failed to record execution")
	}

	h.logger.Info().
		Str("query_name", cq.Name).
		Int64("records_written", recordsWritten).
		Float64("duration_seconds", executionDuration).
		Msg("Continuous query completed")

	return c.JSON(ExecuteCQResponse{
		QueryID:                int64(queryID),
		QueryName:              cq.Name,
		ExecutionID:            executionID,
		Status:                 "completed",
		StartTime:              startTime.Format(time.RFC3339),
		EndTime:                endTime.Format(time.RFC3339),
		RecordsRead:            nil,
		RecordsWritten:         recordsWritten,
		ExecutionTimeSeconds:   executionDuration,
		DestinationMeasurement: cq.DestinationMeasurement,
		DryRun:                 false,
		ExecutedAt:             time.Now().UTC().Format(time.RFC3339),
	})
}

// wrapSourceMeasurement rewrites `FROM <database>.<measurement>` in a
// continuous-query body into the given read_parquet(...) expression.
//
// Scope is deliberately narrow — only the `FROM db.measurement` form is matched
// (not JOIN, comma-joins, or quoted identifiers). A regex cannot safely
// distinguish a genuine table position from a same-named column qualifier
// (`db.m.field`), a projection/GROUP BY column (`SELECT a, db.m`), or text inside
// a string literal without a real SQL tokenizer; widening the pattern to those
// forms silently corrupts valid queries (rewriting a table-valued function into
// a scalar position). Anything not matched here is left unchanged and fails
// loudly at query time rather than being mis-rewritten. Broader, parser-based
// source matching is tracked as a follow-up. See
// [[feedback_regex_sql_gating_is_leaky]].
//
// ReplaceAllLiteralString (not ReplaceAllString) is used so '$' characters in
// the storage path are inserted verbatim rather than interpreted as regexp
// submatch references ($1, $name).
func wrapSourceMeasurement(query, database, measurement, readParquetExpr string) string {
	pattern := regexp.MustCompile(`(?i)\bFROM\s+` + regexp.QuoteMeta(database) + `\.` + regexp.QuoteMeta(measurement) + `\b`)
	return pattern.ReplaceAllLiteralString(query, "FROM "+readParquetExpr)
}

// executeAggregation runs the aggregation query and writes results.
// startTime is the window's inclusive left boundary — it stamps output rows
// when the aggregation query does not select a time column, so re-runs of the
// same window produce identical (tags, time) rows that dedup at compaction (#521).
// The window's end time is intentionally unused here: output rows are labelled by
// the window START (bucket-start convention), so only startTime is needed.
func (h *ContinuousQueryHandler) executeAggregation(ctx context.Context, cq *ContinuousQuery, query string, startTime, _ time.Time) (int64, error) {
	// Build storage path for source measurement (supports local, S3, Azure)
	measurementPath := storage.GetStoragePath(h.storage, cq.Database, cq.SourceMeasurement)

	// Extract CTE names to avoid replacing them with read_parquet paths
	cteNames := extractCTENames(query)

	// Wrap query to read from parquet files
	// Replace database.measurement reference in query with read_parquet
	// Arc uses database.measurement format (e.g., FROM production.cpu)
	// Skip if the source measurement name matches a CTE name (it's a virtual table reference)
	wrappedQuery := query
	if !cteNames[strings.ToLower(cq.SourceMeasurement)] {
		// Escape single quotes: DuckDB read_parquet() paths cannot be
		// parameterized, so the path is interpolated into a SQL string literal.
		readParquetExpr := fmt.Sprintf("read_parquet('%s', union_by_name=true)", sqlutil.EscapeStringLiteral(measurementPath))
		wrappedQuery = wrapSourceMeasurement(query, cq.Database, cq.SourceMeasurement, readParquetExpr)
	}

	h.logger.Debug().Str("query", wrappedQuery).Msg("Executing wrapped query")

	// Execute query using DuckDB
	rows, err := h.db.Query(wrappedQuery)
	if err != nil {
		return 0, fmt.Errorf("query execution failed: %w", err)
	}
	defer rows.Close()

	// Get column names
	columns, err := rows.Columns()
	if err != nil {
		return 0, fmt.Errorf("failed to get columns: %w", err)
	}

	// Read all results
	var records []map[string]interface{}
	for rows.Next() {
		// Create slice of interface{} to scan into
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return 0, fmt.Errorf("failed to scan row: %w", err)
		}

		// Convert to map
		record := make(map[string]interface{})
		for i, col := range columns {
			record[col] = values[i]
		}
		records = append(records, record)
	}

	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("row iteration error: %w", err)
	}

	h.logger.Info().Int("record_count", len(records)).Msg("Aggregation query returned records")

	if len(records) == 0 {
		return 0, nil
	}

	// Write aggregated results to destination measurement using arrow buffer
	if h.arrowBuffer == nil {
		return 0, fmt.Errorf("arrow buffer not initialized")
	}

	// Convert row-based records to columnar format for WriteColumnarDirect
	// First, collect all column names
	columnSet := make(map[string]bool)
	for _, record := range records {
		for col := range record {
			columnSet[col] = true
		}
	}

	// Initialize columnar data structure
	columnarData := make(map[string][]interface{})
	for col := range columnSet {
		columnarData[col] = make([]interface{}, 0, len(records))
	}

	// Fill columnar data from records
	for _, record := range records {
		for col := range columnSet {
			val, ok := record[col]
			if !ok {
				val = nil
			}

			// Handle time field - convert to int64 microseconds (required by ArrowBuffer)
			if col == "time" && val != nil {
				switch t := val.(type) {
				case int64:
					// Already microseconds, keep as int64
					val = t
				case float64:
					val = int64(t)
				case time.Time:
					val = t.UnixMicro()
				case string:
					if parsed, err := time.Parse(time.RFC3339, t); err == nil {
						val = parsed.UTC().UnixMicro()
					}
				}
			}

			columnarData[col] = append(columnarData[col], val)
		}
	}

	// Ensure time column exists (as int64 microseconds).
	// When the aggregation query does not select a time column, stamp every row
	// with the WINDOW START time — not time.Now() (#521). time.Now() gave rows a
	// wall-clock ingestion timestamp (wrong for a time-series) and made a re-run
	// of the same window produce rows with a DIFFERENT timestamp, so they could
	// never be deduped. The window start is stable across re-runs, so duplicate
	// emissions land at the identical (tags, time) and collapse at compaction.
	if _, hasTime := columnarData["time"]; !hasTime {
		windowMicro := startTime.UTC().UnixMicro()
		times := make([]interface{}, len(records))
		for i := range times {
			times[i] = windowMicro
		}
		columnarData["time"] = times
	}

	// Determine which declared tag columns are actually present in the output.
	// A tag name that isn't an output column must NOT reach the Parquet arc:tags
	// metadata: compaction's dedup builds `PARTITION BY "<tag>", "time"` from that
	// metadata, and a name with no matching column fails to bind and wedges the
	// whole partition (it can never compact). Intersecting here makes a stale or
	// mistaken tag_columns entry harmless rather than a compaction wedge (#521).
	var tagColumns []string
	for _, name := range cq.TagColumns {
		if _, ok := columnarData[name]; ok {
			tagColumns = append(tagColumns, name)
		} else {
			h.logger.Warn().
				Str("query_name", cq.Name).
				Str("tag_column", name).
				Msg("Declared tag_column is not an output column of the CQ; skipping (would otherwise wedge compaction)")
		}
	}

	// Decide whether to mark this output for time-only dedup (#521). This ONLY
	// matters when there are no tag columns — with tags, arc:tags already drives
	// dedup on (tags, time). The marker makes compaction dedup on time ALONE, so
	// it is safe ONLY if time is a unique key in the output (one row per window).
	//
	// CRITICAL: we must NOT assume "no tag columns" means "one row per window". A
	// grouped CQ (e.g. GROUP BY host) whose operator forgot to declare
	// tag_columns produces MULTIPLE rows per timestamp with no tags — marking it
	// for time-only dedup would make compaction keep one row per timestamp and
	// silently DELETE every series but one. So we verify uniqueness against the
	// actual output and, if time repeats without tags, skip the marker and warn
	// the operator to declare tag_columns.
	dedupTime := false
	if len(tagColumns) == 0 {
		if timeColumnIsUnique(columnarData["time"]) {
			dedupTime = true
		} else {
			h.logger.Warn().
				Str("query_name", cq.Name).
				Msg("CQ output has multiple rows per timestamp but no tag_columns declared; " +
					"skipping time-only dedup to avoid data loss. Declare tag_columns for the " +
					"grouping dimensions to make this CQ idempotent (#521).")
		}
	}

	// Write using WriteColumnarRecord so TagColumns (and the dedup-time marker)
	// are preserved as Parquet metadata, enabling compaction to dedup CQ output.
	// This goes through the identical buffer/flush/WAL path as WriteColumnarDirect.
	record := &models.ColumnarRecord{
		Measurement: cq.DestinationMeasurement,
		Columnar:    true,
		Columns:     columnarData,
		TagColumns:  tagColumns,
		DedupTime:   dedupTime,
	}
	if err := h.arrowBuffer.WriteColumnarRecord(ctx, cq.Database, record); err != nil {
		return 0, fmt.Errorf("failed to write records: %w", err)
	}

	return int64(len(records)), nil
}

// handleGetExecutions returns execution history for a query
func (h *ContinuousQueryHandler) handleGetExecutions(c *fiber.Ctx) error {
	queryID, err := c.ParamsInt("id")
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "Invalid query ID"})
	}

	limit := c.QueryInt("limit", 50)

	rows, err := h.sqliteDB.Query(`
		SELECT id, query_id, execution_id, execution_time, status, start_time, end_time, records_read, records_written, execution_duration_seconds, error_message
		FROM continuous_query_executions
		WHERE query_id = ?
		ORDER BY execution_time DESC
		LIMIT ?
	`, queryID, limit)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "Failed to get executions",
		})
	}
	defer rows.Close()

	var executions []CQExecution
	for rows.Next() {
		var ex CQExecution
		if err := rows.Scan(&ex.ID, &ex.QueryID, &ex.ExecutionID, &ex.ExecutionTime, &ex.Status, &ex.StartTime, &ex.EndTime, &ex.RecordsRead, &ex.RecordsWritten, &ex.ExecutionDurationSeconds, &ex.ErrorMessage); err != nil {
			continue
		}
		executions = append(executions, ex)
	}

	return c.JSON(fiber.Map{
		"query_id":   queryID,
		"executions": executions,
	})
}

// getQuery retrieves a single query by ID
func (h *ContinuousQueryHandler) getQuery(queryID int64) (*ContinuousQuery, error) {
	row := h.sqliteDB.QueryRow(`
		SELECT
			cq.id, cq.name, cq.description, cq.database, cq.source_measurement, cq.destination_measurement,
			cq.query, cq.interval, cq.tag_columns, cq.retention_days, cq.delete_source_after_days, cq.is_active,
			cq.last_processed_time, cq.created_at, cq.updated_at,
			cqe.execution_time, cqe.status, cqe.records_written
		FROM continuous_queries cq
		LEFT JOIN (
			SELECT query_id, execution_time, status, records_written
			FROM continuous_query_executions
			WHERE id IN (SELECT MAX(id) FROM continuous_query_executions GROUP BY query_id)
		) cqe ON cq.id = cqe.query_id
		WHERE cq.id = ?
	`, queryID)

	var q ContinuousQuery
	var tagColumnsRaw *string
	err := row.Scan(
		&q.ID, &q.Name, &q.Description, &q.Database, &q.SourceMeasurement, &q.DestinationMeasurement,
		&q.Query, &q.Interval, &tagColumnsRaw, &q.RetentionDays, &q.DeleteSourceAfterDays, &q.IsActive,
		&q.LastProcessedTime, &q.CreatedAt, &q.UpdatedAt,
		&q.LastExecutionTime, &q.LastExecutionStatus, &q.LastRecordsWritten,
	)
	if err != nil {
		return nil, err
	}
	tags, decErr := decodeTagColumns(tagColumnsRaw)
	if decErr != nil {
		h.logger.Warn().Err(decErr).Str("query_name", q.Name).
			Msg("Ignoring corrupt tag_columns; CQ will not dedup until it is re-saved (#521)")
	}
	q.TagColumns = tags

	return &q, nil
}

// getQueries retrieves all queries with optional filters
func (h *ContinuousQueryHandler) getQueries(database, isActiveStr string) ([]ContinuousQuery, error) {
	query := `
		SELECT
			cq.id, cq.name, cq.description, cq.database, cq.source_measurement, cq.destination_measurement,
			cq.query, cq.interval, cq.tag_columns, cq.retention_days, cq.delete_source_after_days, cq.is_active,
			cq.last_processed_time, cq.created_at, cq.updated_at,
			cqe.execution_time, cqe.status, cqe.records_written
		FROM continuous_queries cq
		LEFT JOIN (
			SELECT query_id, execution_time, status, records_written
			FROM continuous_query_executions
			WHERE id IN (SELECT MAX(id) FROM continuous_query_executions GROUP BY query_id)
		) cqe ON cq.id = cqe.query_id
	`

	var whereClauses []string
	var args []interface{}

	if database != "" {
		whereClauses = append(whereClauses, "cq.database = ?")
		args = append(args, database)
	}

	if isActiveStr != "" {
		isActive := isActiveStr == "true"
		whereClauses = append(whereClauses, "cq.is_active = ?")
		args = append(args, isActive)
	}

	if len(whereClauses) > 0 {
		query += " WHERE " + strings.Join(whereClauses, " AND ")
	}

	query += " ORDER BY cq.created_at DESC"

	rows, err := h.sqliteDB.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var queries []ContinuousQuery
	for rows.Next() {
		var q ContinuousQuery
		var tagColumnsRaw *string
		if err := rows.Scan(
			&q.ID, &q.Name, &q.Description, &q.Database, &q.SourceMeasurement, &q.DestinationMeasurement,
			&q.Query, &q.Interval, &tagColumnsRaw, &q.RetentionDays, &q.DeleteSourceAfterDays, &q.IsActive,
			&q.LastProcessedTime, &q.CreatedAt, &q.UpdatedAt,
			&q.LastExecutionTime, &q.LastExecutionStatus, &q.LastRecordsWritten,
		); err != nil {
			continue
		}
		tags, decErr := decodeTagColumns(tagColumnsRaw)
		if decErr != nil {
			h.logger.Warn().Err(decErr).Str("query_name", q.Name).
				Msg("Ignoring corrupt tag_columns; CQ will not dedup until it is re-saved (#521)")
		}
		q.TagColumns = tags
		queries = append(queries, q)
	}

	return queries, nil
}

// recordExecutionAndUpdateTime atomically records a successful execution and updates
// the last processed time in a single SQLite transaction. This prevents partial state
// where execution is recorded but last_processed_time is stale (causing time window overlap).
func (h *ContinuousQueryHandler) recordExecutionAndUpdateTime(queryID int64, executionID string, startTime, endTime time.Time, recordsWritten int64, duration float64) error {
	tx, err := h.sqliteDB.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	_, err = tx.Exec(`
		INSERT INTO continuous_query_executions
		(query_id, execution_id, status, start_time, end_time, records_read, records_written, execution_duration_seconds, error_message)
		VALUES (?, ?, 'completed', ?, ?, NULL, ?, ?, NULL)
	`, queryID, executionID, startTime.Format(time.RFC3339), endTime.Format(time.RFC3339), recordsWritten, duration)
	if err != nil {
		return fmt.Errorf("failed to record execution: %w", err)
	}

	_, err = tx.Exec(`
		UPDATE continuous_queries SET last_processed_time = ? WHERE id = ?
	`, endTime.Format(time.RFC3339), queryID)
	if err != nil {
		return fmt.Errorf("failed to update last processed time: %w", err)
	}

	return tx.Commit()
}

// recordExecution records an execution in the database
func (h *ContinuousQueryHandler) recordExecution(queryID int64, executionID, status string, startTime, endTime time.Time, recordsRead *int64, recordsWritten int64, duration float64, errorMessage string) {
	var errMsg *string
	if errorMessage != "" {
		errMsg = &errorMessage
	}

	_, err := h.sqliteDB.Exec(`
		INSERT INTO continuous_query_executions
		(query_id, execution_id, status, start_time, end_time, records_read, records_written, execution_duration_seconds, error_message)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, queryID, executionID, status, startTime.Format(time.RFC3339), endTime.Format(time.RFC3339), recordsRead, recordsWritten, duration, errMsg)

	if err != nil {
		h.logger.Error().Err(err).Msg("Failed to record execution")
	}
}

// encodeTagColumns serializes the tag column list for storage as a SQLite TEXT
// column. An empty/nil list encodes to NULL (stored as a nil *string) so the
// column round-trips cleanly and pre-migration rows read back as no tags.
func encodeTagColumns(tags []string) (*string, error) {
	if len(tags) == 0 {
		return nil, nil
	}
	b, err := json.Marshal(tags)
	if err != nil {
		return nil, err
	}
	s := string(b)
	return &s, nil
}

// decodeTagColumns parses the JSON tag column list read from SQLite. A NULL or
// empty value (pre-migration rows, or CQs created without tags) decodes to nil
// with no error. A non-empty value that fails to parse (corruption, a bad manual
// edit) returns an error so the caller can warn — silently returning nil would
// revert a CQ to no-dedup behavior with no signal, letting duplicates accumulate.
func decodeTagColumns(raw *string) ([]string, error) {
	if raw == nil || *raw == "" {
		return nil, nil
	}
	var tags []string
	if err := json.Unmarshal([]byte(*raw), &tags); err != nil {
		return nil, fmt.Errorf("corrupt tag_columns value %q: %w", *raw, err)
	}
	return tags, nil
}

// maxUniquenessCheckRows caps how many rows timeColumnIsUnique will scan before
// giving up and reporting "not unique" (i.e. not dedup-safe). A tagless CQ that
// legitimately produces one row per window is tiny (one row per interval), so a
// large output almost certainly means a grouping CQ that forgot to declare
// tag_columns — exactly the case where time-only dedup would be unsafe. Bounding
// the scan avoids building a huge map for that misconfiguration, and failing
// closed (treat as not-unique → skip the marker) is the safe direction.
const maxUniquenessCheckRows = 100_000

// timeColumnIsUnique reports whether the time column has no duplicate values —
// i.e. time alone is a unique key for the output. Used to decide whether a
// tagless CQ is safe to mark for time-only dedup (#521): if two rows share a
// timestamp with no tag to distinguish them, time-only dedup would lose one, so
// the marker must be withheld. An empty/absent column is trivially unique.
//
// Fails closed: an output larger than maxUniquenessCheckRows returns false
// (not dedup-safe) rather than building an unbounded map — a tagless CQ that big
// is almost certainly a mis-declared grouping CQ, which must NOT be marked.
func timeColumnIsUnique(times []interface{}) bool {
	if len(times) <= 1 {
		return true
	}
	if len(times) > maxUniquenessCheckRows {
		return false
	}
	seen := make(map[interface{}]struct{}, len(times))
	for _, t := range times {
		// Comparable key: the time column is normalized to int64 micros before
		// this runs, but be defensive about the underlying type. interface{}
		// values are comparable as map keys when the dynamic type is comparable
		// (all our time representations — int64, string, time.Time — are).
		if _, dup := seen[t]; dup {
			return false
		}
		seen[t] = struct{}{}
	}
	return true
}

// validateTagColumns rejects tag column names that aren't safe SQL identifiers.
// The names are emitted verbatim into the Parquet arc:tags metadata and later
// interpolated into compaction's `PARTITION BY "<name>"` dedup query, so an
// unsafe name must never reach storage — validate at the API boundary. Rules
// match internal/compaction/dedup.go#isValidIdentifier (alphanumeric, '_', '-').
func validateTagColumns(tags []string) error {
	for _, name := range tags {
		if name == "" {
			return fmt.Errorf("tag column name must not be empty")
		}
		// "time" is never a valid tag: compaction always appends "time" to the
		// dedup key, so declaring it would produce PARTITION BY "time", "time".
		if strings.EqualFold(name, "time") {
			return fmt.Errorf("tag column %q is reserved: time is always part of the dedup key and must not be listed", name)
		}
		for _, c := range name {
			if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' || c == '-') {
				return fmt.Errorf("invalid tag column %q: only alphanumeric, underscore, and hyphen are allowed", name)
			}
		}
	}
	return nil
}

// updateLastProcessedTime updates the last processed time for a query
func (h *ContinuousQueryHandler) updateLastProcessedTime(queryID int64, processedTime time.Time) {
	_, err := h.sqliteDB.Exec(`
		UPDATE continuous_queries SET last_processed_time = ? WHERE id = ?
	`, processedTime.Format(time.RFC3339), queryID)

	if err != nil {
		h.logger.Error().Err(err).Msg("Failed to update last processed time")
	}
}
