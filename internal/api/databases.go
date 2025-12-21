package api

import (
	"context"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// DatabasesHandler handles database management API endpoints
type DatabasesHandler struct {
	storage      storage.Backend
	deleteConfig *config.DeleteConfig
	logger       zerolog.Logger
}

// CreateDatabaseRequest represents a request to create a new database
type CreateDatabaseRequest struct {
	Name string `json:"name"`
}

// DatabaseInfo represents information about a database
type DatabaseInfo struct {
	Name             string `json:"name"`
	MeasurementCount int    `json:"measurement_count"`
	CreatedAt        string `json:"created_at,omitempty"`
}

// DatabaseListResponse represents the response for listing databases
type DatabaseListResponse struct {
	Databases []DatabaseInfo `json:"databases"`
	Count     int            `json:"count"`
}

// DatabaseMeasurement represents a measurement within a database
type DatabaseMeasurement struct {
	Name      string `json:"name"`
	FileCount int    `json:"file_count,omitempty"`
}

// MeasurementListResponse represents the response for listing measurements
type MeasurementListResponse struct {
	Database     string                `json:"database"`
	Measurements []DatabaseMeasurement `json:"measurements"`
	Count        int                   `json:"count"`
}

// Database name validation
var validDatabaseName = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9_-]*$`)

// Reserved database names that cannot be created
var reservedDatabaseNames = map[string]bool{
	"system":    true,
	"internal":  true,
	"_internal": true,
}

// NewDatabasesHandler creates a new databases handler
func NewDatabasesHandler(storage storage.Backend, deleteConfig *config.DeleteConfig, logger zerolog.Logger) *DatabasesHandler {
	return &DatabasesHandler{
		storage:      storage,
		deleteConfig: deleteConfig,
		logger:       logger.With().Str("component", "databases-handler").Logger(),
	}
}

// RegisterRoutes registers the database management routes
func (h *DatabasesHandler) RegisterRoutes(app *fiber.App) {
	app.Get("/api/v1/databases", h.handleList)
	app.Post("/api/v1/databases", h.handleCreate)
	app.Get("/api/v1/databases/:name", h.handleGet)
	app.Get("/api/v1/databases/:name/measurements", h.handleListMeasurements)
	app.Delete("/api/v1/databases/:name", h.handleDelete)
}

// handleList handles GET /api/v1/databases
func (h *DatabasesHandler) handleList(c *fiber.Ctx) error {
	ctx := context.Background()

	databases, err := h.listDatabases(ctx)
	if err != nil {
		h.logger.Error().Err(err).Msg("Failed to list databases")
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "Failed to list databases: " + err.Error(),
		})
	}

	// Get measurement counts for each database
	databaseInfos := make([]DatabaseInfo, 0, len(databases))
	for _, db := range databases {
		measurementCount := 0
		measurements, err := h.listMeasurements(ctx, db)
		if err == nil {
			measurementCount = len(measurements)
		}

		databaseInfos = append(databaseInfos, DatabaseInfo{
			Name:             db,
			MeasurementCount: measurementCount,
		})
	}

	h.logger.Info().Int("count", len(databaseInfos)).Msg("Listed databases")

	return c.JSON(DatabaseListResponse{
		Databases: databaseInfos,
		Count:     len(databaseInfos),
	})
}

// handleCreate handles POST /api/v1/databases
func (h *DatabasesHandler) handleCreate(c *fiber.Ctx) error {
	var req CreateDatabaseRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "Invalid request body: " + err.Error(),
		})
	}

	// Validate database name
	if req.Name == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "Database name is required",
		})
	}

	if !isValidDatabaseName(req.Name) {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "Invalid database name: must start with a letter and contain only alphanumeric characters, underscores, or hyphens (max 64 characters)",
		})
	}

	if reservedDatabaseNames[strings.ToLower(req.Name)] {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "Database name '" + req.Name + "' is reserved",
		})
	}

	ctx := context.Background()

	// Check if database already exists
	exists, err := h.databaseExists(ctx, req.Name)
	if err != nil {
		h.logger.Error().Err(err).Str("database", req.Name).Msg("Failed to check if database exists")
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "Failed to check if database exists",
		})
	}

	if exists {
		return c.Status(fiber.StatusConflict).JSON(fiber.Map{
			"error": "Database '" + req.Name + "' already exists",
		})
	}

	// Create database by writing a marker file
	// This works for all storage backends (local, S3, Azure)
	markerPath := req.Name + "/.arc-database"
	markerContent := []byte(`{"created_at":"` + time.Now().UTC().Format(time.RFC3339) + `"}`)

	if err := h.storage.Write(ctx, markerPath, markerContent); err != nil {
		h.logger.Error().Err(err).Str("database", req.Name).Msg("Failed to create database")
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "Failed to create database: " + err.Error(),
		})
	}

	h.logger.Info().Str("database", req.Name).Msg("Created database")

	return c.Status(fiber.StatusCreated).JSON(DatabaseInfo{
		Name:             req.Name,
		MeasurementCount: 0,
		CreatedAt:        time.Now().UTC().Format(time.RFC3339),
	})
}

// handleGet handles GET /api/v1/databases/:name
func (h *DatabasesHandler) handleGet(c *fiber.Ctx) error {
	name := c.Params("name")
	if name == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "Database name is required",
		})
	}

	ctx := context.Background()

	// Check if database exists
	exists, err := h.databaseExists(ctx, name)
	if err != nil {
		h.logger.Error().Err(err).Str("database", name).Msg("Failed to check if database exists")
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "Failed to check database",
		})
	}

	if !exists {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
			"error": "Database '" + name + "' not found",
		})
	}

	// Get measurement count
	measurements, err := h.listMeasurements(ctx, name)
	measurementCount := 0
	if err == nil {
		measurementCount = len(measurements)
	}

	return c.JSON(DatabaseInfo{
		Name:             name,
		MeasurementCount: measurementCount,
	})
}

// handleListMeasurements handles GET /api/v1/databases/:name/measurements
func (h *DatabasesHandler) handleListMeasurements(c *fiber.Ctx) error {
	name := c.Params("name")
	if name == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "Database name is required",
		})
	}

	ctx := context.Background()

	// Check if database exists
	exists, err := h.databaseExists(ctx, name)
	if err != nil {
		h.logger.Error().Err(err).Str("database", name).Msg("Failed to check if database exists")
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "Failed to check database",
		})
	}

	if !exists {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
			"error": "Database '" + name + "' not found",
		})
	}

	// List measurements
	measurements, err := h.listMeasurements(ctx, name)
	if err != nil {
		h.logger.Error().Err(err).Str("database", name).Msg("Failed to list measurements")
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "Failed to list measurements: " + err.Error(),
		})
	}

	// Build response
	measurementInfos := make([]DatabaseMeasurement, 0, len(measurements))
	for _, m := range measurements {
		measurementInfos = append(measurementInfos, DatabaseMeasurement{
			Name: m,
		})
	}

	h.logger.Info().
		Str("database", name).
		Int("count", len(measurementInfos)).
		Msg("Listed measurements")

	return c.JSON(MeasurementListResponse{
		Database:     name,
		Measurements: measurementInfos,
		Count:        len(measurementInfos),
	})
}

// handleDelete handles DELETE /api/v1/databases/:name
func (h *DatabasesHandler) handleDelete(c *fiber.Ctx) error {
	// Check if delete is enabled
	if h.deleteConfig == nil || !h.deleteConfig.Enabled {
		return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
			"error": "Delete operations are disabled. Set delete.enabled=true in arc.toml to enable.",
		})
	}

	name := c.Params("name")
	if name == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "Database name is required",
		})
	}

	// Require confirmation
	confirm := c.Query("confirm")
	if confirm != "true" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "Confirmation required. Add ?confirm=true to delete the database.",
		})
	}

	// Prevent deletion of reserved names
	if reservedDatabaseNames[strings.ToLower(name)] {
		return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
			"error": "Cannot delete reserved database '" + name + "'",
		})
	}

	ctx := context.Background()

	// Check if database exists
	exists, err := h.databaseExists(ctx, name)
	if err != nil {
		h.logger.Error().Err(err).Str("database", name).Msg("Failed to check if database exists")
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "Failed to check database",
		})
	}

	if !exists {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
			"error": "Database '" + name + "' not found",
		})
	}

	// List all files in the database
	files, err := h.storage.List(ctx, name+"/")
	if err != nil {
		h.logger.Error().Err(err).Str("database", name).Msg("Failed to list database files")
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "Failed to list database files",
		})
	}

	// Delete all files
	deletedCount := 0
	var deleteErrors []string

	// Try batch delete if available
	if batchDeleter, ok := h.storage.(storage.BatchDeleter); ok && len(files) > 0 {
		if err := batchDeleter.DeleteBatch(ctx, files); err != nil {
			h.logger.Error().Err(err).Str("database", name).Msg("Batch delete failed")
			deleteErrors = append(deleteErrors, err.Error())
		} else {
			deletedCount = len(files)
		}
	} else {
		// Fall back to individual deletes
		for _, file := range files {
			if err := h.storage.Delete(ctx, file); err != nil {
				h.logger.Warn().Err(err).Str("file", file).Msg("Failed to delete file")
				deleteErrors = append(deleteErrors, file+": "+err.Error())
			} else {
				deletedCount++
			}
		}
	}

	// Also delete the .arc-database marker file (not included in List due to hidden file filter)
	markerPath := name + "/.arc-database"
	if err := h.storage.Delete(ctx, markerPath); err == nil {
		deletedCount++
		h.logger.Debug().Str("path", markerPath).Msg("Deleted database marker file")
	}

	// Try to remove the empty database directory
	// This is a best-effort operation - for local storage, we try to remove the directory
	// For S3/Azure, directories don't exist as actual objects, so this is a no-op
	if dirRemover, ok := h.storage.(storage.DirectoryRemover); ok {
		if err := dirRemover.RemoveDirectory(ctx, name); err != nil {
			h.logger.Debug().Err(err).Str("database", name).Msg("Could not remove database directory (may not be empty)")
		}
	}

	if len(deleteErrors) > 0 {
		h.logger.Error().
			Str("database", name).
			Int("deleted", deletedCount).
			Int("errors", len(deleteErrors)).
			Msg("Partial database deletion")

		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error":         "Partial deletion - some files could not be deleted",
			"deleted_count": deletedCount,
			"errors":        deleteErrors,
		})
	}

	h.logger.Info().
		Str("database", name).
		Int("files_deleted", deletedCount).
		Msg("Deleted database")

	return c.JSON(fiber.Map{
		"message":       "Database '" + name + "' deleted successfully",
		"files_deleted": deletedCount,
	})
}

// Helper functions

func isValidDatabaseName(name string) bool {
	if len(name) == 0 || len(name) > 64 {
		return false
	}
	return validDatabaseName.MatchString(name)
}

func (h *DatabasesHandler) listDatabases(ctx context.Context) ([]string, error) {
	var databases []string

	// Use DirectoryLister if available
	if lister, ok := h.storage.(storage.DirectoryLister); ok {
		dirs, err := lister.ListDirectories(ctx, "")
		if err != nil {
			return nil, err
		}
		databases = dirs
	} else {
		// Fall back to List and extract unique top-level directories
		files, err := h.storage.List(ctx, "")
		if err != nil {
			return nil, err
		}
		databases = extractTopLevelDirs(files)
	}

	// Filter out hidden directories and sort
	filtered := make([]string, 0, len(databases))
	for _, db := range databases {
		if !strings.HasPrefix(db, ".") && !strings.HasPrefix(db, "_") {
			filtered = append(filtered, db)
		}
	}
	sort.Strings(filtered)

	return filtered, nil
}

func (h *DatabasesHandler) listMeasurements(ctx context.Context, database string) ([]string, error) {
	var measurements []string

	// Use DirectoryLister if available
	if lister, ok := h.storage.(storage.DirectoryLister); ok {
		dirs, err := lister.ListDirectories(ctx, database+"/")
		if err != nil {
			return nil, err
		}
		measurements = dirs
	} else {
		// Fall back to List and extract unique subdirectories
		files, err := h.storage.List(ctx, database+"/")
		if err != nil {
			return nil, err
		}
		measurements = extractSubdirectories(files, database)
	}

	// Filter out hidden directories and sort
	filtered := make([]string, 0, len(measurements))
	for _, m := range measurements {
		if !strings.HasPrefix(m, ".") && !strings.HasPrefix(m, "_") {
			filtered = append(filtered, m)
		}
	}
	sort.Strings(filtered)

	return filtered, nil
}

func (h *DatabasesHandler) databaseExists(ctx context.Context, name string) (bool, error) {
	databases, err := h.listDatabases(ctx)
	if err != nil {
		return false, err
	}

	for _, db := range databases {
		if db == name {
			return true, nil
		}
	}
	return false, nil
}

func extractTopLevelDirs(files []string) []string {
	dirSet := make(map[string]bool)
	for _, file := range files {
		parts := strings.SplitN(file, "/", 2)
		if len(parts) > 0 && parts[0] != "" {
			dirSet[parts[0]] = true
		}
	}

	dirs := make([]string, 0, len(dirSet))
	for dir := range dirSet {
		dirs = append(dirs, dir)
	}
	return dirs
}

func extractSubdirectories(files []string, prefix string) []string {
	dirSet := make(map[string]bool)
	prefixWithSlash := prefix + "/"

	for _, file := range files {
		if strings.HasPrefix(file, prefixWithSlash) {
			remainder := strings.TrimPrefix(file, prefixWithSlash)
			parts := strings.SplitN(remainder, "/", 2)
			if len(parts) > 0 && parts[0] != "" {
				dirSet[parts[0]] = true
			}
		}
	}

	dirs := make([]string, 0, len(dirSet))
	for dir := range dirSet {
		dirs = append(dirs, dir)
	}
	return dirs
}
