package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/basekick-labs/arc/internal/auth"
	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/database"
	"github.com/basekick-labs/arc/internal/license"
	sqlutil "github.com/basekick-labs/arc/internal/sql"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/basekick-labs/arc/internal/tiering"
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
	// onFilesRemoved invalidates the query caches (and the cluster's) after
	// files were removed or rewritten: see SetOnFilesRemoved.
	onFilesRemoved func()
	storage        storage.Backend
	config         *config.RetentionConfig
	db             *sql.DB // SQLite for policy metadata
	// ownsDB records whether this handler opened db itself. When retention
	// shares the auth database (the default), the handle is borrowed and Close
	// must leave it to its owner.
	ownsDB      bool
	duckdb      *database.DuckDB     // Shared DuckDB for parquet queries
	coordinator RetentionCoordinator // nil in standalone mode
	coldTier    coldRetentionTier    // nil without tiering: retention is hot-only
	// coldDeleteHook replaces the cold object delete in tests; nil in production.
	coldDeleteHook func(ctx context.Context, backend storage.Backend, path string) error
	licenseClient  *license.Client // nil when auth/licensing is disabled
	authManager    *auth.AuthManager
	logger         zerolog.Logger

	// Cluster-wide retention (#1324), see retention_cluster.go. cluster is
	// the cluster state once wired; localMu serializes this node's own
	// writes with the cluster's read of them; mirrorMu serializes the mirror.
	clusterMu sync.RWMutex
	cluster   RetentionClusterSource
	localMu   sync.Mutex
	mirrorMu  sync.Mutex
	// clusterOwned: this node's table has been the cluster's mirror (the
	// retention_cluster_state marker), so its own rows are never run again.
	clusterOwned atomic.Bool
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

// RetentionPolicyRequest represents a request to create/update a policy.
//
// Every field distinguishes an absent key from a null one (#1023). Create
// requires name, database and retention_days; buffer_days and is_active keep
// their long-standing create defaults of 0 and false when absent. Update is a
// partial update: an absent key keeps the stored value and a present one is
// validated and written. Only measurement can be null (it means the whole
// database, as an empty string always has).
//
// Before #1023 update wrote every column from the body, so a PUT that left out
// measurement widened a measurement-pinned policy to delete across the whole
// database, one that left out buffer_days moved the cutoff in by the stored
// buffer, and one that left out is_active stopped the policy — all with a 200.
type RetentionPolicyRequest struct {
	Name          optionalField[string] `json:"name"`
	Database      optionalField[string] `json:"database"`
	Measurement   optionalField[string] `json:"measurement"`
	RetentionDays optionalField[int]    `json:"retention_days"`
	BufferDays    optionalField[int]    `json:"buffer_days"`
	IsActive      optionalField[bool]   `json:"is_active"`
}

// validateRetentionPolicyRequest applies the per-field rules to the fields the
// body carries, and on create requires name, database and retention_days. The
// one rule that spans two fields, retention_days > buffer_days, is checked here
// on create (an absent buffer_days is 0) and, on update, only when the body
// carries both; otherwise handleUpdate enforces it against the stored values in
// the UPDATE itself.
func validateRetentionPolicyRequest(req *RetentionPolicyRequest, create bool) error {
	present := func(name string, set, null bool) (bool, error) {
		switch {
		case !set:
			if create {
				return false, fmt.Errorf("%s is required", name)
			}
			return false, nil
		case null:
			return false, fmt.Errorf("%s may not be null", name)
		}
		return true, nil
	}

	if ok, err := present("name", req.Name.Set, req.Name.Null); err != nil {
		return err
	} else if ok && req.Name.Value == "" {
		return errors.New("name is required")
	}
	// Both names are concatenated into a storage prefix by
	// getMeasurementsToProcess and deleteOldFiles, and the row is replayed by
	// the scheduler forever, so an unvalidated one is a policy that fails on
	// every run long after the request that created it (#741).
	if ok, err := present("database", req.Database.Set, req.Database.Null); err != nil {
		return err
	} else if ok {
		if req.Database.Value == "" {
			return errors.New("database is required")
		}
		if !isSafeStoragePathSegment(req.Database.Value) {
			return fmt.Errorf("invalid database name %q: may not be empty, contain a separator, or start with a dot", req.Database.Value)
		}
		// Arc's own state (_schema anchors, _compaction_state), not a database (#1012).
		if storage.IsReservedRootDir(req.Database.Value) {
			return fmt.Errorf("database name %q is reserved for Arc's own storage", req.Database.Value)
		}
	}
	if req.Measurement.hasValue() && req.Measurement.Value != "" && !isSafeStoragePathSegment(req.Measurement.Value) {
		return fmt.Errorf("invalid measurement name %q: may not contain a separator or start with a dot", req.Measurement.Value)
	}
	if ok, err := present("retention_days", req.RetentionDays.Set, req.RetentionDays.Null); err != nil {
		return err
	} else if ok && req.RetentionDays.Value <= 0 {
		return errors.New("retention_days must be greater than 0")
	}
	if req.BufferDays.Null {
		return errors.New("buffer_days may not be null")
	}
	// A negative buffer moves the cutoff forward, so the policy deletes data
	// younger than retention_days. Create accepted one before #1023.
	if req.BufferDays.Set && req.BufferDays.Value < 0 {
		return errors.New("buffer_days may not be negative")
	}
	if req.IsActive.Null {
		return errors.New("is_active may not be null")
	}
	if req.RetentionDays.hasValue() && req.RetentionDays.Value <= req.BufferDays.Value {
		// On create an absent buffer_days is 0; on update this compares only
		// when both are carried, and the UPDATE checks the stored values.
		if create || req.BufferDays.Set {
			return errors.New("retention_days must be greater than buffer_days")
		}
	}
	return nil
}

// measurementColumn is the stored form of a measurement field: an empty string
// and null both mean the whole database, and are both stored as NULL so GET
// returns one shape for them.
func measurementColumn(f optionalField[string]) *string {
	if !f.hasValue() || f.Value == "" {
		return nil
	}
	return f.ptr()
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
	SkippedFiles  int    `json:"skipped_files,omitempty"`
	SkippedReason string `json:"skipped_reason,omitempty"`
	// Cold tier (#168). DeletedCount and FilesDeleted include these. In a dry
	// run they are what a real run would delete. ColdFilesSkipped counts cold
	// files retention could not examine or delete; ColdSkippedReason says why.
	ColdFilesDeleted     int      `json:"cold_files_deleted,omitempty"`
	ColdRowsDeleted      int64    `json:"cold_rows_deleted,omitempty"`
	ColdFilesSkipped     int      `json:"cold_files_skipped,omitempty"`
	ColdSkippedReason    string   `json:"cold_skipped_reason,omitempty"`
	ExecutionTimeMs      float64  `json:"execution_time_ms"`
	DryRun               bool     `json:"dry_run"`
	CutoffDate           string   `json:"cutoff_date"`
	AffectedMeasurements []string `json:"affected_measurements"`
}

const retentionSkippedReason = "unaddressable storage files cannot be read or deleted"
const retentionUnusableSampleCap = 32

type unusableSample struct {
	key string
	err error
}

type unusableByMeasurement struct {
	counts       map[string]int
	samples      map[string][]unusableSample
	orphans      int
	orphanSample []unusableSample
}

type retentionDiscovery struct {
	measurements []string
	unusable     unusableByMeasurement
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

		// Open SQLite database for policy metadata, with the auth database's
		// settings: the execution history's foreign key is enforced, and a
		// writer waits for the lock rather than failing at once (the cluster
		// mirror writes this database in the background, #1324). Transactions
		// take the write lock when they begin: a read-then-write transaction
		// that started deferred would fail outright, not wait, when another
		// connection wrote in between.
		opened, err := sql.Open("sqlite3", cfg.DBPath+dsnSep(cfg.DBPath)+"_journal_mode=WAL&_busy_timeout=5000&_foreign_keys=ON&_txlock=immediate")
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
		} else if storage.IsReservedRootDir(database) {
			h.logger.Warn().Str("policy", name).Str("database", database).
				Msg("Retention policy targets a storage root Arc reserves for its own state, not a database; delete it")
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

// coldRetentionTier is what retention needs from tiering to apply a policy to
// the cold tier (#168). *tiering.Manager satisfies it.
type coldRetentionTier interface {
	// ColdBackend is nil when this node has no usable cold tier.
	ColdBackend() storage.Backend
	GetColdFilesForRetention(ctx context.Context, database string, cutoff time.Time) ([]tiering.FileMetadata, error)
	RetireColdRows(ctx context.Context, paths []string) (int, error)
	RevertColdRowToHot(ctx context.Context, path string) error
	NotifyColdFilesRemoved(n int)
}

// SetTieringManager makes retention apply its policies to the cold tier too.
func (h *RetentionHandler) SetTieringManager(m *tiering.Manager) {
	if m == nil {
		h.coldTier = nil
		return
	}
	h.coldTier = m
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

	// Dormant rows (#1324): this node's own policies the cluster did not
	// adopt, and their history. Kept, never run.
	_, err = h.db.Exec(`
		CREATE TABLE IF NOT EXISTS retention_policies_dormant (
			dormant_id INTEGER PRIMARY KEY AUTOINCREMENT,
			policy_id INTEGER NOT NULL,
			name TEXT NOT NULL,
			database TEXT NOT NULL,
			measurement TEXT,
			retention_days INTEGER NOT NULL,
			buffer_days INTEGER,
			is_active BOOLEAN,
			created_at TIMESTAMP,
			updated_at TIMESTAMP,
			dormant_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to create retention_policies_dormant table: %w", err)
	}
	_, err = h.db.Exec(`
		CREATE TABLE IF NOT EXISTS retention_executions_dormant (
			dormant_id INTEGER PRIMARY KEY AUTOINCREMENT,
			execution_id INTEGER NOT NULL,
			policy_id INTEGER NOT NULL,
			execution_time TIMESTAMP,
			status TEXT,
			deleted_count INTEGER,
			cutoff_date TIMESTAMP,
			execution_duration_ms FLOAT,
			error_message TEXT
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to create retention_executions_dormant table: %w", err)
	}
	// The cluster mirror's own bookkeeping (#1324): the IDs it holds for the
	// cluster, and the marker that this table is the cluster's.
	if _, err = h.db.Exec(`CREATE TABLE IF NOT EXISTS retention_cluster_ids (id INTEGER PRIMARY KEY)`); err != nil {
		return fmt.Errorf("failed to create retention_cluster_ids table: %w", err)
	}
	if _, err = h.db.Exec(`CREATE TABLE IF NOT EXISTS retention_cluster_state (name TEXT PRIMARY KEY, value TEXT)`); err != nil {
		return fmt.Errorf("failed to create retention_cluster_state table: %w", err)
	}
	var owned int
	if err := h.db.QueryRow(`SELECT COUNT(*) FROM retention_cluster_state WHERE name = 'owned'`).Scan(&owned); err != nil {
		return fmt.Errorf("failed to read retention_cluster_state: %w", err)
	}
	h.clusterOwned.Store(owned > 0)

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

	// Admin-only, including the read-only routes. Retention policies are
	// configured by admins only, and a policy row names the database and
	// measurement it applies to — so listing them on any authenticated token
	// was a tenant-name enumeration surface, the same one closed on the
	// database listing endpoints.
	adminOnly := withAdminAuth(h.authManager)
	group.Get("/", adminOnly, h.handleList)
	group.Get("/:id", adminOnly, h.handleGet)
	group.Get("/:id/executions", adminOnly, h.handleGetExecutions)

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
	if err := validateRetentionPolicyRequest(&req, true); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
	}

	// Insert policy. An absent buffer_days or is_active keeps the create
	// defaults this endpoint has always applied (0 and false) rather than the
	// column defaults: flipping is_active to the schema's TRUE would make a
	// policy created by an existing client start deleting data.
	var result sql.Result
	adopted, err := h.localWrite(func() (err error) {
		result, err = h.db.Exec(`
		INSERT INTO retention_policies (name, database, measurement, retention_days, buffer_days, is_active)
		VALUES (?, ?, ?, ?, ?, ?)
	`, req.Name.Value, req.Database.Value, measurementColumn(req.Measurement), req.RetentionDays.Value, req.BufferDays.Value, req.IsActive.Value)
		return err
	})
	if adopted != nil {
		return h.clusterCreate(c, adopted, &req)
	}
	if errors.Is(err, errRetentionBeingAdopted) || errors.Is(err, errRetentionNotCaughtUp) {
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{"error": err.Error()})
	}

	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE constraint") {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": fmt.Sprintf("Retention policy with name '%s' already exists", req.Name.Value),
			})
		}
		h.logger.Error().Err(err).Msg("Failed to create retention policy")
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "Failed to create retention policy",
		})
	}

	policyID, _ := result.LastInsertId()
	h.logger.Info().Int64("policy_id", policyID).Str("name", req.Name.Value).Msg("Created retention policy")

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
	if errors.Is(err, errRetentionNotCaughtUp) {
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{"error": err.Error()})
	}
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
	if errors.Is(err, errRetentionNotCaughtUp) {
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{"error": err.Error()})
	}
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

	// A partial update (#1023): each field the body carries is validated with
	// the create rules (an update that writes a name verbatim could otherwise
	// store exactly what create refuses, #741), and an absent one keeps its
	// stored value.
	if err := validateRetentionPolicyRequest(&req, false); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
	}

	set := []updateColumn{
		{"name", req.Name.Set, req.Name.Value},
		{"database", req.Database.Set, req.Database.Value},
		{"measurement", req.Measurement.Set, measurementColumn(req.Measurement)},
		{"retention_days", req.RetentionDays.Set, req.RetentionDays.Value},
		{"buffer_days", req.BufferDays.Set, req.BufferDays.Value},
		{"is_active", req.IsActive.Set, req.IsActive.Value},
	}
	assignments, args := buildUpdateSet(set)
	if len(args) == 0 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "no fields to update"})
	}
	var (
		result sql.Result
		rows   int64
		exists int
		lookup error
	)

	// retention_days > buffer_days spans two columns, and a body may carry
	// either one alone, so it is checked against the values the row will hold
	// — the body's where it carries one, the stored one where it does not — in
	// the same statement that writes them, so no concurrent PUT can slip in
	// between a read and the write. A stored NULL buffer reads as 0.
	adopted, err := h.localWrite(func() (err error) {
		result, err = h.db.Exec(
			"UPDATE retention_policies SET "+assignments+", updated_at = CURRENT_TIMESTAMP"+
				" WHERE id = ? AND COALESCE(?, retention_days) > COALESCE(?, buffer_days, 0)",
			append(args, policyID, req.RetentionDays.ptr(), req.BufferDays.ptr())...)
		if err != nil {
			return err
		}
		if rows, _ = result.RowsAffected(); rows == 0 {
			// Either no such policy, or the row's values would break the
			// retention/buffer rule; told apart below, read under the same
			// lock as the write.
			lookup = h.db.QueryRow("SELECT 1 FROM retention_policies WHERE id = ?", policyID).Scan(&exists)
		}
		return nil
	})
	if adopted != nil {
		return h.clusterUpdate(c, adopted, int64(policyID), &req)
	}
	if errors.Is(err, errRetentionBeingAdopted) || errors.Is(err, errRetentionNotCaughtUp) {
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{"error": err.Error()})
	}

	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE constraint") {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": fmt.Sprintf("Retention policy with name %q already exists", req.Name.Value),
			})
		}
		h.logger.Error().Err(err).Msg("Failed to update retention policy")
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "Failed to update retention policy",
		})
	}

	if rows == 0 {
		switch err := lookup; {
		case errors.Is(err, sql.ErrNoRows):
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"error": "Retention policy not found"})
		case err != nil:
			h.logger.Error().Err(err).Msg("Failed to update retention policy")
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"error": "Failed to update retention policy",
			})
		}
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "retention_days must be greater than buffer_days",
		})
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

	var result sql.Result
	adopted, err := h.localWrite(func() (err error) {
		// Delete execution history first
		_, _ = h.db.Exec("DELETE FROM retention_executions WHERE policy_id = ?", policyID)
		// Delete policy
		result, err = h.db.Exec("DELETE FROM retention_policies WHERE id = ?", policyID)
		return err
	})
	if adopted != nil {
		return h.clusterDelete(c, adopted, int64(policyID))
	}
	if errors.Is(err, errRetentionBeingAdopted) || errors.Is(err, errRetentionNotCaughtUp) {
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{"error": err.Error()})
	}
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
	discovery, err := h.getMeasurementsToProcess(ctx, policy)
	if err != nil {
		return nil, fmt.Errorf("failed to discover measurements: %w", err)
	}
	// Cold-only measurements are invisible to the hot listing, so the cold
	// tier adds its own (#168).
	cold := h.planColdRetention(ctx, policy, cutoffDate)
	defer h.notifyColdRemoved(cold)
	measurements := cold.addMeasurements(discovery.measurements)

	h.logger.Info().Strs("measurements", measurements).Msg("Processing measurements")

	// Record execution start
	executionID := h.recordExecutionStart(policyID, cutoffDate)
	if err := h.policyUnchanged(policy); err != nil {
		if executionID > 0 {
			h.recordExecutionComplete(executionID, "failed", 0, float64(time.Since(start).Milliseconds()), err.Error())
		}
		return nil, fmt.Errorf("retention policy %d: %w", policyID, err)
	}

	// Execute retention for each measurement
	var totalDeleted int64
	var totalFilesDeleted int
	var totalSkipped int

	for _, measurement := range measurements {
		deleted, filesDeleted, skipped, err := h.deleteOldFiles(ctx, policy.Database, measurement, cutoffDate, false, retentionManifestReason(policyID))
		// Accumulate before error check: deleteOldFiles returns partial progress
		// on abort so the execution record reflects all completed work accurately.
		totalDeleted += deleted
		totalFilesDeleted += filesDeleted
		totalSkipped += skipped
		totalSkipped += discovery.unusable.counts[measurement]
		if err != nil {
			h.logger.Error().Err(err).Str("measurement", measurement).Msg("Failed to process measurement")
			// Abort on any error — manifest failures are non-transient (Raft quorum loss)
			// and continuing would produce orphaned manifest entries with no retry path.
			if executionID > 0 {
				h.recordExecutionComplete(executionID, "failed", totalDeleted, float64(time.Since(start).Milliseconds()), err.Error())
			}
			h.clearCachesAfterRun(false) // files of earlier measurements may be gone
			return nil, fmt.Errorf("retention aborted for policy %d: %w", policyID, err)
		}

		// Only a cancelled context stops the cold pass; everything else it
		// cannot do is counted in cold, so hot retention always runs.
		coldRows, coldFiles, coldErr := h.deleteOldColdFiles(ctx, cold, measurement, cutoffDate, false)
		totalDeleted += coldRows
		totalFilesDeleted += coldFiles
		if coldErr != nil {
			if executionID > 0 {
				h.recordExecutionComplete(executionID, "failed", totalDeleted, float64(time.Since(start).Milliseconds()), coldErr.Error())
			}
			h.clearCachesAfterRun(false)
			return nil, fmt.Errorf("retention aborted for policy %d: %w", policyID, coldErr)
		}
	}
	totalSkipped += discovery.unusable.orphans

	// Clear DuckDB parquet metadata/data cache and release memory back to OS.
	h.clearCachesAfterRun(false)
	freeOSMemoryThrottled()

	executionTime := float64(time.Since(start).Milliseconds())

	// Record execution completion. A run that could not resolve some files did
	// not do what the policy asks, so it must not be recorded as a clean
	// "completed": the whole failure mode #746 fixed was retention reporting
	// success while deleting nothing.
	coldReason := cold.skippedReason()
	cold.logSkipped(h.logger, policy.Database)
	if executionID > 0 {
		status, detail := "completed", ""
		if totalSkipped > 0 {
			status = "completed_with_errors"
			detail = fmt.Sprintf("%d %s; see the log for the keys", totalSkipped, retentionSkippedReason)
			h.logger.Error().Int("skipped_files", totalSkipped).Str("database", policy.Database).
				Strs("sample", formatUnusableSamples(discovery.unusable)).
				Msg("Retention could not resolve some files; their data will never age out")
		}
		if coldReason != "" {
			status = "completed_with_errors"
			detail = joinRetentionDetail(detail, coldReason)
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
		SkippedFiles:         totalSkipped,
		SkippedReason:        skippedReason(totalSkipped),
		ColdFilesDeleted:     cold.filesDeleted,
		ColdRowsDeleted:      cold.rowsDeleted,
		ColdFilesSkipped:     cold.skippedTotal(),
		ColdSkippedReason:    coldReason,
		ExecutionTimeMs:      executionTime,
		DryRun:               false,
		CutoffDate:           cutoffDate.Format(time.RFC3339),
		AffectedMeasurements: measurements,
	}, nil
}

// GetActivePolicies returns all active retention policies (used by scheduler)
func (h *RetentionHandler) GetActivePolicies() ([]RetentionPolicy, error) {
	if src, err := h.readSource(); err != nil {
		return nil, err
	} else if src != nil {
		return h.clusterPolicies(src, true)
	}
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
	if errors.Is(err, errRetentionNotCaughtUp) {
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{"error": err.Error()})
	}
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
	discovery, err := h.getMeasurementsToProcess(c.Context(), policy)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "Failed to discover measurements: " + err.Error(),
		})
	}
	// Cold-only measurements are invisible to the hot listing, so the cold
	// tier adds its own (#168).
	cold := h.planColdRetention(c.Context(), policy, cutoffDate)
	defer h.notifyColdRemoved(cold)
	measurements := cold.addMeasurements(discovery.measurements)

	h.logger.Info().Strs("measurements", measurements).Msg("Processing measurements")

	// Record execution start
	var executionID int64
	if !req.DryRun {
		executionID = h.recordExecutionStart(int64(policyID), cutoffDate)
		if err := h.policyUnchanged(policy); err != nil {
			if executionID > 0 {
				h.recordExecutionComplete(executionID, "failed", 0, float64(time.Since(start).Milliseconds()), err.Error())
			}
			return c.Status(fiber.StatusConflict).JSON(fiber.Map{"error": err.Error()})
		}
	}

	// Execute retention for each measurement
	var totalDeleted int64
	var totalFilesDeleted int
	var totalSkipped int

	for _, measurement := range measurements {
		deleted, filesDeleted, skipped, err := h.deleteOldFiles(c.Context(), policy.Database, measurement, cutoffDate, req.DryRun, retentionManifestReason(policyID))
		totalSkipped += skipped
		totalSkipped += discovery.unusable.counts[measurement]
		totalDeleted += deleted
		totalFilesDeleted += filesDeleted
		if err != nil {
			h.logger.Error().Err(err).Str("measurement", measurement).Msg("Failed to process measurement")
			if !req.DryRun && executionID > 0 {
				h.recordExecutionComplete(executionID, "failed", totalDeleted, float64(time.Since(start).Milliseconds()), err.Error())
			}
			h.clearCachesAfterRun(req.DryRun) // files of earlier measurements may be gone
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"error": fmt.Sprintf("retention aborted at measurement %q: %s", measurement, err.Error()),
			})
		}
		// Only a cancelled context stops the cold pass; everything else it
		// cannot do is counted in cold, so hot retention always runs.
		coldRows, coldFiles, coldErr := h.deleteOldColdFiles(c.Context(), cold, measurement, cutoffDate, req.DryRun)
		totalDeleted += coldRows
		totalFilesDeleted += coldFiles
		if coldErr != nil {
			if !req.DryRun && executionID > 0 {
				h.recordExecutionComplete(executionID, "failed", totalDeleted, float64(time.Since(start).Milliseconds()), coldErr.Error())
			}
			h.clearCachesAfterRun(req.DryRun)
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"error": fmt.Sprintf("retention aborted at measurement %q: %s", measurement, coldErr.Error()),
			})
		}
	}
	totalSkipped += discovery.unusable.orphans

	// Clear DuckDB parquet metadata/data cache — dry runs also populate the cache via
	// read_parquet calls in getFileMaxTimeAndRowCount, so always clear regardless of dry run.
	h.clearCachesAfterRun(req.DryRun)
	freeOSMemoryThrottled()

	executionTime := float64(time.Since(start).Milliseconds())

	// Record execution completion. See runPolicy: a run that could not resolve
	// some files is not a clean "completed".
	var skipDetail string
	if totalSkipped > 0 {
		skipDetail = fmt.Sprintf("%d %s; see the log for the keys", totalSkipped, retentionSkippedReason)
		h.logger.Error().Int("skipped_files", totalSkipped).Str("database", policy.Database).
			Strs("sample", formatUnusableSamples(discovery.unusable)).
			Msg("Retention could not resolve some files; their data will never age out")
	}
	coldReason := cold.skippedReason()
	cold.logSkipped(h.logger, policy.Database)
	if !req.DryRun && executionID > 0 {
		status := "completed"
		if totalSkipped > 0 || coldReason != "" {
			status = "completed_with_errors"
		}
		h.recordExecutionComplete(executionID, status, totalDeleted, executionTime, joinRetentionDetail(skipDetail, coldReason))
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
		ColdFilesDeleted:     cold.filesDeleted,
		ColdRowsDeleted:      cold.rowsDeleted,
		ColdFilesSkipped:     cold.skippedTotal(),
		ColdSkippedReason:    coldReason,
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
		var executionDurationMs sql.NullFloat64
		if err := rows.Scan(&ex.ID, &ex.PolicyID, &ex.ExecutionTime, &ex.Status, &ex.DeletedCount, &ex.CutoffDate, &executionDurationMs, &ex.ErrorMessage); err != nil {
			continue
		}
		if executionDurationMs.Valid {
			ex.ExecutionDurationMs = executionDurationMs.Float64
		}
		executions = append(executions, ex)
	}

	return c.JSON(fiber.Map{
		"policy_id":  policyID,
		"executions": executions,
	})
}

// getPolicy retrieves a single policy by ID
//
// Once the cluster holds retention policies (#1324) the policy is the
// cluster's, never a row of this node's own: every run reads it here, so a
// mirror that has not caught up cannot change what a run deletes.
func (h *RetentionHandler) getPolicy(policyID int64) (*RetentionPolicy, error) {
	if src, err := h.readSource(); err != nil {
		return nil, err
	} else if src != nil {
		return h.clusterPolicy(src, policyID)
	}
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
	if src, err := h.readSource(); err != nil {
		return nil, err
	} else if src != nil {
		return h.clusterPolicies(src, false)
	}
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
func (h *RetentionHandler) getMeasurementsToProcess(ctx context.Context, policy *RetentionPolicy) (retentionDiscovery, error) {
	prefix := policy.Database + "/"
	pinned := policy.Measurement != nil && *policy.Measurement != ""
	if pinned {
		prefix += *policy.Measurement + "/"
	}

	files, err := h.storage.List(ctx, prefix)
	if err != nil {
		return retentionDiscovery{}, fmt.Errorf("failed to list files: %w", err)
	}

	measurementSet := make(map[string]struct{})
	if pinned {
		measurementSet[*policy.Measurement] = struct{}{}
	}
	for _, f := range files {
		relPath := strings.TrimPrefix(f, prefix)
		if !pinned {
			parts := strings.SplitN(relPath, "/", 2)
			if len(parts) > 0 && parts[0] != "" && !strings.HasPrefix(parts[0], ".") {
				measurementSet[parts[0]] = struct{}{}
			}
		}
	}

	accounting := unusableByMeasurement{
		counts:  make(map[string]int),
		samples: make(map[string][]unusableSample),
	}
	if lister, ok := h.storage.(storage.UnusableLister); ok {
		unusable, err := lister.ListUnusable(ctx, prefix)
		if err != nil {
			h.logger.Warn().Err(err).Str("prefix", prefix).
				Msg("Could not list unusable files during retention measurement discovery")
		} else {
			for _, f := range unusable {
				if !strings.HasSuffix(strings.ToLower(f.Path), ".parquet") {
					continue
				}
				measurement := ""
				if pinned {
					measurement = *policy.Measurement
				}
				if !pinned {
					relPath := strings.TrimPrefix(f.Path, prefix)
					parts := strings.SplitN(relPath, "/", 2)
					if len(parts) == 0 || parts[0] == "" ||
						strings.HasPrefix(parts[0], ".") ||
						storage.ValidateKeySegment(parts[0]) != nil {
						accounting.orphans++
						if len(accounting.orphanSample) < retentionUnusableSampleCap {
							accounting.orphanSample = append(accounting.orphanSample, unusableSample{key: f.Path, err: f.Err})
						}
						continue
					}
					measurement = parts[0]
				}
				measurementSet[measurement] = struct{}{}
				accounting.counts[measurement]++
				if len(accounting.samples[measurement]) < retentionUnusableSampleCap {
					accounting.samples[measurement] = append(accounting.samples[measurement], unusableSample{key: f.Path, err: f.Err})
				}
			}
		}
	}

	var measurements []string
	for m := range measurementSet {
		measurements = append(measurements, m)
	}

	return retentionDiscovery{measurements: measurements, unusable: accounting}, nil
}

func formatUnusableSamples(accounting unusableByMeasurement) []string {
	samples := make([]string, 0, retentionUnusableSampleCap)
	appendSample := func(sample unusableSample) {
		if len(samples) == retentionUnusableSampleCap {
			return
		}
		samples = append(samples, fmt.Sprintf("%s: %v", sample.key, sample.err))
	}
	for _, measurementSamples := range accounting.samples {
		for _, sample := range measurementSamples {
			appendSample(sample)
		}
	}
	for _, sample := range accounting.orphanSample {
		appendSample(sample)
	}
	return samples
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

	var skipped int

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

func skippedReason(skipped int) string {
	if skipped == 0 {
		return ""
	}
	return retentionSkippedReason
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

	query := fmt.Sprintf("SELECT MAX(time) as max_time, COUNT(*) as cnt FROM %s", sqlutil.ReadParquet(sqlutil.QuoteStringLiteral(filePath)))
	row := db.QueryRowContext(ctx, query)

	var maxTime time.Time
	var rowCount int64
	if err := row.Scan(&maxTime, &rowCount); err != nil {
		return time.Time{}, 0, err
	}

	return maxTime.UTC(), rowCount, nil
}

// errPolicyChangedBeforeRun: the policy a run read changed (or went) before
// the run deleted anything.
var errPolicyChangedBeforeRun = errors.New("the policy changed before the run started deleting (the cluster adopted retention policies, or the policy was edited); the next run uses the current policy")

// policyUnchanged re-reads the policy a run read before its discovery, just
// before it deletes: a run that straddles the cluster's adoption (or an edit)
// must not delete under a definition that is no longer the policy.
func (h *RetentionHandler) policyUnchanged(p *RetentionPolicy) error {
	now, err := h.getPolicy(p.ID)
	if err != nil || !now.IsActive || now.Name != p.Name || now.Database != p.Database ||
		measurementKeyOf(now.Measurement) != measurementKeyOf(p.Measurement) ||
		now.RetentionDays != p.RetentionDays || now.BufferDays != p.BufferDays {
		return errPolicyChangedBeforeRun
	}
	return nil
}

func measurementKeyOf(m *string) string {
	if m == nil {
		return ""
	}
	return *m
}

// recordExecutionStart records the start of an execution
func (h *RetentionHandler) recordExecutionStart(policyID int64, cutoffDate time.Time) int64 {
	// The history's foreign key needs this node's row of a cluster policy. A
	// run of a policy deleted cluster-wide after the run read it finds no row
	// and goes unrecorded, as a run of a policy deleted locally always did.
	h.ensureMirrored(context.Background())
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

// retentionManifestReason is the manifest delete reason of a retention
// policy's run: a compaction whose output it removes deletes its inputs too
// (raft.OutputGoneAccounted, #1207 d2a2b).
func retentionManifestReason[T int | int64](policyID T) string {
	return fmt.Sprintf("retention:%d", policyID)
}

// SetOnFilesRemoved sets the function called after this handler removed or
// rewrote parquet files: the query caches hold pruned partition lists, and one
// naming a removed hour makes DuckDB report "No files found", which a query
// answers with a retryable 503 (classifyNoFiles) until the cache is cleared.
func (h *RetentionHandler) SetOnFilesRemoved(fn func()) {
	h.onFilesRemoved = fn
}

// clearCachesAfterRun clears DuckDB's cache_httpfs cache (ClearHTTPCache) and,
// unless the run was a dry run (it removed nothing), the query caches.
func (h *RetentionHandler) clearCachesAfterRun(dryRun bool) {
	h.duckdb.ClearHTTPCache()
	if !dryRun && h.onFilesRemoved != nil {
		h.onFilesRemoved()
	}
}
