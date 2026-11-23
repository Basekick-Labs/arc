package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/basekick-labs/arc/internal/auth"
	"github.com/basekick-labs/arc/internal/license"
	"github.com/basekick-labs/arc/internal/tiering"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// TieringHandler handles tiered storage API operations
type TieringHandler struct {
	manager       *tiering.Manager
	authManager   *auth.AuthManager
	licenseClient *license.Client
	logger        zerolog.Logger
	getFiles      func(context.Context, string, string) ([]tiering.FileMetadata, error)
	// scanTiers and scanBudget are indirected for the same reason getFiles
	// is: the response classification below is worth testing without
	// standing up a Manager, a SQLite file and a licence client.
	scanTiers  func(context.Context) (*tiering.ScanResult, error)
	scanBudget func() time.Duration
	// previewMigration is indirected so the dry_run HTTP contract (filter
	// propagation and response shape) can be tested without a Manager.
	previewMigration func(ctx context.Context, from, to tiering.Tier, database, measurement string) ([]tiering.MigrationCandidate, error)
}

// NewTieringHandler creates a new tiering handler
func NewTieringHandler(manager *tiering.Manager, authManager *auth.AuthManager, licenseClient *license.Client, logger zerolog.Logger) *TieringHandler {
	return &TieringHandler{
		manager:          manager,
		authManager:      authManager,
		licenseClient:    licenseClient,
		logger:           logger.With().Str("component", "tiering-api").Logger(),
		scanTiers:        manager.ScanTiers,
		scanBudget:       manager.ScanBudget,
		previewMigration: manager.PreviewMigration,
		getFiles: func(ctx context.Context, tierParam, database string) ([]tiering.FileMetadata, error) {
			return listTieringFiles(ctx, manager.GetMetadata(), tierParam, database)
		},
	}
}

// tieringFileLister is the part of the tier metadata GET
// /api/v1/tiering/files reads.
type tieringFileLister interface {
	GetFilesByDatabase(ctx context.Context, database string) ([]tiering.FileMetadata, error)
	GetFilesInTier(ctx context.Context, tier tiering.Tier) ([]tiering.FileMetadata, error)
	GetFilesInTierForDatabase(ctx context.Context, database string, tier tiering.Tier) ([]tiering.FileMetadata, error)
}

// listTieringFiles lists the files the tier and database filters select.
// With both set it is the files of the database in that tier: the database
// filter used to return before the tier was read, so the tier was ignored
// (#1430).
func listTieringFiles(ctx context.Context, md tieringFileLister, tierParam, database string) ([]tiering.FileMetadata, error) {
	switch {
	case database != "" && tierParam != "":
		return md.GetFilesInTierForDatabase(ctx, database, tiering.TierFromString(tierParam))
	case database != "":
		return md.GetFilesByDatabase(ctx, database)
	case tierParam != "":
		return md.GetFilesInTier(ctx, tiering.TierFromString(tierParam))
	}
	var files []tiering.FileMetadata
	for _, t := range []tiering.Tier{tiering.TierHot, tiering.TierCold} {
		tierFiles, err := md.GetFilesInTier(ctx, t)
		if err != nil {
			return nil, err
		}
		files = append(files, tierFiles...)
	}
	return files, nil
}

// GetStatus returns the current tiering status
// GET /api/v1/tiering/status
func (h *TieringHandler) GetStatus(c *fiber.Ctx) error {
	ctx, cancel := context.WithTimeout(c.Context(), 30*time.Second)
	defer cancel()

	status, err := h.manager.GetStatus(ctx)
	if err != nil {
		h.logger.Error().Err(err).Msg("Failed to get tiering status")
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "Failed to get tiering status",
		})
	}

	return c.JSON(status)
}

// GetFiles returns files by tier
// GET /api/v1/tiering/files
// Query params: tier (optional), database (optional), limit (optional)
func (h *TieringHandler) GetFiles(c *fiber.Ctx) error {
	ctx, cancel := context.WithTimeout(c.Context(), 30*time.Second)
	defer cancel()

	tierParam := c.Query("tier")
	// TierFromString answers hot for anything it does not recognise, so an
	// unknown or wrong-case tier would list hot files under a 200.
	if tierParam != "" {
		if t := tiering.TierFromString(tierParam); t.String() != tierParam || !t.IsValid() {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid tier"})
		}
	}
	database := c.Query("database")
	limit := 100
	if raw := c.Query("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 0 {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid limit"})
		}
		limit = n
	}

	files, err := h.getFiles(ctx, tierParam, database)

	if err != nil {
		h.logger.Error().Err(err).Msg("Failed to get tiering files")
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "Failed to get tiering files",
		})
	}

	// limit=0 deliberately means an empty result. Positive limits are
	// clamped here as a second line of defence even if boundary validation changes.
	if limit == 0 {
		files = files[:0]
	} else if limit > 0 && len(files) > limit {
		files = files[:limit]
	}

	return c.JSON(fiber.Map{
		"files": files,
		"count": len(files),
	})
}

// TriggerMigrationRequest represents a manual migration request
type TriggerMigrationRequest struct {
	FromTier    string `json:"from_tier,omitempty"`   // Optional: "hot" (2-tier system)
	ToTier      string `json:"to_tier,omitempty"`     // Optional: "cold" (2-tier system)
	Database    string `json:"database,omitempty"`    // Optional: filter by database
	Measurement string `json:"measurement,omitempty"` // Optional: filter by measurement
	DryRun      bool   `json:"dry_run,omitempty"`     // If true, only report what would be migrated
}

// TriggerMigration triggers a manual migration
// POST /api/v1/tiering/migrate
func (h *TieringHandler) TriggerMigration(c *fiber.Ctx) error {
	// tiered_storage.migration_timeout (#1112), the same budget the scheduled
	// cycle gets.
	ctx, cancel := context.WithTimeout(c.Context(), h.manager.MigrationBudget())
	defer cancel()

	// An empty body migrates every database. This is a migration, not the
	// scheduled cycle: no pre-migration scan and no reconciliation
	// (POST /api/v1/tiering/reconcile runs that).
	req, err := parseMigrationRequest(c)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error":   "invalid request body: " + err.Error(),
			"message": `Send JSON such as {"database": "telemetry"}, or an empty body to migrate every database. Nothing was migrated.`,
		})
	}

	fromTier := tiering.TierHot
	if req.FromTier != "" {
		fromTier = tiering.TierFromString(req.FromTier)
		if fromTier.String() != req.FromTier || !fromTier.IsValid() {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid from_tier"})
		}
	}

	toTier := tiering.TierCold
	if req.ToTier != "" {
		toTier = tiering.TierFromString(req.ToTier)
		if toTier.String() != req.ToTier || !toTier.IsValid() {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid to_tier"})
		}
	}
	if fromTier != tiering.TierHot || toTier != tiering.TierCold {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "unsupported migration: only hot to cold is supported",
		})
	}

	h.logger.Info().
		Str("from_tier", req.FromTier).
		Str("to_tier", req.ToTier).
		Str("database", req.Database).
		Bool("dry_run", req.DryRun).
		Msg("Manual migration triggered")

	if req.DryRun {
		candidates, err := h.previewMigration(ctx, fromTier, toTier, req.Database, req.Measurement)
		if err != nil {
			if errors.Is(err, tiering.ErrMigrationRoleGated) {
				_, role := h.manager.MigrationGate()
				h.logger.Info().Str("role", role).Msg("Manual migration preview rejected: node is not the primary writer")
				status, body := migrationRoleGatedResponse(role)
				return c.Status(status).JSON(body)
			}
			h.logger.Error().Err(err).Msg("Failed to preview migration")
			return c.Status(fiber.StatusConflict).JSON(fiber.Map{
				"error": err.Error(),
			})
		}
		var totalBytes int64
		for _, candidate := range candidates {
			totalBytes += candidate.SizeBytes
		}
		return c.JSON(fiber.Map{
			"status":        "dry_run",
			"would_migrate": len(candidates),
			"total_bytes":   totalBytes,
			"from_tier":     fromTier,
			"to_tier":       toTier,
		})
	}

	res, err := h.manager.TriggerFilteredMigration(ctx, fromTier, toTier, req.Database, req.Measurement)
	if err != nil {
		if errors.Is(err, tiering.ErrMigrationRoleGated) {
			_, role := h.manager.MigrationGate()
			h.logger.Info().Str("role", role).Msg("Manual migration rejected: node is not the primary writer")
			status, body := migrationRoleGatedResponse(role)
			return c.Status(status).JSON(body)
		}
		if errors.Is(err, tiering.ErrMigrationCycleRunning) {
			return c.Status(fiber.StatusConflict).JSON(fiber.Map{
				"error":   "Migration cycle already running",
				"message": "A migration cycle is already in progress on this node. Wait for it to complete.",
			})
		}
		h.logger.Error().Err(err).Msg("Failed to trigger migration")
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "Failed to trigger migration",
		})
	}

	return migrationResponse(c, res)
}

// migrationResponse answers a migration with what it did (#1830). It used to
// say "Migration completed successfully" whatever happened, including a run
// cut off by its budget that left migrated files hot.
//   - Files kept hot after their migration (a manifest update or hot delete
//     failed): 500. They are in cold and still read from hot, so a node
//     reading both tiers counts them twice until they are reconciled.
//   - The budget ran out with nothing kept: 200, status "incomplete", and how
//     many files were not attempted; running it again moves them.
//   - Otherwise 200, status "completed".
func migrationResponse(c *fiber.Ctx, res tiering.MigrationResult) error {
	body := fiber.Map{
		"status":          "completed",
		"message":         "Migration completed",
		"candidates":      res.Candidates,
		"migrated":        res.Migrated,
		"errors":          res.Failed,
		"not_attempted":   res.NotAttempted,
		"kept_hot_copies": res.Kept,
		"timed_out":       res.TimedOut,
	}
	switch {
	case res.Kept > 0:
		body["status"] = "incomplete"
		body["message"] = "Some migrated files kept their hot copies and are read twice until reconciled; run POST /api/v1/tiering/reconcile"
		if res.ReleaseErr != nil {
			body["error"] = res.ReleaseErr.Error()
		}
		return c.Status(fiber.StatusInternalServerError).JSON(body)
	case res.NotAttempted > 0:
		body["status"] = "incomplete"
		if res.TimedOut {
			body["message"] = "Migration stopped at tiered_storage.migration_timeout; run it again to move the files not attempted"
		} else {
			body["message"] = "Migration stopped before every file was attempted; run it again to move the rest"
		}
	}
	return c.JSON(body)
}

// ReconcileRequest is the optional body of POST /api/v1/tiering/reconcile.
type ReconcileRequest struct {
	// WindowHours is how far back the orphan sweep looks for migrated files
	// whose hot copy is still there; default 48 (the scheduled cycle's).
	WindowHours int `json:"window_hours,omitempty"`
}

// Reconcile runs the migration cycle's reconciliation on demand (#1830):
// cold sync and hot scan, then the orphan sweep (migrated files whose hot
// copy is still there, removed manifest first once the cold copy is
// verified) and the manifest sweep. It is how an API-driven deployment
// finishes the files a migration reported as kept, without enabling the
// schedule or restarting.
// POST /api/v1/tiering/reconcile
func (h *TieringHandler) Reconcile(c *fiber.Ctx) error {
	var req ReconcileRequest
	// JSON whatever the Content-Type: curl -d sends a form type, and the
	// form decoder would drop window_hours without a word.
	if body := c.Body(); len(bytes.TrimSpace(body)) > 0 {
		dec := json.NewDecoder(bytes.NewReader(body))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&req); err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid request body: expected JSON such as {\"window_hours\": 72}: " + err.Error()})
		}
	}
	if req.WindowHours < 0 || req.WindowHours > 8760 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "window_hours must be between 0 (the default 48) and 8760"})
	}
	windowHours := req.WindowHours
	if windowHours == 0 {
		windowHours = 48
	}
	ctx, cancel := context.WithTimeout(c.Context(), h.manager.MigrationBudget())
	defer cancel()
	res, err := h.manager.Reconcile(ctx, time.Duration(req.WindowHours)*time.Hour)
	if err != nil {
		switch {
		case errors.Is(err, tiering.ErrMigrationRoleGated):
			_, role := h.manager.MigrationGate()
			status, body := migrationRoleGatedResponse(role)
			body["error"] = "Reconciliation runs on the primary writer only; this node is not the primary writer"
			return c.Status(status).JSON(body)
		case errors.Is(err, tiering.ErrMigrationCycleRunning):
			return c.Status(fiber.StatusConflict).JSON(fiber.Map{
				"error":   "Migration cycle already running",
				"message": "A migration or reconciliation is already in progress on this node. Wait for it to complete.",
			})
		case errors.Is(err, tiering.ErrColdSyncFailed):
			return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
				"error":   err.Error(),
				"message": "Check that the cold credentials allow listing the bucket or container, and that tiered_storage.scan_timeout and tiered_storage.migration_timeout cover a full listing.",
			})
		case errors.Is(err, tiering.ErrColdTierDisabled):
			return c.Status(fiber.StatusConflict).JSON(fiber.Map{"error": err.Error()})
		}
		h.logger.Error().Err(err).Msg("Reconciliation failed")
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
	}
	status := "completed"
	if res.Errors > 0 {
		status = "completed_with_errors"
	}
	return c.JSON(fiber.Map{
		"status":                   status,
		"window_hours":             windowHours,
		"cold_synced":              res.ColdSynced,
		"orphans_found":            res.OrphansFound,
		"orphans_deleted":          res.OrphansDeleted,
		"manifest_entries_removed": res.ManifestEntriesRemoved,
		"errors":                   res.Errors,
	})
}

// migrationRequestFields are the fields TriggerMigrationRequest reads.
var migrationRequestFields = map[string]bool{
	"from_tier": true, "to_tier": true, "database": true, "measurement": true, "dry_run": true,
}

// parseMigrationRequest reads the migrate body (#1841). A body it cannot
// read used to be ignored like an empty one, and an empty body migrates
// every database: a scoped request sent as JSON with curl's default form
// Content-Type, malformed, or with a misspelt field ran an unscoped
// migration of everything and answered as if it had done what was asked.
//   - empty: migrate every database, as always;
//   - starting with '{': JSON, whatever the Content-Type, with unknown
//     fields and trailing data refused;
//   - application/x-www-form-urlencoded: form fields (dry_run read as a
//     boolean), unknown or repeated keys refused;
//   - anything else: refused.
//
// In both forms a filter that is present but empty, and a dry_run that is
// not a boolean, are refused: either would otherwise widen the request to a
// real migration of everything.
func parseMigrationRequest(c *fiber.Ctx) (TriggerMigrationRequest, error) {
	var req TriggerMigrationRequest
	body := bytes.TrimSpace(c.Body())
	if len(body) == 0 {
		return req, nil
	}
	if body[0] == '{' {
		// Pointers, so a field that is present but null or empty is told
		// apart from an absent one.
		var raw struct {
			FromTier    *string `json:"from_tier"`
			ToTier      *string `json:"to_tier"`
			Database    *string `json:"database"`
			Measurement *string `json:"measurement"`
			DryRun      *bool   `json:"dry_run"`
		}
		dec := json.NewDecoder(bytes.NewReader(body))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&raw); err != nil {
			return req, err
		}
		// json.Unmarshal of the whole body also refuses anything after the
		// object, which the streaming decoder above does not. And a null
		// decodes like an absent field, and an absent filter means every
		// database: refuse it. A null dry_run would mean a real run.
		var keys map[string]json.RawMessage
		if err := json.Unmarshal(body, &keys); err != nil {
			return req, err
		}
		for k, v := range keys {
			if string(bytes.TrimSpace(v)) == "null" {
				return req, fmt.Errorf("field %q is null", k)
			}
		}
		for name, v := range map[string]*string{"from_tier": raw.FromTier, "to_tier": raw.ToTier, "database": raw.Database, "measurement": raw.Measurement} {
			if v != nil && strings.TrimSpace(*v) == "" {
				return req, fmt.Errorf("field %q is empty; leave it out to mean every one", name)
			}
		}
		if raw.FromTier != nil {
			req.FromTier = *raw.FromTier
		}
		if raw.ToTier != nil {
			req.ToTier = *raw.ToTier
		}
		if raw.Database != nil {
			req.Database = *raw.Database
		}
		if raw.Measurement != nil {
			req.Measurement = *raw.Measurement
		}
		if raw.DryRun != nil {
			req.DryRun = *raw.DryRun
		}
		return req, nil
	}
	if strings.HasPrefix(strings.ToLower(string(c.Request().Header.ContentType())), fiber.MIMEApplicationForm) {
		vals, err := url.ParseQuery(string(body))
		if err != nil {
			return req, err
		}
		for k, v := range vals {
			if !migrationRequestFields[k] {
				return req, fmt.Errorf("unknown field %q", k)
			}
			if len(v) != 1 {
				return req, fmt.Errorf("field %q given %d times", k, len(v))
			}
			if k != "dry_run" && strings.TrimSpace(v[0]) == "" {
				return req, fmt.Errorf("field %q is empty; leave it out to mean every one", k)
			}
		}
		// Decoded here, not by BodyParser: its form decoder matches Go field
		// names rather than the json tags, so dry_run=true was dropped and a
		// dry run asked for as form fields ran a real migration.
		req.FromTier = vals.Get("from_tier")
		req.ToTier = vals.Get("to_tier")
		req.Database = vals.Get("database")
		req.Measurement = vals.Get("measurement")
		if v, ok := vals["dry_run"]; ok {
			// Present means it must parse: a bare "dry_run" or "dry_run="
			// is not a request for a real migration.
			dry, err := strconv.ParseBool(v[0])
			if err != nil {
				return req, fmt.Errorf("dry_run %q is not true or false", v[0])
			}
			req.DryRun = dry
		}
		return req, nil
	}
	return req, errors.New("expected a JSON object or form fields")
}

// migrationRoleGatedResponse is the answer to a manual migration on a node
// the cluster gate excludes. 409: the request conflicts with the node's
// current role rather than being malformed or unauthorized, and the same
// request succeeds on the primary writer.
func migrationRoleGatedResponse(role string) (int, fiber.Map) {
	return fiber.StatusConflict, fiber.Map{
		"error":   "Migration runs on the primary writer only; this node is not the primary writer",
		"role":    role,
		"message": "Retry against the primary writer, or wait for the scheduled cycle: this node keeps its tier metadata in sync on every tick.",
	}
}

// GetStats returns migration statistics
// GET /api/v1/tiering/stats
func (h *TieringHandler) GetStats(c *fiber.Ctx) error {
	ctx, cancel := context.WithTimeout(c.Context(), 30*time.Second)
	defer cancel()

	// Get tier stats
	tierStats, err := h.manager.GetMetadata().GetTierStats(ctx)
	if err != nil {
		h.logger.Error().Err(err).Msg("Failed to get tier stats")
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "Failed to get tier stats",
		})
	}

	// Get recent migrations
	recentMigrations, err := h.manager.GetMetadata().GetRecentMigrations(ctx, 10)
	if err != nil {
		h.logger.Error().Err(err).Msg("Failed to get recent migrations")
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "Failed to get recent migrations",
		})
	}

	return c.JSON(fiber.Map{
		"tier_stats":        tierStats,
		"recent_migrations": recentMigrations,
	})
}

// ScanFiles registers every file in hot storage and, on any node with a usable
// cold tier, first reads the cold listing — which is what rebuilds cold rows
// this node has lost and, on a cluster, what shows it the files other nodes
// moved (#1179).
// POST /api/v1/tiering/scan
//
// Bounded by tiered_storage.scan_timeout, the same budget the startup scan and
// the pre-migration scan use (#1154). The budget is read from the manager
// rather than carried on the handler so it cannot be a zero here.
//
// Synchronous on purpose. c.Context() is the right parent because it is what
// makes a server shutdown cancel an in-flight scan, and the handler blocks
// until the scan returns, so the context never outlives the request. Note it
// is NOT cancelled when the client disconnects: fasthttp closes
// RequestCtx.Done only on shutdown, which is why a second scan is refused
// below rather than left to pile up behind an abandoned curl.
func (h *TieringHandler) ScanFiles(c *fiber.Ctx) error {
	budget := h.scanBudget()
	ctx, cancel := context.WithTimeout(c.Context(), budget)
	defer cancel()

	h.logger.Info().Dur("scan_timeout", budget).Msg("Starting file scan via API")

	result, err := h.scanTiers(ctx)
	switch {
	case errors.Is(err, tiering.ErrScanRunning):
		h.logger.Warn().Msg("File scan via API refused: a scan is already running on this node")
		return c.Status(fiber.StatusConflict).JSON(fiber.Map{
			"error":   "A tier scan is already running on this node",
			"message": "Wait for it to finish and read its result from GET /api/v1/tiering/status, which reports the last scan.",
		})

	case errors.Is(err, context.DeadlineExceeded):
		// The scan ran out of budget. The rows it wrote are real, so the
		// partial counts go back with the error rather than being
		// discarded: a bare 500 told the operator nothing about how far it
		// got, or that no stale hot row was retired.
		h.logger.Warn().Dur("scan_timeout", budget).
			Int("scanned", result.FilesScanned).
			Msg("File scan via API ran out of budget and stopped part way")
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
			"error":   "The tier scan ran out of its budget and stopped part way",
			"message": "The rows below were written, but the scan is incomplete and no stale hot rows were retired. Raise tiered_storage.scan_timeout above the time a full scan takes on this node.",
			"result":  result,
		})

	case errors.Is(err, context.Canceled):
		// The ONLY producer of Canceled here is the server shutting down:
		// fasthttp closes RequestCtx.Done on shutdown and never on a client
		// disconnect. Saying "raise your budget" would be the wrong advice.
		h.logger.Warn().Msg("File scan via API cancelled: the node is shutting down")
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
			"error":   "The tier scan was cancelled because this node is shutting down",
			"message": "Retry after the node restarts; the startup scan may have already covered it.",
			"result":  result,
		})

	case err != nil:
		h.logger.Error().Err(err).Msg("Failed to scan files")
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error":  err.Error(),
			"result": result,
		})
	}

	// A truncation in the retirement tail returns no error of its own, so it
	// arrives here with err nil. Answering 200 would report a scan that left
	// stale hot rows behind as a clean one.
	if result.Truncated {
		h.logger.Warn().Dur("scan_timeout", budget).
			Int("scanned", result.FilesScanned).
			Msg("File scan via API stopped part way")
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
			"error":   "The tier scan stopped part way through",
			"message": "The rows below were written, but the scan is incomplete and no stale hot rows were retired. Raise tiered_storage.scan_timeout above the time a full scan takes on this node.",
			"result":  result,
		})
	}

	return c.JSON(result)
}

// requireTieringLicense checks that the license still includes the tiering feature.
func (h *TieringHandler) requireTieringLicense(c *fiber.Ctx) error {
	if h.licenseClient == nil || !h.licenseClient.CanUseTieredStorage() {
		return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
			"success": false,
			"error":   "Tiered storage requires an enterprise license with the 'tiering' feature enabled",
		})
	}
	return c.Next()
}

// RegisterRoutes registers tiering API routes
func (h *TieringHandler) RegisterRoutes(app fiber.Router) {
	tiering := app.Group("/api/v1/tiering")
	if h.authManager != nil {
		tiering.Use(auth.RequireAdmin(h.authManager))
	}
	tiering.Use(h.requireTieringLicense)

	tiering.Get("/status", h.GetStatus)
	tiering.Get("/files", h.GetFiles)
	tiering.Post("/migrate", h.TriggerMigration)
	tiering.Post("/reconcile", h.Reconcile)
	tiering.Get("/stats", h.GetStats)
	tiering.Post("/scan", h.ScanFiles)
}
