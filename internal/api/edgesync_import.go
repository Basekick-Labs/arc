package api

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/basekick-labs/arc/internal/auth"
	"github.com/basekick-labs/arc/internal/edgesync"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// bundleImportTimeout bounds one import.
//
// Generous for the same reason the export is: a bundle can carry thousands of
// files off removable media, every one hashed twice (verify, then commit). An
// operator who wants a shorter bound cancels the request.
const bundleImportTimeout = 4 * time.Hour

// EdgeSyncImportHandler exposes the hub's air-gap import controls.
//
// Mounted at /api/v1/bundle-import, deliberately NOT under /api/v1/sync-*.
// Fiber's Group().Use() matches by string PREFIX, not path segment, so a group
// at "/api/v1/sync" also matches "/api/v1/sync-import/..." — the spoke-facing
// HMAC group's body limit and middleware would silently run on these operator
// routes. A prefix that is not a string-prefix of another group's is the only
// way to keep them uncoupled.
//
// Different audience from the hub's receive endpoints: those are called by
// spokes and authenticated per-spoke by HMAC; these are called by an operator
// who has just plugged in a drive, and are authenticated by an admin token.
type EdgeSyncImportHandler struct {
	importer    *edgesync.Importer
	policy      *edgesync.DestinationPolicy
	index       *edgesync.BundleIndex
	authManager *auth.AuthManager
	logger      zerolog.Logger
}

// NewEdgeSyncImportHandler validates configuration and returns a ready handler.
func NewEdgeSyncImportHandler(
	importer *edgesync.Importer,
	policy *edgesync.DestinationPolicy,
	index *edgesync.BundleIndex,
	authManager *auth.AuthManager,
	logger zerolog.Logger,
) (*EdgeSyncImportHandler, error) {
	if importer == nil {
		return nil, errors.New("edgesync import: importer is required")
	}
	if policy == nil {
		// The bundle path is operator-supplied and reaches the filesystem
		// directly. Without a policy it would be unchecked.
		return nil, errors.New("edgesync import: destination policy is required")
	}
	if index == nil {
		return nil, errors.New("edgesync import: bundle index is required")
	}
	return &EdgeSyncImportHandler{
		importer:    importer,
		policy:      policy,
		index:       index,
		authManager: authManager,
		logger:      logger.With().Str("component", "edgesync-import").Logger(),
	}, nil
}

// RegisterRoutes mounts the import controls. Admin-only.
//
// Importing writes a spoke's data into hub storage, and the history endpoint
// reveals which edge deployments have delivered and when. Neither is something
// a read-scoped token should reach.
func (h *EdgeSyncImportHandler) RegisterRoutes(app fiber.Router) {
	group := app.Group("/api/v1/bundle-import")

	if h.authManager != nil {
		group.Use(auth.RequireAdmin(h.authManager))
	}

	group.Post("/", h.importBundle)
	group.Get("/history/:spoke_id", h.history)

	h.logger.Info().Msg("Edge sync bundle import routes registered")
}

// importBundle handles POST /api/v1/bundle-import.
//
// Verifies the whole bundle before committing a single byte, then imports.
func (h *EdgeSyncImportHandler) importBundle(c *fiber.Ctx) error {
	var req struct {
		Path string `json:"path"`
	}
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid request body"})
	}
	if req.Path == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "path is required: the bundle directory to import",
		})
	}

	// Checked against the allow-list before anything opens it, for the same
	// reason export is: this path reaches the filesystem directly.
	resolved, err := h.policy.Resolve(req.Path)
	if err != nil {
		h.logger.Warn().Err(err).Msg("Bundle import path refused")
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
	}

	ctx, cancel := context.WithTimeout(c.Context(), bundleImportTimeout)
	defer cancel()

	h.logger.Info().Str("dir", resolved).Msg("Bundle import starting")

	res, err := h.importer.Import(ctx, resolved)
	switch {
	case errors.Is(err, edgesync.ErrBundleAlreadyImported):
		// 409, not an error: the hub already holds this bundle's contents, so
		// the operator's goal is met. The message carries when it arrived, so
		// a duplicate drive is diagnosable rather than mysterious.
		h.logger.Info().Err(err).Msg("Bundle already imported")
		return c.Status(fiber.StatusConflict).JSON(fiber.Map{
			"imported": false,
			"reason":   "already imported",
			"detail":   err.Error(),
		})

	case errors.Is(err, edgesync.ErrBundleInvalid):
		// The bundle itself is bad — tampered, truncated, for another hub, or
		// from an unknown spoke. Retrying the same drive fails identically, so
		// this is the operator's problem to look at, not a transient failure.
		h.logger.Error().Err(err).Str("dir", resolved).Msg("Bundle refused")
		return c.Status(fiber.StatusUnprocessableEntity).JSON(fiber.Map{
			"imported": false,
			"error":    err.Error(),
		})

	case err != nil:
		h.logger.Error().Err(err).Str("dir", resolved).Msg("Bundle import failed")
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"imported": false,
			"error":    "bundle import failed",
		})
	}

	out := fiber.Map{
		"imported":        true,
		"bundle_id":       res.BundleID,
		"spoke_id":        res.SpokeID,
		"created_at":      res.CreatedAt,
		"committed":       res.Committed,
		"already_present": res.AlreadyPresent,
		"bytes_written":   res.BytesWritten,
		"duration_ms":     res.Duration.Milliseconds(),
	}
	// Warnings accumulate: an import can both hit conflicts AND fail to record,
	// and assigning "warning" twice would silently drop the first.
	var warnings []string
	if !res.Recorded {
		// Surfaced rather than left to the log: /history will not show this
		// bundle, and an operator comparing the two deserves to know why.
		out["recorded"] = false
		warnings = append(warnings,
			"The files were imported, but this bundle could not be recorded. "+
				"It will not appear in the import history, and re-importing the same drive will not be refused.")
	}

	// Reported in full rather than counted: each needs a human to decide which
	// copy is right, and a count alone would not say which files to look at.
	conflicts := make([]fiber.Map, 0, len(res.Conflicts))
	for _, cf := range res.Conflicts {
		conflicts = append(conflicts, fiber.Map{
			"path":         cf.Path,
			"their_sha256": cf.TheirSHA256,
		})
	}
	out["conflicts"] = conflicts
	if len(conflicts) > 0 {
		warnings = append(warnings,
			"Some paths already hold different content on this hub. "+
				"They were not overwritten; investigate before re-importing.")
	}
	if len(warnings) > 0 {
		out["warnings"] = warnings
	}

	return c.JSON(out)
}

// history handles GET /api/v1/bundle-import/history/:spoke_id.
//
// Which bundles this hub has taken from a spoke, and when. The answer to "did
// last month's drive ever arrive?" — which, on an air-gap link with no
// telemetry, nothing else can answer.
func (h *EdgeSyncImportHandler) history(c *fiber.Ctx) error {
	spokeID := c.Params("spoke_id")
	if spokeID == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "spoke_id is required"})
	}

	limit := 100
	if raw := c.Query("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n <= 0 {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid limit"})
		}
		if n > 1000 {
			n = 1000
		}
		limit = n
	}

	ctx, cancel := context.WithTimeout(c.Context(), 30*time.Second)
	defer cancel()

	bundles, err := h.index.ListBySpoke(ctx, spokeID, limit)
	if err != nil {
		h.logger.Error().Err(err).Str("spoke_id", spokeID).Msg("Failed to list imported bundles")
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "failed to list imported bundles",
		})
	}

	out := make([]fiber.Map, 0, len(bundles))
	for _, b := range bundles {
		out = append(out, fiber.Map{
			"bundle_id":   b.BundleID,
			"created_at":  b.CreatedAt,
			"imported_at": b.ImportedAt,
			"file_count":  b.FileCount,
			"bytes_total": b.BytesTotal,
			"conflicts":   b.Conflicts,
		})
	}
	return c.JSON(fiber.Map{"spoke_id": spokeID, "bundles": out, "limit": limit})
}
