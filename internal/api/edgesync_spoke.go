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

// syncRunTimeout bounds one manual sync pass.
//
// Generous because a pass drains whatever backlog exists: a spoke returning
// from a long outage may have thousands of files to send, and cutting that
// short would leave it worse off than not having run at all. An operator who
// wants a shorter bound cancels the request.
const syncRunTimeout = 2 * time.Hour

// EdgeSyncSpokeHandler exposes the manual sync controls on a spoke.
//
// Distinct from EdgeSyncHandler, which is the hub's receive side. The two have
// different audiences and different authentication — a hub's endpoints are
// called by spokes and authenticated per-spoke by HMAC; these are called by an
// operator on the edge box and authenticated by an Arc admin token. Mounting
// them together would invite the wrong middleware.
//
// Mounted at /api/v1/spoke-sync, NOT under /api/v1/sync-*. Fiber's
// Group().Use() matches by string PREFIX, not by path segment, so a group at
// "/api/v1/sync" also matches "/api/v1/sync-spoke/..." — the hub's body limit
// and its middleware would silently run on these operator routes. A prefix
// that is not a string-prefix of another group's is the only way to keep the
// two genuinely uncoupled.
type EdgeSyncSpokeHandler struct {
	agent       *edgesync.Agent
	authManager *auth.AuthManager
	logger      zerolog.Logger
}

// NewEdgeSyncSpokeHandler validates configuration and returns a ready handler.
func NewEdgeSyncSpokeHandler(agent *edgesync.Agent, authManager *auth.AuthManager, logger zerolog.Logger) (*EdgeSyncSpokeHandler, error) {
	if agent == nil {
		return nil, errors.New("edgesync spoke: agent is required")
	}
	return &EdgeSyncSpokeHandler{
		agent:       agent,
		authManager: authManager,
		logger:      logger.With().Str("component", "edgesync-spoke").Logger(),
	}, nil
}

// RegisterRoutes mounts the spoke's manual sync controls.
//
// All admin-only. Triggering a sync moves data off this box and consumes
// whatever link budget it has; the ledger view exposes which measurements
// exist and how far behind they are. Neither is something a read-scoped token
// should reach.
func (h *EdgeSyncSpokeHandler) RegisterRoutes(app fiber.Router) {
	group := app.Group("/api/v1/spoke-sync")

	if h.authManager != nil {
		group.Use(auth.RequireAdmin(h.authManager))
	}

	group.Post("/run", h.run)
	group.Get("/status", h.status)
	group.Get("/ledger", h.ledger)

	h.logger.Info().Msg("Edge sync spoke routes registered")
}

// run handles POST /api/v1/spoke-sync/run.
//
// Synchronous: the caller gets the pass's outcome rather than a job ID to poll.
// A manual trigger is something an operator watches, and returning immediately
// would mean inventing job tracking for a feature whose automatic form (phase
// 2) will not need it.
func (h *EdgeSyncSpokeHandler) run(c *fiber.Ctx) error {
	ctx, cancel := context.WithTimeout(c.Context(), syncRunTimeout)
	defer cancel()

	h.logger.Info().Msg("Manual sync pass starting")

	res, err := h.agent.Run(ctx)
	if err != nil {
		h.logger.Error().Err(err).Msg("Manual sync pass failed")
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
			"error": err.Error(),
		})
	}

	h.logger.Info().
		Int("discovered", res.Discovered).
		Int("sent", res.Sent).
		Int64("bytes_sent", res.BytesSent).
		Int("already_present", res.AlreadyPresent).
		Int("partial", res.Partial).
		Int("failed", res.Failed).
		Int("conflicts", len(res.Conflicts)).
		Dur("duration", res.Duration).
		Msg("Manual sync pass complete")

	out := fiber.Map{
		"discovered":      res.Discovered,
		"recovered":       res.Recovered,
		"already_present": res.AlreadyPresent,
		"sent":            res.Sent,
		"bytes_sent":      res.BytesSent,
		"partial":         res.Partial,
		"failed":          res.Failed,
		"duration_ms":     res.Duration.Milliseconds(),
	}

	// Conflicts are reported in full rather than counted. Each one needs a
	// human to decide which copy is right, and a count alone would not say
	// which files to look at.
	conflicts := make([]fiber.Map, 0, len(res.Conflicts))
	for _, cf := range res.Conflicts {
		conflicts = append(conflicts, fiber.Map{
			"path":         cf.Path,
			"their_sha256": cf.TheirSHA256,
		})
	}
	out["conflicts"] = conflicts
	if len(conflicts) > 0 {
		out["warning"] = "Some paths hold different content on the hub. These are not retried; investigate before re-syncing."
	}

	return c.JSON(out)
}

// status handles GET /api/v1/spoke-sync/status.
func (h *EdgeSyncSpokeHandler) status(c *fiber.Ctx) error {
	ctx, cancel := context.WithTimeout(c.Context(), 30*time.Second)
	defer cancel()

	st, err := h.agent.Status(ctx)
	if err != nil {
		h.logger.Error().Err(err).Msg("Failed to read sync status")
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "failed to read sync status",
		})
	}

	out := fiber.Map{
		"hub_id":        st.HubID,
		"pending":       st.Pending,
		"in_flight":     st.InFlight,
		"synced":        st.Synced,
		"failed":        st.Failed,
		"pending_bytes": st.PendingBytes,
	}
	if st.LastSyncedAt != nil {
		out["last_synced_at"] = *st.LastSyncedAt
		// Sync lag is the number an operator actually watches: how long since
		// anything reached the hub. Derived here so a dashboard does not have
		// to do clock arithmetic against a timestamp it may parse differently.
		out["seconds_since_last_sync"] = int64(time.Since(*st.LastSyncedAt).Seconds())
	}
	return c.JSON(out)
}

// ledger handles GET /api/v1/spoke-sync/ledger.
//
// For troubleshooting: which files are stuck, how many attempts they have
// taken, and what the last error was. Without this an operator would have to
// open the SQLite file to answer "why is this not syncing?".
//
// Includes entries that exhausted their retries, not just queued ones — those
// are terminal until someone intervenes, so a pending-only view would omit the
// files most likely to have prompted the question.
func (h *EdgeSyncSpokeHandler) ledger(c *fiber.Ctx) error {
	ctx, cancel := context.WithTimeout(c.Context(), 30*time.Second)
	defer cancel()

	// Bounded by default. The ledger has one row per file the spoke has ever
	// produced, so an unbounded response on a long-running edge box would be
	// enormous.
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

	entries, err := h.agent.UnfinishedEntries(ctx, limit)
	if err != nil {
		h.logger.Error().Err(err).Msg("Failed to read the sync ledger")
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "failed to read the sync ledger",
		})
	}

	out := make([]fiber.Map, 0, len(entries))
	for _, e := range entries {
		row := fiber.Map{
			"path":           e.Path,
			"sha256":         e.SHA256,
			"size_bytes":     e.SizeBytes,
			"state":          string(e.State),
			"attempts":       e.Attempts,
			"bytes_sent":     e.BytesSent,
			"partition_time": e.PartitionTime,
		}
		if e.LastError != "" {
			row["last_error"] = e.LastError
		}
		out = append(out, row)
	}

	return c.JSON(fiber.Map{"entries": out, "limit": limit})
}
