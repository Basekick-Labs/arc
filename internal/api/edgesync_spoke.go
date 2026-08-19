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
	agent *edgesync.Agent

	// exporter is nil unless air-gap bundle export is enabled. It is
	// independent of the agent: a fully air-gapped spoke exports bundles and
	// never runs the network path at all.
	exporter *edgesync.Exporter

	authManager *auth.AuthManager
	logger      zerolog.Logger
}

// NewEdgeSyncSpokeHandler validates configuration and returns a ready handler.
func NewEdgeSyncSpokeHandler(agent *edgesync.Agent, exporter *edgesync.Exporter, authManager *auth.AuthManager, logger zerolog.Logger) (*EdgeSyncSpokeHandler, error) {
	if agent == nil && exporter == nil {
		// One or the other must exist, or the routes would all 503. Both is
		// the normal case for a spoke that has intermittent connectivity AND
		// ships drives.
		return nil, errors.New("edgesync spoke: an agent or an exporter is required")
	}
	return &EdgeSyncSpokeHandler{
		agent:       agent,
		exporter:    exporter,
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
	group.Post("/export", h.export)
	group.Post("/export/:bundle_id/revert", h.revertExport)
	group.Post("/ack", h.applyAck)

	h.logger.Info().Msg("Edge sync spoke routes registered")
}

// run handles POST /api/v1/spoke-sync/run.
//
// Synchronous: the caller gets the pass's outcome rather than a job ID to poll.
// A manual trigger is something an operator watches, and returning immediately
// would mean inventing job tracking for a feature whose automatic form (phase
// 2) will not need it.
func (h *EdgeSyncSpokeHandler) run(c *fiber.Ctx) error {
	if h.agent == nil {
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
			"error": "the network sync agent is not enabled (edge_sync.spoke.enabled)",
		})
	}

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
		Int("skipped", res.Skipped).
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
		"skipped":         res.Skipped,
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

	// Served by whichever side exists: both are pure ledger reads, and an
	// air-gap-only spoke has no agent. Gating this on the agent would hide the
	// exported count from the only operator who needs it — the one whose files
	// are on a drive somewhere.
	var (
		st  *edgesync.Stats
		err error
	)
	switch {
	case h.agent != nil:
		st, err = h.agent.Status(ctx)
	case h.exporter != nil:
		st, err = h.exporter.Status(ctx)
	default:
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
			"error": "no edge sync transport is enabled",
		})
	}
	if err != nil {
		h.logger.Error().Err(err).Msg("Failed to read sync status")
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "failed to read sync status",
		})
	}

	out := fiber.Map{
		"hub_id":    st.HubID,
		"pending":   st.Pending,
		"in_flight": st.InFlight,
		// Files on physical media awaiting an ack. Reported separately so an
		// operator can tell "queued here" from "in transit on a drive".
		"exported": st.Exported,
		"synced":   st.Synced,
		"failed":   st.Failed,
		// Source files that vanished (compaction/retention) before delivery.
		// Terminal bookkeeping, excluded from pending_bytes.
		"skipped":       st.Skipped,
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

	// Same reasoning as status: a pure ledger read, served by either side.
	var (
		entries []*edgesync.LedgerEntry
		err     error
	)
	switch {
	case h.agent != nil:
		entries, err = h.agent.UnfinishedEntries(ctx, limit)
	case h.exporter != nil:
		entries, err = h.exporter.UnfinishedEntries(ctx, limit)
	default:
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
			"error": "no edge sync transport is enabled",
		})
	}
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
		// Which bundle a file left on. The answer an operator needs when a
		// drive does not arrive and they have to decide what to revert.
		if e.ExportedBundleID != "" {
			row["exported_bundle_id"] = e.ExportedBundleID
		}
		if e.ExportedAt != nil {
			row["exported_at"] = *e.ExportedAt
		}
		out = append(out, row)
	}

	return c.JSON(fiber.Map{"entries": out, "limit": limit})
}

// bundleExportTimeout bounds one export.
//
// Generous for the same reason a sync pass is: a bundle can carry thousands of
// files to removable media, and cutting that short leaves a partial tree the
// writer must then clean up. An operator who wants a shorter bound cancels.
const bundleExportTimeout = 2 * time.Hour

// export handles POST /api/v1/spoke-sync/export.
//
// Writes pending files to an air-gap bundle under an operator-supplied path.
// The path is checked against the configured allow-list before anything is
// written — it reaches the filesystem directly, unlike every other Arc write.
func (h *EdgeSyncSpokeHandler) export(c *fiber.Ctx) error {
	if h.exporter == nil {
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
			"error": "air-gap bundle export is not enabled (edge_sync.spoke.bundle.enabled)",
		})
	}

	var req struct {
		Path  string `json:"path"`
		Limit int    `json:"limit"`
	}
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid request body"})
	}
	if req.Path == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "path is required: the directory to write the bundle into",
		})
	}

	ctx, cancel := context.WithTimeout(c.Context(), bundleExportTimeout)
	defer cancel()

	res, err := h.exporter.Export(ctx, req.Path, req.Limit)
	switch {
	case errors.Is(err, edgesync.ErrNothingToExport):
		// Not a failure: a drained backlog is the steady state, and a
		// scheduled export should not look broken when it finds nothing.
		return c.JSON(fiber.Map{"exported": false, "reason": "nothing to export"})

	case errors.Is(err, edgesync.ErrDestinationRefused):
		// The operator's own input, so 400 rather than 503 — retrying without
		// changing the path would fail identically.
		h.logger.Warn().Err(err).Msg("Bundle export destination refused")
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})

	case err != nil:
		// A fixed string, with the detail in the log — matching this file's
		// other handlers. The raw error carries absolute filesystem paths, and
		// while the endpoint is admin-only there is no reason to return them.
		h.logger.Error().Err(err).Msg("Bundle export failed")
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "bundle export failed"})
	}

	h.logger.Info().
		Str("bundle_id", res.BundleID).
		Str("dir", res.Dir).
		Int("files", res.FileCount).
		Int64("bytes", res.Bytes).
		Msg("Bundle exported")

	return c.JSON(fiber.Map{
		"exported":  true,
		"bundle_id": res.BundleID,
		"dir":       res.Dir,
		"files":     res.FileCount,
		"bytes":     res.Bytes,
		// Eligible entries whose source file vanished before export
		// (compaction or retention); recorded as skipped in the ledger.
		"skipped":     res.Skipped,
		"duration_ms": res.Duration.Milliseconds(),
		// The bundle is on media but no hub has confirmed it. Saying so here
		// stops an operator reading "exported" as "delivered".
		"note": "Files are marked exported, not synced. They advance to synced when the hub acknowledges the bundle.",
	})
}

// revertExport handles POST /api/v1/spoke-sync/export/:bundle_id/revert.
//
// For a drive that was lost, damaged, or never delivered: returns that
// bundle's files to pending so a later bundle or contact window carries them.
// Scoped to one bundle so recovering one drive does not disturb others in
// transit.
func (h *EdgeSyncSpokeHandler) revertExport(c *fiber.Ctx) error {
	if h.exporter == nil {
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
			"error": "air-gap bundle export is not enabled (edge_sync.spoke.bundle.enabled)",
		})
	}

	ctx, cancel := context.WithTimeout(c.Context(), 30*time.Second)
	defer cancel()

	bundleID := c.Params("bundle_id")
	n, err := h.exporter.Revert(ctx, bundleID)
	if err != nil {
		h.logger.Warn().Err(err).Str("bundle_id", bundleID).Msg("Bundle revert failed")
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
	}

	h.logger.Info().Str("bundle_id", bundleID).Int64("files", n).Msg("Bundle reverted")
	return c.JSON(fiber.Map{"bundle_id": bundleID, "reverted": n})
}

// applyAck handles POST /api/v1/spoke-sync/ack.
//
// The return leg of the air-gap transport: an operator plugs a drive back in
// after it has been to the hub, and this advances the files the hub confirmed
// from `exported` to `synced`.
//
// This is what makes those entries prunable. Without it `synced` is
// unreachable on an air-gapped spoke, so the ledger grows without bound on the
// box least able to receive a site visit.
func (h *EdgeSyncSpokeHandler) applyAck(c *fiber.Ctx) error {
	if h.exporter == nil {
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
			"error": "air-gap bundle export is not enabled (edge_sync.spoke.bundle.enabled)",
		})
	}

	var req struct {
		Path string `json:"path"`
	}
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid request body"})
	}
	if req.Path == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "path is required: the returned bundle directory",
		})
	}

	ctx, cancel := context.WithTimeout(c.Context(), 30*time.Minute)
	defer cancel()

	res, err := h.exporter.ApplyAck(ctx, req.Path)
	switch {
	case errors.Is(err, edgesync.ErrNoAck):
		// Not an error: a drive that has not yet been to the hub is the normal
		// state on the outbound leg, and an operator checking early should not
		// see a failure.
		return c.JSON(fiber.Map{
			"applied": false,
			"reason":  "this bundle carries no acknowledgment yet",
		})

	case errors.Is(err, edgesync.ErrAckInvalid), errors.Is(err, edgesync.ErrDestinationRefused):
		// The operator's own input — a tampered ack, one from another hub, or
		// a path outside the allow-list. Retrying unchanged fails identically.
		h.logger.Warn().Err(err).Msg("Acknowledgment refused")
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})

	case err != nil:
		h.logger.Error().Err(err).Msg("Failed to apply an acknowledgment")
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "failed to apply the acknowledgment",
		})
	}

	out := fiber.Map{
		"applied":     true,
		"bundle_id":   res.BundleID,
		"hub_id":      res.HubID,
		"imported_at": res.ImportedAt,
		"synced":      res.Synced,
	}
	var warnings []string
	// The three non-advanced cases mean different things, so they are reported
	// apart. Already-synced is the benign replay; untracked is a restored
	// spoke; a discrepancy means the hub holds a file this spoke gave up on,
	// which is the only one that says something is wrong.
	if res.AlreadySynced > 0 {
		out["already_synced"] = res.AlreadySynced
	}
	if res.Untracked > 0 {
		out["untracked"] = res.Untracked
	}
	if res.Discrepancies > 0 {
		out["discrepancies"] = res.Discrepancies
		warnings = append(warnings,
			"The hub acknowledges files this spoke had given up on. They remain failed locally; "+
				"the data IS on the hub, but the local ledger disagrees and is worth investigating.")
	}

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
			"The hub holds different content at these paths. They were NOT acknowledged "+
				"and remain exported; investigate before re-sending.")
	}
	if len(warnings) > 0 {
		out["warnings"] = warnings
	}

	return c.JSON(out)
}
