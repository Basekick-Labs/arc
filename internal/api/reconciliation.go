package api

import (
	"context"
	"errors"

	"github.com/basekick-labs/arc/internal/auth"
	"github.com/basekick-labs/arc/internal/license"
	"github.com/basekick-labs/arc/internal/reconciliation"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// ReconciliationSchedulerInterface is the minimal surface the API needs.
// Defining it as an interface here (rather than depending on the concrete
// reconciliation.Scheduler) keeps the API package mockable in tests
// without dragging in the full reconciliation package.
type ReconciliationSchedulerInterface interface {
	IsRunning() bool
	Status() map[string]interface{}
	TriggerNow(ctx context.Context, dryRun bool) (*reconciliation.Run, error)
	FindRun(id string) (*reconciliation.Run, bool)
}

// ReconciliationHandler exposes the reconciliation engine over HTTP.
// Mirrors the SchedulerHandler shape: GET /status open to any
// authenticated token, mutating routes wrapped with RequireAdmin.
type ReconciliationHandler struct {
	scheduler     ReconciliationSchedulerInterface
	licenseClient *license.Client
	authManager   *auth.AuthManager
	logger        zerolog.Logger
}

// NewReconciliationHandler constructs the handler. scheduler may be nil
// when reconciliation is not available (no clustering, no license, or
// not configured); in that case all routes return 503.
func NewReconciliationHandler(
	scheduler ReconciliationSchedulerInterface,
	licenseClient *license.Client,
	authManager *auth.AuthManager,
	logger zerolog.Logger,
) *ReconciliationHandler {
	return &ReconciliationHandler{
		scheduler:     scheduler,
		licenseClient: licenseClient,
		authManager:   authManager,
		logger:        logger.With().Str("component", "reconciliation-handler").Logger(),
	}
}

// RegisterRoutes wires the routes under /api/v1/reconciliation. Every
// route is gated by an explicit auth middleware per CLAUDE.md's "every
// new API endpoint MUST have auth middleware" rule:
//
//   - /status      — RequireRead (operational state leaks tenant database
//     names + cron timing if exposed unauthenticated)
//   - /trigger     — RequireAdmin (kicks off destructive cluster-wide deletes)
//   - /runs/:id    — RequireAdmin (run details include sample paths)
//
// The authManager-nil branch exists for tests and OSS no-auth deployments
// where the operator has explicitly disabled auth.
func (h *ReconciliationHandler) RegisterRoutes(app *fiber.App) {
	group := app.Group("/api/v1/reconciliation")

	if h.authManager != nil {
		group.Get("/status", auth.RequireRead(h.authManager), h.handleStatus)
		group.Post("/trigger", auth.RequireAdmin(h.authManager), h.handleTrigger)
		group.Get("/runs/:id", auth.RequireAdmin(h.authManager), h.handleGetRun)
	} else {
		group.Get("/status", h.handleStatus)
		group.Post("/trigger", h.handleTrigger)
		group.Get("/runs/:id", h.handleGetRun)
	}
}

// handleStatus returns the scheduler's Status() snapshot. Returns a
// best-effort 200 with `{enabled:false, reason:...}` when reconciliation
// is unavailable, mirroring the scheduler-handler's pattern at
// internal/api/scheduler.go:90-100.
func (h *ReconciliationHandler) handleStatus(c *fiber.Ctx) error {
	if h.scheduler == nil {
		return c.JSON(fiber.Map{
			"enabled": false,
			"reason":  "Reconciliation requires Enterprise clustering and is not configured on this node",
		})
	}
	return c.JSON(h.scheduler.Status())
}

// handleTrigger runs an on-demand reconcile cycle. Defaults to dry-run
// for the manual API path — operators must opt in to act mode with
// `dry_run=false&act=true`. The double-flag pattern (Druid-style "are
// you SURE?") prevents single-keystroke fat-fingers from triggering a
// real delete via curl.
func (h *ReconciliationHandler) handleTrigger(c *fiber.Ctx) error {
	if h.scheduler == nil {
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
			"error": "Reconciliation is not running on this node — Enterprise clustering required",
		})
	}

	dryRunQ := c.Query("dry_run", "true")
	actQ := c.Query("act", "false")
	dryRun := dryRunQ != "false"
	act := actQ == "true"

	// Act mode requires BOTH dry_run=false AND act=true. dry_run=true
	// alone (default) means the operator wants a report.
	if !dryRun && !act {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "act mode requires both dry_run=false AND act=true (safety: avoids accidental deletes)",
		})
	}
	effectiveDryRun := dryRun || !act

	run, err := h.scheduler.TriggerNow(c.Context(), effectiveDryRun)
	if err != nil {
		switch {
		case errors.Is(err, reconciliation.ErrAlreadyRunning):
			return c.Status(fiber.StatusConflict).JSON(fiber.Map{
				"error": "a reconciliation run is already in progress",
			})
		case errors.Is(err, reconciliation.ErrGated):
			return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
				"error": "this node is not permitted to run reconciliation (role-gated)",
			})
		case errors.Is(err, reconciliation.ErrDisabled):
			return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
				"error": "reconciliation feature is disabled",
			})
		case errors.Is(err, reconciliation.ErrManifestTooLarge):
			// Surface the partial run so operators see the manifest size.
			h.logger.Warn().Err(err).Str("run_id", runID(run)).Msg("Reconciliation: manifest too large")
			return c.Status(fiber.StatusUnprocessableEntity).JSON(fiber.Map{
				"error": err.Error(),
				"run":   run,
			})
		default:
			h.logger.Error().Err(err).Str("run_id", runID(run)).Msg("Reconciliation: trigger failed")
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"error": err.Error(),
				"run":   run,
			})
		}
	}

	h.logger.Info().
		Str("run_id", run.ID).
		Bool("dry_run", effectiveDryRun).
		Int("manifest_deletes", run.ManifestDeletes).
		Int("storage_deletes", run.StorageDeletes).
		Msg("Reconciliation: manual trigger completed")

	return c.JSON(fiber.Map{
		"run": run,
	})
}

// handleGetRun returns a specific run from the in-memory ring buffer.
// 404 when the run is not in the buffer (typical for runs older than
// the buffer cap).
func (h *ReconciliationHandler) handleGetRun(c *fiber.Ctx) error {
	if h.scheduler == nil {
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
			"error": "Reconciliation is not running on this node",
		})
	}
	id := c.Params("id")
	run, ok := h.scheduler.FindRun(id)
	if !ok {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
			"error": "run not found in recent-runs buffer",
		})
	}
	return c.JSON(run)
}

func runID(r *reconciliation.Run) string {
	if r == nil {
		return ""
	}
	return r.ID
}
