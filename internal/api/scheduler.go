package api

import (
	"context"
	"errors"

	"github.com/basekick-labs/arc/internal/auth"
	"github.com/basekick-labs/arc/internal/license"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// CQSchedulerInterface defines the interface for CQ scheduler operations
type CQSchedulerInterface interface {
	Status() map[string]interface{}
	// ReloadAll restarts every active CQ's job. It returns
	// ErrCQReloadUnlicensed, leaving the jobs as they are, when the license
	// no longer includes the CQ scheduler.
	ReloadAll() error
	JobCount() int
}

// RetentionSchedulerInterface defines the interface for retention scheduler operations
type RetentionSchedulerInterface interface {
	Status() map[string]interface{}
	// TriggerNow runs every active retention policy now. It returns one of
	// the ErrRetentionTrigger* errors for a trigger it refuses without
	// running anything.
	TriggerNow(ctx context.Context) error
}

// SchedulerLicense is the part of *license.Client the scheduler routes read.
type SchedulerLicense interface {
	GetLicense() *license.License
	CanUseCQScheduler() bool
	CanUseRetentionScheduler() bool
}

// Errors the schedulers return for a control request they refuse without
// doing anything (#1433). They are declared here, not in the scheduler
// package, because that package imports this one.
var (
	// ErrCQReloadUnlicensed: the license no longer includes the CQ scheduler,
	// for example because it expired after startup. ReloadAll returns it
	// before touching the running jobs.
	ErrCQReloadUnlicensed = errors.New("cq: the license does not include the CQ scheduler")
	// ErrRetentionTriggerRoleGated: this node is not the primary writer, the
	// only node that runs retention.
	ErrRetentionTriggerRoleGated = errors.New("retention: node role is not primary writer")
	// ErrRetentionTriggerRunning: a retention cycle is already running.
	ErrRetentionTriggerRunning = errors.New("retention: a cycle is already running")
	// ErrRetentionTriggerUnlicensed: the license no longer includes the
	// retention scheduler, for example because it expired after startup.
	ErrRetentionTriggerUnlicensed = errors.New("retention: the license does not include the retention scheduler")
)

// SchedulerHandler handles scheduler status API endpoints
type SchedulerHandler struct {
	cqScheduler        CQSchedulerInterface
	retentionScheduler RetentionSchedulerInterface
	licenseClient      SchedulerLicense
	authManager        *auth.AuthManager
	logger             zerolog.Logger
}

// NewSchedulerHandler creates a new scheduler handler
func NewSchedulerHandler(
	cqScheduler CQSchedulerInterface,
	retentionScheduler RetentionSchedulerInterface,
	licenseClient *license.Client,
	authManager *auth.AuthManager,
	logger zerolog.Logger,
) *SchedulerHandler {
	h := &SchedulerHandler{
		cqScheduler:        cqScheduler,
		retentionScheduler: retentionScheduler,
		authManager:        authManager,
		logger:             logger.With().Str("component", "scheduler-handler").Logger(),
	}
	// A nil *license.Client in the interface would not be a nil interface.
	if licenseClient != nil {
		h.licenseClient = licenseClient
	}
	return h
}

// RegisterRoutes registers scheduler API routes
func (h *SchedulerHandler) RegisterRoutes(app *fiber.App) {
	group := app.Group("/api/v1/schedulers")

	// Read-only route — any authenticated token
	group.Get("/", h.handleGetStatus)

	// Admin routes — require admin permission
	if h.authManager != nil {
		group.Post("/cq/reload", auth.RequireAdmin(h.authManager), h.handleCQReload)
		group.Post("/retention/trigger", auth.RequireAdmin(h.authManager), h.handleRetentionTrigger)
	} else {
		group.Post("/cq/reload", h.handleCQReload)
		group.Post("/retention/trigger", h.handleRetentionTrigger)
	}
}

// handleGetStatus returns the status of all schedulers
func (h *SchedulerHandler) handleGetStatus(c *fiber.Ctx) error {
	status := make(map[string]interface{})

	// CQ Scheduler status
	if h.cqScheduler != nil {
		status["cq_scheduler"] = h.cqScheduler.Status()
	} else {
		_, reason := h.cqNotRunning()
		status["cq_scheduler"] = map[string]interface{}{
			"enabled": false,
			"reason":  reason,
		}
	}

	// Retention Scheduler status
	if h.retentionScheduler != nil {
		status["retention_scheduler"] = h.retentionScheduler.Status()
	} else {
		_, reason := h.retentionNotRunning()
		status["retention_scheduler"] = map[string]interface{}{
			"enabled": false,
			"reason":  reason,
		}
	}

	return c.JSON(status)
}

// schedulerNotRunning says why a scheduler is not running: the reason the
// status route reports, and the status a control route answers with (#1433).
// A license that is missing, no longer valid, or of a tier without the
// feature is a 403, as other license-gated routes answer. With the license in
// order, the scheduler is off in config or failed to start, which the startup
// log says: a 503.
func schedulerNotRunning(lic *license.License, entitled bool, tierReason, configReason string) (int, string) {
	if lic == nil || !lic.IsValid() {
		return fiber.StatusForbidden, "Enterprise license required"
	}
	if !entitled {
		return fiber.StatusForbidden, tierReason
	}
	return fiber.StatusServiceUnavailable, configReason
}

// currentLicense returns the current license, nil without a license client,
// and whether entitled reports that it includes the feature.
func (h *SchedulerHandler) currentLicense(entitled func(SchedulerLicense) bool) (*license.License, bool) {
	if h.licenseClient == nil {
		return nil, false
	}
	return h.licenseClient.GetLicense(), entitled(h.licenseClient)
}

func (h *SchedulerHandler) cqNotRunning() (int, string) {
	lic, entitled := h.currentLicense(SchedulerLicense.CanUseCQScheduler)
	return schedulerNotRunning(lic, entitled,
		"License tier does not include CQ scheduler feature",
		"CQ feature not enabled (continuous_query.enabled=false), or the scheduler failed to start (see the startup log)")
}

func (h *SchedulerHandler) retentionNotRunning() (int, string) {
	lic, entitled := h.currentLicense(SchedulerLicense.CanUseRetentionScheduler)
	return schedulerNotRunning(lic, entitled,
		"License tier does not include retention scheduler feature",
		"Retention feature not enabled (retention.enabled=false), or the scheduler failed to start (see the startup log)")
}

// handleCQReload reloads all CQ schedules from the database
func (h *SchedulerHandler) handleCQReload(c *fiber.Ctx) error {
	if h.cqScheduler == nil {
		code, reason := h.cqNotRunning()
		h.logger.Warn().Str("reason", reason).Msg("CQ scheduler reload requested but the scheduler is not running")
		return c.Status(code).JSON(fiber.Map{
			"error": "CQ scheduler is not running: " + reason,
		})
	}

	if err := h.cqScheduler.ReloadAll(); err != nil {
		// Refused without touching the running jobs; the scheduler logged why.
		if errors.Is(err, ErrCQReloadUnlicensed) {
			return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
				"error": "CQ scheduler reload refused: " + err.Error(),
			})
		}
		h.logger.Error().Err(err).Msg("Failed to reload CQ scheduler")
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "Failed to reload CQ scheduler: " + err.Error(),
		})
	}

	h.logger.Info().Int("job_count", h.cqScheduler.JobCount()).Msg("CQ scheduler reloaded")

	return c.JSON(fiber.Map{
		"message":   "CQ scheduler reloaded successfully",
		"job_count": h.cqScheduler.JobCount(),
	})
}

// handleRetentionTrigger triggers retention execution immediately
func (h *SchedulerHandler) handleRetentionTrigger(c *fiber.Ctx) error {
	if h.retentionScheduler == nil {
		code, reason := h.retentionNotRunning()
		h.logger.Warn().Str("reason", reason).Msg("Retention trigger requested but the scheduler is not running")
		return c.Status(code).JSON(fiber.Map{
			"error": "Retention scheduler is not running: " + reason,
		})
	}

	if err := h.retentionScheduler.TriggerNow(c.Context()); err != nil {
		// A trigger refused without running anything is not a server failure;
		// the scheduler logged why.
		switch {
		case errors.Is(err, ErrRetentionTriggerRoleGated), errors.Is(err, ErrRetentionTriggerRunning):
			return c.Status(fiber.StatusConflict).JSON(fiber.Map{
				"error": "Retention trigger refused: " + err.Error(),
			})
		case errors.Is(err, ErrRetentionTriggerUnlicensed):
			return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
				"error": "Retention trigger refused: " + err.Error(),
			})
		}
		h.logger.Error().Err(err).Msg("Failed to trigger retention")
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "Retention trigger failed: " + err.Error(),
		})
	}

	h.logger.Info().Msg("Retention triggered manually")

	return c.JSON(fiber.Map{
		"message": "Retention triggered successfully",
	})
}
