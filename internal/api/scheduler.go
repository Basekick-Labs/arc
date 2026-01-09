package api

import (
	"context"

	"github.com/basekick-labs/arc/internal/license"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// CQSchedulerInterface defines the interface for CQ scheduler operations
type CQSchedulerInterface interface {
	Status() map[string]interface{}
	ReloadAll() error
	JobCount() int
}

// RetentionSchedulerInterface defines the interface for retention scheduler operations
type RetentionSchedulerInterface interface {
	Status() map[string]interface{}
	TriggerNow(ctx context.Context) error
}

// SchedulerHandler handles scheduler status API endpoints
type SchedulerHandler struct {
	cqScheduler        CQSchedulerInterface
	retentionScheduler RetentionSchedulerInterface
	licenseClient      *license.Client
	logger             zerolog.Logger
}

// NewSchedulerHandler creates a new scheduler handler
func NewSchedulerHandler(
	cqScheduler CQSchedulerInterface,
	retentionScheduler RetentionSchedulerInterface,
	licenseClient *license.Client,
	logger zerolog.Logger,
) *SchedulerHandler {
	return &SchedulerHandler{
		cqScheduler:        cqScheduler,
		retentionScheduler: retentionScheduler,
		licenseClient:      licenseClient,
		logger:             logger.With().Str("component", "scheduler-handler").Logger(),
	}
}

// RegisterRoutes registers scheduler API routes
func (h *SchedulerHandler) RegisterRoutes(app *fiber.App) {
	app.Get("/api/v1/schedulers", h.handleGetStatus)
	app.Post("/api/v1/schedulers/cq/reload", h.handleCQReload)
	app.Post("/api/v1/schedulers/retention/trigger", h.handleRetentionTrigger)
}

// handleGetStatus returns the status of all schedulers
func (h *SchedulerHandler) handleGetStatus(c *fiber.Ctx) error {
	status := make(map[string]interface{})

	// Check license status first to determine appropriate messages
	hasValidLicense := false
	canUseCQScheduler := false
	canUseRetentionScheduler := false

	if h.licenseClient != nil {
		lic := h.licenseClient.GetLicense()
		if lic != nil && lic.IsValid() {
			hasValidLicense = true
			canUseCQScheduler = h.licenseClient.CanUseCQScheduler()
			canUseRetentionScheduler = h.licenseClient.CanUseRetentionScheduler()
		}
	}

	// CQ Scheduler status
	if h.cqScheduler != nil {
		status["cq_scheduler"] = h.cqScheduler.Status()
	} else {
		cqStatus := map[string]interface{}{
			"enabled": false,
		}
		if !hasValidLicense {
			cqStatus["reason"] = "Enterprise license required"
		} else if !canUseCQScheduler {
			cqStatus["reason"] = "License tier does not include CQ scheduler feature"
		} else {
			cqStatus["reason"] = "CQ feature not enabled (continuous_query.enabled=false)"
		}
		status["cq_scheduler"] = cqStatus
	}

	// Retention Scheduler status
	if h.retentionScheduler != nil {
		status["retention_scheduler"] = h.retentionScheduler.Status()
	} else {
		retentionStatus := map[string]interface{}{
			"enabled": false,
		}
		if !hasValidLicense {
			retentionStatus["reason"] = "Enterprise license required"
		} else if !canUseRetentionScheduler {
			retentionStatus["reason"] = "License tier does not include retention scheduler feature"
		} else {
			retentionStatus["reason"] = "Retention feature not enabled (retention.enabled=false)"
		}
		status["retention_scheduler"] = retentionStatus
	}

	return c.JSON(status)
}

// handleCQReload reloads all CQ schedules from the database
func (h *SchedulerHandler) handleCQReload(c *fiber.Ctx) error {
	if h.cqScheduler == nil {
		h.logger.Warn().Msg("CQ scheduler reload requested but scheduler not running - enterprise license required")
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
			"error": "CQ scheduler is not running - enterprise license required",
		})
	}

	if err := h.cqScheduler.ReloadAll(); err != nil {
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
		h.logger.Warn().Msg("Retention trigger requested but scheduler not running - enterprise license required")
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
			"error": "Retention scheduler is not running - enterprise license required",
		})
	}

	if err := h.retentionScheduler.TriggerNow(c.Context()); err != nil {
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
