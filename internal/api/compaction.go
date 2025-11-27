package api

import (
	"context"
	"strconv"
	"time"

	"github.com/basekick-labs/arc/internal/compaction"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// CompactionHandler handles compaction API endpoints
type CompactionHandler struct {
	manager   *compaction.Manager
	scheduler *compaction.Scheduler
	logger    zerolog.Logger
}

// NewCompactionHandler creates a new compaction handler
func NewCompactionHandler(manager *compaction.Manager, scheduler *compaction.Scheduler, logger zerolog.Logger) *CompactionHandler {
	return &CompactionHandler{
		manager:   manager,
		scheduler: scheduler,
		logger:    logger.With().Str("component", "compaction-handler").Logger(),
	}
}

// RegisterRoutes registers compaction endpoints
func (h *CompactionHandler) RegisterRoutes(app *fiber.App) {
	app.Get("/api/v1/compaction/status", h.getStatus)
	app.Get("/api/v1/compaction/stats", h.getStats)
	app.Get("/api/v1/compaction/candidates", h.getCandidates)
	app.Post("/api/v1/compaction/trigger", h.triggerCompaction)
	app.Get("/api/v1/compaction/jobs", h.getActiveJobs)
	app.Get("/api/v1/compaction/history", h.getHistory)

	h.logger.Info().Msg("Compaction routes registered")
}

// getStatus handles GET /api/v1/compaction/status
func (h *CompactionHandler) getStatus(c *fiber.Ctx) error {
	if h.scheduler == nil {
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
			"error": "Compaction not initialized",
		})
	}

	stats := h.manager.Stats()
	return c.JSON(fiber.Map{
		"scheduler": h.scheduler.Status(),
		"manager": fiber.Map{
			"active_jobs":     stats["active_jobs"],
			"total_completed": stats["total_jobs_completed"],
			"total_failed":    stats["total_jobs_failed"],
		},
	})
}

// getStats handles GET /api/v1/compaction/stats
func (h *CompactionHandler) getStats(c *fiber.Ctx) error {
	if h.manager == nil {
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
			"error": "Compaction not initialized",
		})
	}

	return c.JSON(h.manager.Stats())
}

// getCandidates handles GET /api/v1/compaction/candidates
func (h *CompactionHandler) getCandidates(c *fiber.Ctx) error {
	if h.manager == nil {
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
			"error": "Compaction not initialized",
		})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	candidates, err := h.manager.FindCandidates(ctx)
	if err != nil {
		h.logger.Error().Err(err).Msg("Failed to find compaction candidates")
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "Failed to find candidates: " + err.Error(),
		})
	}

	// Convert candidates to JSON-friendly format
	candidateList := make([]fiber.Map, len(candidates))
	for i, cand := range candidates {
		candidateList[i] = fiber.Map{
			"database":       cand.Database,
			"measurement":    cand.Measurement,
			"partition_path": cand.PartitionPath,
			"file_count":     cand.FileCount,
			"tier":           cand.Tier,
		}
	}

	return c.JSON(fiber.Map{
		"count":      len(candidates),
		"candidates": candidateList,
	})
}

// triggerCompaction handles POST /api/v1/compaction/trigger
func (h *CompactionHandler) triggerCompaction(c *fiber.Ctx) error {
	if h.scheduler == nil || h.manager == nil {
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
			"error": "Compaction not initialized",
		})
	}

	h.logger.Info().Msg("Manual compaction triggered via API")

	// Check if a cycle is already running
	if h.manager.IsCycleRunning() {
		return c.Status(fiber.StatusConflict).JSON(fiber.Map{
			"error":     "Compaction cycle already running",
			"message":   "A compaction cycle is already in progress. Please wait for it to complete.",
			"cycle_id":  h.manager.GetCurrentCycleID(),
			"is_running": true,
		})
	}

	// Trigger compaction asynchronously
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
		defer cancel()

		start := time.Now()
		cycleID, err := h.manager.RunCompactionCycle(ctx)
		duration := time.Since(start)

		if err != nil {
			h.logger.Error().Err(err).Int64("cycle_id", cycleID).Dur("duration", duration).Msg("Manual compaction failed")
		} else {
			h.logger.Info().Int64("cycle_id", cycleID).Dur("duration", duration).Msg("Manual compaction completed")
		}
	}()

	return c.JSON(fiber.Map{
		"message":  "Compaction triggered",
		"status":   "running",
		"cycle_id": h.manager.GetCurrentCycleID() + 1, // Next cycle ID that will be assigned
	})
}

// getActiveJobs handles GET /api/v1/compaction/jobs
func (h *CompactionHandler) getActiveJobs(c *fiber.Ctx) error {
	if h.manager == nil {
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
			"error": "Compaction not initialized",
		})
	}

	stats := h.manager.Stats()
	activeJobCount := 0
	if count, ok := stats["active_jobs"].(int); ok {
		activeJobCount = count
	}

	return c.JSON(fiber.Map{
		"active_jobs": activeJobCount,
		"jobs":        []fiber.Map{}, // TODO: Return actual job details when available
	})
}

// getHistory handles GET /api/v1/compaction/history
func (h *CompactionHandler) getHistory(c *fiber.Ctx) error {
	if h.manager == nil {
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
			"error": "Compaction not initialized",
		})
	}

	// Get limit from query param
	limitStr := c.Query("limit", "10")
	limit, err := strconv.Atoi(limitStr)
	if err != nil || limit < 1 {
		limit = 10
	}

	stats := h.manager.Stats()
	recentJobs := []interface{}{}
	if jobs, ok := stats["recent_jobs"].([]map[string]interface{}); ok {
		// Return at most 'limit' jobs
		startIdx := 0
		if len(jobs) > limit {
			startIdx = len(jobs) - limit
		}
		for _, job := range jobs[startIdx:] {
			recentJobs = append(recentJobs, job)
		}
	}

	return c.JSON(fiber.Map{
		"total_jobs":  stats["total_jobs_completed"],
		"recent_jobs": recentJobs,
	})
}
