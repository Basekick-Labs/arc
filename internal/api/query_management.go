package api

import (
	"strconv"

	"github.com/basekick-labs/arc/internal/auth"
	"github.com/basekick-labs/arc/internal/license"
	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/basekick-labs/arc/internal/queryregistry"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// QueryManagementHandler handles query management API operations.
type QueryManagementHandler struct {
	registry      *queryregistry.Registry
	authManager   *auth.AuthManager
	licenseClient *license.Client
	logger        zerolog.Logger
}

// NewQueryManagementHandler creates a new query management handler.
func NewQueryManagementHandler(registry *queryregistry.Registry, authManager *auth.AuthManager, licenseClient *license.Client, logger zerolog.Logger) *QueryManagementHandler {
	return &QueryManagementHandler{
		registry:      registry,
		authManager:   authManager,
		licenseClient: licenseClient,
		logger:        logger.With().Str("component", "query-mgmt-api").Logger(),
	}
}

// RegisterRoutes registers query management API routes.
func (h *QueryManagementHandler) RegisterRoutes(app fiber.Router) {
	group := app.Group("/api/v1/queries")
	if h.authManager != nil {
		group.Use(auth.RequireAdmin(h.authManager))
	}
	group.Use(h.requireQueryManagementLicense)

	group.Get("/active", h.listActiveQueries)
	group.Get("/history", h.listQueryHistory)
	group.Get("/:id", h.getQuery)
	group.Delete("/:id", h.cancelQuery)
}

// requireQueryManagementLicense checks that the license includes the query management feature.
func (h *QueryManagementHandler) requireQueryManagementLicense(c *fiber.Ctx) error {
	if h.licenseClient == nil || !h.licenseClient.CanUseQueryManagement() {
		return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
			"success": false,
			"error":   "Query management requires an enterprise license with the 'query_management' feature enabled",
		})
	}
	return c.Next()
}

// listActiveQueries returns all currently running queries.
func (h *QueryManagementHandler) listActiveQueries(c *fiber.Ctx) error {
	active := h.registry.GetActive()

	// Truncate SQL for listing view
	type querySummary struct {
		ID             string  `json:"id"`
		SQL            string  `json:"sql"`
		TokenID        int64   `json:"token_id,omitempty"`
		TokenName      string  `json:"token_name,omitempty"`
		RemoteAddr     string  `json:"remote_addr,omitempty"`
		Status         string  `json:"status"`
		StartTime      string  `json:"start_time"`
		DurationMs     float64 `json:"duration_ms"`
		IsParallel     bool    `json:"is_parallel"`
		PartitionCount int     `json:"partition_count,omitempty"`
	}

	queries := make([]querySummary, 0, len(active))
	for _, q := range active {
		sql := q.SQL
		if len(sql) > 200 {
			sql = sql[:200] + "..."
		}
		queries = append(queries, querySummary{
			ID:             q.ID,
			SQL:            sql,
			TokenID:        q.TokenID,
			TokenName:      q.TokenName,
			RemoteAddr:     q.RemoteAddr,
			Status:         string(q.Status),
			StartTime:      q.StartTime.UTC().Format("2006-01-02T15:04:05Z"),
			DurationMs:     q.DurationMs,
			IsParallel:     q.IsParallel,
			PartitionCount: q.PartitionCount,
		})
	}

	// Update metrics
	metrics.Get().SetQueryMgmtActiveQueries(int64(len(active)))

	return c.JSON(fiber.Map{
		"success": true,
		"queries": queries,
		"count":   len(queries),
	})
}

// listQueryHistory returns recently completed queries.
func (h *QueryManagementHandler) listQueryHistory(c *fiber.Ctx) error {
	limit := 50
	if l := c.Query("limit"); l != "" {
		if parsed, err := strconv.Atoi(l); err == nil && parsed > 0 {
			limit = parsed
			if limit > 1000 {
				limit = 1000
			}
		}
	}

	history := h.registry.GetHistory(limit)

	// Update metrics
	metrics.Get().SetQueryMgmtHistorySize(int64(h.registry.HistoryLen()))

	return c.JSON(fiber.Map{
		"success": true,
		"queries": history,
		"count":   len(history),
	})
}

// getQuery returns details for a specific query by ID.
func (h *QueryManagementHandler) getQuery(c *fiber.Ctx) error {
	queryID := c.Params("id")
	if queryID == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"success": false,
			"error":   "Query ID is required",
		})
	}

	q := h.registry.GetQuery(queryID)
	if q == nil {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
			"success": false,
			"error":   "Query not found",
		})
	}

	return c.JSON(fiber.Map{
		"success": true,
		"query":   q,
	})
}

// cancelQuery cancels a running query by ID.
func (h *QueryManagementHandler) cancelQuery(c *fiber.Ctx) error {
	queryID := c.Params("id")
	if queryID == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"success": false,
			"error":   "Query ID is required",
		})
	}

	ok := h.registry.Cancel(queryID)
	if !ok {
		// Check if query exists in history (already completed)
		q := h.registry.GetQuery(queryID)
		if q != nil {
			return c.Status(fiber.StatusConflict).JSON(fiber.Map{
				"success": false,
				"error":   "Query already " + string(q.Status),
			})
		}
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
			"success": false,
			"error":   "Query not found",
		})
	}

	metrics.Get().IncQueryMgmtCancelled()

	h.logger.Info().
		Str("query_id", queryID).
		Msg("Query cancelled via API")

	return c.JSON(fiber.Map{
		"success": true,
		"message": "Query cancelled",
	})
}
