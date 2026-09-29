package api

import (
	"github.com/basekick-labs/arc/internal/auth"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// IngestRuntimeConfig exposes the ingest buffer thresholds that can be
// changed safely while the process is running.
type IngestRuntimeConfig interface {
	RuntimeConfig() (maxBufferSize int, maxBufferAgeMS int)
	PatchRuntimeConfig(maxBufferSize, maxBufferAgeMS *int) error
}

// RuntimeIngestConfigHandler serves process-local ingest buffer settings.
type RuntimeIngestConfigHandler struct {
	buffer      IngestRuntimeConfig
	authManager *auth.AuthManager
	logger      zerolog.Logger
}

// NewRuntimeIngestConfigHandler creates the runtime ingest configuration API.
func NewRuntimeIngestConfigHandler(buffer IngestRuntimeConfig, authManager *auth.AuthManager, logger zerolog.Logger) *RuntimeIngestConfigHandler {
	return &RuntimeIngestConfigHandler{
		buffer:      buffer,
		authManager: authManager,
		logger:      logger.With().Str("component", "runtime-ingest-config").Logger(),
	}
}

// RegisterRoutes registers admin-only runtime ingest configuration endpoints.
func (h *RuntimeIngestConfigHandler) RegisterRoutes(app *fiber.App) {
	path := "/api/v1/config/runtime/ingest"
	if h.authManager != nil {
		app.Get(path, auth.RequireAdmin(h.authManager), h.handleGet)
		app.Patch(path, auth.RequireAdmin(h.authManager), h.handlePatch)
		return
	}
	app.Get(path, h.handleGet)
	app.Patch(path, h.handlePatch)
}

type runtimeIngestConfigResponse struct {
	MaxBufferSize  int    `json:"max_buffer_size"`
	MaxBufferAgeMS int    `json:"max_buffer_age_ms"`
	Scope          string `json:"scope"`
}

func (h *RuntimeIngestConfigHandler) handleGet(c *fiber.Ctx) error {
	if h.buffer == nil {
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{"error": "ingest buffer is unavailable"})
	}
	maxSize, maxAge := h.buffer.RuntimeConfig()
	return c.JSON(runtimeIngestConfigResponse{
		MaxBufferSize:  maxSize,
		MaxBufferAgeMS: maxAge,
		Scope:          "current_process",
	})
}

type runtimeIngestConfigPatch struct {
	MaxBufferSize  *int `json:"max_buffer_size"`
	MaxBufferAgeMS *int `json:"max_buffer_age_ms"`
}

func (h *RuntimeIngestConfigHandler) handlePatch(c *fiber.Ctx) error {
	if h.buffer == nil {
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{"error": "ingest buffer is unavailable"})
	}
	var patch runtimeIngestConfigPatch
	if err := c.BodyParser(&patch); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid JSON body"})
	}
	if patch.MaxBufferSize == nil && patch.MaxBufferAgeMS == nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "provide max_buffer_size and/or max_buffer_age_ms"})
	}

	if err := h.buffer.PatchRuntimeConfig(patch.MaxBufferSize, patch.MaxBufferAgeMS); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
	}
	maxSize, maxAge := h.buffer.RuntimeConfig()

	h.logger.Info().Int("max_buffer_size", maxSize).Int("max_buffer_age_ms", maxAge).Msg("Updated live ingest buffer settings")
	return c.JSON(runtimeIngestConfigResponse{
		MaxBufferSize:  maxSize,
		MaxBufferAgeMS: maxAge,
		Scope:          "current_process",
	})
}
