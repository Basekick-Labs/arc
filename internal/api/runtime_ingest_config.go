package api

import (
	"fmt"
	"sync"

	"github.com/basekick-labs/arc/internal/auth"
	"github.com/basekick-labs/arc/internal/ingest"
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
	store       *ingest.RuntimeIngestConfigStore
	startup     ingest.RuntimeIngestConfig
	runtimeOnly bool
	authManager *auth.AuthManager
	logger      zerolog.Logger
	mutationMu  sync.Mutex
}

// NewRuntimeIngestConfigHandler creates the runtime ingest configuration API.
func NewRuntimeIngestConfigHandler(buffer IngestRuntimeConfig, store *ingest.RuntimeIngestConfigStore, startup ingest.RuntimeIngestConfig, authManager *auth.AuthManager, logger zerolog.Logger) *RuntimeIngestConfigHandler {
	return &RuntimeIngestConfigHandler{
		buffer:      buffer,
		store:       store,
		startup:     startup,
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
		app.Delete(path, auth.RequireAdmin(h.authManager), h.handleDelete)
		return
	}
	app.Get(path, h.handleGet)
	app.Patch(path, h.handlePatch)
	app.Delete(path, h.handleDelete)
}

type runtimeIngestConfigResponse struct {
	MaxBufferSize  int    `json:"max_buffer_size"`
	MaxBufferAgeMS int    `json:"max_buffer_age_ms"`
	Scope          string `json:"scope"`
	Persistent     bool   `json:"persistent"`
	Source         string `json:"source"`
}

func (h *RuntimeIngestConfigHandler) handleGet(c *fiber.Ctx) error {
	h.mutationMu.Lock()
	defer h.mutationMu.Unlock()
	if h.buffer == nil || h.store == nil {
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{"error": "runtime ingest configuration is unavailable"})
	}
	maxSize, maxAge := h.buffer.RuntimeConfig()
	_, hasPersistentOverride, err := h.store.Load()
	if err != nil {
		h.logger.Error().Err(err).Msg("Could not read persisted runtime ingest configuration")
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "could not read runtime ingest configuration"})
	}
	persistent := hasPersistentOverride && !h.runtimeOnly
	source := "startup_config"
	if h.runtimeOnly {
		source = "runtime_override"
	} else if persistent {
		source = "persistent_override"
	}
	return c.JSON(runtimeIngestConfigResponse{
		MaxBufferSize:  maxSize,
		MaxBufferAgeMS: maxAge,
		Scope:          "current_process",
		Persistent:     persistent,
		Source:         source,
	})
}

type runtimeIngestConfigPatch struct {
	MaxBufferSize  *int  `json:"max_buffer_size"`
	MaxBufferAgeMS *int  `json:"max_buffer_age_ms"`
	Persistent     *bool `json:"persistent"`
}

func (h *RuntimeIngestConfigHandler) handlePatch(c *fiber.Ctx) error {
	h.mutationMu.Lock()
	defer h.mutationMu.Unlock()
	if h.buffer == nil || h.store == nil {
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{"error": "runtime ingest configuration is unavailable"})
	}
	var patch runtimeIngestConfigPatch
	if err := c.BodyParser(&patch); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid JSON body"})
	}
	persist := false
	if patch.Persistent != nil {
		persist = *patch.Persistent
	}
	if patch.MaxBufferSize == nil && patch.MaxBufferAgeMS == nil && !persist {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "provide max_buffer_size and/or max_buffer_age_ms"})
	}

	oldSize, oldAge := h.buffer.RuntimeConfig()
	size, age := oldSize, oldAge
	if patch.MaxBufferSize != nil {
		size = *patch.MaxBufferSize
	}
	if patch.MaxBufferAgeMS != nil {
		age = *patch.MaxBufferAgeMS
	}
	if err := h.buffer.PatchRuntimeConfig(&size, &age); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
	}
	if persist {
		if storeErr := h.store.Save(ingest.RuntimeIngestConfig{MaxBufferSize: size, MaxBufferAgeMS: age}); storeErr != nil {
			rollbackErr := h.buffer.PatchRuntimeConfig(&oldSize, &oldAge)
			h.logger.Error().Err(storeErr).Interface("rollback_error", rollbackErr).Msg("Could not save runtime ingest settings; reverted live settings")
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "could not persist runtime ingest configuration"})
		}
	}
	h.runtimeOnly = !persist
	maxSize, maxAge := h.buffer.RuntimeConfig()
	source := "runtime_override"
	if persist {
		source = "persistent_override"
	}

	h.logger.Info().Int("max_buffer_size", maxSize).Int("max_buffer_age_ms", maxAge).Bool("persistent", persist).Msg("Updated live ingest buffer settings")
	return c.JSON(runtimeIngestConfigResponse{
		MaxBufferSize:  maxSize,
		MaxBufferAgeMS: maxAge,
		Scope:          "current_process",
		Persistent:     persist,
		Source:         source,
	})
}

func (h *RuntimeIngestConfigHandler) handleDelete(c *fiber.Ctx) error {
	h.mutationMu.Lock()
	defer h.mutationMu.Unlock()
	if h.buffer == nil || h.store == nil {
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{"error": "runtime ingest configuration is unavailable"})
	}
	oldSize, oldAge := h.buffer.RuntimeConfig()
	if err := h.buffer.PatchRuntimeConfig(&h.startup.MaxBufferSize, &h.startup.MaxBufferAgeMS); err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": fmt.Sprintf("could not restore startup ingest configuration: %v", err)})
	}
	if err := h.store.Delete(); err != nil {
		rollbackErr := h.buffer.PatchRuntimeConfig(&oldSize, &oldAge)
		h.logger.Error().Err(err).Interface("rollback_error", rollbackErr).Msg("Could not clear persisted runtime ingest settings; restored live settings")
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "could not clear persisted runtime ingest configuration"})
	}
	h.runtimeOnly = false
	h.logger.Info().Int("max_buffer_size", h.startup.MaxBufferSize).Int("max_buffer_age_ms", h.startup.MaxBufferAgeMS).Msg("Restored startup ingest buffer settings")
	return c.JSON(runtimeIngestConfigResponse{
		MaxBufferSize:  h.startup.MaxBufferSize,
		MaxBufferAgeMS: h.startup.MaxBufferAgeMS,
		Scope:          "current_process",
		Persistent:     false,
		Source:         "startup_config",
	})
}
