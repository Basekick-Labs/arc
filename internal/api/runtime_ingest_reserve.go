package api

import (
	"sync"

	"github.com/basekick-labs/arc/internal/auth"
	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

type IngestElasticReserve interface {
	ElasticReserveConfig() ingest.RuntimeElasticReserveConfig
	ElasticReserveUsedRecords() int64
	ConfigureElasticReserve(ingest.RuntimeElasticReserveConfig) error
	ConfigureElasticReserveWithPersistence(ingest.RuntimeElasticReserveConfig, func() error) error
}

// RuntimeIngestReserveHandler owns the reserve's independent API and
// persistence lifecycle so resetting buffer thresholds cannot clear it.
type RuntimeIngestReserveHandler struct {
	buffer      IngestElasticReserve
	store       *ingest.RuntimeIngestConfigStore
	authManager *auth.AuthManager
	logger      zerolog.Logger
	mutationMu  sync.Mutex
	runtimeOnly bool
}

func NewRuntimeIngestReserveHandler(buffer IngestElasticReserve, store *ingest.RuntimeIngestConfigStore, authManager *auth.AuthManager, logger zerolog.Logger) *RuntimeIngestReserveHandler {
	return &RuntimeIngestReserveHandler{
		buffer:      buffer,
		store:       store,
		authManager: authManager,
		logger:      logger.With().Str("component", "runtime-ingest-reserve").Logger(),
	}
}

func (h *RuntimeIngestReserveHandler) RegisterRoutes(app *fiber.App) {
	path := "/api/v1/config/runtime/ingest/elastic-reserve"
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

type runtimeIngestReserveResponse struct {
	Enabled         bool   `json:"enabled"`
	CapacityRecords int64  `json:"capacity_records"`
	UsedRecords     int64  `json:"used_records"`
	Persistent      bool   `json:"persistent"`
	Source          string `json:"source"`
	Scope           string `json:"scope"`
}

func (h *RuntimeIngestReserveHandler) handleGet(c *fiber.Ctx) error {
	h.mutationMu.Lock()
	defer h.mutationMu.Unlock()
	if h.buffer == nil || h.store == nil {
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{"error": "runtime ingest reserve is unavailable"})
	}
	cfg := h.buffer.ElasticReserveConfig()
	_, persistent, err := h.store.LoadElasticReserve()
	if err != nil {
		h.logger.Error().Err(err).Msg("Could not read persisted runtime ingest reserve")
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "could not read runtime ingest reserve"})
	}
	source := "startup_default"
	if persistent {
		source = "persistent_override"
	} else if h.runtimeOnly {
		source = "runtime_override"
	}
	return c.JSON(runtimeIngestReserveResponse{
		Enabled:         cfg.Enabled,
		CapacityRecords: cfg.CapacityRecords,
		UsedRecords:     h.buffer.ElasticReserveUsedRecords(),
		Persistent:      persistent,
		Source:          source,
		Scope:           "current_process",
	})
}

type runtimeIngestReservePatch struct {
	Enabled         *bool  `json:"enabled"`
	CapacityRecords *int64 `json:"capacity_records"`
	Persistent      *bool  `json:"persistent"`
}

func (h *RuntimeIngestReserveHandler) handlePatch(c *fiber.Ctx) error {
	h.mutationMu.Lock()
	defer h.mutationMu.Unlock()
	if h.buffer == nil || h.store == nil {
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{"error": "runtime ingest reserve is unavailable"})
	}
	var patch runtimeIngestReservePatch
	if err := c.BodyParser(&patch); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid JSON body"})
	}
	if patch.Enabled == nil && patch.CapacityRecords == nil && patch.Persistent == nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "at least one of enabled, capacity_records, or persistent is required"})
	}
	_, wasPersistent, err := h.store.LoadElasticReserve()
	if err != nil {
		h.logger.Error().Err(err).Msg("Could not read runtime ingest reserve persistence state")
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "could not read runtime ingest reserve"})
	}
	next := h.buffer.ElasticReserveConfig()
	if patch.Enabled != nil {
		next.Enabled = *patch.Enabled
	}
	if patch.CapacityRecords != nil {
		next.CapacityRecords = *patch.CapacityRecords
	}
	persist := wasPersistent
	if patch.Persistent != nil {
		persist = *patch.Persistent
	}
	var persistErr error
	configureErr := h.buffer.ConfigureElasticReserveWithPersistence(next, func() error {
		if persist {
			persistErr = h.store.SaveElasticReserve(next)
		} else {
			persistErr = h.store.DeleteElasticReserve()
		}
		return persistErr
	})
	if configureErr != nil {
		if persistErr != nil {
			h.logger.Error().Err(persistErr).Msg("Could not persist runtime ingest reserve; live settings were not changed")
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "could not persist runtime ingest reserve"})
		}
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": configureErr.Error()})
	}
	h.runtimeOnly = !persist
	return h.reserveResponse(c, next, persist, "")
}

func (h *RuntimeIngestReserveHandler) handleDelete(c *fiber.Ctx) error {
	h.mutationMu.Lock()
	defer h.mutationMu.Unlock()
	if h.buffer == nil || h.store == nil {
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{"error": "runtime ingest reserve is unavailable"})
	}
	next := ingest.RuntimeElasticReserveConfig{}
	var persistErr error
	configureErr := h.buffer.ConfigureElasticReserveWithPersistence(next, func() error {
		persistErr = h.store.DeleteElasticReserve()
		return persistErr
	})
	if configureErr != nil {
		if persistErr != nil {
			h.logger.Error().Err(persistErr).Msg("Could not clear runtime ingest reserve; live settings were not changed")
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "could not clear runtime ingest reserve"})
		}
		return c.Status(fiber.StatusConflict).JSON(fiber.Map{"error": configureErr.Error()})
	}
	h.runtimeOnly = false
	return h.reserveResponse(c, next, false, "startup_default")
}

func (h *RuntimeIngestReserveHandler) reserveResponse(c *fiber.Ctx, cfg ingest.RuntimeElasticReserveConfig, persistent bool, source string) error {
	if source == "" {
		source = "runtime_override"
		if persistent {
			source = "persistent_override"
		}
	}
	return c.JSON(runtimeIngestReserveResponse{
		Enabled:         cfg.Enabled,
		CapacityRecords: cfg.CapacityRecords,
		UsedRecords:     h.buffer.ElasticReserveUsedRecords(),
		Persistent:      persistent,
		Source:          source,
		Scope:           "current_process",
	})
}
