package api

import (
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"

	"github.com/basekick-labs/arc/internal/auth"
	"github.com/basekick-labs/arc/internal/mqtt"
)

// MQTTSubscriptionHandler handles MQTT subscription REST API endpoints
type MQTTSubscriptionHandler struct {
	manager     mqtt.Manager
	authManager *auth.AuthManager
	logger      zerolog.Logger
}

// NewMQTTSubscriptionHandler creates a new MQTT subscription handler
func NewMQTTSubscriptionHandler(manager mqtt.Manager, authManager *auth.AuthManager, logger zerolog.Logger) *MQTTSubscriptionHandler {
	return &MQTTSubscriptionHandler{
		manager:     manager,
		authManager: authManager,
		logger:      logger.With().Str("component", "mqtt-subscription-api").Logger(),
	}
}

// RegisterRoutes registers the MQTT subscription API routes
func (h *MQTTSubscriptionHandler) RegisterRoutes(app *fiber.App) {
	subs := app.Group("/api/v1/mqtt/subscriptions")

	// Require admin authentication for all subscription management
	if h.authManager != nil {
		subs.Use(auth.RequireAdmin(h.authManager))
	}

	// CRUD endpoints
	subs.Post("/", h.handleCreate)
	subs.Get("/", h.handleList)
	subs.Get("/:id", h.handleGet)
	subs.Put("/:id", h.handleUpdate)
	subs.Delete("/:id", h.handleDelete)

	// Lifecycle endpoints
	subs.Post("/:id/start", h.handleStart)
	subs.Post("/:id/stop", h.handleStop)
	subs.Post("/:id/pause", h.handlePause)

	// Stats endpoint
	subs.Get("/:id/stats", h.handleStats)
}

// handleCreate creates a new subscription
func (h *MQTTSubscriptionHandler) handleCreate(c *fiber.Ctx) error {
	var req mqtt.CreateSubscriptionRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"success": false,
			"error":   "Invalid request body",
		})
	}

	// Extract password separately (not stored in request struct for security)
	var body struct {
		Password string `json:"password"`
	}
	c.BodyParser(&body)

	sub, err := h.manager.Create(c.Context(), &req, body.Password)
	if err != nil {
		h.logger.Error().Err(err).Str("name", req.Name).Msg("Failed to create subscription")
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"success": false,
			"error":   "Failed to create subscription",
		})
	}

	h.logger.Info().Str("id", sub.ID).Str("name", sub.Name).Msg("Created MQTT subscription")

	return c.Status(fiber.StatusCreated).JSON(fiber.Map{
		"success":      true,
		"subscription": sub,
	})
}

// handleList lists all subscriptions
func (h *MQTTSubscriptionHandler) handleList(c *fiber.Ctx) error {
	subscriptions, err := h.manager.List(c.Context())
	if err != nil {
		h.logger.Error().Err(err).Msg("Failed to list subscriptions")
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"success": false,
			"error":   "Failed to list subscriptions",
		})
	}

	return c.JSON(fiber.Map{
		"success":       true,
		"subscriptions": subscriptions,
		"count":         len(subscriptions),
	})
}

// handleGet retrieves a subscription by ID
func (h *MQTTSubscriptionHandler) handleGet(c *fiber.Ctx) error {
	id := c.Params("id")

	sub, err := h.manager.Get(c.Context(), id)
	if err != nil {
		h.logger.Debug().Err(err).Str("id", id).Msg("Subscription not found")
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
			"success": false,
			"error":   "Subscription not found",
		})
	}

	return c.JSON(fiber.Map{
		"success":      true,
		"subscription": sub,
	})
}

// handleUpdate updates a subscription
func (h *MQTTSubscriptionHandler) handleUpdate(c *fiber.Ctx) error {
	id := c.Params("id")

	var req mqtt.UpdateSubscriptionRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"success": false,
			"error":   "Invalid request body",
		})
	}

	sub, err := h.manager.Update(c.Context(), id, &req)
	if err != nil {
		h.logger.Error().Err(err).Str("id", id).Msg("Failed to update subscription")

		// Check for specific errors
		if err.Error() == "cannot update running subscription - stop it first" {
			return c.Status(fiber.StatusConflict).JSON(fiber.Map{
				"success": false,
				"error":   err.Error(),
			})
		}

		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"success": false,
			"error":   "Failed to update subscription",
		})
	}

	h.logger.Info().Str("id", id).Msg("Updated MQTT subscription")

	return c.JSON(fiber.Map{
		"success":      true,
		"subscription": sub,
	})
}

// handleDelete deletes a subscription
func (h *MQTTSubscriptionHandler) handleDelete(c *fiber.Ctx) error {
	id := c.Params("id")

	if err := h.manager.Delete(c.Context(), id); err != nil {
		h.logger.Error().Err(err).Str("id", id).Msg("Failed to delete subscription")
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"success": false,
			"error":   "Failed to delete subscription",
		})
	}

	h.logger.Info().Str("id", id).Msg("Deleted MQTT subscription")

	return c.JSON(fiber.Map{
		"success": true,
		"message": "Subscription deleted",
	})
}

// handleStart starts a subscription
func (h *MQTTSubscriptionHandler) handleStart(c *fiber.Ctx) error {
	id := c.Params("id")

	if err := h.manager.StartSubscription(c.Context(), id); err != nil {
		h.logger.Error().Err(err).Str("id", id).Msg("Failed to start subscription")

		if err.Error() == "subscription already running" {
			return c.Status(fiber.StatusConflict).JSON(fiber.Map{
				"success": false,
				"error":   err.Error(),
			})
		}

		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"success": false,
			"error":   "Failed to start subscription",
		})
	}

	h.logger.Info().Str("id", id).Msg("Started MQTT subscription")

	return c.JSON(fiber.Map{
		"success": true,
		"message": "Subscription started",
	})
}

// handleStop stops a subscription
func (h *MQTTSubscriptionHandler) handleStop(c *fiber.Ctx) error {
	id := c.Params("id")

	if err := h.manager.StopSubscription(c.Context(), id); err != nil {
		h.logger.Error().Err(err).Str("id", id).Msg("Failed to stop subscription")

		if err.Error() == "subscription not running" {
			return c.Status(fiber.StatusConflict).JSON(fiber.Map{
				"success": false,
				"error":   err.Error(),
			})
		}

		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"success": false,
			"error":   "Failed to stop subscription",
		})
	}

	h.logger.Info().Str("id", id).Msg("Stopped MQTT subscription")

	return c.JSON(fiber.Map{
		"success": true,
		"message": "Subscription stopped",
	})
}

// handlePause pauses a subscription
func (h *MQTTSubscriptionHandler) handlePause(c *fiber.Ctx) error {
	id := c.Params("id")

	if err := h.manager.PauseSubscription(c.Context(), id); err != nil {
		h.logger.Error().Err(err).Str("id", id).Msg("Failed to pause subscription")

		if err.Error() == "subscription not running" {
			return c.Status(fiber.StatusConflict).JSON(fiber.Map{
				"success": false,
				"error":   err.Error(),
			})
		}

		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"success": false,
			"error":   "Failed to pause subscription",
		})
	}

	h.logger.Info().Str("id", id).Msg("Paused MQTT subscription")

	return c.JSON(fiber.Map{
		"success": true,
		"message": "Subscription paused",
	})
}

// handleStats returns statistics for a subscription
func (h *MQTTSubscriptionHandler) handleStats(c *fiber.Ctx) error {
	id := c.Params("id")

	stats, err := h.manager.GetStats(c.Context(), id)
	if err != nil {
		h.logger.Error().Err(err).Str("id", id).Msg("Failed to get subscription stats")
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"success": false,
			"error":   "Failed to get subscription stats",
		})
	}

	return c.JSON(fiber.Map{
		"success": true,
		"stats":   stats,
	})
}
