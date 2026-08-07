package api

import (
	"context"
	"errors"
	"time"

	"github.com/basekick-labs/arc/internal/auth"
	"github.com/basekick-labs/arc/internal/edgesync"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// adminTimeout bounds a registry operation. Short: these are single-row SQLite
// writes, so anything slow is a symptom rather than a large payload.
const adminTimeout = 30 * time.Second

// EdgeSyncAdminHandler manages spoke registrations.
//
// Separate from EdgeSyncHandler because the two have different audiences and
// different authentication. The sync endpoints are called by spokes and
// authenticated per-spoke by HMAC; these are called by an operator and
// authenticated by an Arc admin token. Mounting them on one handler would
// invite someone to reuse the wrong middleware.
type EdgeSyncAdminHandler struct {
	registry    *edgesync.Registry
	authManager *auth.AuthManager
	logger      zerolog.Logger
}

// NewEdgeSyncAdminHandler validates configuration and returns a ready handler.
func NewEdgeSyncAdminHandler(registry *edgesync.Registry, authManager *auth.AuthManager, logger zerolog.Logger) (*EdgeSyncAdminHandler, error) {
	if registry == nil {
		return nil, errors.New("edgesync admin: registry is required")
	}
	return &EdgeSyncAdminHandler{
		registry:    registry,
		authManager: authManager,
		// Distinct component: logger.Get("edgesync") is shared with the sync
		// handler, so without this an admin action and a spoke transfer are
		// indistinguishable in the log.
		logger: logger.With().Str("component", "edgesync-admin").Logger(),
	}, nil
}

// RegisterRoutes mounts the spoke-management endpoints.
//
// Every route is admin-only, including the read paths: the spoke list is a map
// of which edge deployments exist and when each last reported in, which is not
// something a read-scoped token should be able to enumerate.
//
// Mounted at /api/v1/sync-spokes rather than under /api/v1/sync, deliberately.
//
// Nesting them made behavior depend on registration order: Fiber binds a
// group's middleware only to routes registered afterwards on that group, so
// with admin registered first (as main.go does) the sync group's body limit
// never ran here — but swapping two lines in main.go would silently start
// applying it, and a registration whose JSON exceeds that limit would then get
// a 413 from the WRONG group before this handler's auth ever ran. An earlier
// version of this comment claimed the opposite, having tested the reversed
// order. A sibling path removes the coupling entirely rather than documenting
// which order is safe.
func (h *EdgeSyncAdminHandler) RegisterRoutes(app fiber.Router) {
	group := app.Group("/api/v1/sync-spokes")

	if h.authManager != nil {
		group.Use(auth.RequireAdmin(h.authManager))
	}

	group.Post("/", h.register)
	group.Get("/", h.list)
	group.Get("/:spokeID", h.get)
	group.Post("/:spokeID/rotate", h.rotate)
	group.Post("/:spokeID/enable", h.enable)
	group.Post("/:spokeID/disable", h.disable)
	group.Delete("/:spokeID", h.delete)

	h.logger.Info().Msg("Edge sync spoke admin routes registered")
}

type registerSpokeRequest struct {
	SpokeID string `json:"spoke_id"`
	Name    string `json:"name"`
}

// register handles POST /api/v1/sync/spokes.
//
// The response carries the generated secret, and it is the ONLY time it is
// readable. An operator who loses it must rotate rather than retrieve.
func (h *EdgeSyncAdminHandler) register(c *fiber.Ctx) error {
	var req registerSpokeRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "malformed request body"})
	}

	ctx, cancel := context.WithTimeout(c.Context(), adminTimeout)
	defer cancel()

	secret, err := h.registry.Register(ctx, req.SpokeID, req.Name)
	if err != nil {
		if errors.Is(err, edgesync.ErrSpokeExists) {
			// 409 rather than 400: the request is well-formed, the ID is
			// taken. Re-registering is refused rather than treated as an
			// update, because reissuing a secret would lock out a live edge
			// box with no signal.
			return c.Status(fiber.StatusConflict).JSON(fiber.Map{"error": err.Error()})
		}
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
	}

	return c.Status(fiber.StatusCreated).JSON(fiber.Map{
		"spoke_id": req.SpokeID,
		"name":     req.Name,
		"secret":   secret,
		"warning":  "This secret is shown once and cannot be retrieved. Store it now; if it is lost, rotate.",
	})
}

// list handles GET /api/v1/sync/spokes.
func (h *EdgeSyncAdminHandler) list(c *fiber.Ctx) error {
	ctx, cancel := context.WithTimeout(c.Context(), adminTimeout)
	defer cancel()

	spokes, err := h.registry.List(ctx)
	if err != nil {
		h.logger.Error().Err(err).Msg("Failed to list spokes")
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "failed to list spokes"})
	}

	out := make([]fiber.Map, 0, len(spokes))
	for _, s := range spokes {
		out = append(out, spokeJSON(s))
	}
	return c.JSON(fiber.Map{"spokes": out})
}

// get handles GET /api/v1/sync/spokes/:spokeID.
func (h *EdgeSyncAdminHandler) get(c *fiber.Ctx) error {
	ctx, cancel := context.WithTimeout(c.Context(), adminTimeout)
	defer cancel()

	s, err := h.registry.Get(ctx, c.Params("spokeID"))
	if err != nil {
		return h.registryError(c, err)
	}
	return c.JSON(spokeJSON(s))
}

// rotate handles POST /api/v1/sync/spokes/:spokeID/rotate.
func (h *EdgeSyncAdminHandler) rotate(c *fiber.Ctx) error {
	spokeID := c.Params("spokeID")

	ctx, cancel := context.WithTimeout(c.Context(), adminTimeout)
	defer cancel()

	secret, err := h.registry.RotateSecret(ctx, spokeID)
	if err != nil {
		return h.registryError(c, err)
	}

	return c.JSON(fiber.Map{
		"spoke_id": spokeID,
		"secret":   secret,
		"warning":  "The previous secret no longer authenticates. Reconfigure the spoke before its next contact window.",
	})
}

// enable handles POST /api/v1/sync/spokes/:spokeID/enable.
func (h *EdgeSyncAdminHandler) enable(c *fiber.Ctx) error {
	return h.setEnabled(c, true)
}

// disable handles POST /api/v1/sync/spokes/:spokeID/disable.
//
// Reversible, unlike delete: the spoke's counters and history survive, and
// re-enabling does not require re-provisioning a secret.
func (h *EdgeSyncAdminHandler) disable(c *fiber.Ctx) error {
	return h.setEnabled(c, false)
}

func (h *EdgeSyncAdminHandler) setEnabled(c *fiber.Ctx, enabled bool) error {
	spokeID := c.Params("spokeID")

	ctx, cancel := context.WithTimeout(c.Context(), adminTimeout)
	defer cancel()

	if err := h.registry.SetEnabled(ctx, spokeID, enabled); err != nil {
		return h.registryError(c, err)
	}

	h.logger.Info().Str("spoke_id", spokeID).Bool("enabled", enabled).Msg("Changed spoke enabled state")
	return c.JSON(fiber.Map{"spoke_id": spokeID, "enabled": enabled})
}

// delete handles DELETE /api/v1/sync/spokes/:spokeID.
func (h *EdgeSyncAdminHandler) delete(c *fiber.Ctx) error {
	spokeID := c.Params("spokeID")

	ctx, cancel := context.WithTimeout(c.Context(), adminTimeout)
	defer cancel()

	if err := h.registry.Delete(ctx, spokeID); err != nil {
		return h.registryError(c, err)
	}

	return c.JSON(fiber.Map{
		"spoke_id": spokeID,
		"deleted":  true,
		"note":     "Files already received from this spoke are retained; delete them separately if you want the storage back.",
	})
}

// registryError maps a registry error to a status code.
func (h *EdgeSyncAdminHandler) registryError(c *fiber.Ctx, err error) error {
	if errors.Is(err, edgesync.ErrSpokeNotFound) {
		// A generic message: the internal error carries a package prefix that
		// belongs in logs, not in an API response.
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"error": "spoke not registered"})
	}
	h.logger.Error().Err(err).Msg("Spoke registry operation failed")
	return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "registry operation failed"})
}

// spokeJSON renders a spoke for an API response. There is no secret field, by
// construction — Spoke does not carry one.
func spokeJSON(s *edgesync.Spoke) fiber.Map {
	m := fiber.Map{
		"spoke_id":       s.SpokeID,
		"name":           s.Name,
		"enabled":        s.Enabled,
		"files_received": s.FilesReceived,
		"bytes_received": s.BytesReceived,
		"registered_at":  s.RegisteredAt,
	}
	if s.LastSeenAt != nil {
		m["last_seen_at"] = *s.LastSeenAt
	}
	return m
}
