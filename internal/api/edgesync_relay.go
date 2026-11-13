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

// EdgeSyncRelayHubHandler serves the hub side of the relay transport's
// manual trigger (#1685): POST /api/v1/bundle-import/relay/run polls the
// relay bucket once. Free, like a drive import; the scheduled poll needs the
// edge sync scheduler licence.
type EdgeSyncRelayHubHandler struct {
	poller      *edgesync.RelayPoller
	authManager *auth.AuthManager
	timeout     time.Duration
	logger      zerolog.Logger
}

// NewEdgeSyncRelayHubHandler returns the handler. timeout bounds one manual
// poll (edge_sync.import.timeout); non-positive means 4h.
func NewEdgeSyncRelayHubHandler(poller *edgesync.RelayPoller, authManager *auth.AuthManager, timeout time.Duration, logger zerolog.Logger) (*EdgeSyncRelayHubHandler, error) {
	if poller == nil {
		return nil, errors.New("edgesync relay: a poller is required")
	}
	if timeout <= 0 {
		timeout = 4 * time.Hour
	}
	return &EdgeSyncRelayHubHandler{
		poller: poller, authManager: authManager, timeout: timeout,
		logger: logger.With().Str("component", "edgesync-relay-hub").Logger(),
	}, nil
}

// RegisterRoutes mounts the route behind admin auth: a poll imports data.
func (h *EdgeSyncRelayHubHandler) RegisterRoutes(app fiber.Router) {
	handlers := []fiber.Handler{}
	if h.authManager != nil {
		handlers = append(handlers, auth.RequireAdmin(h.authManager))
	}
	handlers = append(handlers, h.run)
	app.Post("/api/v1/bundle-import/relay/run", handlers...)
}

func (h *EdgeSyncRelayHubHandler) run(c *fiber.Ctx) error {
	ctx, cancel := context.WithTimeout(c.Context(), h.timeout)
	defer cancel()
	res, err := h.poller.Run(ctx)
	if errors.Is(err, edgesync.ErrRelayInProgress) {
		return c.Status(fiber.StatusConflict).JSON(fiber.Map{"error": err.Error()})
	}
	if err != nil {
		h.logger.Error().Err(err).Msg("Manual relay poll failed")
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{"error": err.Error(), "result": res})
	}
	return c.JSON(res)
}
