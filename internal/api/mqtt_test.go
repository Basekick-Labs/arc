package api

import (
	"encoding/json"
	"io"
	"net/http/httptest"
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// TestMQTTHandler_DisabledManager verifies the nil-manager guards in
// handleStats / handleHealth return the documented body shapes when MQTT is
// disabled at wiring time. This branch is load-bearing — if a future refactor
// drops it, the handler will panic.
func TestMQTTHandler_DisabledManager(t *testing.T) {
	h := &MQTTHandler{
		manager: nil,
		logger:  zerolog.Nop(),
	}

	app := fiber.New()
	app.Get("/api/v1/mqtt/stats", h.handleStats)
	app.Get("/api/v1/mqtt/health", h.handleHealth)

	t.Run("stats_returns_503", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/api/v1/mqtt/stats", nil)
		resp, err := app.Test(req)
		if err != nil {
			t.Fatalf("app.Test: %v", err)
		}
		if resp.StatusCode != fiber.StatusServiceUnavailable {
			t.Errorf("status: got %d, want 503", resp.StatusCode)
		}
		body, _ := io.ReadAll(resp.Body)
		var got map[string]any
		if err := json.Unmarshal(body, &got); err != nil {
			t.Fatalf("json.Unmarshal: %v (body=%s)", err, body)
		}
		if got["success"] != false {
			t.Errorf("success: got %v, want false", got["success"])
		}
		if got["error"] != "MQTT subsystem disabled" {
			t.Errorf("error: got %q, want %q", got["error"], "MQTT subsystem disabled")
		}
	})

	t.Run("health_returns_200_disabled", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/api/v1/mqtt/health", nil)
		resp, err := app.Test(req)
		if err != nil {
			t.Fatalf("app.Test: %v", err)
		}
		if resp.StatusCode != fiber.StatusOK {
			t.Errorf("status: got %d, want 200 (disabled is a steady state)", resp.StatusCode)
		}
		body, _ := io.ReadAll(resp.Body)
		var got map[string]any
		if err := json.Unmarshal(body, &got); err != nil {
			t.Fatalf("json.Unmarshal: %v (body=%s)", err, body)
		}
		if got["status"] != "disabled" {
			t.Errorf("status: got %v, want %q", got["status"], "disabled")
		}
		if got["healthy"] != false {
			t.Errorf("healthy: got %v, want false", got["healthy"])
		}
	})
}
