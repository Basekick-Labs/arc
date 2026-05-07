package api

import (
	"encoding/json"
	"io"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// TestMQTTSubscriptionHandler_DisabledManager verifies that every CRUD,
// lifecycle, and stats endpoint short-circuits with 503 when the MQTT subsystem
// is disabled at wiring time (manager is nil). Mirrors the MQTTHandler
// nil-guard policy so all MQTT API endpoints have one consistent
// disabled-response shape.
//
// Protection is applied as Fiber middleware (subs.Use(h.requireEnabled) in
// RegisterRoutes), so any new route added to the /api/v1/mqtt/subscriptions
// group is automatically guarded — no per-handler boilerplate to forget. The
// representative sample below exercises the shared middleware path across
// CRUD, lifecycle, and stats endpoints.
func TestMQTTSubscriptionHandler_DisabledManager(t *testing.T) {
	h := &MQTTSubscriptionHandler{
		manager: nil,
		logger:  zerolog.Nop(),
	}

	app := fiber.New()
	h.RegisterRoutes(app)

	// One representative route per category: CRUD list, CRUD get-by-id,
	// lifecycle action, stats. The helper is shared across all 10 handlers,
	// so coverage of these four exercises the same code path the others use.
	cases := []struct {
		name   string
		method string
		path   string
		body   string
	}{
		{"list", "GET", "/api/v1/mqtt/subscriptions/", ""},
		{"get_by_id", "GET", "/api/v1/mqtt/subscriptions/some-id", ""},
		{"start_lifecycle", "POST", "/api/v1/mqtt/subscriptions/some-id/start", ""},
		{"stats", "GET", "/api/v1/mqtt/subscriptions/some-id/stats", ""},
		{"create", "POST", "/api/v1/mqtt/subscriptions/", `{"name":"x"}`},
		{"delete", "DELETE", "/api/v1/mqtt/subscriptions/some-id", ""},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var bodyReader io.Reader
			if tc.body != "" {
				bodyReader = strings.NewReader(tc.body)
			}
			req := httptest.NewRequest(tc.method, tc.path, bodyReader)
			if tc.body != "" {
				req.Header.Set("Content-Type", "application/json")
			}
			resp, err := app.Test(req)
			if err != nil {
				t.Fatalf("app.Test: %v", err)
			}
			defer resp.Body.Close()

			if resp.StatusCode != fiber.StatusServiceUnavailable {
				t.Errorf("status: got %d, want 503", resp.StatusCode)
			}
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatalf("io.ReadAll: %v", err)
			}
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
	}
}
