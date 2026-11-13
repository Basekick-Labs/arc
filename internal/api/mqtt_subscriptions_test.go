package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"

	"github.com/basekick-labs/arc/internal/mqtt"
)

type stubMQTTManager struct {
	mqtt.Manager
	restartErr error
	getErr     error
}

func (s *stubMQTTManager) Get(ctx context.Context, id string) (*mqtt.Subscription, error) {
	return nil, s.getErr
}

func (s *stubMQTTManager) RestartSubscription(ctx context.Context, id string) error {
	return s.restartErr
}

// TestMQTTSubscriptionHandler_RestartAlreadyRunningConflict verifies that
// handleRestart maps mqtt.ErrSubscriptionAlreadyRunning to 409 Conflict.
func TestMQTTSubscriptionHandler_RestartAlreadyRunningConflict(t *testing.T) {
	h := &MQTTSubscriptionHandler{
		manager: &stubMQTTManager{restartErr: mqtt.ErrSubscriptionAlreadyRunning},
		logger:  zerolog.Nop(),
	}

	app := fiber.New()
	h.RegisterRoutes(app)

	req := httptest.NewRequest("POST", "/api/v1/mqtt/subscriptions/test-id/restart", nil)
	resp, err := app.Test(req, testRequestTimeoutMS)
	if err != nil {
		t.Fatalf("app.Test: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != fiber.StatusConflict {
		t.Errorf("status: got %d, want 409", resp.StatusCode)
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
	if got["error"] != mqtt.ErrSubscriptionAlreadyRunning.Error() {
		t.Errorf("error: got %q, want %q", got["error"], mqtt.ErrSubscriptionAlreadyRunning.Error())
	}
}

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
			resp, err := app.Test(req, testRequestTimeoutMS)
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

// A body the decoder refuses names what is wrong (#479): the bare "Invalid
// request body" was the whole answer a user following the docs got, and it
// told them nothing about which topic_mapping rule to fix.
func TestMQTTSubscriptionHandler_BadBodyNamesTheProblem(t *testing.T) {
	h := &MQTTSubscriptionHandler{manager: &stubMQTTManager{}, logger: zerolog.Nop()}
	app := fiber.New()
	h.RegisterRoutes(app)

	for _, tc := range []struct{ method, path string }{
		{"POST", "/api/v1/mqtt/subscriptions"},
		{"PUT", "/api/v1/mqtt/subscriptions/test-id"},
	} {
		body := `{"name":"x","topic_mapping":{"factory/+":{"tags_from_topic":[{"tag_name":"line"}]}}}`
		req := httptest.NewRequest(tc.method, tc.path, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		resp, err := app.Test(req, testRequestTimeoutMS)
		if err != nil {
			t.Fatalf("%s %s: %v", tc.method, tc.path, err)
		}
		raw, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != fiber.StatusBadRequest || !strings.Contains(string(raw), "position is required") {
			t.Errorf("%s %s: status %d body %s, want 400 naming the missing position", tc.method, tc.path, resp.StatusCode, raw)
		}
	}
}

// newMQTTSubscriptionTestApp serves the subscription routes over a real
// SubscriptionManager on a fresh SQLite store.
func newMQTTSubscriptionTestApp(t *testing.T) *fiber.App {
	t.Helper()
	repo, err := mqtt.NewSQLiteRepository(filepath.Join(t.TempDir(), "mqtt.db"), nil, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewSQLiteRepository: %v", err)
	}
	t.Cleanup(func() { _ = repo.Close() })
	encryptor, err := mqtt.NewPasswordEncryptor(nil)
	if err != nil {
		t.Fatalf("NewPasswordEncryptor: %v", err)
	}
	h := NewMQTTSubscriptionHandler(mqtt.NewSubscriptionManager(repo, encryptor, nil, zerolog.Nop()), nil, zerolog.Nop())
	app := fiber.New()
	h.RegisterRoutes(app)
	return app
}

// Every route that takes a subscription ID answers 404 for an ID with no
// stored subscription. Update, delete, start, restart and stats answered 500,
// and stop and pause 409 "subscription not running", so a wrong or stale ID
// read as a server fault or as a subscription that exists.
func TestMQTTSubscriptionHandler_UnknownIDIsNotFound(t *testing.T) {
	app := newMQTTSubscriptionTestApp(t)

	for _, tc := range []struct{ method, path, body string }{
		{"GET", "/api/v1/mqtt/subscriptions/no-such-id", ""},
		{"PUT", "/api/v1/mqtt/subscriptions/no-such-id", `{"name":"renamed"}`},
		{"DELETE", "/api/v1/mqtt/subscriptions/no-such-id", ""},
		{"POST", "/api/v1/mqtt/subscriptions/no-such-id/start", ""},
		{"POST", "/api/v1/mqtt/subscriptions/no-such-id/stop", ""},
		{"POST", "/api/v1/mqtt/subscriptions/no-such-id/pause", ""},
		{"POST", "/api/v1/mqtt/subscriptions/no-such-id/restart", ""},
		{"GET", "/api/v1/mqtt/subscriptions/no-such-id/stats", ""},
	} {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			var body io.Reader
			if tc.body != "" {
				body = strings.NewReader(tc.body)
			}
			req := httptest.NewRequest(tc.method, tc.path, body)
			if tc.body != "" {
				req.Header.Set("Content-Type", "application/json")
			}
			resp, err := app.Test(req, testRequestTimeoutMS)
			if err != nil {
				t.Fatalf("app.Test: %v", err)
			}
			defer resp.Body.Close()
			raw, _ := io.ReadAll(resp.Body)

			if resp.StatusCode != fiber.StatusNotFound {
				t.Fatalf("status %d body %s, want 404", resp.StatusCode, raw)
			}
			var got map[string]any
			if err := json.Unmarshal(raw, &got); err != nil {
				t.Fatalf("json.Unmarshal: %v (body=%s)", err, raw)
			}
			if got["success"] != false || got["error"] != "Subscription not found" {
				t.Errorf("body = %s, want success false and error \"Subscription not found\"", raw)
			}
		})
	}
}

// A stored subscription that is not running still answers 409 to stop and
// pause: the 404 is only for an ID with nothing stored.
func TestMQTTSubscriptionHandler_StopOfStoppedSubscriptionIsConflict(t *testing.T) {
	app := newMQTTSubscriptionTestApp(t)

	create := httptest.NewRequest("POST", "/api/v1/mqtt/subscriptions/", strings.NewReader(
		`{"name":"idle","broker":"tcp://localhost:1883","topics":["sensors/#"],"database":"iot"}`))
	create.Header.Set("Content-Type", "application/json")
	resp, err := app.Test(create, testRequestTimeoutMS)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	raw, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != fiber.StatusCreated {
		t.Fatalf("create: status %d body %s, want 201", resp.StatusCode, raw)
	}
	var created struct {
		Subscription mqtt.Subscription `json:"subscription"`
	}
	if err := json.Unmarshal(raw, &created); err != nil || created.Subscription.ID == "" {
		t.Fatalf("create body %s: %v", raw, err)
	}

	for _, action := range []string{"stop", "pause"} {
		req := httptest.NewRequest("POST", "/api/v1/mqtt/subscriptions/"+created.Subscription.ID+"/"+action, nil)
		resp, err := app.Test(req, testRequestTimeoutMS)
		if err != nil {
			t.Fatalf("%s: %v", action, err)
		}
		raw, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != fiber.StatusConflict || !strings.Contains(string(raw), mqtt.ErrSubscriptionNotRunning.Error()) {
			t.Errorf("%s: status %d body %s, want 409 %q", action, resp.StatusCode, raw, mqtt.ErrSubscriptionNotRunning)
		}
	}
}

// A GET that fails for a reason other than a missing subscription is a 500:
// it answered 404 for any error, so a store failure read as a deleted
// subscription.
func TestMQTTSubscriptionHandler_GetStoreFailureIsServerError(t *testing.T) {
	h := &MQTTSubscriptionHandler{
		manager: &stubMQTTManager{getErr: errors.New("failed to scan subscription: disk I/O error")},
		logger:  zerolog.Nop(),
	}
	app := fiber.New()
	h.RegisterRoutes(app)

	resp, err := app.Test(httptest.NewRequest("GET", "/api/v1/mqtt/subscriptions/some-id", nil), testRequestTimeoutMS)
	if err != nil {
		t.Fatalf("app.Test: %v", err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != fiber.StatusInternalServerError {
		t.Errorf("status %d body %s, want 500", resp.StatusCode, raw)
	}
	if strings.Contains(string(raw), "disk I/O") {
		t.Errorf("body %s exposes the store error", raw)
	}
}
