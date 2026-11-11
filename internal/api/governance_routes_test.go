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

// newGovernanceRoutesApp mounts the policy routes on a real manager, without
// the admin and license gates RegisterRoutes adds in front of them.
func newGovernanceRoutesApp(t *testing.T) *fiber.App {
	t.Helper()
	h := NewGovernanceHandler(newGovernanceTestManager(t, nil), nil, nil, zerolog.Nop())
	app := fiber.New()
	app.Post("/policies", h.CreatePolicy)
	app.Get("/policies/:token_id", h.GetPolicy)
	app.Put("/policies/:token_id", h.UpdatePolicy)
	app.Delete("/policies/:token_id", h.DeletePolicy)
	return app
}

func sendGovernance(t *testing.T, app *fiber.App, method, path, body string) (int, map[string]any) {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := app.Test(req, testRequestTimeoutMS)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("%s %s: decode %q: %v", method, path, raw, err)
	}
	return resp.StatusCode, out
}

// A second policy for a token is a 409, not a 500: a token has one policy,
// changed with PUT. The first policy is left as it was.
func TestGovernanceCreatePolicyForATokenWithOneIsAConflict(t *testing.T) {
	app := newGovernanceRoutesApp(t)

	if status, body := sendGovernance(t, app, "POST", "/policies", `{"token_id":1,"rate_limit_per_minute":10}`); status != fiber.StatusCreated {
		t.Fatalf("first create: %d %v", status, body)
	}
	status, body := sendGovernance(t, app, "POST", "/policies", `{"token_id":1,"rate_limit_per_minute":20}`)
	if status != fiber.StatusConflict || body["success"] != false || body["error"] != "A policy already exists for this token" {
		t.Fatalf("second create: %d %v; want 409 with the conflict error", status, body)
	}
	status, body = sendGovernance(t, app, "GET", "/policies/1", "")
	policy, _ := body["policy"].(map[string]any)
	if status != fiber.StatusOK || policy["rate_limit_per_minute"] != float64(10) {
		t.Fatalf("policy after the conflict: %d %v; want the first", status, body)
	}
}

// DELETE of a token without a policy is a 404, like GET and PUT; it answered
// 200 "Policy deleted".
func TestGovernanceDeletePolicyOfATokenWithoutOneIsNotFound(t *testing.T) {
	app := newGovernanceRoutesApp(t)
	notFound := func(method, path, body string) {
		t.Helper()
		status, got := sendGovernance(t, app, method, path, body)
		if status != fiber.StatusNotFound || got["success"] != false || got["error"] != "No policy found for this token" {
			t.Fatalf("%s %s: %d %v; want 404 with the not-found error", method, path, status, got)
		}
	}

	notFound("DELETE", "/policies/1", "")

	if status, body := sendGovernance(t, app, "POST", "/policies", `{"token_id":1,"rate_limit_per_minute":10}`); status != fiber.StatusCreated {
		t.Fatalf("create: %d %v", status, body)
	}
	if status, body := sendGovernance(t, app, "DELETE", "/policies/1", ""); status != fiber.StatusOK {
		t.Fatalf("delete: %d %v", status, body)
	}
	notFound("DELETE", "/policies/1", "")
	notFound("GET", "/policies/1", "")
	notFound("PUT", "/policies/1", `{"rate_limit_per_minute":20}`)
}
