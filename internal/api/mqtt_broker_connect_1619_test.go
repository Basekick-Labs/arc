package api

import (
	"encoding/json"
	"io"
	"net"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gofiber/fiber/v2"

	"github.com/basekick-labs/arc/internal/mqtt"
)

// #1619: start and restart against a broker that cannot be reached answered
// 500 "Failed to start subscription", hiding the reason, which only a GET of
// error_message showed. They answer 502 with that same message, which never
// carries the broker URL's userinfo. Stop afterwards stays 409: nothing runs.
func TestMQTTSubscriptionHandler_BrokerConnectFailureIsBadGateway(t *testing.T) {
	app := newMQTTSubscriptionTestApp(t)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	closed := ln.Addr().String()
	_ = ln.Close()

	do := func(method, path, body string) (int, map[string]any, string) {
		t.Helper()
		var r io.Reader
		if body != "" {
			r = strings.NewReader(body)
		}
		req := httptest.NewRequest(method, path, r)
		if body != "" {
			req.Header.Set("Content-Type", "application/json")
		}
		resp, err := app.Test(req, 10000)
		if err != nil {
			t.Fatalf("%s %s: %v", method, path, err)
		}
		defer resp.Body.Close()
		raw, _ := io.ReadAll(resp.Body)
		var got map[string]any
		if err := json.Unmarshal(raw, &got); err != nil {
			t.Fatalf("%s %s: body %s: %v", method, path, raw, err)
		}
		return resp.StatusCode, got, string(raw)
	}

	status, created, raw := do("POST", "/api/v1/mqtt/subscriptions/", `{"name":"down","broker":"tcp://u:secret@`+closed+
		`","topics":["sensors/#"],"database":"iot","connect_timeout_seconds":2}`)
	if status != fiber.StatusCreated {
		t.Fatalf("create: status %d body %s", status, raw)
	}
	id, _ := created["subscription"].(map[string]any)["id"].(string)
	base := "/api/v1/mqtt/subscriptions/" + id

	for _, action := range []string{"start", "restart"} {
		status, got, raw := do("POST", base+"/"+action, "")
		if status != fiber.StatusBadGateway {
			t.Fatalf("%s: status %d body %s, want 502", action, status, raw)
		}
		msg, _ := got["error"].(string)
		if got["success"] != false || !strings.HasPrefix(msg, "could not connect to the broker: ") {
			t.Fatalf("%s: body %s, want the connect failure", action, raw)
		}
		if strings.Contains(raw, "secret") {
			t.Fatalf("%s: body %s carries the broker password", action, raw)
		}

		_, stored, rawGet := do("GET", base, "")
		sub, _ := stored["subscription"].(map[string]any)
		if sub["status"] != string(mqtt.StatusError) || sub["error_message"] != msg {
			t.Fatalf("%s: stored %s, want status error and error_message %q", action, rawGet, msg)
		}
	}

	if status, _, raw := do("POST", base+"/stop", ""); status != fiber.StatusConflict {
		t.Fatalf("stop: status %d body %s, want 409", status, raw)
	}
}
