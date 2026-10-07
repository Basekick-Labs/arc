package api

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

func TestTriggerMigrationRejectsInvalidTiersAtHTTPBoundary(t *testing.T) {
	h := NewTieringHandler(nil, nil, nil, zerolog.Nop())
	app := fiber.New()
	app.Post("/api/v1/tiering/migrate", h.TriggerMigration)

	for _, tc := range []struct {
		name string
		body string
		want string
	}{
		{
			name: "invalid from tier",
			body: `{"from_tier":"bogus","to_tier":"cold"}`,
			want: "invalid from_tier",
		},
		{
			name: "invalid to tier",
			body: `{"from_tier":"hot","to_tier":"bogus"}`,
			want: "invalid to_tier",
		},
		{
			name: "unsupported direction",
			body: `{"from_tier":"cold","to_tier":"hot"}`,
			want: "unsupported migration: only hot to cold is supported",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest("POST", "/api/v1/tiering/migrate", strings.NewReader(tc.body))
			req.Header.Set("Content-Type", "application/json")

			resp, err := app.Test(req)
			if err != nil {
				t.Fatalf("POST /api/v1/tiering/migrate: %v", err)
			}
			defer resp.Body.Close()

			if resp.StatusCode != fiber.StatusBadRequest {
				t.Fatalf("status = %d, want %d", resp.StatusCode, fiber.StatusBadRequest)
			}

			var response map[string]string
			if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
				t.Fatalf("decode response: %v", err)
			}
			if response["error"] != tc.want {
				t.Fatalf("error = %q, want %q", response["error"], tc.want)
			}
		})
	}
}
