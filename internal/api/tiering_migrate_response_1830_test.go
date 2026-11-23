package api

import (
	"encoding/json"
	"errors"
	"io"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/basekick-labs/arc/internal/tiering"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// #1830: the migrate answer says what the run did; it used to answer 200
// "Migration completed successfully" for a run cut off by its budget that
// left migrated files hot.
func TestMigrationResponse1830(t *testing.T) {
	cases := []struct {
		name   string
		res    tiering.MigrationResult
		code   int
		status string
	}{
		{"all moved", tiering.MigrationResult{Candidates: 3, Migrated: 3}, 200, "completed"},
		{"budget ran out", tiering.MigrationResult{Candidates: 3, Migrated: 1, NotAttempted: 2, TimedOut: true}, 200, "incomplete"},
		{"kept hot copies", tiering.MigrationResult{Candidates: 3, Migrated: 3, Kept: 2, Failed: 1, ReleaseErr: errors.New("manifest apply failed")}, 500, "incomplete"},
		{"file errors only", tiering.MigrationResult{Candidates: 3, Migrated: 2, Failed: 1}, 200, "completed"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			app := fiber.New()
			app.Get("/", func(c *fiber.Ctx) error { return migrationResponse(c, tc.res) })
			resp, err := app.Test(httptest.NewRequest("GET", "/", nil))
			if err != nil {
				t.Fatal(err)
			}
			raw, _ := io.ReadAll(resp.Body)
			var body map[string]any
			if err := json.Unmarshal(raw, &body); err != nil {
				t.Fatal(err)
			}
			if resp.StatusCode != tc.code || body["status"] != tc.status {
				t.Fatalf("got %d %v, want %d %s: %s", resp.StatusCode, body["status"], tc.code, tc.status, raw)
			}
			if int(body["kept_hot_copies"].(float64)) != tc.res.Kept || int(body["not_attempted"].(float64)) != tc.res.NotAttempted {
				t.Fatalf("counts not reported: %s", raw)
			}
		})
	}
}

// The reconcile body is read as JSON whatever its Content-Type (curl -d sends
// a form type, whose decoder would drop window_hours), and validated before
// anything runs.
func TestReconcileRequestValidation1830(t *testing.T) {
	h := NewTieringHandler(nil, nil, nil, zerolog.Nop())
	app := fiber.New()
	app.Post("/reconcile", h.Reconcile)
	for _, tc := range []struct{ name, ctype, body string }{
		{"negative window", "application/json", `{"window_hours": -1}`},
		{"window too wide", "application/json", `{"window_hours": 9000}`},
		{"form content type still parsed as JSON", "application/x-www-form-urlencoded", `{"window_hours": 9000}`},
		{"not JSON", "application/json", `window_hours=72`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest("POST", "/reconcile", strings.NewReader(tc.body))
			req.Header.Set("Content-Type", tc.ctype)
			resp, err := app.Test(req)
			if err != nil {
				t.Fatal(err)
			}
			if resp.StatusCode != 400 {
				raw, _ := io.ReadAll(resp.Body)
				t.Fatalf("status %d, want 400: %s", resp.StatusCode, raw)
			}
		})
	}
}
