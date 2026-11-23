package api

import (
	"io"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// #1841: a migrate body that cannot be read used to be ignored like an empty
// one, and an empty body migrates every database.
func TestParseMigrationRequest1841(t *testing.T) {
	cases := []struct {
		name, ctype, body string
		wantDB            string
		wantDry           bool
		wantErr           bool
	}{
		{"empty body: every database", "", "", "", false, false},
		{"whitespace only", "application/json", "  \n", "", false, false},
		{"JSON", "application/json", `{"database":"x"}`, "x", false, false},
		{"JSON sent with curl's default form type", "application/x-www-form-urlencoded", `{"database":"x"}`, "x", false, false},
		{"JSON with no Content-Type", "", `{"database":"x","dry_run":true}`, "x", true, false},
		{"JSON as text/plain", "text/plain", `{"database":"x"}`, "x", false, false},
		{"form fields", "application/x-www-form-urlencoded", "database=x&dry_run=true", "x", true, false},
		{"malformed JSON", "application/json", `{"database":`, "", false, true},
		{"misspelt JSON field", "application/json", `{"databse":"x"}`, "", false, true},
		{"trailing data after the object", "application/json", `{"database":"x"} {"database":"y"}`, "", false, true},
		{"misspelt form field", "application/x-www-form-urlencoded", "databse=x", "", false, true},
		{"form dry_run that is not a boolean", "application/x-www-form-urlencoded", "dry_run=yes", "", false, true},
		{"repeated form field", "application/x-www-form-urlencoded", "database=x&database=y", "", false, true},
		{"neither JSON nor form", "text/plain", "database x", "", false, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			app := fiber.New()
			var got TriggerMigrationRequest
			var gotErr error
			app.Post("/", func(c *fiber.Ctx) error {
				got, gotErr = parseMigrationRequest(c)
				return nil
			})
			req := httptest.NewRequest("POST", "/", strings.NewReader(tc.body))
			if tc.ctype != "" {
				req.Header.Set("Content-Type", tc.ctype)
			}
			if _, err := app.Test(req); err != nil {
				t.Fatal(err)
			}
			if (gotErr != nil) != tc.wantErr {
				t.Fatalf("err = %v, want error %v", gotErr, tc.wantErr)
			}
			if !tc.wantErr && (got.Database != tc.wantDB || got.DryRun != tc.wantDry) {
				t.Fatalf("parsed %+v, want database %q dry_run %v", got, tc.wantDB, tc.wantDry)
			}
		})
	}
}

// A refused body is answered 400 before anything runs.
func TestMigrateRefusesAnUnreadableBody1841(t *testing.T) {
	h := NewTieringHandler(nil, nil, nil, zerolog.Nop())
	app := fiber.New()
	app.Post("/migrate", h.TriggerMigration)
	req := httptest.NewRequest("POST", "/migrate", strings.NewReader(`{"databse":"x"}`))
	req.Header.Set("Content-Type", "application/json")
	resp, err := app.Test(req)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 400 || !strings.Contains(string(raw), "Nothing was migrated") {
		t.Fatalf("status %d: %s", resp.StatusCode, raw)
	}
}
