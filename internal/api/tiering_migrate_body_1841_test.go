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
		{"bare dry_run form field", "application/x-www-form-urlencoded", "database=x&dry_run", "", false, true},
		{"empty dry_run form field", "application/x-www-form-urlencoded", "database=x&dry_run=", "", false, true},
		{"empty database form field", "application/x-www-form-urlencoded", "database=", "", false, true},
		{"empty database JSON field", "application/json", `{"database":""}`, "", false, true},
		{"null database", "application/json", `{"database":null}`, "", false, true},
		{"null dry_run", "application/json", `{"database":"x","dry_run":null}`, "", false, true},
		{"a closing brace after the object", "application/json", `{"database":"x"}}`, "", false, true},
		{"JSON null", "application/json", `null`, "", false, true},
		{"JSON array", "application/json", `[]`, "", false, true},
		{"form type with a charset", "application/x-www-form-urlencoded; charset=utf-8", "database=x", "x", false, false},
		{"url-encoded form value", "application/x-www-form-urlencoded", "database=my%2Ddb", "my-db", false, false},
		{"old Go-field-name form key", "application/x-www-form-urlencoded", "dryrun=true", "", false, true},
		{"multipart form", "multipart/form-data; boundary=x", "--x\r\nContent-Disposition: form-data; name=\"database\"\r\n\r\nx\r\n--x--\r\n", "", false, true},
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

// Every filter field arrives from a form body.
func TestMigrationFormFields1841(t *testing.T) {
	app := fiber.New()
	var got TriggerMigrationRequest
	var gotErr error
	app.Post("/", func(c *fiber.Ctx) error { got, gotErr = parseMigrationRequest(c); return nil })
	req := httptest.NewRequest("POST", "/", strings.NewReader("from_tier=hot&to_tier=cold&database=d&measurement=m&dry_run=1"))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	if _, err := app.Test(req); err != nil {
		t.Fatal(err)
	}
	if gotErr != nil || got != (TriggerMigrationRequest{FromTier: "hot", ToTier: "cold", Database: "d", Measurement: "m", DryRun: true}) {
		t.Fatalf("parsed %+v, err %v", got, gotErr)
	}
}

// JSON sent with curl's default form Content-Type reaches the handler scoped:
// a misspelt field there is refused, not run against every database.
func TestMigrateRefusesMisspeltJSONSentAsForm1841(t *testing.T) {
	h := NewTieringHandler(nil, nil, nil, zerolog.Nop())
	app := fiber.New()
	app.Post("/migrate", h.TriggerMigration)
	req := httptest.NewRequest("POST", "/migrate", strings.NewReader(`{"databse":"x"}`))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := app.Test(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 400 {
		t.Fatalf("status %d, want 400", resp.StatusCode)
	}
}

// Reconcile refuses a misspelt field instead of running its default window.
func TestReconcileRefusesUnknownField1841(t *testing.T) {
	h := NewTieringHandler(nil, nil, nil, zerolog.Nop())
	app := fiber.New()
	app.Post("/reconcile", h.Reconcile)
	req := httptest.NewRequest("POST", "/reconcile", strings.NewReader(`{"window_hourz": 72}`))
	req.Header.Set("Content-Type", "application/json")
	resp, err := app.Test(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 400 {
		t.Fatalf("status %d, want 400", resp.StatusCode)
	}
}
