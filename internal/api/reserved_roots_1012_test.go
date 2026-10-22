package api

import (
	"io"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// GET /api/v1/databases/{name} answers a reserved storage root as not found,
// as the database list and the measurements endpoint do (#1012). Seeded so a
// missing guard would find the directory and answer 200.
func TestDatabasesHandler_GetReservedRoots(t *testing.T) {
	_, app, tmpDir := setupTestDatabasesHandler(t, false)
	defer os.RemoveAll(tmpDir)

	for _, root := range []string{"_schema", "_compaction_state"} {
		t.Run(root, func(t *testing.T) {
			dir := filepath.Join(tmpDir, root, "database")
			if err := os.MkdirAll(dir, 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dir, "measurement.parquet"), []byte("reserved data"), 0o600); err != nil {
				t.Fatal(err)
			}
			resp, err := app.Test(httptest.NewRequest("GET", "/api/v1/databases/"+root, nil), testRequestTimeoutMS)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != fiber.StatusNotFound {
				body, _ := io.ReadAll(resp.Body)
				t.Fatalf("expected 404, got %d: %s", resp.StatusCode, body)
			}
		})
	}
}

// The delete API refuses a reserved storage root as its database and leaves
// the files under it alone (#1012).
func TestDeleteRefusesReservedRoots(t *testing.T) {
	root, backend, db := recordsDuckDB(t)
	h := NewDeleteHandler(db, backend, &config.DeleteConfig{Enabled: true, MaxRowsPerDelete: 1000, ConfirmationThreshold: 1000}, nil, t.TempDir(), zerolog.Nop())
	app := fiber.New()
	h.RegisterRoutes(app)

	for _, reserved := range []string{"_schema", "_compaction_state"} {
		t.Run(reserved, func(t *testing.T) {
			rel := reserved + "/db/cpu/2026/10/11/10/cpu_a.parquet"
			writeRecordsParquet(t, db, root, rel)
			req := httptest.NewRequest("POST", "/api/v1/delete", strings.NewReader(`{"database":"`+reserved+`","measurement":"db","where":"host = 'h1'","confirm":true}`))
			req.Header.Set("Content-Type", "application/json")
			resp, err := app.Test(req, 20000)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			body, _ := io.ReadAll(resp.Body)
			if resp.StatusCode != fiber.StatusBadRequest || !strings.Contains(string(body), "reserved") {
				t.Fatalf("expected 400 naming the reserved root, got %d: %s", resp.StatusCode, body)
			}
			if _, err := os.Stat(filepath.Join(root, filepath.FromSlash(rel))); err != nil {
				t.Fatalf("file under %s touched: %v", reserved, err)
			}
		})
	}
}
