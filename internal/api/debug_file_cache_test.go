package api

import (
	"encoding/json"
	"io"
	"net/http/httptest"
	"testing"

	"github.com/basekick-labs/arc/internal/database"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// Both debug endpoints report whether DuckDB's file cache is on (#1372), so a
// rig can tell an EXTERNAL_FILE_CACHE of 0 that means "off" from one that
// means "nothing read yet".
func TestDebugEndpointsReportExternalFileCache(t *testing.T) {
	for _, mode := range []string{"true", "false"} {
		t.Run(mode, func(t *testing.T) {
			db, err := database.New(&database.Config{MaxConnections: 2, MemoryLimit: "256MB", LocalStorageRoot: t.TempDir(), ExternalFileCache: mode}, zerolog.Nop())
			if err != nil {
				t.Fatalf("database.New: %v", err)
			}
			t.Cleanup(func() { db.Close() })
			app := fiber.New()
			NewDebugHandler(db, nil, zerolog.Nop()).RegisterRoutes(app)
			want := mode == "true"

			get := func(path string) map[string]any {
				resp, err := app.Test(httptest.NewRequest("GET", path, nil), 30000)
				if err != nil {
					t.Fatalf("GET %s: %v", path, err)
				}
				defer resp.Body.Close()
				b, _ := io.ReadAll(resp.Body)
				if resp.StatusCode != 200 {
					t.Fatalf("GET %s: %d %s", path, resp.StatusCode, b)
				}
				var m map[string]any
				if err := json.Unmarshal(b, &m); err != nil {
					t.Fatalf("GET %s: %v", path, err)
				}
				return m
			}
			if got, ok := get("/api/v1/debug/duckdb-memory")["external_file_cache"].(bool); !ok || got != want {
				t.Errorf("duckdb-memory external_file_cache = %v (present %v), want %v", got, ok, want)
			}
			sum, _ := get("/api/v1/debug/memstats")["duckdb_summary"].(map[string]any)
			if got, ok := sum["external_file_cache"].(bool); !ok || got != want {
				t.Errorf("memstats duckdb_summary.external_file_cache = %v (present %v), want %v", got, ok, want)
			}
		})
	}
}
