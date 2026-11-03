//go:build duckdb_arrow

package api

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http/httptest"
	"regexp"
	"testing"

	"github.com/basekick-labs/arc/internal/database"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// Every DuckDB function a SELECT can call that changes or reads state shared
// by all users is refused. Enumerated from the running DuckDB, so a function
// of these families added by an upgrade fails here until it is listed.
func TestSideEffectFunctionPattern_Family(t *testing.T) {
	duck, err := database.New(&database.Config{MemoryLimit: "256MB", ThreadCount: 1, MaxConnections: 1}, zerolog.Nop())
	require.NoError(t, err)
	defer duck.Close()
	rows, err := duck.DB().QueryContext(context.Background(), `SELECT DISTINCT function_name FROM duckdb_functions()
		WHERE function_type IN ('table', 'scalar', 'table_macro', 'macro')
		AND regexp_matches(function_name, '(checkpoint|logging|_logs?$|log_contexts|truncate_duckdb|nextval|currval|setseed|secret|setting|variable|write_log)')`)
	require.NoError(t, err)
	defer rows.Close()
	// Read-only or unrelated members of the families.
	safe := regexp.MustCompile(`^(duckdb_secret_types|create_sort_key|pg_conf_load_time)$`)
	n := 0
	for rows.Next() {
		var name string
		require.NoError(t, rows.Scan(&name))
		if safe.MatchString(name) {
			continue
		}
		n++
		for _, q := range []string{"SELECT * FROM " + name + "()", "SELECT " + name + "('x')"} {
			if ValidateSQLRequest(q) == nil {
				t.Errorf("%s is not refused: %q", name, q)
			}
		}
	}
	require.NoError(t, rows.Err())
	require.Greater(t, n, 10, "the family query found too few functions")
}

// A read token's catalog change through any query endpoint is refused, and a
// built-in it tried to replace still answers as before on every endpoint.
func TestQueryEndpointsRefuseCatalogChanges(t *testing.T) {
	logger := zerolog.Nop()
	backend, err := storage.NewLocalBackend(t.TempDir(), logger)
	require.NoError(t, err)
	duck, err := database.New(&database.Config{MemoryLimit: "256MB", ThreadCount: 2, MaxConnections: 2}, logger)
	require.NoError(t, err)
	defer duck.Close()
	h := NewQueryHandler(duck, backend, logger, 0, 0)
	app := fiber.New(appConfig(DefaultServerConfig(), logger))
	h.RegisterRoutes(app)
	post := func(path, sql string) (int, string) {
		body, _ := json.Marshal(map[string]string{"sql": sql})
		req := httptest.NewRequest("POST", path, bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		resp, err := app.Test(req, -1)
		require.NoError(t, err)
		defer resp.Body.Close()
		out, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(out)
	}
	for _, path := range []string{"/api/v1/query", "/api/v1/query/msgpack", "/api/v1/query/arrow", "/api/v1/query/estimate"} {
		for _, sql := range []string{
			"CREATE MACRO avg(x) AS 999",
			"CREATE OR REPLACE TEMP MACRO avg(x) AS 999",
			"/* /* */ SELECT */ CREATE MACRO avg(x) AS 999",
			"EXPLAIN (ANALYZE) CREATE MACRO avg(x) AS 999",
		} {
			status, body := post(path, sql)
			require.Equal(t, 400, status, "%s %s: %s", path, sql, body)
		}
	}
	status, body := post("/api/v1/query", "SELECT avg(x) AS m FROM (VALUES (10.0), (30.0)) v(x)")
	require.Equal(t, 200, status, body)
	require.Contains(t, body, "[[20]]")
}
