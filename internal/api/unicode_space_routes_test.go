//go:build duckdb_arrow

package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"testing"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/database"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// unicodeSpaceRig is a query handler over local storage holding test/cpu,
// whose host values include one with an ASCII space and one with a
// no-break space, so a test can tell which one a literal matched.
func unicodeSpaceRig(t *testing.T) *fiber.App {
	t.Helper()
	logger := zerolog.Nop()
	root := t.TempDir()
	backend, err := storage.NewLocalBackend(root, logger)
	require.NoError(t, err)
	t.Cleanup(func() { _ = backend.Close() })
	duck, err := database.New(&database.Config{MemoryLimit: "256MB", ThreadCount: 1, MaxConnections: 2, LocalStorageRoot: root}, logger)
	require.NoError(t, err)
	t.Cleanup(func() { _ = duck.Close() })

	full := filepath.Join(root, "test/cpu/2026/11/26/00/cpu_a.parquet")
	require.NoError(t, os.MkdirAll(filepath.Dir(full), 0o700))
	_, err = duck.DB().Exec(fmt.Sprintf(`COPY (SELECT TIMESTAMP '2026-11-26 00:00:00' + to_seconds(v) AS time, host, v
		FROM (VALUES ('a', 1), ('a b', 2), ('a`+nbsp+`b', 3)) AS r(host, v)) TO '%s' (FORMAT PARQUET)`, full))
	require.NoError(t, err)

	h := NewQueryHandler(duck, backend, logger, 0, 0)
	app := fiber.New(appConfig(DefaultServerConfig(), logger))
	h.RegisterRoutes(app)
	return app
}

func usDoJSON(t *testing.T, app *fiber.App, method, target string, body any, header map[string]string) (int, map[string]any) {
	t.Helper()
	var rd io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rd = bytes.NewReader(b)
	}
	req := httptest.NewRequest(method, target, rd)
	req.Header.Set("Content-Type", "application/json")
	for k, v := range header {
		req.Header.Set(k, v)
	}
	resp, err := app.Test(req, -1)
	require.NoError(t, err)
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var out map[string]any
	_ = json.Unmarshal(raw, &out)
	return resp.StatusCode, out
}

// Through /api/v1/query: a table reference after a no-break space is
// rewritten to the measurement's files, which only happens when the
// handler goes on with the canonical text; and a no-break space inside a
// literal is part of the value.
func TestQueryRouteUsesCanonicalSQL(t *testing.T) {
	app := unicodeSpaceRig(t)
	hdr := map[string]string{"x-arc-database": "test"}
	for _, tc := range []struct {
		sql  string
		want float64
	}{
		{"SELECT count(*) AS n FROM" + nbsp + "cpu", 3},
		{"SELECT" + nbsp + "v FROM cpu WHERE host = 'a" + nbsp + "b'", 3},
		{"SELECT v FROM cpu WHERE host = 'a b'", 2},
	} {
		code, out := usDoJSON(t, app, "POST", "/api/v1/query", map[string]string{"sql": tc.sql}, hdr)
		require.Equal(t, 200, code, "%q: %v", tc.sql, out)
		data, _ := out["data"].([]any)
		require.Len(t, data, 1, "%q: %v", tc.sql, out)
		require.Equal(t, tc.want, data[0].([]any)[0], "%q", tc.sql)
	}
}

// Through /api/v1/query/:measurement: the WHERE parameter is read as DuckDB
// will read it, a no-break space in code as a separator and one inside a
// literal as part of the value.
func TestQueryMeasurementRouteUsesCanonicalWhere(t *testing.T) {
	app := unicodeSpaceRig(t)
	for where, want := range map[string]float64{
		"host" + nbsp + "= 'a" + nbsp + "b'": 3,
		"host = 'a b'":                       2,
	} {
		target := "/api/v1/query/cpu?database=test&where=" + url.QueryEscape(where)
		code, out := usDoJSON(t, app, "GET", target, nil, nil)
		require.Equal(t, 200, code, "%q: %v", where, out)
		data, _ := out["data"].([]any)
		require.Len(t, data, 1, "%q: %v", where, out)
		row := data[0].([]any)
		cols, _ := out["columns"].([]any)
		for i, c := range cols {
			if c == "v" {
				require.Equal(t, want, row[i], "%q", where)
			}
		}
	}
}

// The measurement endpoint's own WHERE check reads literals as DuckDB will:
// a dollar-quoted literal after a no-break space holds its semicolon.
func TestQueryMeasurementRouteWhereCheckSeesLiteral(t *testing.T) {
	app := unicodeSpaceRig(t)
	where := "host =" + nbsp + "$$a;b$$"
	code, out := usDoJSON(t, app, "GET", "/api/v1/query/cpu?database=test&where="+url.QueryEscape(where), nil, nil)
	require.Equal(t, 200, code, "%q: %v", where, out)
	data, _ := out["data"].([]any)
	require.Len(t, data, 0, "%q: %v", where, out)
}

// Through /api/v1/delete: the clause is canonical before the measurement
// qualifier is removed, so a dollar-quoted literal after a no-break space
// keeps its text and the DELETE matches the row it names.
func TestDeleteRouteCanonicalisesBeforeUnqualifying(t *testing.T) {
	r := newLeftoverRig(t)
	h := NewDeleteHandler(r.db, r.backend, &config.DeleteConfig{Enabled: true, MaxRowsPerDelete: 1000, ConfirmationThreshold: 1000}, nil, t.TempDir(), zerolog.Nop())
	app := fiber.New()
	h.RegisterRoutes(app)
	r.parquet(leftoverHour+"cpu_20261007_200000_000000000.parquet", "cpu.host", "host", "host", "h1")

	for where, want := range map[string]int64{
		"host =" + nbsp + "$$cpu.host$$": 1, // the literal's text, not "host"
		"host =" + nbsp + "$t$host$t$":   2,
		"cpu" + nbsp + ".host = 'h1'":    1, // qualified column, read as cpu .host
	} {
		code, out := usDoJSON(t, app, "POST", "/api/v1/delete", map[string]any{
			"database": "db", "measurement": "cpu", "where": where, "dry_run": true,
		}, nil)
		require.Equal(t, 200, code, "%q: %v", where, out)
		require.Equal(t, float64(want), out["deleted_count"], "%q: %v", where, out)
	}
}
