//go:build duckdb_arrow

package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/database"
	"github.com/basekick-labs/arc/internal/queryregistry"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// requireClientErr checks a client-visible error: DuckDB's message is there,
// its statement context (Arc's rewritten SQL) and every storage path are not.
func requireClientErr(t *testing.T, where, msg string, want ...string) {
	t.Helper()
	for _, w := range want {
		require.Contains(t, msg, w, "%s", where)
	}
	tmp, _ := filepath.EvalSymlinks(os.TempDir())
	for _, leak := range []string{"LINE ", "read_parquet([", os.TempDir(), tmp} {
		if leak != "" {
			require.NotContains(t, msg, leak, "%s", where)
		}
	}
}

// Each query route answers a statement DuckDB rejects with DuckDB's
// message, not with the rewritten statement and the files it read.
func TestQueryRoutesReturnClientErrorText(t *testing.T) {
	app := unicodeSpaceRig(t)
	hdr := map[string]string{"x-arc-database": "test"}
	const bad = "SELECT avg(vv) FROM cpu"
	for _, path := range []string{"/api/v1/query", "/api/v1/query/arrow", "/api/v1/query/estimate"} {
		code, out := usDoJSON(t, app, "POST", path, map[string]string{"sql": bad}, hdr)
		if path != "/api/v1/query/estimate" { // the estimate answers 200 with the error in the body
			require.GreaterOrEqual(t, code, 400, "%s: %v", path, out)
		}
		msg, _ := out["error"].(string)
		requireClientErr(t, path, msg, "Binder Error", `"vv"`)
	}
	// The MessagePack route answers in its own encoding; the error text is
	// in the body as a string either way.
	body, _ := json.Marshal(map[string]string{"sql": bad})
	req := httptest.NewRequest("POST", "/api/v1/query/msgpack", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-arc-database", "test")
	resp, err := app.Test(req, -1)
	require.NoError(t, err)
	raw, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	require.GreaterOrEqual(t, resp.StatusCode, 400)
	requireClientErr(t, "msgpack", string(raw), "Binder Error", `"vv"`)

	// A damaged file: DuckDB's error names it, the client does not see the
	// path. (DuckDB reads every file's schema at bind with union_by_name, so
	// this fails at the start; the drain path has no reproducible fixture.)
	late := damagedFileRig(t)
	body, _ = json.Marshal(map[string]string{"sql": "SELECT * FROM cpu"})
	req = httptest.NewRequest("POST", "/api/v1/query/msgpack", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-arc-database", "test")
	resp, err = late.Test(req, -1)
	require.NoError(t, err)
	raw, _ = io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	require.GreaterOrEqual(t, resp.StatusCode, 400, "%s", raw)
	requireClientErr(t, "msgpack drain", string(raw))
	require.NotContains(t, string(raw), "cpu_b.parquet", "the damaged file's path reached the client")

	code, out := usDoJSON(t, app, "GET", "/api/v1/query/cpu?database=test&where="+url.QueryEscape("vv > 1"), nil, nil)
	require.GreaterOrEqual(t, code, 400, "%v", out)
	msg, _ := out["error"].(string)
	requireClientErr(t, "measurement", msg, "Binder Error", `"vv"`)
}

// The query history keeps no statement, path or queried value, whoever
// records the failure.
func TestRegistryFailSanitises(t *testing.T) {
	r := queryregistry.NewRegistry(&queryregistry.RegistryConfig{HistorySize: 10}, zerolog.Nop())
	id, _ := r.Register(context.Background(), "SELECT 1", 0, "test", "")
	r.Fail(id, "arrow query failed: Conversion Error: Could not convert string 'secret' to INT32 reading /data/arc/y.parquet\n\nLINE 1: SELECT ... FROM read_parquet(['/data/arc/x.parquet'])")
	q := r.GetQuery(id)
	require.NotNil(t, q)
	require.NotContains(t, q.Error, "LINE ")
	require.NotContains(t, q.Error, "/data/arc")
	require.NotContains(t, q.Error, "secret")
	require.Contains(t, q.Error, "Conversion Error")
}

// A Flight SQL prepare whose statement does not bind gets DuckDB's message,
// not the statement it was bound as.
func TestFlightSQLBindFailureClientText(t *testing.T) {
	h := &QueryHandler{logger: zerolog.Nop()}
	err := fmt.Errorf("%w: %s", database.ErrSchemaBind,
		"Binder Error: Referenced column \"vv\" not found in FROM clause!\n\nLINE 1: SELECT vv FROM read_parquet(['/data/arc/test/cpu/x.parquet'])")
	_, code, msg := h.flightSQLBindFailure(context.Background(), context.Background(), err)
	require.Equal(t, fiber.StatusBadRequest, code)
	requireClientErr(t, "flight bind", msg, `"vv"`)
	require.NotContains(t, msg, "/data/arc")
}

// DELETE drops DuckDB's statement context (every file the scan read) and
// keeps the message.
func TestDeleteRouteDropsStatementContext(t *testing.T) {
	r := newLeftoverRig(t)
	h := NewDeleteHandler(r.db, r.backend, &config.DeleteConfig{Enabled: true, MaxRowsPerDelete: 1000, ConfirmationThreshold: 1000}, nil, t.TempDir(), zerolog.Nop())
	app := fiber.New()
	h.RegisterRoutes(app)
	r.parquet(leftoverHour+"cpu_20261007_200000_000000000.parquet", "h1", "h2")

	code, out := usDoJSON(t, app, "POST", "/api/v1/delete", map[string]any{
		"database": "db", "measurement": "cpu", "where": "CAST(host AS INTEGER) = 1", "dry_run": true,
	}, nil)
	require.GreaterOrEqual(t, code, 400, "%v", out)
	msg, _ := out["error"].(string)
	require.Contains(t, msg, "Conversion Error")
	require.NotContains(t, msg, "LINE ")
	require.False(t, strings.Contains(msg, r.root), "%q names the storage root", msg)

	// A WHERE that does not bind (the scan's read over every file is in
	// DuckDB's statement context) loses that context. Two files whose v
	// columns differ in type make v + 1 fail to bind.
	mixed := filepath.Join(r.root, leftoverHour+"cpu_20261007_200002_000000000.parquet")
	_, err := r.db.DB().Exec(fmt.Sprintf(`COPY (SELECT 'h9' AS host, 'text' AS v) TO '%s' (FORMAT PARQUET)`, mixed))
	require.NoError(t, err)
	code, out = usDoJSON(t, app, "POST", "/api/v1/delete", map[string]any{
		"database": "db", "measurement": "cpu", "where": "v + 1 > 0", "dry_run": true,
	}, nil)
	require.GreaterOrEqual(t, code, 400, "%v", out)
	msg, _ = out["error"].(string)
	require.NotContains(t, msg, "LINE ", "%q", msg)
	require.False(t, strings.Contains(msg, r.root), "%q names the storage root", msg)
	require.NoError(t, os.Remove(mixed))

	// A damaged file is named relative to the database, never by the
	// absolute path DuckDB read it from.
	damaged := leftoverHour + "cpu_20261007_200001_000000000.parquet"
	require.NoError(t, os.WriteFile(filepath.Join(r.root, damaged), []byte("not parquet"), 0o600))
	code, out = usDoJSON(t, app, "POST", "/api/v1/delete", map[string]any{
		"database": "db", "measurement": "cpu", "where": "host = 'h1'", "dry_run": true,
	}, nil)
	require.GreaterOrEqual(t, code, 400, "%v", out)
	msg, _ = out["error"].(string)
	require.Contains(t, msg, "cpu_20261007_200001_000000000.parquet", "%q", msg)
	require.False(t, strings.Contains(msg, r.root), "%q names the storage root", msg)
}

// lateFailureRig holds test/cpu as one good file and one damaged file,
// whose error names the damaged file's path.
func damagedFileRig(t *testing.T) *fiber.App {
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
	_, err = duck.DB().Exec(fmt.Sprintf(`COPY (SELECT TIMESTAMP '2026-11-26 00:00:00' + to_seconds(i) AS time,
		CAST(i AS VARCHAR) AS host FROM range(20000) t(i)) TO '%s' (FORMAT PARQUET)`, full))
	require.NoError(t, err)
	// A second file, cut short: DuckDB fails on it after the first file's rows.
	good, err := os.ReadFile(full)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(filepath.Dir(full), "cpu_b.parquet"), good[:len(good)/2], 0o600))
	h := NewQueryHandler(duck, backend, logger, 0, 0)
	app := fiber.New(appConfig(DefaultServerConfig(), logger))
	h.RegisterRoutes(app)
	return app
}
