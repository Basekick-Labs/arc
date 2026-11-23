package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"

	"github.com/basekick-labs/arc/internal/auth"
	"github.com/basekick-labs/arc/internal/storage"
)

func rehomeRig(t *testing.T) (*RehomeHandler, *fiber.App, string, string, string) {
	t.Helper()
	root := t.TempDir()
	backend, err := storage.NewLocalBackend(root, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = backend.Close() })
	const key = "db/cpu/2026/11/18/23/cpu_20261119_000009_000000000.parquet"
	full := filepath.Join(root, key)
	if err := os.MkdirAll(filepath.Dir(full), 0o700); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(fmt.Sprintf(`COPY (SELECT * FROM (VALUES
		(TIMESTAMPTZ '2026-11-18 23:10:00+00', 1), (TIMESTAMPTZ '2026-11-19 05:00:00+00', 2), (TIMESTAMPTZ '2026-11-19 05:30:00+00', 3)) v("time", n))
		TO '%s' (FORMAT PARQUET)`, full)); err != nil {
		t.Fatal(err)
	}

	am, err := auth.NewAuthManager(filepath.Join(t.TempDir(), "auth.db"), 5*time.Minute, 100, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { am.Close() })
	ctx := context.Background()
	admin, err := am.CreateToken(ctx, "admin-rehome", "", "read,write,delete,admin", nil)
	if err != nil {
		t.Fatal(err)
	}
	writer, err := am.CreateToken(ctx, "writer-rehome", "", "read,write,delete", nil)
	if err != nil {
		t.Fatal(err)
	}
	app := fiber.New()
	mc := auth.DefaultMiddlewareConfig()
	mc.AuthManager = am
	app.Use(auth.NewMiddleware(mc))
	h := NewRehomeHandler(backend, am, zerolog.Nop())
	h.RegisterRoutes(app)
	return h, app, key, admin, writer
}

func rehomeDo(t *testing.T, app *fiber.App, method, url, token, body string) (int, map[string]interface{}) {
	t.Helper()
	var rd io.Reader
	if body != "" {
		rd = strings.NewReader(body)
	}
	req := httptest.NewRequest(method, url, rd)
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := app.Test(req, -1)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := io.ReadAll(resp.Body)
	var out map[string]interface{}
	_ = json.Unmarshal(raw, &out)
	return resp.StatusCode, out
}

// A dry run reports where a file's rows belong and changes nothing; only an
// admin may ask, and only for a data file that exists.
func TestRehomeDryRun1825(t *testing.T) {
	h, app, key, admin, writer := rehomeRig(t)
	body := fmt.Sprintf(`{"path":%q,"dry_run":true}`, key)

	for _, c := range []struct {
		method, url, body string
	}{{"POST", "/api/v1/rehome", body}, {"GET", "/api/v1/rehome", ""}, {"GET", "/api/v1/rehome/x", ""}} {
		if code, _ := rehomeDo(t, app, c.method, c.url, "", c.body); code != 401 {
			t.Errorf("%s %s without a token: %d, want 401", c.method, c.url, code)
		}
		if code, _ := rehomeDo(t, app, c.method, c.url, writer, c.body); code != 403 {
			t.Errorf("%s %s with a token that is not admin: %d, want 403", c.method, c.url, code)
		}
	}

	for name, c := range map[string]struct {
		body string
		code int
	}{
		"not a dry run":      {fmt.Sprintf(`{"path":%q}`, key), 409},
		"path traversal":     {`{"path":"db/cpu/2026/11/18/23/../../../../../etc/passwd","dry_run":true}`, 400},
		"absolute path":      {`{"path":"/etc/passwd","dry_run":true}`, 400},
		"reserved directory": {`{"path":"_schema/cpu/2026/11/18/23/f.parquet","dry_run":true}`, 400},
		"not a data shape":   {`{"path":"db/cpu/f.parquet","dry_run":true}`, 400},
		"not parquet":        {`{"path":"db/cpu/2026/11/18/23/f.json","dry_run":true}`, 400},
		"missing file":       {`{"path":"db/cpu/2026/11/18/23/nope.parquet","dry_run":true}`, 404},
		"not JSON":           {`path=x`, 400},
	} {
		if code, out := rehomeDo(t, app, "POST", "/api/v1/rehome", admin, c.body); code != c.code || out["error"] == nil {
			t.Errorf("%s: %d %v, want %d with a reason", name, code, out, c.code)
		}
	}

	code, started := rehomeDo(t, app, "POST", "/api/v1/rehome", admin, body)
	if code != 202 || started["state"] != "running" || started["id"] == "" {
		t.Fatalf("start: %d %v", code, started)
	}
	id := started["id"].(string)
	var job map[string]interface{}
	for i := 0; i < 200; i++ {
		_, job = rehomeDo(t, app, "GET", "/api/v1/rehome/"+id, admin, "")
		if job["state"] != "running" {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if job["state"] != "done" {
		t.Fatalf("job: %v", job)
	}
	r := job["report"].(map[string]interface{})
	outs := r["outputs"].([]interface{})
	if r["rows"] != float64(3) || r["misfiled"] != true || r["misfiled_rows"] != float64(2) || r["shape"] != "hour" || len(outs) != 2 {
		t.Fatalf("report: %v", r)
	}
	second := outs[1].(map[string]interface{})
	if second["dir"] != "db/cpu/2026/11/19/05" || second["rows"] != float64(2) || second["min_time"] != "2026-11-19T05:00:00Z" {
		t.Fatalf("second output: %v", second)
	}
	if _, local := second["LocalPath"]; local || strings.Contains(fmt.Sprint(job), os.TempDir()) {
		t.Fatalf("the report names a path on this node's disk: %v", job)
	}
	// Nothing changed: storage holds the one file, unchanged in size.
	files, err := h.storage.List(context.Background(), "")
	if err != nil || len(files) != 1 || files[0] != key {
		t.Fatalf("storage after a dry run: %v %v", files, err)
	}
	if _, list := rehomeDo(t, app, "GET", "/api/v1/rehome", admin, ""); list["count"] != float64(1) {
		t.Fatalf("list: %v", list)
	}
	if code, _ := rehomeDo(t, app, "GET", "/api/v1/rehome/nope", admin, ""); code != 404 {
		t.Fatalf("unknown job: %d", code)
	}
}

// One job runs at a time, and a job that panics is reported, not left
// running for good.
func TestRehomeOneJobAtATime1825(t *testing.T) {
	h, app, key, admin, _ := rehomeRig(t)
	body := fmt.Sprintf(`{"path":%q,"dry_run":true}`, key)
	release := make(chan struct{})
	h.run = func(_ context.Context, job *rehomeJob) {
		<-release
		panic("boom")
	}
	code, first := rehomeDo(t, app, "POST", "/api/v1/rehome", admin, body)
	if code != 202 {
		t.Fatalf("first: %d", code)
	}
	if code, out := rehomeDo(t, app, "POST", "/api/v1/rehome", admin, body); code != 409 || out["error"] == nil {
		t.Fatalf("a second job while one runs: %d %v", code, out)
	}
	close(release)
	var job map[string]interface{}
	for i := 0; i < 200; i++ {
		_, job = rehomeDo(t, app, "GET", "/api/v1/rehome/"+first["id"].(string), admin, "")
		if job["state"] != "running" {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if job["state"] != "failed" || !strings.Contains(fmt.Sprint(job["reason"]), "boom") {
		t.Fatalf("a job that panicked: %v", job)
	}
	h.run = h.dryRun
	for i := 0; i < 100; i++ {
		if code, _ = rehomeDo(t, app, "POST", "/api/v1/rehome", admin, body); code == 202 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if code != 202 {
		t.Fatalf("after the first job ended, a new one: %d", code)
	}
}
