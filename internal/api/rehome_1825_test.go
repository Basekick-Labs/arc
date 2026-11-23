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
	return rehomeRigAt(t, t.TempDir())
}

func rehomeRigAt(t *testing.T, scratch string) (*RehomeHandler, *fiber.App, string, string, string) {
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
	h := NewRehomeHandler(backend, am, scratch, zerolog.Nop())
	t.Cleanup(func() { _ = h.Shutdown(context.Background()) })
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
	scratch := t.TempDir()
	// Left by a process that stopped mid-job; and a directory that is not ours.
	for _, d := range []string{"rehome-leftover", "delete-rewrite-other"} {
		if err := os.MkdirAll(filepath.Join(scratch, d, "x"), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	h, app, key, admin, writer := rehomeRigAt(t, scratch)
	if left, _ := os.ReadDir(scratch); len(left) != 1 || left[0].Name() != "delete-rewrite-other" {
		t.Fatalf("after start the scratch root holds %v, want only the directory that is not a re-home one", left)
	}
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
		"not a dry run":      {fmt.Sprintf(`{"path":%q}`, key), 400},
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
	if r["rows"] != float64(3) || r["misfiled"] != true || r["misfiled_rows"] != float64(2) || r["shape"] != "hour" || len(outs) != 2 ||
		r["split_checked"] != true || r["destinations_total"] != float64(2) || len(r["destinations"].([]interface{})) != 2 {
		t.Fatalf("report: %v", r)
	}
	second := outs[1].(map[string]interface{})
	if second["dir"] != "db/cpu/2026/11/19/05" || second["rows"] != float64(2) || second["min_time"] != "2026-11-19T05:00:00Z" {
		t.Fatalf("second output: %v", second)
	}
	if _, local := second["LocalPath"]; local || strings.Contains(fmt.Sprint(job), scratch) {
		t.Fatalf("the report names a path on this node's disk: %v", job)
	}
	// Nothing changed: storage holds the one file, unchanged in size.
	files, err := h.storage.List(context.Background(), "")
	if err != nil || len(files) != 1 || files[0] != key {
		t.Fatalf("storage after a dry run: %v %v", files, err)
	}
	_, list := rehomeDo(t, app, "GET", "/api/v1/rehome", admin, "")
	listed := list["jobs"].([]interface{})[0].(map[string]interface{})["report"].(map[string]interface{})
	if list["count"] != float64(1) || listed["rows"] != float64(3) || listed["outputs"] != nil || listed["destinations"] != nil {
		t.Fatalf("list: %v", list)
	}
	// The job's scratch directory is gone, and the slot is free as soon as
	// the job reads done.
	if left, _ := os.ReadDir(scratch); len(left) != 1 {
		t.Fatalf("after the job the scratch root holds %v", left)
	}
	if code, again := rehomeDo(t, app, "POST", "/api/v1/rehome", admin, body); code != 202 {
		t.Fatalf("a job right after the first read done: %d %v", code, again)
	}
	_ = h.Shutdown(context.Background())
	if code, _ := rehomeDo(t, app, "GET", "/api/v1/rehome/nope", admin, ""); code != 404 {
		t.Fatalf("unknown job: %d", code)
	}
}

// A job that fails says which step failed, and nothing about this node: not
// the scratch path, not the statement.
func TestRehomeFailureReasonNamesNoPath1825(t *testing.T) {
	scratch := t.TempDir()
	h, app, key, admin, _ := rehomeRigAt(t, scratch)
	bad := strings.Replace(key, "cpu_2026", "bad_2026", 1)
	if err := h.storage.Write(context.Background(), bad, []byte("PAR1 not a parquet file PAR1")); err != nil {
		t.Fatal(err)
	}
	_, started := rehomeDo(t, app, "POST", "/api/v1/rehome", admin, fmt.Sprintf(`{"path":%q,"dry_run":true}`, bad))
	var job map[string]interface{}
	for i := 0; i < 200; i++ {
		_, job = rehomeDo(t, app, "GET", "/api/v1/rehome/"+started["id"].(string), admin, "")
		if job["state"] != "running" {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	reason := fmt.Sprint(job["reason"])
	if job["state"] != "failed" || !strings.Contains(reason, "schema and times") {
		t.Fatalf("job: %v", job)
	}
	for _, leak := range []string{scratch, "source.parquet", "read_parquet", "SELECT", os.TempDir()} {
		if strings.Contains(reason, leak) {
			t.Errorf("the reason names %q: %s", leak, reason)
		}
	}
	if left, _ := os.ReadDir(scratch); len(left) != 0 {
		t.Errorf("a failed job left %v in the scratch root", left)
	}
}

// A file scattered over more hours than a split writes files for still gets
// its plan; the split, which is the check on it, is skipped and says so.
func TestRehomeReportsThePlanOfAVeryScatteredFile1825(t *testing.T) {
	h, app, key, admin, _ := rehomeRig(t)
	wide := strings.Replace(key, "cpu_2026", "wide_2026", 1)
	root := filepath.Dir(filepath.Dir(filepath.Dir(filepath.Dir(filepath.Dir(filepath.Dir(filepath.Dir(mustURI(t, h.storage, key))))))))
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(fmt.Sprintf(`COPY (SELECT make_timestamptz(1795000000000000 + i * 3600000000) AS "time", i AS n FROM range(600) t(i)) TO '%s' (FORMAT PARQUET)`, filepath.Join(root, wide))); err != nil {
		t.Fatal(err)
	}
	_, started := rehomeDo(t, app, "POST", "/api/v1/rehome", admin, fmt.Sprintf(`{"path":%q,"dry_run":true}`, wide))
	var job map[string]interface{}
	for i := 0; i < 400; i++ {
		_, job = rehomeDo(t, app, "GET", "/api/v1/rehome/"+started["id"].(string), admin, "")
		if job["state"] != "running" {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if job["state"] != "done" {
		t.Fatalf("job: %v", job)
	}
	r := job["report"].(map[string]interface{})
	if r["destinations_total"] != float64(600) || r["split_checked"] != false || !strings.Contains(fmt.Sprint(r["split_skipped"]), "600") || r["outputs"] != nil || r["rows"] != float64(600) {
		t.Fatalf("report: %v", r)
	}
}

func mustURI(t *testing.T, b storage.Backend, key string) string {
	t.Helper()
	u, err := storage.ObjectURI(b, key)
	if err != nil {
		t.Fatal(err)
	}
	return u
}

// One job runs at a time, and a job that panics is reported, not left
// running for good.
func TestRehomeOneJobAtATime1825(t *testing.T) {
	h, app, key, admin, _ := rehomeRig(t)
	body := fmt.Sprintf(`{"path":%q,"dry_run":true}`, key)
	release := make(chan struct{})
	h.run = func(context.Context, *rehomeJob) (string, string, *rehomeReport) {
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
	if job["state"] != "failed" || strings.Contains(fmt.Sprint(job["reason"]), "boom") || job["reason"] == "" {
		t.Fatalf("a job that panicked: %v (the reason must not carry the panic's text)", job)
	}
	// The slot is free once the job reads failed: no retry needed.
	h.run = func(context.Context, *rehomeJob) (string, string, *rehomeReport) { return "done", "", nil }
	if code, _ = rehomeDo(t, app, "POST", "/api/v1/rehome", admin, body); code != 202 {
		t.Fatalf("after the first job ended, a new one: %d", code)
	}

	// A shutdown ends a running job and waits for it.
	_ = h.Shutdown(context.Background())
	started := make(chan struct{})
	h.ctx, h.stop = context.WithCancel(context.Background())
	h.run = func(ctx context.Context, _ *rehomeJob) (string, string, *rehomeReport) {
		close(started)
		<-ctx.Done()
		return "failed", "stopped", nil
	}
	if code, _ = rehomeDo(t, app, "POST", "/api/v1/rehome", admin, body); code != 202 {
		t.Fatalf("job before shutdown: %d", code)
	}
	<-started
	if err := h.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if h.running {
		t.Fatal("Shutdown returned while the job still held the slot")
	}
}
