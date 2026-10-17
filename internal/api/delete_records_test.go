package api

import (
	"context"
	"encoding/json"
	"errors"
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

	"github.com/basekick-labs/arc/internal/cluster"
	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/database"
	"github.com/basekick-labs/arc/internal/storage"
)

// #1207 d1: DELETE records on the API side.

func TestPinDeleteTime(t *testing.T) {
	now := time.Date(2026, 10, 13, 4, 5, 6, 789000000, time.FixedZone("x", -6*3600))
	cases := map[string]string{
		"time < now() - INTERVAL 30 DAY":     "time < TIMESTAMPTZ '2026-10-13 10:05:06.789+00' - INTERVAL 30 DAY",
		"time < NOW ( )":                     "time < TIMESTAMPTZ '2026-10-13 10:05:06.789+00'",
		"time < current_timestamp":           "time < TIMESTAMPTZ '2026-10-13 10:05:06.789+00'",
		"time < get_current_timestamp()":     "time < TIMESTAMPTZ '2026-10-13 10:05:06.789+00'",
		"d = current_date OR d = today()":    "d = DATE '2026-10-13' OR d = DATE '2026-10-13'",
		"t > current_time":                   "t > TIME '10:05:06.789'",
		"t > localtimestamp":                 "t > TIMESTAMP '2026-10-13 10:05:06.789'",
		"t > current_localtimestamp()":       "t > TIMESTAMP '2026-10-13 10:05:06.789'",
		"t > current_localtime()":            "t > TIME '10:05:06.789'",
		"host = 'now()' AND x < now()":       "host = 'now()' AND x < TIMESTAMPTZ '2026-10-13 10:05:06.789+00'",
		`"now" = 1`:                          `"now" = 1`,
		"current_timestamp_col = 1":          "current_timestamp_col = 1",
		"host = 'a'":                         "host = 'a'",
		"my_now() > 1":                       "my_now() > 1",
		"time < now() AND tag = 'it''s now'": "time < TIMESTAMPTZ '2026-10-13 10:05:06.789+00' AND tag = 'it''s now'",
	}
	for in, want := range cases {
		got, err := pinDeleteTime(in, now)
		if err != nil || got != want {
			t.Errorf("pin(%q) = %q, %v; want %q", in, got, err, want)
		}
	}
	for _, in := range []string{"random() < 0.5", "x = uuid()", "x = gen_random_uuid ()", "x = nextval('s')"} {
		if _, err := pinDeleteTime(in, now); err == nil {
			t.Errorf("pin(%q) accepted", in)
		}
	}
	if _, err := pinDeleteTime("host = 'random()'", now); err != nil {
		t.Errorf("a string literal was refused: %v", err)
	}
}

func recordsDuckDB(t *testing.T) (string, *storage.LocalBackend, *database.DuckDB) {
	t.Helper()
	root := t.TempDir()
	backend, err := storage.NewLocalBackend(root, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = backend.Close() })
	db, err := database.New(&database.Config{MemoryLimit: "256MB", ThreadCount: 1, MaxConnections: 1, LocalStorageRoot: root}, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return root, backend, db
}

func writeRecordsParquet(t *testing.T, db *database.DuckDB, root, rel string) {
	t.Helper()
	full := filepath.Join(root, rel)
	if err := os.MkdirAll(filepath.Dir(full), 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := db.DB().Exec(fmt.Sprintf(`COPY (SELECT * FROM (VALUES ('h1', 1), ('h2', 7), ('h3', 3), ('h1', 9)) AS v(host, v)) TO '%s' (FORMAT PARQUET)`, full)); err != nil {
		t.Fatal(err)
	}
}

func countParquet(t *testing.T, db *database.DuckDB, full, where string) int {
	t.Helper()
	var n int
	if err := db.DB().QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM read_parquet('%s') WHERE %s", full, where)).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestRewriteForRecords(t *testing.T) {
	root, backend, db := recordsDuckDB(t)
	const rel = "db/cpu/2026/10/11/10/cpu_a.parquet"
	writeRecordsParquet(t, db, root, rel)
	h := &DeleteHandler{db: db, storage: backend, logger: zerolog.Nop()}
	ctx := context.Background()

	res, err := h.RewriteForRecords(ctx, rel, []string{"host = 'h1'", "v = 3"})
	if err != nil {
		t.Fatal(err)
	}
	if res.Matched != 3 || res.Left != 1 || res.NewPath == "" || res.NewPath == rel || res.Size <= 0 || res.SHA256 == "" {
		t.Fatalf("result %+v", res)
	}
	if n := countParquet(t, db, filepath.Join(root, res.NewPath), "TRUE"); n != 1 {
		t.Fatalf("rewrite holds %d rows", n)
	}
	if n := countParquet(t, db, filepath.Join(root, rel), "TRUE"); n != 4 {
		t.Fatalf("the original was changed: %d rows", n)
	}
	// Nothing matches: no file written.
	if res, err = h.RewriteForRecords(ctx, rel, []string{"host = 'zz'"}); err != nil || res.Matched != 0 || res.Left != 4 || res.NewPath != "" {
		t.Fatalf("no match: %+v %v", res, err)
	}
	// A column the file lacks matches nothing in it, as the DELETE skips it,
	// and does not keep another record from applying.
	if res, err = h.RewriteForRecords(ctx, rel, []string{"nope = 1"}); err != nil || res.Matched != 0 || res.NewPath != "" {
		t.Fatalf("unbound column: %+v %v", res, err)
	}
	if res, err = h.RewriteForRecords(ctx, rel, []string{"nope = 1", "host = 'h2'"}); err != nil || res.Matched != 1 || res.Left != 3 {
		t.Fatalf("one record unbound: %+v %v", res, err)
	}
	// Every row (a database deletion): nothing left, no file.
	if res, err = h.RewriteForRecords(ctx, rel, []string{"v > 100", ""}); err != nil || res.Matched != 4 || res.Left != 0 || res.NewPath != "" {
		t.Fatalf("every row: %+v %v", res, err)
	}
	// A DELETE is rewriting: try again later.
	h.rewriteMu.Lock()
	_, err = h.RewriteForRecords(ctx, rel, []string{"v = 1"})
	h.rewriteMu.Unlock()
	if !errors.Is(err, cluster.ErrDeleteRewriteBusy) {
		t.Fatalf("busy: %v", err)
	}
	// A missing file is an error, retried.
	if _, err = h.RewriteForRecords(ctx, "db/cpu/2026/10/11/10/gone.parquet", []string{"v = 1"}); err == nil {
		t.Fatal("a missing file rewrote")
	}
}

// recordingCoordinator is a primary with stream takeover on.
type recordingCoordinator struct {
	rewriteManifestCoordinator
	active   bool
	events   *[]string
	missing  []string
	recErr   error
	onRecord func()
	marks    map[string]uint64
}

func (c *recordingCoordinator) DeleteRecordsActive() bool { return c.active }
func (c *recordingCoordinator) RecordDelete(_ context.Context, db, m, where string, marks map[string]uint64) (uint64, error) {
	*c.events = append(*c.events, "record "+db+"/"+m+" "+where)
	c.marks = marks
	if c.onRecord != nil {
		c.onRecord()
	}
	return 7, c.recErr
}
func (c *recordingCoordinator) WaitLocalFiles(_ context.Context, db, m string, _ time.Duration) ([]string, error) {
	*c.events = append(*c.events, "wait "+db+"/"+m)
	return c.missing, nil
}

type recordingFlusher struct {
	events  *[]string
	failure bool
	err     error
}

func (f *recordingFlusher) FlushScopeAndWait(_ context.Context, db, m string) (map[string]uint64, error) {
	*f.events = append(*f.events, "flush "+db+"/"+m)
	return map[string]uint64{db + "/" + m: 4}, f.err
}
func (f *recordingFlusher) HasFlushFailure() bool { return f.failure }

func TestDeleteRecordsTheDeleteBeforeRewriting(t *testing.T) {
	root, backend, db := recordsDuckDB(t)
	const rel = "db/cpu/2026/10/11/10/cpu_a.parquet"
	send := func(h *DeleteHandler, body string) (int, DeleteResponse) {
		t.Helper()
		app := fiber.New()
		h.RegisterRoutes(app)
		req := httptest.NewRequest("POST", "/api/v1/delete", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		resp, err := app.Test(req, 20000)
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := io.ReadAll(resp.Body)
		var out DeleteResponse
		_ = json.Unmarshal(raw, &out)
		return resp.StatusCode, out
	}
	setup := func(active bool, missing []string) (*DeleteHandler, *[]string, *recordingCoordinator, *recordingFlusher) {
		if err := os.RemoveAll(filepath.Join(root, "db")); err != nil {
			t.Fatal(err)
		}
		writeRecordsParquet(t, db, root, rel)
		events := &[]string{}
		c := &recordingCoordinator{active: active, events: events, missing: missing}
		f := &recordingFlusher{events: events}
		h := NewDeleteHandler(db, backend, &config.DeleteConfig{Enabled: true, MaxRowsPerDelete: 1000, ConfirmationThreshold: 1000}, nil, t.TempDir(), zerolog.Nop())
		h.SetCoordinator(c)
		h.SetFlusher(f)
		return h, events, c, f
	}
	body := `{"database":"db","measurement":"cpu","where":"host = 'h1' AND v < 100","confirm":true}`

	h, events, rc, _ := setup(true, nil)
	invalidated := 0
	h.SetOnFilesRemoved(func() { invalidated++ })
	code, out := send(h, body)
	if code != 200 || out.DeletedCount != 2 {
		t.Fatalf("delete: %d %+v", code, out)
	}
	// The rewrite invalidates the query caches (a cached pruned list could
	// name a file it removed).
	if invalidated != 1 {
		t.Fatalf("query caches invalidated %d times, want 1", invalidated)
	}
	if rc.marks["db/cpu"] != 4 {
		t.Fatalf("the record does not carry the flush marks: %v", rc.marks)
	}
	want := "flush db/cpu|wait db/cpu|record db/cpu host = 'h1' AND v < 100|wait db/cpu"
	if got := strings.Join(*events, "|"); got != want {
		t.Fatalf("order\n got %s\nwant %s", got, want)
	}

	// now() is fixed before it is recorded and run: with the DELETE's clock
	// in 2000, every row is "after now".
	deleteNow = func() time.Time { return time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC) }
	t.Cleanup(func() { deleteNow = time.Now })
	h, events, _, _ = setup(true, nil)
	if code, out = send(h, `{"database":"db","measurement":"cpu","where":"TIMESTAMPTZ '2020-01-01 00:00:00+00' > now() AND v < 8","confirm":true}`); code != 200 || out.DeletedCount != 3 {
		t.Fatalf("now(): %d %+v", code, out)
	}
	deleteNow = time.Now
	if rec := (*events)[2]; !strings.Contains(rec, "TIMESTAMPTZ '") || strings.Contains(rec, "now()") {
		t.Fatalf("recorded %q", rec)
	}
	h, events, _, _ = setup(true, nil)
	if code, _ = send(h, `{"database":"db","measurement":"cpu","where":"random() < 2","confirm":true}`); code != 400 || len(*events) != 0 {
		t.Fatalf("random(): %d %v", code, *events)
	}

	// A file that reached this node between the first scan and the record
	// is scanned too.
	h, _, c, _ := setup(true, nil)
	const late = "db/cpu/2026/10/11/11/cpu_b.parquet"
	c.onRecord = func() { writeRecordsParquet(t, db, root, late) }
	if code, out = send(h, body); code != 200 || out.DeletedCount != 4 {
		t.Fatalf("late file: %d %+v", code, out)
	}

	// Files this node lacks: refused before recording, unless allowed.
	h, events, _, _ = setup(true, []string{"db/cpu/x.parquet"})
	if code, out = send(h, body); code != 503 || len(out.UnreachableFiles) != 1 || strings.Contains(strings.Join(*events, "|"), "record") {
		t.Fatalf("unreachable: %d %+v %v", code, out, *events)
	}
	h, _, _, _ = setup(true, []string{"db/cpu/x.parquet"})
	if code, out = send(h, strings.Replace(body, `"confirm":true`, `"confirm":true,"allow_unreachable":true`, 1)); code != 200 || len(out.UnreachableFiles) != 1 {
		t.Fatalf("allowed: %d %+v", code, out)
	}

	// Not recorded: refused, nothing rewritten.
	h, _, c, _ = setup(true, nil)
	c.recErr = errors.New("full")
	if code, _ = send(h, body); code != 503 || len(c.ops) != 0 {
		t.Fatalf("record failed: %d ops %d", code, len(c.ops))
	}

	// Over the row cap: refused before anything is recorded.
	h, events, _, _ = setup(true, nil)
	h.config.MaxRowsPerDelete = 1
	if code, _ = send(h, body); code != 400 || strings.Contains(strings.Join(*events, "|"), "record") {
		t.Fatalf("row cap: %d %v", code, *events)
	}

	// A dry run neither flushes nor records.
	h, events, _, _ = setup(true, nil)
	if code, _ = send(h, `{"database":"db","measurement":"cpu","where":"host = 'h1'","dry_run":true}`); code != 200 || len(*events) != 0 {
		t.Fatalf("dry run: %d %v", code, *events)
	}

	// Takeover off: still flushes first, records nothing.
	h, events, _, _ = setup(false, nil)
	if code, _ = send(h, body); code != 200 || strings.Join(*events, "|") != "flush db/cpu" {
		t.Fatalf("takeover off: %d %v", code, *events)
	}
	// An outstanding flush failure refuses it.
	h, _, _, f := setup(false, nil)
	f.failure = true
	if code, _ = send(h, body); code != 503 {
		t.Fatalf("flush failure: %d", code)
	}
}

type recordingPending struct{ events *[]string }

func (p recordingPending) WithdrawDatabase(_ context.Context, db string, _ uint64) error {
	*p.events = append(*p.events, "withdraw "+db)
	return nil
}

type recordingDatabaseCoordinator struct {
	databaseDeleteCoordinatorFake
	events *[]string
}

func (c *recordingDatabaseCoordinator) DeleteRecordsActive() bool { return true }
func (c *recordingDatabaseCoordinator) RecordDelete(_ context.Context, db, m, where string, _ map[string]uint64) (uint64, error) {
	*c.events = append(*c.events, "record "+db+"/"+m+" "+where)
	return 7, nil
}
func (c *recordingDatabaseCoordinator) WaitLocalFiles(context.Context, string, string, time.Duration) ([]string, error) {
	return nil, nil
}

func TestDatabaseDeletionFlushesRecordsAndWithdraws(t *testing.T) {
	handler, app, tmpDir := setupTestDatabasesHandler(t, true)
	t.Cleanup(func() { _ = os.RemoveAll(tmpDir) })
	ctx := context.Background()
	if err := handler.storage.Write(ctx, "db/.arc-database", []byte("marker")); err != nil {
		t.Fatal(err)
	}
	events := &[]string{}
	handler.SetCoordinator(&recordingDatabaseCoordinator{databaseDeleteCoordinatorFake: databaseDeleteCoordinatorFake{primary: true, role: "writer"}, events: events}, true)
	handler.SetFlusher(&recordingFlusher{events: events})
	handler.SetPendingRegistrations(recordingPending{events: events})
	resp, err := app.Test(httptest.NewRequest("DELETE", "/api/v1/databases/db?confirm=true", nil), testRequestTimeoutMS)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 {
		raw, _ := io.ReadAll(resp.Body)
		t.Fatalf("status %d %s", resp.StatusCode, raw)
	}
	if got := strings.Join(*events, "|"); got != "flush db/|record db/ |withdraw db" {
		t.Fatalf("events %s", got)
	}
}

// recordingOutbox is a pending-registration outbox with the DELETE-record
// calls.
type recordingOutbox struct {
	events *[]string
	paths  map[string]bool
}

func (o *recordingOutbox) ReplacePending(_ context.Context, oldPath, newPath string, _ int64, _ string) (cluster.ReplaceOutcome, raft.FileEntry, error) {
	if o.paths[oldPath] {
		*o.events = append(*o.events, "replace "+filepath.Base(oldPath))
		return cluster.ReplaceSwapped, raft.FileEntry{}, nil
	}
	return cluster.ReplaceNotPending, raft.FileEntry{}, nil
}
func (o *recordingOutbox) Withdraw(_ context.Context, path string) (bool, cluster.ReplaceOutcome, error) {
	*o.events = append(*o.events, "withdraw "+filepath.Base(path))
	return o.paths[path], cluster.ReplaceSwapped, nil
}
func (o *recordingOutbox) SetDeleteSeen(path string, record uint64) {
	*o.events = append(*o.events, fmt.Sprintf("seen %d", record))
}
func (o *recordingOutbox) WithdrawForDelete(_ context.Context, path string, record uint64) (bool, cluster.ReplaceOutcome, error) {
	*o.events = append(*o.events, fmt.Sprintf("withdraw-for-delete %s %d", filepath.Base(path), record))
	return o.paths[path], cluster.ReplaceSwapped, nil
}

// A pending file the DELETE rewrites is marked as written with its record;
// one it empties is withdrawn with the record, so other writers' batches in
// it settle as deleted.
func TestDeleteMarksPendingFilesWithItsRecord(t *testing.T) {
	root, backend, db := recordsDuckDB(t)
	const rel = "db/cpu/2026/10/11/10/cpu_a.parquet"
	for _, tc := range []struct{ where, want string }{
		{"host = 'h1'", "seen 7"},
		{"v > 0", "withdraw-for-delete cpu_a.parquet 7"},
	} {
		if err := os.RemoveAll(filepath.Join(root, "db")); err != nil {
			t.Fatal(err)
		}
		writeRecordsParquet(t, db, root, rel)
		events := &[]string{}
		h := NewDeleteHandler(db, backend, &config.DeleteConfig{Enabled: true, MaxRowsPerDelete: 1000, ConfirmationThreshold: 1000}, nil, t.TempDir(), zerolog.Nop())
		h.SetCoordinator(&recordingCoordinator{active: true, events: &[]string{}})
		h.SetPendingRegistrations(&recordingOutbox{events: events, paths: map[string]bool{rel: true}})
		app := fiber.New()
		h.RegisterRoutes(app)
		req := httptest.NewRequest("POST", "/api/v1/delete", strings.NewReader(`{"database":"db","measurement":"cpu","where":"`+tc.where+`","confirm":true}`))
		req.Header.Set("Content-Type", "application/json")
		resp, err := app.Test(req, 20000)
		if err != nil || resp.StatusCode != 200 {
			t.Fatalf("%s: %v %v", tc.where, resp.StatusCode, err)
		}
		if got := strings.Join(*events, "|"); !strings.Contains(got, tc.want) {
			t.Fatalf("%s: events %s", tc.where, got)
		}
	}
}

type failingDatabasePending struct{}

func (failingDatabasePending) WithdrawDatabase(context.Context, string, uint64) error {
	return errors.New("in flight")
}

// Pending registrations it cannot withdraw stop a database deletion before
// any file is removed.
func TestDatabaseDeletionStopsWhenWithdrawFails(t *testing.T) {
	handler, app, tmpDir := setupTestDatabasesHandler(t, true)
	t.Cleanup(func() { _ = os.RemoveAll(tmpDir) })
	ctx := context.Background()
	if err := handler.storage.Write(ctx, "db/.arc-database", []byte("marker")); err != nil {
		t.Fatal(err)
	}
	if err := handler.storage.Write(ctx, "db/cpu/a.parquet", []byte("x")); err != nil {
		t.Fatal(err)
	}
	handler.SetPendingRegistrations(failingDatabasePending{})
	resp, err := app.Test(httptest.NewRequest("DELETE", "/api/v1/databases/db?confirm=true", nil), testRequestTimeoutMS)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 503 {
		t.Fatalf("status %d", resp.StatusCode)
	}
	if size, _ := handler.storage.StatFile(ctx, "db/cpu/a.parquet"); size < 0 {
		t.Fatal("a file was removed")
	}
}
