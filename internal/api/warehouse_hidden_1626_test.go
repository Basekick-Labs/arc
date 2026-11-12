package api

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/Basekick-Labs/msgpack/v6"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/database"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/basekick-labs/arc/internal/tiering"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// warehouseTree writes a storage root holding a real database, a subdirectory warehouse lv_wh,
// a sibling database sharing its prefix, a root-level namespace dir, and a database "a" whose
// measurement "b" holds a nested warehouse.
func warehouseTree(t *testing.T) *storage.LocalBackend {
	t.Helper()
	b, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })
	ctx := context.Background()
	for _, k := range []string{
		"mydb/cpu/2026/07/14/15/a.parquet",
		"lv_wh-other/cpu/2026/07/14/15/a.parquet",
		"lv_wh/arc_mydb.db/cpu/metadata/v1.metadata.json",
		"arc_mydb.db/cpu/metadata/v1.metadata.json",
		"a/mem/2026/07/14/15/a.parquet",
		"a/b/arc_x.db/t/metadata/v1.metadata.json",
	} {
		if err := b.Write(ctx, k, []byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	return b
}

func doJSON(t *testing.T, app *fiber.App, method, path, body string) (int, string) {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := app.Test(req, testRequestTimeoutMS)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	out, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(out)
}

func warehouseListed(t *testing.T, app *fiber.App) map[string]int {
	t.Helper()
	status, body := doJSON(t, app, http.MethodGet, "/api/v1/databases", "")
	if status != fiber.StatusOK {
		t.Fatalf("list: %d %s", status, body)
	}
	var resp DatabaseListResponse
	if err := json.Unmarshal([]byte(body), &resp); err != nil {
		t.Fatal(err)
	}
	out := map[string]int{}
	for _, d := range resp.Databases {
		out[d.Name] = d.MeasurementCount
	}
	return out
}

// The database routes hide the warehouse's directories with Iceberg on, and only then (#1626).
func TestDatabaseRoutesHideWarehouse_Issue1626(t *testing.T) {
	backend := warehouseTree(t)
	newApp := func(l WarehouseDirs) *fiber.App {
		h := NewDatabasesHandler(backend, &config.DeleteConfig{Enabled: true}, nil, zerolog.Nop())
		if l != nil {
			h.SetWarehouseLayout(l)
		}
		app := fiber.New()
		h.RegisterRoutes(app)
		return app
	}

	off := warehouseListed(t, newApp(nil))
	if _, ok := off["lv_wh"]; !ok {
		t.Fatalf("Iceberg off: lv_wh not listed (%v); nothing may change without a layout", off)
	}

	app := newApp(warehouseAt("lv_wh"))
	got := warehouseListed(t, app)
	for _, hidden := range []string{"lv_wh", "arc_mydb.db"} {
		if _, ok := got[hidden]; ok {
			t.Errorf("GET /databases lists %q: %v", hidden, got)
		}
	}
	for _, shown := range []string{"mydb", "lv_wh-other", "a"} {
		if _, ok := got[shown]; !ok {
			t.Errorf("GET /databases does not list %q: %v", shown, got)
		}
	}

	for _, name := range []string{"lv_wh", "arc_mydb.db"} {
		if status, body := doJSON(t, app, http.MethodGet, "/api/v1/databases/"+name, ""); status != fiber.StatusNotFound {
			t.Errorf("GET %s: %d %s, want 404", name, status, body)
		}
		if status, body := doJSON(t, app, http.MethodGet, "/api/v1/databases/"+name+"/measurements", ""); status != fiber.StatusNotFound {
			t.Errorf("GET %s/measurements: %d %s, want 404", name, status, body)
		}
		if status, body := doJSON(t, app, http.MethodDelete, "/api/v1/databases/"+name+"?confirm=true", ""); status != fiber.StatusForbidden || !strings.Contains(body, "reserved") {
			t.Errorf("DELETE %s: %d %s, want 403 reserved", name, status, body)
		}
	}
	if ok, err := backend.Exists(context.Background(), "lv_wh/arc_mydb.db/cpu/metadata/v1.metadata.json"); err != nil || !ok {
		t.Fatalf("the warehouse metadata is gone after the refused deletes (exists=%v, err=%v)", ok, err)
	}
	if status, body := doJSON(t, app, http.MethodGet, "/api/v1/databases/lv_wh-other", ""); status != fiber.StatusOK {
		t.Errorf("GET lv_wh-other (a sibling): %d %s, want 200", status, body)
	}
	if status, body := doJSON(t, app, http.MethodPost, "/api/v1/databases", `{"name":"lv_wh"}`); status != fiber.StatusBadRequest || !strings.Contains(body, "reserved") {
		t.Errorf("POST lv_wh: %d %s, want 400 reserved", status, body)
	}
	if ok, _ := backend.Exists(context.Background(), "lv_wh/.arc-database"); ok {
		t.Error("the create wrote a marker inside the warehouse")
	}
}

// A nested warehouse <root>/a/b hides measurement b of a, in the list counts and the
// measurement listing; a itself stays a database.
func TestDatabaseRoutesHideNestedWarehouse_Issue1626(t *testing.T) {
	backend := warehouseTree(t)
	h := NewDatabasesHandler(backend, nil, nil, zerolog.Nop())
	h.SetWarehouseLayout(warehouseAt("a/b"))
	app := fiber.New()
	h.RegisterRoutes(app)

	if got := warehouseListed(t, app); got["a"] != 1 {
		t.Errorf("a counted %d measurements, want 1 (mem; b is the warehouse): %v", got["a"], got)
	}
	status, body := doJSON(t, app, http.MethodGet, "/api/v1/databases/a/measurements", "")
	if status != fiber.StatusOK || strings.Contains(body, `"b"`) || !strings.Contains(body, `"mem"`) {
		t.Errorf("a/measurements: %d %s, want mem and not b", status, body)
	}
	status, body = doJSON(t, app, http.MethodGet, "/api/v1/databases/a", "")
	if status != fiber.StatusOK || !strings.Contains(body, `"measurement_count":1`) {
		t.Errorf("GET a: %d %s, want 200 with one measurement", status, body)
	}
}

func newWarehouseQueryApp(t *testing.T, backend storage.Backend, l WarehouseDirs) *fiber.App {
	t.Helper()
	dir := t.TempDir()
	duck, err := database.New(&database.Config{MemoryLimit: "256MB", ThreadCount: 1, MaxConnections: 2, LocalStorageRoot: dir, TempDirectory: dir}, zerolog.Nop())
	if err != nil {
		t.Skipf("DuckDB: %v", err)
	}
	t.Cleanup(func() { duck.Close() })
	h := NewQueryHandler(duck, backend, zerolog.Nop(), 30, 1000)
	h.SetWarehouseLayout(l)
	app := fiber.New()
	h.RegisterRoutes(app)
	return app
}

func queryColumn(t *testing.T, app *fiber.App, sql string, col int) []string {
	t.Helper()
	b, _ := json.Marshal(QueryRequest{SQL: sql})
	status, body := doJSON(t, app, http.MethodPost, "/api/v1/query", string(b))
	if status != fiber.StatusOK {
		t.Fatalf("%s: %d %s", sql, status, body)
	}
	var out struct {
		Data [][]any `json:"data"`
	}
	if err := json.Unmarshal([]byte(body), &out); err != nil {
		t.Fatal(err)
	}
	names := make([]string, 0, len(out.Data))
	for _, row := range out.Data {
		names = append(names, row[col].(string))
	}
	sort.Strings(names)
	return names
}

// SHOW DATABASES, SHOW TABLES and GET /api/v1/measurements apply the same rule (#1626).
func TestQueryListingsHideWarehouse_Issue1626(t *testing.T) {
	backend := warehouseTree(t)

	app := newWarehouseQueryApp(t, backend, warehouseAt("lv_wh"))
	if got := strings.Join(queryColumn(t, app, "SHOW DATABASES", 0), ","); got != "a,lv_wh-other,mydb" {
		t.Errorf("SHOW DATABASES = %s, want a,lv_wh-other,mydb", got)
	}
	if got := queryColumn(t, app, "SHOW TABLES FROM lv_wh", 1); len(got) != 0 {
		t.Errorf("SHOW TABLES FROM lv_wh = %v, want none, as for an unknown database", got)
	}
	status, body := doJSON(t, app, http.MethodGet, "/api/v1/measurements", "")
	if status != fiber.StatusOK || strings.Contains(body, `"database":"lv_wh"`) || strings.Contains(body, `"database":"arc_mydb.db"`) || !strings.Contains(body, `"database":"mydb"`) {
		t.Errorf("GET /api/v1/measurements: %d %s", status, body)
	}

	nested := newWarehouseQueryApp(t, backend, warehouseAt("a/b"))
	if got := strings.Join(queryColumn(t, nested, "SHOW TABLES FROM a", 1), ","); got != "mem" {
		t.Errorf("SHOW TABLES FROM a = %s, want mem", got)
	}
	status, body = doJSON(t, nested, http.MethodGet, "/api/v1/measurements?database=a", "")
	if status != fiber.StatusOK || strings.Contains(body, `"measurement":"b"`) || !strings.Contains(body, `"measurement":"mem"`) {
		t.Errorf("GET /api/v1/measurements?database=a: %d %s", status, body)
	}
}

// setWarehouseRootName sets the writers' warehouse name for one test. Tests that use it must
// not run in parallel: the name is process-wide, as in production.
func setWarehouseRootName(t *testing.T, name string) {
	t.Helper()
	storage.SetWarehouseRootName(name)
	t.Cleanup(func() { storage.SetWarehouseRootName("") })
}

// Every writer refuses a database named after the subdirectory warehouse, with 400; another
// database is unaffected, and with no warehouse name set the name is accepted as before.
func TestIngestRefusesWarehouseDatabase_Issue1626(t *testing.T) {
	r := newLPWriteRig(t, nil)
	app := fiber.New()
	NewLineProtocolHandler(r.buf, zerolog.Nop()).RegisterRoutes(app)
	NewMsgPackHandler(zerolog.Nop(), r.buf, 10<<20).RegisterRoutes(app)
	imp := NewImportHandler(zerolog.Nop())
	imp.SetArrowBuffer(r.buf)
	imp.RegisterRoutes(app)

	lp := func(db string) (int, string) {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/write/line-protocol", strings.NewReader("cpu value=1 1700000000000000000\n"))
		req.Header.Set("x-arc-database", db)
		resp, err := app.Test(req, testRequestTimeoutMS)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(b)
	}
	mp := func(db string) (int, string) {
		payload, err := msgpack.Marshal([]interface{}{row("cpu", 1700000000, map[string]interface{}{"v": 1.5}, nil)})
		if err != nil {
			t.Fatal(err)
		}
		req := httptest.NewRequest(http.MethodPost, "/api/v1/write/msgpack", bytes.NewReader(payload))
		req.Header.Set("x-arc-database", db)
		req.Header.Set("Content-Type", "application/msgpack")
		resp, err := app.Test(req, testRequestTimeoutMS)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(b)
	}

	if status, body := lp("lv_wh"); status != fiber.StatusNoContent {
		t.Fatalf("no warehouse name set: LP into lv_wh %d %s, want 204 as before", status, body)
	}

	setWarehouseRootName(t, "lv_wh")
	const reserved = "reserved for the Iceberg warehouse"
	if status, body := lp("lv_wh"); status != fiber.StatusBadRequest || !strings.Contains(body, reserved) {
		t.Errorf("LP into lv_wh: %d %s, want 400 %q", status, body, reserved)
	}
	if status, body := mp("lv_wh"); status != fiber.StatusBadRequest || !strings.Contains(body, reserved) {
		t.Errorf("msgpack into lv_wh: %d %s, want 400 %q", status, body, reserved)
	}
	if status, body := mp("mydb"); status != fiber.StatusNoContent {
		t.Errorf("msgpack into mydb: %d %s, want 204", status, body)
	}
	if status, body := lp("lv_wh-other"); status != fiber.StatusNoContent {
		t.Errorf("LP into lv_wh-other: %d %s, want 204", status, body)
	}
	if status, body := lp("mydb"); status != fiber.StatusNoContent {
		t.Errorf("LP into mydb: %d %s, want 204", status, body)
	}

	NewTLEHandler(r.buf, zerolog.Nop()).RegisterRoutes(app)
	// CSV and Parquet imports honour their preamble's refusals: before #1626 a refused
	// preamble (whose JSON call returns nil) carried on with an empty database.
	for _, path := range []string{"/api/v1/import/csv?measurement=m", "/api/v1/import/parquet?measurement=m"} {
		var form bytes.Buffer
		mw := multipart.NewWriter(&form)
		fw, _ := mw.CreateFormFile("file", "data")
		_, _ = fw.Write([]byte("time,v\n1,1\n"))
		_ = mw.Close()
		req := httptest.NewRequest(http.MethodPost, path, &form)
		req.Header.Set("Content-Type", mw.FormDataContentType())
		req.Header.Set("x-arc-database", "1bad")
		resp, err := app.Test(req, testRequestTimeoutMS)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != fiber.StatusBadRequest || !strings.Contains(string(b), "invalid database name") {
			t.Errorf("%s into 1bad: %d %s, want 400 invalid database name", path, resp.StatusCode, b)
		}
	}
	for _, path := range []string{"/api/v1/import/lp", "/api/v1/import/tle", "/api/v1/import/csv?measurement=m", "/api/v1/import/parquet?measurement=m", "/api/v1/write/tle"} {
		var form bytes.Buffer
		mw := multipart.NewWriter(&form)
		fw, _ := mw.CreateFormFile("file", "data")
		_, _ = fw.Write([]byte("time,v\n1,1\n"))
		_ = mw.Close()
		req := httptest.NewRequest(http.MethodPost, path, &form)
		req.Header.Set("Content-Type", mw.FormDataContentType())
		req.Header.Set("x-arc-database", "lv_wh")
		resp, err := app.Test(req, testRequestTimeoutMS)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != fiber.StatusBadRequest || !strings.Contains(string(b), reserved) {
			t.Errorf("%s into lv_wh: %d %s, want 400 %q", path, resp.StatusCode, b, reserved)
		}
	}
}

// A continuous query cannot target the warehouse directory either: its results are a write.
func TestCQRefusesWarehouseDatabase_Issue1626(t *testing.T) {
	body := strings.Replace(cqBody, `"database":"db"`, `"database":"lv_wh"`, 1)
	decode := func() *ContinuousQueryRequest {
		var req ContinuousQueryRequest
		if err := json.Unmarshal([]byte(body), &req); err != nil {
			t.Fatal(err)
		}
		return &req
	}
	if err := validateCQDefinition(decode(), true); err != nil {
		t.Fatalf("no warehouse name set: %v", err)
	}
	setWarehouseRootName(t, "lv_wh")
	if err := validateCQDefinition(decode(), true); err == nil || !strings.Contains(err.Error(), "reserved for the Iceberg warehouse") {
		t.Fatalf("CQ into lv_wh: %v, want the warehouse refusal", err)
	}
}

// Cold-tier rows under the warehouse's names (a database of that name tiered before the
// warehouse was pointed there) do not bring them back into the list.
func TestDatabaseListColdRowsHideWarehouse_Issue1626(t *testing.T) {
	ctx := context.Background()
	backend := warehouseTree(t)
	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "arc.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	metadata, err := tiering.NewMetadataStore(db, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range [][2]string{{"lv_wh", "old"}, {"a", "b"}} {
		if _, err := metadata.RecordColdFile(ctx, &tiering.FileMetadata{
			Path: f[0] + "/" + f[1] + "/2026/01/01/00/a.parquet", Database: f[0], Measurement: f[1],
			PartitionTime: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), SizeBytes: 4, CreatedAt: time.Now(),
		}, time.Now()); err != nil {
			t.Fatal(err)
		}
	}
	for _, tc := range []struct {
		rel   string
		check func(map[string]int) bool
	}{
		{"lv_wh", func(got map[string]int) bool { _, ok := got["lv_wh"]; return !ok }},
		{"a/b", func(got map[string]int) bool { return got["a"] == 1 }},
	} {
		h := NewDatabasesHandler(backend, nil, nil, zerolog.Nop())
		h.SetTieringManager(&coldTieringManager{metadata: metadata})
		h.SetWarehouseLayout(warehouseAt(tc.rel))
		app := fiber.New()
		h.RegisterRoutes(app)
		if got := warehouseListed(t, app); !tc.check(got) {
			t.Errorf("warehouse %s: listed %v", tc.rel, got)
		}
	}
}
