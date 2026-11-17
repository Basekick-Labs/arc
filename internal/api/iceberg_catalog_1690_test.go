package api

// #1690: Arc's read-only Iceberg REST catalog, exercised with the real
// iceberg-go REST client over a real listener, and with fakes for the
// failure and permission paths.

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	icecatalog "github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/rest"
	icetable "github.com/apache/iceberg-go/table"
	"github.com/basekick-labs/arc/internal/auth"
	"github.com/basekick-labs/arc/internal/iceberg"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

func writeCatalogTestParquet(t *testing.T, path string, n int) {
	t.Helper()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "time", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, Nullable: true},
		{Name: "host", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "value", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	}, nil)
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()
	for i := 0; i < n; i++ {
		b.Field(0).(*array.TimestampBuilder).Append(arrow.Timestamp(1_700_000_000_000_000 + int64(i)*1_000_000))
		b.Field(1).(*array.StringBuilder).Append("h")
		b.Field(2).(*array.Float64Builder).Append(float64(i))
	}
	rec := b.NewRecord()
	defer rec.Release()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	w, err := pqarrow.NewFileWriter(schema, f, parquet.NewWriterProperties(), pqarrow.NewArrowWriterProperties(pqarrow.WithStoreSchema()))
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Write(rec); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

// exportForCatalog runs the real export over root until every table in want
// is published, and returns the exporter.
func exportForCatalog(t *testing.T, root string, want [][2]string) *iceberg.Exporter {
	t.Helper()
	backend, err := storage.NewLocalBackend(root, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "catalog.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	exp, err := iceberg.NewExporter(db, backend, iceberg.DefaultWarehouse(backend), "arc", 2, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	exp.SetWarehouseAuthoritative(true)
	s := iceberg.NewScheduler(iceberg.SchedulerConfig{
		Exporter: exp, Source: iceberg.NewStorageWalkSource(backend, "arc", zerolog.Nop()),
		Interval: time.Hour, Logger: zerolog.Nop(),
	})
	s.Start()
	defer s.Stop()
	deadline := time.Now().Add(60 * time.Second)
	for _, tm := range want {
		for {
			_, found, err := exp.LoadPublished(context.Background(), tm[0], tm[1])
			if err == nil && found {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s.%s never published (err %v)", tm[0], tm[1], err)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	return exp
}

func serveCatalog(t *testing.T, app *fiber.App) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = app.Listener(ln) }()
	t.Cleanup(func() { _ = app.Shutdown() })
	return "http://" + ln.Addr().String() + IcebergCatalogBasePath
}

func TestIcebergCatalogWithTheRealRESTClient(t *testing.T) {
	root := t.TempDir()
	writeCatalogTestParquet(t, filepath.Join(root, "mydb/cpu/2023/11/14/22/cpu_a.parquet"), 50)
	writeCatalogTestParquet(t, filepath.Join(root, "mydb/mem/2023/11/14/22/mem_a.parquet"), 7)
	writeCatalogTestParquet(t, filepath.Join(root, "other/disk/2023/11/14/22/disk_a.parquet"), 3)
	// A database with no exported table is not a namespace.
	if err := os.MkdirAll(filepath.Join(root, "empty", "x"), 0o700); err != nil {
		t.Fatal(err)
	}
	exp := exportForCatalog(t, root, [][2]string{{"mydb", "cpu"}, {"mydb", "mem"}, {"other", "disk"}})

	am, err := auth.NewAuthManager(filepath.Join(t.TempDir(), "auth.db"), time.Minute, 100, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { am.Close() })
	ctx := context.Background()
	reader, err := am.CreateToken(ctx, "reader", "", "read", nil)
	if err != nil {
		t.Fatal(err)
	}
	writer, err := am.CreateToken(ctx, "writer", "", "write", nil)
	if err != nil {
		t.Fatal(err)
	}
	app := fiber.New()
	mc := auth.DefaultMiddlewareConfig()
	mc.AuthManager = am
	app.Use(auth.NewMiddleware(mc))
	NewIcebergCatalogHandler(exp, am, zerolog.Nop()).RegisterRoutes(app)
	uri := serveCatalog(t, app)

	cat, err := rest.NewCatalog(ctx, "arc", uri, rest.WithOAuthToken(reader))
	if err != nil {
		t.Fatalf("NewCatalog: %v", err)
	}
	nss, err := cat.ListNamespaces(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, ns := range nss {
		got = append(got, strings.Join(ns, "."))
	}
	sort.Strings(got)
	if strings.Join(got, ",") != "arc_mydb,arc_other" {
		t.Fatalf("namespaces = %v, want arc_mydb,arc_other", got)
	}
	var tables []string
	for id, err := range cat.ListTables(ctx, icetable.Identifier{"arc_mydb"}) {
		if err != nil {
			t.Fatal(err)
		}
		tables = append(tables, strings.Join(id, "."))
	}
	if strings.Join(tables, ",") != "arc_mydb.cpu,arc_mydb.mem" {
		t.Fatalf("tables = %v", tables)
	}
	if ok, err := cat.CheckNamespaceExists(ctx, icetable.Identifier{"arc_mydb"}); err != nil || !ok {
		t.Fatalf("namespace exists = %v, %v", ok, err)
	}
	if ok, err := cat.CheckNamespaceExists(ctx, icetable.Identifier{"arc_empty"}); err != nil || ok {
		t.Fatalf("empty namespace exists = %v, %v", ok, err)
	}
	if ok, err := cat.CheckTableExists(ctx, icetable.Identifier{"arc_mydb", "cpu"}); err != nil || !ok {
		t.Fatalf("table exists = %v, %v", ok, err)
	}
	if ok, err := cat.CheckTableExists(ctx, icetable.Identifier{"arc_mydb", "nope"}); err != nil || ok {
		t.Fatalf("missing table exists = %v, %v", ok, err)
	}
	if _, err := cat.LoadTable(ctx, icetable.Identifier{"arc_mydb", "nope"}); !errors.Is(err, icecatalog.ErrNoSuchTable) {
		t.Fatalf("load missing table: %v, want ErrNoSuchTable", err)
	}

	tbl, err := cat.LoadTable(ctx, icetable.Identifier{"arc_mydb", "cpu"})
	if err != nil {
		t.Fatalf("LoadTable: %v", err)
	}
	pub, _, err := exp.LoadPublished(ctx, "mydb", "cpu")
	if err != nil {
		t.Fatal(err)
	}
	if tbl.MetadataLocation() != pub.MetadataLocation {
		t.Fatalf("client loaded %s, published is %s", tbl.MetadataLocation(), pub.MetadataLocation)
	}
	at, err := tbl.Scan().ToArrowTable(ctx)
	if err != nil {
		t.Fatalf("scan through the catalog: %v", err)
	}
	defer at.Release()
	if at.NumRows() != 50 {
		t.Fatalf("scanned %d rows, want 50", at.NumRows())
	}

	// The client refuses writes itself (not advertised) ...
	if err := cat.CreateNamespace(ctx, icetable.Identifier{"arc_new"}, nil); err == nil {
		t.Fatal("CreateNamespace succeeded against a read-only catalog")
	}
	// ... and a client that tries anyway gets 406.
	status, body := rawCatalogRequest(t, http.MethodPost, uri+"/v1/namespaces/arc_mydb/tables", reader)
	if status != http.StatusNotAcceptable || !strings.Contains(body, "UnsupportedOperationException") {
		t.Fatalf("POST table: %d %s", status, body)
	}
	status, body = rawCatalogRequest(t, http.MethodDelete, uri+"/v1/namespaces/arc_mydb/tables/cpu", reader)
	if status != http.StatusNotAcceptable {
		t.Fatalf("DELETE table: %d %s", status, body)
	}
	// The warehouse's own namespace directory is not a database (review H1).
	for _, p := range []string{"/v1/namespaces/arc_arc_mydb.db/tables/cpu", "/v1/namespaces/arc_arc_mydb.db/tables"} {
		if status, body := rawCatalogRequest(t, http.MethodGet, uri+p, reader); status != http.StatusNotFound {
			t.Fatalf("%s: %d %s, want 404", p, status, body)
		}
	}
	if status, _ := rawCatalogRequest(t, http.MethodGet, uri+"/v1/namespaces", ""); status != http.StatusUnauthorized {
		t.Fatalf("no token: %d, want 401", status)
	}
	status, body = rawCatalogRequest(t, http.MethodGet, uri+"/v1/namespaces", writer)
	if status != http.StatusForbidden || !strings.Contains(body, "ForbiddenException") {
		t.Fatalf("write-only token: %d %s, want 403 ForbiddenException", status, body)
	}
}

func rawCatalogRequest(t *testing.T, method, url, token string) (int, string) {
	t.Helper()
	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

// fakePublished is an IcebergPublishedSource with settable contents and
// failures.
type fakePublished struct {
	tables  map[string][]string
	listErr error
	loadErr error
	calls   int
}

func (f *fakePublished) NamespacePrefix() string { return "arc" }
func (f *fakePublished) PublishedDatabases(context.Context) ([]string, error) {
	f.calls++
	if f.listErr != nil {
		return nil, f.listErr
	}
	var out []string
	for db := range f.tables {
		out = append(out, db)
	}
	sort.Strings(out)
	return out, nil
}
func (f *fakePublished) PublishedTables(_ context.Context, db string, limit int) ([]string, error) {
	f.calls++
	if f.listErr != nil {
		return nil, f.listErr
	}
	ts := f.tables[db]
	if limit > 0 && len(ts) > limit {
		ts = ts[:limit]
	}
	return ts, nil
}
func (f *fakePublished) LoadPublished(_ context.Context, db, m string) (iceberg.PublishedTable, bool, error) {
	if f.loadErr != nil {
		return iceberg.PublishedTable{}, false, f.loadErr
	}
	for _, t := range f.tables[db] {
		if t == m {
			return iceberg.PublishedTable{MetadataLocation: "file:///x/" + db + "/" + m + "/metadata/v1.metadata.json", Metadata: []byte(`{"format-version":2}`)}, true, nil
		}
	}
	return iceberg.PublishedTable{}, false, nil
}

// fakeRBAC grants read on exactly the listed database/measurement pairs.
type fakeRBAC struct {
	grants      map[string]map[string]bool
	unavailable bool
	// failDB and failTable fail only the per-database or per-table
	// question, so the door still admits the caller.
	failDB, failTable bool
}

func (r *fakeRBAC) CheckPermission(req *auth.PermissionCheckRequest) *auth.PermissionCheckResult {
	if r.unavailable || r.failTable {
		return &auth.PermissionCheckResult{Source: auth.SourceUnavailable}
	}
	return &auth.PermissionCheckResult{Allowed: r.grants[req.Database][req.Measurement], Source: "rbac"}
}
func (r *fakeRBAC) CanAccessAnythingInErr(_ *auth.TokenInfo, db, _ string) (bool, error) {
	if r.unavailable || r.failDB {
		return false, errors.New("store down")
	}
	return len(r.grants[db]) > 0, nil
}
func (r *fakeRBAC) HasPermissionAnywhere(*auth.TokenInfo, string) (bool, error) {
	if r.unavailable {
		return false, errors.New("store down")
	}
	return len(r.grants) > 0, nil
}

func catalogTestApp(t *testing.T, src IcebergPublishedSource, rbac IcebergCatalogRBAC) (*fiber.App, *IcebergCatalogHandler, string) {
	t.Helper()
	app := fiber.New()
	var am *auth.AuthManager
	token := ""
	if rbac != nil {
		var err error
		am, err = auth.NewAuthManager(filepath.Join(t.TempDir(), "auth.db"), time.Minute, 100, zerolog.Nop())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { am.Close() })
		if token, err = am.CreateToken(context.Background(), "t", "", "read", nil); err != nil {
			t.Fatal(err)
		}
		mc := auth.DefaultMiddlewareConfig()
		mc.AuthManager = am
		app.Use(auth.NewMiddleware(mc))
	}
	h := NewIcebergCatalogHandler(src, am, zerolog.Nop())
	if rbac != nil {
		h.SetRBAC(rbac)
	}
	h.RegisterRoutes(app)
	return app, h, token
}

func catalogGet(t *testing.T, app *fiber.App, method, path, token string) (int, map[string]any) {
	t.Helper()
	req := httptest.NewRequest(method, path, nil)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := app.Test(req, testRequestTimeoutMS)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var body map[string]any
	b, _ := io.ReadAll(resp.Body)
	_ = json.Unmarshal(b, &body)
	return resp.StatusCode, body
}

func TestIcebergCatalogRBACFiltersAndHidesWhatTheCallerCannotRead(t *testing.T) {
	src := &fakePublished{tables: map[string][]string{"mydb": {"cpu", "mem"}, "other": {"disk"}}}
	rbac := &fakeRBAC{grants: map[string]map[string]bool{"mydb": {"cpu": true}}}
	app, _, tok := catalogTestApp(t, src, rbac)

	status, body := catalogGet(t, app, "GET", "/iceberg/v1/namespaces", tok)
	if status != 200 {
		t.Fatalf("list namespaces: %d %v", status, body)
	}
	if b, _ := json.Marshal(body["namespaces"]); string(b) != `[["arc_mydb"]]` {
		t.Fatalf("namespaces = %s, want only arc_mydb", b)
	}
	status, body = catalogGet(t, app, "GET", "/iceberg/v1/namespaces/arc_mydb/tables", tok)
	if b, _ := json.Marshal(body["identifiers"]); status != 200 || string(b) != `[{"name":"cpu","namespace":["arc_mydb"]}]` {
		t.Fatalf("tables = %d %s, want only cpu", status, b)
	}
	for _, p := range []string{"/iceberg/v1/namespaces/arc_mydb/tables/mem", "/iceberg/v1/namespaces/arc_other/tables/disk"} {
		if status, body := catalogGet(t, app, "GET", p, tok); status != 404 {
			t.Fatalf("%s: %d %v, want 404 (no existence oracle)", p, status, body)
		}
	}
	if status, _ := catalogGet(t, app, "GET", "/iceberg/v1/namespaces/arc_other", tok); status != 404 {
		t.Fatalf("unreadable namespace: %d, want 404", status)
	}
	if status, body := catalogGet(t, app, "GET", "/iceberg/v1/namespaces/arc_mydb/tables/cpu", tok); status != 200 || body["metadata-location"] == nil || body["metadata"] == nil {
		t.Fatalf("load cpu: %d %v", status, body)
	}

	// The per-database and per-table questions failing on their own (the
	// door admitted the caller) are 503s too, never a 404 or a shorter list.
	rbac.failDB = true
	for _, p := range []string{"/iceberg/v1/namespaces", "/iceberg/v1/namespaces/arc_mydb", "/iceberg/v1/namespaces/arc_mydb/tables"} {
		if status, body := catalogGet(t, app, "GET", p, tok); status != 503 {
			t.Fatalf("%s with the per-database check failing: %d %v, want 503", p, status, body)
		}
	}
	rbac.failDB, rbac.failTable = false, true
	for _, p := range []string{"/iceberg/v1/namespaces/arc_mydb/tables", "/iceberg/v1/namespaces/arc_mydb/tables/cpu"} {
		if status, body := catalogGet(t, app, "GET", p, tok); status != 503 {
			t.Fatalf("%s with the per-table check failing: %d %v, want 503", p, status, body)
		}
	}
	rbac.failTable = false

	rbac.unavailable = true
	for _, p := range []string{"/iceberg/v1/namespaces", "/iceberg/v1/namespaces/arc_mydb/tables", "/iceberg/v1/namespaces/arc_mydb/tables/cpu"} {
		status, body := catalogGet(t, app, "GET", p, tok)
		errObj, _ := body["error"].(map[string]any)
		if status != 503 || errObj["type"] != "ServiceUnavailableException" {
			t.Fatalf("%s with the permission store down: %d %v, want 503", p, status, body)
		}
	}
}

func TestIcebergCatalogStorageFailureIsA503AndIsNotCached(t *testing.T) {
	src := &fakePublished{tables: map[string][]string{"mydb": {"cpu"}}, listErr: errors.New("s3 down")}
	app, _, _ := catalogTestApp(t, src, nil)
	for _, p := range []string{"/iceberg/v1/namespaces", "/iceberg/v1/namespaces/arc_mydb/tables", "/iceberg/v1/namespaces/arc_mydb"} {
		if status, body := catalogGet(t, app, "GET", p, ""); status != 503 {
			t.Fatalf("%s: %d %v, want 503 rather than an empty answer", p, status, body)
		}
	}
	src.listErr = nil
	if status, body := catalogGet(t, app, "GET", "/iceberg/v1/namespaces", ""); status != 200 {
		t.Fatalf("after recovery: %d %v (the failure was cached)", status, body)
	}
	src.loadErr = errors.New("hint names v9, which is missing")
	if status, _ := catalogGet(t, app, "GET", "/iceberg/v1/namespaces/arc_mydb/tables/cpu", ""); status != 503 {
		t.Fatalf("unservable load: %d, want 503", status)
	}
}

func TestIcebergCatalogListingCacheExpires(t *testing.T) {
	src := &fakePublished{tables: map[string][]string{"mydb": {"cpu"}}}
	app, h, _ := catalogTestApp(t, src, nil)
	now := time.Now()
	h.now = func() time.Time { return now }
	catalogGet(t, app, "GET", "/iceberg/v1/namespaces/arc_mydb/tables", "")
	src.tables["mydb"] = []string{"cpu", "mem"}
	_, body := catalogGet(t, app, "GET", "/iceberg/v1/namespaces/arc_mydb/tables", "")
	if n := len(body["identifiers"].([]any)); n != 1 {
		t.Fatalf("within the TTL: %d tables, want the cached 1", n)
	}
	now = now.Add(icebergListingTTL + time.Second)
	_, body = catalogGet(t, app, "GET", "/iceberg/v1/namespaces/arc_mydb/tables", "")
	if n := len(body["identifiers"].([]any)); n != 2 {
		t.Fatalf("after the TTL: %d tables, want 2", n)
	}
	// Loads are never cached: a table is loadable before the listing learns of it.
	src.tables["mydb"] = []string{"cpu", "mem", "disk"}
	if status, _ := catalogGet(t, app, "GET", "/iceberg/v1/namespaces/arc_mydb/tables/disk", ""); status != 200 {
		t.Fatalf("load of a new table: %d", status)
	}
}

func TestIcebergCatalogPathsAndMethods(t *testing.T) {
	src := &fakePublished{tables: map[string][]string{"mydb": {"cpu"}}}
	app, _, _ := catalogTestApp(t, src, nil)
	for _, tc := range []struct {
		method, path string
		status       int
		typ          string
	}{
		{"GET", "/iceberg/v1/config?warehouse=anything", 200, ""},
		{"HEAD", "/iceberg/v1/namespaces/arc_mydb/tables/cpu", 204, ""},
		{"HEAD", "/iceberg/v1/namespaces/arc_mydb/tables/nope", 404, ""},
		{"HEAD", "/iceberg/v1/namespaces/arc_mydb", 204, ""},
		{"GET", "/iceberg/v1/namespaces/arc_mydb/views", 200, ""},
		{"GET", "/iceberg/v1/namespaces/arc_mydb/views/v", 404, "NoSuchViewException"},
		{"GET", "/iceberg/v1/namespaces?parent=arc_mydb", 200, ""},
		{"GET", "/iceberg/v1/namespaces/arc_%2e%2e/tables/cpu", 404, "NoSuchTableException"},
		{"GET", "/iceberg/v1/namespaces/arc_mydb%1Fx/tables", 404, "NoSuchNamespaceException"},
		{"GET", "/iceberg/v1/namespaces/arc_mydb/tables/..", 404, ""},
		{"GET", "/iceberg/v1/namespaces/mydb/tables/cpu", 404, "NoSuchTableException"},
		{"GET", "/iceberg/v1/prefix/namespaces", 404, "NotFoundException"},
		{"POST", "/iceberg/v1/namespaces", 406, "UnsupportedOperationException"},
		{"POST", "/iceberg/v1/namespaces/arc_mydb/tables/cpu", 406, "UnsupportedOperationException"},
		{"POST", "/iceberg/v1/tables/rename", 406, "UnsupportedOperationException"},
		{"POST", "/iceberg/v1/oauth/tokens", 406, "UnsupportedOperationException"},
	} {
		status, body := catalogGet(t, app, tc.method, tc.path, "")
		if status != tc.status {
			t.Fatalf("%s %s: %d %v, want %d", tc.method, tc.path, status, body, tc.status)
		}
		if tc.typ != "" {
			errObj, _ := body["error"].(map[string]any)
			if errObj["type"] != tc.typ || errObj["code"] != float64(tc.status) {
				t.Fatalf("%s %s: error %v, want type %s code %d", tc.method, tc.path, errObj, tc.typ, tc.status)
			}
		}
	}
	_, body := catalogGet(t, app, "GET", "/iceberg/v1/config", "")
	eps, _ := body["endpoints"].([]any)
	if len(eps) != len(icebergEndpoints) {
		t.Fatalf("endpoints = %v", body["endpoints"])
	}
	for _, e := range eps {
		if s := e.(string); strings.HasPrefix(s, "POST") || strings.HasPrefix(s, "DELETE") || !strings.Contains(s, "/v1/{prefix}/") {
			t.Fatalf("endpoint %q", s)
		}
	}
}

func TestIcebergCatalogNotServableHere(t *testing.T) {
	src := &fakePublished{tables: map[string][]string{"mydb": {"cpu"}}}
	app, h, _ := catalogTestApp(t, src, nil)
	h.SetServable(func() (bool, string) { return false, "not the exporter" })
	status, body := catalogGet(t, app, "GET", "/iceberg/v1/namespaces/arc_mydb/tables/cpu", "")
	errObj, _ := body["error"].(map[string]any)
	if status != 503 || errObj["message"] != "not the exporter" {
		t.Fatalf("%d %v, want 503 with the reason", status, body)
	}
}

// Clients escape a space, '#' or '?' in a name; the catalog decodes the
// segment once, so a listed table is loadable. A second escape, a
// separator or a control character (a multi-level namespace) is not.
func TestIcebergCatalogDecodesEscapedNames(t *testing.T) {
	src := &fakePublished{tables: map[string][]string{"my db": {"cpu load", "a#b"}}}
	app, _, _ := catalogTestApp(t, src, nil)
	for _, tc := range []struct {
		path   string
		status int
	}{
		{"/iceberg/v1/namespaces/arc_my%20db/tables/cpu%20load", 200},
		{"/iceberg/v1/namespaces/arc_my%20db/tables/a%23b", 200},
		{"/iceberg/v1/namespaces/arc_my%20db/tables", 200},
		{"/iceberg/v1/namespaces/arc_my%20db/tables/cpu%2520load", 404}, // decodes to "cpu%20load"
		{"/iceberg/v1/namespaces/arc_my%20db/tables/a%2Fb", 404},
		{"/iceberg/v1/namespaces/arc_my%20db%1Fx/tables", 404},
	} {
		if status, body := catalogGet(t, app, "GET", tc.path, ""); status != tc.status {
			t.Fatalf("%s: %d %v, want %d", tc.path, status, body, tc.status)
		}
	}
}

// An empty table listing is not cached (the keys are caller-supplied), so
// a namespace whose first table appears is listed at once.
func TestIcebergCatalogEmptyListingIsNotCached(t *testing.T) {
	src := &fakePublished{tables: map[string][]string{}}
	app, _, _ := catalogTestApp(t, src, nil)
	if status, _ := catalogGet(t, app, "GET", "/iceberg/v1/namespaces/arc_mydb/tables", ""); status != 404 {
		t.Fatalf("before: %d, want 404", status)
	}
	src.tables["mydb"] = []string{"cpu"}
	if status, body := catalogGet(t, app, "GET", "/iceberg/v1/namespaces/arc_mydb/tables", ""); status != 200 {
		t.Fatalf("after the first table: %d %v, want 200", status, body)
	}
}
