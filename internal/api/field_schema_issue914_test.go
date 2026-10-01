package api

import (
	"context"
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

	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/database"
	"github.com/basekick-labs/arc/internal/fieldschema"
	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/basekick-labs/arc/internal/pruning"
	"github.com/basekick-labs/arc/internal/query"
	"github.com/basekick-labs/arc/internal/storage"
)

// fieldSchemaEnv is a real query path (DuckDB, LocalBackend, ArrowBuffer)
// with the #914 registry wired the way cmd/arc/main.go wires it.
type fieldSchemaEnv struct {
	t        *testing.T
	h        *QueryHandler
	app      *fiber.App
	buf      *ingest.ArrowBuffer
	reg      *fieldschema.Registry
	backend  storage.Backend
	duck     *database.DuckDB
	localDir string
	dataDir  string
}

func newFieldSchemaEnv(t *testing.T, opts fieldschema.Options, attachToIngest bool) *fieldSchemaEnv {
	t.Helper()
	root := t.TempDir()
	dataDir := filepath.Join(root, "data")
	localDir := filepath.Join(root, "upload")
	for _, d := range []string{dataDir, localDir} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	logger := zerolog.Nop()
	backend, err := storage.NewLocalBackend(dataDir, logger)
	if err != nil {
		t.Fatal(err)
	}
	duck, err := database.New(&database.Config{
		MemoryLimit:      "256MB",
		ThreadCount:      2,
		MaxConnections:   4,
		LocalStorageRoot: dataDir,
		UploadDir:        localDir,
	}, logger)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { duck.Close() })
	opts.Enabled = true
	opts.LocalDir = localDir
	reg := fieldschema.New(backend, duck.DB(), opts, logger)
	ctx, cancel := context.WithCancel(context.Background())
	reg.Start(ctx)
	t.Cleanup(func() { cancel(); reg.Stop() })

	buf := ingest.NewArrowBuffer(&config.IngestConfig{
		MaxBufferSize: 100000, MaxBufferAgeMS: 600000, Compression: "snappy",
		FlushWorkers: 2, FlushQueueSize: 8, ShardCount: 4, DataPageVersion: "2.0",
	}, backend, logger)
	if attachToIngest {
		buf.SetFieldSchema(reg)
	}
	t.Cleanup(func() { buf.Close() })

	h := &QueryHandler{
		db:               duck,
		logger:           logger,
		storage:          backend,
		queryCache:       database.NewQueryCache(database.QueryCacheTTL, database.DefaultQueryCacheMaxSize),
		pruner:           pruning.NewPartitionPruner(logger),
		parallelExecutor: query.NewParallelExecutor(duck.DB(), query.DefaultParallelConfig(), logger),
	}
	h.SetFieldSchema(reg)
	app := fiber.New(fiber.Config{DisableStartupMessage: true})
	h.RegisterRoutes(app)
	return &fieldSchemaEnv{t: t, h: h, app: app, buf: buf, reg: reg, backend: backend, duck: duck, localDir: localDir, dataDir: dataDir}
}

// write flushes one file with the given columns at the given hour.
func (e *fieldSchemaEnv) write(db, meas string, at time.Time, cols map[string]interface{}) {
	e.t.Helper()
	n := 0
	for _, v := range cols {
		switch vv := v.(type) {
		case []int64:
			n = len(vv)
		case []float64:
			n = len(vv)
		case []string:
			n = len(vv)
		case []bool:
			n = len(vv)
		}
	}
	out := map[string][]interface{}{}
	ts := make([]interface{}, n)
	for i := range ts {
		ts[i] = at.UnixMicro() + int64(i)
	}
	out["time"] = ts
	for name, v := range cols {
		vals := make([]interface{}, n)
		switch vv := v.(type) {
		case []int64:
			for i := range vv {
				vals[i] = vv[i]
			}
		case []float64:
			for i := range vv {
				vals[i] = vv[i]
			}
		case []string:
			for i := range vv {
				vals[i] = vv[i]
			}
		case []bool:
			for i := range vv {
				vals[i] = vv[i]
			}
		}
		out[name] = vals
	}
	ctx := context.Background()
	if err := e.buf.WriteColumnarDirectNoWAL(ctx, db, meas, out); err != nil {
		e.t.Fatal(err)
	}
	if err := e.buf.FlushAll(ctx); err != nil {
		e.t.Fatal(err)
	}
	e.h.InvalidateCaches()
}

func (e *fieldSchemaEnv) query(sql string) (QueryResponse, int) {
	return e.queryDB(sql, "")
}

// queryDB posts sql with the x-arc-database header set, which selects the
// single-table fast path (and with it the parallel partition executor).
func (e *fieldSchemaEnv) queryDB(sql, headerDB string) (QueryResponse, int) {
	e.t.Helper()
	body, _ := json.Marshal(QueryRequest{SQL: sql})
	req := httptest.NewRequest("POST", "/api/v1/query", strings.NewReader(string(body)))
	req.Header.Set("Content-Type", "application/json")
	if headerDB != "" {
		req.Header.Set("x-arc-database", headerDB)
	}
	resp, err := e.app.Test(req, 30000)
	if err != nil {
		e.t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var out QueryResponse
	if err := json.Unmarshal(raw, &out); err != nil {
		e.t.Fatalf("bad response %s: %v", raw, err)
	}
	return out, resp.StatusCode
}

func (e *fieldSchemaEnv) mustRows(sql string) QueryResponse {
	e.t.Helper()
	out, status := e.query(sql)
	if status != 200 || !out.Success {
		e.t.Fatalf("query failed (%d): %s\n%s", status, out.Error, sql)
	}
	return out
}

var (
	jan1 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	feb1 = time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	mar1 = time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
)

const between = "FROM sch.multiday WHERE time >= '%s' AND time < '%s'"

func rangeSQL(sel string, from, to time.Time, tail string) string {
	return "SELECT " + sel + " " + fmt.Sprintf(between, from.Format(time.RFC3339), to.Format(time.RFC3339)) + tail
}

// seedCustomerScenario writes the reproducer from #914: two days 59 days
// apart, each with its own field set.
func seedCustomerScenario(e *fieldSchemaEnv) {
	e.write("sch", "multiday", jan1, map[string]interface{}{"source": []string{"early"}, "stable": []int64{1}, "retired_field": []int64{77}})
	e.write("sch", "multiday", mar1, map[string]interface{}{"source": []string{"late"}, "stable": []int64{2}, "weeks_later": []int64{99}})
}

func TestFieldSchema_NarrowRangeBindsAbsentFieldIssue914(t *testing.T) {
	e := newFieldSchemaEnv(t, fieldschema.Options{}, true)
	seedCustomerScenario(e)

	// a. The customer's failing query: weeks_later over January only.
	out := e.mustRows(rangeSQL("weeks_later", jan1, jan1.Add(24*time.Hour), ""))
	if out.RowCount != 1 || out.Data[0][0] != nil {
		t.Fatalf("narrow January: %+v", out)
	}
	// b. The symmetric case: the retired field over March only.
	out = e.mustRows(rangeSQL("retired_field", mar1, mar1.Add(24*time.Hour), ""))
	if out.RowCount != 1 || out.Data[0][0] != nil {
		t.Fatalf("narrow March: %+v", out)
	}
	// c. Broad range still returns both rows with values.
	out = e.mustRows(rangeSQL("source, weeks_later, retired_field", jan1, mar1.Add(24*time.Hour), " ORDER BY time"))
	if out.RowCount != 2 || out.Data[0][1] != nil || out.Data[1][1] == nil || out.Data[0][2] == nil || out.Data[1][2] != nil {
		t.Fatalf("broad: %+v", out.Data)
	}
	// d. Empty February: zero rows, full column list.
	out = e.mustRows(rangeSQL("*", feb1, feb1.Add(24*time.Hour), ""))
	if out.RowCount != 0 || !hasAll(out.Columns, "time", "source", "stable", "retired_field", "weeks_later") {
		t.Fatalf("empty range columns=%v rows=%d", out.Columns, out.RowCount)
	}
	// e. SELECT * has the same columns in the same order over any range.
	janStar := e.mustRows(rangeSQL("*", jan1, jan1.Add(24*time.Hour), ""))
	marStar := e.mustRows(rangeSQL("*", mar1, mar1.Add(24*time.Hour), ""))
	if strings.Join(janStar.Columns, ",") != strings.Join(marStar.Columns, ",") || strings.Join(janStar.Columns, ",") != strings.Join(out.Columns, ",") {
		t.Fatalf("SELECT * differs by range: %v vs %v vs %v", janStar.Columns, marStar.Columns, out.Columns)
	}
	// Order is time first, then fields alphabetically as registered (this
	// path carries no tag list), with later fields appended.
	if strings.Join(janStar.Columns, ",") != "time,retired_field,source,stable,weeks_later" {
		t.Fatalf("column order: %v", janStar.Columns)
	}
	// f. COALESCE and try_cast now work on the narrow range.
	out = e.mustRows(rangeSQL("COALESCE(weeks_later, 0) AS w, try_cast(weeks_later AS BIGINT) AS c", jan1, jan1.Add(24*time.Hour), ""))
	if out.RowCount != 1 || fmt.Sprint(out.Data[0][0]) != "0" || out.Data[0][1] != nil {
		t.Fatalf("coalesce: %+v", out.Data)
	}
	// g. An unknown field is still a binding error.
	if resp, status := e.query(rangeSQL("no_such_field", jan1, jan1.Add(24*time.Hour), "")); status == 200 || resp.Success || !strings.Contains(resp.Error, "no_such_field") {
		t.Fatalf("unknown field must error: %d %+v", status, resp)
	}
	// h. Aggregates and predicates keep their semantics.
	out = e.mustRows(rangeSQL("count(*), sum(weeks_later), max(retired_field)", jan1, mar1.Add(24*time.Hour), ""))
	if fmt.Sprint(out.Data[0][0]) != "2" || fmt.Sprint(out.Data[0][1]) != "99" || fmt.Sprint(out.Data[0][2]) != "77" {
		t.Fatalf("aggregates: %+v", out.Data)
	}
	out = e.mustRows(rangeSQL("source", jan1, mar1.Add(24*time.Hour), " AND weeks_later > 50"))
	if out.RowCount != 1 || out.Data[0][0] != "late" {
		t.Fatalf("predicate on a partially present field: %+v", out.Data)
	}
	// i. The transformed SQL lists the local anchor first and keeps one
	//    Parquet scan with projection and filter pushdown.
	transformed := e.h.convertSQLToStoragePaths(context.Background(), rangeSQL("weeks_later", jan1, jan1.Add(24*time.Hour), ""))
	anchorPrefix := filepath.ToSlash(filepath.Join(e.localDir, "schema"))
	if !strings.Contains(transformed, "read_parquet(['"+anchorPrefix) {
		t.Fatalf("anchor not first in %s", transformed)
	}
	rows, err := e.duck.Query("EXPLAIN " + transformed)
	if err != nil {
		t.Fatal(err)
	}
	var plan strings.Builder
	for rows.Next() {
		var k, v string
		if err := rows.Scan(&k, &v); err != nil {
			t.Fatal(err)
		}
		plan.WriteString(v)
	}
	rows.Close()
	p := plan.String()
	// One scan node (one "Function:" box), no UNION, pushdown intact.
	if strings.Count(p, "Function:") != 1 || strings.Contains(p, "UNION") || !strings.Contains(p, "Projections") || !strings.Contains(p, "weeks_later") || !strings.Contains(p, "Filters") {
		t.Fatalf("plan lost pushdown or gained a second scan:\n%s", p)
	}
}

// A materialized anchor removed from disk under a cached SQL transform must
// not turn into a silent empty result; the next query re-materializes it.
func TestFieldSchema_MissingLocalAnchorIsNotEmptySuccessIssue914(t *testing.T) {
	e := newFieldSchemaEnv(t, fieldschema.Options{}, true)
	seedCustomerScenario(e)
	sql := rangeSQL("weeks_later", jan1, jan1.Add(24*time.Hour), "")
	e.mustRows(sql) // warms the transform cache with the anchor path
	path, ok := e.reg.Resolve(context.Background(), "sch", "multiday")
	if !ok {
		t.Fatal("anchor expected")
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	resp, status := e.query(sql)
	if status == 200 && resp.Success && resp.RowCount == 0 {
		t.Fatal("missing anchor answered as an empty measurement")
	}
	out := e.mustRows(sql)
	if out.RowCount != 1 || out.Data[0][0] != nil {
		t.Fatalf("anchor not re-materialized: %+v", out)
	}
}

func TestFieldSchema_DisabledReproducesTodayIssue914(t *testing.T) {
	e := newFieldSchemaEnv(t, fieldschema.Options{}, true)
	seedCustomerScenario(e)
	e.h.SetFieldSchema(nil)
	e.h.InvalidateCaches()
	resp, status := e.query(rangeSQL("weeks_later", jan1, jan1.Add(24*time.Hour), ""))
	if status == 200 || resp.Success || !strings.Contains(resp.Error, "Binder Error") {
		t.Fatalf("without the registry the narrow range must still fail to bind: %d %+v", status, resp)
	}
	// And the transformed SQL is the pre-#914 shape: a bare path, no list.
	transformed := e.h.convertSQLToStoragePaths(context.Background(), rangeSQL("weeks_later", jan1, jan1.Add(24*time.Hour), ""))
	if strings.Contains(transformed, "read_parquet([") {
		t.Fatalf("disabled registry must not change the SQL shape: %s", transformed)
	}
}

func TestFieldSchema_TypeConflictNeverWidensIssue914(t *testing.T) {
	e := newFieldSchemaEnv(t, fieldschema.Options{}, true)
	e.write("sch", "typed", jan1, map[string]interface{}{"status": []int64{5}})
	// A malformed batch writes status as a string on another day.
	e.write("sch", "typed", mar1, map[string]interface{}{"status": []string{"up"}})
	out := e.mustRows("SELECT sum(status), status & 1 FROM sch.typed WHERE time >= '2026-01-01T00:00:00Z' AND time < '2026-01-02T00:00:00Z' GROUP BY status")
	if fmt.Sprint(out.Data[0][0]) != "5" || fmt.Sprint(out.Data[0][1]) != "1" {
		t.Fatalf("integer semantics lost on the well-typed range: %+v", out.Data)
	}
	fields, ok, err := e.reg.Fields(context.Background(), "sch", "typed")
	if err != nil || !ok {
		t.Fatal(err)
	}
	for _, f := range fields {
		if f.Name == "status" && f.Type != "BIGINT" {
			t.Fatalf("anchor widened status to %s", f.Type)
		}
	}
}

func TestFieldSchema_JoinAndSelfReferenceIssue914(t *testing.T) {
	e := newFieldSchemaEnv(t, fieldschema.Options{}, true)
	e.write("sch", "a", jan1, map[string]interface{}{"x": []int64{1}})
	e.write("sch", "a", mar1, map[string]interface{}{"x": []int64{2}, "xa": []int64{20}})
	e.write("sch", "b", jan1, map[string]interface{}{"y": []int64{10}})
	e.write("sch", "b", mar1, map[string]interface{}{"y": []int64{20}, "yb": []int64{200}})
	out := e.mustRows("SELECT a.x, a.xa, b.y, b.yb FROM sch.a a JOIN sch.b b ON a.time = b.time WHERE a.time >= '2026-01-01T00:00:00Z' AND a.time < '2026-01-02T00:00:00Z'")
	if out.RowCount != 1 || out.Data[0][1] != nil || out.Data[0][3] != nil || fmt.Sprint(out.Data[0][0]) != "1" {
		t.Fatalf("join: %+v", out.Data)
	}
}

func TestFieldSchema_ParallelPartitionsIssue914(t *testing.T) {
	e := newFieldSchemaEnv(t, fieldschema.Options{}, true)
	// Four hour partitions in the recent past: above MinPartitionsForParallel
	// (3). The parallel executor is reached through the single-table fast
	// path, which needs the database from the header, no string literals,
	// and a time range the pruner can read (NOW() - INTERVAL n HOUR).
	base := time.Now().UTC().Truncate(time.Hour)
	for h := 0; h < 4; h++ {
		cols := map[string]interface{}{"v": []int64{int64(h)}}
		if h == 3 {
			cols["late"] = []int64{7}
		}
		// 30 minutes into each hour, so an hour boundary crossed while the
		// test runs cannot move NOW()-6h past the first row.
		e.write("sch", "par", base.Add(time.Duration(h-5)*time.Hour+30*time.Minute), cols)
	}
	sql := "SELECT v, late FROM par WHERE time >= NOW() - INTERVAL 6 HOUR AND time < NOW() - INTERVAL 1 HOUR ORDER BY v"
	_, info, _, err := e.h.getTransformedSQLForParallel(context.Background(), sql, "sch")
	if err != nil {
		t.Fatal(err)
	}
	if info == nil || info.AnchorPath == "" || len(info.Paths) != 4 {
		tr, _, _, _ := e.h.getTransformedSQLForParallel(context.Background(), sql, "sch")
		t.Fatalf("expected a parallel plan with an anchor: %+v\ntransformed=%s", info, tr)
	}
	out, status := e.queryDB(sql, "sch")
	if status != 200 || !out.Success {
		t.Fatalf("parallel query failed (%d): %s", status, out.Error)
	}
	if out.RowCount != 4 || out.Data[0][1] != nil || out.Data[3][1] == nil {
		t.Fatalf("parallel: %+v", out.Data)
	}
}

func TestFieldSchema_BootstrapAndRebuildIssue914(t *testing.T) {
	// Files written by an older binary: no registry attached to ingest.
	e := newFieldSchemaEnv(t, fieldschema.Options{Bootstrap: true, BootstrapMaxFiles: 10, NegativeTTL: 10 * time.Millisecond, RefreshTTL: 10 * time.Millisecond}, false)
	seedCustomerScenario(e)
	// First query: no anchor, behaves as today and queues a bootstrap.
	if resp, status := e.query(rangeSQL("weeks_later", jan1, jan1.Add(24*time.Hour), "")); status == 200 || resp.Success {
		t.Fatalf("expected today's Binder Error before bootstrap: %+v", resp)
	}
	deadline := time.Now().Add(20 * time.Second)
	for {
		if _, ok := e.reg.Resolve(context.Background(), "sch", "multiday"); ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("bootstrap did not produce an anchor")
		}
		time.Sleep(20 * time.Millisecond)
	}
	e.h.InvalidateCaches()
	out := e.mustRows(rangeSQL("weeks_later, retired_field", jan1, jan1.Add(24*time.Hour), ""))
	if out.RowCount != 1 || out.Data[0][0] != nil || fmt.Sprint(out.Data[0][1]) != "77" {
		t.Fatalf("after bootstrap: %+v", out.Data)
	}

	// The schema endpoint and the rebuild endpoint.
	req := httptest.NewRequest("GET", "/api/v1/databases/sch/measurements/multiday/schema", nil)
	resp, err := e.app.Test(req, 10000)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || !strings.Contains(string(body), `"weeks_later"`) || !strings.Contains(string(body), `"BIGINT"`) {
		t.Fatalf("schema endpoint: %d %s", resp.StatusCode, body)
	}
	for _, path := range []string{"/api/v1/databases/sch/measurements/nothere/schema", "/api/v1/databases/bad%20name/measurements/x/schema"} {
		resp, err := e.app.Test(httptest.NewRequest("GET", path, nil), 10000)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != 404 && resp.StatusCode != 400 {
			t.Fatalf("%s: %d %s", path, resp.StatusCode, body)
		}
	}
	// A measurement with files but no anchor gets one via POST rebuild.
	e.write("sch", "legacy", jan1, map[string]interface{}{"q": []float64{1.5}})
	resp, err = e.app.Test(httptest.NewRequest("POST", "/api/v1/databases/sch/measurements/legacy/schema/rebuild", nil), 10000)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 202 {
		t.Fatalf("rebuild: %d", resp.StatusCode)
	}
	deadline = time.Now().Add(20 * time.Second)
	for {
		fields, ok, _ := e.reg.Fields(context.Background(), "sch", "legacy")
		if ok && len(fields) == 2 && fields[1].Name == "q" && fields[1].Type == "DOUBLE" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("rebuild did not register the schema: %v", fields)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestFieldSchema_RestartReadsStoredAnchorIssue914(t *testing.T) {
	e := newFieldSchemaEnv(t, fieldschema.Options{}, true)
	seedCustomerScenario(e)
	// A fresh registry (new process) over the same storage, cold caches.
	// The local dir must be inside DuckDB's allowed_directories, as
	// main.go's upload dir is; an arbitrary temp dir is a Permission Error.
	fresh := fieldschema.New(e.backend, nil, fieldschema.Options{Enabled: true, LocalDir: filepath.Join(e.localDir, "fresh")}, zerolog.Nop())
	e.h.SetFieldSchema(fresh)
	e.h.InvalidateCaches()
	out := e.mustRows(rangeSQL("weeks_later", jan1, jan1.Add(24*time.Hour), ""))
	if out.RowCount != 1 || out.Data[0][0] != nil {
		t.Fatalf("after restart: %+v", out)
	}
	// The stored anchor is where main.go expects it.
	if ok, _ := e.backend.Exists(context.Background(), fieldschema.AnchorKey("sch", "multiday")); !ok {
		t.Fatal("stored anchor missing")
	}
	// Anchors are invisible to SHOW DATABASES / measurement listings.
	resp, status := e.query("SHOW DATABASES")
	if status != 200 {
		t.Fatalf("show databases: %+v", resp)
	}
	for _, row := range resp.Data {
		if fmt.Sprint(row[0]) == "_schema" {
			t.Fatal("_schema listed as a database")
		}
	}
}

func hasAll(cols []string, want ...string) bool {
	set := map[string]bool{}
	for _, c := range cols {
		set[c] = true
	}
	for _, w := range want {
		if !set[w] {
			return false
		}
	}
	return true
}
