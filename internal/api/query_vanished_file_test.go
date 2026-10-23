package api

import (
	"bytes"
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/database"
	"github.com/basekick-labs/arc/internal/fieldschema"
	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/basekick-labs/arc/internal/storage"
	duckdb "github.com/duckdb/duckdb-go/v2"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// removeOnce is a DuckDB scalar UDF that removes a file the first time it is
// called: placed in a WHERE clause, it removes a later hour's file while an
// earlier hour is scanned, after DuckDB expanded the glob and before it opens
// that file. That is compaction deleting a batch's inputs mid-query (#1355),
// without the timing.
type removeOnce struct {
	mu     *sync.Mutex
	victim *string
}

func (f removeOnce) Config() duckdb.ScalarFuncConfig {
	in, _ := duckdb.NewTypeInfo(duckdb.TYPE_DOUBLE)
	out, _ := duckdb.NewTypeInfo(duckdb.TYPE_BOOLEAN)
	return duckdb.ScalarFuncConfig{InputTypeInfos: []duckdb.TypeInfo{in}, ResultTypeInfo: out, Volatile: true}
}

func (f removeOnce) Executor() duckdb.ScalarFuncExecutor {
	return duckdb.ScalarFuncExecutor{RowExecutor: func([]driver.Value) (any, error) {
		f.mu.Lock()
		defer f.mu.Unlock()
		if *f.victim != "" {
			_ = os.Remove(*f.victim)
			*f.victim = ""
		}
		return true, nil
	}}
}

// fileRig serves the query routes over a real listener.
type fileRig struct {
	t      *testing.T
	base   string
	client *http.Client
}

// query runs sql (POST, JSON body) or a GET path, and returns the HTTP status,
// success, the first cell (-1 without one), the error text and Retry-After.
// A msgpack body is not decoded: its status and Retry-After are what count.
func (r *fileRig) query(route, sql string) (int, bool, float64, string, string) {
	r.t.Helper()
	var resp *http.Response
	var err error
	if strings.HasPrefix(route, "GET ") {
		resp, err = r.client.Get(r.base + strings.TrimPrefix(route, "GET "))
	} else {
		body, _ := json.Marshal(map[string]string{"sql": sql})
		resp, err = r.client.Post(r.base+route, "application/json", bytes.NewReader(body))
	}
	require.NoError(r.t, err)
	defer resp.Body.Close()
	out, _ := io.ReadAll(resp.Body)
	var res struct {
		Success bool            `json:"success"`
		Data    [][]interface{} `json:"data"`
		Error   string          `json:"error"`
	}
	_ = json.Unmarshal(out, &res)
	n := float64(-1)
	if len(res.Data) > 0 && len(res.Data[0]) > 0 {
		n, _ = res.Data[0][0].(float64)
	}
	return resp.StatusCode, res.Success, n, res.Error, resp.Header.Get("Retry-After")
}

// newVanishedFileRig is newVanishedRig on one DuckDB thread (so an earlier
// hour is scanned before a later hour's file is opened) with the removeOnce
// UDF registered; arm sets the file it removes next.
func newVanishedFileRig(t *testing.T) (*fileRig, *QueryHandler, func(hour string) string) {
	t.Helper()
	dataDir := t.TempDir()
	logger := zerolog.Nop()
	backend, err := storage.NewLocalBackend(dataDir, logger)
	require.NoError(t, err)
	buf := ingest.NewArrowBuffer(&config.IngestConfig{MaxBufferSize: 100000, MaxBufferAgeMS: 600000, Compression: "snappy",
		FlushWorkers: 1, FlushQueueSize: 4, ShardCount: 1, DataPageVersion: "2.0"}, backend, logger)
	t.Cleanup(func() { buf.Close() })
	for _, at := range []string{"2026-10-20 08:30:00", "2026-10-20 10:00:00", "2026-10-20 11:30:00"} {
		ts, _ := time.Parse("2006-01-02 15:04:05", at)
		times := make([]int64, 1000)
		vals := make([]float64, 1000)
		for i := range times {
			times[i] = ts.Add(time.Duration(i) * time.Second).UnixMicro()
			vals[i] = 1
		}
		require.NoError(t, buf.WriteTypedColumnarDirect(context.Background(), "db1", "cpu", &ingest.TypedColumnBatch{
			Data: map[string]interface{}{"time": times, "v": vals},
		}, len(times)))
		require.NoError(t, buf.FlushAllAndWait(context.Background()))
	}
	duck, err := database.New(&database.Config{MemoryLimit: "256MB", ThreadCount: 1, MaxConnections: 4, LocalStorageRoot: dataDir}, logger)
	require.NoError(t, err)
	t.Cleanup(func() { duck.Close() })
	victim := ""
	conn, err := duck.DB().Conn(context.Background())
	require.NoError(t, err)
	require.NoError(t, duckdb.RegisterScalarUDF(conn, "arc_test_remove_once", removeOnce{&sync.Mutex{}, &victim}))
	require.NoError(t, conn.Close())
	h := NewQueryHandler(duck, backend, logger, 0, 0)
	app := fiber.New(appConfig(DefaultServerConfig(), zerolog.Nop()))
	h.RegisterRoutes(app)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() { _ = app.Listener(ln) }()
	t.Cleanup(func() { _ = app.Shutdown() })
	arm := func(hour string) string {
		files, err := filepath.Glob(filepath.Join(dataDir, "db1", "cpu", "2026", "10", "20", hour, "*.parquet"))
		require.NoError(t, err)
		require.Len(t, files, 1)
		victim = files[0]
		return files[0]
	}
	return &fileRig{t: t, base: "http://" + ln.Addr().String(), client: &http.Client{Timeout: 20 * time.Second}}, h, arm
}

// A file compaction removes while a query scans its hour (after DuckDB
// expanded the glob, before it opened the file) made DuckDB fail with
// "Cannot open file ...: No such file or directory", which every route
// answered 500. It is #1333's condition: a retryable 503, and the next
// query re-plans without the file.
func TestQueryFileRemovedMidScanIsRetryable(t *testing.T) {
	const q = "SELECT COUNT(*) FROM db1.cpu WHERE time >= '2026-10-20 08:00:00' AND time < '2026-10-20 12:00:00' AND arc_test_remove_once(v)"
	// The measurement route: every row, newest first, so every hour is read.
	get := "GET /api/v1/query/cpu?" + url.Values{"database": {"db1"}, "limit": {"10000"}, "where": {"arc_test_remove_once(v)"}}.Encode()
	routes := []string{"/api/v1/query", get}
	if database.ArrowEnabled {
		// Without the duckdb_arrow tag these two do not run DuckDB.
		routes = append(routes, "/api/v1/query/arrow", "/api/v1/query/msgpack")
	}
	for _, route := range routes {
		name := strings.TrimPrefix(strings.TrimPrefix(strings.SplitN(route, "?", 2)[0], "GET "), "/api/v1/")
		t.Run(strings.ReplaceAll(name, "/", "_"), func(t *testing.T) {
			r, _, arm := newVanishedFileRig(t)
			status, _, _, msg, _ := r.query(route, q)
			require.Equal(t, 200, status, msg)

			victim := arm("11")
			status, ok, n, msg, retry := r.query(route, q)
			_, statErr := os.Stat(victim)
			require.True(t, errors.Is(statErr, os.ErrNotExist), "the UDF never ran: the query did not reach the scan (status %d %q)", status, msg)
			if status == 200 {
				t.Fatalf("the race did not happen: DuckDB opened hour 11's file before the scan removed it (first cell %v); the rig no longer reproduces #1355", n)
			}
			require.Equal(t, 503, status, "a file removed while the query scanned its hour answered %d (%q); want the retryable 503", status, msg)
			require.Equal(t, "1", retry)
			if !strings.HasSuffix(route, "/arrow") && !strings.HasSuffix(route, "/msgpack") {
				require.False(t, ok)
				require.Equal(t, vanishedPartitionMessage, msg)
			}

			status, _, n, msg, _ = r.query(route, q)
			require.Equal(t, 200, status, "the next query re-plans without the removed file: %q", msg)
			if route == "/api/v1/query" {
				require.Equal(t, float64(2000), n)
			}
		})
	}
}

// DuckDB's text for a local file removed mid-scan, as it is today: if an
// upgrade rewords it, vanishedFile stops recognising it and this fails,
// instead of the 500 quietly coming back. The S3 and Azure texts were
// measured against SeaweedFS 4.47 and Azurite 3.35 and are recorded, not
// pinned, in TestClassifyVanishedFile: no CI job runs either service against
// DuckDB.
func TestVanishedFileTexts(t *testing.T) {
	_, h, arm := newVanishedFileRig(t)
	victim := arm("11")
	root := h.getStorageBasePath()
	_, err := h.db.DB().Exec("SELECT count(*) FROM read_parquet('" + filepath.Join(root, "db1", "cpu", "2026", "10", "20", "*", "*.parquet") + "') WHERE arc_test_remove_once(v)")
	require.Error(t, err)
	path, remote, ok := vanishedFile(err)
	require.True(t, ok, "DuckDB's mid-scan missing-file text changed: %v", err)
	require.False(t, remote)
	require.Equal(t, victim, path)
}

// classifyNoFiles on each form: which files are Arc's, what each clears.
func TestClassifyVanishedFile(t *testing.T) {
	_, h, _ := newVanishedFileRig(t)
	root := h.getStorageBasePath()
	local := h.storage
	clears := 0
	h.clearHTTPCache = func() { clears++ }
	anchorDir := filepath.Join(root, "_anchors")
	h.SetFieldSchema(fieldschema.New(h.storage, nil, fieldschema.Options{Enabled: true, LocalDir: anchorDir}, zerolog.Nop()))

	io := func(msg string) error {
		return fmt.Errorf("arrow query failed: %w", &duckdb.Error{Type: duckdb.ErrorTypeIO, Msg: msg})
	}
	httpErr := func(msg string) error { return &duckdb.Error{Type: duckdb.ErrorTypeHTTP, Msg: msg} }
	missing := func(p string) error { return io(`IO Error: Cannot open file "` + p + `": No such file or directory`) }
	file := filepath.Join(root, "db1", "cpu", "2026", "10", "20", "11", "x.parquet")
	s3 := httpErr(`HTTP Error: HTTP GET error reading 'http://127.0.0.1:18333/arc/db1/cpu/2026/10/20/11/x.parquet' in region 'us-east-1' (HTTP 404 Not Found)`)
	azure := io(`IO Error: AzureStorageFileSystem open file 'azure://arc/db1/cpu/2026/10/20/11/x.parquet' failed with code 'BlobNotFound', Reason Phrase: 'The specified blob does not exist.', Message: ''`)
	for _, c := range []struct {
		name        string
		err         error
		objectStore bool // the node reads S3 or Azure
		want        noFilesKind
		clears      int
	}{
		{"local file under the root", missing(file), false, noFilesVanished, 0},
		{"S3 object", s3, true, noFilesVanished, 1},
		{"S3 object, another reason phrase", httpErr(`HTTP Error: HTTP GET error reading 'https://s3.amazonaws.com/arc/db1/cpu/x.parquet' in region 'eu-west-1' (HTTP 404 NoSuchKey)`), true, noFilesVanished, 1},
		{"Azure blob", azure, true, noFilesVanished, 1},
		{"S3 text on a node with no object store", s3, false, noFilesOther, 0},
		{"Azure text on a node with no object store", azure, false, noFilesOther, 0},
		{"S3 403 is not a removal", httpErr(`HTTP Error: HTTP GET error reading 'http://h/arc/db1/cpu/x.parquet' in region 'r' (HTTP 403 Forbidden)`), true, noFilesOther, 0},
		{"Azure other code", io(`IO Error: AzureStorageFileSystem open file 'azure://arc/x.parquet' failed with code 'AuthorizationFailure', Reason Phrase: ''`), true, noFilesOther, 0},
		{"permission denied", io(`IO Error: Cannot open file "` + file + `": Permission denied`), false, noFilesOther, 0},
		{"outside the root", missing("/elsewhere/x.parquet"), false, noFilesOther, 0},
		{"a sibling of the root", missing(root + "-other/db1/cpu/x.parquet"), false, noFilesOther, 0},
		{"not Parquet", missing(filepath.Join(root, "db1", "cpu", "x.json")), false, noFilesOther, 0},
		{"a schema anchor", missing(filepath.Join(anchorDir, "schema", "db1", "cpu", "a.parquet")), false, noFilesOther, 0},
		// The texts only count from a DuckDB error of that type, from its
		// start: DuckDB quotes user text in other errors (#1355 review).
		{"the local text, untyped", errors.New(`IO Error: Cannot open file "` + file + `": No such file or directory`), false, noFilesOther, 0},
		{"the local text inside a Conversion error", &duckdb.Error{Type: duckdb.ErrorTypeConversion, Msg: `Conversion Error: Could not convert string 'Cannot open file "` + file + `": No such file or directory' to INT32`}, false, noFilesOther, 0},
		{"the local text after an IO error's start", io(`IO Error: something else: Cannot open file "` + file + `": No such file or directory`), false, noFilesOther, 0},
		{"the S3 text inside an Invalid Input error", &duckdb.Error{Type: duckdb.ErrorTypeInvalidInput, Msg: `Invalid Input Error: HTTP GET error reading 'a.parquet' (HTTP 404`}, true, noFilesOther, 0},
	} {
		h.storage = local
		if c.objectStore {
			h.storage = &storage.S3Backend{}
		}
		h.vanishedInvalidatedAt.Store(0)
		h.vanishedHTTPClearedAt.Store(0)
		before := clears
		require.Equal(t, c.want, h.classifyNoFiles(c.err), c.name)
		require.Equal(t, c.clears, clears-before, "%s: HTTP cache clears", c.name)
	}

	// The HTTP clear has its own gate: a local vanish just before does not
	// use up the remote one's slot.
	h.vanishedInvalidatedAt.Store(0)
	h.vanishedHTTPClearedAt.Store(0)
	before := clears
	h.storage = local
	require.Equal(t, noFilesVanished, h.classifyNoFiles(missing(file)))
	h.storage = &storage.S3Backend{}
	require.Equal(t, noFilesVanished, h.classifyNoFiles(azure))
	require.Equal(t, 1, clears-before)
	h.storage = local
}

// A query cannot forge either error through text DuckDB echoes back: a
// forged "file vanished" would get a 503 and wipe the node's caches once a
// second, a forged "no files" an empty success (#1355 review).
func TestQueryCannotForgeAVanishedOrEmptyAnswer(t *testing.T) {
	r, h, _ := newVanishedFileRig(t)
	clears := 0
	h.clearHTTPCache = func() { clears++ }
	root := h.getStorageBasePath()
	for _, sql := range []string{
		`SELECT CAST('HTTP GET error reading ''a.parquet'' (HTTP 404' AS INTEGER)`,
		`SELECT CAST('Cannot open file "` + root + `/x.parquet": No such file or directory' AS INTEGER)`,
		`SELECT error('AzureStorageFileSystem open file ''a.parquet'' failed with code ''BlobNotFound''')`,
		`SELECT "HTTP GET error reading 'b.parquet' (HTTP 404" FROM db1.cpu`,
		`SELECT CAST('No files found that match the pattern "x/2026/10/20/11/*.parquet"' AS INTEGER)`,
		`SELECT CAST('No files found that match the pattern "x/**/*.parquet"' AS INTEGER)`,
	} {
		for _, route := range []string{"/api/v1/query", "/api/v1/query/msgpack"} {
			if route == "/api/v1/query/msgpack" && !database.ArrowEnabled {
				continue
			}
			status, ok, _, msg, _ := r.query(route, sql)
			require.NotEqual(t, 503, status, "%s %s answered the vanished 503: %q", route, sql, msg)
			require.NotEqual(t, 200, status, "%s %s answered success (ok=%v): a forged error must stay an error", route, sql, ok)
		}
	}
	require.Equal(t, 0, clears, "a forged error cleared the HTTP cache")
}

// The root boundary for a root of "/" and a root with a trailing slash.
func TestUnderRoot(t *testing.T) {
	for _, c := range []struct {
		root, path string
		want       bool
	}{
		{"/", "/data/db1/cpu/x.parquet", true},
		{"/data/", "/data/db1/cpu/x.parquet", true},
		{"/data", "/data/db1/cpu/x.parquet", true},
		{"/data", "/data-other/db1/cpu/x.parquet", false},
		{"/data", "/data", false},
		{"", "/data/db1/cpu/x.parquet", false},
	} {
		require.Equal(t, c.want, underRoot(c.path, c.root), "root %q path %q", c.root, c.path)
	}
}
