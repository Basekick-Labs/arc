package api

// /write parses a body straight into typed columns. A point that cannot be
// written as one row of its measurement is dropped, the rest are written,
// and the answer is a "partial write" 400 (InfluxDB's behaviour); each drop
// reason is one Error log line.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/basekick-labs/arc/internal/auth"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

type lpWriteRig struct {
	app *fiber.App
	buf *ingest.ArrowBuffer
	dir string
	log *bytes.Buffer
}

func newLPWriteRig(t *testing.T, cfg *config.IngestConfig) *lpWriteRig {
	return newLPWriteRigRBAC(t, cfg, nil)
}

func newLPWriteRigRBAC(t *testing.T, cfg *config.IngestConfig, rbac RBACChecker) *lpWriteRig {
	t.Helper()
	dir := t.TempDir()
	backend, err := storage.NewLocalBackend(dir, zerolog.Nop())
	require.NoError(t, err)
	if cfg == nil {
		cfg = &config.IngestConfig{}
	}
	cfg.MaxBufferSize, cfg.MaxBufferAgeMS, cfg.Compression = 1_000_000, 600000, "snappy"
	cfg.FlushWorkers, cfg.FlushQueueSize, cfg.ShardCount, cfg.DataPageVersion = 2, 8, 2, "2.0"
	buf := ingest.NewArrowBuffer(cfg, backend, zerolog.Nop())
	t.Cleanup(func() { buf.Close() })
	log := &bytes.Buffer{}
	h := NewLineProtocolHandler(buf, zerolog.New(log))
	app := fiber.New(appConfig(DefaultServerConfig(), zerolog.Nop()))
	if rbac != nil {
		h.SetAuthAndRBAC(nil, rbac)
		app.Use(func(c *fiber.Ctx) error {
			c.Locals("token_info", &auth.TokenInfo{ID: 7, Name: "writer"})
			return c.Next()
		})
	}
	h.RegisterRoutes(app)
	return &lpWriteRig{app: app, buf: buf, dir: dir, log: log}
}

func (r *lpWriteRig) post(t *testing.T, body string) (int, string) {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/write/line-protocol", strings.NewReader(body))
	req.Header.Set("x-arc-database", "d")
	resp, err := r.app.Test(req, -1)
	require.NoError(t, err)
	out, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(out)
}

// stored flushes and returns, per measurement, its row count and its
// columns' Arrow types.
func (r *lpWriteRig) stored(t *testing.T) (map[string]int64, map[string]map[string]string) {
	t.Helper()
	require.NoError(t, r.buf.FlushAllAndWait(context.Background()))
	rows := map[string]int64{}
	types := map[string]map[string]string{}
	require.NoError(t, filepath.WalkDir(r.dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(p, ".parquet") {
			return err
		}
		rel, _ := filepath.Rel(r.dir, p)
		parts := strings.Split(rel, string(filepath.Separator))
		require.Equal(t, "d", parts[0], rel)
		m := parts[1]
		f, err := os.Open(p)
		require.NoError(t, err)
		defer f.Close()
		pf, err := file.NewParquetReader(f)
		require.NoError(t, err)
		defer pf.Close()
		fr, err := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
		require.NoError(t, err)
		sc, err := fr.Schema()
		require.NoError(t, err)
		if types[m] == nil {
			types[m] = map[string]string{}
		}
		for _, fld := range sc.Fields() {
			types[m][fld.Name] = fld.Type.String()
		}
		rows[m] += pf.NumRows()
		return nil
	}))
	return rows, types
}

func (r *lpWriteRig) logLines(t *testing.T) []map[string]interface{} {
	var out []map[string]interface{}
	for _, line := range bytes.Split(bytes.TrimSpace(r.log.Bytes()), []byte{'\n'}) {
		if len(line) == 0 {
			continue
		}
		var m map[string]interface{}
		require.NoError(t, json.Unmarshal(line, &m))
		out = append(out, m)
	}
	return out
}

func TestLPWriteDropsOnlyPointsThatDoNotFit(t *testing.T) {
	r := newLPWriteRig(t, nil)
	status, out := r.post(t, strings.Join([]string{
		"cpu,host=a v=1,n=1i 1700000000000000000",
		"cpu,host=b v=\"x\",n=2i 1700000001000000000", // v is a float column
		"cpu,host=c v=2,n=3i 1700000002000000000",
		"cpu,host=d v=\"y\" 1700000003000000000",
		"mem s=\"a\" 1700000000000000000",
		"mem s=3 1700000001000000000",    // s is a string column
		"cpu host=7 1700000004000000000", // host is a tag (string) column
		"disk u=1i 1700000000000000000",
	}, "\n"))
	require.Equal(t, http.StatusBadRequest, status, out)
	require.Contains(t, out, "partial write: 4 points dropped")
	require.Contains(t, out, "the other points were written")

	rows, types := r.stored(t)
	require.Equal(t, map[string]int64{"cpu": 2, "mem": 1, "disk": 1}, rows)
	require.Equal(t, "float64", types["cpu"]["v"])
	require.Equal(t, "int64", types["cpu"]["n"])
	require.Equal(t, "utf8", types["cpu"]["host"])
	require.Equal(t, "utf8", types["mem"]["s"])

	// One Error line per (measurement, column, reason), with the counts.
	type key struct{ m, c string }
	seen := map[key]map[string]interface{}{}
	for _, l := range r.logLines(t) {
		if l["message"] == "Line protocol points dropped (partial write)" {
			require.Equal(t, "error", l["level"])
			seen[key{l["measurement"].(string), l["column"].(string)}] = l
		}
	}
	require.Len(t, seen, 3)
	v := seen[key{"cpu", "v"}]
	require.EqualValues(t, 2, v["points_dropped"])
	require.Equal(t, "float", v["column_type"])
	require.Equal(t, "string", v["value_type"])
	require.Equal(t, ingest.LPDropKind, v["reason"])
	require.EqualValues(t, 1, seen[key{"mem", "s"}]["points_dropped"])
	require.Equal(t, "string", seen[key{"cpu", "host"}]["column_type"])
}

// A field named like a tag of its line goes to "<name>_value"; a line that
// also has a literal "<name>_value" field gives one row two values: dropped.
func TestLPWriteDropsSameColumnPoints(t *testing.T) {
	r := newLPWriteRig(t, nil)
	status, out := r.post(t, "cpu,host=x host=1i,host_value=2i 1700000000000000000\ncpu,host=y host=3i 1700000001000000000\n")
	require.Equal(t, http.StatusBadRequest, status, out)
	require.Contains(t, out, "partial write: 1 points dropped")
	rows, types := r.stored(t)
	require.Equal(t, map[string]int64{"cpu": 1}, rows)
	require.Equal(t, "int64", types["cpu"]["host_value"])

	// Only such points: nothing written.
	r2 := newLPWriteRig(t, nil)
	status, out = r2.post(t, "cpu,host=x host=1i,host_value=2i 1700000000000000000\n")
	require.Equal(t, http.StatusBadRequest, status, out)
	require.Contains(t, out, "partial write: 1 points dropped")
	require.NotContains(t, out, "the other points were written")
	require.Zero(t, r2.buf.BufferedBytes())
}

// A body with nothing to drop is a 204 and every point is stored, typed by
// its first value (an integer column takes a later float, truncated).
func TestLPWriteValidBody(t *testing.T) {
	r := newLPWriteRig(t, nil)
	status, out := r.post(t, "cpu,host=a v=1i,b=t,s=\"x\" 1700000000000000000\ncpu,host=b v=2.9 1700000001000000000\nmem free=10u 1700000000000000000\n")
	require.Equal(t, http.StatusNoContent, status, out)
	rows, types := r.stored(t)
	require.Equal(t, map[string]int64{"cpu": 2, "mem": 1}, rows)
	require.Equal(t, "int64", types["cpu"]["v"])
	require.Equal(t, "bool", types["cpu"]["b"])
	require.Equal(t, "utf8", types["cpu"]["s"])
	require.Equal(t, "int64", types["mem"]["free"])
	require.NotContains(t, r.log.String(), "points dropped")

	status, out = r.post(t, "# only a comment\n")
	require.Equal(t, http.StatusBadRequest, status)
	require.Contains(t, out, "No valid records in request")
}

// A deployment with decimal columns keeps the record path: the buffer's
// typing converts any value of a decimal column.
func TestLPWriteDecimalDeploymentKeepsRecordPath(t *testing.T) {
	r := newLPWriteRig(t, &config.IngestConfig{DecimalColumns: []string{"trades:price=18,8"}})
	require.True(t, r.buf.HasDecimalColumns())
	status, out := r.post(t, "trades price=1.25,qty=3i 1700000000000000000\ntrades price=2i,qty=4i 1700000001000000000\n")
	require.Equal(t, http.StatusNoContent, status, out)
	rows, types := r.stored(t)
	require.Equal(t, map[string]int64{"trades": 2}, rows)
	require.Contains(t, types["trades"]["price"], "decimal")
}

// RBAC is checked for every measurement of the body before anything is
// written.
func TestLPWriteChecksEveryMeasurement(t *testing.T) {
	rbac := &mockRBACChecker{enabled: true, allowedDBs: map[string]bool{"d": true}, deniedMeasurements: map[string]bool{"d.mem": true}}
	r := newLPWriteRigRBAC(t, nil, rbac)
	status, out := r.post(t, "cpu v=1 1700000000000000000\nmem v=2 1700000000000000000\n")
	require.Equal(t, http.StatusForbidden, status, out)
	require.Contains(t, out, "d.mem")
	require.Zero(t, r.buf.BufferedBytes())

	status, out = r.post(t, "cpu v=1 1700000000000000000\n")
	require.Equal(t, http.StatusNoContent, status, out)
}

// An invalid measurement name refuses the request, nothing written.
func TestLPWriteRefusesInvalidMeasurementName(t *testing.T) {
	r := newLPWriteRig(t, nil)
	status, out := r.post(t, "cpu v=1 1700000000000000000\n1bad v=2 1700000000000000000\n")
	require.Equal(t, http.StatusBadRequest, status, out)
	require.Contains(t, out, "invalid measurement name")
	require.Zero(t, r.buf.BufferedBytes())
}

// The handler's record count is the rows written, not the points sent.
func TestLPWriteStatsCountWrittenRows(t *testing.T) {
	r := newLPWriteRig(t, nil)
	status, _ := r.post(t, "cpu v=1 1700000000000000000\ncpu v=\"x\" 1700000001000000000\ncpu v=3 1700000002000000000\n")
	require.Equal(t, http.StatusBadRequest, status)
	resp, err := r.app.Test(httptest.NewRequest(http.MethodGet, "/api/v1/write/line-protocol/stats", nil), -1)
	require.NoError(t, err)
	var stats struct {
		Stats struct {
			TotalRecords int64 `json:"total_records"`
		} `json:"stats"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&stats))
	require.EqualValues(t, 2, stats.Stats.TotalRecords)
}

// Drop logging is bounded per request, and a refused request logs no drops.
func TestLPWriteDropLogsBoundedAndOnlyForWrites(t *testing.T) {
	r := newLPWriteRig(t, nil)
	var body strings.Builder
	body.WriteString("ok v=1 1700000000000000000\n")
	for i := 0; i < 30; i++ {
		fmt.Fprintf(&body, "m%02d v=1 1700000000000000000\nm%02d v=\"x\" 1700000001000000000\n", i, i)
	}
	status, out := r.post(t, body.String())
	require.Equal(t, http.StatusBadRequest, status, out)
	require.Contains(t, out, "partial write: 30 points dropped")
	var lines, rest int
	for _, l := range r.logLines(t) {
		switch l["message"] {
		case "Line protocol points dropped (partial write)":
			lines++
		case "Line protocol points dropped (partial write): more measurements and columns":
			rest++
			require.EqualValues(t, 20, l["points_dropped"])
			require.EqualValues(t, 20, l["reasons"])
		}
	}
	require.Equal(t, maxDropLogLines, lines)
	require.Equal(t, 1, rest)

	rbac := &mockRBACChecker{enabled: true, allowedDBs: map[string]bool{"d": true}, deniedMeasurements: map[string]bool{"d.cpu": true}}
	r2 := newLPWriteRigRBAC(t, nil, rbac)
	status, out = r2.post(t, "cpu v=1 1700000000000000000\ncpu v=\"x\" 1700000001000000000\n")
	require.Equal(t, http.StatusForbidden, status, out)
	require.NotContains(t, r2.log.String(), "points dropped")
}
