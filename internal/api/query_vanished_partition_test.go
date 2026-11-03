//go:build duckdb_arrow

package api

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/database"
	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

type vanishedRig struct {
	t       *testing.T
	dataDir string
	base    string
	client  *http.Client
}

// newVanishedRig ingests one row into each of hours 08, 10 and 11 of
// 2026-10-20 for db1.cpu and serves the query routes on a real listener.
func newVanishedRig(t *testing.T) *vanishedRig {
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
		require.NoError(t, buf.WriteTypedColumnarDirect(context.Background(), "db1", "cpu", &ingest.TypedColumnBatch{
			Data: map[string]interface{}{"time": []int64{ts.UnixMicro()}, "v": []float64{1}},
		}, 1))
		require.NoError(t, buf.FlushAllAndWait(context.Background()))
	}
	duck, err := database.New(&database.Config{MemoryLimit: "256MB", ThreadCount: 2, MaxConnections: 4, LocalStorageRoot: dataDir}, logger)
	require.NoError(t, err)
	t.Cleanup(func() { duck.Close() })
	h := NewQueryHandler(duck, backend, logger, 0, 0)
	app := fiber.New(appConfig(DefaultServerConfig(), zerolog.Nop()))
	h.RegisterRoutes(app)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() { _ = app.Listener(ln) }()
	t.Cleanup(func() { _ = app.Shutdown() })
	return &vanishedRig{t: t, dataDir: dataDir, base: "http://" + ln.Addr().String(), client: &http.Client{Timeout: 20 * time.Second}}
}

// count runs sql on route and returns the HTTP status, success, and the
// single count it answered (-1 when there is no row).
func (r *vanishedRig) count(route, sql string) (int, bool, float64) {
	status, ok, n, _, _ := r.query(route, sql)
	return status, ok, n
}

// query is count plus the error text and the Retry-After header.
func (r *vanishedRig) query(route, sql string) (int, bool, float64, string, string) {
	r.t.Helper()
	body, _ := json.Marshal(map[string]string{"sql": sql})
	resp, err := r.client.Post(r.base+route, "application/json", bytes.NewReader(body))
	require.NoError(r.t, err)
	defer resp.Body.Close()
	out, _ := io.ReadAll(resp.Body)
	var res struct {
		Success bool            `json:"success"`
		Data    [][]interface{} `json:"data"`
		Error   string          `json:"error"`
	}
	_ = json.Unmarshal(out, &res)
	retry := resp.Header.Get("Retry-After")
	if len(res.Data) == 0 || len(res.Data[0]) == 0 {
		return resp.StatusCode, res.Success, -1, res.Error, retry
	}
	n, _ := res.Data[0][0].(float64)
	return resp.StatusCode, res.Success, n, res.Error, retry
}

func (r *vanishedRig) removeHour(hour string) {
	r.t.Helper()
	require.NoError(r.t, os.RemoveAll(filepath.Join(r.dataDir, "db1", "cpu", "2026", "10", "20", hour)))
}

// An hour partition removed after a query was planned (retention, compaction
// or a DELETE taking it mid-flight, or a cached transform outliving it) made
// DuckDB report "No files found" for that hour's glob, and the handler
// answered success with NO rows: every row of the query was lost, not just
// the removed hour's. The query must fail instead, and the next one re-plans
// without the removed hour.
func TestQueryVanishedPartitionIsNotAnEmptyResult(t *testing.T) {
	r := newVanishedRig(t)
	q := "SELECT COUNT(*) FROM db1.cpu WHERE time >= '2026-10-20 08:00:00' AND time < '2026-10-20 12:00:00'"
	status, ok, n := r.count("/api/v1/query", q)
	require.Equal(t, 200, status)
	require.True(t, ok)
	require.Equal(t, float64(3), n)

	r.removeHour("11") // the cached transform still names hour 11
	status, ok, n, msg, retry := r.query("/api/v1/query", q)
	require.Equal(t, 503, status, "a query whose pruned hour vanished answered %v (count %v); want a retryable 503, never an empty result", ok, n)
	require.False(t, ok)
	require.Equal(t, vanishedPartitionMessage, msg)
	require.Equal(t, "1", retry)
	status, ok, n = r.count("/api/v1/query", q)
	require.Equal(t, 200, status, "the next query re-plans without the removed hour")
	require.True(t, ok)
	require.Equal(t, float64(2), n)
}

// The same on the Arrow route, which builds an empty reader for the error.
func TestQueryArrowVanishedPartitionIsNotAnEmptyResult(t *testing.T) {
	r := newVanishedRig(t)
	q := "SELECT COUNT(*) FROM db1.cpu WHERE time >= '2026-10-20 08:00:00' AND time < '2026-10-20 12:00:00'"
	status, _, _ := r.count("/api/v1/query/arrow", q)
	require.Equal(t, 200, status)
	r.removeHour("10")
	status, _, _ = r.count("/api/v1/query/arrow", q)
	require.Equal(t, 503, status, "an Arrow query whose pruned hour vanished must fail, not stream an empty result")
}

// The same on the msgpack route.
func TestQueryMsgPackVanishedPartitionIsNotAnEmptyResult(t *testing.T) {
	r := newVanishedRig(t)
	q := "SELECT COUNT(*) FROM db1.cpu WHERE time >= '2026-10-20 08:00:00' AND time < '2026-10-20 12:00:00'"
	status, _, _ := r.count("/api/v1/query/msgpack", q)
	require.Equal(t, 200, status)
	r.removeHour("10")
	status, _, _ = r.count("/api/v1/query/msgpack", q)
	require.Equal(t, 503, status, "a msgpack query whose pruned hour vanished must fail, not answer empty")
	status, _, _ = r.count("/api/v1/query/msgpack", "SELECT COUNT(*) FROM db1.nothing_here")
	require.Equal(t, 200, status, "a measurement with no data still answers empty on msgpack")
}

// A measurement with no data at all is still an empty result: its unpruned
// glob matching nothing is the one case that means "no rows".
func TestQueryEmptyMeasurementIsStillAnEmptyResult(t *testing.T) {
	r := newVanishedRig(t)
	status, ok, n := r.count("/api/v1/query", "SELECT COUNT(*) FROM db1.nothing_here")
	require.Equal(t, 200, status)
	require.True(t, ok)
	require.Equal(t, float64(-1), n, "no rows, as before")
}
