//go:build duckdb_arrow

package api

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/database"
	"github.com/basekick-labs/arc/internal/fieldschema"
	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// A start-only query counts rows stamped beyond the pruner's assumed end
// (now + 24h): before, the pruned file list stopped there and the query
// returned fewer rows than it asked for, with no error.
func TestQueryOpenEndedRangeCountsFutureRows(t *testing.T) {
	now := time.Now().UTC()
	dataDir := t.TempDir()
	logger := zerolog.Nop()
	backend, err := storage.NewLocalBackend(dataDir, logger)
	require.NoError(t, err)
	buf := ingest.NewArrowBuffer(&config.IngestConfig{MaxBufferSize: 100000, MaxBufferAgeMS: 600000, Compression: "snappy", FlushWorkers: 1, FlushQueueSize: 4, ShardCount: 1, DataPageVersion: "2.0"}, backend, logger)
	defer buf.Close()
	write := func(at time.Time) {
		require.NoError(t, buf.WriteTypedColumnarDirect(context.Background(), "db1", "cpu", &ingest.TypedColumnBatch{
			Data: map[string]interface{}{"time": []int64{at.UnixMicro()}, "v": []float64{1}},
		}, 1))
		require.NoError(t, buf.FlushAllAndWait(context.Background()))
	}
	write(now.Add(-5 * time.Minute))
	write(now.Add(48 * time.Hour))
	write(now.Add(400 * 24 * time.Hour))

	duck, err := database.New(&database.Config{MemoryLimit: "256MB", ThreadCount: 2, MaxConnections: 4, LocalStorageRoot: dataDir}, logger)
	require.NoError(t, err)
	defer duck.Close()
	h := NewQueryHandler(duck, backend, logger, 0, 0)
	app := fiber.New(appConfig(DefaultServerConfig(), zerolog.Nop()))
	h.RegisterRoutes(app)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() { _ = app.Listener(ln) }()
	defer app.Shutdown()
	client := &http.Client{Timeout: 20 * time.Second}
	count := func(sql string) float64 {
		body, _ := json.Marshal(map[string]string{"sql": sql})
		resp, err := client.Post("http://"+ln.Addr().String()+"/api/v1/query", "application/json", bytes.NewReader(body))
		require.NoError(t, err)
		defer resp.Body.Close()
		out, _ := io.ReadAll(resp.Body)
		require.Equal(t, 200, resp.StatusCode, string(out))
		var r struct{ Data [][]float64 }
		require.NoError(t, json.Unmarshal(out, &r), string(out))
		require.Len(t, r.Data, 1, string(out))
		return r.Data[0][0]
	}
	require.Equal(t, float64(3), count("SELECT COUNT(*) FROM db1.cpu WHERE time > NOW() - INTERVAL '1 hour'"), "rows beyond now+24h are missing")
	require.Equal(t, float64(2), count("SELECT COUNT(*) FROM db1.cpu WHERE time > NOW() + INTERVAL '1 day'"))
	require.Equal(t, float64(0), count("SELECT COUNT(*) FROM db1.cpu WHERE time > NOW() + INTERVAL '500 days'"))
}

// An open-ended window with no data reads the measurement's newest partition
// behind the complete schema anchor, not the whole measurement; the anchor
// still supplies every column, including those only older files have.
func TestQueryOpenEndedEmptyWindowReadsOnePartition(t *testing.T) {
	e := newFieldSchemaEnv(t, fieldschema.Options{}, true)
	now := time.Now().UTC()
	old, newest := now.Add(-30*time.Hour), now.Add(-5*time.Hour)
	e.write("sch", "quiet", old, map[string]interface{}{"old_col": []int64{1}})
	e.write("sch", "quiet", newest, map[string]interface{}{"new_col": []int64{2}})
	require.True(t, e.reg.IsComplete("sch", "quiet"))

	sql := "SELECT * FROM sch.quiet WHERE time > NOW() - INTERVAL '1 hour'"
	tr := e.h.convertSQLToStoragePaths(context.Background(), sql)
	require.Contains(t, tr, newest.Format("2006/01/02/15"), tr)
	require.NotContains(t, tr, old.Format("2006/01/02/15"), tr)
	require.NotContains(t, tr, "**", "whole-measurement glob: %s", tr)

	out := e.mustRows(sql)
	require.Equal(t, 0, out.RowCount)
	require.True(t, hasAll(out.Columns, "time", "old_col", "new_col"), "%v", out.Columns)

	// Data arriving in the window afterwards is seen (never cached).
	e.write("sch", "quiet", now, map[string]interface{}{"new_col": []int64{3}})
	require.Equal(t, 1, e.mustRows(sql).RowCount)
}

// With an incomplete anchor (files from before the registry) the empty
// window keeps the whole measurement: one partition alone could not
// advertise the columns only older files have.
func TestQueryOpenEndedEmptyWindowIncompleteAnchorKeepsGlob(t *testing.T) {
	e := newFieldSchemaEnv(t, fieldschema.Options{}, false)
	now := time.Now().UTC()
	e.write("sch", "legacy", now.Add(-30*time.Hour), map[string]interface{}{"old_only": []int64{1}})
	e.buf.SetFieldSchema(e.reg)
	e.write("sch", "legacy", now.Add(-5*time.Hour), map[string]interface{}{"recent": []int64{2}})
	require.False(t, e.reg.IsComplete("sch", "legacy"))

	out := e.mustRows("SELECT * FROM sch.legacy WHERE time > NOW() - INTERVAL '1 hour'")
	require.Equal(t, 0, out.RowCount)
	require.True(t, hasAll(out.Columns, "old_only", "recent"), "%v", out.Columns)
}

// The empty-window answer applies only where the extracted start is the
// query's real predicate for its only table: a JOIN or subquery applies one
// table's start to a dimension measurement whose newest partition does not
// hold every row, and an OR admits rows outside the window.
func TestQueryOpenEndedEmptyWindowNeedsTheWholePredicate(t *testing.T) {
	e := newFieldSchemaEnv(t, fieldschema.Options{}, true)
	now := time.Now().UTC()
	e.write("sch", "hosts", now.Add(-30*time.Hour), map[string]interface{}{"hid": []int64{1}})
	e.write("sch", "hosts", now.Add(-5*time.Hour), map[string]interface{}{"hid": []int64{2}})
	e.write("sch", "cpu", now.Add(-5*time.Minute), map[string]interface{}{"hid": []int64{1}})
	e.write("sch", "cpu", now.Add(-6*time.Minute), map[string]interface{}{"hid": []int64{2}})
	e.write("sch", "ev", now.Add(-30*time.Hour), map[string]interface{}{"alarm": []int64{1}})
	e.write("sch", "ev", now.Add(-5*time.Hour), map[string]interface{}{"alarm": []int64{0}})
	for sql, want := range map[string]int{
		"SELECT c.hid FROM sch.cpu c JOIN sch.hosts h ON c.hid = h.hid WHERE c.time > NOW() - INTERVAL '1 hour'":                          2,
		"SELECT hid FROM sch.cpu WHERE hid IN (SELECT hid FROM sch.hosts) AND time > NOW() - INTERVAL '1 hour'":                           2,
		"SELECT * FROM sch.ev WHERE time > NOW() - INTERVAL '1 hour' OR alarm = 1":                                                        1,
		"SELECT * FROM sch.ev WHERE NOT (time < NOW() - INTERVAL '1 hour') AND time > NOW() - INTERVAL '1 hour'":                          0,
		"WITH h AS (SELECT hid FROM sch.hosts) SELECT c.hid FROM sch.cpu c, h WHERE c.hid = h.hid AND c.time > NOW() - INTERVAL '1 hour'": 2,
	} {
		require.Equal(t, want, e.mustRows(sql).RowCount, sql)
	}
}
