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
	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// Every query here returned too few rows on main: the time range read from
// the SQL was narrower than the query, so partition pruning left out hours
// holding rows the query asked for. Each answer must now match DuckDB's own
// over all the data.
func TestQueryTimeRangeShapesReturnEveryRow(t *testing.T) {
	dataDir := t.TempDir()
	logger := zerolog.Nop()
	backend, err := storage.NewLocalBackend(dataDir, logger)
	require.NoError(t, err)
	buf := ingest.NewArrowBuffer(&config.IngestConfig{MaxBufferSize: 100000, MaxBufferAgeMS: 600000, Compression: "snappy",
		FlushWorkers: 1, FlushQueueSize: 4, ShardCount: 1, DataPageVersion: "2.0"}, backend, logger)
	defer buf.Close()
	write := func(meas, at, host string) {
		ts, _ := time.Parse("2006-01-02 15:04:05", at)
		require.NoError(t, buf.WriteTypedColumnarDirect(context.Background(), "db1", meas, &ingest.TypedColumnBatch{
			Data: map[string]interface{}{"time": []int64{ts.UnixMicro()}, "v": []float64{1}, "host": []string{host}},
		}, 1))
		require.NoError(t, buf.FlushAllAndWait(context.Background()))
	}
	write("cpu", "2019-06-01 12:00:00", "a")
	write("cpu", "2023-01-01 00:30:00", "c")
	write("cpu", "2026-10-20 08:30:00", "a")
	write("cpu", "2026-10-20 10:00:00", "a")
	write("cpu", "2026-10-20 11:30:00", "b")
	write("mem", "2026-10-19 05:00:00", "a")
	write("mem", "2026-10-20 10:30:00", "a")

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
		var r struct {
			Success bool
			Data    [][]interface{}
			Error   string
		}
		require.NoError(t, json.Unmarshal(out, &r), string(out))
		require.True(t, r.Success, "%s: %s", sql, r.Error)
		if len(r.Data) == 0 || len(r.Data[0]) == 0 || r.Data[0][0] == nil {
			return 0
		}
		n, _ := r.Data[0][0].(float64)
		return n
	}
	for _, tc := range []struct {
		want float64
		sql  string
	}{
		{4, "SELECT COUNT(*) FROM db1.cpu WHERE time >= '2026-10-20 11:00:00' OR host = 'a'"},
		{2, "SELECT COUNT(*) FROM db1.cpu WHERE time >= '2026-10-20 08:00:00' AND time <= '2026-10-20 10:00:00'"},
		{2, "SELECT COUNT(*) FROM db1.cpu WHERE time BETWEEN '2026-10-20 08:00:00' AND '2026-10-20 10:00:00'"},
		{2, "SELECT COUNT(*) FROM db1.cpu WHERE time < '2024-01-01'"},
		{4, "SELECT COUNT(*) FROM db1.cpu c JOIN db1.mem m ON true WHERE c.time >= '2026-10-20 10:00:00'"},
		{4, "SELECT COUNT(*) FROM (SELECT time FROM db1.cpu WHERE time >= '2026-10-20 10:00:00' UNION ALL SELECT time FROM db1.mem)"},
		{4, "WITH m AS (SELECT * FROM db1.mem) SELECT COUNT(*) FROM db1.cpu c, m WHERE c.time >= '2026-10-20 10:00:00'"},
		{3, "SELECT COUNT(*) FROM db1.cpu WHERE host = 'a'\n-- AND time >= '2026-10-20 10:00:00'\n"},
		{0, "SELECT COUNT(*) FROM (SELECT time, lag(time) OVER (ORDER BY time) p FROM db1.cpu) WHERE time >= '2026-10-20 10:00:00' AND p IS NULL"},
		{4, "SELECT SUM(c) FROM (SELECT host, max(time) AS time, count(*) c FROM db1.cpu GROUP BY host) s WHERE time >= '2026-10-20 10:00:00'"},
		{3, "SELECT COUNT(*) FROM db1.cpu WHERE time >= '2026-10-20 11:00:00'::timestamp - INTERVAL '3 hours'"},
		{3, "SELECT COUNT(*) FROM db1.cpu WHERE NOT (time >= '2026-10-20 10:00:00')"},
		// Still pruned, still right.
		{2, "SELECT COUNT(*) FROM db1.cpu WHERE (time) >= '2026-10-20T10:00:00Z' AND (time) < '2026-10-20T12:00:00Z'"},
		{1, "SELECT COUNT(*) FROM db1.cpu WHERE time >= '2026-10-20 10:00:00' AND host IN ('a')"},
	} {
		require.Equal(t, tc.want, count(tc.sql), tc.sql)
	}
}
