//go:build duckdb_arrow

package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
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
	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// File-level time pruning end to end, with the feature on: a live-hour file
// flushed before the query's start is left out, the answer is unchanged, and
// a file flushed between two identical queries is read by the second (an
// expanded list is never served from a cache).
func TestQueryFileTimePruningLiveHour(t *testing.T) {
	now := time.Now().UTC()
	hour := now.Truncate(time.Hour)
	if now.Sub(hour) < 90*time.Second || hour.Add(time.Hour).Sub(now) < 30*time.Second {
		t.Skip("too close to an hour boundary for a live-hour test")
	}
	dataDir := t.TempDir()
	logger := zerolog.Nop()
	backend, err := storage.NewLocalBackend(dataDir, logger)
	require.NoError(t, err)
	buf := ingest.NewArrowBuffer(&config.IngestConfig{MaxBufferSize: 100000, MaxBufferAgeMS: 600000, Compression: "snappy",
		FlushWorkers: 1, FlushQueueSize: 4, ShardCount: 1, DataPageVersion: "2.0"}, backend, logger)
	defer buf.Close()
	write := func(at time.Time) {
		require.NoError(t, buf.WriteTypedColumnarDirect(context.Background(), "db1", "m", &ingest.TypedColumnBatch{
			Data: map[string]interface{}{"time": []int64{at.UnixMicro()}, "v": []float64{1}},
		}, 1))
		require.NoError(t, buf.FlushAllAndWait(context.Background()))
	}
	hourDir := filepath.Join(dataDir, "db1", "m", hour.Format("2006"), hour.Format("01"), hour.Format("02"), hour.Format("15"))

	// An early file of the hour: its row at hour+1s, and renamed to that
	// flush time (as if flushed then).
	early := hour.Add(time.Second)
	write(early)
	files, _ := filepath.Glob(filepath.Join(hourDir, "*.parquet"))
	require.Len(t, files, 1)
	require.NoError(t, os.Rename(files[0], filepath.Join(hourDir,
		fmt.Sprintf("m_%s_%09d.parquet", early.Format("20060102_150405"), 0))))
	write(now.Add(-5 * time.Second))

	duck, err := database.New(&database.Config{MemoryLimit: "256MB", ThreadCount: 2, MaxConnections: 4, LocalStorageRoot: dataDir}, logger)
	require.NoError(t, err)
	defer duck.Close()
	h := NewQueryHandler(duck, backend, logger, 0, 0)
	h.SetFileTimePruning(true, time.Second)
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
		}
		require.NoError(t, json.Unmarshal(out, &r), string(out))
		require.True(t, r.Success, string(out))
		n, _ := r.Data[0][0].(float64)
		return n
	}
	sql := fmt.Sprintf("SELECT COUNT(*) FROM db1.m WHERE time >= '%s'", now.Add(-30*time.Second).Format("2006-01-02 15:04:05"))
	before := metrics.Get().Snapshot()["file_time_pruning_expanded_total"].(int64)
	require.Equal(t, float64(1), count(sql), "the early file's row is before the start; only the recent row counts")
	require.Equal(t, int64(1), metrics.Get().Snapshot()["file_time_pruning_expanded_total"].(int64)-before,
		"the early file should have been left out of the read")

	write(time.Now().UTC())
	require.Equal(t, float64(2), count(sql), "a file flushed after the first query must be read by the second")
}
