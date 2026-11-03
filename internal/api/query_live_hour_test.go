//go:build duckdb_arrow

package api

// A dashboard query sees the current hour's rows as soon as that hour's first
// file is flushed: the pruned partition list it got before is not reused
// from the partition, glob or SQL transform caches.

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
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

func TestQuerySeesNewHourAfterItsFirstFlush(t *testing.T) {
	// The current hour must not change during the test.
	if rest := time.Until(time.Now().UTC().Truncate(time.Hour).Add(time.Hour)); rest < 30*time.Second {
		time.Sleep(rest + time.Second)
	}
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
	write(now.Truncate(time.Hour).Add(-30 * time.Minute)) // the previous hour

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

	count := func(sql, header string) float64 {
		body, _ := json.Marshal(map[string]string{"sql": sql})
		req, _ := http.NewRequest(http.MethodPost, "http://"+ln.Addr().String()+"/api/v1/query", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		if header != "" {
			req.Header.Set("x-arc-database", header)
		}
		resp, err := client.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		out, _ := io.ReadAll(resp.Body)
		require.Equal(t, 200, resp.StatusCode, string(out))
		var r struct{ Data [][]float64 }
		require.NoError(t, json.Unmarshal(out, &r), string(out))
		require.Len(t, r.Data, 1, string(out))
		return r.Data[0][0]
	}
	queries := []struct{ sql, header string }{
		{"SELECT COUNT(*) FROM db1.cpu WHERE time > NOW() - INTERVAL '2 hour'", ""}, // cached transform path
		{"SELECT COUNT(*) FROM cpu WHERE time > NOW() - INTERVAL '2 hour'", "db1"},  // header DB, quoted
		{"SELECT COUNT(*) FROM cpu WHERE time > NOW() - INTERVAL 2 hour", "db1"},    // header DB fast path
		// a bounded range ending after the current hour
		{"SELECT COUNT(*) FROM db1.cpu WHERE time >= '" + now.Truncate(time.Hour).Add(-time.Hour).Format(time.RFC3339) +
			"' AND time < '" + now.Truncate(time.Hour).Add(time.Hour).Format(time.RFC3339) + "'", ""},
	}
	for _, q := range queries {
		require.Equal(t, float64(1), count(q.sql, q.header), q.sql)
	}
	write(now) // the current hour's first file
	for _, q := range queries {
		require.Equal(t, float64(2), count(q.sql, q.header), "%s: the current hour's rows are missing", q.sql)
	}
}

// The transform cache never serves SQL pruned before an hour started: at
// 14:59:50 hour 15 has not started and its absence is cached (as before); at
// 15:00:10 it has, and its first file must be read.
func TestTransformCacheKeyedByHour(t *testing.T) {
	dataDir := t.TempDir()
	logger := zerolog.Nop()
	backend, err := storage.NewLocalBackend(dataDir, logger)
	require.NoError(t, err)
	duck, err := database.New(&database.Config{MemoryLimit: "256MB", ThreadCount: 2, MaxConnections: 4, LocalStorageRoot: dataDir}, logger)
	require.NoError(t, err)
	defer duck.Close()
	h := NewQueryHandler(duck, backend, logger, 0, 0)
	hour := func(hh int) {
		dir := filepath.Join(dataDir, "db1", "cpu", "2026", "10", "17", fmt.Sprintf("%02d", hh))
		require.NoError(t, os.MkdirAll(dir, 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(dir, "f.parquet"), []byte("x"), 0o644))
	}
	hour(14)
	now := time.Date(2026, 10, 17, 14, 59, 50, 0, time.UTC)
	h.pruner.SetClock(func() time.Time { return now })
	sql := "SELECT COUNT(*) FROM db1.cpu WHERE time >= '2026-10-17T14:00:00Z' AND time < '2026-10-17T16:00:00Z'"
	out, _, err := h.getTransformedSQL(context.Background(), sql, "")
	require.NoError(t, err)
	require.NotContains(t, out, "/15/")
	now = time.Date(2026, 10, 17, 15, 0, 10, 0, time.UTC)
	hour(15)
	out, _, err = h.getTransformedSQL(context.Background(), sql, "")
	require.NoError(t, err)
	require.Contains(t, out, "/15/", "SQL pruned before the hour started was served after it")
}
