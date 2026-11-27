//go:build duckdb_arrow

package api

// A row stamped outside the current hour (late data, or a future stamp) is
// read by queries as soon as the flush has written its file (#1757). Before,
// a query that had found the row's hour empty kept answering without it:
// 30 s for any query, 60 s for a repeated one.

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

// End to end, as cmd/arc wires it: the ingest buffer reports each file to the
// query handler. With the wiring removed the same steps show the bug.
func TestQuerySeesALateRowAsSoonAsItsFileIsWritten(t *testing.T) {
	// The current hour must not change during the test.
	if rest := time.Until(time.Now().UTC().Truncate(time.Hour).Add(time.Hour)); rest < 30*time.Second {
		time.Sleep(rest + time.Second)
	}
	now := time.Now().UTC()
	for name, tc := range map[string]struct {
		wired bool
		row   time.Time
	}{
		"late row":                    {true, now.Add(-3 * time.Hour)},
		"future-stamped row":          {true, now.Add(2 * time.Hour)},
		"late row without the wiring": {false, now.Add(-3 * time.Hour)},
	} {
		t.Run(name, func(t *testing.T) {
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
			duck, err := database.New(&database.Config{MemoryLimit: "256MB", ThreadCount: 2, MaxConnections: 4, LocalStorageRoot: dataDir}, logger)
			require.NoError(t, err)
			defer duck.Close()
			h := NewQueryHandler(duck, backend, logger, 0, 0)
			if tc.wired {
				buf.SetOnFilePublished(h.FilePublished)
			}
			app := fiber.New(appConfig(DefaultServerConfig(), zerolog.Nop()))
			h.RegisterRoutes(app)
			ln, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			go func() { _ = app.Listener(ln) }()
			defer app.Shutdown()
			client := &http.Client{Timeout: 20 * time.Second}
			count := func(sql string) float64 {
				body, _ := json.Marshal(map[string]string{"sql": sql})
				req, _ := http.NewRequest(http.MethodPost, "http://"+ln.Addr().String()+"/api/v1/query", bytes.NewReader(body))
				req.Header.Set("Content-Type", "application/json")
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
			// A range that spans hour partitions, from 5 h back to 4 h ahead.
			where := " WHERE time >= '" + now.Add(-5*time.Hour).Format(time.RFC3339) + "' AND time < '" + now.Add(4*time.Hour).Format(time.RFC3339) + "'"
			repeated := "SELECT COUNT(*) FROM db1.cpu" + where
			fresh := "SELECT COUNT(v) FROM db1.cpu" + where

			write(now) // the measurement exists, in the current hour
			require.Equal(t, float64(1), count(repeated))

			write(tc.row) // its hour had no directory when the query above ran
			want := float64(2)
			if !tc.wired {
				want = 1 // the bug: both queries are answered from what was cached
			}
			require.Equal(t, want, count(repeated), "the query that found the hour empty, repeated")
			require.Equal(t, want, count(fresh), "a query text not sent before")
		})
	}
}

// The SQL transform cache holds rewritten SQL with a path list baked in. A
// file published for a measurement that SQL reads makes the entry a miss,
// and leaves an entry that reads another measurement alone.
func TestTransformCacheDropsSQLForAMeasurementWithANewFile(t *testing.T) {
	dataDir := t.TempDir()
	logger := zerolog.Nop()
	local, err := storage.NewLocalBackend(dataDir, logger)
	require.NoError(t, err)
	duck, err := database.New(&database.Config{MemoryLimit: "256MB", ThreadCount: 2, MaxConnections: 4, LocalStorageRoot: dataDir}, logger)
	require.NoError(t, err)
	defer duck.Close()
	// The transform cache is off on a volume that folds names (#1509): every
	// APFS volume does, case-sensitive ones included (Unicode forms), so on
	// macOS there is nothing to pin. Linux volumes, and CI's, do not.
	if storageFoldsNames(local) {
		t.Skip("this volume folds names, so the SQL transform cache is off; this test runs on Linux")
	}
	h := NewQueryHandler(duck, local, logger, 0, 0)
	hour := func(measurement string, hh int) {
		dir := filepath.Join(dataDir, "db1", measurement, "2026", "10", "17", fmt.Sprintf("%02d", hh))
		require.NoError(t, os.MkdirAll(dir, 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(dir, "f.parquet"), []byte("x"), 0o644))
	}
	now := time.Date(2026, 10, 17, 14, 20, 0, 0, time.UTC)
	h.pruner.SetClock(func() time.Time { return now })
	hour("cpu", 14)
	hour("mem", 14)
	const rng = " WHERE time >= '2026-10-17T10:00:00Z' AND time < '2026-10-17T15:00:00Z'"
	cpuSQL, memSQL := "SELECT COUNT(*) FROM db1.cpu"+rng, "SELECT COUNT(*) FROM db1.mem"+rng
	transform := func(sql string) (string, bool) {
		out, hit, err := h.getTransformedSQL(context.Background(), sql, "")
		require.NoError(t, err)
		return out, hit
	}
	out, hit := transform(cpuSQL)
	require.False(t, hit)
	require.NotContains(t, out, "/11/")
	transform(memSQL)

	hour("cpu", 11) // the late row's file
	out, hit = transform(cpuSQL)
	require.True(t, hit, "the transform cache is off: this test pins nothing")
	require.NotContains(t, out, "/11/", "the cached SQL was dropped without a report: this test pins nothing")

	h.FilePublished("db1", "cpu", time.Date(2026, 10, 17, 11, 5, 0, 0, time.UTC))
	out, hit = transform(cpuSQL)
	require.False(t, hit, "SQL cached before the file was published was served after it")
	require.Contains(t, out, "/11/")
	_, hit = transform(memSQL)
	require.True(t, hit, "a file for cpu dropped the cached SQL of mem")
	_, hit = transform(cpuSQL)
	require.True(t, hit, "the SQL transformed after the file was published is not cached")

	// A file in the current hour changes nothing: its absence was never cached.
	h.FilePublished("db1", "cpu", now)
	_, hit = transform(cpuSQL)
	require.True(t, hit, "a file in the current hour dropped the cached SQL")
}
