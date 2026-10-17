//go:build duckdb_arrow

package api

// A query over several hour partitions with the database in the header (the
// shape dashboards send) answers the same, correct result on every endpoint.
// The JSON endpoint fanned such queries out, one DuckDB query per partition,
// and concatenated the outputs: COUNT(*) came back as one count per hour,
// ORDER BY ... LIMIT as LIMIT rows per hour. It also held a pooled
// connection per partition until the response was sent, so a query over
// more partitions than free connections never answered; and the msgpack
// endpoint ran the fan-out's template SQL ("FROM {PARTITION_PATH}") and
// failed with a parser error.

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/Basekick-Labs/msgpack/v6"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/database"
	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

func TestQueryOverPartitionsIsCorrectOnEveryEndpoint(t *testing.T) {
	dataDir := t.TempDir()
	logger := zerolog.Nop()
	backend, err := storage.NewLocalBackend(dataDir, logger)
	require.NoError(t, err)
	buf := ingest.NewArrowBuffer(&config.IngestConfig{MaxBufferSize: 100000, MaxBufferAgeMS: 600000, Compression: "snappy", FlushWorkers: 1, FlushQueueSize: 4, ShardCount: 1, DataPageVersion: "2.0"}, backend, logger)
	// Five hour partitions, 30 minutes into each hour (an hour boundary
	// crossed while the test runs cannot move NOW()-6h past them).
	base := time.Now().UTC().Truncate(time.Hour)
	for h := 0; h < 5; h++ {
		ts := base.Add(-time.Duration(h+1)*time.Hour + 30*time.Minute).UnixMicro()
		require.NoError(t, buf.WriteTypedColumnarDirect(context.Background(), "db1", "cpu", &ingest.TypedColumnBatch{
			Data: map[string]interface{}{"time": []int64{ts, ts + 1}, "v": []float64{float64(h), float64(h) + 0.5}},
		}, 2))
	}
	require.NoError(t, buf.FlushAllAndWait(context.Background()))
	buf.Close()
	// Fewer connections than partitions.
	duck, err := database.New(&database.Config{MemoryLimit: "256MB", ThreadCount: 2, MaxConnections: 2, LocalStorageRoot: dataDir}, logger)
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

	post := func(t *testing.T, path, sql string) []byte {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, "http://"+ln.Addr().String()+path, bytes.NewReader([]byte(`{"sql":"`+sql+`"}`)))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("x-arc-database", "db1")
		resp, err := client.Do(req)
		require.NoError(t, err, "%s %s", path, sql) // a hang times out here
		defer resp.Body.Close()
		out, _ := io.ReadAll(resp.Body)
		require.Equal(t, 200, resp.StatusCode, "%s %s: %s", path, sql, out)
		return out
	}
	// Each endpoint's answer as one column of float64 values.
	column := map[string]func(t *testing.T, body []byte) []float64{
		"/api/v1/query": func(t *testing.T, body []byte) []float64 {
			var r struct{ Data [][]float64 }
			require.NoError(t, json.Unmarshal(body, &r))
			var out []float64
			for _, row := range r.Data {
				out = append(out, row[0])
			}
			return out
		},
		"/api/v1/query/msgpack": func(t *testing.T, body []byte) []float64 {
			var r struct {
				Data [][]interface{} `msgpack:"data"`
			}
			require.NoError(t, msgpack.Unmarshal(body, &r))
			var out []float64
			for _, v := range r.Data[0] {
				switch x := v.(type) {
				case float64:
					out = append(out, x)
				case int64:
					out = append(out, float64(x))
				case uint64:
					out = append(out, float64(x))
				case int8:
					out = append(out, float64(x))
				}
			}
			return out
		},
		"/api/v1/query/arrow": func(t *testing.T, body []byte) []float64 {
			rdr, err := ipc.NewReader(bytes.NewReader(body))
			require.NoError(t, err)
			defer rdr.Release()
			var out []float64
			for rdr.Next() {
				switch c := rdr.Record().Column(0).(type) {
				case *array.Float64:
					out = append(out, c.Float64Values()...)
				case *array.Int64:
					for _, v := range c.Int64Values() {
						out = append(out, float64(v))
					}
				}
			}
			return out
		},
	}
	cases := []struct {
		sql  string
		want []float64
	}{
		{"SELECT COUNT(*) AS n FROM cpu WHERE time > NOW() - INTERVAL 6 hour", []float64{10}},
		{"SELECT SUM(v) AS s FROM cpu WHERE time > NOW() - INTERVAL 6 hour", []float64{22.5}},
		{"SELECT v FROM cpu WHERE time > NOW() - INTERVAL 6 hour ORDER BY v DESC LIMIT 3", []float64{4.5, 4, 3.5}},
		{"SELECT MAX(v) AS m FROM cpu WHERE time > NOW() - INTERVAL 6 hour GROUP BY 1 = 1", []float64{4.5}},
	}
	for path, decode := range column {
		t.Run(path, func(t *testing.T) {
			for _, tc := range cases {
				require.Equal(t, tc.want, decode(t, post(t, path, tc.sql)), "%s %s", path, tc.sql)
			}
		})
	}
}
