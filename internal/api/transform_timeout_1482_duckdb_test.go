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

// query.timeout bounds the SQL transform as well as the execution, and every
// endpoint that transforms answers a timeout there with 504 (#1482). A
// nanosecond timeout has always passed by the time the transform finishes, so
// each request below is refused by the transform's own check; the SQL is never
// cached.
func TestEveryEndpointAnswers504WhenTheTransformTimesOut_Issue1482(t *testing.T) {
	dataDir := t.TempDir()
	logger := zerolog.Nop()
	backend, err := storage.NewLocalBackend(dataDir, logger)
	require.NoError(t, err)
	buf := ingest.NewArrowBuffer(&config.IngestConfig{MaxBufferSize: 100000, MaxBufferAgeMS: 600000, Compression: "snappy", FlushWorkers: 1, FlushQueueSize: 4, ShardCount: 1, DataPageVersion: "2.0"}, backend, logger)
	defer buf.Close()
	require.NoError(t, buf.WriteTypedColumnarDirect(context.Background(), "default", "cpu", &ingest.TypedColumnBatch{
		Data: map[string]interface{}{"time": []int64{time.Now().UnixMicro()}, "host": []string{"a"}}}, 1))
	require.NoError(t, buf.FlushAllAndWait(context.Background()))
	duck, err := database.New(&database.Config{MemoryLimit: "256MB", ThreadCount: 2, MaxConnections: 4, LocalStorageRoot: dataDir}, logger)
	require.NoError(t, err)
	defer duck.Close()
	h := NewQueryHandler(duck, backend, logger, 0, 0)
	h.queryTimeout = time.Nanosecond
	app := fiber.New(appConfig(DefaultServerConfig(), zerolog.Nop()))
	h.RegisterRoutes(app)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() { _ = app.Listener(ln) }()
	defer app.Shutdown()
	base := "http://" + ln.Addr().String()

	sqlBody := func(sql string) []byte {
		b, _ := json.Marshal(map[string]string{"sql": sql})
		return b
	}
	for _, tt := range []struct {
		name, method, path string
		body               []byte
	}{
		{"json", http.MethodPost, "/api/v1/query", sqlBody("SELECT * FROM cpu, cpu c2")},
		{"msgpack", http.MethodPost, "/api/v1/query/msgpack", sqlBody("SELECT * FROM cpu, cpu c3")},
		{"arrow", http.MethodPost, "/api/v1/query/arrow", sqlBody("SELECT * FROM cpu, cpu c4")},
		{"estimate", http.MethodPost, "/api/v1/query/estimate", sqlBody("SELECT * FROM cpu, cpu c5")},
		{"measurement", http.MethodGet, "/api/v1/query/cpu?database=default", nil},
	} {
		t.Run(tt.name, func(t *testing.T) {
			req, _ := http.NewRequest(tt.method, base+tt.path, bytes.NewReader(tt.body))
			req.Header.Set("Content-Type", "application/json")
			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			defer resp.Body.Close()
			out, _ := io.ReadAll(resp.Body)
			require.Equal(t, http.StatusGatewayTimeout, resp.StatusCode, string(out))
			require.Contains(t, string(out), "Query timed out")
		})
	}
	require.Zero(t, h.queryCache.Size())
}
