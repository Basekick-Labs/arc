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
	"strings"
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

// The reporter's case: the latest value of thousands of named channels in one
// query. 2,500 IDs make a statement of about 70 KB, refused at the default cap
// and answered with query.max_sql_length raised.
func TestQueryLongChannelList_Issue1448(t *testing.T) {
	now := time.Now().UTC()
	dataDir := t.TempDir()
	logger := zerolog.Nop()
	backend, err := storage.NewLocalBackend(dataDir, logger)
	require.NoError(t, err)
	buf := ingest.NewArrowBuffer(&config.IngestConfig{MaxBufferSize: 100000, MaxBufferAgeMS: 600000, Compression: "snappy", FlushWorkers: 1, FlushQueueSize: 4, ShardCount: 1, DataPageVersion: "2.0"}, backend, logger)
	defer buf.Close()
	const channels = 2500
	ts := make([]int64, channels)
	ids := make([]string, channels)
	vals := make([]float64, channels)
	for i := range ids {
		ts[i] = now.Add(-time.Duration(i) * time.Second).UnixMicro()
		ids[i] = fmt.Sprintf("site1/line3/motor%04d/temp", i)
		vals[i] = float64(i)
	}
	require.NoError(t, buf.WriteTypedColumnarDirect(context.Background(), "plant", "readings", &ingest.TypedColumnBatch{
		Data: map[string]interface{}{"time": ts, "channel_id": ids, "value": vals},
	}, channels))
	require.NoError(t, buf.FlushAllAndWait(context.Background()))

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

	quoted := make([]string, channels)
	for i, id := range ids {
		quoted[i] = "'" + id + "'"
	}
	sql := "SELECT count(*) FROM (SELECT channel_id, arg_max(value, time) FROM readings WHERE channel_id IN (" +
		strings.Join(quoted, ", ") + ") GROUP BY channel_id)"
	require.Greater(t, len(sql), config.DefaultMaxSQLLength)

	post := func() (int, string) {
		body, _ := json.Marshal(map[string]string{"sql": sql})
		req, _ := http.NewRequest(http.MethodPost, "http://"+ln.Addr().String()+"/api/v1/query", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("x-arc-database", "plant")
		resp, err := (&http.Client{Timeout: 30 * time.Second}).Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		out, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(out)
	}

	SetMaxSQLLength(0)
	code, out := post()
	require.Equal(t, 400, code, out)
	require.Contains(t, out, "query.max_sql_length")

	SetMaxSQLLength(200000)
	t.Cleanup(func() { SetMaxSQLLength(0) })
	code, out = post()
	require.Equal(t, 200, code, out)
	require.Contains(t, out, fmt.Sprintf("[[%d]]", channels))
}
