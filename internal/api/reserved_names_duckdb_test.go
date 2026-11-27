//go:build duckdb_arrow

package api

import (
	"bytes"
	"context"
	"encoding/json"
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

// Through /api/v1/query: a reserved name never reads anything, and in the
// x-arc-database header it is refused outright.
func TestQueryReservedPlaceholderNames(t *testing.T) {
	dataDir := t.TempDir()
	logger := zerolog.Nop()
	backend, err := storage.NewLocalBackend(dataDir, logger)
	require.NoError(t, err)
	buf := ingest.NewArrowBuffer(&config.IngestConfig{MaxBufferSize: 100000, MaxBufferAgeMS: 600000, Compression: "snappy", FlushWorkers: 1, FlushQueueSize: 4, ShardCount: 1, DataPageVersion: "2.0"}, backend, logger)
	defer buf.Close()
	now := time.Now().UTC()
	for db, v := range map[string]float64{"default": 1, "other": 4242} {
		require.NoError(t, buf.WriteTypedColumnarDirect(context.Background(), db, "x", &ingest.TypedColumnBatch{
			Data: map[string]interface{}{"time": []int64{now.UnixMicro()}, "v": []float64{v}},
		}, 1))
	}
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
	client := &http.Client{Timeout: 20 * time.Second}
	post := func(headerDB, sql string) (int, string) {
		body, _ := json.Marshal(map[string]string{"sql": sql})
		req, _ := http.NewRequest(http.MethodPost, "http://"+ln.Addr().String()+"/api/v1/query", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		if headerDB != "" {
			req.Header.Set("x-arc-database", headerDB)
		}
		resp, err := client.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		out, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(out)
	}

	code, out := post("", "SELECT v FROM x")
	require.Equal(t, 200, code, out)
	require.Contains(t, out, "[[1]]", "control query")

	for _, sql := range []string{
		"SELECT v FROM x__STR_0__ WHERE v <> '||chr(47)||chr(46)||chr(46)||chr(47)||'other'||chr(47)||'x'||'",
		"SELECT v FROM x__STR_0__ /* '||chr(47)||chr(46)||chr(46)||chr(47)||'other'||chr(47)||'x'||' */",
		`SELECT v AS "'||chr(47)||chr(46)||chr(46)||chr(47)||'other'||chr(47)||'x'||'" FROM x__IDENT_0__`,
	} {
		_, out := post("", sql)
		require.NotContains(t, out, "4242", sql)
		require.False(t, strings.Contains(out, "read_parquet('") && strings.Contains(out, "x'||"), "literal spliced into a path: %s", out)
	}

	code, out = post("x__STR_0__", "SELECT v FROM x WHERE v <> 'a'")
	require.Equal(t, 400, code, out)
}
