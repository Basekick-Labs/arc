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

// A measurement with a column named like itself, as Telegraf's cpu has a cpu
// tag. A bare use of that column, wherever it sits, is not an alias, so the
// table keeps its name; another relation given the name is, so the table does
// not (#1450).
func TestQueryTableNameQualifierBesideAColumnOfTheSameName_Issue1450(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	dataDir := t.TempDir()
	logger := zerolog.Nop()
	backend, err := storage.NewLocalBackend(dataDir, logger)
	require.NoError(t, err)
	buf := ingest.NewArrowBuffer(&config.IngestConfig{MaxBufferSize: 100000, MaxBufferAgeMS: 600000, Compression: "snappy", FlushWorkers: 1, FlushQueueSize: 4, ShardCount: 1, DataPageVersion: "2.0"}, backend, logger)
	defer buf.Close()
	require.NoError(t, buf.WriteTypedColumnarDirect(context.Background(), "plant", "cpu", &ingest.TypedColumnBatch{
		Data: map[string]interface{}{
			"time":  []int64{now.Add(-2 * time.Hour).UnixMicro(), now.Add(-time.Hour).UnixMicro()},
			"host":  []string{"a", "b"},
			"cpu":   []string{"cpu0", "cpu1"},
			"usage": []float64{1, 3},
		},
	}, 2))
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

	query := func(sql string) string {
		t.Helper()
		body, _ := json.Marshal(map[string]string{"sql": sql})
		req, _ := http.NewRequest(http.MethodPost, "http://"+ln.Addr().String()+"/api/v1/query", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("x-arc-database", "plant")
		resp, err := client.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		out, _ := io.ReadAll(resp.Body)
		require.Equal(t, 200, resp.StatusCode, "%s\n%s", sql, out)
		var r struct{ Data [][]any }
		require.NoError(t, json.Unmarshal(out, &r), string(out))
		return fmt.Sprint(r.Data)
	}

	for _, tt := range []struct{ name, sql, want string }{
		{"column alias named like the table",
			"SELECT cpu.host, max(cpu.usage) AS cpu FROM cpu GROUP BY 1 ORDER BY 1", "[[a 1] [b 3]]"},
		{"DISTINCT ON then the column",
			"SELECT DISTINCT ON (host) cpu, cpu.usage FROM cpu ORDER BY host", "[[cpu0 1] [cpu1 3]]"},
		{"star EXCLUDE the column",
			"SELECT cpu.host, * EXCLUDE (cpu, time, host) FROM cpu ORDER BY 1", "[[a 1] [b 3]]"},
		{"FROM inside a function body",
			"SELECT cpu.host, trim(BOTH 'c' FROM cpu) FROM cpu ORDER BY 1", "[[a pu0] [b pu1]]"},
		{"another relation named cpu",
			"SELECT cpu.host, count(*) FROM cpu JOIN (SELECT 'a' AS host) cpu USING (host) GROUP BY 1", "[[a 1]]"},
		{"another relation named cpu, quoted",
			`SELECT cpu.host, count(*) FROM cpu JOIN (SELECT 'a' AS host) "cpu" USING (host) GROUP BY 1`, "[[a 1]]"},
		{"another relation named cpu, after a comment",
			"SELECT cpu.host, count(*) FROM cpu JOIN (SELECT 'a' AS host) /* c */ cpu USING (host) GROUP BY 1", "[[a 1]]"},
		{"another relation named cpu after a join condition",
			"SELECT cpu.host, count(*) FROM cpu JOIN cpu c2 ON true, (SELECT 'a' AS host) cpu GROUP BY 1", "[[a 4]]"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, query(tt.sql))
		})
	}
}
