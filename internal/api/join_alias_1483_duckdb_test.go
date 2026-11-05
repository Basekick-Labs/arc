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

// Two measurements joined without aliases were both named "read_parquet" by
// DuckDB, so any unqualified column or * failed to bind: "Ambiguous
// reference to table read_parquet" (#1483). With and without a header
// database, since both rewrite paths share the naming.
func TestJoinOfTwoMeasurementsBindsUnqualifiedColumns_Issue1483(t *testing.T) {
	at := time.Now().UTC().Add(-time.Hour).Truncate(time.Second)
	dataDir := t.TempDir()
	logger := zerolog.Nop()
	backend, err := storage.NewLocalBackend(dataDir, logger)
	require.NoError(t, err)
	buf := ingest.NewArrowBuffer(&config.IngestConfig{MaxBufferSize: 100000, MaxBufferAgeMS: 600000, Compression: "snappy", FlushWorkers: 1, FlushQueueSize: 4, ShardCount: 1, DataPageVersion: "2.0"}, backend, logger)
	defer buf.Close()
	write := func(db, measurement, field string, v float64) {
		require.NoError(t, buf.WriteTypedColumnarDirect(context.Background(), db, measurement, &ingest.TypedColumnBatch{
			Data: map[string]interface{}{"time": []int64{at.UnixMicro()}, "host": []string{"a"}, field: []float64{v}},
		}, 1))
	}
	for _, db := range []string{"plant", "default"} {
		write(db, "cpu", "usage", 1)
		write(db, "mem", "used", 2)
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

	query := func(headerDB, sql string) (int, string) {
		t.Helper()
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
		var r struct {
			Data  [][]any
			Error string
		}
		require.NoError(t, json.Unmarshal(out, &r), string(out))
		if resp.StatusCode != http.StatusOK {
			return resp.StatusCode, r.Error
		}
		return resp.StatusCode, fmt.Sprint(r.Data)
	}

	for _, db := range []string{"plant", ""} {
		for _, tt := range []struct{ name, sql, want string }{
			{"USING with an unqualified filter", "SELECT count(*) FROM cpu JOIN mem USING (time, host) WHERE host = 'a'", "[[1]]"},
			{"USING with a time filter", "SELECT count(*) FROM cpu JOIN mem USING (time, host) WHERE time > now() - INTERVAL 1 DAY", "[[1]]"},
			{"USING, star", "SELECT host, usage, used FROM (SELECT * FROM cpu JOIN mem USING (time, host))", "[[a 1 2]]"},
			{"ON, star", "SELECT host, usage, used FROM (SELECT * FROM cpu JOIN mem ON true)", "[[a 1 2]]"},
			{"ON, unqualified columns", "SELECT usage, used FROM cpu JOIN mem ON cpu.host = mem.host", "[[1 2]]"},
			{"comma join, star", "SELECT host, usage, used FROM (SELECT * FROM cpu, mem)", "[[a 1 2]]"},
			{"comma join, unqualified filter", "SELECT count(*) FROM cpu, mem WHERE usage < used", "[[1]]"},
			{"NATURAL JOIN", "SELECT host, usage, used FROM cpu NATURAL JOIN mem", "[[a 1 2]]"},
			{"a subquery is a scope of its own",
				"SELECT usage FROM cpu WHERE host IN (SELECT host FROM mem WHERE used > 1)", "[[1]]"},
			{"another relation named like the table is left alone (#1450)",
				"SELECT count(*) FROM cpu JOIN (SELECT 'a' AS host) cpu USING (host)", "[[1]]"},
			{"a CTE named like a measurement is not given a second relation of its name",
				"WITH cpu AS (SELECT * FROM mem) SELECT count(*) FROM cpu JOIN mem USING (time)", "[[1]]"},
			{"an aliased table keeps its alias", "SELECT c.usage, m.used FROM cpu c JOIN mem AS m USING (host)", "[[1 2]]"},
			// A name the query also uses for something else keeps binding
			// to that: DuckDB binds a table alias first (review finding).
			{"a SELECT-list alias named like a table, in GROUP BY",
				"SELECT usage + 1 AS mem, count(*) FROM cpu JOIN mem USING (host) GROUP BY mem", "[[2 1]]"},
			{"a SELECT-list alias named like a table, in WHERE",
				"SELECT usage AS mem FROM cpu JOIN mem USING (host) WHERE mem > 0", "[[1]]"},
			{"an outer column named like a table, in a correlated subquery",
				"SELECT (SELECT count(*) FROM cpu JOIN mem USING (host) WHERE mem = 42) FROM (SELECT 42 AS mem) t", "[[1]]"},
			{"a quoted column named like a table",
				`SELECT "mem" FROM (SELECT 7 AS mem) t WHERE "mem" IN (SELECT usage + 6 FROM cpu JOIN mem USING (host))`, "[[7]]"},
		} {
			t.Run(fmt.Sprintf("db=%q/%s", db, tt.name), func(t *testing.T) {
				status, got := query(db, tt.sql)
				require.Equal(t, http.StatusOK, status, got)
				require.Equal(t, tt.want, got)
			})
		}
	}

	// The same name twice in one FROM is still ambiguous: both references
	// get the name, as both used to get "read_parquet". Aliases fix it.
	t.Run("self-join without aliases stays ambiguous", func(t *testing.T) {
		status, got := query("", "SELECT count(*) FROM cpu JOIN plant.cpu USING (time) WHERE host = 'a'")
		require.NotEqual(t, http.StatusOK, status)
		require.Contains(t, got, "Ambiguous reference")
		status, got = query("", "SELECT count(*) FROM cpu a JOIN plant.cpu b USING (time) WHERE a.host = 'a'")
		require.Equal(t, http.StatusOK, status, got)
		require.Equal(t, "[[1]]", got)
	})
}
