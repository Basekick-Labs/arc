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
	"sort"
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

// duckdbNonAliasKeywords must be exactly DuckDB's reserved and type_function
// keywords, so an upgrade that moves a keyword fails here rather than in a
// query that gets an alias where DuckDB reads a keyword, or none where it
// reads an alias.
func TestDuckDBNonAliasKeywordsMatchDuckDB(t *testing.T) {
	duck, err := database.New(&database.Config{MemoryLimit: "256MB", ThreadCount: 1, MaxConnections: 1, LocalStorageRoot: t.TempDir()}, zerolog.Nop())
	require.NoError(t, err)
	defer duck.Close()
	rows, err := duck.DB().Query("SELECT keyword_name FROM duckdb_keywords() WHERE keyword_category IN ('reserved', 'type_function')")
	require.NoError(t, err)
	defer rows.Close()
	var got []string
	for rows.Next() {
		var k string
		require.NoError(t, rows.Scan(&k))
		got = append(got, k)
	}
	require.NoError(t, rows.Err())
	want := make([]string, 0, len(duckdbNonAliasKeywords))
	for k := range duckdbNonAliasKeywords {
		want = append(want, k)
	}
	sort.Strings(got)
	sort.Strings(want)
	require.Equal(t, got, want)
}

// End to end through /api/v1/query on real Parquet: queries that qualify
// columns by the table's own name return rows instead of "Referenced table
// readings not found" (#1447).
func TestQueryTableNameQualifier_Issue1447(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	dataDir := t.TempDir()
	logger := zerolog.Nop()
	backend, err := storage.NewLocalBackend(dataDir, logger)
	require.NoError(t, err)
	buf := ingest.NewArrowBuffer(&config.IngestConfig{MaxBufferSize: 100000, MaxBufferAgeMS: 600000, Compression: "snappy", FlushWorkers: 1, FlushQueueSize: 4, ShardCount: 1, DataPageVersion: "2.0"}, backend, logger)
	defer buf.Close()
	write := func(measurement string, at time.Time, ch string, field string, v float64) {
		require.NoError(t, buf.WriteTypedColumnarDirect(context.Background(), "plant", measurement, &ingest.TypedColumnBatch{
			Data: map[string]interface{}{"time": []int64{at.UnixMicro()}, "channel_id": []string{ch}, field: []float64{v}},
		}, 1))
	}
	write("readings", now.Add(-3*time.Hour), "a", "value", 1)
	write("readings", now.Add(-1*time.Hour), "a", "value", 2)
	write("readings", now.Add(-2*time.Hour), "b", "value", 10)
	write("readings", now.Add(-5*24*time.Hour), "c", "value", 100)
	write("limits", now.Add(-1*time.Hour), "a", "max", 1.5)
	write("limits", now.Add(-1*time.Hour), "b", "max", 50)
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

	query := func(headerDB, sql string) string {
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
		require.Equal(t, 200, resp.StatusCode, "%s\n%s", sql, out)
		var r struct{ Data [][]any }
		require.NoError(t, json.Unmarshal(out, &r), string(out))
		return fmt.Sprint(r.Data)
	}

	at := "TIMESTAMP '" + now.Add(-90*time.Minute).Format("2006-01-02 15:04:05") + "'"
	for _, tt := range []struct {
		name, db, sql, want string
	}{
		{"issue repro, header database", "plant",
			"SELECT readings.value FROM readings WHERE readings.channel_id = 'b'", "[[10]]"},
		{"fast path, header database", "plant",
			"SELECT readings.value FROM readings ORDER BY readings.value LIMIT 1", "[[1]]"},
		{"database-qualified reference", "",
			"SELECT readings.value FROM plant.readings WHERE readings.channel_id = 'b'", "[[10]]"},
		{"value at a point in time, LATERAL per channel", "plant",
			"SELECT c.ch, r.value FROM (VALUES ('a'), ('b'), ('c')) c(ch) LEFT JOIN LATERAL (" +
				"SELECT value FROM readings WHERE readings.channel_id = c.ch AND time <= " + at +
				" ORDER BY time DESC LIMIT 1) r ON true ORDER BY c.ch",
			"[[a 1] [b 10] [c 100]]"},
		{"correlated scalar subquery", "plant",
			"SELECT c.ch, (SELECT max(readings.value) FROM readings WHERE readings.channel_id = c.ch) " +
				"FROM (VALUES ('a'), ('c')) c(ch) ORDER BY c.ch",
			"[[a 2] [c 100]]"},
		{"JOIN of two measurements", "plant",
			"SELECT readings.channel_id, readings.value FROM readings JOIN limits " +
				"ON readings.channel_id = limits.channel_id WHERE readings.value > limits.max",
			"[[a 2]]"},
		{"comma join", "plant",
			"SELECT readings.channel_id FROM limits, readings WHERE readings.channel_id = limits.channel_id " +
				"AND readings.value > limits.max",
			"[[a]]"},
		// A name the query also gives another relation is not given to the
		// table (#1450), so these read as they did before #1447. Naming the
		// table too made the first a duplicate alias and changed which
		// relation the second's subquery reads.
		{"another relation aliased with the table's name: JOIN", "plant",
			"SELECT readings.channel_id, count(*) FROM readings JOIN (SELECT 'b' AS channel_id) readings " +
				"USING (channel_id) GROUP BY 1",
			"[[b 1]]"},
		{"another relation aliased with the table's name: outer scope", "plant",
			"SELECT count(*) FROM (VALUES (5)) readings(value) WHERE EXISTS (" +
				"SELECT 1 FROM readings WHERE readings.value = 100)",
			"[[0]]"},
		{"unqualified query unchanged", "plant",
			"SELECT count(*) FROM readings", "[[4]]"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, query(tt.db, tt.sql))
		})
	}
}
