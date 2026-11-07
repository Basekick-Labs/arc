package api

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/database"
	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// The continuous-query source rewrite touches only a `FROM db.m` that is SQL:
// one inside a literal, a quoted identifier or a comment is left as written,
// and everything outside the rewritten reference is byte for byte the body
// (#1487).
func TestWrapSourceMeasurementSkipsQuotedTextAndComments(t *testing.T) {
	const rp = "read_parquet('/d/prod/cpu/**/*.parquet')"
	for _, tt := range []struct{ name, query, want string }{
		{"single-quoted literal",
			"SELECT 'FROM prod.cpu' AS s FROM prod.cpu",
			"SELECT 'FROM prod.cpu' AS s FROM " + rp},
		{"escape string with a backslash quote",
			`SELECT E'it\'s FROM prod.cpu' AS s FROM prod.cpu`,
			`SELECT E'it\'s FROM prod.cpu' AS s FROM ` + rp},
		{"doubled quote",
			"SELECT 'it''s FROM prod.cpu' AS s FROM prod.cpu",
			"SELECT 'it''s FROM prod.cpu' AS s FROM " + rp},
		{"dollar-quoted",
			"SELECT $$FROM prod.cpu$$ AS s FROM prod.cpu",
			"SELECT $$FROM prod.cpu$$ AS s FROM " + rp},
		{"tagged dollar-quoted",
			"SELECT $t$FROM prod.cpu$t$ AS s FROM prod.cpu",
			"SELECT $t$FROM prod.cpu$t$ AS s FROM " + rp},
		{"quoted identifier",
			`SELECT 1 AS "FROM prod.cpu" FROM prod.cpu`,
			`SELECT 1 AS "FROM prod.cpu" FROM ` + rp},
		{"line comment",
			"SELECT 1 -- FROM prod.cpu\nFROM prod.cpu",
			"SELECT 1 -- FROM prod.cpu\nFROM " + rp},
		{"block comment",
			"SELECT 1 /* FROM prod.cpu */ FROM prod.cpu",
			"SELECT 1 /* FROM prod.cpu */ FROM " + rp},
		{"nested block comment",
			"SELECT 1 /* a /* b */ FROM prod.cpu */ FROM prod.cpu",
			"SELECT 1 /* a /* b */ FROM prod.cpu */ FROM " + rp},
		// Before #1487 this was left unrewritten and failed in DuckDB.
		{"comment between FROM and the name",
			"SELECT 1 FROM /* c */ prod.cpu",
			"SELECT 1 FROM " + rp},
		{"only inside a literal: nothing to rewrite",
			"SELECT 'FROM prod.cpu'",
			"SELECT 'FROM prod.cpu'"},
		{"unterminated block comment kept, so DuckDB still refuses it",
			"SELECT 1 FROM prod.cpu /* AND x",
			"SELECT 1 FROM " + rp + " /* AND x"},
		// A quote ends a word, as in DuckDB.
		{"identifier written against FROM",
			`SELECT "host"FROM prod.cpu`,
			`SELECT "host"FROM ` + rp},
		{"literal written against FROM",
			"SELECT 'a'FROM prod.cpu",
			"SELECT 'a'FROM " + rp},
		{"quoted alias written against the name",
			`SELECT c.host FROM prod.cpu"c"`,
			`SELECT c.host FROM ` + rp + `"c"`},
		// DuckDB continues an identifier through `$` and non-ASCII bytes.
		{"name continued by a non-ASCII byte", "SELECT 1 FROM prod.cpué", "SELECT 1 FROM prod.cpué"},
		{"name continued by a dollar", "SELECT 1 FROM prod.cpu$x", "SELECT 1 FROM prod.cpu$x"},
		{"FROM continuing a word", "SELECT 1 AS éFROM prod.cpu", "SELECT 1 AS éFROM prod.cpu"},
		// A word shaped like the masker's placeholder is a word.
		{"placeholder-shaped words",
			"SELECT __STR_0__ AS a, 'x' AS b, __IDENT_0__, \"h\" FROM prod.cpu",
			"SELECT __STR_0__ AS a, 'x' AS b, __IDENT_0__, \"h\" FROM " + rp},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := wrapSourceMeasurement(tt.query, "prod", "cpu", rp); got != tt.want {
				t.Errorf("\n got: %s\nwant: %s", got, tt.want)
			}
		})
	}
}

func TestCQStructureViewKeepsOffsets(t *testing.T) {
	for _, q := range []string{
		"SELECT 'a' FROM prod.cpu -- x\n",
		`SELECT E'\'', "a""b", $t$x$t$ /* y */ FROM prod.cpu`,
		"SELECT 'unterminated",
		"SELECT __STR_0__, 'x'",
		"",
	} {
		v := cqStructureView(q)
		if len(v) != len(q) {
			t.Fatalf("view of %q has length %d, want %d", q, len(v), len(q))
		}
		if i := strings.Index(q, "FROM prod.cpu"); i >= 0 && v[i:i+len("FROM prod.cpu")] != "FROM prod.cpu" {
			t.Errorf("view of %q lost the real reference: %q", q, v)
		}
	}
	if v := cqStructureView(`SELECT 'x' "y"`); v != `SELECT ' ' " "` {
		t.Errorf("quote bytes not kept: %q", v)
	}
}

// End to end: each body runs and writes its rows. A CTE named after the
// source measurement used to skip the rewrite and fail.
func TestExecuteCQ_SourceRewriteSkipsQuotedText_Issue1487(t *testing.T) {
	root := t.TempDir()
	dataDir := filepath.Join(root, "data")
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		t.Fatal(err)
	}
	logger := zerolog.Nop()
	backend, err := storage.NewLocalBackend(dataDir, logger)
	if err != nil {
		t.Fatal(err)
	}
	duck, err := database.New(&database.Config{MemoryLimit: "256MB", ThreadCount: 1, MaxConnections: 2, LocalStorageRoot: dataDir}, logger)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { duck.Close() })
	buf := ingest.NewArrowBuffer(&config.IngestConfig{
		MaxBufferSize: 100000, MaxBufferAgeMS: 600000, Compression: "snappy",
		FlushWorkers: 1, FlushQueueSize: 4, ShardCount: 1, DataPageVersion: "2.0",
	}, backend, logger)
	t.Cleanup(func() { buf.Close() })

	ctx := context.Background()
	hour := time.Now().UTC().Truncate(time.Hour).Add(-2 * time.Hour)
	us := hour.UnixMicro()
	if err := buf.WriteColumnarDirectNoWAL(ctx, "db1", "cpu", map[string][]interface{}{
		"time":  {us, us + 1, us + 2, us + 3},
		"host":  {"a", "a", "b", "b"},
		"usage": {1.0, 3.0, 10.0, 20.0},
	}); err != nil {
		t.Fatal(err)
	}
	if err := buf.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}

	h := newSQLiteOnlyCQHandler(t, filepath.Join(root, "cq.db"))
	t.Cleanup(func() { h.sqliteDB.Close() })
	h.db, h.storage, h.arrowBuffer = duck, backend, buf

	watermark := hour.Add(-time.Hour).Format(time.RFC3339)
	const window = " WHERE time >= {start_time} AND time < {end_time} GROUP BY 1, 2"
	bodies := []struct {
		name, query string
		want        int64
	}{
		{"literal", "SELECT time_bucket(INTERVAL '1 hour', time) AS time, host, avg(usage) AS usage, 'FROM db1.cpu' AS note FROM db1.cpu" + window + ", 4", 2},
		{"comment", "SELECT time_bucket(INTERVAL '1 hour', time) AS time, host, avg(usage) AS usage /* FROM db1.cpu */ FROM db1.cpu" + window, 2},
		{"quote against FROM", "SELECT time_bucket(INTERVAL '1 hour', time) AS time, host, avg(usage) AS \"usage\"FROM db1.cpu" + window, 2},
		{"cte named after the source", "WITH cpu AS (SELECT * FROM db1.cpu WHERE time >= {start_time} AND time < {end_time}) " +
			"SELECT time_bucket(INTERVAL '1 hour', time) AS time, host, avg(usage) AS usage FROM cpu GROUP BY 1, 2", 2},
	}
	for i, b := range bodies {
		if _, err := h.sqliteDB.Exec(`INSERT INTO continuous_queries
			(id, name, database, source_measurement, destination_measurement, query, interval, tag_columns, is_active, last_processed_time)
			VALUES (?, ?, 'db1', 'cpu', ?, ?, '1h', '["host"]', 1, ?)`, i+1, b.name, "out_"+strings.ReplaceAll(b.name, " ", "_"), b.query, watermark); err != nil {
			t.Fatal(err)
		}
	}
	for i, b := range bodies {
		resp, err := h.ExecuteCQ(ctx, int64(i+1))
		if err != nil {
			t.Fatalf("%s: run failed: %v", b.name, err)
		}
		if resp.RecordsWritten != b.want {
			t.Errorf("%s: wrote %d rows, want %d", b.name, resp.RecordsWritten, b.want)
		}
	}
}
