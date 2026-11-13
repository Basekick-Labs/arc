package api

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/database"
	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// A name the query also gives another relation is not a name to alias a table
// with (#1450): naming the table too is a duplicate alias, or changes which
// relation a nested qualifier reads.
func TestQualifierNamesSkipsNamesUsedAsAliases_Issue1450(t *testing.T) {
	ident := func(name string) (string, bool) { return name, true }
	for _, tt := range []struct {
		sql  string
		want []string
	}{
		{"SELECT cpu.host FROM cpu", []string{"cpu"}},
		{"SELECT cpu.host FROM db.cpu", []string{"cpu"}},
		{"SELECT cpu.host FROM cpu JOIN (SELECT 'h' host) cpu USING (host)", nil},
		{"SELECT cpu.host FROM cpu JOIN (SELECT 'h' host) AS cpu USING (host)", nil},
		{"SELECT cpu.host FROM cpu JOIN hosts cpu ON true", nil},
		{"SELECT cpu.host FROM cpu JOIN db.hosts cpu ON true", nil},
		// A keyword can follow a dot as a table name; the word after it is still an alias.
		{"SELECT cpu.host FROM cpu JOIN db.group cpu ON true", nil},
		{"SELECT count(*) FROM (VALUES (5)) cpu(v) WHERE EXISTS (SELECT 1 FROM cpu WHERE cpu.v = 1)", nil},
		// Columns named like the table, wherever they sit, are not aliases.
		{"SELECT usage AS cpu, cpu.usage FROM cpu", []string{"cpu"}},
		{"SELECT max(cpu.usage) cpu FROM cpu", []string{"cpu"}},
		{"SELECT DISTINCT ON (host) cpu, cpu.usage FROM cpu", []string{"cpu"}},
		{"SELECT cpu.host, * EXCLUDE cpu FROM cpu", []string{"cpu"}},
		// FROM-first: the select list after FROM is still not a FROM list.
		{"FROM cpu SELECT DISTINCT ON (host) cpu, cpu.usage", []string{"cpu"}},
		{"SELECT cpu.host, * REPLACE (upper(cpu) AS cpu) FROM cpu", []string{"cpu"}},
		{"SELECT cpu.host, trim(BOTH __FROM_MASK_0__ cpu) FROM cpu", []string{"cpu"}},
		{"SELECT cpu.host FROM cpu WHERE host IN (SELECT host FROM mem cpu)", nil},
		{"SELECT cpu.host FROM cpu LEFT JOIN LATERAL (SELECT 1 AS v) cpu ON true", nil},
		{"SELECT cpu.host FROM cpu, (VALUES (1)) cpu(v)", nil},
		// A comma after an earlier join's condition still continues the FROM list.
		{"SELECT cpu.host FROM cpu JOIN mem ON true, (SELECT 'a' host) cpu", nil},
		{"SELECT cpu.host FROM cpu JOIN mem USING (host), (SELECT 'a' host) cpu", nil},
		{"SELECT cpu.host FROM cpu JOIN mem ON mem.host = 'a', LATERAL (SELECT 'z' host) cpu", []string{"mem"}},
		{"SELECT cpu.host FROM cpu JOIN mem ON true, mem cpu", nil},
		{"SELECT cpu.host FROM t.cpu JOIN t.mem ON true AND 1 = 1, t.mem cpu", nil},
		// The database part of a reference is not a qualifier.
		{"SELECT avg(usage) FROM cpu.cpu", nil},
		{"SELECT cpu.usage FROM cpu.cpu", []string{"cpu"}},
		// Positions that are not aliases.
		{"SELECT cpu, cpu.usage FROM cpu", []string{"cpu"}},
		{"SELECT cpu.usage FROM cpu WHERE host IN (SELECT host FROM cpu)", []string{"cpu"}},
		{"SELECT cpu.usage FROM cpu, mem WHERE mem.host = cpu.host", []string{"cpu", "mem"}},
		{"SELECT cpu.usage FROM cpu c", []string{"cpu"}},
	} {
		got := qualifierNames(tt.sql, ident)
		var names []string
		for n := range got {
			names = append(names, n)
		}
		sort.Strings(names)
		if strings.Join(names, ",") != strings.Join(tt.want, ",") {
			t.Errorf("qualifierNames(%q) = %v, want %v", tt.sql, names, tt.want)
		}
	}
}

// /api/v1/query: a table whose name the query gives another relation keeps
// the unaliased form it had before #1447.
func TestTableAliasNotAddedWhenTheNameIsAnotherAlias_Issue1450(t *testing.T) {
	h := newCommaJoinTestHandler()
	for _, sql := range []string{
		"SELECT readings.channel_id FROM readings JOIN (SELECT 'b' AS channel_id) readings USING (channel_id)",
		"SELECT count(*) FROM (VALUES (5)) readings(value) WHERE EXISTS (SELECT 1 FROM readings WHERE readings.value = 100)",
	} {
		if got := h.convertSQLToStoragePaths(context.Background(), sql); strings.Contains(got, aliasReadings) {
			t.Errorf("aliased a table whose name is another relation's alias:\n%s", got)
		}
	}
}

func TestWrapSourceMeasurementNamesTheRead_Issue1450(t *testing.T) {
	const rp = "read_parquet('s3://b/prod/cpu/**/*.parquet', union_by_name=true)"
	const cpu = rp + ` AS "cpu"`
	for _, tt := range []struct {
		name, measurement, query, want string
	}{
		{"issue repro", "cpu",
			"SELECT time_bucket(INTERVAL '1 minute', time) AS time, cpu.host, avg(cpu.usage) AS usage FROM prod.cpu GROUP BY 1, 2",
			"SELECT time_bucket(INTERVAL '1 minute', time) AS time, cpu.host, avg(cpu.usage) AS usage FROM " + cpu + " GROUP BY 1, 2"},
		{"no qualifier: unchanged", "cpu",
			"SELECT avg(usage) FROM prod.cpu WHERE host = 'a'",
			"SELECT avg(usage) FROM " + rp + " WHERE host = 'a'"},
		{"own alias", "cpu",
			"SELECT c.usage, cpu.usage FROM prod.cpu c",
			"SELECT c.usage, cpu.usage FROM " + rp + " c"},
		{"own alias with AS", "cpu",
			"SELECT cpu.usage FROM prod.cpu AS c",
			"SELECT cpu.usage FROM " + rp + " AS c"},
		{"quoted own alias", "cpu",
			`SELECT cpu.usage FROM prod.cpu "c"`,
			`SELECT cpu.usage FROM ` + rp + ` "c"`},
		{"own alias after a comment", "cpu",
			"SELECT cpu.usage FROM prod.cpu /* src */ c",
			"SELECT cpu.usage FROM " + rp + " /* src */ c"},
		{"comment before WHERE", "cpu",
			"SELECT cpu.usage FROM prod.cpu -- src\nWHERE cpu.usage > 0",
			"SELECT cpu.usage FROM " + cpu + " -- src\nWHERE cpu.usage > 0"},
		{"qualifier only in a string literal", "cpu",
			"SELECT 'cpu.usage' AS label, usage FROM prod.cpu",
			"SELECT 'cpu.usage' AS label, usage FROM " + rp},
		{"qualifier only in a comment", "cpu",
			"SELECT usage -- cpu.usage\nFROM prod.cpu",
			"SELECT usage -- cpu.usage\nFROM " + rp},
		{"upper-case qualifier", "cpu",
			"SELECT CPU.usage FROM prod.cpu",
			"SELECT CPU.usage FROM " + cpu},
		{"upper-case stored measurement", "CPU",
			"SELECT cpu.usage FROM prod.CPU",
			"SELECT cpu.usage FROM " + rp + ` AS "CPU"`},
		{"qualified star", "cpu",
			"SELECT cpu.* FROM prod.cpu",
			"SELECT cpu.* FROM " + cpu},
		{"inside a subquery", "cpu",
			"SELECT max(u) FROM (SELECT cpu.usage AS u FROM prod.cpu)",
			"SELECT max(u) FROM (SELECT cpu.usage AS u FROM " + cpu + ")"},
		{"two references, one aliased", "cpu",
			"SELECT cpu.usage FROM prod.cpu WHERE host IN (SELECT c.host FROM prod.cpu c)",
			"SELECT cpu.usage FROM " + cpu + " WHERE host IN (SELECT c.host FROM " + rp + " c)"},
		{"TABLESAMPLE", "cpu",
			"SELECT cpu.usage FROM prod.cpu TABLESAMPLE 10%",
			"SELECT cpu.usage FROM " + cpu + " TABLESAMPLE 10%"},
		{"WITH ORDINALITY stays next to the call", "cpu",
			"SELECT cpu.usage FROM prod.cpu WITH ORDINALITY",
			"SELECT cpu.usage FROM " + rp + " WITH ORDINALITY"},
		{"another relation aliased cpu", "cpu",
			"SELECT cpu.host, count(*) FROM prod.cpu JOIN (SELECT 'h0' host) cpu USING (host) GROUP BY 1",
			"SELECT cpu.host, count(*) FROM " + rp + " JOIN (SELECT 'h0' host) cpu USING (host) GROUP BY 1"},
		{"outer relation aliased cpu", "cpu",
			"SELECT count(*) FROM (SELECT 'h0' host) cpu WHERE EXISTS (SELECT 1 FROM prod.cpu WHERE cpu.host = 'h0')",
			"SELECT count(*) FROM (SELECT 'h0' host) cpu WHERE EXISTS (SELECT 1 FROM " + rp + " WHERE cpu.host = 'h0')"},
		{"aggregate named after the measurement", "cpu",
			"SELECT cpu.host, avg(cpu.usage) AS cpu FROM prod.cpu GROUP BY 1",
			"SELECT cpu.host, avg(cpu.usage) AS cpu FROM " + cpu + " GROUP BY 1"},
		{"FROM inside a function body is not a FROM list", "cpu",
			"SELECT cpu.host, trim(BOTH FROM cpu) FROM prod.cpu",
			"SELECT cpu.host, trim(BOTH FROM cpu) FROM " + cpu},
		{"qualifier after a FROM inside EXTRACT", "cpu",
			"SELECT max(EXTRACT(epoch FROM cpu.time)) AS e, host FROM prod.cpu GROUP BY host",
			"SELECT max(EXTRACT(epoch FROM cpu.time)) AS e, host FROM " + cpu + " GROUP BY host"},
		{"comma after a join condition, another relation aliased cpu", "cpu",
			"SELECT cpu.host FROM prod.cpu JOIN prod.mem ON true, (SELECT 'a' host) cpu",
			"SELECT cpu.host FROM " + rp + " JOIN prod.mem ON true, (SELECT 'a' host) cpu"},
		{"a match in a line comment is left alone (#1487)", "cpu",
			"SELECT cpu.usage FROM prod.cpu c -- was FROM prod.cpu\n",
			"SELECT cpu.usage FROM " + rp + " c -- was FROM prod.cpu\n"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := wrapSourceMeasurement(tt.query, "prod", tt.measurement, rp); got != tt.want {
				t.Errorf("\n got: %s\nwant: %s", got, tt.want)
			}
		})
	}
}

// A database named like its measurement (`FROM cpu.cpu`) is not a qualifier:
// a body that qualifies nothing is rewritten as before.
func TestWrapSourceMeasurementDatabaseNamedLikeTheMeasurement_Issue1450(t *testing.T) {
	const rp = "read_parquet('p', union_by_name=true)"
	if got := wrapSourceMeasurement("SELECT avg(usage) FROM cpu.cpu", "cpu", "cpu", rp); got != "SELECT avg(usage) FROM "+rp {
		t.Errorf("got %s", got)
	}
	if got := wrapSourceMeasurement("SELECT cpu.usage FROM cpu.cpu", "cpu", "cpu", rp); got != "SELECT cpu.usage FROM "+rp+` AS "cpu"` {
		t.Errorf("got %s", got)
	}
}

// The alias goes on the finished expression: after the open-compaction
// exclusion's closing parenthesis, and after the schema-anchor list form.
func TestWrapSourceMeasurementAliasesTheFinishedRead_Issue1450(t *testing.T) {
	for _, expr := range []string{
		"(SELECT * EXCLUDE (__arc_file) FROM read_parquet('p', union_by_name=true, filename='__arc_file') WHERE __arc_file NOT IN ('x'))",
		"read_parquet(['anchor.parquet', 'p/**/*.parquet'], union_by_name=true)",
	} {
		got := wrapSourceMeasurement("SELECT cpu.usage FROM prod.cpu", "prod", "cpu", expr)
		if want := "SELECT cpu.usage FROM " + expr + ` AS "cpu"`; got != want {
			t.Errorf("\n got: %s\nwant: %s", got, want)
		}
	}
}

// A scheduled run reads its source through the rewrite: a body that qualifies
// a column by the measurement's name writes the same rows as the unqualified
// body and the aliased workaround (#1450), and a body that gives another
// relation the measurement's name runs as before.
func TestExecuteCQ_SourceQualifiedByItsName_Issue1450(t *testing.T) {
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
		{"qualified", "SELECT time_bucket(INTERVAL '1 hour', time) AS time, cpu.host, avg(cpu.usage) AS usage FROM db1.cpu" + window, 2},
		{"unqualified", "SELECT time_bucket(INTERVAL '1 hour', time) AS time, host, avg(usage) AS usage FROM db1.cpu" + window, 2},
		{"aliased", "SELECT time_bucket(INTERVAL '1 hour', c.time) AS time, c.host, avg(c.usage) AS usage FROM db1.cpu c" +
			" WHERE c.time >= {start_time} AND c.time < {end_time} GROUP BY 1, 2", 2},
		// The joined relation takes the name, so cpu.host is its 'a'.
		{"another relation named cpu", "SELECT time_bucket(INTERVAL '1 hour', time) AS time, cpu.host, avg(usage) AS usage " +
			"FROM db1.cpu JOIN (SELECT 'a' AS host) cpu USING (host)" + window, 1},
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
