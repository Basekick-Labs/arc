package api

import (
	"context"
	"strings"
	"testing"

	"github.com/basekick-labs/arc/internal/pruning"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// Regression tests for #1447: the storage-path rewrite left the relation
// named "read_parquet", so a column qualified by the table's own name
// (`SELECT readings.value FROM readings`) failed with "Referenced table
// readings not found". The rewritten relation now carries its table name when
// the query qualifies by it and gives the reference no alias of its own.

const aliasReadings = ` AS "readings"`

func TestTableAliasRewrite_Issue1447(t *testing.T) {
	h := newCommaJoinTestHandler()
	tail := ", union_by_name=true)"

	tests := []struct {
		name    string
		sql     string
		want    []string
		notWant []string
	}{
		// --- the alias is added ---
		{
			name: "issue repro",
			sql:  "SELECT readings.value FROM readings LIMIT 1",
			want: []string{"FROM " + rp("default", "readings") + tail + aliasReadings + " LIMIT 1"},
		},
		{
			name: "end of statement",
			sql:  "SELECT readings.value FROM readings",
			want: []string{rp("default", "readings") + tail + aliasReadings},
		},
		{
			name: "qualified star is the only qualifier",
			sql:  "SELECT readings.* FROM readings WHERE value > 1",
			want: []string{tail + aliasReadings + " WHERE"},
		},
		{
			name: "qualifier with spaces around the dot",
			sql:  "SELECT readings . value FROM readings",
			want: []string{tail + aliasReadings},
		},
		{
			name: "qualifier in another case",
			sql:  "SELECT READINGS.value FROM readings",
			want: []string{tail + aliasReadings},
		},
		{
			name: "LATERAL subquery qualified by the table name",
			sql: "SELECT c.ch, r.value FROM channels c LEFT JOIN LATERAL (SELECT time, value FROM readings " +
				"WHERE readings.channel_id = c.ch ORDER BY time DESC LIMIT 1) r ON true",
			want:    []string{rp("default", "readings") + tail + aliasReadings + " WHERE readings.channel_id"},
			notWant: []string{rp("default", "channels") + tail + ` AS "channels"`},
		},
		{
			name: "JOIN target",
			sql:  "SELECT cpu.host, readings.value FROM cpu JOIN readings ON cpu.host = readings.host",
			want: []string{"FROM " + rp("default", "cpu") + tail + ` AS "cpu"`, "JOIN " + rp("default", "readings") + tail + aliasReadings + " ON"},
		},
		{
			name: "comma join",
			sql:  "SELECT readings.value FROM cpu c, readings WHERE readings.host = c.host",
			want: []string{", " + rp("default", "readings") + tail + aliasReadings + " WHERE"},
		},
		{
			name: "database-qualified reference takes the table name",
			sql:  "SELECT readings.value FROM plant.readings",
			want: []string{rp("plant", "readings") + tail + aliasReadings},
		},
		{
			name: "database-qualified comma join",
			sql:  "SELECT readings.value FROM cpu c, plant.readings WHERE readings.host = c.host",
			want: []string{", " + rp("plant", "readings") + tail + aliasReadings + " WHERE"},
		},
		{
			name: "database-qualified JOIN",
			sql:  "SELECT readings.value FROM cpu c JOIN plant.readings ON readings.host = c.host",
			want: []string{rp("plant", "readings") + tail + aliasReadings + " ON"},
		},
		{
			name: "followed by a keyword that cannot be an alias",
			sql:  "SELECT readings.value FROM readings\nORDER BY readings.time",
			want: []string{tail + aliasReadings + "\nORDER BY"},
		},
		{
			name: "followed by TABLESAMPLE",
			sql:  "SELECT readings.value FROM readings TABLESAMPLE 10%",
			want: []string{tail + aliasReadings + " TABLESAMPLE"},
		},
		{
			name: "followed by USING SAMPLE",
			sql:  "SELECT readings.value FROM readings USING SAMPLE 10",
			want: []string{tail + aliasReadings + " USING SAMPLE"},
		},
		{
			name: "followed by a closing parenthesis",
			sql:  "SELECT * FROM (SELECT readings.value FROM readings) t",
			want: []string{tail + aliasReadings + ") t"},
		},
		{
			name: "quoted table name",
			sql:  `SELECT "my-table".value FROM "my-table"`,
			want: []string{rp("default", "my-table") + tail + ` AS "my-table"`},
		},

		// --- unchanged ---
		{
			name:    "no qualifier anywhere",
			sql:     "SELECT value FROM readings WHERE value > 1",
			notWant: []string{aliasReadings},
		},
		{
			name:    "own alias",
			sql:     "SELECT r.value, readings.x FROM readings r",
			want:    []string{tail + " r"},
			notWant: []string{aliasReadings},
		},
		{
			name:    "own alias with AS",
			sql:     "SELECT readings.value FROM readings AS r",
			want:    []string{tail + " AS r"},
			notWant: []string{aliasReadings},
		},
		{
			name:    "own quoted alias",
			sql:     `SELECT readings.value FROM readings "r"`,
			notWant: []string{aliasReadings},
		},
		{
			name:    "WITH ORDINALITY must stay next to the call",
			sql:     "SELECT readings.value FROM readings WITH ORDINALITY",
			want:    []string{tail + " WITH ORDINALITY"},
			notWant: []string{aliasReadings},
		},
		{
			name:    "qualifier only inside a string literal",
			sql:     "SELECT value FROM readings WHERE note = 'readings.value'",
			notWant: []string{aliasReadings},
		},
		{
			name:    "qualifier only inside a comment",
			sql:     "SELECT value FROM readings -- readings.value\n",
			notWant: []string{aliasReadings},
		},
		{
			name:    "a number is not a qualifier",
			sql:     "SELECT value * 1.5 FROM readings",
			notWant: []string{aliasReadings},
		},
		{
			name:    "a CTE is not rewritten, so not aliased",
			sql:     "WITH readings AS (SELECT 1 AS value) SELECT readings.value FROM readings",
			notWant: []string{aliasReadings, "read_parquet"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := h.convertSQLToStoragePaths(context.Background(), tt.sql)
			for _, w := range tt.want {
				if !strings.Contains(got, w) {
					t.Errorf("missing %q\n got: %s", w, got)
				}
			}
			for _, nw := range tt.notWant {
				if strings.Contains(got, nw) {
					t.Errorf("unexpected %q\n got: %s", nw, got)
				}
			}
		})
	}
}

// The header-database transform and its single-table fast path alias the
// same way.
func TestTableAliasRewriteWithHeaderDB_Issue1447(t *testing.T) {
	h := newCommaJoinTestHandler()
	tail := ", union_by_name=true)"
	for _, tt := range []struct {
		name, sql string
		alias     bool
	}{
		{"fast path", "SELECT readings.value FROM readings", true},
		{"fast path, no qualifier", "SELECT value FROM readings", false},
		{"fast path, own alias", "SELECT readings.value FROM readings r", false},
		{"slow path (quotes)", "SELECT readings.value FROM readings WHERE host = 'a'", true},
		{"JOIN", "SELECT readings.value FROM cpu c JOIN readings ON readings.host = c.host", true},
		{"comma join", "SELECT readings.value FROM cpu c, readings WHERE readings.host = c.host", true},
		{"LATERAL", "SELECT c.ch FROM channels c LEFT JOIN LATERAL (SELECT value FROM readings WHERE readings.ch = c.ch LIMIT 1) r ON true", true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got := h.convertSQLToStoragePathsWithHeaderDB(context.Background(), tt.sql, "plant")
			if has := strings.Contains(got, rp("plant", "readings")+tail+aliasReadings); has != tt.alias {
				t.Fatalf("alias present = %v, want %v\n got: %s", has, tt.alias, got)
			}
		})
	}
}

// The alias goes on the finished expression, after the open-compaction
// exclusion: that exclusion only recognises an expression ending in the
// read_parquet call's parenthesis, so aliasing first would silently stop it
// and count a compaction's rows twice.
func TestTableAliasAfterOpenCompactionExclusion_Issue1447(t *testing.T) {
	b, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	h := &QueryHandler{storage: b, pruner: pruning.NewPartitionPruner(zerolog.Nop()), logger: zerolog.Nop()}
	h.SetOpenCompactions(&fakeOpenCompactionsPlain{keys: []string{"default/readings/2026/11/03/16/a.parquet"}})
	got := h.convertSQLToStoragePaths(context.Background(), "SELECT readings.value FROM readings")
	if !strings.Contains(got, "WHERE __arc_file NOT IN (") {
		t.Fatalf("open compaction not excluded: %s", got)
	}
	if !strings.HasSuffix(got, "a.parquet'))"+aliasReadings) {
		t.Fatalf("alias not on the finished expression: %s", got)
	}
}

func TestKeepsOwnName_Issue1447(t *testing.T) {
	for _, tt := range []struct {
		after string
		keeps bool
	}{
		{"", false},
		{")", false},
		{", x", false},
		{";", false},
		{" WHERE x", false},
		{" where x", false},
		{" LEFT JOIN y", false},
		{" ASOF JOIN y", false},
		{" POSITIONAL JOIN y", false},
		{" TABLESAMPLE 1%", false},
		{" USING SAMPLE 1", false},
		{" PIVOT (x)", false},
		{" QUALIFY x", false},
		{" WINDOW w AS ()", false},
		{" SELECT x", false}, // FROM-first syntax
		{" r", true},
		{" AS r", true},
		{" as r", true},
		{" __IDENT_0__", true},
		{" time", true},   // column_name keyword: valid as a bare alias
		{" sample", true}, // unreserved keyword: valid as a bare alias
		{" WITH ORDINALITY", true},
		{" with  ordinality", true},
		{" WITH x AS (SELECT 1)", false},
	} {
		sql := "FROM readings" + tt.after
		if got := keepsOwnName(sql, len("FROM readings")); got != tt.keeps {
			t.Errorf("keepsOwnName(%q) = %v, want %v", sql, got, tt.keeps)
		}
	}
}

// A name the identifier resolver rejected never gets an alias.
func TestAliasTableRefRefusesRejectedNames_Issue1447(t *testing.T) {
	q := map[string]bool{"x": true, "x__str_0__": true}
	if got := aliasTableRef("FROM e", "FROM x", 6, "x", false, q); got != "FROM e" {
		t.Errorf("rejected name aliased: %q", got)
	}
	if got := aliasTableRef("FROM e", "FROM x__STR_0__", 15, "x__STR_0__", true, q); got != "FROM e" {
		t.Errorf("placeholder-shaped name aliased: %q", got)
	}
	if got := aliasTableRef("", "FROM x", 6, "x", true, q); got != "" {
		t.Errorf("refused read got an alias: %q", got)
	}
	if got := aliasTableRef("FROM e", `FROM x`, 6, `a"b`, true, map[string]bool{`a"b`: true}); got != `FROM e AS "a""b"` {
		t.Errorf("quote not doubled: %q", got)
	}
}
