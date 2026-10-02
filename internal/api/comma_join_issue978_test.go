package api

import (
	"context"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/basekick-labs/arc/internal/pruning"
	sqlutil "github.com/basekick-labs/arc/internal/sql"
	"github.com/rs/zerolog"
)

// Regression tests for #978: a table that continues the FROM list after a
// cross-join comma (`FROM otel_logs a, otel_logs b`) was left as a bare name
// by the storage-path rewriters, so DuckDB failed the query with "Table with
// name otel_logs does not exist". The same blind spot sat in the RBAC table
// extractor, the cross-database check, and the single-table fast path.

func newCommaJoinTestHandler() *QueryHandler {
	return &QueryHandler{
		storage: &mockLocalBackend{basePath: "./data"},
		pruner:  pruning.NewPartitionPruner(zerolog.Nop()),
		logger:  zerolog.Nop(),
	}
}

// rp builds the read_parquet prefix the mock backend produces for db/table.
func rp(db, table string) string {
	return "read_parquet('./data/" + db + "/" + table + "/**/*.parquet'"
}

func TestCommaJoinRewrite_Issue978(t *testing.T) {
	h := newCommaJoinTestHandler()

	tests := []struct {
		name    string
		sql     string
		want    []string
		notWant []string
	}{
		// --- positive: the comma-continued table is rewritten ---
		{
			name: "issue repro: self cross-join with a string literal",
			sql:  "SELECT count(*) FROM otel_logs a, otel_logs b WHERE a.TraceId = b.TraceId AND a.TraceId <> ''",
			want: []string{
				"FROM " + rp("default", "otel_logs") + ", union_by_name=true) a, " + rp("default", "otel_logs") + ", union_by_name=true) b WHERE a.TraceId = b.TraceId AND a.TraceId <> ''",
			},
			notWant: []string{", otel_logs b"},
		},
		{
			name: "three-way chain",
			sql:  "SELECT * FROM cpu, mem, disk",
			want: []string{"FROM " + rp("default", "cpu"), ", " + rp("default", "mem"), ", " + rp("default", "disk")},
		},
		{
			name: "AS aliases",
			sql:  "SELECT * FROM cpu AS c, mem AS m",
			want: []string{", " + rp("default", "mem") + ", union_by_name=true) AS m"},
		},
		{
			name: "newline and tab around the comma",
			sql:  "SELECT *\nFROM cpu c,\n\tmem m\nWHERE c.host = m.host",
			want: []string{", " + rp("default", "mem") + ", union_by_name=true) m\nWHERE"},
		},
		{
			name: "subquery then comma table",
			sql:  "SELECT * FROM (SELECT * FROM cpu) a, mem b",
			want: []string{"(SELECT * FROM " + rp("default", "cpu"), ") a, " + rp("default", "mem") + ", union_by_name=true) b"},
		},
		{
			name: "comma table after a JOIN ON predicate",
			sql:  "SELECT * FROM a JOIN b ON a.id = b.id, c",
			want: []string{"JOIN " + rp("default", "b"), "ON a.id = b.id, " + rp("default", "c")},
		},
		{
			name: "comma table in the second UNION branch",
			sql:  "SELECT 1 FROM a UNION ALL SELECT 2 FROM u, v",
			want: []string{"FROM " + rp("default", "u"), ", " + rp("default", "v")},
		},
		{
			name: "comma table inside a WHERE subquery",
			sql:  "SELECT * FROM cpu WHERE host IN (SELECT host FROM a, b)",
			want: []string{"FROM " + rp("default", "a"), ", " + rp("default", "b") + ", union_by_name=true))"},
		},
		{
			name:    "comma tables inside a CTE body and after the CTE",
			sql:     "WITH t AS (SELECT * FROM a, b) SELECT * FROM t, c",
			want:    []string{"FROM " + rp("default", "a"), ", " + rp("default", "b"), "FROM t, " + rp("default", "c")},
			notWant: []string{rp("default", "t")},
		},
		{
			name: "quoted identifier after the comma",
			sql:  `SELECT * FROM cpu, "rocket-01" r`,
			want: []string{", " + rp("default", "rocket-01") + ", union_by_name=true) r"},
		},
		{
			name: "database-qualified table after the comma",
			sql:  "SELECT * FROM cpu, mydb.mem m",
			want: []string{", " + rp("mydb", "mem") + ", union_by_name=true) m"},
		},
		{
			name: "quoted database-qualified table after the comma",
			sql:  `SELECT * FROM cpu, "my-db"."mem"`,
			want: []string{", " + rp("my-db", "mem")},
		},
		{
			name:    "invalid quoted name after the comma resolves to the inert sentinel",
			sql:     `SELECT b.s FROM cpu, "db2/**/*.parquet" b`,
			want:    []string{", read_parquet('./data/default/" + arcInvalidIdentifierSentinel + "/**/*.parquet'"},
			notWant: []string{`"db2/**/*.parquet"`, "__IDENT_"},
		},
		{
			name: "FROM-first form with a comma table before SELECT",
			sql:  "FROM a, b SELECT *",
			want: []string{"FROM " + rp("default", "a"), ", " + rp("default", "b") + ", union_by_name=true) SELECT *"},
		},

		// --- negative: a comma that is not a cross-join must not rewrite ---
		{
			name:    "projection commas",
			sql:     "SELECT a, b FROM t",
			want:    []string{"SELECT a, b FROM " + rp("default", "t")},
			notWant: []string{rp("default", "a"), rp("default", "b")},
		},
		{
			name:    "GROUP BY commas",
			sql:     "SELECT a, count(*) FROM t GROUP BY a, b",
			want:    []string{"GROUP BY a, b"},
			notWant: []string{rp("default", "a"), rp("default", "b")},
		},
		{
			name:    "ORDER BY commas",
			sql:     "SELECT * FROM t ORDER BY a, b",
			want:    []string{"ORDER BY a, b"},
			notWant: []string{rp("default", "a"), rp("default", "b")},
		},
		{
			name:    "IN-list commas",
			sql:     "SELECT * FROM t WHERE x IN (a, b)",
			want:    []string{"IN (a, b)"},
			notWant: []string{rp("default", "a"), rp("default", "b")},
		},
		{
			name:    "function-argument commas",
			sql:     "SELECT coalesce(a, b) FROM t",
			want:    []string{"coalesce(a, b)"},
			notWant: []string{rp("default", "a"), rp("default", "b")},
		},
		{
			name:    "column-alias list on the FROM table",
			sql:     "SELECT * FROM t x(c1, c2)",
			want:    []string{"x(c1, c2)"},
			notWant: []string{rp("default", "c1"), rp("default", "c2")},
		},
		{
			name:    "WINDOW clause commas",
			sql:     "SELECT sum(x) OVER w FROM t WINDOW w AS (ORDER BY y), w2 AS (ORDER BY z)",
			want:    []string{"WINDOW w AS (ORDER BY y), w2 AS (ORDER BY z)"},
			notWant: []string{rp("default", "w2")},
		},
		{
			name:    "FROM-first projection commas",
			sql:     "FROM cpu SELECT a, b",
			want:    []string{"FROM " + rp("default", "cpu"), "SELECT a, b"},
			notWant: []string{rp("default", "a"), rp("default", "b")},
		},
		{
			name:    "LATERAL function after the comma",
			sql:     "SELECT * FROM t, LATERAL unnest(t.arr) u",
			want:    []string{", LATERAL unnest(t.arr) u"},
			notWant: []string{rp("default", "lateral"), rp("default", "unnest")},
		},
		{
			name:    "LATERAL subquery after the comma",
			sql:     "SELECT * FROM t, LATERAL (SELECT 1) l",
			want:    []string{", LATERAL (SELECT 1) l"},
			notWant: []string{rp("default", "lateral")},
		},
		{
			name:    "table function after the comma",
			sql:     "SELECT * FROM t, generate_series(1, 10) g",
			want:    []string{", generate_series(1, 10) g"},
			notWant: []string{rp("default", "generate_series")},
		},
		{
			name:    "spaced table function after the comma",
			sql:     "SELECT * FROM t, generate_series (1, 10) g",
			notWant: []string{rp("default", "generate_series")},
		},
		{
			name:    "subquery after the comma",
			sql:     "SELECT * FROM t, (SELECT 1) s",
			want:    []string{", (SELECT 1) s"},
			notWant: []string{"default/s/"},
		},
		{
			name:    "CTE name after the comma",
			sql:     "WITH x AS (SELECT 1) SELECT * FROM t, x",
			want:    []string{"FROM " + rp("default", "t") + ", union_by_name=true), x"},
			notWant: []string{rp("default", "x")},
		},
		{
			name:    "already-converted read_parquet after the comma",
			sql:     "SELECT * FROM t, read_parquet('/p') r",
			want:    []string{", read_parquet('/p') r"},
			notWant: []string{rp("default", "read_parquet")},
		},
		{
			name:    "dotted name with whitespace around the dot is left alone",
			sql:     "SELECT * FROM cpu, mydb . mem",
			want:    []string{", mydb . mem"},
			notWant: []string{rp("mydb", "mem"), rp("default", "mydb")},
		},
		{
			name:    "schema-qualified function call after the comma",
			sql:     "SELECT * FROM cpu, mydb.fn(1) f",
			want:    []string{", mydb.fn(1) f"},
			notWant: []string{rp("mydb", "fn")},
		},
		{
			name:    "comma inside a string literal",
			sql:     "SELECT * FROM cpu WHERE msg = 'FROM a, b'",
			want:    []string{"WHERE msg = 'FROM a, b'"},
			notWant: []string{rp("default", "a"), rp("default", "b")},
		},
		{
			name:    "comma inside a comment",
			sql:     "SELECT * FROM cpu -- , secret\n WHERE 1=1",
			notWant: []string{rp("default", "secret")},
		},
		// Review findings: list/struct literals carry commas at paren depth 0
		// while an ON predicate keeps the FROM clause armed.
		{
			name:    "list literal in ON predicate",
			sql:     "SELECT * FROM t JOIN u ON u.ids = [1, 2] AND u.x = 1",
			want:    []string{"ON u.ids = [1, 2] AND u.x = 1"},
			notWant: []string{rp("default", "AND")},
		},
		{
			name:    "list of dotted refs in ON predicate",
			sql:     "SELECT * FROM t JOIN u ON u.tags = [u.a, u.b] WHERE 1=1",
			want:    []string{"ON u.tags = [u.a, u.b] WHERE 1=1"},
			notWant: []string{rp("u", "b")},
		},
		{
			name:    "list cast in ON predicate",
			sql:     "SELECT * FROM t JOIN u ON u.ids = [1, 2]::INT[] AND u.x = 1",
			want:    []string{"[1, 2]::INT[] AND u.x = 1"},
			notWant: []string{rp("default", "INT")},
		},
		{
			name:    "ARRAY constructor in ON predicate",
			sql:     "SELECT * FROM t JOIN u ON u.x = 1 AND t.v = ARRAY[1, 2] OR t.w = 0",
			want:    []string{"ARRAY[1, 2] OR t.w = 0"},
			notWant: []string{rp("default", "OR")},
		},
		{
			name:    "struct literal in ON predicate",
			sql:     "SELECT * FROM t JOIN u ON u.m = {'k': 'v', 'k2': 'v2'} AND u.x = 1",
			want:    []string{"{'k': 'v', 'k2': 'v2'} AND u.x = 1"},
			notWant: []string{rp("default", "AND")},
		},
		{
			name:    "list literal in a projection before a comma join",
			sql:     "SELECT [a, b], c FROM t, u",
			want:    []string{"SELECT [a, b], c FROM " + rp("default", "t"), ", " + rp("default", "u")},
			notWant: []string{rp("default", "b"), rp("default", "c")},
		},
		// Review finding: bytes the tokeniser skips must not be swallowed
		// into a rewrite; the FROM pass leaves these alone, so does this one.
		{
			name:    "backtick-quoted name after the comma is left alone",
			sql:     "SELECT * FROM cpu, `rocket-01` r",
			want:    []string{", `rocket-01` r"},
			notWant: []string{rp("default", "rocket")},
		},
		{
			name:    "non-ASCII byte before the name is left alone",
			sql:     "SELECT * FROM cpu, éb",
			want:    []string{", éb"},
			notWant: []string{rp("default", "b")},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := h.convertSQLToStoragePaths(context.Background(), tt.sql)
			for _, want := range tt.want {
				if !strings.Contains(got, want) {
					t.Errorf("missing %q\n  sql: %q\n  got: %s", want, tt.sql, got)
				}
			}
			for _, notWant := range tt.notWant {
				if strings.Contains(got, notWant) {
					t.Errorf("should not contain %q\n  sql: %q\n  got: %s", notWant, tt.sql, got)
				}
			}
		})
	}
}

// The header-database path has its own rewriter AND a no-regex fast path
// (isSingleTableQuery → convertSingleTableQuery) that a quote-free comma join
// used to take: one FROM, no JOIN keyword, so only the first table was
// rewritten. The issue's repro carried a string literal and so never exposed
// the fast path.
func TestCommaJoinRewriteWithHeaderDB_Issue978(t *testing.T) {
	h := newCommaJoinTestHandler()

	tests := []struct {
		name    string
		sql     string
		want    []string
		notWant []string
	}{
		{
			name: "quote-free self cross-join (fast-path shape)",
			sql:  "SELECT count(*) FROM otel_logs a, otel_logs b WHERE a.TraceId = b.TraceId",
			want: []string{
				"FROM " + rp("searchbench", "otel_logs") + ", union_by_name=true) a, " + rp("searchbench", "otel_logs") + ", union_by_name=true) b WHERE",
			},
			notWant: []string{", otel_logs b"},
		},
		{
			name: "issue repro with a string literal (slow-path shape)",
			sql:  "SELECT count(*) FROM otel_logs a, otel_logs b WHERE a.TraceId = b.TraceId AND a.TraceId <> ''",
			want: []string{
				"FROM " + rp("searchbench", "otel_logs") + ", union_by_name=true) a, " + rp("searchbench", "otel_logs") + ", union_by_name=true) b WHERE a.TraceId = b.TraceId AND a.TraceId <> ''",
			},
			notWant: []string{", otel_logs b"},
		},
		{
			name: "three-way chain under the header database",
			sql:  "SELECT * FROM cpu, mem, disk",
			want: []string{"FROM " + rp("searchbench", "cpu"), ", " + rp("searchbench", "mem"), ", " + rp("searchbench", "disk")},
		},
		{
			name: "quoted identifier after the comma",
			sql:  `SELECT * FROM cpu, "rocket-01" r`,
			want: []string{", " + rp("searchbench", "rocket-01")},
		},
		{
			name:    "database-qualified table after the comma is left alone (rejected upstream)",
			sql:     "SELECT * FROM cpu, otherdb.mem",
			want:    []string{"FROM " + rp("searchbench", "cpu"), ", otherdb.mem"},
			notWant: []string{"./data/otherdb/"},
		},
		{
			name:    "projection and GROUP BY commas",
			sql:     "SELECT a, b FROM t GROUP BY a, b",
			want:    []string{"SELECT a, b FROM " + rp("searchbench", "t"), "GROUP BY a, b"},
			notWant: []string{rp("searchbench", "a"), rp("searchbench", "b")},
		},
		{
			name:    "column-alias list on the FROM table",
			sql:     "SELECT * FROM t x(c1, c2)",
			notWant: []string{rp("searchbench", "c1"), rp("searchbench", "c2")},
		},
		{
			name:    "CTE name after the comma",
			sql:     "WITH x AS (SELECT 1) SELECT * FROM t, x",
			notWant: []string{rp("searchbench", "x")},
		},
		// Two more shapes the same fast path mishandled (found by probing the
		// gate while fixing #978): a JOIN that starts a new line, and a table
		// function in FROM position.
		{
			name:    "quote-free multi-line JOIN",
			sql:     "SELECT *\nFROM a x\nJOIN b y ON x.id = y.id",
			want:    []string{"FROM " + rp("searchbench", "a"), "JOIN " + rp("searchbench", "b") + ", union_by_name=true) y ON"},
			notWant: []string{"JOIN b y"},
		},
		{
			name:    "quote-free table function in FROM position",
			sql:     "SELECT * FROM generate_series(1, 10)",
			want:    []string{"SELECT * FROM generate_series(1, 10)"},
			notWant: []string{"read_parquet"},
		},
		{
			name:    "quote-free sample clause then comma table",
			sql:     "SELECT * FROM cpu c TABLESAMPLE bernoulli(10), mem m",
			want:    []string{"FROM " + rp("searchbench", "cpu") + ", union_by_name=true) c TABLESAMPLE bernoulli(10), " + rp("searchbench", "mem")},
			notWant: []string{", mem m"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := h.convertSQLToStoragePathsWithHeaderDB(context.Background(), tt.sql, "searchbench")
			for _, want := range tt.want {
				if !strings.Contains(got, want) {
					t.Errorf("missing %q\n  sql: %q\n  got: %s", want, tt.sql, got)
				}
			}
			for _, notWant := range tt.notWant {
				if strings.Contains(got, notWant) {
					t.Errorf("should not contain %q\n  sql: %q\n  got: %s", notWant, tt.sql, got)
				}
			}
		})
	}
}

func TestIsSingleTableQuery_CommaJoin_Issue978(t *testing.T) {
	single := []string{
		"select * from cpu",
		"select * from cpu where (a = 1)",
		"select * from cpu c where x in (1, 2)",
		"select * from cpu as c order by a, b",
		"select a, b from cpu",
		"select * from cpu limit 10",
		"select * from cpu\nwhere host = 1",
		"select join_count from cpu",
		"select * from joined_t",
	}
	for _, sql := range single {
		if !isSingleTableQuery(sql) {
			t.Errorf("isSingleTableQuery(%q) = false, want true", sql)
		}
	}

	multi := []string{
		"select * from cpu, mem",
		"select * from cpu c, mem m",
		"select * from cpu as c, mem",
		"select * from cpu\n, mem",
		"select * from cpu c(a, b)",
		"select * from generate_series(1, 10)",
		"select * from a\njoin b on a.x = b.x",
		"select * from a x\n  left join b y on x.id = y.id",
		"select * from a\tjoin b using (x)",
		"select * from cpu c tablesample bernoulli(10), mem",
		"select * from cpu tablesample bernoulli(10), mem m",
		"select * from cpu as c using sample 10%, mem",
	}
	for _, sql := range multi {
		if isSingleTableQuery(sql) {
			t.Errorf("isSingleTableQuery(%q) = true, want false", sql)
		}
	}
}

// normaliseForExtraction applies the same normalisation checkQueryPermissions
// applies before extractTableReferences.
func normaliseForExtraction(sql string) (string, map[string]string) {
	features := scanSQLFeatures(sql)
	normalised, masks := sqlutil.MaskStringLiterals(sql, features.hasQuotes)
	normalised, _ = sqlutil.MaskFromKeywordsInFunctionBodies(normalised)
	normalised = stripSQLComments(normalised, features.hasDashComment || features.hasBlockComment)
	return normalised, sqlutil.IdentifierNames(masks)
}

func refKeys(refs []TableReference) []string {
	keys := make([]string, 0, len(refs))
	for _, r := range refs {
		keys = append(keys, r.Database+"/"+r.Measurement)
	}
	sort.Strings(keys)
	return keys
}

func TestExtractTableReferences_CommaJoin_Issue978(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want []string
	}{
		{"bare comma table", "SELECT * FROM cpu, mem", []string{"default/cpu", "default/mem"}},
		{"aliases", "SELECT * FROM cpu c, mem m, disk AS d", []string{"default/cpu", "default/disk", "default/mem"}},
		{"qualified comma table", "SELECT * FROM cpu a, mydb.mem b", []string{"default/cpu", "mydb/mem"}},
		{"quoted comma table", `SELECT * FROM cpu, "rocket-01" r`, []string{"default/cpu", "default/rocket-01"}},
		{"quoted qualified comma table", `SELECT * FROM cpu, "my-db"."mem"`, []string{"default/cpu", "my-db/mem"}},
		{"subquery then comma", "SELECT * FROM (SELECT * FROM cpu) a, mem b", []string{"default/cpu", "default/mem"}},
		{"JOIN ON then comma", "SELECT * FROM a JOIN b ON a.id = b.id, c", []string{"default/a", "default/b", "default/c"}},
		{"CTE after the comma is virtual", "WITH t AS (SELECT * FROM a) SELECT * FROM t, b", []string{"default/a", "default/b"}},
		{"table function after the comma", "SELECT * FROM cpu, generate_series(1, 10) g", []string{"default/cpu"}},
		{"LATERAL after the comma", "SELECT * FROM cpu, LATERAL unnest(cpu.arr) u", []string{"default/cpu"}},
		{"projection and GROUP BY commas", "SELECT a, b FROM cpu GROUP BY a, b", []string{"default/cpu"}},
		{"FROM-first projection commas", "FROM cpu SELECT a, b", []string{"default/cpu"}},
		{"comma inside a literal", "SELECT * FROM cpu WHERE msg = 'FROM a, secret'", []string{"default/cpu"}},
		{"comma inside a comment", "SELECT * FROM cpu /* , secret */ WHERE 1=1", []string{"default/cpu"}},
		{"IN-list after a comma join", "SELECT * FROM cpu, mem WHERE x IN (1, 2)", []string{"default/cpu", "default/mem"}},

		// #827: the dedup key must not fold case. Object keys are
		// case-sensitive on S3, so cpu and CPU are two measurements with two
		// separate grants — folding them checks one grant and reads both.
		// #832 fixed this for FROM and JOIN; the comma-continued path did not
		// exist yet. The bare-ref key is query.go:1547, the qualified one
		// query.go:1532. refKeys sorts, and ASCII puts 'C' (67) before 'c'
		// (99), 'M' (77) before 'd' (100) before 'm' (109).
		{"case-distinct comma tables", "SELECT * FROM cpu, CPU", []string{"default/CPU", "default/cpu"}},
		{"case-distinct comma tables with aliases", "SELECT * FROM cpu c, CPU u WHERE c.x = u.x", []string{"default/CPU", "default/cpu"}},
		{"case-distinct comma databases", "SELECT * FROM cpu, MYDB.cpu, mydb.cpu", []string{"MYDB/cpu", "default/cpu", "mydb/cpu"}},
		{"four spellings of one name", "SELECT * FROM cpu, CPU, cPu, CpU", []string{"default/CPU", "default/CpU", "default/cPu", "default/cpu"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			normalised, identNames := normaliseForExtraction(tt.sql)
			got := refKeys(extractTableReferences(normalised, identNames))
			if strings.Join(got, ",") != strings.Join(tt.want, ",") {
				t.Errorf("extractTableReferences(%q) = %v, want %v", tt.sql, got, tt.want)
			}
		})
	}
}

// The RBAC invariant: the set of measurements the permission check extracts
// must equal the set the executed query reads. For every fixture, compare the
// extractor's output with the read_parquet paths the rewriter emitted.
func TestCommaJoinExtractRewriteParity_Issue978(t *testing.T) {
	h := newCommaJoinTestHandler()
	pathPattern := regexp.MustCompile(`read_parquet\('\./data/([^/']+)/([^/']+)/\*\*/\*\.parquet'`)

	fixtures := []string{
		"SELECT count(*) FROM otel_logs a, otel_logs b WHERE a.TraceId = b.TraceId AND a.TraceId <> ''",
		"SELECT * FROM cpu, mem, disk",
		"SELECT * FROM (SELECT * FROM cpu) a, mem b",
		"SELECT * FROM a JOIN b ON a.id = b.id, c",
		"SELECT 1 FROM a UNION ALL SELECT 2 FROM u, v",
		`SELECT * FROM cpu, "rocket-01" r`,
		"SELECT * FROM cpu a, mydb.mem b",
		`SELECT * FROM cpu, "my-db"."mem"`,
		"WITH t AS (SELECT * FROM a, b) SELECT * FROM t, c",
		"SELECT a, b FROM t GROUP BY a, b ORDER BY a, b",
		"FROM cpu SELECT a, b",
		"SELECT * FROM t, LATERAL unnest(t.arr) u",
		"SELECT * FROM t, generate_series(1, 10) g",
		"SELECT * FROM cpu WHERE host IN (SELECT host FROM a, b)",
		"SELECT * FROM cpu WHERE msg = 'FROM a, secret' -- , other\n",

		// #827: case-distinct comma-continued refs must be checked AND read as
		// distinct measurements. This is the stronger of the two guards — it
		// asserts the check set equals the read set directly, which is the
		// invariant #827 is about.
		"SELECT * FROM cpu, CPU",
		"SELECT * FROM cpu c, CPU u WHERE c.x = u.x",
		"SELECT * FROM cpu, MYDB.cpu, mydb.cpu",
		// cteNames is lower-cased on BOTH sides, so CPU here names the CTE for
		// the extractor and for the rewriter alike (DuckDB resolves identifiers
		// case-insensitively, so it really is the CTE). A characterization
		// fixture, not a regression guard: making the REWRITER's CTE check
		// case-sensitive would turn this into an #827-shaped under-check.
		"WITH cpu AS (SELECT 1) SELECT * FROM a, CPU",
	}

	for _, sql := range fixtures {
		t.Run(sql, func(t *testing.T) {
			normalised, identNames := normaliseForExtraction(sql)
			extracted := refKeys(extractTableReferences(normalised, identNames))

			rewritten := h.convertSQLToStoragePaths(context.Background(), sql)
			seen := map[string]bool{}
			var read []string
			for _, m := range pathPattern.FindAllStringSubmatch(rewritten, -1) {
				key := m[1] + "/" + m[2]
				if !seen[key] {
					seen[key] = true
					read = append(read, key)
				}
			}
			sort.Strings(read)

			if strings.Join(extracted, ",") != strings.Join(read, ",") {
				t.Errorf("RBAC extraction and rewrite disagree\n  sql: %q\n  extracted: %v\n  rewritten reads: %v\n  rewritten: %s", sql, extracted, read, rewritten)
			}
		})
	}
}

func TestHasCrossDatabaseSyntax_CommaJoin_Issue978(t *testing.T) {
	tests := []struct {
		sql  string
		want bool
	}{
		{"SELECT * FROM cpu, otherdb.mem", true},
		{"SELECT * FROM cpu c, otherdb.mem m", true},
		{`SELECT * FROM cpu, "other"."mem"`, true},
		{"SELECT * FROM (SELECT * FROM cpu) a, otherdb.mem", true},
		{"SELECT * FROM a JOIN b ON a.id = b.id, otherdb.mem", true},
		{"SELECT * FROM cpu, mem", false},
		{"SELECT a.x, b.y FROM cpu a, mem b", false},
		{"SELECT * FROM cpu c WHERE c.x IN (1, 2)", false},
		{"SELECT * FROM cpu WHERE q = ', otherdb.mem'", false},
		{"SELECT * FROM cpu -- , otherdb.mem\n", false},
		{"SELECT * FROM cpu /* , otherdb.mem */", false},
		{"SELECT a, otherdb.mem FROM cpu", false},
		{"SELECT * FROM cpu GROUP BY a, otherdb.mem", false},
		{"SELECT * FROM cpu, mydb . mem", false},
	}
	for _, tt := range tests {
		t.Run(tt.sql, func(t *testing.T) {
			if got := hasCrossDatabaseSyntax(tt.sql); got != tt.want {
				t.Errorf("hasCrossDatabaseSyntax(%q) = %v, want %v", tt.sql, got, tt.want)
			}
		})
	}
}

// SELECT now terminates a FROM clause's table list (DuckDB's FROM-first form),
// so a projection literal after it is a value, not a replacement scan — while
// a string that really continues the table list is still rejected.
func TestValidateSQLRequest_FromFirstForm_Issue978(t *testing.T) {
	if err := ValidateSQLRequest("FROM cpu SELECT a, '/x'"); err != nil {
		t.Errorf("FROM-first projection literal rejected: %v", err)
	}
	if err := ValidateSQLRequest("FROM cpu, '/data/arc/db2/x.parquet' SELECT 1"); err == nil {
		t.Errorf("replacement scan in FROM-first form was accepted")
	}
}

// A string literal standing as the TABLE part of a qualified name (`FROM
// db.'…'`, in the FROM, JOIN and comma positions) is in table position too:
// validation rejects it, and the transform never lets a masked literal become
// a path segment (the unmask step would otherwise restore the raw literal
// inside the quoted read_parquet path). A valid quoted identifier in that
// position is still fine.
func TestStringLiteralAsQualifiedTablePart_Issue978(t *testing.T) {
	h := newCommaJoinTestHandler()
	literal := "'||concat(chr(46),chr(46),chr(47),chr(100),chr(98),chr(50))||'"
	rejected := []string{
		"SELECT * FROM mydb." + literal,
		"SELECT * FROM cpu JOIN mydb." + literal + " x ON 1=1",
		"SELECT * FROM cpu, mydb." + literal,
		`SELECT * FROM "mydb"."db2/**/*.parquet"`,
		`SELECT * FROM cpu, "mydb"."db2/**/*.parquet"`,
		"SELECT * FROM mydb.$$" + literal[1:len(literal)-1] + "$$",
	}
	for _, sql := range rejected {
		if err := ValidateSQLRequest(sql); err == nil {
			t.Errorf("%q accepted by validation", sql)
		}
		got := h.convertSQLToStoragePaths(context.Background(), sql)
		if strings.Contains(got, "concat(") || strings.Contains(got, "||") || strings.Contains(got, "db2/**") {
			t.Errorf("literal reached the storage path\n  sql: %q\n  got: %s", sql, got)
		}
		if !strings.Contains(got, arcInvalidIdentifierSentinel) {
			t.Errorf("sentinel path not emitted\n  sql: %q\n  got: %s", sql, got)
		}
	}
	for _, sql := range []string{"SELECT * FROM '" + literal[1:len(literal)-1] + "'", "SELECT * FROM cpu, '/x' b"} {
		got := h.convertSQLToStoragePathsWithHeaderDB(context.Background(), sql, "mydb")
		if strings.Contains(got, "concat(") || strings.Contains(got, "read_parquet('./data/mydb//x") {
			t.Errorf("literal reached the storage path (header path)\n  sql: %q\n  got: %s", sql, got)
		}
	}
	accepted := []string{
		`SELECT * FROM mydb."rocket-01"`,
		`SELECT * FROM "my-db"."cpu"`,
		`SELECT * FROM cpu, mydb."rocket-01" r`,
		`SELECT * FROM cpu c JOIN mydb."rocket-01" r ON c.id = r.id`,
	}
	for _, sql := range accepted {
		if err := ValidateSQLRequest(sql); err != nil {
			t.Errorf("%q rejected: %v", sql, err)
		}
	}
	if got := h.convertSQLToStoragePaths(context.Background(), `SELECT * FROM cpu, mydb."rocket-01" r`); !strings.Contains(got, rp("mydb", "rocket-01")) {
		t.Errorf("valid quoted qualified name after the comma not rewritten: %s", got)
	}
}

// A quoted reserved word used as an alias (`FROM cpu "where", '…'`) must not
// disarm the replacement-scan guard: the guard now runs on the normalisation
// where a quoted identifier stays a placeholder instead of coming back as the
// bare keyword that terminates the table list.
func TestValidateSQLRequest_QuotedKeywordAlias_Issue978(t *testing.T) {
	rejected := []string{
		`SELECT * FROM cpu "where", '/data/arc/db2/x.parquet'`,
		`SELECT * FROM cpu "select", '/data/arc/db2/x.parquet'`,
		`SELECT * FROM cpu AS "group", '/data/arc/db2/x.parquet' b`,
		"SELECT * FROM cpu `order`, '/data/arc/db2/x.parquet'",
		`SELECT * FROM cpu "where" JOIN mem "limit" ON 1=1, '/data/arc/db2/x.parquet'`,
	}
	for _, sql := range rejected {
		if err := ValidateSQLRequest(sql); err == nil {
			t.Errorf("%q accepted: quoted keyword alias hid the replacement scan", sql)
		}
	}
	accepted := []string{
		`SELECT * FROM cpu "where" WHERE "where".x = 'a'`,
		`SELECT * FROM cpu "order" ORDER BY a, 'x'`,
		`SELECT "from", 'x' FROM cpu "select" WHERE 1=1`,
		`SELECT * FROM cpu "where", mem "group" WHERE 1=1`,
	}
	for _, sql := range accepted {
		if err := ValidateSQLRequest(sql); err != nil {
			t.Errorf("%q rejected: %v", sql, err)
		}
	}
}

// Brackets now open a nested group in the shared walker, so a string inside a
// list or struct literal in an ON predicate is a value (these were false
// rejections before). A bracket standing in table position itself does not
// hide the string that follows it from the replacement-scan guard.
func TestValidateSQLRequest_BracketLiterals_Issue978(t *testing.T) {
	accepted := []string{
		"SELECT * FROM t JOIN u ON u.tags = ['a', 'b'] WHERE 1=1",
		"SELECT * FROM t JOIN u ON u.m = {'k': 'v', 'k2': 'v2'} AND u.x = 1",
		"SELECT * FROM t JOIN u ON u.ids = [1, 2] AND u.x = 1",
	}
	for _, sql := range accepted {
		if err := ValidateSQLRequest(sql); err != nil {
			t.Errorf("%q rejected: %v", sql, err)
		}
	}
	rejected := []string{
		"SELECT * FROM cpu, ['/data/arc/db2/x.parquet']",
		"SELECT * FROM ['/data/arc/db2/x.parquet']",
		"SELECT * FROM cpu, {'/data/arc/db2/x.parquet'}",
		// Unbalanced brackets outside quotes (syntax errors in DuckDB) must
		// not disarm the guard for a string that does stand in table position.
		"SELECT [1 FROM cpu, '/data/arc/db2/x.parquet'",
		"SELECT * FROM cpu ], '/data/arc/db2/x.parquet'",
		"SELECT * FROM cpu[1], '/data/arc/db2/x.parquet'",
		"SELECT * FROM cpu } JOIN '/data/arc/db2/x.parquet' x ON 1=1",
	}
	for _, sql := range rejected {
		if err := ValidateSQLRequest(sql); err == nil {
			t.Errorf("%q accepted: a string after a bracket in table position must stay flagged", sql)
		}
	}
}
