package compaction

import (
	"strings"
	"testing"
)

// buildCompactionSQL joins the statement(s) buildCompactionQuery returns into a
// single string for the string-assertion unit tests below. The dedup path
// returns two statements (CREATE TEMP TABLE + COPY); the standard path one.
func buildCompactionSQL(fileListSQL, orderByClause, outputFile string, tagColumns []string, dedupTime bool) string {
	return strings.Join(buildCompactionQuery(fileListSQL, orderByClause, outputFile, tagColumns, legacyDedupMode(tagColumns, dedupTime)), "\n;\n")
}

func TestBuildCompactionQuery_NoDedup(t *testing.T) {
	query := buildCompactionSQL("['a.parquet', 'b.parquet']", `ORDER BY "time"`, "/tmp/out.parquet", nil, false)

	if strings.Contains(query, "ROW_NUMBER") {
		t.Error("expected no ROW_NUMBER without dedup keys")
	}
	if !strings.Contains(query, "read_parquet") {
		t.Error("expected read_parquet in query")
	}
	if !strings.Contains(query, `ORDER BY "time"`) {
		t.Error("expected ORDER BY clause")
	}
}

func TestBuildCompactionQueryDisablesHivePartitioning(t *testing.T) {
	query := buildCompactionSQL("['a.parquet', 'b.parquet']", `ORDER BY "time"`, "/tmp/out.parquet", []string{"host"}, false)
	if strings.Count(query, "read_parquet") != strings.Count(query, "hive_partitioning=false") {
		t.Fatalf("every read_parquet call must disable Hive partition inference, got:\n%s", query)
	}
}

func TestBuildCompactionQuery_EmptyDedup(t *testing.T) {
	query := buildCompactionSQL("['a.parquet']", "", "/tmp/out.parquet", []string{}, false)

	if strings.Contains(query, "ROW_NUMBER") {
		t.Error("expected no ROW_NUMBER with empty dedup keys")
	}
}

func TestBuildCompactionQuery_SpecialCharsInPath(t *testing.T) {
	query := buildCompactionSQL("['a.parquet']", "", "/tmp/it's out.parquet", []string{"host"}, false)

	if !strings.Contains(query, "it''s") {
		t.Error("expected escaped single quote in output path")
	}
}

func TestBuildCompactionQuery_IdentifierEscaping(t *testing.T) {
	// Defense-in-depth: even if validation is bypassed, identifiers are properly escaped
	query := buildCompactionSQL("['a.parquet']", "", "/tmp/out.parquet", []string{`host"injection`}, false)

	// Double-quote inside identifier should be doubled
	if !strings.Contains(query, `"host""injection"`) {
		t.Errorf("expected escaped double-quote in identifier, got: %s", query)
	}
}

func TestIsValidIdentifier(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  bool
	}{
		{"simple", "host", true},
		{"with_underscore", "host_name", true},
		{"with_hyphen", "host-name", true},
		{"with_numbers", "sensor123", true},
		{"mixed_case", "HostName", true},
		{"empty", "", false},
		{"with_double_quote", `host"`, false},
		{"with_space", "host name", false},
		{"with_semicolon", "host;DROP", false},
		{"with_paren", "host()", false},
		{"with_comma", "a,b", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isValidIdentifier(tt.input)
			if got != tt.want {
				t.Errorf("isValidIdentifier(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

func TestReadTagColumnsValidation(t *testing.T) {
	// isValidIdentifier is called internally by readTagColumnsFromParquet
	// but we can't easily test that without a DuckDB instance.
	// Instead, test the validation logic directly.

	// Valid tags should pass
	for _, tag := range []string{"host", "region", "sensor_id", "host-name"} {
		if !isValidIdentifier(tag) {
			t.Errorf("expected %q to be valid", tag)
		}
	}

	// Injection attempts should fail
	for _, tag := range []string{`host" OR 1=1--`, `"; DROP TABLE`, `host,region`} {
		if isValidIdentifier(tag) {
			t.Errorf("expected %q to be invalid", tag)
		}
	}
}

// TestBuildCompactionQuery_DedupNonDedupBothNormalizeTime asserts both branches
// (with and without tag columns) normalize time to TIMESTAMPTZ so a VARCHAR-time
// file never wedges compaction. String-only (no DuckDB), so it lives here in the
// untagged test file rather than the duckdb_arrow-gated integration test.
func TestBuildCompactionQuery_DedupNonDedupBothNormalizeTime(t *testing.T) {
	withTags := buildCompactionSQL("['a.parquet']", "", "/tmp/o.parquet", []string{"host"}, false)
	noTags := buildCompactionSQL("['a.parquet']", "", "/tmp/o.parquet", nil, false)
	for name, q := range map[string]string{"dedup": withTags, "non-dedup": noTags} {
		if !strings.Contains(q, "make_timestamptz") || !strings.Contains(q, "TIMESTAMPTZ") {
			t.Errorf("%s branch does not normalize time to TIMESTAMPTZ:\n%s", name, q)
		}
	}
}

// legacyDedupMode maps the (tags, dedupTime) pair older tests pass onto the
// mode decideDedup chooses: either one makes the batch an exact-row dedup.
func legacyDedupMode(tagColumns []string, dedupTime bool) dedupMode {
	if len(tagColumns) > 0 || dedupTime {
		return dedupExact
	}
	return dedupNone
}

// Exact-row dedup (#1232): rows whose (tags, time) key is unique are copied as
// they are; for keys that repeat, one copy of each distinct row is kept. The
// key never decides on its own that rows are duplicates.
func TestBuildCompactionQuery_ExactRows(t *testing.T) {
	stmts := buildCompactionQuery("['a.parquet']", `ORDER BY "time"`, "/tmp/o.parquet", []string{"host", "region"}, dedupExact)
	if len(stmts) != 3 {
		t.Fatalf("got %d statements, want stage + keys + copy", len(stmts))
	}
	q := strings.Join(stmts, "\n;\n")
	for _, want := range []string{
		`GROUP BY ALL HAVING count(*) > 1`,
		`SELECT "host", "region", "time" FROM ` + dedupStagingTable,
		`k."host" IS NOT DISTINCT FROM s."host" AND k."region" IS NOT DISTINCT FROM s."region" AND k."time" IS NOT DISTINCT FROM s."time"`,
		`WHERE NOT EXISTS`,
		`SELECT DISTINCT * FROM ` + dedupStagingTable + ` s WHERE EXISTS`,
		`make_timestamptz`,
	} {
		if !strings.Contains(q, want) {
			t.Errorf("exact-row query lacks %q:\n%s", want, q)
		}
	}
	if strings.Contains(q, "ROW_NUMBER") || strings.Contains(q, "PARTITION BY") {
		t.Errorf("exact-row query still keeps one row per key:\n%s", q)
	}
	if strings.Contains(q, "KV_METADATA") {
		t.Errorf("tagged exact-row output is marked:\n%s", q)
	}
}

// With no tags (continuous-query output) the key is time alone and the output
// stays marked so a later pass checks it again. UNION ALL + DISTINCT keep no
// order, so the output is ordered by time when no sort keys are configured,
// and by the sort keys when they are.
func TestBuildCompactionQuery_ExactRowsTaglessOrderAndMarker(t *testing.T) {
	q := strings.Join(buildCompactionQuery("['a.parquet']", "", "/tmp/o.parquet", nil, dedupExact), "\n;\n")
	if !strings.Contains(q, `SELECT "time" FROM `+dedupStagingTable) || strings.Contains(q, `SELECT , "time"`) {
		t.Errorf("tagless key is not time alone:\n%s", q)
	}
	if !strings.Contains(q, `KV_METADATA {'arc:dedup_exact': 'true'}`) {
		t.Errorf("tagless exact-row output is not marked:\n%s", q)
	}
	if !strings.Contains(q, `ORDER BY "time"`) {
		t.Errorf("no ORDER BY fallback without sort keys:\n%s", q)
	}
	q = strings.Join(buildCompactionQuery("['a.parquet']", `ORDER BY "host", "time"`, "/tmp/o.parquet", nil, dedupExact), "\n;\n")
	if !strings.Contains(q, `ORDER BY "host", "time"`) {
		t.Errorf("configured sort keys ignored:\n%s", q)
	}
}
