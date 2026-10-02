package api

import (
	"context"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/database"
	"github.com/basekick-labs/arc/internal/pruning"
	"github.com/rs/zerolog"
)

// The RBAC invariant these tests defend: the set of measurements
// checkQueryPermissions extracts must never be SMALLER than the set the
// executed query reads. The extractor and the rewriters normalise the SQL
// independently, so every place their normalisation can disagree is a place
// the check set can silently shrink below the read set.
//
// extractTableReferences detects CTEs with patternCTENames (`\bWITH\s+`, where
// \s matches \n, \t and \r), while three rewriter gates used to test
// `strings.Contains(sqlLower, "with ")` — a literal space. `WITH\nt AS (...)`
// was therefore a CTE to the permission check and a real measurement to the
// rewriter: zero refs checked, <headerDB>.t read. Same keyword-as-word lesson
// as #978, which fixed it for JOIN and introduced containsSQLWord.

func newParityTestHandler() *QueryHandler {
	return &QueryHandler{
		storage:    &mockLocalBackend{basePath: "./data"},
		pruner:     pruning.NewPartitionPruner(zerolog.Nop()),
		queryCache: database.NewQueryCache(time.Minute, 128),
		logger:     zerolog.Nop(),
	}
}

var parityPathPattern = regexp.MustCompile(`read_parquet\('\./data/([^/']+)/([^/']+)/\*\*/\*\.parquet'`)

// parityReads returns the sorted, deduplicated database/measurement pairs the
// rewritten SQL actually reads.
// parityReads recovers the read set by matching the local-backend glob the
// test handler emits. It is deliberately strict: if the rewritten SQL contains
// a read_parquet( the pattern did NOT match — the array form, a cold-tier
// UNION ALL, an anchor path — the caller must know, because silently
// under-reporting the read set would make assertNoUnderCheck vacuous and let a
// real bypass pass the suite.
func parityReads(rewritten string) []string {
	if n := strings.Count(rewritten, "read_parquet("); n != len(parityPathPattern.FindAllStringSubmatch(rewritten, -1)) {
		panic("parityReads: unrecognised read_parquet form in rewritten SQL; widen parityPathPattern: " + rewritten)
	}
	seen := map[string]bool{}
	out := []string{}
	for _, m := range parityPathPattern.FindAllStringSubmatch(rewritten, -1) {
		key := m[1] + "/" + m[2]
		if !seen[key] {
			seen[key] = true
			out = append(out, key)
		}
	}
	sort.Strings(out)
	return out
}

// parityChecks returns the sorted database/measurement pairs
// checkQueryPermissions would check, including the x-arc-database override it
// applies at query.go:1604 (only refs left at the "default" database).
func parityChecks(sql, headerDB string) []string {
	normalised, identNames := normaliseForExtraction(sql)
	out := []string{}
	for _, ref := range extractTableReferences(normalised, identNames) {
		db := ref.Database
		if headerDB != "" && db == "default" {
			db = headerDB
		}
		out = append(out, db+"/"+ref.Measurement)
	}
	sort.Strings(out)
	return out
}

func assertNoUnderCheck(t *testing.T, sql string, check, read []string) {
	t.Helper()
	inCheck := map[string]bool{}
	for _, c := range check {
		inCheck[c] = true
	}
	for _, r := range read {
		if !inCheck[r] {
			t.Errorf("UNDER-CHECK: query reads a measurement the permission check never saw\n  sql:   %q\n  check: %v\n  read:  %v\n  unchecked read: %s",
				sql, check, read, r)
		}
	}
}

// TestRBACParity_CTEKeywordAsWord covers every whitespace form that can follow
// WITH. Pre-fix, the \n, \t and \r\n rows read <headerDB>.t with an empty check
// set — an authorization bypass needing no grant at all.
func TestRBACParity_CTEKeywordAsWord(t *testing.T) {
	h := newParityTestHandler()
	ctx := context.Background()
	const headerDB = "sensitive"

	tests := []struct {
		name      string
		sql       string
		wantCheck []string
		wantRead  []string
	}{
		{
			name:      "space after WITH (the only form that ever worked)",
			sql:       "WITH t AS (SELECT 1) SELECT * FROM t",
			wantCheck: []string{},
			wantRead:  []string{},
		},
		{
			name:      "newline after WITH",
			sql:       "WITH\nt AS (SELECT 1) SELECT * FROM t",
			wantCheck: []string{},
			wantRead:  []string{},
		},
		{
			name:      "tab after WITH",
			sql:       "WITH\tt AS (SELECT 1) SELECT * FROM t",
			wantCheck: []string{},
			wantRead:  []string{},
		},
		{
			name:      "CRLF after WITH",
			sql:       "WITH\r\nt AS (SELECT 1) SELECT * FROM t",
			wantCheck: []string{},
			wantRead:  []string{},
		},
		{
			name:      "multiple spaces after WITH",
			sql:       "WITH   t AS (SELECT 1) SELECT * FROM t",
			wantCheck: []string{},
			wantRead:  []string{},
		},
		{
			name:      "newline after WITH RECURSIVE",
			sql:       "WITH\nRECURSIVE t AS (SELECT 1) SELECT * FROM t",
			wantCheck: []string{},
			wantRead:  []string{},
		},
		{
			name:      "newline-WITH, CTE body reads a real measurement",
			sql:       "WITH\nt AS (SELECT * FROM a) SELECT * FROM t",
			wantCheck: []string{"sensitive/a"},
			wantRead:  []string{"sensitive/a"},
		},
		{
			name:      "newline-WITH plus a comma-continued real measurement",
			sql:       "WITH\nt AS (SELECT 1) SELECT * FROM t, cpu",
			wantCheck: []string{"sensitive/cpu"},
			wantRead:  []string{"sensitive/cpu"},
		},
		{
			name:      "newline-WITH plus a JOINed real measurement",
			sql:       "WITH\nt AS (SELECT 1) SELECT * FROM cpu JOIN t ON cpu.id = t.id",
			wantCheck: []string{"sensitive/cpu"},
			wantRead:  []string{"sensitive/cpu"},
		},
		{
			name:      "a measurement actually named with_history is not a CTE",
			sql:       "SELECT * FROM with_history",
			wantCheck: []string{"sensitive/with_history"},
			wantRead:  []string{"sensitive/with_history"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			check := parityChecks(tt.sql, headerDB)
			if strings.Join(check, ",") != strings.Join(tt.wantCheck, ",") {
				t.Errorf("check set = %v, want %v", check, tt.wantCheck)
			}

			// Rewriter 1: the header-DB path, which carries the fast-path gate
			// and the cteNames gate.
			readHeader := parityReads(h.convertSQLToStoragePathsWithHeaderDB(ctx, tt.sql, headerDB))
			if strings.Join(readHeader, ",") != strings.Join(tt.wantRead, ",") {
				t.Errorf("convertSQLToStoragePathsWithHeaderDB read set = %v, want %v", readHeader, tt.wantRead)
			}
			assertNoUnderCheck(t, tt.sql, check, readHeader)

			// Rewriter 2: the parallel-transform entry point, whose own gate
			// decides whether the fast path (no CTE guard at all) is reached.
			parallel, _, _, err := h.getTransformedSQLForParallel(ctx, tt.sql, headerDB)
			if err != nil {
				t.Fatalf("getTransformedSQLForParallel: %v", err)
			}
			readParallel := parityReads(parallel)
			if strings.Join(readParallel, ",") != strings.Join(tt.wantRead, ",") {
				t.Errorf("getTransformedSQLForParallel read set = %v, want %v\n  rewritten: %s", readParallel, tt.wantRead, parallel)
			}
			assertNoUnderCheck(t, tt.sql, check, readParallel)

			// Rewriter 3: the no-header path. Bare refs stay at "default".
			checkNoHeader := parityChecks(tt.sql, "")
			readNoHeader := parityReads(h.convertSQLToStoragePaths(ctx, tt.sql))
			assertNoUnderCheck(t, tt.sql, checkNoHeader, readNoHeader)
		})
	}
}

// TestRBACParity_CTERegexWithoutWithKeyword covers patternCTENames' SECOND
// alternative, `, name AS (`, which has no WITH anchor. A multi-definition
// WINDOW clause matches it, so the extractor treated the FROM table as a
// virtual CTE and emitted zero refs — allowed outright at query.go:1593 —
// while every rewriter, gated on a WITH keyword, rewrote the name. Pre-fix
// these rows read <headerDB>/secret with no grant whatsoever.
//
// The predicate now lives inside extractCTENames, so the extractor and the
// rewriters cannot disagree about whether to consult it.
func TestRBACParity_CTERegexWithoutWithKeyword(t *testing.T) {
	h := newParityTestHandler()
	ctx := context.Background()
	const headerDB = "sensitive"

	tests := []struct {
		name      string
		sql       string
		wantCheck []string
		wantRead  []string
	}{
		{
			name:      "WINDOW clause, no WITH anywhere",
			sql:       "SELECT * FROM secret WINDOW w AS (), secret AS ()",
			wantCheck: []string{"sensitive/secret"},
			wantRead:  []string{"sensitive/secret"},
		},
		{
			name:      "WINDOW clause shadowing a comma-continued table",
			sql:       "SELECT * FROM cpu a, secret b WINDOW w AS (), secret AS ()",
			wantCheck: []string{"sensitive/cpu", "sensitive/secret"},
			wantRead:  []string{"sensitive/cpu", "sensitive/secret"},
		},
		{
			name:      "WINDOW clause shadowing a JOINed table",
			sql:       "SELECT * FROM cpu a JOIN secret b ON a.id = b.id WINDOW w AS (), secret AS ()",
			wantCheck: []string{"sensitive/cpu", "sensitive/secret"},
			wantRead:  []string{"sensitive/cpu", "sensitive/secret"},
		},
		{
			name:      "single-definition WINDOW has no comma, so never matched",
			sql:       "SELECT * FROM secret WINDOW w AS ()",
			wantCheck: []string{"sensitive/secret"},
			wantRead:  []string{"sensitive/secret"},
		},
		{
			name:      "a real multi-CTE WITH list still suppresses both names",
			sql:       "WITH a AS (SELECT 1), b AS (SELECT 2) SELECT * FROM a JOIN b ON 1=1",
			wantCheck: []string{},
			wantRead:  []string{},
		},
		{
			name:      "a real WITH list plus a genuine measurement",
			sql:       "WITH a AS (SELECT 1) SELECT * FROM cpu JOIN a ON 1=1",
			wantCheck: []string{"sensitive/cpu"},
			wantRead:  []string{"sensitive/cpu"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			check := parityChecks(tt.sql, headerDB)
			if strings.Join(check, ",") != strings.Join(tt.wantCheck, ",") {
				t.Errorf("check set = %v, want %v", check, tt.wantCheck)
			}
			readHeader := parityReads(h.convertSQLToStoragePathsWithHeaderDB(ctx, tt.sql, headerDB))
			if strings.Join(readHeader, ",") != strings.Join(tt.wantRead, ",") {
				t.Errorf("convertSQLToStoragePathsWithHeaderDB read set = %v, want %v", readHeader, tt.wantRead)
			}
			assertNoUnderCheck(t, tt.sql, check, readHeader)

			parallel, _, _, err := h.getTransformedSQLForParallel(ctx, tt.sql, headerDB)
			if err != nil {
				t.Fatalf("getTransformedSQLForParallel: %v", err)
			}
			readParallel := parityReads(parallel)
			if strings.Join(readParallel, ",") != strings.Join(tt.wantRead, ",") {
				t.Errorf("getTransformedSQLForParallel read set = %v, want %v\n  rewritten: %s", readParallel, tt.wantRead, parallel)
			}
			assertNoUnderCheck(t, tt.sql, check, readParallel)
		})
	}
}

// TestRBACParity_FromKeywordWordBoundary covers the fast paths' FROM locator,
// which searched for the literal "from " with no LEADING word boundary while
// the extractor's patternSimpleTable requires `\bFROM`. A digit is an
// identifier byte, so `\b` declined where strings.Index matched — and DuckDB
// lexes `1from` as `1` then `FROM`. `SELECT *,1from secret` was invisible to
// the permission check and read by the fast path with no grant.
//
// Per CLAUDE.md, the fix's OWN edge cases are tested here, not just the shape
// that prompted it: a name ending in the keyword, a name starting with it, and
// the keyword at the very start of the string.
func TestRBACParity_FromKeywordWordBoundary(t *testing.T) {
	h := newParityTestHandler()
	ctx := context.Background()
	const headerDB = "sensitive"

	tests := []struct {
		name      string
		sql       string
		wantCheck []string
		wantRead  []string
	}{
		{
			name:      "integer literal glued to FROM",
			sql:       "SELECT *,1from secret",
			wantCheck: []string{},
			wantRead:  []string{},
		},
		{
			name:      "float literal glued to FROM",
			sql:       "SELECT *,1.0from secret ORDER BY 1",
			wantCheck: []string{},
			wantRead:  []string{},
		},
		{
			name:      "exponent literal glued to FROM",
			sql:       "SELECT 1e0from secret",
			wantCheck: []string{},
			wantRead:  []string{},
		},
		{
			name:      "identifier ending in from",
			sql:       "SELECT xfrom secret",
			wantCheck: []string{},
			wantRead:  []string{},
		},
		{
			name:      "ordinary FROM still resolves",
			sql:       "SELECT * FROM cpu",
			wantCheck: []string{"sensitive/cpu"},
			wantRead:  []string{"sensitive/cpu"},
		},
		{
			name:      "FROM-first form, keyword at index 0",
			sql:       "FROM cpu SELECT a",
			wantCheck: []string{"sensitive/cpu"},
			wantRead:  []string{"sensitive/cpu"},
		},
		{
			name:      "a column named from_host is not the keyword",
			sql:       "SELECT from_host FROM cpu",
			wantCheck: []string{"sensitive/cpu"},
			wantRead:  []string{"sensitive/cpu"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			check := parityChecks(tt.sql, headerDB)
			if strings.Join(check, ",") != strings.Join(tt.wantCheck, ",") {
				t.Errorf("check set = %v, want %v", check, tt.wantCheck)
			}
			readHeader := parityReads(h.convertSQLToStoragePathsWithHeaderDB(ctx, tt.sql, headerDB))
			if strings.Join(readHeader, ",") != strings.Join(tt.wantRead, ",") {
				t.Errorf("convertSQLToStoragePathsWithHeaderDB read set = %v, want %v", readHeader, tt.wantRead)
			}
			assertNoUnderCheck(t, tt.sql, check, readHeader)

			parallel, _, _, err := h.getTransformedSQLForParallel(ctx, tt.sql, headerDB)
			if err != nil {
				t.Fatalf("getTransformedSQLForParallel: %v", err)
			}
			readParallel := parityReads(parallel)
			if strings.Join(readParallel, ",") != strings.Join(tt.wantRead, ",") {
				t.Errorf("getTransformedSQLForParallel read set = %v, want %v\n  rewritten: %s", readParallel, tt.wantRead, parallel)
			}
			assertNoUnderCheck(t, tt.sql, check, readParallel)
		})
	}
}

// TestRBACParity_WhitespaceBeforeCallOrDot covers the other place the two
// normalisations could disagree: the extractor skips a table-valued function
// with isFunctionCallAt, which skips space, \t, \n and \r; the rewriter uses
// isDotOrCallAt, which trimmed only " \t". `FROM generate_series\n(1, 10)` was
// therefore a function to the check and a measurement to the rewriter.
//
// DuckDB rejects the rewritten SQL rather than reading it, so this was a parity
// and correctness defect rather than a bypass — but the two sides must agree,
// and the broken form is ordinary multi-line SQL.
func TestRBACParity_WhitespaceBeforeCallOrDot(t *testing.T) {
	h := newParityTestHandler()
	ctx := context.Background()

	tests := []struct {
		name      string
		sql       string
		wantCheck []string
		wantRead  []string
	}{
		{
			name:      "newline before a table-function paren",
			sql:       "SELECT * FROM generate_series\n(1, 10)",
			wantCheck: []string{},
			wantRead:  []string{},
		},
		{
			name:      "space before a table-function paren",
			sql:       "SELECT * FROM generate_series (1, 10)",
			wantCheck: []string{},
			wantRead:  []string{},
		},
		{
			name:      "CRLF before a table-function paren",
			sql:       "SELECT * FROM generate_series\r\n(1, 10)",
			wantCheck: []string{},
			wantRead:  []string{},
		},
		{
			name:      "newline before a JOINed table-function paren",
			sql:       "SELECT * FROM cpu JOIN gen\n(1) ON 1=1",
			wantCheck: []string{"default/cpu"},
			wantRead:  []string{"default/cpu"},
		},
		{
			name:      "adjacent paren still skipped",
			sql:       "SELECT * FROM generate_series(1, 10)",
			wantCheck: []string{},
			wantRead:  []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			check := parityChecks(tt.sql, "")
			if strings.Join(check, ",") != strings.Join(tt.wantCheck, ",") {
				t.Errorf("check set = %v, want %v", check, tt.wantCheck)
			}
			read := parityReads(h.convertSQLToStoragePaths(ctx, tt.sql))
			if strings.Join(read, ",") != strings.Join(tt.wantRead, ",") {
				t.Errorf("read set = %v, want %v", read, tt.wantRead)
			}
			assertNoUnderCheck(t, tt.sql, check, read)
		})
	}
}

// A whitespace-separated database qualifier (`FROM mydb .cpu`) is the mirror
// image of the cases above: the extractor's dot check looks only at the
// immediately-next byte (query.go:1458), so it emits `mydb` as a measurement,
// while patternDBTable needs an adjacent dot and never treats the pair as
// qualified. The two sides land in different places depending on the rewriter,
// and BOTH are safe — but in different ways, so assert each rather than one
// blanket claim:
//
//   - no header: the regex rewriter skips the token via isDotOrCallAt, so the
//     read set is empty while the check set holds `default/mydb` — an
//     OVER-check, i.e. a false denial.
//   - with a header: the token is rewritten to `<headerDB>/mydb`, which the
//     check set also contains — exact parity.
//
// Neither direction is an under-check, which is the invariant that matters.
// Locked in here so the direction cannot silently flip.
func TestRBACParity_SpacedDotQualifier(t *testing.T) {
	h := newParityTestHandler()
	ctx := context.Background()
	const headerDB = "sensitive"

	for _, sql := range []string{
		"SELECT * FROM mydb .cpu",
		"SELECT * FROM mydb\n.cpu",
		"SELECT * FROM mydb\t.cpu",
	} {
		t.Run(sql, func(t *testing.T) {
			// No header: over-check.
			checkNoHeader := parityChecks(sql, "")
			readNoHeader := parityReads(h.convertSQLToStoragePaths(ctx, sql))
			if strings.Join(checkNoHeader, ",") != "default/mydb" {
				t.Errorf("no-header check set = %v, want [default/mydb]", checkNoHeader)
			}
			if len(readNoHeader) != 0 {
				t.Errorf("no-header read set = %v, want empty (isDotOrCallAt must skip the token)", readNoHeader)
			}
			assertNoUnderCheck(t, sql, checkNoHeader, readNoHeader)

			// With a header: exact parity on both rewriters.
			check := parityChecks(sql, headerDB)
			for name, read := range map[string][]string{
				"WithHeaderDB": parityReads(h.convertSQLToStoragePathsWithHeaderDB(ctx, sql, headerDB)),
			} {
				if strings.Join(read, ",") != strings.Join(check, ",") {
					t.Errorf("%s read set = %v, want %v (parity with the check set)", name, read, check)
				}
				assertNoUnderCheck(t, sql, check, read)
			}
			parallel, _, _, err := h.getTransformedSQLForParallel(ctx, sql, headerDB)
			if err != nil {
				t.Fatalf("getTransformedSQLForParallel: %v", err)
			}
			readParallel := parityReads(parallel)
			if strings.Join(readParallel, ",") != strings.Join(check, ",") {
				t.Errorf("ForParallel read set = %v, want %v (parity with the check set)", readParallel, check)
			}
			assertNoUnderCheck(t, sql, check, readParallel)
		})
	}
}
