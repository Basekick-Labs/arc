package sql

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// The guard: a read_parquet / parquet_scan call may only be SPELLED in one of
// the files below. Everything else must build its reads through ReadParquet or
// ReadParquetList, which always disable Hive partition inference (#1005).
//
// This is a FILE inventory, not a per-violation allowlist, and that is the
// point. There is no global switch for hive_partitioning, so the flag has to
// travel with every read; a literal-based "does this string also contain
// hive_partitioning=false" check cannot work, because 11 of the 12 sites
// receive the flag from a variable or a later WriteString and would each need
// an exemption — a guard that is half allowlist trains exactly the shallow
// pattern-matching this repo keeps getting burned by. Here a NEW sink fails
// the test by existing, and the only way to pass is to route through the
// helper or to argue for a new entry in this list.
var readParquetAllowedFiles = map[string]string{
	"internal/sql/readparquet.go": "the helper itself — the one place the call is spelled",

	// arcx's SQL recognizer accepts ONLY paths between the parentheses and
	// rejects any option argument (arcx/src/parse.rs, expect_read_parquet_paths,
	// pinned by its own test rejects_union_by_name_option). Adding the flag here
	// would make every arcx query decline — silently in serve mode. arcx reads
	// Parquet via arrow-rs and performs no Hive inference, so it needs no flag;
	// the DuckDB shadow oracle gets it through the normal query path.
	// See TestArcxGeneratedSQLCarriesNoReadParquetOptions in internal/arcxrouter.
	"internal/arcxrouter/router.go": "arcx rejects option arguments and does no Hive inference — must NOT be patched",
}

// readParquetCall matches any spelling DuckDB accepts. Case-insensitive with
// optional whitespace before the paren, because `READ_PARQUET(`,
// `Read_Parquet(` and `read_parquet (` all execute and all infer — a literal
// exact-match check lets every one of them through, which would make this
// guard decorative. The trailing `(` is required so the function names in the
// SQL denylists (internal/api/delete.go, internal/api/query.go) are not hits.
var readParquetCall = regexp.MustCompile(`(?i)(read_parquet|parquet_scan)\s*\(`)

// guardRoots are the Go trees walked. cmd/, pkg/ and scripts/ are included
// because scripts/duckdbseed already opens DuckDB and nothing stops a read
// landing there.
var guardRoots = []string{"internal", "cmd", "pkg", "scripts"}

// findReadParquetLiterals returns one message per offending literal, plus the
// number of files actually parsed. The count is returned so a caller can assert
// the walk did any work at all: WalkDir reports a bad root through the callback
// error, which this swallows along with unreadable files, so a typo'd root
// would otherwise scan nothing and pass.
func findReadParquetLiterals(root string, allowed map[string]string) (violations []string, parsed int, err error) {
	for _, sub := range guardRoots {
		dir := filepath.Join(root, sub)
		if _, statErr := os.Stat(dir); statErr != nil {
			continue // tree absent in this checkout
		}
		walkErr := filepath.WalkDir(dir, func(path string, d fs.DirEntry, walkErr error) error {
			if walkErr != nil || d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			rel, relErr := filepath.Rel(root, path)
			if relErr != nil {
				return relErr
			}
			rel = filepath.ToSlash(rel)
			if _, ok := allowed[rel]; ok {
				return nil
			}
			fset := token.NewFileSet()
			f, parseErr := parser.ParseFile(fset, path, nil, 0)
			if parseErr != nil {
				return parseErr
			}
			parsed++
			ast.Inspect(f, func(n ast.Node) bool {
				lit, ok := n.(*ast.BasicLit)
				if !ok || lit.Kind != token.STRING {
					return true
				}
				// Unquote so an escaped or raw literal is matched on its VALUE,
				// not on its source spelling.
				val, unqErr := strconv.Unquote(lit.Value)
				if unqErr != nil {
					val = lit.Value
				}
				if m := readParquetCall.FindString(val); m != "" {
					violations = append(violations, fmt.Sprintf("%s:%d spells %q",
						rel, fset.Position(lit.Pos()).Line, m))
				}
				return true
			})
			return nil
		})
		if walkErr != nil {
			return violations, parsed, walkErr
		}
	}
	return violations, parsed, nil
}

func TestNoUnscopedReadParquetLiterals(t *testing.T) {
	root := filepath.Join("..", "..") // repo root from internal/sql
	violations, parsed, err := findReadParquetLiterals(root, readParquetAllowedFiles)
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	t.Logf("parsed %d non-test .go files under %v", parsed, guardRoots)
	// Guards against a silently empty walk (a moved package, a bad root).
	// 258 files at the time of writing (260 under the roots, 2 allowlisted);
	// 200 leaves room to delete a package without a spurious failure while
	// still catching a walk that collapses to a handful of files or none.
	const minParsed = 200
	if parsed < minParsed {
		t.Fatalf("only %d files parsed (want >= %d) — the walk is not covering the tree, so a pass here means nothing", parsed, minParsed)
	}
	for _, v := range violations {
		t.Errorf("%s — build the read through sqlutil.ReadParquet/ReadParquetList so Hive inference stays off (#1005), or add the file to readParquetAllowedFiles with a justification", v)
	}
}

// TestGuardCatchesPlantedViolations runs the REAL guard against a synthetic
// tree, so the guard cannot be vacuous. Every spelling here executes in DuckDB
// and infers, so every one must be caught.
func TestGuardCatchesPlantedViolations(t *testing.T) {
	for _, spelling := range []string{
		`"SELECT * FROM read_parquet('/a.parquet')"`,
		`"SELECT * FROM READ_PARQUET('/a.parquet')"`,
		`"SELECT * FROM Read_Parquet('/a.parquet')"`,
		`"SELECT * FROM read_parquet ('/a.parquet')"`,
		`"SELECT * FROM parquet_scan('/a.parquet')"`,
		"`SELECT * FROM read_parquet('/a.parquet')`",
	} {
		t.Run(spelling, func(t *testing.T) {
			root := t.TempDir()
			dir := filepath.Join(root, "internal", "planted")
			if err := os.MkdirAll(dir, 0o700); err != nil {
				t.Fatal(err)
			}
			src := "package planted\n\nvar q = " + spelling + "\n"
			if err := os.WriteFile(filepath.Join(dir, "p.go"), []byte(src), 0o600); err != nil {
				t.Fatal(err)
			}
			violations, parsed, err := findReadParquetLiterals(root, nil)
			if err != nil {
				t.Fatal(err)
			}
			if parsed != 1 {
				t.Fatalf("parsed = %d, want 1", parsed)
			}
			if len(violations) != 1 {
				t.Errorf("the guard did not catch %s — it would let this spelling into the tree", spelling)
			}
		})
	}
}

// TestGuardReportsTheLineNotTheColumn pins the diagnostic, which is the only
// thing a failing run gives the next person.
func TestGuardReportsTheLineNotTheColumn(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "internal", "planted")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	src := "package planted\n\n\n\nvar q = \"read_parquet('/a.parquet')\"\n"
	if err := os.WriteFile(filepath.Join(dir, "p.go"), []byte(src), 0o600); err != nil {
		t.Fatal(err)
	}
	violations, _, err := findReadParquetLiterals(root, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(violations) != 1 || !strings.Contains(violations[0], ":5 ") {
		t.Errorf("want the literal reported on line 5, got %v", violations)
	}
}

func TestReadParquetAlwaysDisablesHiveInference(t *testing.T) {
	for _, tt := range []struct {
		name string
		got  string
		want string
	}{
		{"single path", ReadParquet("'/a.parquet'"), "read_parquet('/a.parquet', hive_partitioning=false)"},
		{"with one option", ReadParquet("'/a.parquet'", "union_by_name=true"),
			"read_parquet('/a.parquet', hive_partitioning=false, union_by_name=true)"},
		{"with two options", ReadParquet("'/a.parquet'", "filename=true", "union_by_name=true"),
			"read_parquet('/a.parquet', hive_partitioning=false, filename=true, union_by_name=true)"},
		{"empty option skipped", ReadParquet("'/a.parquet'", ""),
			"read_parquet('/a.parquet', hive_partitioning=false)"},
		{"list form", ReadParquetList([]string{"'/a.parquet'", "'/b.parquet'"}),
			"read_parquet(['/a.parquet', '/b.parquet'], hive_partitioning=false)"},
		{"list with option", ReadParquetList([]string{"'/a.parquet'"}, "union_by_name=true"),
			"read_parquet(['/a.parquet'], hive_partitioning=false, union_by_name=true)"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if tt.got != tt.want {
				t.Errorf("got  %s\nwant %s", tt.got, tt.want)
			}
		})
	}
}
