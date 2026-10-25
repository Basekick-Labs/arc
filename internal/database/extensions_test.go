package database

import (
	"database/sql"
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/rs/zerolog"
)

// loadTestExtension loads a DuckDB extension from the local extension cache.
// With ARC_TEST_EXTENSIONS=1 (CI) it installs a missing one and fails the
// test when it cannot; otherwise a missing extension skips the test, so a
// local run needs no network.
func loadTestExtension(t *testing.T, db *sql.DB, name string) {
	t.Helper()
	if _, err := db.Exec("LOAD " + quoteDuckDBIdent(name)); err == nil {
		return
	} else if os.Getenv("ARC_TEST_EXTENSIONS") != "1" {
		t.Skipf("DuckDB extension %s is not in the local cache (set ARC_TEST_EXTENSIONS=1 to install it): %v", name, err)
	}
	if _, err := db.Exec("INSTALL " + quoteDuckDBIdent(name)); err != nil {
		t.Fatalf("install %s: %v", name, err)
	}
	if _, err := db.Exec("LOAD " + quoteDuckDBIdent(name)); err != nil {
		t.Fatalf("load %s: %v", name, err)
	}
}

type duckdbFunction struct {
	name, kind string
	textArg    bool
}

func listFunctions(t *testing.T, db *sql.DB) map[string]duckdbFunction {
	t.Helper()
	rows, err := db.Query(`SELECT lower(function_name), function_type, parameter_types::VARCHAR FROM duckdb_functions()`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	out := map[string]duckdbFunction{}
	for rows.Next() {
		var name, kind, params string
		if err := rows.Scan(&name, &kind, &params); err != nil {
			t.Fatal(err)
		}
		f := out[name]
		f.name, f.kind = name, kind
		f.textArg = f.textArg || strings.Contains(params, "VARCHAR") || strings.Contains(params, "ANY")
		out[name] = f
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// Every function a supported extension adds that is a table function, a
// pragma, or takes text (which can name a file or a URL) is either denied by
// the validators or recorded as reviewed (#440). A newer build of the
// extension that adds one fails here until someone has looked at it.
//
// Runs on a raw engine: after Arc's lockdown the extension catalog is not
// readable.
func TestSupportedExtensionFunctionsReviewed(t *testing.T) {
	for _, name := range SupportedExtensionNames() {
		t.Run(name, func(t *testing.T) {
			db, err := sql.Open("duckdb", "")
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			db.SetMaxOpenConns(1)
			before := listFunctions(t, db)
			loadTestExtension(t, db, name)
			after := listFunctions(t, db)
			ext := supportedExtensions[name]
			known := map[string]bool{}
			for _, n := range append(append([]string{}, ext.denied...), ext.reviewed...) {
				known[n] = true
			}
			var unreviewed []string
			added := 0
			for fn, f := range after {
				if _, ok := before[fn]; ok {
					continue
				}
				added++
				needsReview := f.textArg || (f.kind != "scalar" && f.kind != "aggregate" && f.kind != "macro")
				if needsReview && !known[fn] {
					unreviewed = append(unreviewed, fn+" ("+f.kind+")")
				}
			}
			if added == 0 {
				t.Fatalf("loading %s added no functions; the diff is not seeing the extension", name)
			}
			sort.Strings(unreviewed)
			if len(unreviewed) > 0 {
				t.Errorf("%s adds functions nobody has reviewed; add each to supportedExtensions[%q].denied (refused by the validators) or .reviewed:\n  %s",
					name, name, strings.Join(unreviewed, "\n  "))
			}
			for _, fn := range ext.denied {
				if _, ok := after[fn]; !ok {
					t.Errorf("denied function %s does not exist in %s any more; check what replaced it", fn, name)
				}
			}
		})
	}
}

// Only a supported extension's bare name is installed: a path, a URL, an
// unsupported or a community extension fails startup before any INSTALL.
func TestUnsupportedExtensionRefusedAtStartup(t *testing.T) {
	for _, bad := range []string{"h3", "vss", "/tmp/x.duckdb_extension", "https://example.invalid/x.duckdb_extension", "spatial FROM community", "Spatial", ""} {
		root := t.TempDir()
		_, err := New(&Config{MaxConnections: 1, MemoryLimit: "256MB", ThreadCount: 1, TempDirectory: root, LocalStorageRoot: root,
			Extensions: []string{bad}}, zerolog.Nop())
		if err == nil || !strings.Contains(err.Error(), "not a supported DuckDB extension") {
			t.Errorf("extension %q: New() error = %v, want refusal", bad, err)
		}
	}
}
