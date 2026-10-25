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

// duckdbSignature is one overload of a function: its name, type and
// parameter types, and a macro's body.
type duckdbSignature struct {
	name, kind, params, macro string
}

func listSignatures(t *testing.T, db *sql.DB) map[duckdbSignature]bool {
	t.Helper()
	rows, err := db.Query(`SELECT lower(function_name), function_type, parameter_types::VARCHAR, coalesce(macro_definition, '') FROM duckdb_functions()`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	out := map[duckdbSignature]bool{}
	for rows.Next() {
		var sig duckdbSignature
		if err := rows.Scan(&sig.name, &sig.kind, &sig.params, &sig.macro); err != nil {
			t.Fatal(err)
		}
		out[sig] = true
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// Every signature a supported extension adds that is a table function, a
// pragma, a macro, or takes text (which can name a file or a URL) belongs to
// a function that is either denied by the validators or recorded as
// reviewed (#440). Signatures, not names: a new overload of a reviewed name
// (or of a core one) is reviewed again. A macro whose body calls a denied
// function fails too, since the validators see only the macro's name. A
// newer build of the extension that adds any of these fails here until
// someone has looked at it.
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
			before := listSignatures(t, db)
			loadTestExtension(t, db, name)
			after := listSignatures(t, db)
			ext := supportedExtensions[name]
			known := map[string]bool{}
			for _, n := range append(append([]string{}, ext.denied...), ext.reviewed...) {
				known[n] = true
			}
			deniedNames := DeniedExtensionFunctions()
			var unreviewed []string
			names := map[string]bool{}
			added := 0
			for sig := range after {
				names[sig.name] = true
				if before[sig] {
					continue
				}
				added++
				text := strings.Contains(sig.params, "VARCHAR") || strings.Contains(sig.params, "ANY")
				needsReview := text || (sig.kind != "scalar" && sig.kind != "aggregate")
				if needsReview && !known[sig.name] {
					unreviewed = append(unreviewed, sig.name+" ("+sig.kind+" "+sig.params+")")
				}
				for _, d := range deniedNames {
					if sig.macro != "" && strings.Contains(strings.ToLower(sig.macro), d+"(") {
						unreviewed = append(unreviewed, sig.name+" (macro calling denied "+d+")")
					}
				}
			}
			if added == 0 {
				t.Fatalf("loading %s added no functions; the diff is not seeing the extension", name)
			}
			sort.Strings(unreviewed)
			if len(unreviewed) > 0 {
				t.Errorf("%s adds signatures nobody has reviewed; add each function to supportedExtensions[%q].denied (refused by the validators) or .reviewed:\n  %s",
					name, name, strings.Join(unreviewed, "\n  "))
			}
			for _, fn := range ext.denied {
				if !names[fn] {
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
