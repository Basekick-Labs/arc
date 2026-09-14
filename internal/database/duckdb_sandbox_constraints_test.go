package database

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// This file pins the DuckDB behaviours that decide issue #641 (per-tenant
// scoping of allowed_directories). The design note is
// docs/progress/2026-09-12-duckdb-sandbox-scoping.md; these tests exist so the
// note cannot go quietly stale across a DuckDB bump.
//
// Every assertion here is deliberately "this is still impossible". A failure is
// therefore good news, not a regression in Arc: it means the constraint that
// made per-query scoping unbuildable has lifted, and #641 plus the note should
// be reopened. Each failure message says so.

// openRawDuckDB returns a fresh in-memory DuckDB instance. A separate instance
// per subtest is mandatory: enable_external_access=false is one-way for the
// life of the instance, so a shared handle would leak lockdown state between
// subtests and make the ordering assertions meaningless.
func openRawDuckDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// sandboxFixture lays out <root>/db1/t.csv and <root>/db2/t.csv, standing in
// for two tenants' storage directories.
func sandboxFixture(t *testing.T) (root, db1File, db2File string) {
	t.Helper()
	root = t.TempDir()
	write := func(db string) string {
		dir := filepath.Join(root, db)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", dir, err)
		}
		p := filepath.Join(dir, "t.csv")
		if err := os.WriteFile(p, []byte("a\n1\n"), 0o644); err != nil {
			t.Fatalf("write %s: %v", p, err)
		}
		return p
	}
	return root, write("db1"), write("db2")
}

func allowDir(t *testing.T, db *sql.DB, dir string) error {
	t.Helper()
	_, err := db.Exec(fmt.Sprintf("SET GLOBAL allowed_directories = ['%s']", escapeSQLString(dir)))
	return err
}

func lockdown(t *testing.T, db *sql.DB) {
	t.Helper()
	if _, err := db.Exec("SET GLOBAL enable_external_access = false"); err != nil {
		t.Fatalf("lockdown: %v", err)
	}
}

// readCSV reports the error from reading path, or nil when the read succeeded.
func readCSV(db *sql.DB, path string) error {
	var n int
	return db.QueryRow(fmt.Sprintf("SELECT count(*) FROM read_csv('%s')", escapeSQLString(path))).Scan(&n)
}

const reopen641 = "this constraint has lifted: reopen #641 and revisit docs/progress/2026-09-12-duckdb-sandbox-scoping.md"

func TestSandboxScopingConstraints(t *testing.T) {
	// Constraint 1: allowed_directories has no per-connection scope, so two
	// concurrent queries on Arc's shared handle cannot see different
	// allowlists. This is the reason per-query scoping needs a second
	// instance rather than a SET on the query's own connection.
	t.Run("no session or local scope", func(t *testing.T) {
		db := openRawDuckDB(t)
		root, _, _ := sandboxFixture(t)

		var scope string
		if err := db.QueryRow(
			"SELECT scope FROM duckdb_settings() WHERE name = 'allowed_directories'",
		).Scan(&scope); err != nil {
			t.Fatalf("read allowed_directories scope: %v", err)
		}
		if scope != "GLOBAL" {
			t.Errorf("allowed_directories scope = %q, want GLOBAL: %s", scope, reopen641)
		}

		// The two qualifiers fail for different reasons, and only the first
		// is evidence about this setting: SESSION is refused because
		// allowed_directories has no local scope, whereas SET LOCAL is
		// unimplemented in 1.5.5 for every setting. Asserting the substrings
		// keeps a future generic parse error from passing this as a silent
		// "still rejected".
		for _, tc := range []struct {
			qualifier   string
			wantSubstrs []string
		}{
			{"SESSION", []string{"cannot be set locally"}},
			// SET LOCAL is unimplemented in 1.5.5 for every setting, so its
			// rejection is not by itself evidence about this one. Either
			// spelling is accepted: if DuckDB implements SET LOCAL generically
			// while allowed_directories stays GLOBAL-only, the message becomes
			// the "cannot be set locally" form and nothing relevant has
			// changed. Requiring only "not implemented" would fail the build
			// on that entirely uninteresting day.
			{"LOCAL", []string{"not implemented", "cannot be set locally"}},
		} {
			stmt := fmt.Sprintf("SET %s allowed_directories = ['%s']", tc.qualifier, escapeSQLString(root))
			_, err := db.Exec(stmt)
			if err == nil {
				t.Errorf("SET %s allowed_directories succeeded, want rejection: %s", tc.qualifier, reopen641)
				continue
			}
			got := strings.ToLower(err.Error())
			matched := false
			for _, want := range tc.wantSubstrs {
				if strings.Contains(got, want) {
					matched = true
					break
				}
			}
			if !matched {
				t.Errorf("SET %s allowed_directories failed with %q, want one of %q",
					tc.qualifier, err, tc.wantSubstrs)
			}
		}
	})

	// Constraint 3: the allowlist is inert until external access is off. This
	// pins the ordering inside lockdownExternalAccess. If the two statements
	// were ever reordered, or the lockdown step were dropped, the allowlist
	// would be decorative and every I/O function would reach any path.
	t.Run("allowlist is inert until lockdown", func(t *testing.T) {
		db := openRawDuckDB(t)
		root, _, db2File := sandboxFixture(t)

		if err := allowDir(t, db, filepath.Join(root, "db1")); err != nil {
			t.Fatalf("set allowlist: %v", err)
		}
		if err := readCSV(db, db2File); err != nil {
			t.Fatalf("pre-lockdown read of a non-allowlisted path failed: %v\n"+
				"If the fixture is sound, DuckDB has started enforcing allowed_directories "+
				"eagerly rather than only after lockdown. That would mean a scoped instance "+
				"can be sandboxed before it finishes loading extensions and creating secrets, "+
				"which is the expensive constraint in the rejected design: %s", err, reopen641)
		}

		lockdown(t, db)

		err := readCSV(db, db2File)
		if err == nil {
			t.Fatalf("post-lockdown read of a non-allowlisted path succeeded, want Permission Error: %s", reopen641)
		}
		if !strings.Contains(err.Error(), "Permission Error") {
			t.Errorf("post-lockdown read failed with %q, want a Permission Error; the denial may "+
				"be coming from something other than the sandbox", err)
		}
	})

	// Constraint 2: the allowlist cannot be narrowed after lockdown, and
	// lockdown cannot be undone to get it back. Together with constraint 1
	// this is what makes per-query scoping on Arc's startup-locked handle
	// impossible rather than merely awkward.
	t.Run("immutable after lockdown", func(t *testing.T) {
		db := openRawDuckDB(t)
		root, db1File, _ := sandboxFixture(t)

		if err := allowDir(t, db, root); err != nil {
			t.Fatalf("set allowlist: %v", err)
		}
		lockdown(t, db)

		// Substring-matched so a future unrelated failure (a parse error, a
		// renamed setting) cannot pass as "still immutable".
		mustReject := func(what, wantSubstr string, err error) {
			t.Helper()
			if err == nil {
				t.Errorf("%s succeeded after lockdown, want rejection: %s", what, reopen641)
				return
			}
			if !strings.Contains(err.Error(), wantSubstr) {
				t.Errorf("%s failed with %q, want a %q rejection", what, err, wantSubstr)
			}
		}

		mustReject("narrowing allowed_directories", "Cannot change allowed_directories",
			allowDir(t, db, filepath.Join(root, "db1")))
		_, resetErr := db.Exec("RESET GLOBAL allowed_directories")
		mustReject("RESET allowed_directories", "Cannot change allowed_directories", resetErr)
		_, reenableErr := db.Exec("SET GLOBAL enable_external_access = true")
		mustReject("re-enabling external access", "Cannot enable external access", reenableErr)

		// The sandbox must still permit Arc's own reads inside the allowlist,
		// so a failure above is not masked by a wholly broken instance.
		if err := readCSV(db, db1File); err != nil {
			t.Errorf("allowlisted read failed after lockdown: %v", err)
		}
	})

	// Constraint 6: scoping is available per instance, and only per instance.
	// This is the mechanism the rejected instance-pool design would have used;
	// it is pinned so the note's "the only available mechanism" claim stays
	// checkable.
	t.Run("scope is per instance", func(t *testing.T) {
		root, db1File, db2File := sandboxFixture(t)

		newScoped := func(dir string) *sql.DB {
			db := openRawDuckDB(t)
			if err := allowDir(t, db, dir); err != nil {
				t.Fatalf("set allowlist %s: %v", dir, err)
			}
			lockdown(t, db)
			return db
		}

		instanceA := newScoped(filepath.Join(root, "db1"))
		instanceB := newScoped(filepath.Join(root, "db2"))

		if err := readCSV(instanceA, db1File); err != nil {
			t.Errorf("instance A cannot read its own directory: %v", err)
		}
		if err := readCSV(instanceB, db2File); err != nil {
			t.Errorf("instance B cannot read its own directory: %v", err)
		}
		// Substring-matched like the other denials: "any error" would also be
		// satisfied by a missing fixture file, which would quietly stop this
		// subtest from testing isolation at all.
		mustDeny := func(what string, err error) {
			t.Helper()
			if err == nil {
				t.Errorf("%s, want Permission Error; per-instance allowlists are no longer "+
					"isolated: %s", what, reopen641)
				return
			}
			if !strings.Contains(err.Error(), "Permission Error") {
				t.Errorf("%s and failed with %q, want a Permission Error", what, err)
			}
		}
		mustDeny("instance A read instance B's directory", readCSV(instanceA, db2File))
		mustDeny("instance B read instance A's directory", readCSV(instanceB, db1File))
	})
}

// TestLockConfigurationStillBlocksCacheInvalidation pins the reason
// lock_configuration is not enabled in lockdownExternalAccess: it would block
// the parquet_metadata_cache toggle that ClearHTTPCache performs after every
// delete, compaction and retention pass, and 1.5.5 offers no lock-immune
// substitute. If this test fails, the trade-off recorded in the design note has
// changed and lock_configuration becomes cheap hardening worth adopting.
func TestLockConfigurationStillBlocksCacheInvalidation(t *testing.T) {
	db := openRawDuckDB(t)

	// Match Arc: configureDatabase sets parquet_metadata_cache=true
	// (internal/database/duckdb.go). A raw instance defaults to false, and
	// starting from false hides a regression in the disable direction, which is
	// the direction ClearHTTPCache actually needs for invalidation.
	if _, err := db.Exec("SET GLOBAL parquet_metadata_cache=true"); err != nil {
		t.Fatalf("seed parquet_metadata_cache: %v", err)
	}

	if _, err := db.Exec("SET GLOBAL lock_configuration = true"); err != nil {
		t.Fatalf("lock_configuration is unavailable (%v); the design note assumes it exists", err)
	}

	// The exact pair ClearHTTPCache issues.
	for _, stmt := range []string{
		"SET GLOBAL parquet_metadata_cache=false",
		"SET GLOBAL parquet_metadata_cache=true",
	} {
		if _, err := db.Exec(stmt); err == nil {
			t.Errorf("%q succeeded under lock_configuration; lock_configuration no longer "+
				"conflicts with ClearHTTPCache, so revisit enabling it in lockdownExternalAccess "+
				"(docs/progress/2026-09-12-duckdb-sandbox-scoping.md)", stmt)
		} else if !strings.Contains(err.Error(), "locked") {
			t.Errorf("%q failed for an unexpected reason: %v", stmt, err)
		}
	}

	// The object-cache pragmas survive the lock but are inert, so they are not
	// a substitute. Pinned because "use the pragma instead" is the obvious
	// wrong fix for someone revisiting this.
	// Sample BETWEEN the two pragmas, not just around the pair. Running both and
	// comparing only the endpoints cannot see a wired disable_object_cache that
	// a following enable_object_cache puts back, and disable is the half that
	// would matter.
	before := currentSetting(t, db, "parquet_metadata_cache")
	if _, err := db.Exec("PRAGMA disable_object_cache"); err != nil {
		t.Fatalf("PRAGMA disable_object_cache failed: %v", err)
	}
	mid := currentSetting(t, db, "parquet_metadata_cache")
	if _, err := db.Exec("PRAGMA enable_object_cache"); err != nil {
		t.Fatalf("PRAGMA enable_object_cache failed: %v", err)
	}
	after := currentSetting(t, db, "parquet_metadata_cache")

	const pragmaNowWorks = "object-cache pragmas now move parquet_metadata_cache under " +
		"lock_configuration, so they may be the lock-immune substitute for the ClearHTTPCache " +
		"toggle that 1.5.5 lacked; revisit enabling lock_configuration " +
		"(docs/progress/2026-09-12-duckdb-sandbox-scoping.md)"
	if mid != before {
		t.Errorf("PRAGMA disable_object_cache changed parquet_metadata_cache (%s -> %s): %s",
			before, mid, pragmaNowWorks)
	}
	if after != before {
		t.Errorf("PRAGMA enable_object_cache changed parquet_metadata_cache (%s -> %s): %s",
			mid, after, pragmaNowWorks)
	}
}

// TestNoLocalScopeSettingAffectsFileAccess pins constraint 5 of the design
// note. A per-connection setting that gated file access would be a way to scope
// a query without a second instance, which is the mechanism #641 wanted. This
// asserts the LOCAL-scope surface is still confined to the known-harmless set,
// so a DuckDB bump that adds a file-access-relevant LOCAL setting shows up as a
// build failure rather than as a stale sentence in the note.
//
// profile_output, profiling_output and http_logging_output are the interesting
// members: they are LOCAL and they name a file DuckDB writes. They are listed as
// known because the sandbox still gates that write after lockdown, which the
// subtest below verifies rather than assumes.
func TestNoLocalScopeSettingAffectsFileAccess(t *testing.T) {
	// The complete LOCAL-scope set in DuckDB 1.5.5, read from duckdb_settings().
	// Three of these name a file DuckDB writes (profile_output,
	// profiling_output, http_logging_output); the rest are profiling, progress
	// bar, schema resolution, buffer sizing, and optimiser toggles. None opens
	// a file of the query's choosing for reading.
	known := map[string]bool{
		"custom_profiling_settings": true,
		"debug_force_external":      true,
		"enable_caching_operators":  true,
		"enable_http_logging":       true,
		"enable_profiling":          true,
		"enable_progress_bar":       true,
		"enable_progress_bar_print": true,
		"http_logging_output":       true,
		"profile_output":            true,
		"profiling_coverage":        true,
		"profiling_mode":            true,
		"profiling_output":          true,
		"progress_bar_time":         true,
		"schema":                    true,
		"search_path":               true,
		"streaming_buffer_size":     true,
	}

	db := openRawDuckDB(t)
	rows, err := db.Query("SELECT name FROM duckdb_settings() WHERE scope = 'LOCAL' ORDER BY name")
	if err != nil {
		t.Fatalf("list LOCAL settings: %v", err)
	}
	defer rows.Close()

	var unexpected []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan setting name: %v", err)
		}
		if !known[name] {
			unexpected = append(unexpected, name)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate LOCAL settings: %v", err)
	}
	if len(unexpected) > 0 {
		t.Errorf("new LOCAL-scope settings present: %v\n"+
			"If any of these gates file access, DuckDB now has a per-connection knob that could "+
			"scope a query without a second instance: %s", unexpected, reopen641)
	}

	// The one LOCAL setting that names a written file must still be gated by
	// the sandbox, otherwise it is a post-lockdown write primitive.
	t.Run("profile_output is still sandboxed", func(t *testing.T) {
		scoped := openRawDuckDB(t)
		root := t.TempDir()
		if err := allowDir(t, scoped, filepath.Join(root, "allowed")); err != nil {
			t.Fatalf("set allowlist: %v", err)
		}
		lockdown(t, scoped)

		outside := filepath.Join(root, "outside", "profile.json")
		if _, err := scoped.Exec("SET enable_profiling = 'json'"); err != nil {
			t.Fatalf("enable profiling: %v", err)
		}
		if _, err := scoped.Exec(fmt.Sprintf("SET profile_output = '%s'", escapeSQLString(outside))); err != nil {
			t.Fatalf("set profile_output: %v", err)
		}

		var n int
		err := scoped.QueryRow("SELECT count(*) FROM range(0, 10)").Scan(&n)
		if _, statErr := os.Stat(outside); statErr == nil {
			t.Errorf("profile_output wrote %s outside the allowlist (query err: %v); a LOCAL "+
				"setting is now a post-lockdown write primitive: %s", outside, err, reopen641)
		}
	})
}

func currentSetting(t *testing.T, db *sql.DB, name string) string {
	t.Helper()
	var v string
	if err := db.QueryRow(fmt.Sprintf("SELECT current_setting('%s')", escapeSQLString(name))).Scan(&v); err != nil {
		t.Fatalf("read setting %s: %v", name, err)
	}
	return v
}
