package config

import (
	"os"
	"testing"
)

// compaction.exclude_databases: default empty, TOML array form, and the
// comma-separated environment form (viper does not split env strings on
// commas; the compaction manager normalizes, so the raw value just has to
// arrive intact).
func TestCompactionExcludeDatabasesConfig(t *testing.T) {
	t.Run("default empty", func(t *testing.T) {
		t.Chdir(t.TempDir())
		cfg, err := Load()
		if err != nil {
			t.Fatal(err)
		}
		if len(cfg.Compaction.ExcludeDatabases) != 0 {
			t.Fatalf(
				"default exclude_databases = %v, want empty",
				cfg.Compaction.ExcludeDatabases,
			)
		}
	})

	t.Run("toml array", func(t *testing.T) {
		dir := t.TempDir()
		t.Chdir(dir)
		toml := "[compaction]\nexclude_databases = [\"staging\", \"imports_backlog\"]\n"
		if err := os.WriteFile(dir+"/arc.toml", []byte(toml), 0o600); err != nil {
			t.Fatal(err)
		}
		cfg, err := Load()
		if err != nil {
			t.Fatal(err)
		}
		got := cfg.Compaction.ExcludeDatabases
		if len(got) != 2 || got[0] != "staging" || got[1] != "imports_backlog" {
			t.Fatalf("exclude_databases = %v, want [staging imports_backlog]", got)
		}
	})

	t.Run("env whitespace form splits", func(t *testing.T) {
		t.Chdir(t.TempDir())
		t.Setenv("ARC_COMPACTION_EXCLUDE_DATABASES", "staging imports_backlog")
		cfg, err := Load()
		if err != nil {
			t.Fatal(err)
		}
		got := cfg.Compaction.ExcludeDatabases
		if len(got) != 2 || got[0] != "staging" || got[1] != "imports_backlog" {
			t.Fatalf("env exclude_databases = %v, want [staging imports_backlog]", got)
		}
	})

	t.Run("env comma form stays one verbatim entry", func(t *testing.T) {
		// Commas are legal inside spoke namespace IDs, so nothing may
		// split on them — "fleet,eu" must arrive as a single name.
		t.Chdir(t.TempDir())
		t.Setenv("ARC_COMPACTION_EXCLUDE_DATABASES", "fleet,eu")
		cfg, err := Load()
		if err != nil {
			t.Fatal(err)
		}
		got := cfg.Compaction.ExcludeDatabases
		if len(got) != 1 || got[0] != "fleet,eu" {
			t.Fatalf("env exclude_databases = %v, want the single entry [fleet,eu]", got)
		}
	})
}
