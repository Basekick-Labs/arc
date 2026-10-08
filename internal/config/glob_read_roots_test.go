package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Regression tests for the B4 finding on #990: compaction interpolates a full
// filesystem path into read_parquet(), which DuckDB reads as a PATTERN. The
// per-file guard in internal/compaction cannot be where an operator discovers
// this, because every input fails it on every cycle and compaction silently
// never progresses. These pin the startup refusal instead.
//
// The metacharacters */?[]{} are all legal in POSIX directory names, which is
// why "/data/arc[prod]" is a realistic root rather than a contrived one.
func TestLoad_RefusesGlobUnsafeStorageRoot(t *testing.T) {
	for _, root := range []string{"arc[prod]", "arc*", "arc?", "arc{a,b}", "arc]x["} {
		t.Run(root, func(t *testing.T) {
			t.Chdir(t.TempDir())
			t.Setenv("ARC_STORAGE_BACKEND", "local")
			t.Setenv("ARC_STORAGE_LOCAL_PATH", filepath.Join("data", root))

			_, err := Load()
			if err == nil {
				t.Fatalf("Load() accepted storage.local_path containing a glob metacharacter (%q); "+
					"every compaction input would then fail the read_parquet guard, on every cycle, forever", root)
			}
			if !strings.Contains(err.Error(), "storage.local_path") {
				t.Errorf("error does not name the key the operator must change: %v", err)
			}
			if !strings.Contains(err.Error(), "read_parquet") {
				t.Errorf("error does not say WHY the path is refused, so an operator cannot tell it from a typo check: %v", err)
			}
		})
	}
}

// The control. A refusal that fires on ordinary paths is worse than the bug:
// dots, dashes and parentheses are not glob metacharacters and must start.
func TestLoad_AcceptsOrdinaryStorageRoots(t *testing.T) {
	for _, root := range []string{"arc", "arc-v1.0", "arc_prod", "arc (old)", "data/arc"} {
		t.Run(root, func(t *testing.T) {
			t.Chdir(t.TempDir())
			t.Setenv("ARC_STORAGE_BACKEND", "local")
			t.Setenv("ARC_STORAGE_LOCAL_PATH", filepath.Join("data", root))

			if _, err := Load(); err != nil {
				t.Fatalf("Load() refused an ordinary storage.local_path %q: %v", root, err)
			}
		})
	}
}

// compaction.temp_directory reaches read_parquet on the REMOTE backend path,
// where inputs are still streamed into it and read from there — so it needs the
// same refusal even though #969's fast path does not use it.
func TestLoad_RefusesGlobUnsafeCompactionTempDirectory(t *testing.T) {
	t.Chdir(t.TempDir())
	t.Setenv("ARC_STORAGE_BACKEND", "local")
	t.Setenv("ARC_COMPACTION_ENABLED", "true")
	t.Setenv("ARC_COMPACTION_TEMP_DIRECTORY", "./data/compaction[1]")

	_, err := Load()
	if err == nil {
		t.Fatal("Load() accepted compaction.temp_directory containing a glob metacharacter")
	}
	if !strings.Contains(err.Error(), "compaction.temp_directory") {
		t.Errorf("error does not name the key: %v", err)
	}
}

// A non-local primary backend must not be refused for a glob character in
// storage.local_path, which it never reads. Checking it would refuse a node
// whose unused local path happens to contain a bracket.
func TestLoad_GlobUnsafeLocalPathIgnoredOnRemoteBackend(t *testing.T) {
	t.Chdir(t.TempDir())
	t.Setenv("ARC_STORAGE_BACKEND", "s3")
	t.Setenv("ARC_STORAGE_LOCAL_PATH", "./data/arc[prod]")
	t.Setenv("ARC_STORAGE_S3_BUCKET", "arc-test")

	if _, err := Load(); err != nil && strings.Contains(err.Error(), "storage.local_path") {
		t.Fatalf("refused an unused storage.local_path on an s3 backend: %v", err)
	}
}

// The shipped default must pass, or the release breaks every stock deployment.
func TestLoad_ShippedDefaultStorageRootIsGlobSafe(t *testing.T) {
	t.Chdir(t.TempDir())
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() with defaults: %v", err)
	}
	if _, err := os.Stat("."); err != nil {
		t.Fatal(err)
	}
	if strings.ContainsAny(cfg.Storage.LocalPath, `*?[]{}`) {
		t.Fatalf("the shipped default storage.local_path %q is itself glob-unsafe", cfg.Storage.LocalPath)
	}
}
