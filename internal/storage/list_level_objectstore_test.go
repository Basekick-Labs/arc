//go:build objectstore

package storage

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"
)

// assertListLevelContract writes one object directly in a directory and one
// in each of two subdirectories, on a real store, and checks ListLevel
// against them: the directory's subdirectories and its own object, as bare
// names, and nothing from beneath a subdirectory.
//
// The pruner decides from this whether a day holds daily files. A store whose
// delimiter listing returned no objects, or returned them under another
// name, would make every day look like it has none.
func assertListLevelContract(t *testing.T, b Backend) {
	t.Helper()
	level, ok := b.(LevelLister)
	if !ok {
		t.Fatalf("%T does not implement LevelLister", b)
	}
	ctx := context.Background()
	day := fmt.Sprintf("levelcontract-%d/cpu/2026/01/15/", time.Now().UnixNano())
	keys := []string{day + "cpu_daily.parquet", day + "10/a.parquet", day + "11/b.parquet"}
	for _, k := range keys {
		if err := b.Write(ctx, k, []byte("level contract "+k)); err != nil {
			t.Fatalf("Write(%s): %v", k, err)
		}
	}
	t.Cleanup(func() {
		for _, k := range keys {
			_ = b.Delete(context.Background(), k)
		}
	})

	check := func(prefix, wantDirs, wantFiles string) {
		t.Helper()
		dirs, files, err := level.ListLevel(ctx, prefix)
		if err != nil {
			t.Fatalf("ListLevel(%q): %v", prefix, err)
		}
		sort.Strings(dirs)
		sort.Strings(files)
		if got := strings.Join(dirs, ","); got != wantDirs {
			t.Errorf("ListLevel(%q) dirs = %q, want %q", prefix, got, wantDirs)
		}
		if got := strings.Join(files, ","); got != wantFiles {
			t.Errorf("ListLevel(%q) files = %q, want %q", prefix, got, wantFiles)
		}
	}
	check(day, "10,11", "cpu_daily.parquet")
	// Without the trailing separator: the method adds it.
	check(strings.TrimSuffix(day, "/"), "10,11", "cpu_daily.parquet")
	check(day+"10/", "", "a.parquet")
	check(day+"12/", "", "")
}

// On the S3-compatible store CI runs (SeaweedFS), with and without a
// configured object prefix.
func TestListLevelOnRealS3(t *testing.T) {
	for _, prefix := range []string{"", "tenant-a/arc"} {
		t.Run("prefix="+prefix, func(t *testing.T) {
			assertListLevelContract(t, prefixedMinioBackend(t, prefix))
		})
	}
}

func TestListLevelOnRealAzure(t *testing.T) {
	assertListLevelContract(t, azureBackend(t, "arc-contract/list-level"))
}

func TestListLevelOnRealGCS(t *testing.T) {
	assertListLevelContract(t, gcsBackend(t, gcsRunPrefix("list-level")))
}
