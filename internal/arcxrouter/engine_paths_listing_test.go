package arcxrouter

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/basekick-labs/arc/internal/pruning"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// Listing contract: a range-pruned object list must not promote an
// ordinary directory to a readable parquet object when LocalBackend.List
// explicitly excludes directories.
func TestEngineParquetEntries_PreservesLocalListingDirectoryContract(t *testing.T) {
	base := t.TempDir()
	local, err := storage.NewLocalBackend(base, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	hour := filepath.Join(base, "prod/cpu/2026/02/03/10")
	if err := os.MkdirAll(filepath.Join(hour, "not-an-object.parquet"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(hour, "real.parquet"), []byte("local fixture"), 0644); err != nil {
		t.Fatal(err)
	}
	expected, err := local.List(context.Background(), "prod/cpu/")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(expected, []string{"prod/cpu/2026/02/03/10/real.parquet"}) {
		t.Fatalf("baseline LocalBackend.List = %v", expected)
	}
	actual, pruned := engineParquetEntries(context.Background(), local, ShapeDateTruncCent, "prod", "cpu", "time >= '2026-02-03T10:00:00Z' AND time < '2026-02-03T11:00:00Z'", nil, pruning.NewPartitionPruner(zerolog.Nop()))
	if !pruned {
		t.Fatal("expected range pruning")
	}
	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("pruned object list = %v; unpruned LocalBackend.List = %v", actual, expected)
	}
}

func TestEngineParquetEntries_PreservesLocalListingSymlinkContract(t *testing.T) {
	for _, kind := range []string{"file", "dangling-file", "hour-directory", "day-directory", "measurement-directory"} {
		t.Run(kind, func(t *testing.T) {
			base := t.TempDir()
			target := t.TempDir()
			local, err := storage.NewLocalBackend(base, zerolog.Nop())
			if err != nil {
				t.Fatal(err)
			}
			hourRel := "prod/cpu/2026/02/03/10"
			hour := filepath.Join(base, hourRel)
			if err := os.MkdirAll(hour, 0755); err != nil {
				t.Fatal(err)
			}
			var link, dest string
			switch kind {
			case "file", "dangling-file":
				link, dest = filepath.Join(hour, "link.parquet"), filepath.Join(target, "target.parquet")
				if kind == "file" {
					if err := os.WriteFile(dest, []byte("local fixture"), 0644); err != nil {
						t.Fatal(err)
					}
				}
			case "hour-directory":
				link, dest = hour, target
				if err := os.Remove(link); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(target, "linked.parquet"), []byte("local fixture"), 0644); err != nil {
					t.Fatal(err)
				}
			case "day-directory":
				link, dest = filepath.Dir(hour), target
				if err := os.Remove(hour); err != nil {
					t.Fatal(err)
				}
				if err := os.Remove(link); err != nil {
					t.Fatal(err)
				}
				if err := os.MkdirAll(filepath.Join(target, "10"), 0755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(target, "10/linked.parquet"), []byte("local fixture"), 0644); err != nil {
					t.Fatal(err)
				}
			case "measurement-directory":
				link, dest = filepath.Join(base, "prod/cpu"), target
				// This entire tree is a fixture created above, not repository data.
				if err := os.RemoveAll(link); err != nil {
					t.Fatal(err)
				}
				if err := os.MkdirAll(filepath.Join(target, "2026/02/03/10"), 0755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(target, "2026/02/03/10/linked.parquet"), []byte("local fixture"), 0644); err != nil {
					t.Fatal(err)
				}
			}
			if err := os.Symlink(dest, link); err != nil {
				t.Fatal(err)
			}
			listed, err := local.List(context.Background(), "prod/cpu/")
			if err != nil {
				t.Fatal(err)
			}
			var expected []string
			for _, key := range listed {
				if filepath.Ext(key) == ".parquet" {
					expected = append(expected, key)
				}
			}
			actual, pruned := engineParquetEntries(context.Background(), local, ShapeDateTruncCent, "prod", "cpu", "time >= '2026-02-03T10:00:00Z' AND time < '2026-02-03T11:00:00Z'", nil, pruning.NewPartitionPruner(zerolog.Nop()))
			wantPruned := kind == "file" || kind == "dangling-file"
			if pruned != wantPruned {
				t.Fatalf("pruned = %v, want %v for %s", pruned, wantPruned, kind)
			}
			// A safe decline preserves the caller's existing full-list behavior.
			if !pruned {
				actual = expected
			}
			if !reflect.DeepEqual(actual, expected) {
				t.Fatalf("pruned parquet list = %v; unpruned LocalBackend.List parquet files = %v", actual, expected)
			}
		})
	}
}

func TestEngineParquetEntries_NormalizedBasePaths(t *testing.T) {
	base := t.TempDir()
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	relative, err := filepath.Rel(cwd, base)
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{base + string(filepath.Separator), base + string(filepath.Separator) + ".", relative} {
		t.Run(path, func(t *testing.T) {
			local, err := storage.NewLocalBackend(path, zerolog.Nop())
			if err != nil {
				t.Fatal(err)
			}
			if local.GetBasePath() != filepath.Clean(base) {
				t.Fatalf("base = %q, want %q", local.GetBasePath(), base)
			}
			key := "prod/cpu/2026/02/03/10/real.parquet"
			full := filepath.Join(base, key)
			if err := os.MkdirAll(filepath.Dir(full), 0755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(full, []byte("local fixture"), 0644); err != nil {
				t.Fatal(err)
			}
			actual, pruned := engineParquetEntries(context.Background(), local, ShapeDateTruncCent, "prod", "cpu", "time >= '2026-02-03T10:00:00Z' AND time < '2026-02-03T11:00:00Z'", nil, pruning.NewPartitionPruner(zerolog.Nop()))
			if !pruned || !reflect.DeepEqual(actual, []string{key}) {
				t.Fatalf("entries = %v, pruned = %v; want %s", actual, pruned, key)
			}
		})
	}
}
