package arcxrouter

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"

	"github.com/basekick-labs/arc/internal/pruning"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

func TestEngineQueryEntries_ExcludesBeforePrunedOrFullListing(t *testing.T) {
	for _, shape := range []string{ShapeDateTruncCent, ShapeCountStar} {
		t.Run(shape, func(t *testing.T) {
			root := t.TempDir()
			local, err := storage.NewLocalBackend(root, zerolog.Nop())
			if err != nil {
				t.Fatal(err)
			}
			write := func(key string) {
				t.Helper()
				path := filepath.Join(root, key)
				if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, []byte("local fixture"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			const hour = "db/m/2026/02/03/10/"
			const input, output, keep = hour + "in.parquet", hour + "out.parquet", hour + "keep.parquet"
			const history = "db/m/2025/02/03/10/history.parquet"
			write(input)
			write(keep)
			write(history)
			write(hour + "ignore.txt")
			calls := 0
			ctx := context.Background()
			exclude := func(gotCtx context.Context, prefix string) []string {
				calls++
				if gotCtx != ctx || prefix != "db/m/" {
					t.Fatalf("exclusion context/prefix changed: %q", prefix)
				}
				// Output becomes visible at the exclusion snapshot. Listing
				// before this call would drop the input without its replacement.
				write(output)
				return []string{input}
			}
			actual, err := engineQueryEntries(ctx, local, shape, "db", "m", "time >= '2026-02-03T10:00:00Z' AND time < '2026-02-03T11:00:00Z'", nil, pruning.NewPartitionPruner(zerolog.Nop()), exclude)
			if err != nil {
				t.Fatal(err)
			}
			expected := []string{keep, output}
			if shape == ShapeCountStar {
				expected = append(expected, history)
			}
			sort.Strings(actual)
			sort.Strings(expected)
			if calls != 1 || !reflect.DeepEqual(actual, expected) {
				t.Fatalf("entries = %v, exclusion calls = %d; want %v and one snapshot", actual, calls, expected)
			}
		})
	}
}

func TestEngineQueryEntries_SymlinkFallbackStillExcludesInputs(t *testing.T) {
	root, linked := t.TempDir(), t.TempDir()
	local, err := storage.NewLocalBackend(root, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	day := filepath.Join(root, "db/m/2026/02/03")
	if err := os.MkdirAll(day, 0700); err != nil {
		t.Fatal(err)
	}
	for _, file := range []string{"in.parquet", "out.parquet"} {
		if err := os.WriteFile(filepath.Join(day, file), []byte("local fixture"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(linked, "linked.parquet"), []byte("local fixture"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(linked, filepath.Join(day, "10")); err != nil {
		t.Fatal(err)
	}
	exclude := func(context.Context, string) []string { return []string{"db/m/2026/02/03/in.parquet"} }
	actual, err := engineQueryEntries(context.Background(), local, ShapeDateTruncCent, "db", "m", "time >= '2026-02-03T10:00:00Z' AND time < '2026-02-03T11:00:00Z'", nil, pruning.NewPartitionPruner(zerolog.Nop()), exclude)
	if err != nil {
		t.Fatal(err)
	}
	if expected := []string{"db/m/2026/02/03/out.parquet"}; !reflect.DeepEqual(actual, expected) {
		t.Fatalf("entries = %v, want %v", actual, expected)
	}
}
