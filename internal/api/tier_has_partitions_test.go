package api

// An unpruned tier is kept in a multi-tier read only if it holds a parquet
// file; empty partition directories — what compaction and migration leave
// behind — do not count, and a backend that cannot answer keeps the tier.

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

func TestTierHasFiles(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	backend, err := storage.NewLocalBackend(root, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewLocalBackend: %v", err)
	}
	dir := filepath.Join(root, "db1", "cpu", "2024", "03", "15", "19")
	check := func(when string, wantHas, wantVerified bool) {
		t.Helper()
		if has, verified := tierHasFiles(ctx, backend, "db1", "cpu"); has != wantHas || verified != wantVerified {
			t.Fatalf("%s: has=%v verified=%v, want (%v, %v)", when, has, verified, wantHas, wantVerified)
		}
	}

	check("absent measurement", false, true)

	// Every file gone, the partition directories still there: the live bug.
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	check("empty partition directories", false, true)

	// A file in the hour directory.
	if err := os.WriteFile(filepath.Join(dir, "cpu_raw.parquet"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	check("hour-level file", true, true)

	// Only a day-level daily file.
	if err := os.Remove(filepath.Join(dir, "cpu_raw.parquet")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "db1", "cpu", "2024", "03", "15", "cpu_daily.parquet"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	check("day-level file", true, true)

	// A backend that cannot list directories is answered by one recursive
	// listing instead.
	if has, verified := tierHasFiles(ctx, noDirBackend{backend}, "db1", "cpu"); !has || !verified {
		t.Fatalf("non-listing backend with a file: has=%v verified=%v, want (true, true)", has, verified)
	}

	// A listing error leaves the question open, whether the walk or the
	// recursive fallback hits it.
	if has, verified := tierHasFiles(ctx, erringBackend{backend}, "db1", "cpu"); has || verified {
		t.Fatalf("failing store: has=%v verified=%v, want (false, false)", has, verified)
	}
	if has, verified := tierHasFiles(ctx, noDirBackend{erringBackend{backend}}, "db1", "cpu"); has || verified {
		t.Fatalf("failing non-listing store: has=%v verified=%v, want (false, false)", has, verified)
	}
}

// A replicating reader never prunes the empty partition directories that
// compaction and migration leave behind: months of history become a forest
// of empty Y/M/D/H chains, far more than the walk's budget. The walk must
// still answer — newest first when a file exists, and by falling back to
// one recursive listing when nothing is found before the budget runs out.
func TestTierHasFiles_EmptyForestBeyondBudget(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	backend, err := storage.NewLocalBackend(root, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewLocalBackend: %v", err)
	}
	// 90 days of empty day/hour chains across four months: well past 64 listings.
	for month := 1; month <= 4; month++ {
		for day := 1; day <= 23; day++ {
			dir := filepath.Join(root, "db1", "cpu", "2024", fmtTwo(month), fmtTwo(day), "19")
			if err := os.MkdirAll(dir, 0o755); err != nil {
				t.Fatal(err)
			}
		}
	}
	if has, verified := tierHasFiles(ctx, backend, "db1", "cpu"); has || !verified {
		t.Fatalf("empty forest: has=%v verified=%v, want (false, true) via the recursive fallback", has, verified)
	}

	// A file in the OLDEST chain: unreachable within the budget walking
	// newest-first, found by the fallback.
	oldest := filepath.Join(root, "db1", "cpu", "2024", "01", "01", "19", "cpu_old.parquet")
	if err := os.WriteFile(oldest, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if has, verified := tierHasFiles(ctx, backend, "db1", "cpu"); !has || !verified {
		t.Fatalf("file in the oldest chain: has=%v verified=%v, want (true, true)", has, verified)
	}

	// A file in the NEWEST chain: found by the walk itself.
	newest := filepath.Join(root, "db1", "cpu", "2024", "04", "23", "19", "cpu_new.parquet")
	if err := os.WriteFile(newest, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if has, verified := tierHasFiles(ctx, backend, "db1", "cpu"); !has || !verified {
		t.Fatalf("file in the newest chain: has=%v verified=%v, want (true, true)", has, verified)
	}
}

func fmtTwo(n int) string {
	if n < 10 {
		return "0" + string(rune('0'+n))
	}
	return string(rune('0'+n/10)) + string(rune('0'+n%10))
}

// noDirBackend hides DirectoryLister from an embedded backend.
type noDirBackend struct{ storage.Backend }

// erringBackend fails every listing, as an unreachable object store would.
type erringBackend struct{ storage.Backend }

func (erringBackend) List(context.Context, string) ([]string, error) {
	return nil, errors.New("store unreachable")
}

func (erringBackend) ListDirectories(context.Context, string) ([]string, error) {
	return nil, errors.New("store unreachable")
}
