package pruning

import (
	"context"
	"reflect"
	"sort"
	"sync"
	"testing"

	"github.com/rs/zerolog"
)

// levelBackend is an object store that can list one level (storage.LevelLister).
// A recursive List fails the test: with a level listing available, the pruner
// must not enumerate everything under a day to learn whether it holds files
// of its own.
type levelBackend struct {
	*mockS3Backend
	t      *testing.T
	mu     sync.Mutex
	levels map[string]level // prefix -> its directories and direct files
	fail   map[string]error // prefix -> error, checked before levels
	calls  []string         // prefixes asked for, in order
}

type level struct {
	dirs, files []string
}

func (b *levelBackend) ListLevel(_ context.Context, prefix string) ([]string, []string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.calls = append(b.calls, prefix)
	if err := b.fail[prefix]; err != nil {
		return nil, nil, err
	}
	l := b.levels[prefix]
	return l.dirs, l.files, nil
}

func (b *levelBackend) ListDirectories(ctx context.Context, prefix string) ([]string, error) {
	dirs, _, err := b.ListLevel(ctx, prefix)
	return dirs, err
}

func (b *levelBackend) List(_ context.Context, prefix string) ([]string, error) {
	b.t.Errorf("recursive List(%q) called although the backend can list one level", prefix)
	return nil, nil
}

func (b *levelBackend) callsFor(prefix string) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	n := 0
	for _, c := range b.calls {
		if c == prefix {
			n++
		}
	}
	return n
}

const (
	lvlBase  = "s3://bucket"
	lvlMonth = "db/cpu/2025/01/"
	lvlDay   = "db/cpu/2025/01/15/"
	lvlDayG  = "s3://bucket/db/cpu/2025/01/15/*.parquet"
	lvlHourG = "s3://bucket/db/cpu/2025/01/15/10/*.parquet"
)

func newLevelPruner(t *testing.T, day level) (*PartitionPruner, *levelBackend) {
	t.Helper()
	b := &levelBackend{
		mockS3Backend: &mockS3Backend{},
		t:             t,
		levels: map[string]level{
			lvlMonth: {dirs: []string{"15"}},
			lvlDay:   day,
		},
		fail: map[string]error{},
	}
	p := NewPartitionPruner(zerolog.Nop())
	p.SetStorageBackend(b)
	return p, b
}

func TestDayLevelCheckUsesOneLevelListing(t *testing.T) {
	both := []string{lvlDayG, lvlHourG}
	cases := []struct {
		name string
		day  level
		in   []string
		want []string
		// how many times the day directory is listed
		dayListings int
	}{
		// The hour check already lists the day directory, and that listing
		// says whether the day holds daily files: no second request.
		{"daily files and hours: one listing answers both", level{dirs: []string{"10"}, files: []string{"cpu_daily.parquet"}}, both, both, 1},
		{"only hour directories: the day path is dropped", level{dirs: []string{"10"}}, both, []string{lvlHourG}, 1},
		{"direct files that are not parquet do not count", level{dirs: []string{"10"}, files: []string{"notes.txt"}}, both, []string{lvlHourG}, 1},
		// The newest-partition shortcut asks about a day glob by itself.
		{"the day glob alone, with daily files", level{files: []string{"cpu_daily.parquet"}}, []string{lvlDayG}, []string{lvlDayG}, 1},
		{"the day glob alone, without", level{dirs: []string{"10"}}, []string{lvlDayG}, []string{}, 1},
		{"the day glob alone, direct files that are not parquet", level{dirs: []string{"10"}, files: []string{"notes.txt"}}, []string{lvlDayG}, []string{}, 1},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p, b := newLevelPruner(t, c.day)
			got, verified, hits := p.filterExistingRemotePathsOpts(context.Background(), c.in, lvlBase, 0)
			if !reflect.DeepEqual(got, c.want) {
				t.Errorf("paths = %v, want %v", got, c.want)
			}
			if !verified {
				t.Error("verified = false on a store that answered every listing")
			}
			// Nothing here came from the cache: the empty-range proof reads a
			// hit as "this pass leaned on remembered listings" and lists again.
			if hits != 0 {
				t.Errorf("cache hits = %d on a first pass, want 0", hits)
			}
			if n := b.callsFor(lvlDay); n != c.dayListings {
				t.Errorf("the day directory was listed %d times, want %d (calls: %v)", n, c.dayListings, b.calls)
			}
		})
	}
}

// When the level listing of the day fails, nothing about the day is known:
// its hours are assumed to exist, its day path is dropped, and the result is
// unverified (so it is not cached, TestUnverifiedPruningResultIsNotCached).
func TestDayLevelListingFailureWithOneLevelListing(t *testing.T) {
	p, b := newLevelPruner(t, level{dirs: []string{"10"}, files: []string{"cpu_daily.parquet"}})
	b.fail[lvlDay] = errListing

	got, verified, _ := p.filterExistingRemotePathsOpts(context.Background(), []string{lvlDayG, lvlHourG}, lvlBase, 0)
	if want := []string{lvlHourG}; !reflect.DeepEqual(got, want) {
		t.Errorf("paths = %v, want %v", got, want)
	}
	if verified {
		t.Error("verified = true although the day could not be listed")
	}

	// Storage answers again. The failed listing must not have been
	// remembered as "this day has no daily files": asked about the day glob
	// alone (as the newest-partition shortcut does), it is kept.
	delete(b.fail, lvlDay)
	got, verified, _ = p.filterExistingRemotePathsOpts(context.Background(), []string{lvlDayG}, lvlBase, 0)
	if want := []string{lvlDayG}; !reflect.DeepEqual(got, want) || !verified {
		t.Errorf("day glob alone after recovery: paths = %v, verified = %v; want %v, true", got, verified, want)
	}

	// And the full set sees the daily files too.
	p.InvalidateGlobCache()
	got, verified, _ = p.filterExistingRemotePathsOpts(context.Background(), []string{lvlDayG, lvlHourG}, lvlBase, 0)
	if want := []string{lvlDayG, lvlHourG}; !reflect.DeepEqual(got, want) || !verified {
		t.Errorf("after recovery: paths = %v, verified = %v; want %v, true", got, verified, want)
	}
}

// tierLevelMock is a cold-tier backend that can list one level; a recursive
// List fails the test.
type tierLevelMock struct {
	*tierMock
	t      *testing.T
	levels map[string][]string // day prefix -> files directly in it
}

func (m *tierLevelMock) ListLevel(ctx context.Context, prefix string) ([]string, []string, error) {
	dirs, err := m.tierMock.ListDirectories(ctx, prefix)
	return dirs, m.levels[prefix], err
}

func (m *tierLevelMock) List(_ context.Context, prefix string) ([]string, error) {
	m.t.Errorf("recursive List(%q) called although the tier backend can list one level", prefix)
	return nil, nil
}

// The tier read asks a day for its own files the same way.
func TestTierDayLevelCheckUsesOneLevelListing(t *testing.T) {
	const day = "s3://bucket/tenant1/db/cpu/2024/03/15/*.parquet"
	const hour = "s3://bucket/tenant1/db/cpu/2024/03/15/14/*.parquet"
	for _, c := range []struct {
		name  string
		files []string
		want  []string
	}{
		{"daily file present", []string{"cpu_daily.parquet"}, []string{day, hour}},
		{"only a non-parquet file", []string{"notes.txt"}, []string{hour}},
		{"no direct files", nil, []string{hour}},
	} {
		t.Run(c.name, func(t *testing.T) {
			p := NewPartitionPruner(zerolog.Nop())
			backend := &tierLevelMock{
				tierMock: &tierMock{
					mockS3Backend: &mockS3Backend{existingDirs: map[string][]string{
						"db/cpu/2024/03/":    {"15"},
						"db/cpu/2024/03/15/": {"14"},
					}},
					files: map[string][]string{},
				},
				t:      t,
				levels: map[string][]string{"db/cpu/2024/03/15/": c.files},
			}
			paths, outcome := p.PruneTierPaths(context.Background(), "s3://bucket/tenant1/db/cpu/**/*.parquet", "db", "cpu",
				tierRange(t, "2024-03-15T14:00:00Z", "2024-03-15T15:00:00Z"), backend, false)
			if outcome != TierPrunePruned {
				t.Fatalf("outcome = %v, want TierPrunePruned", outcome)
			}
			got := append([]string(nil), paths...)
			sort.Strings(got)
			if !reflect.DeepEqual(got, c.want) {
				t.Errorf("paths = %v, want %v", got, c.want)
			}
		})
	}
}
