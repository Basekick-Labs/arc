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
	fail   map[string]error // prefix -> error for ListLevel, checked before levels
	// failFirst makes the first N ListLevel calls for a prefix fail.
	failFirst map[string]int
	calls     []string // prefixes ListLevel was asked for, in order
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
	if b.failFirst[prefix] > 0 {
		b.failFirst[prefix]--
		return nil, nil, errListing
	}
	l := b.levels[prefix]
	return l.dirs, l.files, nil
}

// ListDirectories answers from the same levels but is not subject to fail:
// the open-ended resolver lists with it, and a test needs to fail the day
// check alone.
func (b *levelBackend) ListDirectories(_ context.Context, prefix string) ([]string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.levels[prefix].dirs, nil
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
		fail:      map[string]error{},
		failFirst: map[string]int{},
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

// tierLevelMock is a cold-tier backend that can list one level; a recursive
// List fails the test.
type tierLevelMock struct {
	*tierMock
	t          *testing.T
	levels     map[string][]string // day prefix -> files directly in it
	levelCalls []string
}

func (m *tierLevelMock) ListLevel(ctx context.Context, prefix string) ([]string, []string, error) {
	m.levelCalls = append(m.levelCalls, prefix)
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
			// The hour check's listing of the day answers the day check.
			n := 0
			for _, c := range backend.levelCalls {
				if c == "db/cpu/2024/03/15/" {
					n++
				}
			}
			if n != 1 {
				t.Errorf("the day directory was listed %d times, want 1 (calls: %v)", n, backend.levelCalls)
			}
		})
	}
}
