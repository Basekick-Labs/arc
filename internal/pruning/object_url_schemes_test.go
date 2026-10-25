package pruning

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// Every remote branch of the pruner answers the same for any object-store
// scheme as it does for s3://: the branch is chosen by storage.IsObjectURL and
// lists through the Backend. Before #1105 it was chosen by testing for "s3://"
// and "azure://" by name, so a gs:// path took the LOCAL branch, filepath.Glob
// found nothing, and every partition read as absent.
//
// s3 is the reference: each case runs it, then the same input with only the
// scheme changed, and requires the same output with only the scheme changed.
var objectURLSchemes = []string{"azure", "gs", "x-future"}

func swapScheme(v any, scheme string) any {
	switch x := v.(type) {
	case string:
		return strings.Replace(x, "s3://", scheme+"://", 1)
	case []string:
		out := make([]string, len(x))
		for i, s := range x {
			out[i] = strings.Replace(s, "s3://", scheme+"://", 1)
		}
		return out
	}
	return v
}

func TestEveryObjectSchemePrunesLikeS3(t *testing.T) {
	dirs := map[string][]string{
		"default/cpu/2026/01/15/": {"10", "11"},
		"default/cpu/2026/01/":    {"15"},
		"default/cpu/2026/":       {"01"},
		"default/cpu/":            {"2026"},
	}
	sql := "SELECT * FROM cpu WHERE time >= '2026-01-15 10:00:00' AND time < '2026-01-15 14:00:00'"
	run := func(glob string) (any, bool) {
		p := NewPartitionPruner(zerolog.Nop())
		p.SetStorageBackend(&mockS3Backend{existingDirs: dirs})
		return p.OptimizeTablePath(context.Background(), glob, sql)
	}
	ref, refOK := run("s3://mybucket/default/cpu/**/*.parquet")
	if !refOK {
		t.Fatalf("s3 reference did not prune: %v", ref)
	}
	for _, scheme := range objectURLSchemes {
		got, ok := run(scheme + "://mybucket/default/cpu/**/*.parquet")
		if !ok || !reflect.DeepEqual(got, swapScheme(ref, scheme)) {
			t.Errorf("%s: OptimizeTablePath = %v (pruned %v), want %v", scheme, got, ok, swapScheme(ref, scheme))
		}
	}
}

func TestEveryObjectSchemeTierPrunesLikeS3(t *testing.T) {
	for _, scheme := range append([]string{"s3"}, objectURLSchemes...) {
		backend := &tierMock{
			mockS3Backend: &mockS3Backend{existingDirs: map[string][]string{"db/cpu/2024/03/15/": {"14"}}},
			files:         map[string][]string{},
		}
		paths, outcome := NewPartitionPruner(zerolog.Nop()).PruneTierPaths(context.Background(),
			scheme+"://bucket/tenant1/db/cpu/**/*.parquet", "db", "cpu",
			tierRange(t, "2024-03-15T14:00:00Z", "2024-03-15T16:00:00Z"), backend, false)
		want := scheme + "://bucket/tenant1/db/cpu/2024/03/15/14/*.parquet"
		if outcome != TierPrunePruned || len(paths) != 1 || paths[0] != want {
			t.Errorf("%s: PruneTierPaths = %v (%v), want [%s] pruned", scheme, paths, outcome, want)
		}
	}
}

func TestEveryObjectSchemeFindsOpenEndPartitionsLikeS3(t *testing.T) {
	now := time.Now().UTC()
	future := now.Add(72 * time.Hour)
	dirs := map[string][]string{"db/cpu/": {now.Format("2006")}}
	if future.Format("2006") != now.Format("2006") {
		dirs["db/cpu/"] = append(dirs["db/cpu/"], future.Format("2006"))
	}
	for _, h := range []time.Time{now, future} {
		y, m, d := h.Format("2006"), h.Format("01"), h.Format("02")
		dirs["db/cpu/"+y+"/"] = appendOnce(dirs["db/cpu/"+y+"/"], m)
		dirs["db/cpu/"+y+"/"+m+"/"] = appendOnce(dirs["db/cpu/"+y+"/"+m+"/"], d)
		dirs["db/cpu/"+y+"/"+m+"/"+d+"/"] = appendOnce(dirs["db/cpu/"+y+"/"+m+"/"+d+"/"], h.Format("15"))
	}
	sql := "SELECT * FROM t WHERE time > NOW() - INTERVAL '1 hour'"
	for _, scheme := range append([]string{"s3"}, objectURLSchemes...) {
		p := livePruner(now)
		p.SetStorageBackend(&countingRemote{mockS3Backend: mockS3Backend{existingDirs: dirs}, dayFiles: map[string][]string{}})
		r, ok := p.OptimizeTablePath(context.Background(), scheme+"://bucket/db/cpu/**/*.parquet", sql)
		if !ok || !hasRemoteHour(asList(r), future.Format("15")) {
			t.Errorf("%s: open-end partition missing: %v %v", scheme, ok, r)
		}
		for _, path := range asList(r) {
			if !strings.HasPrefix(path, scheme+"://bucket/") {
				t.Errorf("%s: path %q lost its scheme (built with filepath.Join?)", scheme, path)
			}
		}
	}
}

func appendOnce(s []string, v string) []string {
	for _, x := range s {
		if x == v {
			return s
		}
	}
	return append(s, v)
}

func TestEveryObjectSchemeProvesAnEmptyRangeLikeS3(t *testing.T) {
	sql := "SELECT v FROM db.cpu WHERE time >= '2026-02-01T00:00:00Z' AND time < '2026-02-03T00:00:00Z'"
	for _, scheme := range append([]string{"s3"}, objectURLSchemes...) {
		p := NewPartitionPruner(zerolog.Nop())
		p.SetStorageBackend(&countingRemote{mockS3Backend: mockS3Backend{existingDirs: map[string][]string{
			"db/cpu/":         {"2026"},
			"db/cpu/2026/01/": {"01"},
		}}})
		_, optimized, empty := p.OptimizeTablePathVerdict(context.Background(), scheme+"://bucket/db/cpu/**/*.parquet", sql)
		if !empty || optimized {
			t.Errorf("%s: empty=%v optimized=%v, want a proven empty range", scheme, empty, optimized)
		}
	}
}

func TestFileTimePruningNeverTouchesAnObjectURL(t *testing.T) {
	now := time.Date(2026, 8, 28, 20, 40, 0, 0, time.UTC)
	p := newTestPruner(now)
	tr := &TimeRange{Start: now.Add(-10 * time.Minute), End: now}
	for _, scheme := range append([]string{"s3"}, objectURLSchemes...) {
		remote := fmt.Sprintf("%s://bucket/db/m/%s/*.parquet", scheme, now.Truncate(time.Hour).Format("2006/01/02/15"))
		out, changed := p.applyFileTimePruning(context.Background(), []string{remote}, tr)
		if changed || len(out) != 1 || out[0] != remote {
			t.Errorf("%s: remote path changed to %v", scheme, out)
		}
	}
}
