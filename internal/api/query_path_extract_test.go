package api

import (
	"testing"

	"github.com/basekick-labs/arc/internal/storage"
)

type queryPathPrefixBackend struct {
	storage.Backend
	prefix string
}

func (b queryPathPrefixBackend) GetPrefix() string { return b.prefix }

// Paths without a configured object-store backend retain the existing
// best-effort extraction behaviour.
func TestExtractDBMeasurementFromPathWithoutConfiguredPrefix(t *testing.T) {
	h := &QueryHandler{}

	for _, tc := range []struct {
		name            string
		path            string
		wantDB, wantMea string
	}{
		// Globs: no year segment at all, so the fallback decides.
		{"azure glob no prefix", "azure://cont/mydb/cpu/**/*.parquet", "mydb", "cpu"},
		{"azure glob with prefix", "azure://cont/arc/mydb/cpu/**/*.parquet", "mydb", "cpu"},
		{"azure glob nested prefix", "azure://cont/a/b/mydb/cpu/**/*.parquet", "mydb", "cpu"},
		{"s3 glob with prefix", "s3://bkt/tenant/mydb/cpu/**/*.parquet", "mydb", "cpu"},

		// Full file paths: the year scan decides, from the end.
		{"azure file no prefix", "azure://cont/mydb/cpu/2026/10/06/14/f.parquet", "mydb", "cpu"},
		{"azure file with prefix", "azure://cont/arc/mydb/cpu/2026/10/06/14/f.parquet", "mydb", "cpu"},
		{"azure file nested prefix", "azure://cont/a/b/mydb/cpu/2026/10/06/14/f.parquet", "mydb", "cpu"},
		{"s3 file with prefix", "s3://bkt/tenant/mydb/cpu/2026/10/06/14/f.parquet", "mydb", "cpu"},

		// A prefix whose last segment is digits but not a year-shaped one.
		{"azure glob numeric prefix", "azure://cont/1234/mydb/cpu/**/*.parquet", "mydb", "cpu"},

		// Local paths are unaffected.
		{"local file", "/var/lib/arc/data/mydb/cpu/2026/10/06/14/f.parquet", "mydb", "cpu"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, mea := h.extractDBMeasurementFromPath(tc.path)
			if db != tc.wantDB || mea != tc.wantMea {
				t.Errorf("extractDBMeasurementFromPath(%q) = (%q, %q), want (%q, %q)",
					tc.path, db, mea, tc.wantDB, tc.wantMea)
			}
		})
	}
}

// A configured prefix whose last segment is year-shaped resolves to the real
// database and measurement, for both backends that carry a prefix (#1108).
//
// Only the two GLOB cases regress the fix; verified by mutation, by disabling
// the trim and re-running. The other four pass against the unfixed function
// and are here to pin that the trim does not over-reach:
//
//   - the FILE cases were never broken. The scan walks backwards from the end,
//     so it reaches the partition year at index 5 before the prefix year at
//     index 2 and answers correctly either way.
//   - the one- and two-segment prefixes were never broken either, because the
//     scan stops at `i >= 2` and the prefix year sits at index 0 or 1. That
//     bound is why the problem needed three prefix segments to appear at all,
//     and it is the only place that reasoning is now written down — the
//     load-time warning that used to carry it is gone with the bug.
func TestExtractDBMeasurementFromPathHandlesYearShapedStoragePrefixes(t *testing.T) {
	for _, tc := range []struct {
		name   string
		prefix string
		path   string
	}{
		{
			name:   "Azure glob",
			prefix: "a/b/2026/",
			path:   "azure://cont/a/b/2026/mydb/cpu/**/*.parquet",
		},
		{
			name:   "Azure file",
			prefix: "a/b/2026/",
			path:   "azure://cont/a/b/2026/mydb/cpu/2026/10/06/14/f.parquet",
		},
		{
			name:   "S3 glob",
			prefix: "a/b/2026/",
			path:   "s3://bkt/a/b/2026/mydb/cpu/**/*.parquet",
		},
		{
			name:   "S3 file",
			prefix: "a/b/2026/",
			path:   "s3://bkt/a/b/2026/mydb/cpu/2026/10/06/14/f.parquet",
		},
		{
			name:   "one-segment year prefix",
			prefix: "2026/",
			path:   "azure://cont/2026/mydb/cpu/**/*.parquet",
		},
		{
			name:   "two-segment year prefix",
			prefix: "a/2026/",
			path:   "azure://cont/a/2026/mydb/cpu/**/*.parquet",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := &QueryHandler{storage: queryPathPrefixBackend{prefix: tc.prefix}}
			db, mea := h.extractDBMeasurementFromPath(tc.path)
			if db != "mydb" || mea != "cpu" {
				t.Fatalf("extractDBMeasurementFromPath(%q) = (%q, %q), want (mydb, cpu)", tc.path, db, mea)
			}
		})
	}
}
