package api

import "testing"

// extractDBMeasurementFromPath strips the scheme and the bucket or container
// from a storage URI, which made it a candidate for breaking when Azure gained
// a key prefix (#1102). It does not, and this pins why: both of its rules key
// off the END of the path — the year scan walks backwards from the last
// segment, and the fallback takes the last two non-glob segments — so leading
// prefix segments are simply skipped either way. S3 has had a prefix since
// before this function was written, which is the existing evidence; these
// cases make it explicit for both backends.
//
// Added as part of the #1102 trace, which asked for the answer rather than an
// assumption.
func TestExtractDBMeasurementFromPathIsPrefixAgnostic(t *testing.T) {
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

// The one shape that does mis-parse, recorded rather than fixed: a prefix of
// three or more segments whose LAST segment looks like a year (20xx) puts that
// segment at index >= 2 of the trimmed path, where the backwards year scan
// finds it and reads the two segments before it as the database and
// measurement.
//
// This is pre-existing and backend-independent — S3 has had a prefix all
// along, and the same spelling mis-parses there identically, as the second
// case shows. It is NOT introduced by #1102 and is deliberately left alone
// here; it is tracked as #1108, and config.Load now emits a startup warning
// for a prefix whose last segment is year-shaped (see
// Config.checkObjectPrefix). The test exists so the behaviour is written down
// rather than rediscovered, and so whoever fixes it sees these cases flip.
func TestExtractDBMeasurementFromPathMisparsesAYearShapedPrefixTail(t *testing.T) {
	h := &QueryHandler{}

	// Prefix "a/b/2026/": the real database and measurement are mydb and cpu.
	if db, mea := h.extractDBMeasurementFromPath("azure://cont/a/b/2026/mydb/cpu/**/*.parquet"); db != "a" || mea != "b" {
		t.Errorf("behaviour changed: got (%q, %q), the documented pre-existing answer is (\"a\", \"b\"); if this is now (\"mydb\", \"cpu\") the bug is fixed and this test should be removed", db, mea)
	}
	// Identical on S3, which is what makes it pre-existing rather than new.
	if db, mea := h.extractDBMeasurementFromPath("s3://bkt/a/b/2026/mydb/cpu/**/*.parquet"); db != "a" || mea != "b" {
		t.Errorf("S3 answer = (%q, %q); it must match the Azure answer above", db, mea)
	}
	// A one- or two-segment prefix ending in a year is fine, because the scan
	// stops at index 2.
	if db, mea := h.extractDBMeasurementFromPath("azure://cont/2026/mydb/cpu/**/*.parquet"); db != "mydb" || mea != "cpu" {
		t.Errorf("single-segment year prefix = (%q, %q), want (\"mydb\", \"cpu\")", db, mea)
	}
	if db, mea := h.extractDBMeasurementFromPath("azure://cont/a/2026/mydb/cpu/**/*.parquet"); db != "mydb" || mea != "cpu" {
		t.Errorf("two-segment year-tailed prefix = (%q, %q), want (\"mydb\", \"cpu\")", db, mea)
	}
}
