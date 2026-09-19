package api

import "testing"

func TestExtractDBMeasurementFromPathRejectsBackslashIssue750(t *testing.T) {
	h := &QueryHandler{}

	tests := []struct {
		name        string
		path        string
		wantDB      string
		wantMeasure string
	}{
		{
			name: "invalid relative namespace",
			path: `db\cpu/2026/09/19/03/**/*.parquet`,
		},
		{
			name: "invalid S3 namespace",
			path: `s3://bucket/db\cpu/2026/09/19/03/**/*.parquet`,
		},
		{
			name: "invalid Azure namespace",
			path: `azure://container/db\cpu/2026/09/19/03/**/*.parquet`,
		},
		{
			name: "invalid measurement separator",
			path: `db/cpu\2026/09/19/03/**/*.parquet`,
		},
		{
			name:   "valid relative namespace",
			path:   `db/cpu/2026/09/19/03/**/*.parquet`,
			wantDB: "db", wantMeasure: "cpu",
		},
		{
			name:   "valid S3 namespace",
			path:   `s3://bucket/db/cpu/2026/09/19/03/**/*.parquet`,
			wantDB: "db", wantMeasure: "cpu",
		},
		{
			name:   "valid local absolute path",
			path:   `/var/arc/db/cpu/2026/09/19/03/**/*.parquet`,
			wantDB: "db", wantMeasure: "cpu",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db, measurement := h.extractDBMeasurementFromPath(tc.path)
			if db != tc.wantDB || measurement != tc.wantMeasure {
				t.Errorf(
					"path %q was misrouted: got (%q, %q), want (%q, %q)",
					tc.path, db, measurement,
					tc.wantDB, tc.wantMeasure,
				)
			}
		})
	}
}
