package api

import "testing"

func TestExtractDBMeasurementFromPathRejectsBackslashIssue750(t *testing.T) {
	h := &QueryHandler{}

	valid := []struct {
		path        string
		database    string
		measurement string
	}{
		{
			"azure://container/prod/cpu/2026/09/12/13/f.parquet",
			"prod",
			"cpu",
		},
		{
			"s3://bucket/prod/cpu/**/*.parquet",
			"prod",
			"cpu",
		},
		{
			"prod/cpu/2026/09/12/13/f.parquet",
			"prod",
			"cpu",
		},
	}

	for _, tc := range valid {
		db, measurement := h.extractDBMeasurementFromPath(tc.path)
		if db != tc.database || measurement != tc.measurement {
			t.Errorf(
				"valid path %q: got (%q, %q), want (%q, %q)",
				tc.path, db, measurement,
				tc.database, tc.measurement,
			)
		}
	}

	invalid := []string{
		`azure://container/prod\private/cpu/2026/09/12/13/f.parquet`,
		`s3://bucket/prod\private/cpu/**/*.parquet`,
		`prod\private/cpu/2026/09/12/13/f.parquet`,
	}

	for _, path := range invalid {
		db, measurement := h.extractDBMeasurementFromPath(path)

		if db != "" || measurement != "" {
			t.Errorf(
				"invalid path %q was interpreted as database=%q measurement=%q",
				path, db, measurement,
			)
		}
	}
}
