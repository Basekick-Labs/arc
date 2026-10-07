package api

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

func TestExtractDBMeasurementRejectsBackslashesIssue750(t *testing.T) {
	h := &QueryHandler{}

	paths := []string{
		`db\cpu/**/*.parquet`,
		`db/cpu\metrics/**/*.parquet`,
		`s3://bucket/db\cpu/**/*.parquet`,
		`azure://container/db\cpu/**/*.parquet`,
		`azure://container/db/cpu\metrics/2026/09/19/04/file.parquet`,
	}

	for _, path := range paths {
		t.Run(path, func(t *testing.T) {
			database, measurement := h.extractDBMeasurementFromPath(path)
			if database != "" || measurement != "" {
				t.Fatalf(
					"invalid path %q mapped to database=%q measurement=%q",
					path, database, measurement,
				)
			}
		})
	}
}

func TestExtractDBMeasurementPreservesValidPathsIssue750(t *testing.T) {
	h := &QueryHandler{}

	cases := []struct {
		path        string
		database    string
		measurement string
	}{
		{"db/cpu/**/*.parquet", "db", "cpu"},
		{"/srv/arc/db/cpu/**/*.parquet", "db", "cpu"},
		{"s3://bucket/db/cpu/**/*.parquet", "db", "cpu"},
		{"azure://container/db/cpu/**/*.parquet", "db", "cpu"},
		{"azure://container/db/cpu/2026/09/19/04/file.parquet", "db", "cpu"},
	}

	for _, tc := range cases {
		t.Run(tc.path, func(t *testing.T) {
			database, measurement := h.extractDBMeasurementFromPath(tc.path)
			if database != tc.database || measurement != tc.measurement {
				t.Fatalf(
					"got (%q, %q), want (%q, %q)",
					database, measurement, tc.database, tc.measurement,
				)
			}
		})
	}
}

func TestExtractDBMeasurementLocalRootIssue750(t *testing.T) {
	// On Windows the backslash is a directory separator. On Unix this
	// creates a directory whose name contains a literal backslash.
	// Both cases verify that only the trusted root gets this exception.
	root := filepath.Join(t.TempDir(), `root\directory`)

	backend, err := storage.NewLocalBackend(root, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}

	valid, err := storage.GetStoragePath(backend, "db", "cpu")
	if err != nil {
		t.Fatal(err)
	}

	h := &QueryHandler{storage: backend}

	database, measurement := h.extractDBMeasurementFromPath(valid)
	if database != "db" || measurement != "cpu" {
		t.Fatalf(
			"valid local path mapped to (%q, %q), want (db, cpu): %q",
			database, measurement, valid,
		)
	}

	suffix := "db/cpu/**/*.parquet"
	if !strings.HasSuffix(valid, suffix) {
		t.Fatalf("unexpected local storage path: %q", valid)
	}

	// Even inside a legitimate local root, a malformed storage key
	// must never be reinterpreted as a different database/measurement.
	invalid := strings.TrimSuffix(valid, suffix) + `db\cpu/**/*.parquet`

	database, measurement = h.extractDBMeasurementFromPath(invalid)
	if database != "" || measurement != "" {
		t.Fatalf(
			"invalid local key mapped to (%q, %q): %q",
			database, measurement, invalid,
		)
	}
}
