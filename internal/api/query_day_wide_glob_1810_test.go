package api

import (
	"fmt"
	"testing"

	"github.com/duckdb/duckdb-go/v2"
	"github.com/stretchr/testify/require"
)

// A day read through one recursive glob (because its listing failed) that
// matches nothing is a pruned partition with no files, never "the
// measurement is empty": answering empty for it would answer the whole
// query with no rows. The measurement's own unpruned glob still means empty.
func TestClassifyNoFilesTellsADayWideGlobFromAnEmptyMeasurement(t *testing.T) {
	_, h, _ := newVanishedFileRig(t)
	root := h.storageRoot()
	require.NotEmpty(t, root, "the rig's storage root")

	noFiles := func(pattern string) error {
		return fmt.Errorf("arrow query failed: %w", &duckdb.Error{
			Type: duckdb.ErrorTypeIO,
			Msg:  `IO Error: No files found that match the pattern "` + pattern + `"`,
		})
	}
	for _, c := range []struct {
		name, pattern string
		want          noFilesKind
	}{
		{"the measurement's unpruned glob", root + "db1/cpu/**/*.parquet", noFilesEmpty},
		{"a day-wide glob", root + "db1/cpu/2026/10/20/**/*.parquet", noFilesVanished},
		{"a day-wide glob in a spoke namespace", root + "spoke/child/cpu/2026/10/20/**/*.parquet", noFilesVanished},
		// Database "10" and measurement "20" under a root ending "2026" would
		// look like a date; under the known root it is still a measurement.
		{"an unpruned glob with numeric names", root + "10/20/**/*.parquet", noFilesEmpty},
		// Not under this node's root (another tier): the date shape decides,
		// and it decides for the retryable answer.
		{"a date-shaped glob under an unknown root", "s3://elsewhere/db1/cpu/2026/10/20/**/*.parquet", noFilesVanished},
		{"an unpruned glob under an unknown root", "s3://elsewhere/db1/cpu/**/*.parquet", noFilesEmpty},
		{"an hour glob", root + "db1/cpu/2026/10/20/11/*.parquet", noFilesVanished},
	} {
		h.vanishedInvalidatedAt.Store(0)
		require.Equal(t, c.want, h.classifyNoFiles(noFiles(c.pattern)), c.name)
	}
}
