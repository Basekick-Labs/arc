package api

import (
	"fmt"
	"strings"
	"testing"

	"github.com/duckdb/duckdb-go/v2"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	"github.com/basekick-labs/arc/internal/storage"
)

// #1825: a measurement's glob on the cold tier matching nothing is "the
// measurement is empty" in general, and a query to retry when Arc has just
// removed cold files: the read was planned while the last cold file was
// there, and its other paths hold the rows of the files that replaced it.
func TestClassifyNoFilesAfterAColdRemoval1825(t *testing.T) {
	_, h, _ := newVanishedFileRig(t)
	cold, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	require.NoError(t, err)
	uri, err := storage.ObjectURI(cold, "x")
	require.NoError(t, err)
	coldRoot := strings.TrimSuffix(uri, "x")
	noFiles := func(pattern string) error {
		return fmt.Errorf("arrow query failed: %w", &duckdb.Error{
			Type: duckdb.ErrorTypeIO,
			Msg:  `IO Error: No files found that match the pattern "` + pattern + `"`,
		})
	}
	recent := false
	h.coldRemoval = func() (bool, storage.Backend) { return recent, cold }
	coldGlob := coldRoot + "db1/cpu/**/*.parquet"
	hotGlob := h.storageRoot() + "db1/cpu/**/*.parquet"

	require.Equal(t, noFilesEmpty, h.classifyNoFiles(noFiles(coldGlob)), "no cold removal lately: an empty cold tier is an empty answer")
	recent = true
	h.vanishedInvalidatedAt.Store(0)
	require.Equal(t, noFilesVanished, h.classifyNoFiles(noFiles(coldGlob)), "just after a cold removal: retry")
	h.vanishedInvalidatedAt.Store(0)
	require.Equal(t, noFilesEmpty, h.classifyNoFiles(noFiles(hotGlob)), "the hot tier's glob is not the cold tier's")
	h.coldRemoval = func() (bool, storage.Backend) { return true, nil }
	require.Equal(t, noFilesEmpty, h.classifyNoFiles(noFiles(coldGlob)), "no cold backend")
}
