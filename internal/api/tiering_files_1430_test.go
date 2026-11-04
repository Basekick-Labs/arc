package api

// #1430: GET /api/v1/tiering/files with both tier and database returned the
// database's files in every tier: the database filter returned before the
// tier was read.

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/tiering"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

func TestTieringFilesFiltersCombine(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "arc.db"))
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	store, err := tiering.NewMetadataStore(db, zerolog.Nop())
	require.NoError(t, err)

	day := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	for _, f := range []tiering.FileMetadata{
		{Path: "sdktier/cpu/hot.parquet", Database: "sdktier", Tier: tiering.TierHot, PartitionTime: day},
		{Path: "sdktier/cpu/cold.parquet", Database: "sdktier", Tier: tiering.TierCold, PartitionTime: day.AddDate(0, 0, -1)},
		{Path: "sdktier/cpu/colder.parquet", Database: "sdktier", Tier: tiering.TierCold, PartitionTime: day.AddDate(0, 0, -2)},
		{Path: "other/cpu/hot.parquet", Database: "other", Tier: tiering.TierHot, PartitionTime: day},
		{Path: "other/cpu/cold.parquet", Database: "other", Tier: tiering.TierCold, PartitionTime: day},
	} {
		f.Measurement, f.SizeBytes = "cpu", 1
		require.NoError(t, store.RecordFile(ctx, &f))
	}

	h := &TieringHandler{getFiles: func(ctx context.Context, tierParam, database string) ([]tiering.FileMetadata, error) {
		return listTieringFiles(ctx, store, tierParam, database)
	}}
	app := fiber.New()
	app.Get("/api/v1/tiering/files", h.GetFiles)

	// With both filters the files are the database listing's, in its order:
	// newest partition first.
	for _, tc := range []struct {
		query   string
		want    []string
		ordered bool
	}{
		{"?tier=cold&database=sdktier", []string{"sdktier/cpu/cold.parquet", "sdktier/cpu/colder.parquet"}, true},
		{"?tier=cold&database=sdktier&limit=1", []string{"sdktier/cpu/cold.parquet"}, true},
		{"?tier=hot&database=sdktier", []string{"sdktier/cpu/hot.parquet"}, true},
		{"?database=sdktier", []string{"sdktier/cpu/hot.parquet", "sdktier/cpu/cold.parquet", "sdktier/cpu/colder.parquet"}, true},
		{"?tier=cold", []string{"other/cpu/cold.parquet", "sdktier/cpu/cold.parquet", "sdktier/cpu/colder.parquet"}, false},
		{"", []string{"other/cpu/hot.parquet", "sdktier/cpu/hot.parquet", "other/cpu/cold.parquet", "sdktier/cpu/cold.parquet", "sdktier/cpu/colder.parquet"}, false},
	} {
		t.Run(tc.query, func(t *testing.T) {
			resp, err := app.Test(httptest.NewRequest(http.MethodGet, "/api/v1/tiering/files"+tc.query, nil), -1)
			require.NoError(t, err)
			defer resp.Body.Close()
			require.Equal(t, http.StatusOK, resp.StatusCode)
			var body struct {
				Files []tiering.FileMetadata `json:"files"`
				Count int                    `json:"count"`
			}
			require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
			var got []string
			for _, f := range body.Files {
				got = append(got, f.Path)
			}
			if tc.ordered {
				require.Equal(t, tc.want, got)
			} else {
				require.ElementsMatch(t, tc.want, got)
			}
			require.Equal(t, len(tc.want), body.Count)
		})
	}

	// An unknown or wrong-case tier is refused before the store is read,
	// rather than listed as hot.
	var listed atomic.Bool
	unread := &TieringHandler{getFiles: func(context.Context, string, string) ([]tiering.FileMetadata, error) {
		listed.Store(true)
		return nil, nil
	}}
	refusing := fiber.New()
	refusing.Get("/api/v1/tiering/files", unread.GetFiles)
	for _, query := range []string{"?tier=COLD&database=sdktier", "?tier=warm"} {
		t.Run(query, func(t *testing.T) {
			resp, err := refusing.Test(httptest.NewRequest(http.MethodGet, "/api/v1/tiering/files"+query, nil), -1)
			require.NoError(t, err)
			defer resp.Body.Close()
			require.Equal(t, http.StatusBadRequest, resp.StatusCode)
			require.False(t, listed.Load(), "files were listed for an invalid tier")
		})
	}
}
