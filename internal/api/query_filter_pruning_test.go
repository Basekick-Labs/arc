package api

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// #1360: a query with an aggregate FILTER (WHERE …) read every hour of the
// measurement, because the FILTER's WHERE counted as a second one. Hour 08's
// file is corrupted here, so a query that reads it fails and one pruned to
// 10:00-12:00 does not.
func TestAggregateFilterQueryIsPruned(t *testing.T) {
	r, _, arm := newVanishedFileRig(t)
	require.NoError(t, os.WriteFile(arm("08"), []byte("not parquet"), 0o600))

	const window = " FROM db1.cpu WHERE time >= '2026-10-20 10:00:00' AND time < '2026-10-20 12:00:00'"
	for _, sql := range []string{
		"SELECT count(*)" + window,
		"SELECT count(*), count(*) FILTER (WHERE v > 0)" + window,
		"SELECT count(*) FILTER (WHERE v > 0), sum(v) FILTER (WHERE time >= '2026-10-20 11:00:00')" + window,
	} {
		status, ok, n, msg, _ := r.query("/api/v1/query", sql)
		require.Equal(t, 200, status, "%s read hour 08: %q", sql, msg)
		require.True(t, ok)
		require.Equal(t, float64(2000), n, sql)
	}

	// A bound only inside a FILTER bounds nothing: the query reads every
	// hour, so it meets the corrupt file.
	status, _, _, _, _ := r.query("/api/v1/query", "SELECT count(*), count(*) FILTER (WHERE time >= '2026-10-20 10:00:00') FROM db1.cpu")
	require.NotEqual(t, 200, status, "a FILTER bound narrowed the whole-table count")
}
