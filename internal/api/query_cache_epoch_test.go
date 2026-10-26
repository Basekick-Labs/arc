//go:build duckdb_arrow

package api

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/database"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// The SQL transform cache is keyed by the hours a query's range covers, not
// only by the current hour: a relative end that crosses into a new hour must
// be re-planned, or that hour stays out of the cached SQL for its 60s TTL.
func TestTransformCacheFollowsARelativeEndIntoANewHour(t *testing.T) {
	now := time.Now().UTC()
	h := now.Truncate(time.Hour)
	if now.Sub(h) < 10*time.Second || h.Add(time.Hour).Sub(now) < 10*time.Second {
		t.Skip("too close to an hour boundary")
	}
	dataDir := t.TempDir()
	for _, hr := range []time.Time{h.Add(-time.Hour), h} {
		d := filepath.Join(dataDir, "db1", "m", hr.Format("2006"), hr.Format("01"), hr.Format("02"), hr.Format("15"))
		require.NoError(t, os.MkdirAll(d, 0o700))
		require.NoError(t, os.WriteFile(filepath.Join(d, "m_x.parquet"), []byte("x"), 0o600))
	}
	logger := zerolog.Nop()
	backend, err := storage.NewLocalBackend(dataDir, logger)
	require.NoError(t, err)
	duck, err := database.New(&database.Config{MemoryLimit: "256MB", ThreadCount: 1, MaxConnections: 2, LocalStorageRoot: dataDir}, logger)
	require.NoError(t, err)
	defer duck.Close()
	hd := NewQueryHandler(duck, backend, logger, 0, 0)

	// The end sits 2s before the current hour, then crosses into it.
	x := int((now.Sub(h) + 2*time.Second) / time.Second)
	sql := fmt.Sprintf("SELECT * FROM db1.m WHERE time >= now() - INTERVAL '3 hours' AND time < now() - INTERVAL '%d seconds'", x)
	cur := filepath.Join("m", h.Format("2006"), h.Format("01"), h.Format("02"), h.Format("15"))
	first, _, err := hd.getTransformedSQL(context.Background(), sql, "")
	require.NoError(t, err)
	require.NotContains(t, first, cur, "precondition: the current hour is past the end")
	time.Sleep(4 * time.Second)
	second, _, err := hd.getTransformedSQL(context.Background(), sql, "")
	require.NoError(t, err)
	require.True(t, strings.Contains(second, cur), "the end entered the current hour but the cached SQL still leaves it out:\n%s", second)
}
