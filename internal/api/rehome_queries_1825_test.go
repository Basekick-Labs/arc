//go:build duckdb_arrow

package api

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	"github.com/basekick-labs/arc/internal/rehome"
	"github.com/basekick-labs/arc/internal/storage"
)

// A swap seen through the real query path: its pruner, its caches, its
// exclusions. Queries are sent after every step, and before the job to fill
// the caches with what they would remember: that the directories the outputs
// go to do not exist.
//
// The file sits in 2026-11-18 23h and holds six rows: two of that hour, one of
// 11-19 00h, three of 11-19 05h.
func TestSwapThroughTheQueryHandler1825(t *testing.T) {
	r := newOverflowRig(t)
	full := filepath.Join(r.data, swapKey)
	require.NoError(t, os.MkdirAll(filepath.Dir(full), 0o700))
	_, err := r.duck.Exec(fmt.Sprintf(`COPY (SELECT * FROM (VALUES
		(TIMESTAMPTZ '2026-11-18 23:10:00+00', 1), (TIMESTAMPTZ '2026-11-19 05:00:00+00', 2), (TIMESTAMPTZ '2026-11-18 23:20:00+00', 4),
		(TIMESTAMPTZ '2026-11-19 00:00:01+00', 8), (TIMESTAMPTZ '2026-11-19 05:30:00+00', 16), (TIMESTAMPTZ '2026-11-19 05:59:59+00', 32)) v("time", n))
		TO '%s' (FORMAT PARQUET)`, full))
	require.NoError(t, err)
	// A well-filed row of 11-19 00h, so that day has a directory and a query
	// of it is pruned to it (with no directory at all for its range, a query
	// reads the whole measurement).
	writeRow1825(t, r, "db/cpu/2026/11/19/00/ok.parquet", "2026-11-19 00:30:00", 64)

	backend, err := storage.NewLocalBackend(r.data, zerolog.Nop())
	require.NoError(t, err)
	t.Cleanup(func() { backend.Close() })
	reg := rehome.NewRegistry(backend)
	require.NoError(t, reg.Seed(context.Background()))
	r.h.SetOpenCompactions(reg)

	h := NewRehomeHandler(backend, nil, t.TempDir(), zerolog.Nop())
	t.Cleanup(func() { _ = h.Shutdown(context.Background()) })
	h.SetSwap(&RehomeSwap{Registry: reg, Reads: r.h.Reads(), FilePublished: r.h.FilePublished, InvalidateCaches: r.h.InvalidateCaches})
	app := fiber.New()
	h.RegisterRoutes(app)

	const (
		all    = "SELECT sum(n) AS s FROM db.cpu"
		both   = "SELECT sum(n) AS s FROM db.cpu WHERE time >= '2026-11-18 00:00:00' AND time < '2026-11-20 00:00:00'"
		day19  = "SELECT coalesce(sum(n), 0) AS s FROM db.cpu WHERE time >= '2026-11-19 00:00:00' AND time < '2026-11-20 00:00:00'"
		hour05 = "SELECT coalesce(sum(n), 0) AS s FROM db.cpu WHERE time >= '2026-11-19 05:00:00' AND time < '2026-11-19 06:00:00'"
	)
	sum := func(sql string) int { return count1825(t, r.post("/api/v1/query", sql)) }
	// Each row is a distinct power of two, so a sum names the rows read.
	check := func(when string, switched bool) {
		t.Helper()
		require.Equal(t, 127, sum(all), "%s: an unfiltered query", when)
		require.Equal(t, 127, sum(both), "%s: a query over both days", when)
		// The file's rows of 11-19 are behind the wrong directory until
		// reads are switched to the new files: that is the fault being
		// repaired. Only the well-filed row is seen before.
		want19 := 64
		if switched {
			want19 = 64 + 2 + 8 + 16 + 32
		}
		require.Equal(t, want19, sum(day19), "%s: a query of 11-19", when)
		if switched {
			require.Equal(t, 2+16+32, sum(hour05), "%s: a query of 11-19 05h", when)
		} else {
			_ = sum(hour05) // asked, so its answer is in the caches
		}
	}
	check("before the job", false)
	check("before the job, from the caches", false)

	switched := false
	h.afterStep = func(step string) {
		if step == "latched" {
			switched = true
		}
		check("after step "+step, switched)
	}
	code, out := rehomeDo(t, app, "POST", "/api/v1/rehome", "", fmt.Sprintf(`{"path":%q}`, swapKey))
	require.Equal(t, 202, code, "%v", out)
	var job map[string]interface{}
	require.Eventually(t, func() bool {
		_, job = rehomeDo(t, app, "GET", "/api/v1/rehome/"+out["id"].(string), "", "")
		return job["state"] != "running"
	}, 60*time.Second, 20*time.Millisecond)
	require.Equal(t, "done", job["state"], "%v", job)
	require.True(t, switched)
	check("after the job", true)
	check("after the job, from the caches", true)
	_, err = os.Stat(full)
	require.True(t, os.IsNotExist(err), "the file that was moved is still there")
}
