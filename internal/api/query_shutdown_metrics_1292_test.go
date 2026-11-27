//go:build duckdb_arrow

package api

import (
	"context"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/stretchr/testify/require"
)

// A query that shutdown cancels counts as one failed execution on every query
// route (#1292), so success + errors = requests holds through a shutdown. The
// registry still records it as cancelled (#1644).
func TestShutdownCancelCountsAsAFailureOnEveryRoute1292(t *testing.T) {
	for _, tc := range []struct {
		name, route string
		plain       bool
	}{
		{"plain_json", "/api/v1/query", true}, {"arrow_json", "/api/v1/query", false},
		{"arrow_ipc", "/api/v1/query/arrow", false}, {"msgpack", "/api/v1/query/msgpack", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			original := arrowJSONQueryFunc
			if tc.plain {
				arrowJSONQueryFunc = nil
			}
			t.Cleanup(func() { arrowJSONQueryFunc = original })
			r, h, _ := newVanishedFileRig(t)
			lifetime, endLifetime := context.WithCancel(context.Background())
			defer endLifetime()
			h.SetLifetime(lifetime)
			counts := func() (int64, int64, int64, int64) {
				s := metrics.Get().Snapshot()
				e := s["query_engine_executions"].(map[string]map[string]int64)[metrics.QueryEngineDuckDB]
				return e["success"], e["error"], s["query_success_total"].(int64), s["query_errors_total"].(int64)
			}
			es, ee, as, ae := counts()
			timer := time.AfterFunc(300*time.Millisecond, endLifetime)
			defer timer.Stop()
			status, _, _, msg, _ := r.query(tc.route, "SELECT sum(a.range * b.range) FROM range(200000) a, range(200000) b")
			require.Equal(t, 503, status, msg)
			es2, ee2, as2, ae2 := counts()
			require.Equal(t, es, es2, "no engine success")
			require.Equal(t, ee+1, ee2, "one engine failure")
			require.Equal(t, as, as2, "no aggregate success")
			require.Equal(t, ae+1, ae2, "one aggregate error")
		})
	}
}
