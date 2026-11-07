//go:build duckdb_arrow

package api

import (
	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

// Each request that reaches DuckDB contributes exactly one engine outcome.
// The rig removes a real Parquet file during a scan; no engine is mocked.
func TestQueryExecutionOutcomeInvariant(t *testing.T) {
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
			r, h, arm := newVanishedFileRig(t)
			counts := func() (int64, int64, int64, int64) {
				s := metrics.Get().Snapshot()
				e := s["query_engine_executions"].(map[string]map[string]int64)[metrics.QueryEngineDuckDB]
				return e["success"], e["error"], s["query_success_total"].(int64), s["query_errors_total"].(int64)
			}
			check := func(sql string, status int, success bool) {
				t.Helper()
				a, b, c, d := counts()
				got, _, _, msg, _ := r.query(tc.route, sql)
				require.Equal(t, status, got, msg)
				aa, bb, cc, dd := counts()
				if success {
					require.Equal(t, a+1, aa)
					require.Equal(t, b, bb)
					require.Equal(t, c+1, cc)
					require.Equal(t, d, dd)
				} else {
					require.Equal(t, a, aa)
					require.Equal(t, b+1, bb)
					require.Equal(t, c, cc)
					require.Equal(t, d+1, dd)
				}
			}
			check("SELECT 1", 200, true)
			check("SELECT missing_column FROM range(1)", 500, false)
			const q = "SELECT COUNT(*) FROM db1.cpu WHERE time >= '2026-10-20 08:00:00' AND time < '2026-10-20 12:00:00' AND arc_test_remove_once(v)"
			check(q, 200, true)
			arm("11")
			check(q, 503, false)
			if tc.name == "arrow_json" || tc.name == "msgpack" {
				before := queryMetricInt(t, "query_latency_count")
				check("SELECT COUNT(*) FROM db1.nothing_here", 200, true)
				require.Equal(t, before+1, queryMetricInt(t, "query_latency_count"))
			}
			if tc.name == "arrow_ipc" {
				h.queryTimeout = time.Nanosecond
				before := queryMetricInt(t, "query_timeouts_total")
				check("SELECT sum(i) FROM range(100000000) t(i)", 504, false)
				require.Equal(t, before+1, queryMetricInt(t, "query_timeouts_total"))
			}
		})
	}
}
