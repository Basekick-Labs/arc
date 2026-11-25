//go:build duckdb_arrow

package api

import (
	"io"
	"testing"

	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/gofiber/fiber/v2"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fasthttp"
)

type failedQueryWriter struct{ writes, failAt int }

func (w *failedQueryWriter) Write(p []byte) (int, error) {
	w.writes++
	if w.writes >= w.failAt {
		return 0, io.ErrClosedPipe
	}
	return len(p), nil
}

// Run real handlers/engines, then reject writes only at the response sink.
// Tiny results stay buffered until final flush, exposing ignored flush errors.
func TestQueryFinalFlushRecordsFailure(t *testing.T) {
	for _, tc := range []struct {
		name, route, encoding string
		plain                 bool
	}{
		{"plain_json", "/api/v1/query", "", true},
		{"arrow_json", "/api/v1/query", "", false},
		{"arrow_json_gzip", "/api/v1/query", "gzip", false},
		{"msgpack", "/api/v1/query/msgpack", "", false},
		{"msgpack_gzip", "/api/v1/query/msgpack", "gzip", false},
		{"arrow_ipc", "/api/v1/query/arrow", "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			original := arrowJSONQueryFunc
			if tc.plain {
				arrowJSONQueryFunc = nil
			}
			t.Cleanup(func() { arrowJSONQueryFunc = original })
			_, h, _ := newVanishedFileRig(t)
			h.disableClientDisconnect = true
			var fctx fasthttp.RequestCtx
			fctx.Request.Header.SetMethod("POST")
			fctx.Request.SetRequestURI(tc.route)
			fctx.Request.Header.SetContentType("application/json")
			fctx.Request.Header.Set("Accept-Encoding", tc.encoding)
			fctx.Request.SetBodyString(`{"sql":"SELECT 1"}`)
			app := fiber.New()
			c := app.AcquireCtx(&fctx)
			defer app.ReleaseCtx(c)
			s := metrics.Get().Snapshot()
			before := s["query_engine_executions"].(map[string]map[string]int64)[metrics.QueryEngineDuckDB]
			errorsBefore := s["query_errors_total"].(int64)
			successBefore := s["query_success_total"].(int64)
			disconnectKey := ""
			switch tc.name {
			case "plain_json":
				disconnectKey = "query_client_disconnects_sql_json_total"
			case "arrow_json", "arrow_json_gzip":
				disconnectKey = "query_client_disconnects_arrow_json_total"
			case "arrow_ipc":
				disconnectKey = "query_client_disconnects_arrow_ipc_total"
			}
			var err error
			if tc.route == "/api/v1/query/arrow" {
				err = h.executeQueryArrow(c)
			} else if tc.route == "/api/v1/query/msgpack" {
				err = h.executeQueryMsgPack(c)
			} else {
				err = h.executeQuery(c)
			}
			require.NoError(t, err)
			require.Equal(t, 200, fctx.Response.StatusCode())
			body := fctx.Response.BodyStream()
			require.NotNil(t, body, "must reach actual response stream")
			sink := &failedQueryWriter{failAt: 1}
			if tc.name == "msgpack" {
				sink.failAt = 2
			}
			_, err = body.(io.WriterTo).WriteTo(sink)
			require.GreaterOrEqual(t, sink.writes, sink.failAt, "failure injection must be reached")
			require.ErrorIs(t, err, io.ErrClosedPipe)
			require.NoError(t, body.(io.Closer).Close())
			after := metrics.Get().Snapshot()
			if disconnectKey != "" {
				require.Equal(t, s[disconnectKey].(int64)+1, after[disconnectKey], "disconnect must be counted once")
			}
			engine := after["query_engine_executions"].(map[string]map[string]int64)[metrics.QueryEngineDuckDB]
			require.Equal(t, before["error"]+1, engine["error"])
			require.Equal(t, before["success"], engine["success"])
			require.Equal(t, errorsBefore+1, after["query_errors_total"])
			require.Equal(t, successBefore, after["query_success_total"])
		})
	}
}
