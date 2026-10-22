package api

// Row-format msgpack writes drop only the points that cannot be written
// (line protocol's rules) and answer "partial write", as line protocol does.

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Basekick-Labs/msgpack/v6"
	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

func newMsgPackRowsRig(t *testing.T) (*lpWriteRig, func(payload interface{}) (int, string)) {
	t.Helper()
	r := newLPWriteRig(t, nil) // buffer over a local dir, JSON log
	app := fiber.New(appConfig(DefaultServerConfig(), zerolog.Nop()))
	NewMsgPackHandler(zerolog.New(r.log), r.buf, 100<<20).RegisterRoutes(app)
	post := func(payload interface{}) (int, string) {
		body, err := msgpack.Marshal(payload)
		require.NoError(t, err)
		req := httptest.NewRequest(http.MethodPost, "/api/v1/write/msgpack", bytes.NewReader(body))
		req.Header.Set("x-arc-database", "d")
		req.Header.Set("Content-Type", "application/msgpack")
		resp, err := app.Test(req, -1)
		require.NoError(t, err)
		out, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(out)
	}
	return r, post
}

func row(m string, ts int64, fields map[string]interface{}, tags map[string]interface{}) map[string]interface{} {
	r := map[string]interface{}{"m": m, "t": ts, "fields": fields}
	if tags != nil {
		r["tags"] = tags
	}
	return r
}

func TestMsgPackRowsPartialWrite(t *testing.T) {
	r, post := newMsgPackRowsRig(t)
	status, out := post([]interface{}{
		row("cpu", 1700000000, map[string]interface{}{"v": 1.5}, map[string]interface{}{"host": "a"}),
		row("cpu", 1700000001, map[string]interface{}{"v": "x"}, map[string]interface{}{"host": "b"}),
		row("cpu", 1700000002, map[string]interface{}{"v": 2.5, "time": int64(1)}, nil),
		row("mem", 1700000000, map[string]interface{}{"free": int64(10)}, nil),
	})
	require.Equal(t, http.StatusBadRequest, status, out)
	require.Contains(t, out, "partial write: 2 points dropped")
	require.Contains(t, out, "the other points were written")
	rows, types := r.stored(t)
	require.Equal(t, map[string]int64{"cpu": 1, "mem": 1}, rows)
	require.Equal(t, "float64", types["cpu"]["v"])
	require.Equal(t, "utf8", types["cpu"]["host"])
	require.Contains(t, r.log.String(), "MessagePack points dropped (partial write)")
}

func TestMsgPackRowsAllDropped(t *testing.T) {
	r, post := newMsgPackRowsRig(t)
	status, out := post([]interface{}{row("cpu", 1700000000, map[string]interface{}{"time": int64(1)}, nil)})
	require.Equal(t, http.StatusBadRequest, status, out)
	require.Contains(t, out, "partial write: 1 points dropped")
	require.NotContains(t, out, "the other points were written")
	require.Zero(t, r.buf.BufferedBytes())
	require.True(t, strings.Contains(r.log.String(), "MessagePack points dropped"))
}

func TestMsgPackRowsValid(t *testing.T) {
	r, post := newMsgPackRowsRig(t)
	status, out := post([]interface{}{
		row("cpu", 1700000000, map[string]interface{}{"v": 1.5, "n": int64(1)}, map[string]interface{}{"host": "a"}),
		row("cpu", 1700000001, map[string]interface{}{"v": 2.5, "n": nil}, map[string]interface{}{"host": "b"}),
	})
	require.Equal(t, http.StatusNoContent, status, out)
	rows, types := r.stored(t)
	require.Equal(t, map[string]int64{"cpu": 2}, rows)
	require.Equal(t, "int64", types["cpu"]["n"])
	require.NotContains(t, r.log.String(), "points dropped")
}

// A payload the typed decoder leaves to the record path (a float32 value)
// with an unwritable row: the others are written, the answer is a partial
// write, as on the typed path.
func TestMsgPackRowsGenericPathPartialWrite(t *testing.T) {
	r, post := newMsgPackRowsRig(t)
	status, out := post([]interface{}{
		row("cpu", 1700000000, map[string]interface{}{"v": float32(1.5)}, nil),
		row("cpu", 1700000001, map[string]interface{}{"v": float32(2.5), "time": int64(1)}, nil),
	})
	require.Equal(t, http.StatusBadRequest, status, out)
	require.Contains(t, out, "partial write: 1 points dropped")
	require.Contains(t, out, "the other points were written")
	rows, _ := r.stored(t)
	require.Equal(t, map[string]int64{"cpu": 1}, rows)
}

// #1342: a row whose names Arc could not read back is dropped (partial
// write); a columnar item is dropped whole and the other items written.
func TestMsgPackDropsUnstorableNames(t *testing.T) {
	r, post := newMsgPackRowsRig(t)
	status, out := post([]interface{}{
		row("cpu", 1700000000, map[string]interface{}{"v": 1.5}, nil),
		row("cpu", 1700000001, map[string]interface{}{"Time": 2.5}, nil),
	})
	require.Equal(t, http.StatusBadRequest, status, out)
	require.Contains(t, out, "partial write: 1 points dropped")
	rows, _ := r.stored(t)
	require.Equal(t, map[string]int64{"cpu": 1}, rows)

	r2, post2 := newMsgPackRowsRig(t)
	status, out = post2(map[string]interface{}{"batch": []interface{}{
		map[string]interface{}{"m": "a", "columns": map[string]interface{}{"time": []interface{}{int64(1700000000000)}, "v": []interface{}{1.0}}},
		map[string]interface{}{"m": "b", "columns": map[string]interface{}{"time": []interface{}{int64(1700000000000), int64(1700000000001)}, "Temp": []interface{}{1.0, 2.0}, "temp": []interface{}{3.0, 4.0}}},
	}})
	require.Equal(t, http.StatusBadRequest, status, out)
	require.Contains(t, out, "partial write: 2 points dropped")
	require.Contains(t, out, ingest.ColumnNameCaseTwin)
	rows, _ = r2.stored(t)
	require.Equal(t, map[string]int64{"a": 1}, rows)
}
