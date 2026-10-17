package api

// A tag or field named "time" is refused by LP import and /write before
// anything is written; imports refuse the WAL row's target names
// "_measurement" and "_database" as column names.

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

func TestLPImportRefusesReservedKeys(t *testing.T) {
	for _, tc := range []struct {
		name, line string
		status     int
	}{
		{"tag time", "cpu,time=a v=1 1700000000000000000\n", http.StatusBadRequest},
		{"field time", "cpu v=1,time=5i 1700000000000000000\n", http.StatusBadRequest},
		{"tag _database", "cpu,_database=x v=1 1700000000000000000\n", http.StatusBadRequest},
		{"field _measurement", "cpu v=1,_measurement=\"x\" 1700000000000000000\n", http.StatusBadRequest},
		{"field time_value is fine", "cpu v=1,time_value=2 1700000000000000000\n", http.StatusOK},
		{"tag Time is fine", "cpu,Time=a v=1 1700000000000000000\n", http.StatusOK},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ci := newChunkedImport(t, 10)
			// The reserved key only in the last line: refused before the
			// first chunk is written.
			setVar(t, &lpChunkBytes, 64)
			var lp strings.Builder
			for i := 0; i < 10; i++ {
				lp.WriteString("cpu v=1 1700000000000000000\n")
			}
			lp.WriteString(tc.line)
			status, body := ci.post(t, "/api/v1/import/lp?db=d", []byte(lp.String()))
			require.Equal(t, tc.status, status, body)
			if tc.status != http.StatusOK {
				require.Empty(t, ci.writes)
			}
		})
	}
}

func TestCSVImportRefusesTargetColumnNames(t *testing.T) {
	for _, name := range []string{"_measurement", "_database"} {
		ci := newChunkedImport(t, 10)
		status, body := ci.post(t, "/api/v1/import/csv?db=d&measurement=m&time_format=epoch_s", []byte("time,"+name+"\n1700000000,x\n"))
		require.Equal(t, http.StatusBadRequest, status, body)
		require.Empty(t, ci.writes)
	}
	// The legacy names are ordinary columns.
	ci := newChunkedImport(t, 10)
	status, body := ci.post(t, "/api/v1/import/csv?db=d&measurement=m&time_format=epoch_s", []byte("time,measurement,database,m\n1700000000,a,b,c\n"))
	require.Equal(t, http.StatusOK, status, body)
}

// /write drops only the points with a tag or field named "time"; the others
// are written and the answer is a "partial write" 400 (InfluxDB's behaviour).
func TestLPWriteDropsTimeKeyPoints(t *testing.T) {
	ci := newChunkedImport(t, 1000)
	h := NewLineProtocolHandler(ci.buf, zerolog.Nop())
	app := fiber.New(appConfig(DefaultServerConfig(), zerolog.Nop()))
	h.RegisterRoutes(app)
	post := func(body string) (int, string) {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/write/line-protocol", bytes.NewReader([]byte(body)))
		req.Header.Set("x-arc-database", "d")
		resp, err := app.Test(req, -1)
		require.NoError(t, err)
		out, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(out)
	}

	status, out := post("cpu v=1 1700000000000000000\ncpu v=2,time=5i 1700000001000000000\ncpu,time=a v=3 1700000002000000000\n")
	require.Equal(t, http.StatusBadRequest, status)
	require.Contains(t, out, "partial write: 2 points dropped")
	require.NoError(t, ci.buf.FlushAllAndWait(context.Background()))
	rows, types := ci.stored(t)
	require.EqualValues(t, 1, rows, "the valid point was not written, or a dropped one was")
	require.NotContains(t, types["time"], "utf8")

	// Only bad points: nothing written.
	ci2 := newChunkedImport(t, 1000)
	h2 := NewLineProtocolHandler(ci2.buf, zerolog.Nop())
	app2 := fiber.New(appConfig(DefaultServerConfig(), zerolog.Nop()))
	h2.RegisterRoutes(app2)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/write/line-protocol", bytes.NewReader([]byte("cpu v=2,time=5i 1700000001000000000\n")))
	req.Header.Set("x-arc-database", "d")
	resp, err := app2.Test(req, -1)
	require.NoError(t, err)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	require.Zero(t, ci2.buf.BufferedBytes())
}
