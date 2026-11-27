package api

// #1437: a line-protocol or MessagePack write of several measurements is one
// buffer call per measurement. A failure after some were buffered used to be
// answered as if nothing was written (a refusal's 503 + Retry-After, or a 503
// "rejected"), so a client resent the whole body and wrote the buffered part
// twice. It is now a 500 naming what was written, without the refusal headers;
// a failure before anything was buffered keeps its old answer.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/Basekick-Labs/msgpack/v6"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/basekick-labs/arc/pkg/models"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

const threeMeasurements = "aaa,h=a v=1 1700000000000000000\n" +
	"bbb,h=a v=2 1700000000000000000\nbbb,h=b v=3 1700000000000000000\n" +
	"ccc,h=a v=4 1700000000000000000\n"

type stopAnswer struct {
	status  int
	header  http.Header
	body    map[string]interface{}
	rawBody string
}

func postLP(t *testing.T, r *lpWriteRig, body string) stopAnswer {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/write/line-protocol", strings.NewReader(body))
	req.Header.Set("x-arc-database", "d")
	resp, err := r.app.Test(req, -1)
	require.NoError(t, err)
	raw, _ := io.ReadAll(resp.Body)
	a := stopAnswer{status: resp.StatusCode, header: resp.Header, rawBody: string(raw)}
	_ = json.Unmarshal(raw, &a.body)
	return a
}

// failLPAt makes the buffer call for measurement fail with err, on both
// line-protocol paths, and restores them after the test.
func failLPAt(t *testing.T, measurement string, err error) {
	t.Helper()
	typed, record := lpWriteTyped, lpWriteRecord
	t.Cleanup(func() { lpWriteTyped, lpWriteRecord = typed, record })
	lpWriteTyped = func(b *ingest.ArrowBuffer, ctx context.Context, db, m string, batch *ingest.TypedColumnBatch, n int) error {
		if m == measurement {
			return err
		}
		return typed(b, ctx, db, m, batch, n)
	}
	lpWriteRecord = func(b *ingest.ArrowBuffer, ctx context.Context, db string, rec *models.ColumnarRecord) error {
		if rec.Measurement == measurement {
			return err
		}
		return record(b, ctx, db, rec)
	}
}

// lpPaths are the two line-protocol write paths: typed, and the record path
// that a deployment with decimal columns takes.
var lpPaths = []struct {
	name string
	cfg  func() *config.IngestConfig
}{
	{"typed", func() *config.IngestConfig { return nil }},
	{"record", func() *config.IngestConfig {
		return &config.IngestConfig{DecimalColumns: []string{"trades:price=18,8"}}
	}},
}

var stopCauses = []struct {
	name string
	err  error
}{
	{"schema churn", ingest.ErrSchemaChurnExceeded},
	{"buffer closing", ingest.ErrBufferClosing},
	{"wal not durable (a refusal)", ingest.ErrWALNotDurable},
	{"fence (a refusal)", ingest.ErrIngestFenced},
	{"generic", errors.New("disk on fire")},
}

func requireNoRetryHeaders(t *testing.T, h http.Header) {
	t.Helper()
	for _, k := range []string{fiber.HeaderRetryAfter, IngestFencedHeader, IngestRefusedHeader} {
		require.Empty(t, h.Get(k), "%s must not be set once part of the request is stored", k)
	}
}

func TestLPWriteStoppedPartWayNamesWhatWasWritten(t *testing.T) {
	for _, path := range lpPaths {
		for _, cause := range stopCauses {
			t.Run(path.name+"/"+cause.name, func(t *testing.T) {
				r := newLPWriteRig(t, path.cfg())
				failLPAt(t, "bbb", cause.err)

				a := postLP(t, r, threeMeasurements)
				require.Equal(t, http.StatusInternalServerError, a.status, a.rawBody)
				requireNoRetryHeaders(t, a.header)
				require.Equal(t, float64(1), a.body["rows_written"], a.rawBody)
				require.Equal(t, []interface{}{"aaa"}, a.body["written_measurements"], a.rawBody)
				require.Equal(t, "bbb", a.body["stopped_at_measurement"], a.rawBody)
				msg, _ := a.body["error"].(string)
				require.Contains(t, msg, "write stopped part-way")
				require.Contains(t, msg, "Resending the whole request writes the written rows again")
				require.NotContains(t, strings.ToLower(msg), "partial write", "the SDK and InfluxDB clients read that phrase as a 400 drop")

				rows, _ := r.stored(t)
				require.Equal(t, map[string]int64{"aaa": 1}, rows, "aaa stored, bbb and ccc not")
			})
		}
	}
}

// Nothing buffered yet: the answer each failure always had.
func TestLPWriteFailingFirstMeasurementIsUnchanged(t *testing.T) {
	for _, path := range lpPaths {
		t.Run(path.name, func(t *testing.T) {
			cases := []struct {
				err    error
				status int
				header string
				text   string
			}{
				{ingest.ErrSchemaChurnExceeded, http.StatusServiceUnavailable, "", "Write rejected (schema churn)"},
				{ingest.ErrBufferClosing, http.StatusServiceUnavailable, "", "Write rejected (server shutting down)"},
				{ingest.ErrWALNotDurable, http.StatusServiceUnavailable, IngestRefusedHeader, "could not be made durable"},
				{errors.New("disk on fire"), http.StatusInternalServerError, "", "Write failed: disk on fire"},
			}
			for _, tc := range cases {
				r := newLPWriteRig(t, path.cfg())
				failLPAt(t, "aaa", tc.err)
				a := postLP(t, r, threeMeasurements)
				require.Equal(t, tc.status, a.status, a.rawBody)
				require.Contains(t, a.rawBody, tc.text)
				if tc.header != "" {
					require.NotEmpty(t, a.header.Get(tc.header), a.rawBody)
				}
				require.NotContains(t, a.rawBody, "written_measurements")
				rows, _ := r.stored(t)
				require.Empty(t, rows)
			}
		})
	}
}

// A measurement refused for its content is dropped; the others are written.
func TestLPWriteDropsAContentRefusalAndWritesTheRest(t *testing.T) {
	for _, path := range lpPaths {
		for _, cause := range []error{ingest.ErrColumnTypeChange, ingest.ErrUnstorableColumnName, ingest.ErrUnstorableColumnValue} {
			t.Run(path.name+"/"+cause.Error(), func(t *testing.T) {
				r := newLPWriteRig(t, path.cfg())
				failLPAt(t, "bbb", cause)
				a := postLP(t, r, threeMeasurements)
				require.Equal(t, http.StatusBadRequest, a.status, a.rawBody)
				msg, _ := a.body["error"].(string)
				require.True(t, strings.HasPrefix(msg, "partial write: 2 points dropped:"), msg)
				require.Contains(t, msg, `"bbb"`)
				require.True(t, strings.HasSuffix(msg, "the other points were written"), msg)
				rows, _ := r.stored(t)
				require.Equal(t, map[string]int64{"aaa": 1, "ccc": 1}, rows)
			})
		}
	}
}

func TestLPWriteContentRefusalWithNothingWrittenIsUnchanged(t *testing.T) {
	for _, path := range lpPaths {
		t.Run(path.name, func(t *testing.T) {
			r := newLPWriteRig(t, path.cfg())
			failLPAt(t, "bbb", ingest.ErrColumnTypeChange)
			a := postLP(t, r, "bbb,h=a v=2 1700000000000000000\n")
			require.Equal(t, http.StatusBadRequest, a.status, a.rawBody)
			require.Equal(t, ingest.ErrColumnTypeChange.Error(), a.body["error"])
		})
	}
}

// A content refusal, then a failure: nothing was buffered, so the failure's
// own answer (here a retryable 503) is still true.
func TestLPWriteContentRefusalThenFailureWithNothingWritten(t *testing.T) {
	r := newLPWriteRig(t, nil)
	typed := lpWriteTyped
	t.Cleanup(func() { lpWriteTyped = typed })
	lpWriteTyped = func(b *ingest.ArrowBuffer, ctx context.Context, db, m string, batch *ingest.TypedColumnBatch, n int) error {
		switch m {
		case "aaa":
			return ingest.ErrColumnTypeChange
		case "bbb":
			return ingest.ErrSchemaChurnExceeded
		}
		return typed(b, ctx, db, m, batch, n)
	}
	a := postLP(t, r, threeMeasurements)
	require.Equal(t, http.StatusServiceUnavailable, a.status, a.rawBody)
	require.Contains(t, a.rawBody, "Write rejected (schema churn)")
}

// cancelAfterCalls is a context whose Err turns to context.Canceled after its
// first n calls, so ArrowBuffer.Write itself stops after its first record.
type cancelAfterCalls struct {
	context.Context
	n     int64
	calls atomic.Int64
}

func (c *cancelAfterCalls) Err() error {
	if c.calls.Add(1) > c.n {
		return context.Canceled
	}
	return nil
}

func TestMsgPackWriteStoppedPartWayNamesWhatWasWritten(t *testing.T) {
	r := newLPWriteRig(t, nil)
	app := fiber.New(appConfig(DefaultServerConfig(), zerolog.Nop()))
	NewMsgPackHandler(zerolog.New(r.log), r.buf, 100<<20).RegisterRoutes(app)
	write := msgpackWrite
	t.Cleanup(func() { msgpackWrite = write })
	msgpackWrite = func(b *ingest.ArrowBuffer, ctx context.Context, db string, records interface{}) error {
		return write(b, &cancelAfterCalls{Context: ctx, n: 1}, db, records)
	}

	columnar := func(m string, n int) map[string]interface{} {
		ts, vs := make([]int64, n), make([]float64, n)
		for i := range ts {
			ts[i], vs[i] = 1700000000000+int64(i), float64(i)
		}
		return map[string]interface{}{"m": m, "columns": map[string]interface{}{"time": ts, "v": vs}}
	}
	body, err := msgpack.Marshal([]interface{}{columnar("aaa", 2), columnar("bbb", 3)})
	require.NoError(t, err)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/write/msgpack", bytes.NewReader(body))
	req.Header.Set("x-arc-database", "d")
	req.Header.Set("Content-Type", "application/msgpack")
	resp, err := app.Test(req, -1)
	require.NoError(t, err)
	raw, _ := io.ReadAll(resp.Body)

	require.Equal(t, http.StatusInternalServerError, resp.StatusCode, string(raw))
	requireNoRetryHeaders(t, resp.Header)
	var got map[string]interface{}
	require.NoError(t, json.Unmarshal(raw, &got))
	require.Equal(t, float64(2), got["rows_written"], string(raw))
	require.Equal(t, []interface{}{"aaa"}, got["written_measurements"])
	require.Equal(t, "bbb", got["stopped_at_measurement"])
	rows, _ := r.stored(t)
	require.Equal(t, map[string]int64{"aaa": 2}, rows)
}

// The stop is checked before the refusal branch: a stop whose cause is a
// refusal must not get the refusal's "nothing written" answer.
func TestMsgPackStopIsAnsweredBeforeItsRefusalCause(t *testing.T) {
	r := newLPWriteRig(t, nil)
	app := fiber.New(appConfig(DefaultServerConfig(), zerolog.Nop()))
	NewMsgPackHandler(zerolog.New(r.log), r.buf, 100<<20).RegisterRoutes(app)
	write := msgpackWrite
	t.Cleanup(func() { msgpackWrite = write })
	msgpackWrite = func(*ingest.ArrowBuffer, context.Context, string, interface{}) error {
		return &ingest.WriteStoppedError{Err: ingest.ErrWALNotDurable, RowsWritten: 1, Written: []string{"aaa"}, StoppedAt: "bbb"}
	}
	body, err := msgpack.Marshal(map[string]interface{}{"m": "aaa", "t": int64(1700000000000), "fields": map[string]interface{}{"v": 1.0}})
	require.NoError(t, err)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/write/msgpack", bytes.NewReader(body))
	req.Header.Set("x-arc-database", "d")
	req.Header.Set("Content-Type", "application/msgpack")
	resp, err := app.Test(req, -1)
	require.NoError(t, err)
	raw, _ := io.ReadAll(resp.Body)
	require.Equal(t, http.StatusInternalServerError, resp.StatusCode, string(raw))
	requireNoRetryHeaders(t, resp.Header)
	require.Contains(t, string(raw), `"stopped_at_measurement":"bbb"`)
}

// Points dropped for good are in the 500's body, so a client resending the
// unwritten part does not take them for parts merely not reached.
func TestLPWriteStoppedReportsDroppedPoints(t *testing.T) {
	r := newLPWriteRig(t, nil)
	failLPAt(t, "bbb", ingest.ErrSchemaChurnExceeded)
	// zzz's only field is named time: the point is dropped at parse.
	a := postLP(t, r, threeMeasurements+"zzz,h=a time=1i 1700000000000000000\n")
	require.Equal(t, http.StatusInternalServerError, a.status, a.rawBody)
	require.Equal(t, float64(1), a.body["points_dropped"], a.rawBody)
	require.Contains(t, a.body["dropped"], `"zzz"`)
}

// A MessagePack batch whose every record was refused for its content is a
// client error, whichever content check refused it.
func TestMsgPackContentRefusalWithNothingWrittenIs400(t *testing.T) {
	for _, cause := range []error{ingest.ErrColumnTypeChange, ingest.ErrUnstorableColumnName, ingest.ErrUnstorableColumnValue} {
		r := newLPWriteRig(t, nil)
		app := fiber.New(appConfig(DefaultServerConfig(), zerolog.Nop()))
		NewMsgPackHandler(zerolog.New(r.log), r.buf, 100<<20).RegisterRoutes(app)
		write := msgpackWrite
		msgpackWrite = func(*ingest.ArrowBuffer, context.Context, string, interface{}) error { return cause }
		body, err := msgpack.Marshal(map[string]interface{}{"m": "aaa", "t": int64(1700000000000), "fields": map[string]interface{}{"v": 1.0}})
		require.NoError(t, err)
		req := httptest.NewRequest(http.MethodPost, "/api/v1/write/msgpack", bytes.NewReader(body))
		req.Header.Set("x-arc-database", "d")
		req.Header.Set("Content-Type", "application/msgpack")
		resp, err := app.Test(req, -1)
		msgpackWrite = write
		require.NoError(t, err)
		raw, _ := io.ReadAll(resp.Body)
		require.Equal(t, http.StatusBadRequest, resp.StatusCode, "%v: %s", cause, raw)
	}
}
