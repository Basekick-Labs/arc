//go:build duckdb_arrow

package api

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Basekick-Labs/msgpack/v6"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/basekick-labs/arc/internal/database"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// #1784: a scale-0 decimal result value outside int64 (DuckDB's sum() of an
// integer column is a HUGEINT, exported as decimal(38,0)) cut the response
// after it had started. It is now a 400 naming the column, the row and the SQL
// cast that returns the value, when it is in the first batch.

// The issue's repro: one row, the sum beyond int64.
const overflowRepro = "SELECT sum(epoch_ns(TIMESTAMP '2026-11-19')) AS s FROM range(6)"

// hugeAt returns n rows of a HUGEINT column v whose value at row bad (-1:
// none) is beyond int64; every other row fits.
func hugeAt(n, bad int) string {
	return fmt.Sprintf("SELECT CASE WHEN i = %d THEN 100000000000000000000::HUGEINT ELSE i::HUGEINT END AS v FROM range(%d) t(i)", bad, n)
}

type overflowRig struct {
	t      *testing.T
	base   string
	client *http.Client
	h      *QueryHandler
	duck   *database.DuckDB
	data   string
}

func newOverflowRig(t *testing.T) *overflowRig {
	t.Helper()
	logger := zerolog.Nop()
	dataDir := t.TempDir()
	backend, err := storage.NewLocalBackend(dataDir, logger)
	require.NoError(t, err)
	t.Cleanup(func() { backend.Close() })
	duck, err := database.New(&database.Config{MemoryLimit: "256MB", ThreadCount: 2, MaxConnections: 4, LocalStorageRoot: dataDir}, logger)
	require.NoError(t, err)
	t.Cleanup(func() { duck.Close() })
	h := NewQueryHandler(duck, backend, logger, 0, 0)
	app := fiber.New(appConfig(DefaultServerConfig(), zerolog.Nop()))
	h.RegisterRoutes(app)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() { _ = app.Listener(ln) }()
	t.Cleanup(func() { _ = app.Shutdown() })
	return &overflowRig{t: t, base: "http://" + ln.Addr().String(), client: &http.Client{Timeout: 30 * time.Second}, h: h, duck: duck, data: dataDir}
}

func (r *overflowRig) post(route, sql string) *http.Response {
	r.t.Helper()
	body, _ := json.Marshal(map[string]string{"sql": sql})
	resp, err := r.client.Post(r.base+route, "application/json", bytes.NewReader(body))
	require.NoError(r.t, err)
	return resp
}

// jsonError reads an error body: JSON, or msgpack from the msgpack route.
func jsonError(t *testing.T, resp *http.Response) (bool, string) {
	t.Helper()
	defer resp.Body.Close()
	var doc struct {
		Success bool   `json:"success" msgpack:"success"`
		Error   string `json:"error" msgpack:"error"`
	}
	raw, _ := io.ReadAll(resp.Body)
	if strings.Contains(resp.Header.Get("Content-Type"), "msgpack") {
		require.NoError(t, msgpack.Unmarshal(raw, &doc), "body: %q", raw)
	} else {
		require.NoError(t, json.Unmarshal(raw, &doc), "body: %s", raw)
	}
	return doc.Success, doc.Error
}

func requireOverflowText(t *testing.T, msg, column string, row int) {
	t.Helper()
	require.True(t, strings.HasPrefix(msg, decimalOverflowPrefix), "message: %q", msg)
	require.Contains(t, msg, fmt.Sprintf(`column "%s"`, column))
	require.Contains(t, msg, fmt.Sprintf("at row %d;", row))
	require.Contains(t, msg, "::DOUBLE")
	require.Contains(t, msg, "::VARCHAR")
	require.NotContains(t, msg, "read_parquet", "the message must not carry SQL (plannings#49)")
	require.NotContains(t, msg, "SELECT")
}

func TestDecimalOverflowFirstBatchIs400(t *testing.T) {
	r := newOverflowRig(t)
	for _, c := range []struct {
		name, sql, column string
		row               int
	}{
		{"sum", overflowRepro, "s", 0},
		{"row 7 of a column", hugeAt(20, 7), "v", 7},
	} {
		t.Run(c.name, func(t *testing.T) {
			for _, route := range []string{"/api/v1/query", "/api/v1/query/arrow", "/api/v1/query/msgpack"} {
				resp := r.post(route, c.sql)
				require.Equal(t, http.StatusBadRequest, resp.StatusCode, route)
				// msgpack answers its errors in its own wire format.
				ok, msg := jsonError(t, resp)
				require.False(t, ok, route)
				requireOverflowText(t, msg, c.column, c.row)
			}
		})
	}
}

// A value only past the first batch still cuts the streaming responses, as
// published; the reason now names it, with its row. msgpack, which buffers
// the result, answers 400.
func TestDecimalOverflowLaterBatch(t *testing.T) {
	r := newOverflowRig(t)
	const bad = 100000
	sql := hugeAt(bad+10, bad)

	t.Run("json", func(t *testing.T) {
		resp := r.post("/api/v1/query", sql)
		defer resp.Body.Close()
		require.Equal(t, http.StatusOK, resp.StatusCode)
		var doc map[string]any
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&doc))
		require.Equal(t, true, doc["truncated"])
		reason, _ := doc["truncation_reason"].(string)
		require.True(t, strings.HasPrefix(reason, decimalOverflowPrefix), "reason %q", reason)
		require.Contains(t, reason, fmt.Sprintf("at row %d;", bad))
		require.Less(t, doc["row_count"].(float64), float64(bad+1))
	})
	t.Run("arrow", func(t *testing.T) {
		resp := r.post("/api/v1/query/arrow", sql)
		defer resp.Body.Close()
		require.Equal(t, http.StatusOK, resp.StatusCode)
		_, _ = io.Copy(io.Discard, resp.Body)
		reason := resp.Trailer.Get(arrowStreamTruncatedTrailer)
		require.True(t, strings.HasPrefix(reason, decimalOverflowPrefix), "trailer %q", reason)
		require.Contains(t, reason, fmt.Sprintf("at row %d;", bad))
	})
	t.Run("msgpack", func(t *testing.T) {
		resp := r.post("/api/v1/query/msgpack", sql)
		require.Equal(t, http.StatusBadRequest, resp.StatusCode)
		_, msg := jsonError(t, resp)
		requireOverflowText(t, msg, "v", bad)
	})
}

// Results that fit are unchanged: a scale-0 decimal is int64, a decimal with a
// scale is float64, and the first batch is neither lost nor sent twice.
func TestDecimalOverflowFittingValuesUnchanged(t *testing.T) {
	r := newOverflowRig(t)
	resp := r.post("/api/v1/query", "SELECT sum(i)::HUGEINT AS s, 1.25::DECIMAL(10,2) AS d, NULL::HUGEINT AS z FROM range(10) t(i)")
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var doc struct {
		Data [][]any `json:"data"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&doc))
	require.Equal(t, [][]any{{float64(45), 1.25, nil}}, doc.Data)

	const n = 5000 // more than one batch
	for _, route := range []string{"/api/v1/query", "/api/v1/query/arrow"} {
		resp := r.post(route, hugeAt(n, -1))
		require.Equal(t, http.StatusOK, resp.StatusCode, route)
		if route == "/api/v1/query/arrow" {
			reader, err := ipc.NewReader(resp.Body)
			require.NoError(t, err)
			rows := 0
			var sum int64
			for reader.Next() {
				rec := reader.RecordBatch()
				require.Equal(t, arrow.PrimitiveTypes.Int64, rec.Schema().Field(0).Type)
				col := rec.Column(0).(*array.Int64)
				for i := 0; i < col.Len(); i++ {
					sum += col.Value(i)
				}
				rows += int(rec.NumRows())
			}
			require.NoError(t, reader.Err())
			reader.Release()
			resp.Body.Close()
			require.Equal(t, n, rows)
			require.Equal(t, int64(n*(n-1)/2), sum, "each row once, in order")
			continue
		}
		var doc struct {
			Data     [][]float64 `json:"data"`
			RowCount int         `json:"row_count"`
		}
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&doc))
		resp.Body.Close()
		require.Equal(t, n, doc.RowCount)
		for i, row := range doc.Data {
			require.Equal(t, float64(i), row[0], "row %d", i)
		}
	}
}

// The first-batch check and the JSON stream look only at the rows the
// governance cap lets through: a value past the cap no longer cuts a capped
// result (it did on JSON) and is not a 400.
func TestDecimalOverflowPastTheGovernanceCap(t *testing.T) {
	r := newOverflowRig(t)
	const capRows, bad = 5, 9
	reader, conn, err := r.duck.ArrowQueryContext(context.Background(), hugeAt(20, bad))
	require.NoError(t, err)
	defer conn.Close()
	peeked, overflow := peekDecimalOverflow(reader, capRows)
	require.Nil(t, overflow, "the bad row is past the cap")

	var buf bytes.Buffer
	w := bufio.NewWriter(&buf)
	rows, streamErr := streamArrowJSON(context.Background(), w, peeked, capRows, nil, time.Now(), "ts")
	require.NoError(t, streamErr)
	require.Equal(t, capRows, rows)
	peeked.Release()

	// A cap reached on a batch boundary, with the bad value in the next batch.
	reader, conn2, err := r.duck.ArrowQueryContext(context.Background(), hugeAt(4000, 3000))
	require.NoError(t, err)
	defer conn2.Close()
	require.True(t, reader.Next())
	first := int(reader.RecordBatch().NumRows())
	require.Less(t, first, 3000, "the bad value must be past the first batch")
	replay := &peekedRecordReader{RecordReader: reader, pending: true, first: true}
	buf.Reset()
	rows, streamErr = streamArrowJSON(context.Background(), w, replay, first, nil, time.Now(), "ts")
	require.NoError(t, streamErr, "a batch past the cap is never sent, so its values do not matter")
	require.Equal(t, first, rows)
	reader.Release()
}

// stubReader is a reader whose first Next fails, as a deadline does.
type stubReader struct {
	array.RecordReader
	schema *arrow.Schema
	err    error
}

func (s *stubReader) Schema() *arrow.Schema { return s.schema }
func (s *stubReader) Next() bool            { return false }
func (s *stubReader) Err() error            { return s.err }

// Anything but a value outside int64 is left for the stream, unchanged: the
// first Next's result and error are replayed.
func TestDecimalOverflowPeekReplaysOtherOutcomes(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{{Name: "v", Type: &arrow.Decimal128Type{Precision: 38, Scale: 0}, Nullable: true}}, nil)
	deadline := fmt.Errorf("reading the result: %w", context.DeadlineExceeded)
	peeked, overflow := peekDecimalOverflow(&stubReader{schema: schema, err: deadline}, 0)
	require.Nil(t, overflow)
	require.False(t, peeked.Next())
	require.True(t, errors.Is(peeked.Err(), context.DeadlineExceeded))

	// Narrow decimals are not checked at all.
	narrow := arrow.NewSchema([]arrow.Field{{Name: "v", Type: &arrow.Decimal128Type{Precision: 18, Scale: 0}}}, nil)
	stub := &stubReader{schema: narrow}
	got, overflow := peekDecimalOverflow(stub, 0)
	require.Nil(t, overflow)
	require.Same(t, stub, got, "no peek without a wide scale-0 decimal")
}

// The bridge answers Flight SQL: a first-batch overflow is InvalidArgument
// with the message; one in a later batch ends the stream with it too, where
// it used to be a generic Internal error.
func TestDecimalOverflowFlightSQL(t *testing.T) {
	logger := zerolog.Nop()
	dataDir := t.TempDir()
	backend, err := storage.NewLocalBackend(dataDir, logger)
	require.NoError(t, err)
	t.Cleanup(func() { backend.Close() })
	duck, err := database.New(&database.Config{MemoryLimit: "256MB", ThreadCount: 2, MaxConnections: 4, LocalStorageRoot: dataDir}, logger)
	require.NoError(t, err)
	t.Cleanup(func() { duck.Close() })
	h := NewQueryHandler(duck, backend, logger, 0, 0)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := ln.Addr().(*net.TCPAddr).Port
	require.NoError(t, ln.Close())
	server, err := NewFlightSQLServer(h, FlightSQLServerConfig{Host: "127.0.0.1", Port: port}, logger)
	require.NoError(t, err)
	require.NoError(t, server.Start())
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
	})
	client, err := flightsql.NewClient(fmt.Sprintf("127.0.0.1:%d", port), nil, nil, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { client.Client.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	run := func(sql string) error {
		info, err := client.Execute(ctx, sql)
		if err != nil {
			return err
		}
		reader, err := client.DoGet(ctx, info.Endpoint[0].Ticket)
		if err != nil {
			return err
		}
		defer reader.Release()
		for reader.Next() {
		}
		return reader.Err()
	}

	err = run(overflowRepro)
	require.Equal(t, codes.InvalidArgument, status.Code(err), "%v", err)
	requireOverflowText(t, status.Convert(err).Message(), "s", 0)

	const bad = 100000
	err = run(hugeAt(bad+10, bad))
	require.Equal(t, codes.InvalidArgument, status.Code(err), "%v", err)
	msg := status.Convert(err).Message()
	require.True(t, strings.HasPrefix(msg, decimalOverflowPrefix), "%q", msg)
	require.Contains(t, msg, fmt.Sprintf("at row %d;", bad))

	require.NoError(t, run(hugeAt(10, -1)))
}

func TestFlightTruncationError(t *testing.T) {
	err := flightTruncationError("decimal cast failed at row 3: invalid")
	require.Equal(t, codes.Internal, status.Code(err))
	require.Equal(t, "Arc query stream was truncated", status.Convert(err).Message())
	// A reason that only contains the prefix is not this error.
	err = flightTruncationError("arrow reader error after 3 rows: " + decimalOverflowPrefix)
	require.Equal(t, codes.Internal, status.Code(err))
	err = flightTruncationError(decimalOverflowPrefix + `: column "..." (DECIMAL(38,0)/HUGEINT) at row 9; cast it`)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

// A column stored as DECIMAL(38,0) (ingest.decimal_columns, #276) holding a
// value beyond int64 fails the same way on a plain SELECT, on the SQL route,
// the measurement route and the profiled JSON path.
func TestDecimalOverflowStoredColumn(t *testing.T) {
	r := newOverflowRig(t)
	dir := filepath.Join(r.data, "db1", "big", "2026", "10", "20", "08")
	require.NoError(t, os.MkdirAll(dir, 0o755))
	file := filepath.Join(dir, "big_1.parquet")
	_, err := r.duck.DB().Exec(fmt.Sprintf(`COPY (SELECT TIMESTAMP '2026-10-20 08:00:00' + INTERVAL (i) SECOND AS time,
		CASE WHEN i = 3 THEN 12345678901234567890123::DECIMAL(38,0) ELSE i::DECIMAL(38,0) END AS v
		FROM range(10) t(i)) TO '%s' (FORMAT parquet)`, file))
	require.NoError(t, err)

	resp := r.post("/api/v1/query", "SELECT v FROM db1.big ORDER BY time")
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	_, msg := jsonError(t, resp)
	requireOverflowText(t, msg, "v", 3)

	body, _ := json.Marshal(map[string]string{"sql": "SELECT v FROM db1.big ORDER BY time"})
	req, _ := http.NewRequest(http.MethodPost, r.base+"/api/v1/query", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-arc-profile", "true")
	resp, err = r.client.Do(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode, "profiled path")
	_, msg = jsonError(t, resp)
	requireOverflowText(t, msg, "v", 3)

	resp, err = r.client.Get(r.base + "/api/v1/query/big?" + url.Values{"database": {"db1"}, "limit": {"100"}}.Encode())
	require.NoError(t, err)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode, "measurement route")
	_, msg = jsonError(t, resp)
	require.True(t, strings.HasPrefix(msg, decimalOverflowPrefix), "%q", msg)

	// The same column read with the cast the message suggests.
	resp = r.post("/api/v1/query", "SELECT v::VARCHAR AS v FROM db1.big ORDER BY time")
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var doc struct {
		Data [][]string `json:"data"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&doc))
	require.Equal(t, "12345678901234567890123", doc.Data[3][0])
}

// The range check reads values in place: a null slot is skipped whatever
// bytes lie under it, and a sliced array is read from its own offset.
func TestFirstDecimalOverflowNullsAndOffsets(t *testing.T) {
	dt := &arrow.Decimal128Type{Precision: 38, Scale: 0}
	huge := decimal128.FromU64(math.MaxUint64) // beyond int64
	b := array.NewDecimal128Builder(memory.DefaultAllocator, dt)
	defer b.Release()
	b.AppendValues([]decimal128.Num{decimal128.FromI64(1), huge, decimal128.FromI64(-5), huge},
		[]bool{true, false, true, true})
	arr := b.NewArray()
	defer arr.Release()
	schema := arrow.NewSchema([]arrow.Field{{Name: "v", Type: dt, Nullable: true}}, nil)
	rec := array.NewRecord(schema, []arrow.Array{arr}, int64(arr.Len()))
	defer rec.Release()

	got, found := firstDecimalOverflow(rec, rec.NumRows())
	require.True(t, found)
	require.EqualValues(t, 3, got.Row, "row 1 is null over an out-of-range value and is skipped")

	_, found = firstDecimalOverflow(rec, 3)
	require.False(t, found, "the limit stops before row 3")

	sliced := rec.NewSlice(1, 3) // rows 1 and 2: the null and -5
	defer sliced.Release()
	_, found = firstDecimalOverflow(sliced, sliced.NumRows())
	require.False(t, found)

	min := decimal128.FromI64(math.MinInt64)
	b.AppendValues([]decimal128.Num{min}, nil)
	edge := b.NewArray()
	defer edge.Release()
	edgeRec := array.NewRecord(schema, []arrow.Array{edge}, 1)
	defer edgeRec.Release()
	_, found = firstDecimalOverflow(edgeRec, 1)
	require.False(t, found, "math.MinInt64 fits")
}

// Arrow and msgpack slice to the cap before they cast, so a value past the
// cap never fails them either.
func TestDecimalOverflowPastTheCapArrowAndMsgpack(t *testing.T) {
	r := newOverflowRig(t)
	const capRows, bad = 5, 9
	reader, conn, err := r.duck.ArrowQueryContext(context.Background(), hugeAt(20, bad))
	require.NoError(t, err)
	defer conn.Close()
	peeked, overflow := peekDecimalOverflow(reader, capRows)
	require.Nil(t, overflow)
	info := normalizeDecimalSchema(peeked.Schema())
	var buf bytes.Buffer
	w := bufio.NewWriter(&buf)
	rows, err := streamArrowIPC(context.Background(), w, peeked, info.schema, info, false, "", capRows, zerolog.Nop())
	require.NoError(t, err)
	require.EqualValues(t, capRows, rows)
	peeked.Release()

	reader, conn2, err := r.duck.ArrowQueryContext(context.Background(), hugeAt(20, bad))
	require.NoError(t, err)
	defer conn2.Close()
	batches, n, err := drainArrowBatches(context.Background(), reader, capRows, normalizeDecimalSchema(reader.Schema()))
	require.NoError(t, err)
	require.Equal(t, capRows, n)
	for _, b := range batches {
		b.Release()
	}
	reader.Release()
}
