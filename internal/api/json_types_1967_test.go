//go:build duckdb_arrow

package api

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/basekick-labs/arc/internal/database"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// #1967: the JSON query response carries "types", the names the msgpack
// response carries for the same query. The spellings are pinned once, by
// TestArrowTypeName_Golden; these tests only compare the two wires and check
// that each JSON value is the kind its name promises.

func jsonTypesRig(t *testing.T) *fiber.App {
	t.Helper()
	logger := zerolog.Nop()
	root := t.TempDir()
	backend, err := storage.NewLocalBackend(root, logger)
	require.NoError(t, err)
	t.Cleanup(func() { _ = backend.Close() })
	duck, err := database.New(&database.Config{MemoryLimit: "256MB", ThreadCount: 1, MaxConnections: 2, LocalStorageRoot: root}, logger)
	require.NoError(t, err)
	t.Cleanup(func() { _ = duck.Close() })

	full := filepath.Join(root, "test/cpu/2026/11/26/00/cpu_a.parquet")
	require.NoError(t, os.MkdirAll(filepath.Dir(full), 0o700))
	_, err = duck.DB().Exec(fmt.Sprintf(`COPY (SELECT TIMESTAMP '2026-11-26 00:00:00' + to_seconds(i) AS time,
		'h' || CAST(i AS VARCHAR) AS host, i::BIGINT AS n, i * 1.5 AS usage, i %% 2 = 0 AS up
		FROM range(5) t(i)) TO '%s' (FORMAT PARQUET)`, full))
	require.NoError(t, err)

	h := NewQueryHandler(duck, backend, logger, 0, 0)
	app := fiber.New(appConfig(DefaultServerConfig(), logger))
	h.RegisterRoutes(app)
	return app
}

// jtJSON runs a JSON request and returns the status, the decoded envelope
// and the raw body.
func jtJSON(t *testing.T, app *fiber.App, method, target, sql string) (int, map[string]any, string) {
	t.Helper()
	var rd io.Reader
	if sql != "" {
		b, _ := json.Marshal(map[string]string{"sql": sql})
		rd = bytes.NewReader(b)
	}
	req := httptest.NewRequest(method, target, rd)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-arc-database", "test")
	resp, err := app.Test(req, -1)
	require.NoError(t, err)
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var out map[string]any
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	_ = d.Decode(&out)
	return resp.StatusCode, out, string(raw)
}

func jtMsgpack(t *testing.T, app *fiber.App, sql string) map[string]any {
	t.Helper()
	b, _ := json.Marshal(map[string]string{"sql": sql})
	req := httptest.NewRequest("POST", "/api/v1/query/msgpack", bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-arc-database", "test")
	resp, err := app.Test(req, -1)
	require.NoError(t, err)
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	require.Equal(t, 200, resp.StatusCode, "msgpack %q: %x", sql, raw)
	return decodeMsgpack(t, raw)
}

func jtStrings(t *testing.T, v any, what string) []string {
	t.Helper()
	arr, ok := v.([]any)
	require.True(t, ok, "%s is not an array: %#v", what, v)
	out := make([]string, len(arr))
	for i, e := range arr {
		s, ok := e.(string)
		require.True(t, ok, "%s[%d] is not a string: %#v", what, i, e)
		out[i] = s
	}
	return out
}

// jsonKindOK reports whether a non-null JSON value is what the wire type
// name promises. This is what catches the JSON encoder (appendValue) and the
// msgpack one (encodeColumn) drifting apart: the names are computed once for
// both, so only the values can disagree.
func jsonKindOK(name string, v any) (bool, string) {
	switch name {
	case wireTypeInt8, wireTypeInt16, wireTypeInt32, wireTypeInt64,
		wireTypeUint8, wireTypeUint16, wireTypeUint32, wireTypeUint64:
		n, ok := v.(json.Number)
		if !ok {
			return false, "an integer number"
		}
		if strings.ContainsAny(n.String(), ".eE") {
			return false, "an integer number"
		}
		return true, ""
	case wireTypeFloat32, wireTypeFloat64:
		_, ok := v.(json.Number)
		return ok, "a number"
	case wireTypeBool:
		_, ok := v.(bool)
		return ok, "a bool"
	case wireTypeNull:
		return false, "null"
	case "date32":
		s, ok := v.(string)
		if !ok {
			return false, "an RFC 3339 string"
		}
		_, err := time.Parse(time.RFC3339Nano, s)
		return err == nil, "an RFC 3339 string"
	}
	if strings.HasPrefix(name, "timestamp[") {
		s, ok := v.(string)
		if !ok {
			return false, "an RFC 3339 string"
		}
		_, err := time.Parse(time.RFC3339Nano, s)
		return err == nil, "an RFC 3339 string"
	}
	if strings.HasPrefix(name, "decimal(") {
		// normalizeDecimalSchema runs before the names are taken, so a
		// decimal name on the JSON wire is itself the bug.
		return false, "no decimal name at all (decimals are normalised)"
	}
	// Strings on the wire. Listed, not defaulted: a name this test does not
	// know (a new typed name, say) must fail here until its kind is added,
	// or the drift this test exists for passes as "a string".
	switch {
	case name == wireTypeUTF8, name == "large_utf8", name == wireTypeBinary, name == "large_binary",
		name == wireTypeStringEncoded, name == wireTypeList, name == wireTypeStruct, name == wireTypeMap,
		strings.HasPrefix(name, wireTypeUnknownPrefix):
		_, ok := v.(string)
		return ok, "a string"
	}
	return false, "a name this test knows (add its JSON kind to jsonKindOK)"
}

func TestJSONTypesMatchMsgpack_1967(t *testing.T) {
	app := jsonTypesRig(t)
	for _, tc := range []struct{ name, expr string }{
		{"tinyint", "42::TINYINT"},
		{"smallint", "42::SMALLINT"},
		{"integer", "42::INTEGER"},
		{"bigint", "42::BIGINT"},
		{"utinyint", "42::UTINYINT"},
		{"usmallint", "42::USMALLINT"},
		{"uinteger", "42::UINTEGER"},
		{"ubigint", "18446744073709551615::UBIGINT"},
		{"float", "1.5::FLOAT"},
		{"double", "1.5::DOUBLE"},
		{"boolean", "true"},
		{"varchar", "'x'"},
		{"blob", "'\\xAA\\xBB'::BLOB"},
		{"uuid", "'4ac7a9e9-607c-4c8a-84f3-843f0191e3fd'::UUID"},
		{"timestamp", "TIMESTAMP '2026-11-26 01:02:03.456789'"},
		{"timestamptz", "TIMESTAMPTZ '2026-11-26 01:02:03+02'"},
		{"timestamp_ns", "'2026-11-26 01:02:03.123456789'::TIMESTAMP_NS"},
		{"timestamp_ms", "'2026-11-26 01:02:03.123'::TIMESTAMP_MS"},
		{"timestamp_s", "'2026-11-26 01:02:03'::TIMESTAMP_S"},
		{"date", "DATE '2026-11-26'"},
		{"time", "TIME '01:02:03'"},
		{"interval", "INTERVAL 90 MINUTE"},
		{"hugeint", "123::HUGEINT"},
		{"sum_decimal", "SUM(n)"},
		{"sum_scaled_decimal", "SUM(usage::DECIMAL(10,2))"},
		{"decimal_scaled", "1.25::DECIMAL(10,2)"},
		{"list", "[1, 2, 3]"},
		{"struct", "{'a': 1, 'b': 'x'}"},
		{"map", "MAP {'k': 1}"},
		{"enum", "'b'::ENUM('a', 'b')"},
		{"null", "NULL"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sql := "SELECT " + tc.expr + " AS v FROM cpu LIMIT 1"
			if strings.HasPrefix(tc.expr, "SUM(") {
				sql = "SELECT " + tc.expr + " AS v FROM cpu"
			}
			code, out, raw := jtJSON(t, app, "POST", "/api/v1/query", sql)
			require.Equal(t, 200, code, raw)
			cols := jtStrings(t, out["columns"], "columns")
			types := jtStrings(t, out["types"], "types")
			require.Len(t, types, len(cols))

			mp := jtMsgpack(t, app, sql)
			require.Equal(t, jtStrings(t, mp["types"], "msgpack types"), types,
				"JSON and msgpack name the same column differently")

			data, _ := out["data"].([]any)
			require.Len(t, data, 1, raw)
			v := data[0].([]any)[0]
			t.Logf("%s -> %s", tc.expr, types[0])
			if tc.expr == "NULL" {
				// DuckDB types a bare NULL literal (int32 today); the
				// value is null whatever the name.
				require.Nil(t, v)
				return
			}
			require.NotNil(t, v, "a non-null fixture came back null: %s", raw)
			ok, want := jsonKindOK(types[0], v)
			require.True(t, ok, "type %q promises %s, the JSON value is %#v", types[0], want, v)
		})
	}
}

// The field sits right after "columns", as the issue's envelope shows.
func TestJSONTypesEnvelopeOrder_1967(t *testing.T) {
	app := jsonTypesRig(t)
	code, _, raw := jtJSON(t, app, "POST", "/api/v1/query", "SELECT host, n FROM cpu LIMIT 1")
	require.Equal(t, 200, code, raw)
	require.True(t, strings.HasPrefix(raw, `{"success":true,"columns":["host","n"],"types":["utf8","int64"],"data":[`), raw)
}

// Same data, both routes: the measurement route carries the same names.
func TestJSONTypesMeasurementRoute_1967(t *testing.T) {
	app := jsonTypesRig(t)
	byName := func(out map[string]any) map[string]string {
		cols := jtStrings(t, out["columns"], "columns")
		types := jtStrings(t, out["types"], "types")
		require.Len(t, types, len(cols))
		m := make(map[string]string, len(cols))
		for i, c := range cols {
			m[c] = types[i]
		}
		return m
	}
	code, q, raw := jtJSON(t, app, "POST", "/api/v1/query", "SELECT * FROM cpu")
	require.Equal(t, 200, code, raw)
	code, m, raw := jtJSON(t, app, "GET", "/api/v1/query/cpu?database=test", "")
	require.Equal(t, 200, code, raw)
	require.Equal(t, byName(q), byName(m))
	require.Len(t, byName(m), 5)
}

// A result with a schema and no rows names its columns; a measurement with no
// files has neither (and says so with [], not by leaving the key out).
func TestJSONTypesEmptyAnswers_1967(t *testing.T) {
	app := jsonTypesRig(t)

	code, out, raw := jtJSON(t, app, "POST", "/api/v1/query", "SELECT host, n FROM cpu WHERE false")
	require.Equal(t, 200, code, raw)
	require.Equal(t, []string{wireTypeUTF8, wireTypeInt64}, jtStrings(t, out["types"], "types"))
	require.Equal(t, []any{}, out["data"])

	for _, tc := range []struct{ method, target, sql string }{
		{"POST", "/api/v1/query", "SELECT * FROM nosuch"},
		{"GET", "/api/v1/query/nosuch?database=test", ""},
	} {
		code, out, raw := jtJSON(t, app, tc.method, tc.target, tc.sql)
		require.Equal(t, 200, code, raw)
		require.Contains(t, raw, `"columns":[],"types":[],"data":[]`, "%s %s", tc.method, tc.target)
		require.Equal(t, []any{}, out["types"])
	}
}

// An error answer has no "types" key (omitzero leaves nil out).
func TestJSONTypesAbsentOnError_1967(t *testing.T) {
	app := jsonTypesRig(t)
	code, out, raw := jtJSON(t, app, "POST", "/api/v1/query", "SELECT nope FROM cpu")
	require.GreaterOrEqual(t, code, 400, raw)
	_, has := out["types"]
	require.False(t, has, raw)
}

// SHOW answers in JSON carry the names they already sent in msgpack.
func TestJSONTypesShow_1967(t *testing.T) {
	app := jsonTypesRig(t)
	for _, sql := range []string{"SHOW DATABASES", "SHOW TABLES"} {
		code, out, raw := jtJSON(t, app, "POST", "/api/v1/query", sql)
		require.Equal(t, 200, code, raw)
		types := jtStrings(t, out["types"], sql+" types")
		require.Len(t, types, len(jtStrings(t, out["columns"], sql+" columns")))
		require.Equal(t, jtStrings(t, jtMsgpack(t, app, sql)["types"], sql+" msgpack types"), types, sql)
	}
}

// The database/sql fallback (taken only when the driver declines Arrow) sends
// no "types": its colType mapping collapses types (every integer to int64,
// DECIMAL to float64, HUGEINT to a string), so any name there would either
// misreport the payload or disagree with the Arrow path for the same SQL.
func TestJSONTypesAbsentOnSQLFallback_1967(t *testing.T) {
	var buf bytes.Buffer
	w := bufio.NewWriter(&buf)
	_, err := streamTypedJSON(context.Background(), w, []string{"a"}, []colType{colInt64},
		&fakeRowScanner{}, 0, nil, time.Now(), "ts")
	require.NoError(t, err)
	require.NoError(t, w.Flush())
	var out map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &out), buf.String())
	_, has := out["types"]
	require.False(t, has, buf.String())
}

type fakeRowScanner struct{}

func (fakeRowScanner) Next() bool             { return false }
func (fakeRowScanner) Scan(dest ...any) error { return nil }
func (fakeRowScanner) Err() error             { return nil }

// The JSON encoder, per Arrow type, including the ones DuckDB SQL does not
// produce (DATE64, FLOAT16, LARGE_*, FIXED_SIZE_BINARY, DURATION, …): one
// value through jsonValueEncoder.appendValue must be the kind its shared
// wire name promises. The route test above covers what DuckDB emits; this
// covers the rest of the list TestArrowTypeName_NoTypedNameWithoutEncoderCase
// walks for the msgpack side.
func TestJSONKindPerArrowType_1967(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)
	for _, tc := range []struct {
		dt  arrow.DataType
		val string // the JSON array FromJSON reads: one non-null value
	}{
		{arrow.FixedWidthTypes.Boolean, `[true]`},
		{arrow.PrimitiveTypes.Int8, `[1]`}, {arrow.PrimitiveTypes.Int16, `[1]`},
		{arrow.PrimitiveTypes.Int32, `[1]`}, {arrow.PrimitiveTypes.Int64, `[1]`},
		{arrow.PrimitiveTypes.Uint8, `[1]`}, {arrow.PrimitiveTypes.Uint16, `[1]`},
		{arrow.PrimitiveTypes.Uint32, `[1]`}, {arrow.PrimitiveTypes.Uint64, `[18446744073709551615]`},
		{arrow.PrimitiveTypes.Float32, `[1.5]`}, {arrow.PrimitiveTypes.Float64, `[1.5]`},
		{arrow.FixedWidthTypes.Float16, `[1.5]`},
		{arrow.BinaryTypes.String, `["x"]`}, {arrow.BinaryTypes.LargeString, `["x"]`},
		{arrow.BinaryTypes.Binary, `["qrs="]`}, {arrow.BinaryTypes.LargeBinary, `["qrs="]`},
		{&arrow.FixedSizeBinaryType{ByteWidth: 2}, `["qrs="]`},
		{arrow.FixedWidthTypes.Date32, `["2026-11-26"]`}, {arrow.FixedWidthTypes.Date64, `["2026-11-26"]`},
		{arrow.FixedWidthTypes.Timestamp_s, `["2026-11-26T01:02:03Z"]`},
		{arrow.FixedWidthTypes.Timestamp_ms, `["2026-11-26T01:02:03Z"]`},
		{arrow.FixedWidthTypes.Timestamp_us, `["2026-11-26T01:02:03Z"]`},
		{arrow.FixedWidthTypes.Timestamp_ns, `["2026-11-26T01:02:03Z"]`},
		{arrow.FixedWidthTypes.Time32s, `["01:02:03"]`}, {arrow.FixedWidthTypes.Time64ns, `["01:02:03"]`},
		{arrow.FixedWidthTypes.Duration_ns, `[5]`},
		{arrow.FixedWidthTypes.MonthInterval, `[{"months": 2}]`},
		{arrow.FixedWidthTypes.DayTimeInterval, `[{"days": 1, "milliseconds": 2}]`},
		{arrow.FixedWidthTypes.MonthDayNanoInterval, `[{"months": 1, "days": 1, "nanoseconds": 2}]`},
		{arrow.ListOf(arrow.PrimitiveTypes.Int32), `[[1, 2]]`},
		{arrow.StructOf(arrow.Field{Name: "a", Type: arrow.PrimitiveTypes.Int32}), `[{"a": 1}]`},
		{arrow.MapOf(arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int32), `[[{"key": "k", "value": 1}]]`},
	} {
		name := arrowTypeName(tc.dt)
		arr, _, err := array.FromJSON(mem, tc.dt, strings.NewReader(tc.val))
		require.NoError(t, err, "%s", tc.dt)
		require.False(t, arr.IsNull(0), "%s", tc.dt)
		enc := &jsonValueEncoder{}
		raw := enc.appendValue(nil, arr, 0)
		arr.Release()
		var v any
		d := json.NewDecoder(bytes.NewReader(raw))
		d.UseNumber()
		require.NoError(t, d.Decode(&v), "%s: %s", tc.dt, raw)
		ok, want := jsonKindOK(name, v)
		require.True(t, ok, "%s: name %q promises %s, appendValue wrote %s", tc.dt, name, want, raw)
	}
}
