//go:build duckdb_arrow

package api

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/basekick-labs/arc/internal/database"
	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/rs/zerolog"
	"github.com/vmihailenco/msgpack/v5"
)

// Tests for #724. An Enterprise governance row cap truncated a result with no
// error, no header, no field and no log entry, so a capped response was
// byte-identical to a complete one on every wire format and a dashboard
// quietly cut at 10,000 rows drew conclusions from truncated data.
//
// The signal deliberately means "this result reached the cap and may be
// incomplete", not "rows were definitely dropped": see rowCapReached.

func TestRowCapReached(t *testing.T) {
	tests := []struct {
		name     string
		maxRows  int
		rowCount int64
		want     bool
	}{
		{"no policy cap, empty result", 0, 0, false},
		{"no policy cap, large result", 0, 1 << 20, false},
		{"negative cap is treated as no cap", -1, 100, false},
		{"below the cap", 10, 9, false},
		{"empty result under a cap", 10, 0, false},
		{"exactly at the cap", 10, 10, true},
		{"past the cap", 10, 11, true},
		{"cap of one", 1, 1, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := rowCapReached(tt.maxRows, tt.rowCount); got != tt.want {
				t.Errorf("rowCapReached(%d, %d) = %v, want %v", tt.maxRows, tt.rowCount, got, tt.want)
			}
		})
	}
}

// jsonCapFields pulls the two #724 keys out of an encoded JSON envelope.
func jsonCapFields(t *testing.T, body []byte) (capped bool, rowCap float64, hasCap bool) {
	t.Helper()
	var env map[string]interface{}
	if err := json.Unmarshal(body, &env); err != nil {
		t.Fatalf("envelope is not valid JSON: %v\n%s", err, body)
	}
	if v, ok := env["rows_capped"]; ok {
		b, isBool := v.(bool)
		if !isBool {
			t.Fatalf("rows_capped is %T, want bool", v)
		}
		capped = b
	}
	if v, ok := env["row_cap"]; ok {
		hasCap = true
		rowCap = v.(float64)
	}
	return capped, rowCap, hasCap
}

func TestStreamTypedJSONMarksCappedResult(t *testing.T) {
	columns := []string{"id"}
	colTypes := []colType{colInt64}
	rows := func(n int) [][]interface{} {
		out := make([][]interface{}, n)
		for i := range out {
			out[i] = []interface{}{int64(i)}
		}
		return out
	}

	t.Run("a capped result carries both keys", func(t *testing.T) {
		body, rowCount := streamToBytes(columns, colTypes, &mockRowScanner{rows: rows(25)}, 10, nil)
		if rowCount != 10 {
			t.Fatalf("rowCount = %d, want 10", rowCount)
		}
		capped, rowCap, hasCap := jsonCapFields(t, body)
		if !capped {
			t.Errorf("rows_capped missing or false on a capped result: %s", body)
		}
		if !hasCap || rowCap != 10 {
			t.Errorf("row_cap = %v (present=%v), want 10", rowCap, hasCap)
		}
	})

	t.Run("an uncapped result carries neither key", func(t *testing.T) {
		body, _ := streamToBytes(columns, colTypes, &mockRowScanner{rows: rows(25)}, 0, nil)
		if bytes.Contains(body, []byte("rows_capped")) || bytes.Contains(body, []byte("row_cap")) {
			t.Errorf("uncapped response must be unchanged on the wire, got: %s", body)
		}
	})

	t.Run("a result short of the cap carries neither key", func(t *testing.T) {
		body, rowCount := streamToBytes(columns, colTypes, &mockRowScanner{rows: rows(3)}, 10, nil)
		if rowCount != 3 {
			t.Fatalf("rowCount = %d, want 3", rowCount)
		}
		if bytes.Contains(body, []byte("rows_capped")) {
			t.Errorf("a result under the cap must not be marked, got: %s", body)
		}
	})

	t.Run("the marked envelope is still valid JSON alongside a profile", func(t *testing.T) {
		body, _ := streamToBytes(columns, colTypes, &mockRowScanner{rows: rows(25)}, 10,
			&database.QueryProfile{})
		var env map[string]interface{}
		if err := json.Unmarshal(body, &env); err != nil {
			t.Fatalf("profiled capped envelope is not valid JSON: %v\n%s", err, body)
		}
		if _, ok := env["profile"]; !ok {
			t.Error("profile key went missing from a capped envelope")
		}
	})
}

func TestStreamArrowJSONMarksCappedResult(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)

	newReader := func(batches, perBatch int) *simpleRecordReader {
		recs := make([]arrow.Record, batches)
		for b := range recs {
			rows := make([][]interface{}, perBatch)
			for r := range rows {
				rows[r] = []interface{}{int64(b*perBatch + r)}
			}
			recs[b] = buildArrowBatch(alloc, schema, rows)
		}
		return newSimpleRecordReader(schema, recs)
	}

	t.Run("a capped result carries both keys", func(t *testing.T) {
		reader := newReader(3, 10)
		body, rowCount := arrowStreamToBytes(reader, 12, nil)
		reader.Release()
		if rowCount != 12 {
			t.Fatalf("rowCount = %d, want 12", rowCount)
		}
		capped, rowCap, hasCap := jsonCapFields(t, body)
		if !capped {
			t.Errorf("rows_capped missing or false on a capped result: %s", body)
		}
		if !hasCap || rowCap != 12 {
			t.Errorf("row_cap = %v (present=%v), want 12", rowCap, hasCap)
		}
	})

	t.Run("an uncapped result carries neither key", func(t *testing.T) {
		reader := newReader(3, 10)
		body, rowCount := arrowStreamToBytes(reader, 0, nil)
		reader.Release()
		if rowCount != 30 {
			t.Fatalf("rowCount = %d, want 30", rowCount)
		}
		if bytes.Contains(body, []byte("rows_capped")) || bytes.Contains(body, []byte("row_cap")) {
			t.Errorf("uncapped response must be unchanged on the wire, got: %s", body)
		}
	})

	t.Run("a cap larger than the result marks nothing", func(t *testing.T) {
		reader := newReader(2, 5)
		body, rowCount := arrowStreamToBytes(reader, 100, nil)
		reader.Release()
		if rowCount != 10 {
			t.Fatalf("rowCount = %d, want 10", rowCount)
		}
		if bytes.Contains(body, []byte("rows_capped")) {
			t.Errorf("a result under the cap must not be marked, got: %s", body)
		}
	})
}

func TestMsgPackEnvelopeMarksCappedResult(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)

	newReader := func(batches, perBatch int) *simpleRecordReader {
		recs := make([]arrow.Record, batches)
		for b := range recs {
			rows := make([][]interface{}, perBatch)
			for r := range rows {
				rows[r] = []interface{}{int64(b*perBatch + r)}
			}
			recs[b] = buildArrowBatch(alloc, schema, rows)
		}
		return newSimpleRecordReader(schema, recs)
	}

	decode := func(t *testing.T, body []byte) map[string]interface{} {
		t.Helper()
		var env map[string]interface{}
		if err := msgpack.Unmarshal(body, &env); err != nil {
			t.Fatalf("envelope is not decodable msgpack: %v", err)
		}
		return env
	}

	t.Run("a capped result carries both keys and still decodes", func(t *testing.T) {
		reader := newReader(3, 10)
		body, rowCount := msgpackStreamToBytes(reader, 12)
		reader.Release()
		if rowCount != 12 {
			t.Fatalf("rowCount = %d, want 12", rowCount)
		}
		env := decode(t, body)
		// A wrong EncodeMapLen does not drop a field, it leaves trailing
		// bytes that decode as a second object, so the key count is the
		// assertion that catches it.
		if len(env) != 9 {
			t.Fatalf("capped envelope has %d keys, want 9: %v", len(env), env)
		}
		if env["rows_capped"] != true {
			t.Errorf("rows_capped = %v, want true", env["rows_capped"])
		}
		// msgpack picks the narrowest integer encoding, so the decoded Go
		// type depends on the value; compare numerically, not by type.
		if got := asInt64(t, env["row_cap"]); got != 12 {
			t.Errorf("row_cap = %d, want 12", got)
		}
	})

	t.Run("an uncapped result keeps the historical seven-key shape", func(t *testing.T) {
		reader := newReader(3, 10)
		body, _ := msgpackStreamToBytes(reader, 0)
		reader.Release()
		env := decode(t, body)
		if len(env) != 7 {
			t.Fatalf("uncapped envelope has %d keys, want 7: %v", len(env), env)
		}
		if _, ok := env["rows_capped"]; ok {
			t.Error("uncapped response must not carry rows_capped")
		}
	})

	t.Run("a result short of the cap is not marked", func(t *testing.T) {
		reader := newReader(2, 5)
		body, _ := msgpackStreamToBytes(reader, 100)
		reader.Release()
		env := decode(t, body)
		if len(env) != 7 {
			t.Fatalf("under-cap envelope has %d keys, want 7: %v", len(env), env)
		}
	})
}

// TestMsgPackMapLenMatchesEncodedKeys covers all four combinations of the two
// optional key groups. EncodeMapLen is written before the pairs, so an
// arithmetic slip there does not drop a field: the decoder stops early and the
// remaining bytes parse as a second top-level object, or the decode fails
// outright. Only an exact key count catches it.
func TestMsgPackMapLenMatchesEncodedKeys(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)

	tests := []struct {
		name     string
		maxRows  int
		profile  *database.QueryProfile
		wantKeys int
	}{
		{"uncapped, no profile", 0, nil, 7},
		{"uncapped, profiled", 0, &database.QueryProfile{}, 8},
		{"capped, no profile", 4, nil, 9},
		{"capped, profiled", 4, &database.QueryProfile{}, 10},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// The encoder writes the inner data array as
			// EncodeArrayLen(rowCount) and then emits every row the
			// batches hold, so rowCount must equal the batch contents.
			// drainArrowBatches guarantees that in production by trimming
			// the batch that crosses the cap; mirror it here.
			rowCount := 10
			if tt.maxRows > 0 {
				rowCount = tt.maxRows
			}
			rows := make([][]interface{}, rowCount)
			for i := range rows {
				rows[i] = []interface{}{int64(i)}
			}
			rec := buildArrowBatch(alloc, schema, rows)
			defer rec.Release()

			var buf bytes.Buffer
			w := bufio.NewWriter(&buf)
			if _, err := streamMsgPackFromBatches(context.Background(), w, schema,
				[]arrow.Record{rec}, rowCount, tt.maxRows, tt.profile,
				time.Now(), "2024-01-15T12:00:00Z"); err != nil {
				t.Fatalf("streamMsgPackFromBatches: %v", err)
			}
			w.Flush()

			// Decode into the stream decoder so trailing bytes are visible:
			// Unmarshal alone would happily stop after a short map.
			dec := msgpack.NewDecoder(bytes.NewReader(buf.Bytes()))
			var env map[string]interface{}
			if err := dec.Decode(&env); err != nil {
				t.Fatalf("envelope did not decode: %v", err)
			}
			if len(env) != tt.wantKeys {
				t.Errorf("envelope has %d keys, want %d: %v", len(env), tt.wantKeys, env)
			}
			if _, err := dec.DecodeInterface(); err != io.EOF {
				t.Errorf("bytes remain after the envelope (mapLen undercounts the pairs written): err=%v", err)
			}
		})
	}
}

// asInt64 normalizes whichever width the msgpack decoder chose for an integer.
func asInt64(t *testing.T, v interface{}) int64 {
	t.Helper()
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return rv.Int()
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return int64(rv.Uint())
	default:
		t.Fatalf("value %v (%T) is not an integer", v, v)
		return 0
	}
}

// deferredErrRowScanner reports an error from Err() rather than from Scan,
// the shape *sql.Rows uses for a failure discovered after iteration stops.
type deferredErrRowScanner struct {
	*mockRowScanner
	err error
}

func (d *deferredErrRowScanner) Err() error { return d.err }

// TestStreamTypedJSONMarksCappedResultThatThenFailed covers the case where both
// #723's and #724's markers apply: the stream reaches the cap and the iterator
// then reports a deferred error. The client must be told both facts, and the
// operator-side record must not be the one that goes missing, which is why
// logGovernanceRowCap is called ahead of the error branch at every call site.
func TestStreamTypedJSONMarksCappedResultThatThenFailed(t *testing.T) {
	rows := make([][]interface{}, 25)
	for i := range rows {
		rows[i] = []interface{}{int64(i)}
	}
	// A deferred iterator error, which is what *sql.Rows reports after the
	// loop has already stopped. The cap breaks the loop first, then Err()
	// surfaces the failure, so the result is both capped and truncated.
	scanner := &deferredErrRowScanner{
		mockRowScanner: &mockRowScanner{rows: rows},
		err:            errors.New("connection reset"),
	}

	var buf bytes.Buffer
	w := bufio.NewWriter(&buf)
	rowCount, streamErr := streamTypedJSON(context.Background(), w, []string{"id"},
		[]colType{colInt64}, scanner, 10, nil, time.Now(), "2024-01-15T12:00:00Z")
	w.Flush()

	if rowCount != 10 {
		t.Fatalf("rowCount = %d, want 10", rowCount)
	}
	if streamErr == nil {
		t.Fatal("deferred iterator error was swallowed; the stream must report it")
	}

	var env map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &env); err != nil {
		t.Fatalf("envelope is not valid JSON: %v\n%s", err, buf.Bytes())
	}
	if env["rows_capped"] != true {
		t.Errorf("a result that reached the cap must be marked capped: %v", env)
	}

	// The operator half must fire for this query too, not only on the clean
	// path. rowCapReached is the predicate every call site shares.
	metrics.Init(zerolog.Nop())
	h := &QueryHandler{logger: zerolog.Nop()}
	before := metrics.Get().Snapshot()["governance_queries_capped_total"].(int64)
	h.logGovernanceRowCap("json", "SELECT 1", 42, "capped", 10, int64(rowCount))
	if got := metrics.Get().Snapshot()["governance_queries_capped_total"].(int64); got != before+1 {
		t.Errorf("capped-then-failed query did not reach the operator counter: %d -> %d", before, got)
	}
}

func TestLogGovernanceRowCapCountsOnlyCappedQueries(t *testing.T) {
	metrics.Init(zerolog.Nop())
	h := &QueryHandler{logger: zerolog.Nop()}

	read := func() int64 {
		return metrics.Get().Snapshot()["governance_queries_capped_total"].(int64)
	}

	before := read()
	h.logGovernanceRowCap("json", "SELECT 1", 42, "capped", 10, 9)
	if got := read(); got != before {
		t.Errorf("a result under the cap incremented the counter: %d -> %d", before, got)
	}
	h.logGovernanceRowCap("json", "SELECT 1", 42, "capped", 0, 1000)
	if got := read(); got != before {
		t.Errorf("an ungoverned query incremented the counter: %d -> %d", before, got)
	}
	h.logGovernanceRowCap("json", "SELECT 1", 42, "capped", 10, 10)
	if got := read(); got != before+1 {
		t.Errorf("a capped query did not increment the counter: %d -> %d", before, got)
	}
}
