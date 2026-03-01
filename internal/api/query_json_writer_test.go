package api

import (
	"bufio"
	"bytes"
	"database/sql"
	"encoding/json"
	"math"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/database"
)

// mockRowScanner simulates *sql.Rows for testing streamTypedJSON.
type mockRowScanner struct {
	rows    [][]interface{}
	idx     int
	scanErr error
}

func (m *mockRowScanner) Next() bool {
	return m.idx < len(m.rows)
}

func (m *mockRowScanner) Scan(dest ...interface{}) error {
	if m.scanErr != nil {
		return m.scanErr
	}
	row := m.rows[m.idx]
	for i, v := range row {
		*dest[i].(*interface{}) = v
	}
	m.idx++
	return nil
}

func (m *mockRowScanner) Err() error { return nil }

// streamToBytes is a test helper that runs streamTypedJSON into a buffer.
func streamToBytes(
	columns []string,
	colTypes []colType,
	scanner rowScanner,
	governanceMaxRows int,
	profile *database.QueryProfile,
) ([]byte, int) {
	var buf bytes.Buffer
	w := bufio.NewWriter(&buf)
	start := time.Now()
	rowCount := streamTypedJSON(w, columns, colTypes, scanner, governanceMaxRows, profile, start, "2024-01-15T12:00:00Z")
	w.Flush()
	return buf.Bytes(), rowCount
}

func TestMapColumnType(t *testing.T) {
	tests := []struct {
		dbType   string
		expected colType
	}{
		{"BIGINT", colInt64},
		{"INT64", colInt64},
		{"INTEGER", colInt64},
		{"INT", colInt64},
		{"INT32", colInt64},
		{"SMALLINT", colInt64},
		{"TINYINT", colInt64},
		{"HUGEINT", colString},
		{"UBIGINT", colString},
		{"DOUBLE", colFloat64},
		{"FLOAT", colFloat64},
		{"REAL", colFloat64},
		{"DECIMAL(10,2)", colFloat64},
		{"NUMERIC", colFloat64},
		{"BOOLEAN", colBool},
		{"BOOL", colBool},
		{"TIMESTAMP", colTimestamp},
		{"TIMESTAMP WITH TIME ZONE", colTimestamp},
		{"DATETIME", colTimestamp},
		{"DATE", colTimestamp},
		{"VARCHAR", colString},
		{"TEXT", colString},
		{"BLOB", colString},
		{"", colString},
	}

	for _, tt := range tests {
		got := mapColumnType(tt.dbType)
		if got != tt.expected {
			t.Errorf("mapColumnType(%q) = %d, want %d", tt.dbType, got, tt.expected)
		}
	}
}

func TestStreamTypedJSON_BasicTypes(t *testing.T) {
	ts := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)

	scanner := &mockRowScanner{
		rows: [][]interface{}{
			{int64(42), 72.5, "server1", true, ts},
			{int64(43), 73.1, "server2", false, ts.Add(time.Hour)},
		},
	}

	columns := []string{"id", "value", "host", "active", "time"}
	colTypes := []colType{colInt64, colFloat64, colString, colBool, colTimestamp}

	data, rowCount := streamToBytes(columns, colTypes, scanner, 0, nil)
	if rowCount != 2 {
		t.Fatalf("expected 2 rows, got %d", rowCount)
	}

	// Verify output is valid JSON
	var result map[string]interface{}
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatalf("invalid JSON: %v\noutput: %s", err, string(data))
	}

	// Check envelope fields
	if result["success"] != true {
		t.Errorf("expected success=true, got %v", result["success"])
	}
	if result["row_count"].(float64) != 2 {
		t.Errorf("expected row_count=2, got %v", result["row_count"])
	}

	// Check columns
	cols := result["columns"].([]interface{})
	if len(cols) != 5 {
		t.Fatalf("expected 5 columns, got %d", len(cols))
	}

	// Check data
	rows := result["data"].([]interface{})
	if len(rows) != 2 {
		t.Fatalf("expected 2 data rows, got %d", len(rows))
	}

	row0 := rows[0].([]interface{})
	if row0[0].(float64) != 42 {
		t.Errorf("row[0][0] expected 42, got %v", row0[0])
	}
	if row0[1].(float64) != 72.5 {
		t.Errorf("row[0][1] expected 72.5, got %v", row0[1])
	}
	if row0[2].(string) != "server1" {
		t.Errorf("row[0][2] expected 'server1', got %v", row0[2])
	}
	if row0[3].(bool) != true {
		t.Errorf("row[0][3] expected true, got %v", row0[3])
	}
	if row0[4].(string) != "2024-01-15T10:30:00Z" {
		t.Errorf("row[0][4] expected timestamp, got %v", row0[4])
	}
}

func TestStreamTypedJSON_NullValues(t *testing.T) {
	scanner := &mockRowScanner{
		rows: [][]interface{}{
			{nil, nil, nil, nil, nil},
			{int64(1), 2.0, "hello", true, time.Now()},
		},
	}

	columns := []string{"a", "b", "c", "d", "e"}
	colTypes := []colType{colInt64, colFloat64, colString, colBool, colTimestamp}

	data, rowCount := streamToBytes(columns, colTypes, scanner, 0, nil)
	if rowCount != 2 {
		t.Fatalf("expected 2 rows, got %d", rowCount)
	}

	var result map[string]interface{}
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}

	rows := result["data"].([]interface{})
	row0 := rows[0].([]interface{})
	for i, v := range row0 {
		if v != nil {
			t.Errorf("row[0][%d] expected null, got %v", i, v)
		}
	}
}

func TestStreamTypedJSON_EmptyResult(t *testing.T) {
	scanner := &mockRowScanner{rows: [][]interface{}{}}

	data, rowCount := streamToBytes([]string{"time", "value"}, []colType{colTimestamp, colFloat64}, scanner, 0, nil)
	if rowCount != 0 {
		t.Fatalf("expected 0 rows, got %d", rowCount)
	}

	var result map[string]interface{}
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}

	rows := result["data"].([]interface{})
	if len(rows) != 0 {
		t.Errorf("expected empty data array, got %d items", len(rows))
	}
}

func TestStreamTypedJSON_GovernanceMaxRows(t *testing.T) {
	rows := make([][]interface{}, 100)
	for i := range rows {
		rows[i] = []interface{}{int64(i)}
	}

	scanner := &mockRowScanner{rows: rows}

	_, rowCount := streamToBytes([]string{"id"}, []colType{colInt64}, scanner, 10, nil)
	if rowCount != 10 {
		t.Fatalf("expected 10 rows (governance limit), got %d", rowCount)
	}
}

func TestStreamTypedJSON_SpecialCharsInStrings(t *testing.T) {
	scanner := &mockRowScanner{
		rows: [][]interface{}{
			{`hello "world"`},
			{`back\slash`},
			{"line\nbreak"},
			{"tab\there"},
			{"\x00\x01\x1f"},
		},
	}

	data, rowCount := streamToBytes([]string{"msg"}, []colType{colString}, scanner, 0, nil)
	if rowCount != 5 {
		t.Fatalf("expected 5 rows, got %d", rowCount)
	}

	var result map[string]interface{}
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatalf("invalid JSON: %v\noutput: %s", err, string(data))
	}

	rows := result["data"].([]interface{})
	expected := []string{
		`hello "world"`,
		`back\slash`,
		"line\nbreak",
		"tab\there",
		"\x00\x01\x1f",
	}
	for i, exp := range expected {
		row := rows[i].([]interface{})
		if row[0].(string) != exp {
			t.Errorf("row[%d] expected %q, got %q", i, exp, row[0])
		}
	}
}

func TestStreamTypedJSON_NaN_Inf(t *testing.T) {
	scanner := &mockRowScanner{
		rows: [][]interface{}{
			{math.NaN()},
			{math.Inf(1)},
			{math.Inf(-1)},
		},
	}

	data, rowCount := streamToBytes([]string{"v"}, []colType{colFloat64}, scanner, 0, nil)
	if rowCount != 3 {
		t.Fatalf("expected 3 rows, got %d", rowCount)
	}

	var result map[string]interface{}
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatalf("invalid JSON: %v\noutput: %s", err, string(data))
	}

	rows := result["data"].([]interface{})
	for i, row := range rows {
		r := row.([]interface{})
		if r[0] != nil {
			t.Errorf("row[%d] expected null for NaN/Inf, got %v", i, r[0])
		}
	}
}

func TestStreamTypedJSON_SqlNullTypes(t *testing.T) {
	testCases := []struct {
		val  interface{}
		ct   colType
		name string
	}{
		{sql.NullInt64{Int64: 42, Valid: true}, colInt64, "valid NullInt64"},
		{sql.NullInt64{Valid: false}, colInt64, "null NullInt64"},
		{sql.NullFloat64{Float64: 3.14, Valid: true}, colFloat64, "valid NullFloat64"},
		{sql.NullFloat64{Valid: false}, colFloat64, "null NullFloat64"},
		{sql.NullString{String: "hello", Valid: true}, colString, "valid NullString"},
		{sql.NullString{Valid: false}, colString, "null NullString"},
		{sql.NullBool{Bool: true, Valid: true}, colBool, "valid NullBool"},
		{sql.NullBool{Valid: false}, colBool, "null NullBool"},
	}

	for _, tc := range testCases {
		s := &mockRowScanner{rows: [][]interface{}{{tc.val}}}
		data, _ := streamToBytes([]string{"v"}, []colType{tc.ct}, s, 0, nil)

		var result map[string]interface{}
		if err := json.Unmarshal(data, &result); err != nil {
			t.Fatalf("%s: invalid JSON: %v\noutput: %s", tc.name, err, string(data))
		}
	}
}

func TestStreamTypedJSON_WithProfile(t *testing.T) {
	scanner := &mockRowScanner{
		rows: [][]interface{}{{int64(1)}},
	}

	profile := &database.QueryProfile{
		TotalMs:     100.5,
		PlannerMs:   10.2,
		ExecutionMs: 90.3,
		RowsScanned: 50000,
		Latency:     100.0,
	}

	data, _ := streamToBytes([]string{"id"}, []colType{colInt64}, scanner, 0, profile)

	var result map[string]interface{}
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatalf("invalid JSON: %v\noutput: %s", err, string(data))
	}

	p, ok := result["profile"].(map[string]interface{})
	if !ok {
		t.Fatal("expected profile in response")
	}
	if p["total_ms"].(float64) != 100.5 {
		t.Errorf("expected total_ms=100.5, got %v", p["total_ms"])
	}
}

func TestStreamTypedJSON_DuckDBTypeCrossover(t *testing.T) {
	// DuckDB sometimes returns float64 for integer columns (e.g., COUNT(*))
	scanner := &mockRowScanner{
		rows: [][]interface{}{
			{float64(12345)},  // float64 in int64 column — should render as integer
			{float64(99.99)},  // actual float in int64 column — should render as float
		},
	}

	data, _ := streamToBytes([]string{"count"}, []colType{colInt64}, scanner, 0, nil)

	var result map[string]interface{}
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatalf("invalid JSON: %v\noutput: %s", err, string(data))
	}

	rows := result["data"].([]interface{})
	row0 := rows[0].([]interface{})
	if row0[0].(float64) != 12345 {
		t.Errorf("expected 12345, got %v", row0[0])
	}
	row1 := rows[1].([]interface{})
	if row1[0].(float64) != 99.99 {
		t.Errorf("expected 99.99, got %v", row1[0])
	}
}

func TestWriteJSONString_Escaping(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{`hello`, `"hello"`},
		{`he"llo`, `"he\"llo"`},
		{`back\slash`, `"back\\slash"`},
		{"new\nline", `"new\nline"`},
		{"tab\there", `"tab\there"`},
		{"\r\n", `"\r\n"`},
		{"\b\f", `"\b\f"`},
		{"\x00", `"\u0000"`},
		{"\x1f", `"\u001f"`},
		{"", `""`},
		{"normal string", `"normal string"`},
	}

	scratch := make([]byte, 0, 64)
	for _, tt := range tests {
		var buf bytes.Buffer
		w := bufio.NewWriter(&buf)
		writeJSONString(w, scratch, tt.input)
		w.Flush()
		if buf.String() != tt.expected {
			t.Errorf("writeJSONString(%q) = %s, want %s", tt.input, buf.String(), tt.expected)
		}
	}
}

// BenchmarkStreamTypedJSON benchmarks the typed writer against json.Marshal.
func BenchmarkStreamTypedJSON(b *testing.B) {
	// Build 10K rows of mixed types
	numRows := 10000
	rows := make([][]interface{}, numRows)
	ts := time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC)
	for i := range rows {
		rows[i] = []interface{}{
			int64(i),
			72.5 + float64(i)*0.01,
			"server-01",
			true,
			ts.Add(time.Duration(i) * time.Second),
		}
	}

	columns := []string{"id", "value", "host", "active", "time"}
	colTypes := []colType{colInt64, colFloat64, colString, colBool, colTimestamp}

	b.Run("StreamTypedJSON", func(b *testing.B) {
		var buf bytes.Buffer
		for i := 0; i < b.N; i++ {
			buf.Reset()
			w := bufio.NewWriter(&buf)
			scanner := &mockRowScanner{rows: rows}
			streamTypedJSON(w, columns, colTypes, scanner, 0, nil, time.Now(), "ts")
			w.Flush()
		}
	})

	b.Run("StdlibJSON", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			// Simulate current approach: convertValue + json.Marshal
			data := make([][]interface{}, numRows)
			for j, row := range rows {
				converted := make([]interface{}, len(row))
				for k, v := range row {
					switch val := v.(type) {
					case time.Time:
						converted[k] = val.UTC().Format(time.RFC3339Nano)
					default:
						converted[k] = val
					}
				}
				data[j] = converted
			}
			resp := QueryResponse{
				Success:         true,
				Columns:         columns,
				Data:            data,
				RowCount:        numRows,
				ExecutionTimeMs: 100.0,
				Timestamp:       "ts",
			}
			json.Marshal(resp)
		}
	})
}
