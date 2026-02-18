package api

import (
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/basekick-labs/arc/pkg/models"
)

func TestBuildReadExpression_CSV(t *testing.T) {
	h := &ImportHandler{}

	tests := []struct {
		name     string
		opts     importOptions
		expected string
	}{
		{
			name: "default CSV",
			opts: importOptions{format: "csv", delimiter: ","},
			expected: "read_csv('/tmp/test.csv', auto_detect=true, header=true)",
		},
		{
			name: "tab delimiter",
			opts: importOptions{format: "csv", delimiter: "\t"},
			expected: "read_csv('/tmp/test.csv', auto_detect=true, header=true, delim='\t')",
		},
		{
			name: "skip rows",
			opts: importOptions{format: "csv", delimiter: ",", skipRows: 2},
			expected: "read_csv('/tmp/test.csv', auto_detect=true, header=true, skip=2)",
		},
		{
			name: "semicolon with skip",
			opts: importOptions{format: "csv", delimiter: ";", skipRows: 1},
			expected: "read_csv('/tmp/test.csv', auto_detect=true, header=true, delim=';', skip=1)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := h.buildReadExpression("/tmp/test.csv", tt.opts)
			if result != tt.expected {
				t.Errorf("got %q, want %q", result, tt.expected)
			}
		})
	}
}

func TestBuildReadExpression_Parquet(t *testing.T) {
	h := &ImportHandler{}

	result := h.buildReadExpression("/tmp/test.parquet", importOptions{format: "parquet"})
	expected := "read_parquet('/tmp/test.parquet')"
	if result != expected {
		t.Errorf("got %q, want %q", result, expected)
	}
}

func TestBuildTimeCast(t *testing.T) {
	h := &ImportHandler{}

	tests := []struct {
		name       string
		timeCol    string
		timeFormat string
		expected   string
	}{
		{"auto detect", "time", "", `"time"::TIMESTAMP`},
		{"epoch seconds", "ts", "epoch_s", `to_timestamp("ts"::BIGINT)`},
		{"epoch milliseconds", "timestamp", "epoch_ms", `to_timestamp("timestamp"::BIGINT / 1000.0)`},
		{"epoch microseconds", "time", "epoch_us", `to_timestamp("time"::BIGINT / 1000000.0)`},
		{"epoch nanoseconds", "time", "epoch_ns", `to_timestamp("time"::BIGINT / 1000000000.0)`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := h.buildTimeCast(tt.timeCol, tt.timeFormat)
			if result != tt.expected {
				t.Errorf("got %q, want %q", result, tt.expected)
			}
		})
	}
}

func TestGenerateStoragePath(t *testing.T) {
	partTime := time.Date(2025, 3, 15, 14, 0, 0, 0, time.UTC)
	path := generateStoragePath("mydb", "cpu", partTime)

	// Check prefix structure
	prefix := "mydb/cpu/2025/03/15/14/cpu_"
	if len(path) < len(prefix) || path[:len(prefix)] != prefix {
		t.Errorf("path %q does not start with expected prefix %q", path, prefix)
	}

	// Check it ends with .parquet
	if path[len(path)-8:] != ".parquet" {
		t.Errorf("path %q does not end with .parquet", path)
	}
}

func TestGenerateStoragePath_DifferentHours(t *testing.T) {
	tests := []struct {
		name           string
		partTime       time.Time
		expectedPrefix string
	}{
		{
			"midnight",
			time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
			"db/metric/2025/01/01/00/metric_",
		},
		{
			"noon",
			time.Date(2025, 6, 15, 12, 0, 0, 0, time.UTC),
			"db/metric/2025/06/15/12/metric_",
		},
		{
			"end of day",
			time.Date(2025, 12, 31, 23, 0, 0, 0, time.UTC),
			"db/metric/2025/12/31/23/metric_",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := generateStoragePath("db", "metric", tt.partTime)
			if path[:len(tt.expectedPrefix)] != tt.expectedPrefix {
				t.Errorf("got prefix %q, want %q", path[:len(tt.expectedPrefix)], tt.expectedPrefix)
			}
		})
	}
}

func TestPrecisionTimestampAdjustment(t *testing.T) {
	// ParseBatchWithPrecision converts raw timestamps to μs based on precision.
	parser := ingest.NewLineProtocolParser()

	tests := []struct {
		name       string
		precision  string
		lpLine     string
		expectedUs int64
	}{
		{
			name:       "nanosecond precision (default)",
			precision:  "ns",
			lpLine:     "cpu value=1.0 1609459200000000000", // 2021-01-01T00:00:00Z in ns
			expectedUs: 1609459200000000,
		},
		{
			name:       "microsecond precision",
			precision:  "us",
			lpLine:     "cpu value=1.0 1609459200000000", // 2021-01-01T00:00:00Z in μs
			expectedUs: 1609459200000000,
		},
		{
			name:       "millisecond precision",
			precision:  "ms",
			lpLine:     "cpu value=1.0 1609459200000", // 2021-01-01T00:00:00Z in ms
			expectedUs: 1609459200000000,
		},
		{
			name:       "second precision",
			precision:  "s",
			lpLine:     "cpu value=1.0 1609459200", // 2021-01-01T00:00:00Z in s
			expectedUs: 1609459200000000,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			records := parser.ParseBatchWithPrecision([]byte(tt.lpLine), tt.precision)
			if len(records) != 1 {
				t.Fatalf("expected 1 record, got %d", len(records))
			}

			if records[0].Timestamp != tt.expectedUs {
				t.Errorf("precision %s: got timestamp %d μs, want %d μs",
					tt.precision, records[0].Timestamp, tt.expectedUs)
			}
		})
	}
}

func TestParseBatchToColumnar_RoundTrip(t *testing.T) {
	parser := ingest.NewLineProtocolParser()

	lpData := []byte("cpu,host=server01 value=0.64,load=1.5 1609459200000000000\n" +
		"cpu,host=server02 value=0.72,load=2.1 1609459200000000000\n" +
		"mem,host=server01 used=1024 1609459200000000000\n")

	records := parser.ParseBatch(lpData)
	if len(records) != 3 {
		t.Fatalf("expected 3 records, got %d", len(records))
	}

	columnar := ingest.BatchToColumnar(records)

	// Should have 2 measurements
	if len(columnar) != 2 {
		t.Fatalf("expected 2 measurements, got %d", len(columnar))
	}

	// cpu should have 2 rows
	cpuCols, ok := columnar["cpu"]
	if !ok {
		t.Fatal("missing 'cpu' measurement")
	}
	if timeCols, ok := cpuCols["time"]; !ok || len(timeCols) != 2 {
		t.Errorf("cpu: expected 2 time values, got %d", len(cpuCols["time"]))
	}

	// mem should have 1 row
	memCols, ok := columnar["mem"]
	if !ok {
		t.Fatal("missing 'mem' measurement")
	}
	if timeCols, ok := memCols["time"]; !ok || len(timeCols) != 1 {
		t.Errorf("mem: expected 1 time value, got %d", len(memCols["time"]))
	}
}

func TestMeasurementFilter(t *testing.T) {
	parser := ingest.NewLineProtocolParser()

	lpData := []byte("cpu,host=a value=1.0 1609459200000000000\n" +
		"mem,host=a used=1024 1609459200000000000\n" +
		"cpu,host=b value=2.0 1609459200000000000\n")

	records := parser.ParseBatch(lpData)

	// Filter to cpu only
	filtered := make([]*models.Record, 0)
	for _, r := range records {
		if r.Measurement == "cpu" {
			filtered = append(filtered, r)
		}
	}

	if len(filtered) != 2 {
		t.Errorf("expected 2 cpu records after filter, got %d", len(filtered))
	}
	for _, r := range filtered {
		if r.Measurement != "cpu" {
			t.Errorf("expected measurement 'cpu', got %q", r.Measurement)
		}
	}
}

func TestEscapeSQLString(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"normal", "normal"},
		{"it's", "it''s"},
		{"a'b'c", "a''b''c"},
		{"", ""},
	}

	for _, tt := range tests {
		result := escapeSQLString(tt.input)
		if result != tt.expected {
			t.Errorf("escapeSQLString(%q) = %q, want %q", tt.input, result, tt.expected)
		}
	}
}
