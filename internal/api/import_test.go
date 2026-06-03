package api

import (
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"

	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/basekick-labs/arc/pkg/models"
)

func TestInferAndConvertColumn(t *testing.T) {
	tests := []struct {
		name string
		raw  []string
		want []interface{}
	}{
		{"all ints", []string{"1", "2", "3"}, []interface{}{int64(1), int64(2), int64(3)}},
		{"all floats", []string{"1.5", "2.0", "3.25"}, []interface{}{1.5, 2.0, 3.25}},
		{"int then float -> float", []string{"1", "2", "3.5"}, []interface{}{1.0, 2.0, 3.5}},
		{"bools", []string{"true", "false", "TRUE"}, []interface{}{true, false, true}},
		{"strings", []string{"a", "b", "c"}, []interface{}{"a", "b", "c"}},
		{"mixed -> strings", []string{"1", "abc", "2"}, []interface{}{"1", "abc", "2"}},
		{"empty int cell -> nil", []string{"1", "", "3"}, []interface{}{int64(1), nil, int64(3)}},
		{"empty float cell -> nil", []string{"1.5", "", "3.5"}, []interface{}{1.5, nil, 3.5}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := inferAndConvertColumn(tt.raw)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("inferAndConvertColumn(%v) = %v, want %v", tt.raw, got, tt.want)
			}
		})
	}
}

func TestOneTimeValueToMicros(t *testing.T) {
	tests := []struct {
		name       string
		value      string
		timeFormat string
		want       int64
		wantErr    bool
	}{
		{"epoch_s", "1609459200", "epoch_s", 1609459200000000, false},
		{"epoch_ms", "1609459200000", "epoch_ms", 1609459200000000, false},
		{"epoch_us", "1609459200000000", "epoch_us", 1609459200000000, false},
		{"epoch_ns", "1609459200000000000", "epoch_ns", 1609459200000000, false},
		{"auto seconds", "1609459200", "", 1609459200000000, false},
		{"auto millis", "1609459200000", "", 1609459200000000, false},
		{"auto micros", "1609459200000000", "", 1609459200000000, false},
		{"auto nanos", "1609459200000000000", "", 1609459200000000, false},
		{"auto negative seconds (pre-1970)", "-1000000000", "", -1000000000000000, false},
		{"auto negative millis (pre-1970)", "-1000000000000", "", -1000000000000, false},
		{"auto RFC3339", "2021-01-01T00:00:00Z", "", 1609459200000000, false},
		{"auto date-only", "2021-01-01", "", 1609459200000000, false},
		{"auto space-separated", "2021-01-01 00:00:00", "", 1609459200000000, false},
		{"epoch with non-numeric -> err", "notanumber", "epoch_s", 0, true},
		{"auto unparseable -> err", "not a date", "", 0, true},
		{"bad format -> err", "1609459200", "epoch_weeks", 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := oneTimeValueToMicros(tt.value, tt.timeFormat)
			if tt.wantErr {
				if err == nil {
					t.Errorf("expected error for %q (%s), got nil", tt.value, tt.timeFormat)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("oneTimeValueToMicros(%q, %q) = %d, want %d", tt.value, tt.timeFormat, got, tt.want)
			}
		})
	}
}

func TestStringsToTimeMicros_EmptyValueErrors(t *testing.T) {
	if _, err := stringsToTimeMicros([]string{"1609459200", "", "1609459300"}, "epoch_s"); err == nil {
		t.Error("expected error for empty time value, got nil")
	}
}

func TestStringsToTimeMicros_CachedLayoutMultiRow(t *testing.T) {
	// Homogeneous RFC3339 column: layout cached after row 0, applied to all.
	got, err := stringsToTimeMicros([]string{
		"2021-01-01T00:00:00Z",
		"2021-01-01T01:00:00Z",
		"2021-01-01T02:00:00Z",
	}, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []int64{1609459200000000, 1609462800000000, 1609466400000000}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestInferAndConvertColumn_MixedBoolReps(t *testing.T) {
	// "true"/"1" mixed -> not all-int (true isn't int), all bool-literal -> bool.
	got := inferAndConvertColumn([]string{"true", "1", "false", "0"})
	want := []interface{}{true, true, false, false}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
	// Pure 0/1 stays integer (isInt wins over isBool in the switch).
	gotInt := inferAndConvertColumn([]string{"0", "1", "1", "0"})
	wantInt := []interface{}{int64(0), int64(1), int64(1), int64(0)}
	if !reflect.DeepEqual(gotInt, wantInt) {
		t.Errorf("pure 0/1: got %v, want %v", gotInt, wantInt)
	}
}

func TestValidateImportHeader(t *testing.T) {
	tests := []struct {
		name       string
		header     []string
		timeColumn string
		wantIdx    int
		wantErr    bool
	}{
		{"time column present", []string{"time", "host", "v"}, "time", 0, false},
		{"renamed time column", []string{"ts", "host", "v"}, "ts", 0, false},
		{"missing time column", []string{"a", "b"}, "ts", -1, true},
		{"duplicate column names", []string{"a", "a", "b"}, "a", -1, true},
		{"rename collides with existing time", []string{"ts", "time", "v"}, "ts", -1, true},
		{"time_column==time, no collision", []string{"time", "host"}, "time", 0, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			idx, err := validateImportHeader(tt.header, tt.timeColumn)
			if tt.wantErr {
				if err == nil {
					t.Errorf("expected error for header %v (time=%q), got nil", tt.header, tt.timeColumn)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if idx != tt.wantIdx {
				t.Errorf("timeIdx = %d, want %d", idx, tt.wantIdx)
			}
		})
	}
}

func TestLimitedReader(t *testing.T) {
	// Reading within the limit succeeds and passes through EOF.
	r := &limitedReader{r: strings.NewReader("hello"), limit: 10}
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("unexpected error under limit: %v", err)
	}
	if string(got) != "hello" {
		t.Errorf("got %q, want hello", got)
	}

	// Exceeding the limit returns errImportTooLarge (NOT a silent EOF — that's
	// the bug: io.LimitReader would let csv.Reader truncate quietly).
	r2 := &limitedReader{r: strings.NewReader("0123456789ABCDEF"), limit: 8}
	_, err = io.ReadAll(r2)
	if !errors.Is(err, errImportTooLarge) {
		t.Errorf("expected errImportTooLarge, got %v", err)
	}
}

func TestBuildImportResult(t *testing.T) {
	// Two distinct hours -> partitions_created = 2. time column renamed to "time".
	header := []string{"ts", "host", "value"}
	timeMicros := []int64{
		1609459200000000, // 2021-01-01T00:00:00Z (hour bucket A)
		1609459260000000, // 2021-01-01T00:01:00Z (hour bucket A)
		1609462800000000, // 2021-01-01T01:00:00Z (hour bucket B)
	}
	res := buildImportResult("mydb", "cpu", header, "ts", timeMicros)

	if res.Database != "mydb" || res.Measurement != "cpu" {
		t.Errorf("db/measurement mismatch: %+v", res)
	}
	if res.RowsImported != 3 {
		t.Errorf("RowsImported = %d, want 3", res.RowsImported)
	}
	if res.PartitionsCreated != 2 {
		t.Errorf("PartitionsCreated = %d, want 2 (distinct hour buckets)", res.PartitionsCreated)
	}
	wantCols := []string{"time", "host", "value"} // "ts" renamed to "time"
	if !reflect.DeepEqual(res.Columns, wantCols) {
		t.Errorf("Columns = %v, want %v", res.Columns, wantCols)
	}
	if res.TimeRangeMin != "2021-01-01T00:00:00Z" {
		t.Errorf("TimeRangeMin = %q, want 2021-01-01T00:00:00Z", res.TimeRangeMin)
	}
	if res.TimeRangeMax != "2021-01-01T01:00:00Z" {
		t.Errorf("TimeRangeMax = %q, want 2021-01-01T01:00:00Z", res.TimeRangeMax)
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
	cpuRecord, ok := columnar["cpu"]
	if !ok {
		t.Fatal("missing 'cpu' measurement")
	}
	if timeCols, ok := cpuRecord.Columns["time"]; !ok || len(timeCols) != 2 {
		t.Errorf("cpu: expected 2 time values, got %d", len(cpuRecord.Columns["time"]))
	}

	// mem should have 1 row
	memRecord, ok := columnar["mem"]
	if !ok {
		t.Fatal("missing 'mem' measurement")
	}
	if timeCols, ok := memRecord.Columns["time"]; !ok || len(timeCols) != 1 {
		t.Errorf("mem: expected 1 time value, got %d", len(memRecord.Columns["time"]))
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
