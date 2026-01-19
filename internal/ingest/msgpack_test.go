package ingest

import (
	"testing"
	"time"

	"github.com/basekick-labs/arc/pkg/models"
	"github.com/rs/zerolog"
	"github.com/vmihailenco/msgpack/v5"
)

func newTestDecoder() *MessagePackDecoder {
	return NewMessagePackDecoder(zerolog.Nop())
}

func TestMessagePackDecoder_DecodeRowFormat(t *testing.T) {
	decoder := newTestDecoder()

	// Create a row format payload
	payload := map[string]interface{}{
		"m": "cpu",
		"t": int64(1609459200000000), // microseconds
		"h": "server01",
		"fields": map[string]interface{}{
			"usage": 90.5,
			"count": int64(42),
		},
		"tags": map[string]interface{}{
			"region": "us-west",
		},
	}

	data, err := msgpack.Marshal(payload)
	if err != nil {
		t.Fatalf("failed to marshal payload: %v", err)
	}

	result, err := decoder.Decode(data)
	if err != nil {
		t.Fatalf("failed to decode: %v", err)
	}

	results, ok := result.([]interface{})
	if !ok {
		t.Fatalf("expected []interface{}, got %T", result)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	record, ok := results[0].(*models.Record)
	if !ok {
		t.Fatalf("expected *models.Record, got %T", results[0])
	}

	if record.Measurement != "cpu" {
		t.Errorf("measurement: got %q, want 'cpu'", record.Measurement)
	}

	if record.Tags["host"] != "server01" {
		t.Errorf("host tag: got %q, want 'server01'", record.Tags["host"])
	}

	if record.Tags["region"] != "us-west" {
		t.Errorf("region tag: got %q, want 'us-west'", record.Tags["region"])
	}

	if record.Fields["usage"] != 90.5 {
		t.Errorf("usage field: got %v, want 90.5", record.Fields["usage"])
	}
}

func TestMessagePackDecoder_DecodeColumnarFormat(t *testing.T) {
	decoder := newTestDecoder()

	// Create a columnar format payload
	payload := map[string]interface{}{
		"m": "cpu",
		"columns": map[string]interface{}{
			"time":   []interface{}{int64(1609459200000000), int64(1609459200000001)},
			"host":   []interface{}{"server01", "server02"},
			"usage":  []interface{}{90.5, 85.0},
			"region": []interface{}{"us-west", "us-east"},
		},
	}

	data, err := msgpack.Marshal(payload)
	if err != nil {
		t.Fatalf("failed to marshal payload: %v", err)
	}

	result, err := decoder.Decode(data)
	if err != nil {
		t.Fatalf("failed to decode: %v", err)
	}

	results, ok := result.([]interface{})
	if !ok {
		t.Fatalf("expected []interface{}, got %T", result)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	columnar, ok := results[0].(*models.ColumnarRecord)
	if !ok {
		t.Fatalf("expected *models.ColumnarRecord, got %T", results[0])
	}

	if columnar.Measurement != "cpu" {
		t.Errorf("measurement: got %q, want 'cpu'", columnar.Measurement)
	}

	if !columnar.Columnar {
		t.Error("expected Columnar to be true")
	}

	// Check columns
	if len(columnar.Columns["time"]) != 2 {
		t.Errorf("time column: expected 2 values, got %d", len(columnar.Columns["time"]))
	}

	if len(columnar.Columns["host"]) != 2 {
		t.Errorf("host column: expected 2 values, got %d", len(columnar.Columns["host"]))
	}
}

func TestMessagePackDecoder_DecodeBatchFormat(t *testing.T) {
	decoder := newTestDecoder()

	// Create a batch format payload
	payload := map[string]interface{}{
		"batch": []interface{}{
			map[string]interface{}{
				"m": "cpu",
				"t": int64(1609459200000000),
				"h": "server01",
				"fields": map[string]interface{}{
					"usage": 90.5,
				},
			},
			map[string]interface{}{
				"m": "memory",
				"t": int64(1609459200000001),
				"h": "server01",
				"fields": map[string]interface{}{
					"free": int64(1024),
				},
			},
		},
	}

	data, err := msgpack.Marshal(payload)
	if err != nil {
		t.Fatalf("failed to marshal payload: %v", err)
	}

	result, err := decoder.Decode(data)
	if err != nil {
		t.Fatalf("failed to decode: %v", err)
	}

	results, ok := result.([]interface{})
	if !ok {
		t.Fatalf("expected []interface{}, got %T", result)
	}

	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}

	// Check first record
	record1, ok := results[0].(*models.Record)
	if !ok {
		t.Fatalf("expected *models.Record, got %T", results[0])
	}
	if record1.Measurement != "cpu" {
		t.Errorf("first record measurement: got %q, want 'cpu'", record1.Measurement)
	}

	// Check second record
	record2, ok := results[1].(*models.Record)
	if !ok {
		t.Fatalf("expected *models.Record, got %T", results[1])
	}
	if record2.Measurement != "memory" {
		t.Errorf("second record measurement: got %q, want 'memory'", record2.Measurement)
	}
}

func TestMessagePackDecoder_DecodeArrayFormat(t *testing.T) {
	decoder := newTestDecoder()

	// Create an array of records
	payload := []interface{}{
		map[string]interface{}{
			"m": "cpu",
			"t": int64(1609459200000000),
			"h": "server01",
			"fields": map[string]interface{}{
				"usage": 90.5,
			},
		},
		map[string]interface{}{
			"m": "memory",
			"t": int64(1609459200000001),
			"h": "server02",
			"fields": map[string]interface{}{
				"free": int64(2048),
			},
		},
	}

	data, err := msgpack.Marshal(payload)
	if err != nil {
		t.Fatalf("failed to marshal payload: %v", err)
	}

	result, err := decoder.Decode(data)
	if err != nil {
		t.Fatalf("failed to decode: %v", err)
	}

	results, ok := result.([]interface{})
	if !ok {
		t.Fatalf("expected []interface{}, got %T", result)
	}

	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
}

func TestMessagePackDecoder_ExtractMeasurement(t *testing.T) {
	decoder := newTestDecoder()

	tests := []struct {
		name    string
		input   interface{}
		want    string
		wantErr bool
	}{
		{
			name:  "string measurement",
			input: "cpu",
			want:  "cpu",
		},
		{
			name:  "integer measurement",
			input: int64(42),
			want:  "measurement_42",
		},
		{
			name:    "nil measurement",
			input:   nil,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := decoder.extractMeasurement(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("extractMeasurement() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("extractMeasurement() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMessagePackDecoder_ExtractTimestamp(t *testing.T) {
	decoder := newTestDecoder()

	tests := []struct {
		name      string
		input     interface{}
		wantUnit  string
		wantErr   bool
	}{
		{
			name:     "seconds timestamp",
			input:    int64(1609459200),
			wantUnit: "seconds",
		},
		{
			name:     "milliseconds timestamp",
			input:    int64(1609459200000),
			wantUnit: "milliseconds",
		},
		{
			name:     "microseconds timestamp",
			input:    int64(1609459200000000),
			wantUnit: "microseconds",
		},
		{
			name:     "nanoseconds timestamp",
			input:    int64(1609459200000000000),
			wantUnit: "nanoseconds",
		},
		{
			name:     "nil timestamp (uses current time)",
			input:    nil,
			wantUnit: "current",
		},
		{
			name:     "float timestamp",
			input:    float64(1609459200000),
			wantUnit: "milliseconds",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := decoder.extractTimestamp(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("extractTimestamp() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				return
			}

			// Just verify we got a valid time
			if got.IsZero() {
				t.Error("extractTimestamp() returned zero time")
			}

			// For nil input, verify it's recent
			if tt.input == nil {
				if time.Since(got) > time.Second {
					t.Error("extractTimestamp(nil) should return current time")
				}
			}
		})
	}
}

func TestMessagePackDecoder_ExtractHost(t *testing.T) {
	decoder := newTestDecoder()

	tests := []struct {
		name  string
		input interface{}
		want  string
	}{
		{
			name:  "string host",
			input: "server01",
			want:  "server01",
		},
		{
			name:  "integer host",
			input: int64(42),
			want:  "host_42",
		},
		{
			name:  "nil host",
			input: nil,
			want:  "unknown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := decoder.extractHost(tt.input)
			if got != tt.want {
				t.Errorf("extractHost() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMessagePackDecoder_NormalizeTimestamps(t *testing.T) {
	decoder := newTestDecoder()

	tests := []struct {
		name     string
		input    []interface{}
		expected int64 // first value after normalization
	}{
		{
			name:     "seconds to microseconds",
			input:    []interface{}{int64(1609459200)},
			expected: 1609459200000000,
		},
		{
			name:     "milliseconds to microseconds",
			input:    []interface{}{int64(1609459200000)},
			expected: 1609459200000000,
		},
		{
			name:     "microseconds unchanged",
			input:    []interface{}{int64(1609459200000000)},
			expected: 1609459200000000,
		},
		{
			name:     "nanoseconds to microseconds",
			input:    []interface{}{int64(1609459200000000000)}, // 19 digits
			expected: 1609459200000000,                          // 16 digits
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			columns := map[string][]interface{}{
				"time": tt.input,
			}

			err := decoder.normalizeTimestamps(columns)
			if err != nil {
				t.Fatalf("normalizeTimestamps() error = %v", err)
			}

			got := columns["time"][0].(int64)
			if got != tt.expected {
				t.Errorf("normalizeTimestamps() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestMessagePackDecoder_ColumnarValidation(t *testing.T) {
	decoder := newTestDecoder()

	// Test mismatched column lengths
	payload := map[string]interface{}{
		"m": "cpu",
		"columns": map[string]interface{}{
			"time":  []interface{}{int64(1), int64(2), int64(3)},
			"host":  []interface{}{"a", "b"}, // Only 2 values
			"usage": []interface{}{1.0, 2.0, 3.0},
		},
	}

	data, err := msgpack.Marshal(payload)
	if err != nil {
		t.Fatalf("failed to marshal payload: %v", err)
	}

	_, err = decoder.Decode(data)
	if err == nil {
		t.Error("expected error for mismatched column lengths")
	}
}

func TestMessagePackDecoder_MissingMeasurement(t *testing.T) {
	decoder := newTestDecoder()

	// Test missing measurement
	payload := map[string]interface{}{
		"fields": map[string]interface{}{
			"usage": 90.5,
		},
	}

	data, err := msgpack.Marshal(payload)
	if err != nil {
		t.Fatalf("failed to marshal payload: %v", err)
	}

	_, err = decoder.Decode(data)
	if err == nil {
		t.Error("expected error for missing measurement")
	}
}

func TestMessagePackDecoder_GetStats(t *testing.T) {
	decoder := newTestDecoder()

	// Decode some valid data
	payload := map[string]interface{}{
		"m": "cpu",
		"t": int64(1609459200000000),
		"h": "server01",
		"fields": map[string]interface{}{
			"usage": 90.5,
		},
	}

	data, _ := msgpack.Marshal(payload)
	decoder.Decode(data)
	decoder.Decode(data)

	stats := decoder.GetStats()

	if stats["total_decoded"].(uint64) != 2 {
		t.Errorf("total_decoded: got %v, want 2", stats["total_decoded"])
	}

	if stats["total_errors"].(uint64) != 0 {
		t.Errorf("total_errors: got %v, want 0", stats["total_errors"])
	}
}

func TestMessagePackDecoder_InvalidPayload(t *testing.T) {
	decoder := newTestDecoder()

	// Invalid msgpack data
	_, err := decoder.Decode([]byte{0xFF, 0xFF, 0xFF})
	if err == nil {
		t.Error("expected error for invalid msgpack data")
	}

	stats := decoder.GetStats()
	if stats["total_errors"].(uint64) == 0 {
		t.Error("expected errors to be counted")
	}
}

// Benchmark tests
func BenchmarkMessagePackDecoder_DecodeRow(b *testing.B) {
	decoder := newTestDecoder()

	payload := map[string]interface{}{
		"m": "cpu",
		"t": int64(1609459200000000),
		"h": "server01",
		"fields": map[string]interface{}{
			"usage_idle":   90.5,
			"usage_system": 2.1,
			"usage_user":   7.4,
		},
		"tags": map[string]interface{}{
			"region": "us-west",
			"env":    "prod",
		},
	}

	data, _ := msgpack.Marshal(payload)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		decoder.Decode(data)
	}
}

func BenchmarkMessagePackDecoder_DecodeColumnar(b *testing.B) {
	decoder := newTestDecoder()

	// Create columnar payload with 1000 records
	times := make([]interface{}, 1000)
	hosts := make([]interface{}, 1000)
	usage := make([]interface{}, 1000)

	for i := 0; i < 1000; i++ {
		times[i] = int64(1609459200000000 + i)
		hosts[i] = "server01"
		usage[i] = 90.5
	}

	payload := map[string]interface{}{
		"m": "cpu",
		"columns": map[string]interface{}{
			"time":  times,
			"host":  hosts,
			"usage": usage,
		},
	}

	data, _ := msgpack.Marshal(payload)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		decoder.Decode(data)
	}
}
