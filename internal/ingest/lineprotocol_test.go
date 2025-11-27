package ingest

import (
	"testing"
)

func TestLineProtocolParser_ParseLine_Basic(t *testing.T) {
	parser := NewLineProtocolParser()

	tests := []struct {
		name        string
		input       string
		wantMeasure string
		wantTags    map[string]string
		wantFields  map[string]interface{}
		wantNil     bool
	}{
		{
			name:        "simple measurement with one field",
			input:       "cpu usage=90.5 1609459200000000000",
			wantMeasure: "cpu",
			wantTags:    map[string]string{},
			wantFields:  map[string]interface{}{"usage": 90.5},
		},
		{
			name:        "measurement with tags and fields",
			input:       "cpu,host=server01,region=us-west usage_idle=90.5,usage_system=2.1 1609459200000000000",
			wantMeasure: "cpu",
			wantTags:    map[string]string{"host": "server01", "region": "us-west"},
			wantFields:  map[string]interface{}{"usage_idle": 90.5, "usage_system": 2.1},
		},
		{
			name:        "measurement with integer field",
			input:       "http_requests,method=GET count=42i 1609459200000000000",
			wantMeasure: "http_requests",
			wantTags:    map[string]string{"method": "GET"},
			wantFields:  map[string]interface{}{"count": int64(42)},
		},
		{
			name:        "measurement with unsigned integer field",
			input:       "memory bytes=1024u 1609459200000000000",
			wantMeasure: "memory",
			wantTags:    map[string]string{},
			wantFields:  map[string]interface{}{"bytes": uint64(1024)},
		},
		{
			name:        "measurement with boolean fields",
			input:       "status active=true,error=false 1609459200000000000",
			wantMeasure: "status",
			wantTags:    map[string]string{},
			wantFields:  map[string]interface{}{"active": true, "error": false},
		},
		{
			name:        "measurement with string field",
			input:       `event,type=error message="disk full" 1609459200000000000`,
			wantMeasure: "event",
			wantTags:    map[string]string{"type": "error"},
			wantFields:  map[string]interface{}{"message": "disk full"},
		},
		{
			name:        "measurement without timestamp",
			input:       "temperature,sensor=bedroom temp=22.5",
			wantMeasure: "temperature",
			wantTags:    map[string]string{"sensor": "bedroom"},
			wantFields:  map[string]interface{}{"temp": 22.5},
		},
		{
			name:    "empty line",
			input:   "",
			wantNil: true,
		},
		{
			name:    "comment line",
			input:   "# this is a comment",
			wantNil: true,
		},
		{
			name:    "whitespace only",
			input:   "   \t  ",
			wantNil: true,
		},
		{
			name:    "measurement only (no fields)",
			input:   "cpu",
			wantNil: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			record := parser.ParseLine([]byte(tt.input))

			if tt.wantNil {
				if record != nil {
					t.Errorf("expected nil record, got %+v", record)
				}
				return
			}

			if record == nil {
				t.Fatal("expected non-nil record")
			}

			if record.Measurement != tt.wantMeasure {
				t.Errorf("measurement: got %q, want %q", record.Measurement, tt.wantMeasure)
			}

			// Check tags
			if len(record.Tags) != len(tt.wantTags) {
				t.Errorf("tags count: got %d, want %d", len(record.Tags), len(tt.wantTags))
			}
			for k, v := range tt.wantTags {
				if record.Tags[k] != v {
					t.Errorf("tag %q: got %q, want %q", k, record.Tags[k], v)
				}
			}

			// Check fields
			if len(record.Fields) != len(tt.wantFields) {
				t.Errorf("fields count: got %d, want %d", len(record.Fields), len(tt.wantFields))
			}
			for k, want := range tt.wantFields {
				got := record.Fields[k]
				if got != want {
					t.Errorf("field %q: got %v (%T), want %v (%T)", k, got, got, want, want)
				}
			}
		})
	}
}

func TestLineProtocolParser_ParseLine_EscapedCharacters(t *testing.T) {
	parser := NewLineProtocolParser()

	tests := []struct {
		name        string
		input       string
		wantMeasure string
		wantTags    map[string]string
		wantFields  map[string]interface{}
	}{
		{
			name:        "escaped comma in measurement",
			input:       `cpu\,usage value=1.0 1609459200000000000`,
			wantMeasure: "cpu,usage",
			wantTags:    map[string]string{},
			wantFields:  map[string]interface{}{"value": 1.0},
		},
		{
			name:        "escaped space in tag value",
			input:       `cpu,host=server\ 01 value=1.0 1609459200000000000`,
			wantMeasure: "cpu",
			wantTags:    map[string]string{"host": "server 01"},
			wantFields:  map[string]interface{}{"value": 1.0},
		},
		{
			name:        "escaped equals in tag value",
			input:       `cpu,equation=a\=b value=1.0 1609459200000000000`,
			wantMeasure: "cpu",
			wantTags:    map[string]string{"equation": "a=b"},
			wantFields:  map[string]interface{}{"value": 1.0},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			record := parser.ParseLine([]byte(tt.input))

			if record == nil {
				t.Fatal("expected non-nil record")
			}

			if record.Measurement != tt.wantMeasure {
				t.Errorf("measurement: got %q, want %q", record.Measurement, tt.wantMeasure)
			}

			for k, v := range tt.wantTags {
				if record.Tags[k] != v {
					t.Errorf("tag %q: got %q, want %q", k, record.Tags[k], v)
				}
			}
		})
	}
}

func TestLineProtocolParser_ParseLine_FieldTypes(t *testing.T) {
	parser := NewLineProtocolParser()

	tests := []struct {
		name       string
		input      string
		fieldName  string
		wantValue  interface{}
		wantType   string
	}{
		{
			name:      "float without decimal",
			input:     "test value=42 1609459200000000000",
			fieldName: "value",
			wantValue: 42.0,
			wantType:  "float64",
		},
		{
			name:      "float with decimal",
			input:     "test value=42.5 1609459200000000000",
			fieldName: "value",
			wantValue: 42.5,
			wantType:  "float64",
		},
		{
			name:      "negative float",
			input:     "test value=-42.5 1609459200000000000",
			fieldName: "value",
			wantValue: -42.5,
			wantType:  "float64",
		},
		{
			name:      "integer with i suffix",
			input:     "test value=42i 1609459200000000000",
			fieldName: "value",
			wantValue: int64(42),
			wantType:  "int64",
		},
		{
			name:      "negative integer",
			input:     "test value=-42i 1609459200000000000",
			fieldName: "value",
			wantValue: int64(-42),
			wantType:  "int64",
		},
		{
			name:      "unsigned integer with u suffix",
			input:     "test value=42u 1609459200000000000",
			fieldName: "value",
			wantValue: uint64(42),
			wantType:  "uint64",
		},
		{
			name:      "boolean true lowercase",
			input:     "test value=true 1609459200000000000",
			fieldName: "value",
			wantValue: true,
			wantType:  "bool",
		},
		{
			name:      "boolean TRUE uppercase",
			input:     "test value=TRUE 1609459200000000000",
			fieldName: "value",
			wantValue: true,
			wantType:  "bool",
		},
		{
			name:      "boolean t",
			input:     "test value=t 1609459200000000000",
			fieldName: "value",
			wantValue: true,
			wantType:  "bool",
		},
		{
			name:      "boolean false lowercase",
			input:     "test value=false 1609459200000000000",
			fieldName: "value",
			wantValue: false,
			wantType:  "bool",
		},
		{
			name:      "boolean f",
			input:     "test value=f 1609459200000000000",
			fieldName: "value",
			wantValue: false,
			wantType:  "bool",
		},
		{
			name:      "quoted string",
			input:     `test value="hello world" 1609459200000000000`,
			fieldName: "value",
			wantValue: "hello world",
			wantType:  "string",
		},
		{
			name:      "empty quoted string",
			input:     `test value="" 1609459200000000000`,
			fieldName: "value",
			wantValue: "",
			wantType:  "string",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			record := parser.ParseLine([]byte(tt.input))
			if record == nil {
				t.Fatal("expected non-nil record")
			}

			got := record.Fields[tt.fieldName]
			if got != tt.wantValue {
				t.Errorf("field value: got %v (%T), want %v (%T)", got, got, tt.wantValue, tt.wantValue)
			}
		})
	}
}

func TestLineProtocolParser_ParseBatch(t *testing.T) {
	parser := NewLineProtocolParser()

	input := `cpu,host=server01 usage=90.5 1609459200000000000
# this is a comment
memory,host=server01 free=1024i 1609459200000000000

disk,host=server01 used=50.0 1609459200000000000`

	records := parser.ParseBatch([]byte(input))

	if len(records) != 3 {
		t.Errorf("expected 3 records, got %d", len(records))
	}

	// Check first record
	if records[0].Measurement != "cpu" {
		t.Errorf("first record measurement: got %q, want 'cpu'", records[0].Measurement)
	}

	// Check second record
	if records[1].Measurement != "memory" {
		t.Errorf("second record measurement: got %q, want 'memory'", records[1].Measurement)
	}

	// Check third record
	if records[2].Measurement != "disk" {
		t.Errorf("third record measurement: got %q, want 'disk'", records[2].Measurement)
	}
}

func TestLineProtocolParser_ParseTimestamp(t *testing.T) {
	parser := NewLineProtocolParser()

	tests := []struct {
		name      string
		input     string
		wantMicro int64 // expected microseconds
	}{
		{
			name:      "nanosecond timestamp",
			input:     "test value=1 1609459200000000000",
			wantMicro: 1609459200000000, // 1609459200000000000 / 1000
		},
		{
			name:      "another nanosecond timestamp",
			input:     "test value=1 1609459200123456789",
			wantMicro: 1609459200123456, // truncated to microseconds
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			record := parser.ParseLine([]byte(tt.input))
			if record == nil {
				t.Fatal("expected non-nil record")
			}

			if record.Timestamp != tt.wantMicro {
				t.Errorf("timestamp: got %d, want %d", record.Timestamp, tt.wantMicro)
			}
		})
	}
}

func TestToFlatRecord(t *testing.T) {
	parser := NewLineProtocolParser()
	record := parser.ParseLine([]byte("cpu,host=server01,region=us-west usage=90.5,count=42i 1609459200000000000"))

	flat := ToFlatRecord(record)

	// Check required fields
	if flat["time"] != record.Timestamp {
		t.Errorf("time: got %v, want %v", flat["time"], record.Timestamp)
	}
	if flat["measurement"] != "cpu" {
		t.Errorf("measurement: got %v, want 'cpu'", flat["measurement"])
	}

	// Check tags are flattened
	if flat["host"] != "server01" {
		t.Errorf("host tag: got %v, want 'server01'", flat["host"])
	}
	if flat["region"] != "us-west" {
		t.Errorf("region tag: got %v, want 'us-west'", flat["region"])
	}

	// Check fields are flattened
	if flat["usage"] != 90.5 {
		t.Errorf("usage field: got %v, want 90.5", flat["usage"])
	}
	if flat["count"] != int64(42) {
		t.Errorf("count field: got %v, want 42", flat["count"])
	}
}

func TestBatchToColumnar(t *testing.T) {
	parser := NewLineProtocolParser()

	input := `cpu,host=server01 usage=90.5 1609459200000000000
cpu,host=server02 usage=85.0 1609459200000000001
memory,host=server01 free=1024i 1609459200000000000`

	records := parser.ParseBatch([]byte(input))
	columnar := BatchToColumnar(records)

	// Should have two measurements
	if len(columnar) != 2 {
		t.Errorf("expected 2 measurements, got %d", len(columnar))
	}

	// Check cpu measurement
	cpuData := columnar["cpu"]
	if cpuData == nil {
		t.Fatal("expected cpu measurement data")
	}
	if len(cpuData["time"]) != 2 {
		t.Errorf("cpu time column: expected 2 values, got %d", len(cpuData["time"]))
	}
	if len(cpuData["host"]) != 2 {
		t.Errorf("cpu host column: expected 2 values, got %d", len(cpuData["host"]))
	}
	if len(cpuData["usage"]) != 2 {
		t.Errorf("cpu usage column: expected 2 values, got %d", len(cpuData["usage"]))
	}

	// Check memory measurement
	memData := columnar["memory"]
	if memData == nil {
		t.Fatal("expected memory measurement data")
	}
	if len(memData["time"]) != 1 {
		t.Errorf("memory time column: expected 1 value, got %d", len(memData["time"]))
	}
}

// Benchmark tests
func BenchmarkLineProtocolParser_ParseLine(b *testing.B) {
	parser := NewLineProtocolParser()
	line := []byte("cpu,host=server01,region=us-west,env=prod usage_idle=90.5,usage_system=2.1,usage_user=7.4 1609459200000000000")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		parser.ParseLine(line)
	}
}

func BenchmarkLineProtocolParser_ParseBatch(b *testing.B) {
	parser := NewLineProtocolParser()
	batch := []byte(`cpu,host=server01 usage=90.5 1609459200000000000
cpu,host=server02 usage=85.0 1609459200000000001
cpu,host=server03 usage=75.0 1609459200000000002
cpu,host=server04 usage=88.0 1609459200000000003
cpu,host=server05 usage=92.0 1609459200000000004
memory,host=server01 free=1024i 1609459200000000000
memory,host=server02 free=2048i 1609459200000000001
disk,host=server01 used=50.0 1609459200000000000
disk,host=server02 used=75.0 1609459200000000001
disk,host=server03 used=30.0 1609459200000000002`)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		parser.ParseBatch(batch)
	}
}
