package ingest

import (
	"context"
	"io"
	"os"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/pkg/models"
	"github.com/rs/zerolog"
)

// createTestArrowBuffer creates an ArrowBuffer for testing
func createTestArrowBuffer(t *testing.T) *ArrowBuffer {
	t.Helper()
	logger := zerolog.New(os.Stderr).Level(zerolog.Disabled)
	cfg := &config.IngestConfig{
		MaxBufferSize:  1000,
		MaxBufferAgeMS: 60000,
		Compression:    "snappy",
	}

	// Create a mock storage backend for testing
	mockStorage := &mockStorageBackend{}

	return &ArrowBuffer{
		config:       cfg,
		storage:      mockStorage,
		writer:       NewArrowWriter(cfg, logger),
		shardCount:   32,
		flushQueue:   make(chan flushTask, 100),
		flushWorkers: 1,
		logger:       logger,
	}
}

// mockStorageBackend is a simple mock for testing that implements storage.Backend
type mockStorageBackend struct{}

func (m *mockStorageBackend) Write(ctx context.Context, path string, data []byte) error { return nil }
func (m *mockStorageBackend) WriteReader(ctx context.Context, path string, reader io.Reader, size int64) error {
	return nil
}
func (m *mockStorageBackend) Read(ctx context.Context, path string) ([]byte, error) { return nil, nil }
func (m *mockStorageBackend) ReadTo(ctx context.Context, path string, writer io.Writer) error {
	return nil
}
func (m *mockStorageBackend) Delete(ctx context.Context, path string) error { return nil }
func (m *mockStorageBackend) Exists(ctx context.Context, path string) (bool, error) {
	return false, nil
}
func (m *mockStorageBackend) List(ctx context.Context, prefix string) ([]string, error) {
	return nil, nil
}
func (m *mockStorageBackend) Close() error       { return nil }
func (m *mockStorageBackend) Type() string       { return "mock" }
func (m *mockStorageBackend) ConfigJSON() string { return "{}" }

func TestRowsToColumnar_SingleRecord(t *testing.T) {
	buffer := createTestArrowBuffer(t)

	now := time.Now()
	rows := []*models.Record{
		{
			Measurement: "cpu",
			Time:        now,
			Timestamp:   now.UnixMicro(),
			Fields: map[string]interface{}{
				"usage":  75.5,
				"system": 10.2,
			},
			Tags: map[string]string{
				"host":   "server01",
				"region": "us-east",
			},
		},
	}

	result := buffer.rowsToColumnar("cpu", rows)

	// Verify basic structure
	if result.Measurement != "cpu" {
		t.Errorf("Expected measurement 'cpu', got %q", result.Measurement)
	}
	if !result.Columnar {
		t.Error("Expected Columnar to be true")
	}

	// Verify time column
	timeCol, ok := result.Columns["time"]
	if !ok {
		t.Fatal("Missing time column")
	}
	if len(timeCol) != 1 {
		t.Errorf("Expected 1 time value, got %d", len(timeCol))
	}
	if timeCol[0].(int64) != now.UnixMicro() {
		t.Errorf("Expected timestamp %d, got %v", now.UnixMicro(), timeCol[0])
	}

	// Verify field columns
	usageCol, ok := result.Columns["usage"]
	if !ok {
		t.Fatal("Missing 'usage' field column")
	}
	if usageCol[0].(float64) != 75.5 {
		t.Errorf("Expected usage 75.5, got %v", usageCol[0])
	}

	systemCol, ok := result.Columns["system"]
	if !ok {
		t.Fatal("Missing 'system' field column")
	}
	if systemCol[0].(float64) != 10.2 {
		t.Errorf("Expected system 10.2, got %v", systemCol[0])
	}

	// Verify tag columns (stored directly by name, matching Line Protocol)
	hostCol, ok := result.Columns["host"]
	if !ok {
		t.Fatal("Missing 'host' column")
	}
	if hostCol[0].(string) != "server01" {
		t.Errorf("Expected host 'server01', got %v", hostCol[0])
	}

	regionCol, ok := result.Columns["region"]
	if !ok {
		t.Fatal("Missing 'region' column")
	}
	if regionCol[0].(string) != "us-east" {
		t.Errorf("Expected region 'us-east', got %v", regionCol[0])
	}
}

func TestRowsToColumnar_MultipleRecords(t *testing.T) {
	buffer := createTestArrowBuffer(t)

	now := time.Now()
	rows := []*models.Record{
		{
			Measurement: "cpu",
			Timestamp:   now.UnixMicro(),
			Fields:      map[string]interface{}{"usage": 75.5},
			Tags:        map[string]string{"host": "server01"},
		},
		{
			Measurement: "cpu",
			Timestamp:   now.Add(time.Second).UnixMicro(),
			Fields:      map[string]interface{}{"usage": 80.2},
			Tags:        map[string]string{"host": "server02"},
		},
		{
			Measurement: "cpu",
			Timestamp:   now.Add(2 * time.Second).UnixMicro(),
			Fields:      map[string]interface{}{"usage": 65.0},
			Tags:        map[string]string{"host": "server03"},
		},
	}

	result := buffer.rowsToColumnar("cpu", rows)

	// Verify column lengths
	timeCol := result.Columns["time"]
	if len(timeCol) != 3 {
		t.Errorf("Expected 3 time values, got %d", len(timeCol))
	}

	usageCol := result.Columns["usage"]
	if len(usageCol) != 3 {
		t.Errorf("Expected 3 usage values, got %d", len(usageCol))
	}

	hostCol := result.Columns["host"]
	if len(hostCol) != 3 {
		t.Errorf("Expected 3 host values, got %d", len(hostCol))
	}

	// Verify values
	expectedUsages := []float64{75.5, 80.2, 65.0}
	for i, expected := range expectedUsages {
		if usageCol[i].(float64) != expected {
			t.Errorf("Row %d: expected usage %f, got %v", i, expected, usageCol[i])
		}
	}

	expectedHosts := []string{"server01", "server02", "server03"}
	for i, expected := range expectedHosts {
		if hostCol[i].(string) != expected {
			t.Errorf("Row %d: expected host %q, got %v", i, expected, hostCol[i])
		}
	}
}

func TestRowsToColumnar_SchemaVariation(t *testing.T) {
	buffer := createTestArrowBuffer(t)

	// Records with different fields - some have extra fields
	now := time.Now()
	rows := []*models.Record{
		{
			Measurement: "cpu",
			Timestamp:   now.UnixMicro(),
			Fields:      map[string]interface{}{"usage": 75.5}, // Only usage
			Tags:        map[string]string{"host": "server01"},
		},
		{
			Measurement: "cpu",
			Timestamp:   now.Add(time.Second).UnixMicro(),
			Fields:      map[string]interface{}{"usage": 80.2, "system": 15.0}, // Has system
			Tags:        map[string]string{"host": "server02", "region": "us-east"},
		},
		{
			Measurement: "cpu",
			Timestamp:   now.Add(2 * time.Second).UnixMicro(),
			Fields:      map[string]interface{}{"system": 20.0}, // Only system
			Tags:        map[string]string{"region": "us-west"},
		},
	}

	result := buffer.rowsToColumnar("cpu", rows)

	// Verify all fields exist
	if _, ok := result.Columns["usage"]; !ok {
		t.Error("Missing 'usage' column")
	}
	if _, ok := result.Columns["system"]; !ok {
		t.Error("Missing 'system' column")
	}

	// Verify all tags exist (stored directly by name, matching Line Protocol)
	if _, ok := result.Columns["host"]; !ok {
		t.Error("Missing 'host' column")
	}
	if _, ok := result.Columns["region"]; !ok {
		t.Error("Missing 'region' column")
	}

	// Verify nil values for missing fields
	usageCol := result.Columns["usage"]
	if usageCol[0].(float64) != 75.5 {
		t.Errorf("Row 0: expected usage 75.5, got %v", usageCol[0])
	}
	if usageCol[2] != nil {
		t.Errorf("Row 2: expected nil usage, got %v", usageCol[2])
	}

	systemCol := result.Columns["system"]
	if systemCol[0] != nil {
		t.Errorf("Row 0: expected nil system, got %v", systemCol[0])
	}
	if systemCol[1].(float64) != 15.0 {
		t.Errorf("Row 1: expected system 15.0, got %v", systemCol[1])
	}

	// Verify nil values for missing tags
	hostCol := result.Columns["host"]
	if hostCol[2] != nil {
		t.Errorf("Row 2: expected nil host, got %v", hostCol[2])
	}
}

func TestRowsToColumnar_EmptyRecords(t *testing.T) {
	buffer := createTestArrowBuffer(t)

	result := buffer.rowsToColumnar("cpu", []*models.Record{})

	if result.Measurement != "cpu" {
		t.Errorf("Expected measurement 'cpu', got %q", result.Measurement)
	}
	if !result.Columnar {
		t.Error("Expected Columnar to be true")
	}
	if len(result.Columns) != 0 {
		t.Errorf("Expected empty columns, got %d columns", len(result.Columns))
	}
}

func TestRowsToColumnar_TimestampFallback(t *testing.T) {
	buffer := createTestArrowBuffer(t)

	now := time.Now()
	rows := []*models.Record{
		{
			// Has both Timestamp and Time - should prefer Timestamp
			Measurement: "cpu",
			Timestamp:   12345678,
			Time:        now,
			Fields:      map[string]interface{}{"value": 1.0},
		},
		{
			// Only has Time - should convert to micros
			Measurement: "cpu",
			Time:        now,
			Fields:      map[string]interface{}{"value": 2.0},
		},
		{
			// Has neither - should use current time
			Measurement: "cpu",
			Fields:      map[string]interface{}{"value": 3.0},
		},
	}

	result := buffer.rowsToColumnar("cpu", rows)
	timeCol := result.Columns["time"]

	// First row: should use Timestamp directly
	if timeCol[0].(int64) != 12345678 {
		t.Errorf("Row 0: expected timestamp 12345678, got %v", timeCol[0])
	}

	// Second row: should use Time.UnixMicro()
	if timeCol[1].(int64) != now.UnixMicro() {
		t.Errorf("Row 1: expected timestamp %d, got %v", now.UnixMicro(), timeCol[1])
	}

	// Third row: should be a recent timestamp (not zero)
	ts := timeCol[2].(int64)
	if ts == 0 {
		t.Error("Row 2: expected non-zero timestamp")
	}
}

func TestRowsToColumnar_DifferentFieldTypes(t *testing.T) {
	buffer := createTestArrowBuffer(t)

	now := time.Now()
	rows := []*models.Record{
		{
			Measurement: "metrics",
			Timestamp:   now.UnixMicro(),
			Fields: map[string]interface{}{
				"float_val":  3.14159,
				"int_val":    int64(42),
				"string_val": "hello",
				"bool_val":   true,
			},
		},
	}

	result := buffer.rowsToColumnar("metrics", rows)

	// Verify field types are preserved
	if result.Columns["float_val"][0].(float64) != 3.14159 {
		t.Errorf("Expected float 3.14159, got %v", result.Columns["float_val"][0])
	}
	if result.Columns["int_val"][0].(int64) != 42 {
		t.Errorf("Expected int 42, got %v", result.Columns["int_val"][0])
	}
	if result.Columns["string_val"][0].(string) != "hello" {
		t.Errorf("Expected string 'hello', got %v", result.Columns["string_val"][0])
	}
	if result.Columns["bool_val"][0].(bool) != true {
		t.Errorf("Expected bool true, got %v", result.Columns["bool_val"][0])
	}
}

// BenchmarkGetColumnSignature benchmarks the column signature function
func BenchmarkGetColumnSignature(b *testing.B) {
	// Typical columnar payload columns
	columns := map[string]interface{}{
		"time":        nil,
		"host":        nil,
		"region":      nil,
		"datacenter":  nil,
		"usage_idle":  nil,
		"usage_user":  nil,
		"usage_system": nil,
		"usage_iowait": nil,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		getColumnSignature(columns)
	}
}

func TestGetColumnSignature(t *testing.T) {
	tests := []struct {
		name     string
		columns  map[string]interface{}
		expected string
	}{
		{
			name:     "empty columns",
			columns:  map[string]interface{}{},
			expected: "",
		},
		{
			name: "single column",
			columns: map[string]interface{}{
				"value": nil,
			},
			expected: "value",
		},
		{
			name: "multiple columns sorted",
			columns: map[string]interface{}{
				"zebra": nil,
				"apple": nil,
				"mango": nil,
			},
			expected: "apple,mango,zebra",
		},
		{
			name: "skips internal columns",
			columns: map[string]interface{}{
				"value":   nil,
				"time":    nil,
				"_hidden": nil,
			},
			expected: "time,value",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getColumnSignature(tt.columns)
			if result != tt.expected {
				t.Errorf("Expected %q, got %q", tt.expected, result)
			}
		})
	}
}

// BenchmarkRowsToColumnar benchmarks the row-to-columnar conversion
func BenchmarkRowsToColumnar(b *testing.B) {
	buffer := &ArrowBuffer{
		logger: zerolog.New(os.Stderr).Level(zerolog.Disabled),
	}

	// Create 1000 rows
	now := time.Now()
	rows := make([]*models.Record, 1000)
	for i := 0; i < 1000; i++ {
		rows[i] = &models.Record{
			Measurement: "cpu",
			Timestamp:   now.Add(time.Duration(i) * time.Second).UnixMicro(),
			Fields: map[string]interface{}{
				"usage":  float64(i) / 10.0,
				"system": float64(i) / 20.0,
			},
			Tags: map[string]string{
				"host":   "server01",
				"region": "us-east",
			},
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buffer.rowsToColumnar("cpu", rows)
	}
}

// BenchmarkRowsToColumnar_SchemaVariation benchmarks conversion with schema variations
func BenchmarkRowsToColumnar_SchemaVariation(b *testing.B) {
	buffer := &ArrowBuffer{
		logger: zerolog.New(os.Stderr).Level(zerolog.Disabled),
	}

	// Create 1000 rows with varying schemas
	now := time.Now()
	rows := make([]*models.Record, 1000)
	for i := 0; i < 1000; i++ {
		fields := map[string]interface{}{"usage": float64(i) / 10.0}
		if i%2 == 0 {
			fields["system"] = float64(i) / 20.0
		}
		if i%3 == 0 {
			fields["idle"] = float64(100-i) / 10.0
		}

		tags := map[string]string{"host": "server01"}
		if i%4 == 0 {
			tags["region"] = "us-east"
		}

		rows[i] = &models.Record{
			Measurement: "cpu",
			Timestamp:   now.Add(time.Duration(i) * time.Second).UnixMicro(),
			Fields:      fields,
			Tags:        tags,
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buffer.rowsToColumnar("cpu", rows)
	}
}

// TestMergeBatches_SparseColumns tests that mergeBatches handles schema evolution correctly
// This reproduces Issue #130: panic during high-concurrency writes with different column sets
func TestMergeBatches_SparseColumns(t *testing.T) {
	buffer := createTestArrowBuffer(t)

	// Create batches with different column sets (simulating schema evolution)
	batch1 := map[string]interface{}{
		"time": []int64{1, 2, 3},
		"cpu":  []float64{10.0, 20.0, 30.0},
	}
	batch2 := map[string]interface{}{
		"time":        []int64{4, 5},
		"temperature": []float64{70.0, 80.0}, // New column not in batch1
	}
	batch3 := map[string]interface{}{
		"time": []int64{6, 7, 8},
		"cpu":  []float64{40.0, 50.0, 60.0}, // cpu column returns
	}

	batches := []interface{}{batch1, batch2, batch3}

	merged, err := buffer.mergeBatches(batches)
	if err != nil {
		t.Fatalf("mergeBatches failed: %v", err)
	}

	// Verify all columns have the same length (8 total rows)
	expectedLen := 8
	for colName, colData := range merged.Data {
		var actualLen int
		switch col := colData.(type) {
		case []int64:
			actualLen = len(col)
		case []float64:
			actualLen = len(col)
		case []string:
			actualLen = len(col)
		case []bool:
			actualLen = len(col)
		}
		if actualLen != expectedLen {
			t.Errorf("Column %q has length %d, expected %d", colName, actualLen, expectedLen)
		}
	}

	// Verify time column has all values
	timeCol := merged.Data["time"].([]int64)
	expectedTime := []int64{1, 2, 3, 4, 5, 6, 7, 8}
	for i, expected := range expectedTime {
		if timeCol[i] != expected {
			t.Errorf("time[%d] = %d, expected %d", i, timeCol[i], expected)
		}
	}

	// Verify cpu column: data values at positions 0,1,2 and 5,6,7, placeholder zeros at 3,4
	cpuCol := merged.Data["cpu"].([]float64)
	expectedCpu := []float64{10.0, 20.0, 30.0, 0.0, 0.0, 40.0, 50.0, 60.0}
	for i, expected := range expectedCpu {
		if cpuCol[i] != expected {
			t.Errorf("cpu[%d] = %f, expected %f", i, cpuCol[i], expected)
		}
	}

	// Verify cpu validity: positions 3,4 should be null (false)
	cpuValidity := merged.Validity["cpu"]
	if cpuValidity == nil {
		t.Fatal("cpu column should have validity bitmap for sparse positions")
	}
	expectedCpuValidity := []bool{true, true, true, false, false, true, true, true}
	for i, expected := range expectedCpuValidity {
		if cpuValidity[i] != expected {
			t.Errorf("cpu validity[%d] = %v, expected %v", i, cpuValidity[i], expected)
		}
	}

	// Verify temperature column: data values at positions 3,4, placeholder zeros elsewhere
	tempCol := merged.Data["temperature"].([]float64)
	expectedTemp := []float64{0.0, 0.0, 0.0, 70.0, 80.0, 0.0, 0.0, 0.0}
	for i, expected := range expectedTemp {
		if tempCol[i] != expected {
			t.Errorf("temperature[%d] = %f, expected %f", i, tempCol[i], expected)
		}
	}

	// Verify temperature validity: only positions 3,4 should be valid
	tempValidity := merged.Validity["temperature"]
	if tempValidity == nil {
		t.Fatal("temperature column should have validity bitmap for sparse positions")
	}
	expectedTempValidity := []bool{false, false, false, true, true, false, false, false}
	for i, expected := range expectedTempValidity {
		if tempValidity[i] != expected {
			t.Errorf("temperature validity[%d] = %v, expected %v", i, tempValidity[i], expected)
		}
	}

	// Verify time column has NO validity bitmap (always present in all batches)
	if _, hasTimeValidity := merged.Validity["time"]; hasTimeValidity {
		t.Error("time column should not have validity bitmap (present in all batches)")
	}
}

// TestSliceColumnsByIndices_BoundsCheck tests that sliceColumnsByIndices handles sparse columns
func TestSliceColumnsByIndices_BoundsCheck(t *testing.T) {
	// Create columns with different lengths (sparse scenario)
	columns := map[string]interface{}{
		"time":        []int64{1, 2, 3, 4, 5, 6, 7, 8},
		"cpu":         []float64{10.0, 20.0, 30.0, 40.0, 50.0, 60.0}, // Only 6 elements
		"temperature": []float64{70.0, 80.0},                         // Only 2 elements
		"host":        []string{"a", "b", "c", "d"},                  // Only 4 elements
		"active":      []bool{true, false, true},                     // Only 3 elements
	}

	// Try to slice with indices that exceed shorter columns' lengths
	indices := []int{0, 2, 4, 6, 7} // Indices 6,7 exceed temperature, host, active columns

	// This should NOT panic (previously would panic with index out of range)
	result := sliceColumnsByIndices(columns, indices)

	// Verify time column (all indices valid)
	timeCol := result["time"].([]int64)
	expectedTime := []int64{1, 3, 5, 7, 8}
	for i, expected := range expectedTime {
		if timeCol[i] != expected {
			t.Errorf("time[%d] = %d, expected %d", i, timeCol[i], expected)
		}
	}

	// Verify cpu column (indices 0,2,4 valid; 6 invalid -> 0.0)
	cpuCol := result["cpu"].([]float64)
	expectedCpu := []float64{10.0, 30.0, 50.0, 0.0, 0.0} // Last two are zero (out of bounds)
	for i, expected := range expectedCpu {
		if cpuCol[i] != expected {
			t.Errorf("cpu[%d] = %f, expected %f", i, cpuCol[i], expected)
		}
	}

	// Verify temperature column (only indices 0 valid)
	tempCol := result["temperature"].([]float64)
	expectedTemp := []float64{70.0, 0.0, 0.0, 0.0, 0.0} // Only first is valid
	for i, expected := range expectedTemp {
		if tempCol[i] != expected {
			t.Errorf("temperature[%d] = %f, expected %f", i, tempCol[i], expected)
		}
	}

	// Verify host column (indices 0,2 valid)
	hostCol := result["host"].([]string)
	expectedHost := []string{"a", "c", "", "", ""} // Indices 4,6,7 out of bounds
	for i, expected := range expectedHost {
		if hostCol[i] != expected {
			t.Errorf("host[%d] = %q, expected %q", i, hostCol[i], expected)
		}
	}

	// Verify active column (indices 0,2 valid)
	activeCol := result["active"].([]bool)
	expectedActive := []bool{true, true, false, false, false} // Indices 4,6,7 out of bounds
	for i, expected := range expectedActive {
		if activeCol[i] != expected {
			t.Errorf("active[%d] = %v, expected %v", i, activeCol[i], expected)
		}
	}
}
