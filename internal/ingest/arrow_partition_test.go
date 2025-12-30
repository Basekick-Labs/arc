package ingest

import (
	"strings"
	"testing"
	"time"
)

// TestExtractTimeRange tests extracting min/max timestamps from columnar data
func TestExtractTimeRange(t *testing.T) {
	tests := []struct {
		name      string
		columns   map[string]interface{}
		wantMin   int64 // microseconds
		wantMax   int64
		wantError bool
	}{
		{
			name: "simple range",
			columns: map[string]interface{}{
				"time":  []int64{1000, 2000, 3000},
				"value": []float64{1.0, 2.0, 3.0},
			},
			wantMin:   1000,
			wantMax:   3000,
			wantError: false,
		},
		{
			name: "unsorted times",
			columns: map[string]interface{}{
				"time":  []int64{3000, 1000, 2000},
				"value": []float64{1.0, 2.0, 3.0},
			},
			wantMin:   1000,
			wantMax:   3000,
			wantError: false,
		},
		{
			name: "single timestamp",
			columns: map[string]interface{}{
				"time":  []int64{5000},
				"value": []float64{1.0},
			},
			wantMin:   5000,
			wantMax:   5000,
			wantError: false,
		},
		{
			name: "missing time column",
			columns: map[string]interface{}{
				"value": []float64{1.0, 2.0},
			},
			wantError: true,
		},
		{
			name: "empty time column",
			columns: map[string]interface{}{
				"time":  []int64{},
				"value": []float64{},
			},
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			minTime, maxTime, err := extractTimeRange(tt.columns)

			if tt.wantError {
				if err == nil {
					t.Errorf("extractTimeRange() expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("extractTimeRange() unexpected error: %v", err)
				return
			}

			if minTime.UnixMicro() != tt.wantMin {
				t.Errorf("extractTimeRange() minTime = %v, want %v", minTime.UnixMicro(), tt.wantMin)
			}

			if maxTime.UnixMicro() != tt.wantMax {
				t.Errorf("extractTimeRange() maxTime = %v, want %v", maxTime.UnixMicro(), tt.wantMax)
			}
		})
	}
}

// TestSortColumnsByTime tests sorting columnar data by time
func TestSortColumnsByTime(t *testing.T) {
	tests := []struct {
		name      string
		columns   map[string]interface{}
		wantTimes []int64
		wantError bool
	}{
		{
			name: "already sorted",
			columns: map[string]interface{}{
				"time":  []int64{1000, 2000, 3000},
				"value": []float64{10.0, 20.0, 30.0},
			},
			wantTimes: []int64{1000, 2000, 3000},
			wantError: false,
		},
		{
			name: "reverse sorted",
			columns: map[string]interface{}{
				"time":  []int64{3000, 2000, 1000},
				"value": []float64{30.0, 20.0, 10.0},
			},
			wantTimes: []int64{1000, 2000, 3000},
			wantError: false,
		},
		{
			name: "random order with multiple columns",
			columns: map[string]interface{}{
				"time":   []int64{2000, 1000, 3000},
				"value":  []float64{20.0, 10.0, 30.0},
				"name":   []string{"b", "a", "c"},
				"active": []bool{false, true, false},
			},
			wantTimes: []int64{1000, 2000, 3000},
			wantError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sorted, err := sortColumnsByTime(tt.columns)

			if tt.wantError {
				if err == nil {
					t.Errorf("sortColumnsByTime() expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("sortColumnsByTime() unexpected error: %v", err)
				return
			}

			// Check that times are sorted
			times := sorted["time"].([]int64)
			if len(times) != len(tt.wantTimes) {
				t.Errorf("sortColumnsByTime() got %d times, want %d", len(times), len(tt.wantTimes))
				return
			}

			for i, wantTime := range tt.wantTimes {
				if times[i] != wantTime {
					t.Errorf("sortColumnsByTime() time[%d] = %v, want %v", i, times[i], wantTime)
				}
			}

			// Verify all columns have the same length
			numRows := len(times)
			for colName, colData := range sorted {
				var colLen int
				switch col := colData.(type) {
				case []int64:
					colLen = len(col)
				case []float64:
					colLen = len(col)
				case []string:
					colLen = len(col)
				case []bool:
					colLen = len(col)
				}

				if colLen != numRows {
					t.Errorf("sortColumnsByTime() column %s has length %d, want %d", colName, colLen, numRows)
				}
			}

			// For the detailed test, verify the permutation was applied correctly
			if tt.name == "random order with multiple columns" {
				values := sorted["value"].([]float64)
				names := sorted["name"].([]string)
				expected := []float64{10.0, 20.0, 30.0}
				expectedNames := []string{"a", "b", "c"}

				for i := range values {
					if values[i] != expected[i] {
						t.Errorf("sortColumnsByTime() value[%d] = %v, want %v", i, values[i], expected[i])
					}
					if names[i] != expectedNames[i] {
						t.Errorf("sortColumnsByTime() name[%d] = %v, want %v", i, names[i], expectedNames[i])
					}
				}
			}
		})
	}
}

// TestFindHourBoundaries tests finding hour partition boundaries
func TestFindHourBoundaries(t *testing.T) {
	// Helper to create microsecond timestamps
	microTime := func(year, month, day, hour, min, sec int) int64 {
		return time.Date(year, time.Month(month), day, hour, min, sec, 0, time.UTC).UnixMicro()
	}

	tests := []struct {
		name          string
		times         []int64
		wantNumSplits int
		wantHourKeys  []string
	}{
		{
			name: "single hour",
			times: []int64{
				microTime(2024, 1, 1, 10, 0, 0),
				microTime(2024, 1, 1, 10, 30, 0),
				microTime(2024, 1, 1, 10, 59, 59),
			},
			wantNumSplits: 1,
			wantHourKeys:  []string{"2024010110"},
		},
		{
			name: "two consecutive hours",
			times: []int64{
				microTime(2024, 1, 1, 10, 30, 0),
				microTime(2024, 1, 1, 10, 59, 59),
				microTime(2024, 1, 1, 11, 0, 0),
				microTime(2024, 1, 1, 11, 30, 0),
			},
			wantNumSplits: 2,
			wantHourKeys:  []string{"2024010110", "2024010111"},
		},
		{
			name: "three hours with gaps",
			times: []int64{
				microTime(2024, 1, 1, 10, 0, 0),
				microTime(2024, 1, 1, 12, 0, 0),
				microTime(2024, 1, 1, 14, 0, 0),
			},
			wantNumSplits: 3,
			wantHourKeys:  []string{"2024010110", "2024010112", "2024010114"},
		},
		{
			name: "hour boundary exact",
			times: []int64{
				microTime(2024, 1, 1, 10, 59, 59),
				microTime(2024, 1, 1, 11, 0, 0),
			},
			wantNumSplits: 2,
			wantHourKeys:  []string{"2024010110", "2024010111"},
		},
		{
			name: "day boundary crossing",
			times: []int64{
				microTime(2024, 1, 1, 23, 30, 0),
				microTime(2024, 1, 2, 0, 30, 0),
			},
			wantNumSplits: 2,
			wantHourKeys:  []string{"2024010123", "2024010200"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			boundaries, err := findHourBoundaries(tt.times)
			if err != nil {
				t.Errorf("findHourBoundaries() unexpected error: %v", err)
				return
			}

			if len(boundaries) != tt.wantNumSplits {
				t.Errorf("findHourBoundaries() got %d boundaries, want %d", len(boundaries), tt.wantNumSplits)
				return
			}

			for i, boundary := range boundaries {
				if boundary.hourKey != tt.wantHourKeys[i] {
					t.Errorf("findHourBoundaries() boundary[%d].hourKey = %v, want %v", i, boundary.hourKey, tt.wantHourKeys[i])
				}

				// Verify indices are valid
				if boundary.startIdx < 0 || boundary.endIdx > len(tt.times) {
					t.Errorf("findHourBoundaries() boundary[%d] invalid indices: start=%d, end=%d, len=%d",
						i, boundary.startIdx, boundary.endIdx, len(tt.times))
				}

				if boundary.startIdx >= boundary.endIdx {
					t.Errorf("findHourBoundaries() boundary[%d] startIdx >= endIdx: start=%d, end=%d",
						i, boundary.startIdx, boundary.endIdx)
				}
			}

			// Verify boundaries cover all data
			if boundaries[0].startIdx != 0 {
				t.Errorf("findHourBoundaries() first boundary doesn't start at 0: start=%d", boundaries[0].startIdx)
			}
			if boundaries[len(boundaries)-1].endIdx != len(tt.times) {
				t.Errorf("findHourBoundaries() last boundary doesn't end at len(times): end=%d, len=%d",
					boundaries[len(boundaries)-1].endIdx, len(tt.times))
			}

			// Verify boundaries don't overlap and have no gaps
			for i := 1; i < len(boundaries); i++ {
				if boundaries[i].startIdx != boundaries[i-1].endIdx {
					t.Errorf("findHourBoundaries() gap/overlap between boundaries[%d] and boundaries[%d]: prev.end=%d, curr.start=%d",
						i-1, i, boundaries[i-1].endIdx, boundaries[i].startIdx)
				}
			}
		})
	}
}

// TestSplitColumnsByBoundaries tests splitting columns by hour boundaries
func TestSplitColumnsByBoundaries(t *testing.T) {
	microTime := func(year, month, day, hour, min, sec int) int64 {
		return time.Date(year, time.Month(month), day, hour, min, sec, 0, time.UTC).UnixMicro()
	}

	columns := map[string]interface{}{
		"time": []int64{
			microTime(2024, 1, 1, 10, 0, 0),
			microTime(2024, 1, 1, 10, 30, 0),
			microTime(2024, 1, 1, 11, 0, 0),
			microTime(2024, 1, 1, 11, 30, 0),
		},
		"value": []float64{1.0, 2.0, 3.0, 4.0},
		"name":  []string{"a", "b", "c", "d"},
	}

	times := columns["time"].([]int64)
	boundaries, err := findHourBoundaries(times)
	if err != nil {
		t.Fatalf("findHourBoundaries() failed: %v", err)
	}

	splits := splitColumnsByBoundaries(columns, boundaries)

	if len(splits) != 2 {
		t.Errorf("splitColumnsByBoundaries() got %d splits, want 2", len(splits))
		return
	}

	// Check first hour (10:00-10:59)
	hour10 := splits["2024010110"]
	if hour10 == nil {
		t.Errorf("splitColumnsByBoundaries() missing split for hour 2024010110")
		return
	}

	time10 := hour10["time"].([]int64)
	value10 := hour10["value"].([]float64)
	name10 := hour10["name"].([]string)

	if len(time10) != 2 {
		t.Errorf("splitColumnsByBoundaries() hour 10 has %d records, want 2", len(time10))
	}

	if value10[0] != 1.0 || value10[1] != 2.0 {
		t.Errorf("splitColumnsByBoundaries() hour 10 values = %v, want [1.0, 2.0]", value10)
	}

	if name10[0] != "a" || name10[1] != "b" {
		t.Errorf("splitColumnsByBoundaries() hour 10 names = %v, want [a, b]", name10)
	}

	// Check second hour (11:00-11:59)
	hour11 := splits["2024010111"]
	if hour11 == nil {
		t.Errorf("splitColumnsByBoundaries() missing split for hour 2024010111")
		return
	}

	time11 := hour11["time"].([]int64)
	value11 := hour11["value"].([]float64)
	name11 := hour11["name"].([]string)

	if len(time11) != 2 {
		t.Errorf("splitColumnsByBoundaries() hour 11 has %d records, want 2", len(time11))
	}

	if value11[0] != 3.0 || value11[1] != 4.0 {
		t.Errorf("splitColumnsByBoundaries() hour 11 values = %v, want [3.0, 4.0]", value11)
	}

	if name11[0] != "c" || name11[1] != "d" {
		t.Errorf("splitColumnsByBoundaries() hour 11 names = %v, want [c, d]", name11)
	}
}

// TestGenerateStoragePath tests the storage path generation with data time
func TestGenerateStoragePath(t *testing.T) {
	buffer := &ArrowBuffer{}

	testTime := time.Date(2024, 11, 25, 16, 30, 45, 0, time.UTC)
	path := buffer.generateStoragePath("mydb", "cpu", testTime)

	// Should contain the date/hour from the partition time
	expectedPrefix := "mydb/cpu/2024/11/25/16/"
	if len(path) < len(expectedPrefix) || path[:len(expectedPrefix)] != expectedPrefix {
		t.Errorf("generateStoragePath() path = %v, want prefix %v", path, expectedPrefix)
	}

	// Should end with .parquet
	if path[len(path)-8:] != ".parquet" {
		t.Errorf("generateStoragePath() path doesn't end with .parquet: %v", path)
	}
}

// TestSortedOutputVerification verifies that sorting maintains data integrity
func TestSortedOutputVerification(t *testing.T) {
	// Create unsorted data
	columns := map[string]interface{}{
		"time":   []int64{3000, 1000, 2000, 5000, 4000},
		"value":  []float64{30.0, 10.0, 20.0, 50.0, 40.0},
		"host":   []string{"c", "a", "b", "e", "d"},
		"active": []bool{false, true, false, true, false},
	}

	// Sort
	sorted, err := sortColumnsByTime(columns)
	if err != nil {
		t.Fatalf("sortColumnsByTime() failed: %v", err)
	}

	// Verify times are sorted
	times := sorted["time"].([]int64)
	for i := 1; i < len(times); i++ {
		if times[i] < times[i-1] {
			t.Errorf("sortColumnsByTime() times not sorted at index %d: %v < %v", i, times[i], times[i-1])
		}
	}

	// Verify all columns were permuted correctly
	values := sorted["value"].([]float64)
	hosts := sorted["host"].([]string)
	active := sorted["active"].([]bool)

	expectedTimes := []int64{1000, 2000, 3000, 4000, 5000}
	expectedValues := []float64{10.0, 20.0, 30.0, 40.0, 50.0}
	expectedHosts := []string{"a", "b", "c", "d", "e"}
	expectedActive := []bool{true, false, false, false, true}

	for i := range times {
		if times[i] != expectedTimes[i] {
			t.Errorf("sortColumnsByTime() time[%d] = %v, want %v", i, times[i], expectedTimes[i])
		}
		if values[i] != expectedValues[i] {
			t.Errorf("sortColumnsByTime() value[%d] = %v, want %v", i, values[i], expectedValues[i])
		}
		if hosts[i] != expectedHosts[i] {
			t.Errorf("sortColumnsByTime() host[%d] = %v, want %v", i, hosts[i], expectedHosts[i])
		}
		if active[i] != expectedActive[i] {
			t.Errorf("sortColumnsByTime() active[%d] = %v, want %v", i, active[i], expectedActive[i])
		}
	}
}

// TestSortPreservesAlreadySortedData verifies no unnecessary work for sorted data
func TestSortPreservesAlreadySortedData(t *testing.T) {
	columns := map[string]interface{}{
		"time":  []int64{1000, 2000, 3000, 4000},
		"value": []float64{10.0, 20.0, 30.0, 40.0},
	}

	sorted, err := sortColumnsByTime(columns)
	if err != nil {
		t.Fatalf("sortColumnsByTime() failed: %v", err)
	}

	times := sorted["time"].([]int64)
	values := sorted["value"].([]float64)

	// Verify data is unchanged
	expectedTimes := []int64{1000, 2000, 3000, 4000}
	expectedValues := []float64{10.0, 20.0, 30.0, 40.0}

	for i := range times {
		if times[i] != expectedTimes[i] {
			t.Errorf("sortColumnsByTime() modified already sorted time[%d] = %v, want %v", i, times[i], expectedTimes[i])
		}
		if values[i] != expectedValues[i] {
			t.Errorf("sortColumnsByTime() modified already sorted value[%d] = %v, want %v", i, values[i], expectedValues[i])
		}
	}
}

// BenchmarkSortColumnsByTime benchmarks the sorting performance
func BenchmarkSortColumnsByTime(b *testing.B) {
	sizes := []int{100, 1000, 10000, 100000}

	for _, size := range sizes {
		b.Run(string(rune(size)), func(b *testing.B) {
			// Create unsorted test data
			times := make([]int64, size)
			values := make([]float64, size)
			names := make([]string, size)

			now := time.Now().UnixMicro()
			for i := 0; i < size; i++ {
				times[i] = now + int64(size-i)*1000 // Reverse order
				values[i] = float64(i)
				names[i] = string(rune('a' + (i % 26)))
			}

			columns := map[string]interface{}{
				"time":  times,
				"value": values,
				"name":  names,
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _ = sortColumnsByTime(columns)
			}
		})
	}
}

// BenchmarkFindHourBoundaries benchmarks the boundary finding performance
func BenchmarkFindHourBoundaries(b *testing.B) {
	// Create sorted times spanning 24 hours
	size := 100000
	times := make([]int64, size)
	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).UnixMicro()

	for i := 0; i < size; i++ {
		// Spread evenly across 24 hours
		times[i] = baseTime + int64(i)*(24*3600*1000000)/int64(size)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = findHourBoundaries(times)
	}
}

// TestSortColumnsByKeys tests multi-key sorting
func TestSortColumnsByKeys(t *testing.T) {
	tests := []struct {
		name          string
		columns       map[string]interface{}
		sortKeys      []string
		wantOrder     []int // Expected row order after sort
		wantError     bool
		errorContains string
	}{
		{
			name: "sort by sensor_id then time",
			columns: map[string]interface{}{
				"tag_sensor_id": []string{"B", "A", "B", "A"},
				"time":          []int64{2000, 1000, 1000, 2000},
				"value":         []float64{20.0, 10.0, 30.0, 40.0},
			},
			sortKeys: []string{"tag_sensor_id", "time"},
			// Expected order: A,1000,10.0 -> A,2000,40.0 -> B,1000,30.0 -> B,2000,20.0
			wantOrder: []int{1, 3, 2, 0},
		},
		{
			name: "sort by time only (single key)",
			columns: map[string]interface{}{
				"tag_sensor_id": []string{"B", "A", "C"},
				"time":          []int64{3000, 1000, 2000},
				"value":         []float64{3.0, 1.0, 2.0},
			},
			sortKeys:  []string{"time"},
			wantOrder: []int{1, 2, 0}, // 1000, 2000, 3000
		},
		{
			name: "sort by int64 column then time",
			columns: map[string]interface{}{
				"device_id": []int64{2, 1, 2, 1},
				"time":      []int64{4000, 3000, 2000, 1000},
				"value":     []float64{4.0, 3.0, 2.0, 1.0},
			},
			sortKeys:  []string{"device_id", "time"},
			wantOrder: []int{3, 1, 2, 0}, // device_id=1,time=1000 -> 1,3000 -> 2,2000 -> 2,4000
		},
		{
			name: "sort by float64 column",
			columns: map[string]interface{}{
				"priority": []float64{3.5, 1.2, 2.7},
				"time":     []int64{1000, 2000, 3000},
			},
			sortKeys:  []string{"priority", "time"},
			wantOrder: []int{1, 2, 0}, // 1.2, 2.7, 3.5
		},
		{
			name: "sort by bool column then time",
			columns: map[string]interface{}{
				"active": []bool{true, false, true, false},
				"time":   []int64{4000, 3000, 2000, 1000},
			},
			sortKeys:  []string{"active", "time"},
			wantOrder: []int{3, 1, 2, 0}, // false,1000 -> false,3000 -> true,2000 -> true,4000
		},
		{
			name: "three-key sort",
			columns: map[string]interface{}{
				"region": []string{"US", "EU", "US", "EU"},
				"host":   []string{"host1", "host1", "host2", "host2"},
				"time":   []int64{2000, 1000, 4000, 3000},
			},
			sortKeys:  []string{"region", "host", "time"},
			wantOrder: []int{1, 3, 0, 2}, // EU,host1,1000 -> EU,host2,3000 -> US,host1,2000 -> US,host2,4000
		},
		{
			name: "missing sort key column",
			columns: map[string]interface{}{
				"time":  []int64{1000, 2000},
				"value": []float64{1.0, 2.0},
			},
			sortKeys:      []string{"nonexistent", "time"},
			wantError:     true,
			errorContains: "sort key column not found",
		},
		{
			name: "no sort keys provided",
			columns: map[string]interface{}{
				"time": []int64{1000, 2000},
			},
			sortKeys:      []string{},
			wantError:     true,
			errorContains: "no sort keys provided",
		},
		{
			name: "empty data",
			columns: map[string]interface{}{
				"time":  []int64{},
				"value": []float64{},
			},
			sortKeys:  []string{"time"},
			wantOrder: []int{}, // No change, empty
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sorted, err := sortColumnsByKeys(tt.columns, tt.sortKeys)

			if tt.wantError {
				if err == nil {
					t.Errorf("sortColumnsByKeys() expected error but got none")
					return
				}
				if tt.errorContains != "" && !strings.Contains(err.Error(), tt.errorContains) {
					t.Errorf("sortColumnsByKeys() error = %v, want error containing %q", err, tt.errorContains)
				}
				return
			}

			if err != nil {
				t.Errorf("sortColumnsByKeys() unexpected error: %v", err)
				return
			}

			// Verify sort order by checking all columns
			if len(tt.wantOrder) > 0 {
				// Check that data is in expected order
				for colName, colData := range sorted {
					switch col := colData.(type) {
					case []int64:
						for i, expectedIdx := range tt.wantOrder {
							originalCol := tt.columns[colName].([]int64)
							if col[i] != originalCol[expectedIdx] {
								t.Errorf("sortColumnsByKeys() column %s[%d] = %v, want %v (original[%d])",
									colName, i, col[i], originalCol[expectedIdx], expectedIdx)
							}
						}
					case []float64:
						for i, expectedIdx := range tt.wantOrder {
							originalCol := tt.columns[colName].([]float64)
							if col[i] != originalCol[expectedIdx] {
								t.Errorf("sortColumnsByKeys() column %s[%d] = %v, want %v (original[%d])",
									colName, i, col[i], originalCol[expectedIdx], expectedIdx)
							}
						}
					case []string:
						for i, expectedIdx := range tt.wantOrder {
							originalCol := tt.columns[colName].([]string)
							if col[i] != originalCol[expectedIdx] {
								t.Errorf("sortColumnsByKeys() column %s[%d] = %v, want %v (original[%d])",
									colName, i, col[i], originalCol[expectedIdx], expectedIdx)
							}
						}
					case []bool:
						for i, expectedIdx := range tt.wantOrder {
							originalCol := tt.columns[colName].([]bool)
							if col[i] != originalCol[expectedIdx] {
								t.Errorf("sortColumnsByKeys() column %s[%d] = %v, want %v (original[%d])",
									colName, i, col[i], originalCol[expectedIdx], expectedIdx)
							}
						}
					}
				}
			}
		})
	}
}

// BenchmarkSortColumnsByKeys benchmarks multi-key sorting performance
func BenchmarkSortColumnsByKeys(b *testing.B) {
	sizes := []int{100, 1000, 10000}

	for _, size := range sizes {
		b.Run("size_"+string(rune(size)), func(b *testing.B) {
			// Create test data
			columns := map[string]interface{}{
				"tag_sensor_id": make([]string, size),
				"time":          make([]int64, size),
				"value":         make([]float64, size),
			}

			// Populate with semi-random data (deterministic for consistency)
			for i := 0; i < size; i++ {
				columns["tag_sensor_id"].([]string)[i] = "sensor_" + string(rune('A'+i%26))
				columns["time"].([]int64)[i] = int64(size - i) // Reverse order
				columns["value"].([]float64)[i] = float64(i) * 1.5
			}

			sortKeys := []string{"tag_sensor_id", "time"}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _ = sortColumnsByKeys(columns, sortKeys)
			}
		})
	}
}

