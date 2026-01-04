package ingest

import (
	"strings"
	"testing"
	"time"
)

// TestGetSortKeys tests the getSortKeys function ensures time is always included
func TestGetSortKeys(t *testing.T) {
	tests := []struct {
		name            string
		measurement     string
		sortKeysConfig  map[string][]string
		defaultSortKeys []string
		wantKeys        []string
	}{
		{
			name:            "empty config defaults to time",
			measurement:     "cpu",
			sortKeysConfig:  map[string][]string{},
			defaultSortKeys: []string{"time"},
			wantKeys:        []string{"time"},
		},
		{
			name:            "single key without time appends time",
			measurement:     "sensor",
			sortKeysConfig:  map[string][]string{"sensor": {"tag_id"}},
			defaultSortKeys: []string{"time"},
			wantKeys:        []string{"tag_id", "time"},
		},
		{
			name:            "multiple keys without time appends time",
			measurement:     "metrics",
			sortKeysConfig:  map[string][]string{"metrics": {"tag_host", "tag_region"}},
			defaultSortKeys: []string{"time"},
			wantKeys:        []string{"tag_host", "tag_region", "time"},
		},
		{
			name:            "keys with time unchanged",
			measurement:     "temp",
			sortKeysConfig:  map[string][]string{"temp": {"tag_sensor_id", "time"}},
			defaultSortKeys: []string{"time"},
			wantKeys:        []string{"tag_sensor_id", "time"},
		},
		{
			name:            "time only unchanged",
			measurement:     "humidity",
			sortKeysConfig:  map[string][]string{"humidity": {"time"}},
			defaultSortKeys: []string{"time"},
			wantKeys:        []string{"time"},
		},
		{
			name:            "time as second key unchanged",
			measurement:     "pressure",
			sortKeysConfig:  map[string][]string{"pressure": {"tag_location", "time"}},
			defaultSortKeys: []string{"time"},
			wantKeys:        []string{"tag_location", "time"},
		},
		{
			name:            "unmapped measurement uses default",
			measurement:     "unmapped",
			sortKeysConfig:  map[string][]string{"other": {"tag_foo"}},
			defaultSortKeys: []string{"tag_bar", "time"},
			wantKeys:        []string{"tag_bar", "time"},
		},
		{
			name:            "default without time gets time appended",
			measurement:     "custom",
			sortKeysConfig:  map[string][]string{},
			defaultSortKeys: []string{"tag_device"},
			wantKeys:        []string{"tag_device", "time"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a minimal buffer with just the fields we need
			buffer := &ArrowBuffer{
				sortKeysConfig:  tt.sortKeysConfig,
				defaultSortKeys: tt.defaultSortKeys,
			}

			got := buffer.getSortKeys(tt.measurement)

			if len(got) != len(tt.wantKeys) {
				t.Errorf("getSortKeys() returned %d keys, want %d: got %v, want %v", len(got), len(tt.wantKeys), got, tt.wantKeys)
				return
			}

			for i, key := range got {
				if key != tt.wantKeys[i] {
					t.Errorf("getSortKeys()[%d] = %q, want %q", i, key, tt.wantKeys[i])
				}
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

// TestGroupByHourWithMultiKeySort tests that groupByHour correctly handles
// multi-hour batches with multi-key sorting where times are NOT globally sorted
func TestGroupByHourWithMultiKeySort(t *testing.T) {
	// Helper to create microsecond timestamps
	microTime := func(year, month, day, hour, min, sec int) int64 {
		return time.Date(year, time.Month(month), day, hour, min, sec, 0, time.UTC).UnixMicro()
	}

	// Create data spanning 2 hours (10:00 and 11:00) with multiple sensors
	// When sorted by ["tag_sensor_id", "time"], times will NOT be globally sorted
	// Example: [A@10:30, A@11:15, B@10:15, B@11:45, C@10:45, C@11:30]
	// Times:   [10:30,   11:15,   10:15,   11:45,   10:45,   11:30] <- NOT sorted!
	times := []int64{
		microTime(2024, 1, 1, 10, 30, 0), // sensor_A, hour 10
		microTime(2024, 1, 1, 11, 15, 0), // sensor_A, hour 11
		microTime(2024, 1, 1, 10, 15, 0), // sensor_B, hour 10
		microTime(2024, 1, 1, 11, 45, 0), // sensor_B, hour 11
		microTime(2024, 1, 1, 10, 45, 0), // sensor_C, hour 10
		microTime(2024, 1, 1, 11, 30, 0), // sensor_C, hour 11
	}

	// Call groupByHour
	buckets, globalMin, globalMax, err := groupByHour(times)
	if err != nil {
		t.Fatalf("groupByHour() unexpected error: %v", err)
	}

	// Verify global min/max
	expectedMin := microTime(2024, 1, 1, 10, 15, 0) // sensor_B at 10:15
	expectedMax := microTime(2024, 1, 1, 11, 45, 0) // sensor_B at 11:45
	if globalMin != expectedMin {
		t.Errorf("groupByHour() globalMin = %v, want %v",
			time.UnixMicro(globalMin), time.UnixMicro(expectedMin))
	}
	if globalMax != expectedMax {
		t.Errorf("groupByHour() globalMax = %v, want %v",
			time.UnixMicro(globalMax), time.UnixMicro(expectedMax))
	}

	// Verify we got exactly 2 hour buckets
	if len(buckets) != 2 {
		t.Fatalf("groupByHour() got %d buckets, want 2", len(buckets))
	}

	// Calculate hourIDs using the same formula as groupByHour
	// hourID = timestamp / microPerHour (where microPerHour = 3600_000_000)
	hour10ID := microTime(2024, 1, 1, 10, 0, 0) / microPerHour
	hour11ID := microTime(2024, 1, 1, 11, 0, 0) / microPerHour

	// Verify hour 10 bucket
	hour10, exists := buckets[hour10ID]
	if !exists {
		t.Fatalf("groupByHour() missing bucket for hour 10 (ID=%d)", hour10ID)
	}

	// Hour 10 should have indices 0, 2, 4 (sensors A, B, C at 10:xx)
	expectedIndices10 := []int{0, 2, 4}
	if len(hour10.indices) != len(expectedIndices10) {
		t.Errorf("hour 10 got %d indices, want %d", len(hour10.indices), len(expectedIndices10))
	}
	for i, expectedIdx := range expectedIndices10 {
		if hour10.indices[i] != expectedIdx {
			t.Errorf("hour 10 indices[%d] = %d, want %d", i, hour10.indices[i], expectedIdx)
		}
	}

	// Verify hour 10 min/max
	if hour10.minTime != microTime(2024, 1, 1, 10, 15, 0) {
		t.Errorf("hour 10 minTime = %v, want 10:15:00", time.UnixMicro(hour10.minTime))
	}
	if hour10.maxTime != microTime(2024, 1, 1, 10, 45, 0) {
		t.Errorf("hour 10 maxTime = %v, want 10:45:00", time.UnixMicro(hour10.maxTime))
	}

	// Verify hour 11 bucket
	hour11, exists := buckets[hour11ID]
	if !exists {
		t.Fatalf("groupByHour() missing bucket for hour 11 (ID=%d)", hour11ID)
	}

	// Hour 11 should have indices 1, 3, 5 (sensors A, B, C at 11:xx)
	expectedIndices11 := []int{1, 3, 5}
	if len(hour11.indices) != len(expectedIndices11) {
		t.Errorf("hour 11 got %d indices, want %d", len(hour11.indices), len(expectedIndices11))
	}
	for i, expectedIdx := range expectedIndices11 {
		if hour11.indices[i] != expectedIdx {
			t.Errorf("hour 11 indices[%d] = %d, want %d", i, hour11.indices[i], expectedIdx)
		}
	}

	// Verify hour 11 min/max
	if hour11.minTime != microTime(2024, 1, 1, 11, 15, 0) {
		t.Errorf("hour 11 minTime = %v, want 11:15:00", time.UnixMicro(hour11.minTime))
	}
	if hour11.maxTime != microTime(2024, 1, 1, 11, 45, 0) {
		t.Errorf("hour 11 maxTime = %v, want 11:45:00", time.UnixMicro(hour11.maxTime))
	}
}

// TestSliceColumnsByIndices tests extracting rows by index list
func TestSliceColumnsByIndices(t *testing.T) {
	// Create sample data
	columns := map[string]interface{}{
		"time":           []int64{100, 200, 300, 400, 500, 600},
		"tag_sensor_id":  []string{"A", "A", "B", "B", "C", "C"},
		"value":          []float64{1.1, 2.2, 3.3, 4.4, 5.5, 6.6},
		"tag_location":   []string{"room1", "room2", "room1", "room2", "room1", "room2"},
	}

	// Extract indices [0, 2, 4] (first occurrence of each sensor)
	indices := []int{0, 2, 4}
	result := sliceColumnsByIndices(columns, indices)

	// Verify all columns were sliced
	if len(result) != len(columns) {
		t.Fatalf("sliceColumnsByIndices() got %d columns, want %d", len(result), len(columns))
	}

	// Verify time column
	resultTime := result["time"].([]int64)
	expectedTime := []int64{100, 300, 500}
	if len(resultTime) != len(expectedTime) {
		t.Errorf("time column length = %d, want %d", len(resultTime), len(expectedTime))
	}
	for i, want := range expectedTime {
		if resultTime[i] != want {
			t.Errorf("time[%d] = %d, want %d", i, resultTime[i], want)
		}
	}

	// Verify sensor_id column
	resultSensors := result["tag_sensor_id"].([]string)
	expectedSensors := []string{"A", "B", "C"}
	if len(resultSensors) != len(expectedSensors) {
		t.Errorf("sensor_id column length = %d, want %d", len(resultSensors), len(expectedSensors))
	}
	for i, want := range expectedSensors {
		if resultSensors[i] != want {
			t.Errorf("sensor_id[%d] = %s, want %s", i, resultSensors[i], want)
		}
	}

	// Verify value column
	resultValues := result["value"].([]float64)
	expectedValues := []float64{1.1, 3.3, 5.5}
	if len(resultValues) != len(expectedValues) {
		t.Errorf("value column length = %d, want %d", len(resultValues), len(expectedValues))
	}
	for i, want := range expectedValues {
		if resultValues[i] != want {
			t.Errorf("value[%d] = %f, want %f", i, resultValues[i], want)
		}
	}

	// Verify location column
	resultLocations := result["tag_location"].([]string)
	expectedLocations := []string{"room1", "room1", "room1"}
	if len(resultLocations) != len(expectedLocations) {
		t.Errorf("location column length = %d, want %d", len(resultLocations), len(expectedLocations))
	}
	for i, want := range expectedLocations {
		if resultLocations[i] != want {
			t.Errorf("location[%d] = %s, want %s", i, resultLocations[i], want)
		}
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

