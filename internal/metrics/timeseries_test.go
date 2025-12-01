package metrics

import (
	"testing"
	"time"
)

func TestNewTimeSeriesBuffer(t *testing.T) {
	buf := NewTimeSeriesBuffer(100, time.Second)
	if buf == nil {
		t.Fatal("NewTimeSeriesBuffer returned nil")
	}
	if buf.size != 100 {
		t.Errorf("buffer size = %d, want 100", buf.size)
	}
	if buf.interval != time.Second {
		t.Errorf("buffer interval = %v, want %v", buf.interval, time.Second)
	}
	if len(buf.points) != 100 {
		t.Errorf("points slice length = %d, want 100", len(buf.points))
	}
}

func TestTimeSeriesBuffer_Add(t *testing.T) {
	buf := NewTimeSeriesBuffer(5, time.Second)

	// Add a point
	now := time.Now()
	point := TimeSeriesPoint{
		Timestamp: now,
		Values: map[string]interface{}{
			"test_metric": 42,
		},
	}
	buf.Add(point)

	if buf.count != 1 {
		t.Errorf("count = %d, want 1", buf.count)
	}
	if buf.writePos != 1 {
		t.Errorf("writePos = %d, want 1", buf.writePos)
	}
}

func TestTimeSeriesBuffer_RingBuffer(t *testing.T) {
	buf := NewTimeSeriesBuffer(3, time.Second)

	// Add more points than buffer size
	for i := 0; i < 5; i++ {
		buf.Add(TimeSeriesPoint{
			Timestamp: time.Now(),
			Values:    map[string]interface{}{"value": i},
		})
	}

	// Count should be capped at buffer size
	if buf.count != 3 {
		t.Errorf("count = %d, want 3 (buffer size)", buf.count)
	}

	// writePos should wrap around
	if buf.writePos != 2 { // 5 % 3 = 2
		t.Errorf("writePos = %d, want 2", buf.writePos)
	}
}

func TestTimeSeriesBuffer_GetRecent(t *testing.T) {
	buf := NewTimeSeriesBuffer(10, time.Second)

	// Add points with timestamps spread over time
	baseTime := time.Now().Add(-5 * time.Minute)
	for i := 0; i < 6; i++ {
		buf.Add(TimeSeriesPoint{
			Timestamp: baseTime.Add(time.Duration(i) * time.Minute),
			Values:    map[string]interface{}{"minute": i},
		})
	}

	// Get last 3 minutes (should include minutes 3, 4, 5)
	recent := buf.GetRecent(3)
	if len(recent) != 3 {
		t.Errorf("GetRecent(3) returned %d points, want 3", len(recent))
	}

	// Get all (should include all 6 points)
	all := buf.GetRecent(10)
	if len(all) != 6 {
		t.Errorf("GetRecent(10) returned %d points, want 6", len(all))
	}
}

func TestNewTimeSeriesCollector(t *testing.T) {
	collector := NewTimeSeriesCollector(100, 5*time.Second)
	if collector == nil {
		t.Fatal("NewTimeSeriesCollector returned nil")
	}
	if collector.system == nil {
		t.Error("system buffer is nil")
	}
	if collector.application == nil {
		t.Error("application buffer is nil")
	}
	if collector.api == nil {
		t.Error("api buffer is nil")
	}
	if collector.interval != 5*time.Second {
		t.Errorf("interval = %v, want 5s", collector.interval)
	}
}

func TestTimeSeriesCollector_StartStop(t *testing.T) {
	collector := NewTimeSeriesCollector(10, 100*time.Millisecond)

	collector.Start()

	// Wait for at least one collection cycle
	time.Sleep(150 * time.Millisecond)

	collector.Stop()

	// Verify that data was collected
	systemData := collector.GetSystem(1)
	if len(systemData) == 0 {
		t.Error("No system data collected")
	}

	appData := collector.GetApplication(1)
	if len(appData) == 0 {
		t.Error("No application data collected")
	}

	apiData := collector.GetAPI(1)
	if len(apiData) == 0 {
		t.Error("No API data collected")
	}
}

func TestTimeSeriesCollector_CollectedMetrics(t *testing.T) {
	collector := NewTimeSeriesCollector(10, 100*time.Millisecond)

	collector.Start()
	time.Sleep(150 * time.Millisecond)
	collector.Stop()

	// Verify system metrics contain expected fields
	systemData := collector.GetSystem(1)
	if len(systemData) > 0 {
		values := systemData[0].Values
		expectedSystemKeys := []string{
			"goroutines",
			"memory_alloc_mb",
			"memory_heap_mb",
			"memory_sys_mb",
			"memory_stack_mb",
			"memory_gc_sys_mb",
			"gc_cycles",
			"gc_pause_ns",
			"cpu_cgo_calls",
		}
		for _, key := range expectedSystemKeys {
			if _, ok := values[key]; !ok {
				t.Errorf("system metrics missing key: %s", key)
			}
		}
	}

	// Verify application metrics contain expected fields
	appData := collector.GetApplication(1)
	if len(appData) > 0 {
		values := appData[0].Values
		expectedAppKeys := []string{
			"ingest_records_total",
			"ingest_bytes_total",
			"ingest_batches_total",
			"ingest_errors_total",
			"msgpack_requests_total",
			"msgpack_records_total",
			"msgpack_bytes_total",
			"lineprotocol_requests_total",
			"lineprotocol_records_total",
			"lineprotocol_bytes_total",
			"query_requests_total",
			"buffer_queue_depth",
			"buffer_flushes_total",
			"storage_writes_total",
			"storage_write_bytes_total",
			"storage_reads_total",
			"storage_read_bytes_total",
			"storage_errors_total",
			"compaction_jobs_total",
			"compaction_jobs_success",
			"compaction_jobs_failed",
		}
		for _, key := range expectedAppKeys {
			if _, ok := values[key]; !ok {
				t.Errorf("application metrics missing key: %s", key)
			}
		}
	}

	// Verify API metrics contain expected fields
	apiData := collector.GetAPI(1)
	if len(apiData) > 0 {
		values := apiData[0].Values
		expectedAPIKeys := []string{
			"http_requests_total",
			"http_requests_success",
			"http_requests_error",
			"http_latency_avg_us",
			"query_latency_avg_us",
			"db_connections_open",
			"db_connections_in_use",
			"db_queries_total",
			"auth_requests_total",
			"auth_cache_hits",
			"auth_failures_total",
		}
		for _, key := range expectedAPIKeys {
			if _, ok := values[key]; !ok {
				t.Errorf("API metrics missing key: %s", key)
			}
		}
	}
}

func TestCalculateAvgLatency(t *testing.T) {
	tests := []struct {
		name     string
		sum      int64
		count    int64
		expected float64
	}{
		{"zero count", 100, 0, 0},
		{"normal case", 1000, 10, 100},
		{"single value", 500, 1, 500},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := calculateAvgLatency(tt.sum, tt.count)
			if result != tt.expected {
				t.Errorf("calculateAvgLatency(%d, %d) = %f, want %f", tt.sum, tt.count, result, tt.expected)
			}
		})
	}
}

func TestBufferSizeCalculation(t *testing.T) {
	// Test that buffer size is calculated correctly from retention and interval
	tests := []struct {
		retentionMinutes int
		intervalSeconds  int
		expectedSize     int
	}{
		{30, 5, 360},   // 30 min * 60 sec / 5 sec = 360 points
		{60, 10, 360},  // 60 min * 60 sec / 10 sec = 360 points
		{30, 1, 1800},  // 30 min * 60 sec / 1 sec = 1800 points
		{1, 1, 60},     // 1 min * 60 sec / 1 sec = 60 points
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			bufferSize := (tt.retentionMinutes * 60) / tt.intervalSeconds
			if bufferSize != tt.expectedSize {
				t.Errorf("buffer size for %d min retention, %d sec interval = %d, want %d",
					tt.retentionMinutes, tt.intervalSeconds, bufferSize, tt.expectedSize)
			}
		})
	}
}
