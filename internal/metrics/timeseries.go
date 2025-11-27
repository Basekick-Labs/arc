package metrics

import (
	"runtime"
	"sync"
	"time"
)

// TimeSeriesPoint represents a single data point in a time series
type TimeSeriesPoint struct {
	Timestamp time.Time              `json:"timestamp"`
	Values    map[string]interface{} `json:"values"`
}

// TimeSeriesBuffer stores time-series metrics data
type TimeSeriesBuffer struct {
	mu       sync.RWMutex
	points   []TimeSeriesPoint
	size     int
	writePos int
	count    int
	interval time.Duration
	lastAdd  time.Time
}

// TimeSeriesCollector collects time-series metrics at regular intervals
type TimeSeriesCollector struct {
	system      *TimeSeriesBuffer // System metrics (CPU, memory, goroutines)
	application *TimeSeriesBuffer // Application metrics (ingest, query counts)
	api         *TimeSeriesBuffer // API metrics (HTTP requests, latency)
	stopCh      chan struct{}
	wg          sync.WaitGroup
}

var (
	tsCollector *TimeSeriesCollector
	tsOnce      sync.Once
)

// GetTimeSeriesCollector returns the singleton time-series collector
func GetTimeSeriesCollector() *TimeSeriesCollector {
	tsOnce.Do(func() {
		tsCollector = NewTimeSeriesCollector(
			1800,          // 30 minutes of 1-second samples
			time.Second,   // Collect every second
		)
		tsCollector.Start()
	})
	return tsCollector
}

// NewTimeSeriesCollector creates a new time-series collector
func NewTimeSeriesCollector(bufferSize int, interval time.Duration) *TimeSeriesCollector {
	return &TimeSeriesCollector{
		system:      NewTimeSeriesBuffer(bufferSize, interval),
		application: NewTimeSeriesBuffer(bufferSize, interval),
		api:         NewTimeSeriesBuffer(bufferSize, interval),
		stopCh:      make(chan struct{}),
	}
}

// NewTimeSeriesBuffer creates a new time-series buffer
func NewTimeSeriesBuffer(size int, interval time.Duration) *TimeSeriesBuffer {
	return &TimeSeriesBuffer{
		points:   make([]TimeSeriesPoint, size),
		size:     size,
		interval: interval,
	}
}

// Start begins collecting time-series data
func (c *TimeSeriesCollector) Start() {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-c.stopCh:
				return
			case <-ticker.C:
				c.collect()
			}
		}
	}()
}

// Stop stops the time-series collector
func (c *TimeSeriesCollector) Stop() {
	close(c.stopCh)
	c.wg.Wait()
}

// collect gathers all metrics at the current time
func (c *TimeSeriesCollector) collect() {
	now := time.Now()
	m := Get()

	// System metrics
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	c.system.Add(TimeSeriesPoint{
		Timestamp: now,
		Values: map[string]interface{}{
			"goroutines":           runtime.NumGoroutine(),
			"memory_alloc_mb":      float64(memStats.Alloc) / 1024 / 1024,
			"memory_heap_mb":       float64(memStats.HeapAlloc) / 1024 / 1024,
			"memory_sys_mb":        float64(memStats.Sys) / 1024 / 1024,
			"gc_cycles":            memStats.NumGC,
			"gc_pause_ns":          memStats.PauseNs[(memStats.NumGC+255)%256],
			"cpu_cgo_calls":        runtime.NumCgoCall(),
		},
	})

	// Application metrics
	c.application.Add(TimeSeriesPoint{
		Timestamp: now,
		Values: map[string]interface{}{
			"ingest_records_total":  m.ingestRecordsTotal.Load(),
			"ingest_bytes_total":    m.ingestBytesTotal.Load(),
			"query_requests_total":  m.queryRequestsTotal.Load(),
			"query_rows_total":      m.queryRowsTotal.Load(),
			"buffer_queue_depth":    m.bufferQueueDepth.Load(),
			"storage_writes_total":  m.storageWritesTotal.Load(),
			"compaction_jobs_total": m.compactionJobsTotal.Load(),
		},
	})

	// API metrics
	c.api.Add(TimeSeriesPoint{
		Timestamp: now,
		Values: map[string]interface{}{
			"http_requests_total":   m.httpRequestsTotal.Load(),
			"http_requests_success": m.httpRequestsSuccess.Load(),
			"http_requests_error":   m.httpRequestsError.Load(),
			"http_latency_avg_us":   calculateAvgLatency(m.httpLatencySum.Load(), m.httpLatencyCount.Load()),
			"query_latency_avg_us":  calculateAvgLatency(m.queryLatencySum.Load(), m.queryLatencyCount.Load()),
			"db_connections_open":   m.dbConnectionsOpen.Load(),
			"db_connections_in_use": m.dbConnectionsInUse.Load(),
		},
	})
}

func calculateAvgLatency(sum, count int64) float64 {
	if count == 0 {
		return 0
	}
	return float64(sum) / float64(count)
}

// Add adds a point to the buffer
func (b *TimeSeriesBuffer) Add(point TimeSeriesPoint) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.points[b.writePos] = point
	b.writePos = (b.writePos + 1) % b.size
	if b.count < b.size {
		b.count++
	}
	b.lastAdd = point.Timestamp
}

// GetRecent returns points from the last N minutes
func (b *TimeSeriesBuffer) GetRecent(durationMinutes int) []TimeSeriesPoint {
	b.mu.RLock()
	defer b.mu.RUnlock()

	cutoff := time.Now().Add(-time.Duration(durationMinutes) * time.Minute)
	var result []TimeSeriesPoint

	// Read from oldest to newest within the time range
	for i := 0; i < b.count; i++ {
		// Start from the oldest entry
		idx := (b.writePos - b.count + i + b.size) % b.size
		point := b.points[idx]

		if point.Timestamp.After(cutoff) {
			result = append(result, point)
		}
	}

	return result
}

// GetSystem returns system time-series data
func (c *TimeSeriesCollector) GetSystem(durationMinutes int) []TimeSeriesPoint {
	return c.system.GetRecent(durationMinutes)
}

// GetApplication returns application time-series data
func (c *TimeSeriesCollector) GetApplication(durationMinutes int) []TimeSeriesPoint {
	return c.application.GetRecent(durationMinutes)
}

// GetAPI returns API time-series data
func (c *TimeSeriesCollector) GetAPI(durationMinutes int) []TimeSeriesPoint {
	return c.api.GetRecent(durationMinutes)
}
