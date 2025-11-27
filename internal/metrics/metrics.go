package metrics

import (
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
)

// Metrics holds all Arc metrics for Prometheus export
type Metrics struct {
	startTime time.Time

	// HTTP request metrics
	httpRequestsTotal   atomic.Int64
	httpRequestsSuccess atomic.Int64
	httpRequestsError   atomic.Int64

	// HTTP latency histogram buckets (microseconds)
	// Buckets: 1ms, 5ms, 10ms, 25ms, 50ms, 100ms, 250ms, 500ms, 1s, +Inf
	httpLatencyBuckets     [10]atomic.Int64
	httpLatencySum         atomic.Int64
	httpLatencyCount       atomic.Int64

	// Ingestion metrics
	ingestRecordsTotal    atomic.Int64
	ingestBytesTotal      atomic.Int64
	ingestBatchesTotal    atomic.Int64
	ingestErrorsTotal     atomic.Int64

	// MessagePack specific
	msgpackRequestsTotal  atomic.Int64
	msgpackRecordsTotal   atomic.Int64
	msgpackBytesTotal     atomic.Int64

	// Line Protocol specific
	lineprotocolRequestsTotal atomic.Int64
	lineprotocolRecordsTotal  atomic.Int64
	lineprotocolBytesTotal    atomic.Int64

	// Query metrics
	queryRequestsTotal    atomic.Int64
	querySuccessTotal     atomic.Int64
	queryErrorsTotal      atomic.Int64
	queryRowsTotal        atomic.Int64
	queryLatencySum       atomic.Int64 // microseconds
	queryLatencyCount     atomic.Int64

	// Arrow buffer metrics
	bufferRecordsBuffered atomic.Int64
	bufferRecordsWritten  atomic.Int64
	bufferFlushesTotal    atomic.Int64
	bufferErrorsTotal     atomic.Int64
	bufferQueueDepth      atomic.Int64

	// Storage metrics
	storageWritesTotal    atomic.Int64
	storageWriteBytesTotal atomic.Int64
	storageReadsTotal     atomic.Int64
	storageReadBytesTotal atomic.Int64
	storageErrorsTotal    atomic.Int64

	// Compaction metrics
	compactionJobsTotal     atomic.Int64
	compactionJobsSuccess   atomic.Int64
	compactionJobsFailed    atomic.Int64
	compactionFilesCompacted atomic.Int64
	compactionBytesRead     atomic.Int64
	compactionBytesWritten  atomic.Int64

	// Auth metrics
	authRequestsTotal     atomic.Int64
	authCacheHits         atomic.Int64
	authCacheMisses       atomic.Int64
	authFailuresTotal     atomic.Int64

	// DuckDB connection pool
	dbConnectionsOpen     atomic.Int64
	dbConnectionsInUse    atomic.Int64
	dbConnectionsIdle     atomic.Int64
	dbQueriesTotal        atomic.Int64
	dbQueryErrorsTotal    atomic.Int64

	logger zerolog.Logger
}

var (
	instance *Metrics
	once     sync.Once
)

// Get returns the singleton metrics instance
func Get() *Metrics {
	once.Do(func() {
		instance = &Metrics{
			startTime: time.Now(),
		}
	})
	return instance
}

// Init initializes the metrics with a logger
func Init(logger zerolog.Logger) *Metrics {
	m := Get()
	m.logger = logger.With().Str("component", "metrics").Logger()
	m.logger.Info().Msg("Metrics collector initialized")
	return m
}

// HTTP Metrics
func (m *Metrics) IncHTTPRequests()        { m.httpRequestsTotal.Add(1) }
func (m *Metrics) IncHTTPSuccess()         { m.httpRequestsSuccess.Add(1) }
func (m *Metrics) IncHTTPError()           { m.httpRequestsError.Add(1) }

// RecordHTTPLatency records HTTP request latency in microseconds
func (m *Metrics) RecordHTTPLatency(durationMicros int64) {
	m.httpLatencySum.Add(durationMicros)
	m.httpLatencyCount.Add(1)

	// Update histogram bucket
	bucketIdx := m.getLatencyBucket(durationMicros)
	m.httpLatencyBuckets[bucketIdx].Add(1)
}

func (m *Metrics) getLatencyBucket(micros int64) int {
	// Buckets: 1ms, 5ms, 10ms, 25ms, 50ms, 100ms, 250ms, 500ms, 1s, +Inf
	switch {
	case micros <= 1000:
		return 0
	case micros <= 5000:
		return 1
	case micros <= 10000:
		return 2
	case micros <= 25000:
		return 3
	case micros <= 50000:
		return 4
	case micros <= 100000:
		return 5
	case micros <= 250000:
		return 6
	case micros <= 500000:
		return 7
	case micros <= 1000000:
		return 8
	default:
		return 9
	}
}

// Ingestion Metrics
func (m *Metrics) IncIngestRecords(count int64)  { m.ingestRecordsTotal.Add(count) }
func (m *Metrics) IncIngestBytes(bytes int64)    { m.ingestBytesTotal.Add(bytes) }
func (m *Metrics) IncIngestBatches()             { m.ingestBatchesTotal.Add(1) }
func (m *Metrics) IncIngestErrors()              { m.ingestErrorsTotal.Add(1) }

// MessagePack Metrics
func (m *Metrics) IncMsgPackRequests()           { m.msgpackRequestsTotal.Add(1) }
func (m *Metrics) IncMsgPackRecords(count int64) { m.msgpackRecordsTotal.Add(count) }
func (m *Metrics) IncMsgPackBytes(bytes int64)   { m.msgpackBytesTotal.Add(bytes) }

// Line Protocol Metrics
func (m *Metrics) IncLineProtocolRequests()           { m.lineprotocolRequestsTotal.Add(1) }
func (m *Metrics) IncLineProtocolRecords(count int64) { m.lineprotocolRecordsTotal.Add(count) }
func (m *Metrics) IncLineProtocolBytes(bytes int64)   { m.lineprotocolBytesTotal.Add(bytes) }

// Query Metrics
func (m *Metrics) IncQueryRequests()             { m.queryRequestsTotal.Add(1) }
func (m *Metrics) IncQuerySuccess()              { m.querySuccessTotal.Add(1) }
func (m *Metrics) IncQueryErrors()               { m.queryErrorsTotal.Add(1) }
func (m *Metrics) IncQueryRows(count int64)      { m.queryRowsTotal.Add(count) }

// RecordQueryLatency records query latency in microseconds
func (m *Metrics) RecordQueryLatency(durationMicros int64) {
	m.queryLatencySum.Add(durationMicros)
	m.queryLatencyCount.Add(1)
}

// Buffer Metrics
func (m *Metrics) SetBufferRecordsBuffered(count int64) { m.bufferRecordsBuffered.Store(count) }
func (m *Metrics) SetBufferRecordsWritten(count int64)  { m.bufferRecordsWritten.Store(count) }
func (m *Metrics) SetBufferFlushes(count int64)         { m.bufferFlushesTotal.Store(count) }
func (m *Metrics) SetBufferErrors(count int64)          { m.bufferErrorsTotal.Store(count) }
func (m *Metrics) SetBufferQueueDepth(depth int64)      { m.bufferQueueDepth.Store(depth) }

// Storage Metrics
func (m *Metrics) IncStorageWrites()             { m.storageWritesTotal.Add(1) }
func (m *Metrics) IncStorageWriteBytes(bytes int64) { m.storageWriteBytesTotal.Add(bytes) }
func (m *Metrics) IncStorageReads()              { m.storageReadsTotal.Add(1) }
func (m *Metrics) IncStorageReadBytes(bytes int64) { m.storageReadBytesTotal.Add(bytes) }
func (m *Metrics) IncStorageErrors()             { m.storageErrorsTotal.Add(1) }

// Compaction Metrics
func (m *Metrics) IncCompactionJobs()            { m.compactionJobsTotal.Add(1) }
func (m *Metrics) IncCompactionSuccess()         { m.compactionJobsSuccess.Add(1) }
func (m *Metrics) IncCompactionFailed()          { m.compactionJobsFailed.Add(1) }
func (m *Metrics) IncCompactionFilesCompacted(count int64) { m.compactionFilesCompacted.Add(count) }
func (m *Metrics) IncCompactionBytesRead(bytes int64) { m.compactionBytesRead.Add(bytes) }
func (m *Metrics) IncCompactionBytesWritten(bytes int64) { m.compactionBytesWritten.Add(bytes) }

// Auth Metrics
func (m *Metrics) IncAuthRequests()              { m.authRequestsTotal.Add(1) }
func (m *Metrics) IncAuthCacheHit()              { m.authCacheHits.Add(1) }
func (m *Metrics) IncAuthCacheMiss()             { m.authCacheMisses.Add(1) }
func (m *Metrics) IncAuthFailures()              { m.authFailuresTotal.Add(1) }

// Database Metrics
func (m *Metrics) SetDBConnectionsOpen(count int64)  { m.dbConnectionsOpen.Store(count) }
func (m *Metrics) SetDBConnectionsInUse(count int64) { m.dbConnectionsInUse.Store(count) }
func (m *Metrics) SetDBConnectionsIdle(count int64)  { m.dbConnectionsIdle.Store(count) }
func (m *Metrics) IncDBQueries()                     { m.dbQueriesTotal.Add(1) }
func (m *Metrics) IncDBQueryErrors()                 { m.dbQueryErrorsTotal.Add(1) }

// Snapshot returns all metrics as a map (for JSON endpoint)
func (m *Metrics) Snapshot() map[string]interface{} {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	return map[string]interface{}{
		// Process info
		"uptime_seconds":    time.Since(m.startTime).Seconds(),
		"goroutines":        runtime.NumGoroutine(),
		"go_version":        runtime.Version(),
		"num_cpu":           runtime.NumCPU(),
		"gomaxprocs":        runtime.GOMAXPROCS(0),

		// Memory (Go runtime)
		"memory_alloc_bytes":       memStats.Alloc,
		"memory_total_alloc_bytes": memStats.TotalAlloc,
		"memory_sys_bytes":         memStats.Sys,
		"memory_heap_alloc_bytes":  memStats.HeapAlloc,
		"memory_heap_sys_bytes":    memStats.HeapSys,
		"memory_heap_inuse_bytes":  memStats.HeapInuse,
		"memory_stack_inuse_bytes": memStats.StackInuse,
		"gc_cycles":                memStats.NumGC,
		"gc_pause_total_ns":        memStats.PauseTotalNs,

		// HTTP
		"http_requests_total":   m.httpRequestsTotal.Load(),
		"http_requests_success": m.httpRequestsSuccess.Load(),
		"http_requests_error":   m.httpRequestsError.Load(),
		"http_latency_sum_us":   m.httpLatencySum.Load(),
		"http_latency_count":    m.httpLatencyCount.Load(),

		// Ingestion
		"ingest_records_total": m.ingestRecordsTotal.Load(),
		"ingest_bytes_total":   m.ingestBytesTotal.Load(),
		"ingest_batches_total": m.ingestBatchesTotal.Load(),
		"ingest_errors_total":  m.ingestErrorsTotal.Load(),

		// MessagePack
		"msgpack_requests_total": m.msgpackRequestsTotal.Load(),
		"msgpack_records_total":  m.msgpackRecordsTotal.Load(),
		"msgpack_bytes_total":    m.msgpackBytesTotal.Load(),

		// Line Protocol
		"lineprotocol_requests_total": m.lineprotocolRequestsTotal.Load(),
		"lineprotocol_records_total":  m.lineprotocolRecordsTotal.Load(),
		"lineprotocol_bytes_total":    m.lineprotocolBytesTotal.Load(),

		// Query
		"query_requests_total":  m.queryRequestsTotal.Load(),
		"query_success_total":   m.querySuccessTotal.Load(),
		"query_errors_total":    m.queryErrorsTotal.Load(),
		"query_rows_total":      m.queryRowsTotal.Load(),
		"query_latency_sum_us":  m.queryLatencySum.Load(),
		"query_latency_count":   m.queryLatencyCount.Load(),

		// Buffer
		"buffer_records_buffered": m.bufferRecordsBuffered.Load(),
		"buffer_records_written":  m.bufferRecordsWritten.Load(),
		"buffer_flushes_total":    m.bufferFlushesTotal.Load(),
		"buffer_errors_total":     m.bufferErrorsTotal.Load(),
		"buffer_queue_depth":      m.bufferQueueDepth.Load(),

		// Storage
		"storage_writes_total":      m.storageWritesTotal.Load(),
		"storage_write_bytes_total": m.storageWriteBytesTotal.Load(),
		"storage_reads_total":       m.storageReadsTotal.Load(),
		"storage_read_bytes_total":  m.storageReadBytesTotal.Load(),
		"storage_errors_total":      m.storageErrorsTotal.Load(),

		// Compaction
		"compaction_jobs_total":      m.compactionJobsTotal.Load(),
		"compaction_jobs_success":    m.compactionJobsSuccess.Load(),
		"compaction_jobs_failed":     m.compactionJobsFailed.Load(),
		"compaction_files_compacted": m.compactionFilesCompacted.Load(),
		"compaction_bytes_read":      m.compactionBytesRead.Load(),
		"compaction_bytes_written":   m.compactionBytesWritten.Load(),

		// Auth
		"auth_requests_total":   m.authRequestsTotal.Load(),
		"auth_cache_hits":       m.authCacheHits.Load(),
		"auth_cache_misses":     m.authCacheMisses.Load(),
		"auth_failures_total":   m.authFailuresTotal.Load(),

		// Database
		"db_connections_open":   m.dbConnectionsOpen.Load(),
		"db_connections_in_use": m.dbConnectionsInUse.Load(),
		"db_connections_idle":   m.dbConnectionsIdle.Load(),
		"db_queries_total":      m.dbQueriesTotal.Load(),
		"db_query_errors_total": m.dbQueryErrorsTotal.Load(),
	}
}

// PrometheusFormat returns metrics in Prometheus text exposition format
func (m *Metrics) PrometheusFormat() string {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	uptimeSeconds := time.Since(m.startTime).Seconds()

	// Build Prometheus format
	var b []byte
	b = append(b, "# HELP arc_uptime_seconds Time since Arc started\n"...)
	b = append(b, "# TYPE arc_uptime_seconds gauge\n"...)
	b = appendMetric(b, "arc_uptime_seconds", uptimeSeconds)

	b = append(b, "# HELP arc_goroutines Number of goroutines\n"...)
	b = append(b, "# TYPE arc_goroutines gauge\n"...)
	b = appendMetric(b, "arc_goroutines", float64(runtime.NumGoroutine()))

	// Memory metrics
	b = append(b, "# HELP arc_memory_alloc_bytes Current allocated memory\n"...)
	b = append(b, "# TYPE arc_memory_alloc_bytes gauge\n"...)
	b = appendMetric(b, "arc_memory_alloc_bytes", float64(memStats.Alloc))

	b = append(b, "# HELP arc_memory_heap_alloc_bytes Heap memory allocated\n"...)
	b = append(b, "# TYPE arc_memory_heap_alloc_bytes gauge\n"...)
	b = appendMetric(b, "arc_memory_heap_alloc_bytes", float64(memStats.HeapAlloc))

	b = append(b, "# HELP arc_memory_sys_bytes Total memory obtained from system\n"...)
	b = append(b, "# TYPE arc_memory_sys_bytes gauge\n"...)
	b = appendMetric(b, "arc_memory_sys_bytes", float64(memStats.Sys))

	b = append(b, "# HELP arc_gc_cycles_total Total number of GC cycles\n"...)
	b = append(b, "# TYPE arc_gc_cycles_total counter\n"...)
	b = appendMetric(b, "arc_gc_cycles_total", float64(memStats.NumGC))

	// HTTP metrics
	b = append(b, "# HELP arc_http_requests_total Total HTTP requests\n"...)
	b = append(b, "# TYPE arc_http_requests_total counter\n"...)
	b = appendMetric(b, "arc_http_requests_total", float64(m.httpRequestsTotal.Load()))

	b = append(b, "# HELP arc_http_requests_success_total Successful HTTP requests\n"...)
	b = append(b, "# TYPE arc_http_requests_success_total counter\n"...)
	b = appendMetric(b, "arc_http_requests_success_total", float64(m.httpRequestsSuccess.Load()))

	b = append(b, "# HELP arc_http_requests_error_total Failed HTTP requests\n"...)
	b = append(b, "# TYPE arc_http_requests_error_total counter\n"...)
	b = appendMetric(b, "arc_http_requests_error_total", float64(m.httpRequestsError.Load()))

	// HTTP latency histogram
	b = append(b, "# HELP arc_http_latency_seconds HTTP request latency\n"...)
	b = append(b, "# TYPE arc_http_latency_seconds histogram\n"...)
	bucketLabels := []string{"0.001", "0.005", "0.01", "0.025", "0.05", "0.1", "0.25", "0.5", "1", "+Inf"}
	var cumulative int64
	for i, label := range bucketLabels {
		cumulative += m.httpLatencyBuckets[i].Load()
		b = appendMetricWithLabel(b, "arc_http_latency_seconds_bucket", "le", label, float64(cumulative))
	}
	b = appendMetric(b, "arc_http_latency_seconds_sum", float64(m.httpLatencySum.Load())/1000000.0)
	b = appendMetric(b, "arc_http_latency_seconds_count", float64(m.httpLatencyCount.Load()))

	// Ingestion metrics
	b = append(b, "# HELP arc_ingest_records_total Total records ingested\n"...)
	b = append(b, "# TYPE arc_ingest_records_total counter\n"...)
	b = appendMetric(b, "arc_ingest_records_total", float64(m.ingestRecordsTotal.Load()))

	b = append(b, "# HELP arc_ingest_bytes_total Total bytes ingested\n"...)
	b = append(b, "# TYPE arc_ingest_bytes_total counter\n"...)
	b = appendMetric(b, "arc_ingest_bytes_total", float64(m.ingestBytesTotal.Load()))

	b = append(b, "# HELP arc_ingest_batches_total Total batches ingested\n"...)
	b = append(b, "# TYPE arc_ingest_batches_total counter\n"...)
	b = appendMetric(b, "arc_ingest_batches_total", float64(m.ingestBatchesTotal.Load()))

	b = append(b, "# HELP arc_ingest_errors_total Total ingestion errors\n"...)
	b = append(b, "# TYPE arc_ingest_errors_total counter\n"...)
	b = appendMetric(b, "arc_ingest_errors_total", float64(m.ingestErrorsTotal.Load()))

	// MessagePack metrics
	b = append(b, "# HELP arc_msgpack_requests_total Total MessagePack requests\n"...)
	b = append(b, "# TYPE arc_msgpack_requests_total counter\n"...)
	b = appendMetric(b, "arc_msgpack_requests_total", float64(m.msgpackRequestsTotal.Load()))

	b = append(b, "# HELP arc_msgpack_records_total Total records via MessagePack\n"...)
	b = append(b, "# TYPE arc_msgpack_records_total counter\n"...)
	b = appendMetric(b, "arc_msgpack_records_total", float64(m.msgpackRecordsTotal.Load()))

	// Line Protocol metrics
	b = append(b, "# HELP arc_lineprotocol_requests_total Total Line Protocol requests\n"...)
	b = append(b, "# TYPE arc_lineprotocol_requests_total counter\n"...)
	b = appendMetric(b, "arc_lineprotocol_requests_total", float64(m.lineprotocolRequestsTotal.Load()))

	b = append(b, "# HELP arc_lineprotocol_records_total Total records via Line Protocol\n"...)
	b = append(b, "# TYPE arc_lineprotocol_records_total counter\n"...)
	b = appendMetric(b, "arc_lineprotocol_records_total", float64(m.lineprotocolRecordsTotal.Load()))

	// Query metrics
	b = append(b, "# HELP arc_query_requests_total Total query requests\n"...)
	b = append(b, "# TYPE arc_query_requests_total counter\n"...)
	b = appendMetric(b, "arc_query_requests_total", float64(m.queryRequestsTotal.Load()))

	b = append(b, "# HELP arc_query_success_total Successful queries\n"...)
	b = append(b, "# TYPE arc_query_success_total counter\n"...)
	b = appendMetric(b, "arc_query_success_total", float64(m.querySuccessTotal.Load()))

	b = append(b, "# HELP arc_query_errors_total Failed queries\n"...)
	b = append(b, "# TYPE arc_query_errors_total counter\n"...)
	b = appendMetric(b, "arc_query_errors_total", float64(m.queryErrorsTotal.Load()))

	b = append(b, "# HELP arc_query_rows_total Total rows returned by queries\n"...)
	b = append(b, "# TYPE arc_query_rows_total counter\n"...)
	b = appendMetric(b, "arc_query_rows_total", float64(m.queryRowsTotal.Load()))

	// Buffer metrics
	b = append(b, "# HELP arc_buffer_records_buffered Records currently buffered\n"...)
	b = append(b, "# TYPE arc_buffer_records_buffered gauge\n"...)
	b = appendMetric(b, "arc_buffer_records_buffered", float64(m.bufferRecordsBuffered.Load()))

	b = append(b, "# HELP arc_buffer_records_written_total Records written to storage\n"...)
	b = append(b, "# TYPE arc_buffer_records_written_total counter\n"...)
	b = appendMetric(b, "arc_buffer_records_written_total", float64(m.bufferRecordsWritten.Load()))

	b = append(b, "# HELP arc_buffer_flushes_total Total buffer flushes\n"...)
	b = append(b, "# TYPE arc_buffer_flushes_total counter\n"...)
	b = appendMetric(b, "arc_buffer_flushes_total", float64(m.bufferFlushesTotal.Load()))

	b = append(b, "# HELP arc_buffer_queue_depth Current flush queue depth\n"...)
	b = append(b, "# TYPE arc_buffer_queue_depth gauge\n"...)
	b = appendMetric(b, "arc_buffer_queue_depth", float64(m.bufferQueueDepth.Load()))

	// Storage metrics
	b = append(b, "# HELP arc_storage_writes_total Total storage writes\n"...)
	b = append(b, "# TYPE arc_storage_writes_total counter\n"...)
	b = appendMetric(b, "arc_storage_writes_total", float64(m.storageWritesTotal.Load()))

	b = append(b, "# HELP arc_storage_write_bytes_total Total bytes written to storage\n"...)
	b = append(b, "# TYPE arc_storage_write_bytes_total counter\n"...)
	b = appendMetric(b, "arc_storage_write_bytes_total", float64(m.storageWriteBytesTotal.Load()))

	b = append(b, "# HELP arc_storage_errors_total Total storage errors\n"...)
	b = append(b, "# TYPE arc_storage_errors_total counter\n"...)
	b = appendMetric(b, "arc_storage_errors_total", float64(m.storageErrorsTotal.Load()))

	// Compaction metrics
	b = append(b, "# HELP arc_compaction_jobs_total Total compaction jobs\n"...)
	b = append(b, "# TYPE arc_compaction_jobs_total counter\n"...)
	b = appendMetric(b, "arc_compaction_jobs_total", float64(m.compactionJobsTotal.Load()))

	b = append(b, "# HELP arc_compaction_jobs_success_total Successful compaction jobs\n"...)
	b = append(b, "# TYPE arc_compaction_jobs_success_total counter\n"...)
	b = appendMetric(b, "arc_compaction_jobs_success_total", float64(m.compactionJobsSuccess.Load()))

	b = append(b, "# HELP arc_compaction_jobs_failed_total Failed compaction jobs\n"...)
	b = append(b, "# TYPE arc_compaction_jobs_failed_total counter\n"...)
	b = appendMetric(b, "arc_compaction_jobs_failed_total", float64(m.compactionJobsFailed.Load()))

	// Auth metrics
	b = append(b, "# HELP arc_auth_requests_total Total authentication requests\n"...)
	b = append(b, "# TYPE arc_auth_requests_total counter\n"...)
	b = appendMetric(b, "arc_auth_requests_total", float64(m.authRequestsTotal.Load()))

	b = append(b, "# HELP arc_auth_cache_hits_total Auth cache hits\n"...)
	b = append(b, "# TYPE arc_auth_cache_hits_total counter\n"...)
	b = appendMetric(b, "arc_auth_cache_hits_total", float64(m.authCacheHits.Load()))

	b = append(b, "# HELP arc_auth_cache_misses_total Auth cache misses\n"...)
	b = append(b, "# TYPE arc_auth_cache_misses_total counter\n"...)
	b = appendMetric(b, "arc_auth_cache_misses_total", float64(m.authCacheMisses.Load()))

	// Database metrics
	b = append(b, "# HELP arc_db_connections_open Open database connections\n"...)
	b = append(b, "# TYPE arc_db_connections_open gauge\n"...)
	b = appendMetric(b, "arc_db_connections_open", float64(m.dbConnectionsOpen.Load()))

	b = append(b, "# HELP arc_db_connections_in_use Database connections in use\n"...)
	b = append(b, "# TYPE arc_db_connections_in_use gauge\n"...)
	b = appendMetric(b, "arc_db_connections_in_use", float64(m.dbConnectionsInUse.Load()))

	b = append(b, "# HELP arc_db_queries_total Total database queries\n"...)
	b = append(b, "# TYPE arc_db_queries_total counter\n"...)
	b = appendMetric(b, "arc_db_queries_total", float64(m.dbQueriesTotal.Load()))

	return string(b)
}

// Helper functions for Prometheus format
func appendMetric(b []byte, name string, value float64) []byte {
	b = append(b, name...)
	b = append(b, ' ')
	b = appendFloat(b, value)
	b = append(b, '\n')
	return b
}

func appendMetricWithLabel(b []byte, name, labelName, labelValue string, value float64) []byte {
	b = append(b, name...)
	b = append(b, '{')
	b = append(b, labelName...)
	b = append(b, '=', '"')
	b = append(b, labelValue...)
	b = append(b, '"', '}', ' ')
	b = appendFloat(b, value)
	b = append(b, '\n')
	return b
}

func appendFloat(b []byte, v float64) []byte {
	// Simple float formatting - enough for metrics
	if v == float64(int64(v)) {
		return appendInt(b, int64(v))
	}
	// Format with up to 6 decimal places
	intPart := int64(v)
	fracPart := int64((v - float64(intPart)) * 1000000)
	if fracPart < 0 {
		fracPart = -fracPart
	}
	b = appendInt(b, intPart)
	b = append(b, '.')
	// Pad with zeros
	if fracPart < 100000 {
		b = append(b, '0')
	}
	if fracPart < 10000 {
		b = append(b, '0')
	}
	if fracPart < 1000 {
		b = append(b, '0')
	}
	if fracPart < 100 {
		b = append(b, '0')
	}
	if fracPart < 10 {
		b = append(b, '0')
	}
	b = appendInt(b, fracPart)
	return b
}

func appendInt(b []byte, v int64) []byte {
	if v < 0 {
		b = append(b, '-')
		v = -v
	}
	if v == 0 {
		return append(b, '0')
	}
	var digits [20]byte
	i := len(digits)
	for v > 0 {
		i--
		digits[i] = byte('0' + v%10)
		v /= 10
	}
	return append(b, digits[i:]...)
}
