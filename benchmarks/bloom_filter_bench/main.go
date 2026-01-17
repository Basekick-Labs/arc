// Bloom filter benchmark for Arc text search
// Usage: go run benchmarks/bloom_filter_bench/main.go [flags]
//
// This benchmark:
// 1. Generates realistic log data
// 2. Ingests it into Arc
// 3. Runs various text search queries
// 4. Reports query latencies and throughput
//
// Examples:
//   go run benchmarks/bloom_filter_bench/main.go --records 1000000 --ingest
//   go run benchmarks/bloom_filter_bench/main.go --query-only --iterations 10

package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/vmihailenco/msgpack/v5"
)

// Log levels with realistic distribution
var logLevels = []struct {
	level  string
	weight int
}{
	{"DEBUG", 70},
	{"INFO", 20},
	{"WARN", 5},
	{"ERROR", 4},
	{"FATAL", 1},
}

// Services
var services = []string{
	"api-gateway", "auth-service", "user-service", "payment-service",
	"notification-service", "search-service", "analytics-service",
	"order-service", "inventory-service", "shipping-service",
}

// Hosts
var hosts = []string{
	"prod-server-01", "prod-server-02", "prod-server-03", "prod-server-04",
	"prod-server-05", "prod-server-06", "prod-server-07", "prod-server-08",
}

// Message templates for different log levels
var messageTemplates = map[string][]string{
	"DEBUG": {
		"Processing request with params: %s",
		"Cache lookup for key: user_%d",
		"Database query executed in %dms",
		"Memory usage: %dMB, CPU: %d%%",
		"Connection pool status: active=%d, idle=%d",
	},
	"INFO": {
		"Request completed successfully: %s",
		"User %d authenticated from IP %s",
		"Order %d created for customer %d",
		"Payment processed: $%.2f",
		"Notification sent to user %d",
	},
	"WARN": {
		"Slow query detected: %dms (threshold: 100ms)",
		"Rate limit approaching for client %s",
		"Retry attempt %d for operation %s",
		"Connection timeout after %dms, retrying",
		"High memory usage detected: %dMB",
	},
	"ERROR": {
		"Failed to process request: connection refused",
		"Database connection failed: timeout after %dms",
		"Authentication failed for user %d: invalid credentials",
		"Payment processing error: %s",
		"Service unavailable: %s returned 503",
	},
	"FATAL": {
		"Critical: Database cluster unreachable",
		"Fatal: Out of memory, shutting down",
		"Critical: Configuration file missing",
		"Fatal: Unable to bind to port %d",
		"Critical: Data corruption detected in table %s",
	},
}

// IPs for log messages
var sampleIPs = []string{
	"192.168.1.100", "10.0.0.50", "172.16.0.25", "192.168.2.200",
	"10.10.10.10", "172.31.0.100", "192.168.100.1", "10.20.30.40",
}

type Config struct {
	Host       string
	Port       int
	Token      string
	Database   string
	Records    int
	BatchSize  int
	Workers    int
	Iterations int
	IngestOnly bool
	QueryOnly  bool
	OutputFile string
}

type LogRecord struct {
	Timestamp int64             `json:"timestamp"`
	Level     string            `json:"level"`
	Service   string            `json:"service"`
	Host      string            `json:"host"`
	Message   string            `json:"message"`
	TraceID   string            `json:"trace_id"`
	RequestID string            `json:"request_id"`
	Extra     map[string]string `json:"extra,omitempty"`
}

type IngestRequest struct {
	Measurement string            `json:"measurement"`
	Tags        map[string]string `json:"tags"`
	Fields      map[string]any    `json:"fields"`
	Timestamp   int64             `json:"timestamp"`
}

type QueryRequest struct {
	SQL string `json:"sql"`
}

type BenchmarkResult struct {
	QueryName    string    `json:"query_name"`
	QueryType    string    `json:"query_type"`
	Iterations   int       `json:"iterations"`
	AvgLatencyMs float64   `json:"avg_latency_ms"`
	P50LatencyMs float64   `json:"p50_latency_ms"`
	P95LatencyMs float64   `json:"p95_latency_ms"`
	P99LatencyMs float64   `json:"p99_latency_ms"`
	MinLatencyMs float64   `json:"min_latency_ms"`
	MaxLatencyMs float64   `json:"max_latency_ms"`
	AvgRows      float64   `json:"avg_rows"`
	AvgBytes     float64   `json:"avg_bytes"`
	Timestamp    time.Time `json:"timestamp"`
}

func pickLevel(r *rand.Rand) string {
	total := 0
	for _, l := range logLevels {
		total += l.weight
	}
	n := r.Intn(total)
	for _, l := range logLevels {
		n -= l.weight
		if n < 0 {
			return l.level
		}
	}
	return "INFO"
}

func generateMessage(r *rand.Rand, level string) string {
	templates := messageTemplates[level]
	template := templates[r.Intn(len(templates))]

	// Fill in template placeholders with random values
	result := template
	for strings.Contains(result, "%d") {
		result = strings.Replace(result, "%d", fmt.Sprintf("%d", r.Intn(10000)), 1)
	}
	for strings.Contains(result, "%s") {
		result = strings.Replace(result, "%s", sampleIPs[r.Intn(len(sampleIPs))], 1)
	}
	for strings.Contains(result, "%.2f") {
		result = strings.Replace(result, "%.2f", fmt.Sprintf("%.2f", r.Float64()*1000), 1)
	}
	return result
}

func generateTraceID(r *rand.Rand) string {
	return fmt.Sprintf("%08x%08x%08x%08x", r.Uint32(), r.Uint32(), r.Uint32(), r.Uint32())
}

func generateRequestID(r *rand.Rand) string {
	return fmt.Sprintf("req-%08x", r.Uint32())
}

func generateLogRecords(count int, baseTime time.Time) []LogRecord {
	records := make([]LogRecord, count)
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	for i := 0; i < count; i++ {
		level := pickLevel(r)
		// Spread records over 24 hours
		offset := time.Duration(r.Int63n(int64(24 * time.Hour)))
		ts := baseTime.Add(-offset)

		records[i] = LogRecord{
			Timestamp: ts.UnixNano(),
			Level:     level,
			Service:   services[r.Intn(len(services))],
			Host:      hosts[r.Intn(len(hosts))],
			Message:   generateMessage(r, level),
			TraceID:   generateTraceID(r),
			RequestID: generateRequestID(r),
		}
	}
	return records
}

// MsgPackRecord represents a single record in msgpack batch format
type MsgPackRecord struct {
	M      string            `msgpack:"m"`      // measurement
	T      int64             `msgpack:"t"`      // timestamp
	Tags   map[string]string `msgpack:"tags"`   // tags
	Fields map[string]any    `msgpack:"fields"` // fields
}

// MsgPackBatch represents a batch of records
type MsgPackBatch struct {
	Batch []MsgPackRecord `msgpack:"batch"`
}

func ingestBatch(cfg *Config, client *http.Client, records []LogRecord) error {
	// Convert to MsgPack batch format
	batch := MsgPackBatch{
		Batch: make([]MsgPackRecord, len(records)),
	}

	for i, rec := range records {
		batch.Batch[i] = MsgPackRecord{
			M: "logs",
			T: rec.Timestamp,
			Tags: map[string]string{
				"level":   rec.Level,
				"service": rec.Service,
				"host":    rec.Host,
			},
			Fields: map[string]any{
				"message":    rec.Message,
				"trace_id":   rec.TraceID,
				"request_id": rec.RequestID,
			},
		}
	}

	body, err := msgpack.Marshal(batch)
	if err != nil {
		return err
	}

	url := fmt.Sprintf("http://%s:%d/api/v1/write/msgpack", cfg.Host, cfg.Port)
	req, err := http.NewRequest("POST", url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/msgpack")
	req.Header.Set("X-Arc-Database", cfg.Database)
	if cfg.Token != "" {
		req.Header.Set("Authorization", "Bearer "+cfg.Token)
	}

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("ingest failed with status %d: %s", resp.StatusCode, string(respBody))
	}
	return nil
}

func runIngest(cfg *Config) error {
	fmt.Printf("Generating %d log records...\n", cfg.Records)
	baseTime := time.Now()
	records := generateLogRecords(cfg.Records, baseTime)
	fmt.Printf("Generated %d records\n", len(records))

	client := &http.Client{Timeout: 30 * time.Second}

	// Ingest in batches with workers
	var wg sync.WaitGroup
	batchCh := make(chan []LogRecord, cfg.Workers*2)
	var ingested int64
	var errors int64
	startTime := time.Now()

	// Start workers
	for i := 0; i < cfg.Workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for batch := range batchCh {
				if err := ingestBatch(cfg, client, batch); err != nil {
					atomic.AddInt64(&errors, 1)
					fmt.Printf("Error: %v\n", err)
				} else {
					atomic.AddInt64(&ingested, int64(len(batch)))
				}
			}
		}()
	}

	// Send batches
	for i := 0; i < len(records); i += cfg.BatchSize {
		end := i + cfg.BatchSize
		if end > len(records) {
			end = len(records)
		}
		batchCh <- records[i:end]

		// Progress
		if i > 0 && i%(cfg.BatchSize*10) == 0 {
			fmt.Printf("  Sent %d/%d records (%.1f%%)\n", i, len(records), float64(i)/float64(len(records))*100)
		}
	}
	close(batchCh)
	wg.Wait()

	elapsed := time.Since(startTime)
	rate := float64(ingested) / elapsed.Seconds()
	fmt.Printf("\nIngestion complete:\n")
	fmt.Printf("  Records: %d\n", ingested)
	fmt.Printf("  Errors: %d\n", errors)
	fmt.Printf("  Duration: %v\n", elapsed)
	fmt.Printf("  Rate: %.0f records/sec\n", rate)

	return nil
}

func runQuery(cfg *Config, client *http.Client, sql string) (latencyMs float64, rows int64, respBytes int64, err error) {
	url := fmt.Sprintf("http://%s:%d/api/v1/query", cfg.Host, cfg.Port)

	reqBody := QueryRequest{SQL: sql}
	body, _ := json.Marshal(reqBody)

	req, err := http.NewRequest("POST", url, bytes.NewReader(body))
	if err != nil {
		return 0, 0, 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Arc-Database", cfg.Database)
	if cfg.Token != "" {
		req.Header.Set("Authorization", "Bearer "+cfg.Token)
	}

	start := time.Now()
	resp, err := client.Do(req)
	if err != nil {
		return 0, 0, 0, err
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	latencyMs = float64(time.Since(start).Microseconds()) / 1000.0

	if resp.StatusCode != http.StatusOK {
		return latencyMs, 0, int64(len(respBody)), fmt.Errorf("query failed: %s", string(respBody))
	}

	// Parse response to count rows
	var result struct {
		Data []any `json:"data"`
	}
	json.Unmarshal(respBody, &result)

	return latencyMs, int64(len(result.Data)), int64(len(respBody)), nil
}

func benchmarkQuery(cfg *Config, client *http.Client, name, queryType, sql string) BenchmarkResult {
	var latencies []float64
	var rowCounts []int64
	var byteCounts []int64

	// Warmup
	runQuery(cfg, client, sql)

	for i := 0; i < cfg.Iterations; i++ {
		latency, rows, bytes, err := runQuery(cfg, client, sql)
		if err != nil {
			fmt.Printf("  Error on iteration %d: %v\n", i, err)
			continue
		}
		latencies = append(latencies, latency)
		rowCounts = append(rowCounts, rows)
		byteCounts = append(byteCounts, bytes)
	}

	if len(latencies) == 0 {
		return BenchmarkResult{QueryName: name, QueryType: queryType}
	}

	sort.Float64s(latencies)

	var sumLatency float64
	for _, l := range latencies {
		sumLatency += l
	}

	var sumRows, sumBytes int64
	for i := range rowCounts {
		sumRows += rowCounts[i]
		sumBytes += byteCounts[i]
	}

	n := len(latencies)
	return BenchmarkResult{
		QueryName:    name,
		QueryType:    queryType,
		Iterations:   n,
		AvgLatencyMs: sumLatency / float64(n),
		P50LatencyMs: latencies[n/2],
		P95LatencyMs: latencies[int(float64(n)*0.95)],
		P99LatencyMs: latencies[int(float64(n)*0.99)],
		MinLatencyMs: latencies[0],
		MaxLatencyMs: latencies[n-1],
		AvgRows:      float64(sumRows) / float64(n),
		AvgBytes:     float64(sumBytes) / float64(n),
		Timestamp:    time.Now(),
	}
}

func runQueries(cfg *Config) []BenchmarkResult {
	client := &http.Client{Timeout: 120 * time.Second}

	// Define benchmark queries (use just table name, database is set via header)
	queries := []struct {
		name      string
		queryType string
		sql       string
	}{
		// Exact match queries (benefit most from bloom filters)
		{
			name:      "exact_match_level_error",
			queryType: "exact_match",
			sql:       "SELECT * FROM logs WHERE level = 'ERROR' LIMIT 10000",
		},
		{
			name:      "exact_match_level_fatal",
			queryType: "exact_match",
			sql:       "SELECT * FROM logs WHERE level = 'FATAL' LIMIT 10000",
		},
		{
			name:      "exact_match_service",
			queryType: "exact_match",
			sql:       "SELECT * FROM logs WHERE service = 'api-gateway' LIMIT 10000",
		},
		{
			name:      "exact_match_host",
			queryType: "exact_match",
			sql:       "SELECT * FROM logs WHERE host = 'prod-server-01' LIMIT 10000",
		},
		{
			name:      "multi_exact_match",
			queryType: "exact_match",
			sql:       "SELECT * FROM logs WHERE level = 'ERROR' AND service = 'api-gateway' LIMIT 10000",
		},

		// Prefix search queries (partial benefit from bloom filters)
		{
			name:      "prefix_search_message",
			queryType: "prefix_search",
			sql:       "SELECT * FROM logs WHERE message LIKE 'Failed%' LIMIT 10000",
		},
		{
			name:      "prefix_search_trace",
			queryType: "prefix_search",
			sql:       "SELECT * FROM logs WHERE trace_id LIKE 'a%' LIMIT 10000",
		},

		// Substring search queries (no benefit from bloom filters)
		{
			name:      "substring_search_timeout",
			queryType: "substring_search",
			sql:       "SELECT * FROM logs WHERE message LIKE '%timeout%' LIMIT 10000",
		},
		{
			name:      "substring_search_connection",
			queryType: "substring_search",
			sql:       "SELECT * FROM logs WHERE message LIKE '%connection%' LIMIT 10000",
		},
		{
			name:      "substring_search_error",
			queryType: "substring_search",
			sql:       "SELECT * FROM logs WHERE message LIKE '%error%' LIMIT 10000",
		},

		// Count queries
		{
			name:      "count_all",
			queryType: "aggregation",
			sql:       "SELECT COUNT(*) FROM logs",
		},
		{
			name:      "count_by_level",
			queryType: "aggregation",
			sql:       "SELECT level, COUNT(*) as cnt FROM logs GROUP BY level ORDER BY cnt DESC",
		},
		{
			name:      "count_errors_by_service",
			queryType: "aggregation",
			sql:       "SELECT service, COUNT(*) as cnt FROM logs WHERE level = 'ERROR' GROUP BY service ORDER BY cnt DESC",
		},
	}

	fmt.Printf("\nRunning %d queries with %d iterations each...\n\n", len(queries), cfg.Iterations)

	var results []BenchmarkResult
	for _, q := range queries {
		fmt.Printf("Running: %s\n", q.name)
		result := benchmarkQuery(cfg, client, q.name, q.queryType, q.sql)
		results = append(results, result)
		fmt.Printf("  Avg: %.2fms, P50: %.2fms, P95: %.2fms, P99: %.2fms, Rows: %.0f\n",
			result.AvgLatencyMs, result.P50LatencyMs, result.P95LatencyMs, result.P99LatencyMs, result.AvgRows)
	}

	return results
}

func printResults(results []BenchmarkResult) {
	fmt.Println("\n" + strings.Repeat("=", 100))
	fmt.Println("BENCHMARK RESULTS")
	fmt.Println(strings.Repeat("=", 100))

	fmt.Printf("\n%-30s %-15s %10s %10s %10s %10s %10s\n",
		"Query", "Type", "Avg(ms)", "P50(ms)", "P95(ms)", "P99(ms)", "Rows")
	fmt.Println(strings.Repeat("-", 100))

	for _, r := range results {
		fmt.Printf("%-30s %-15s %10.2f %10.2f %10.2f %10.2f %10.0f\n",
			r.QueryName, r.QueryType, r.AvgLatencyMs, r.P50LatencyMs, r.P95LatencyMs, r.P99LatencyMs, r.AvgRows)
	}
	fmt.Println(strings.Repeat("=", 100))

	// Summary by query type
	fmt.Println("\nSUMMARY BY QUERY TYPE:")
	typeStats := make(map[string][]float64)
	for _, r := range results {
		typeStats[r.QueryType] = append(typeStats[r.QueryType], r.AvgLatencyMs)
	}

	for qtype, latencies := range typeStats {
		var sum float64
		for _, l := range latencies {
			sum += l
		}
		avg := sum / float64(len(latencies))
		fmt.Printf("  %-20s: %.2fms avg across %d queries\n", qtype, avg, len(latencies))
	}
}

func saveResults(results []BenchmarkResult, filename string) error {
	data, err := json.MarshalIndent(results, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filename, data, 0644)
}

func main() {
	cfg := &Config{}

	flag.StringVar(&cfg.Host, "host", "localhost", "Arc host")
	flag.IntVar(&cfg.Port, "port", 8000, "Arc port")
	flag.StringVar(&cfg.Token, "token", "", "Auth token")
	flag.StringVar(&cfg.Database, "database", "benchmark_logs", "Database name")
	flag.IntVar(&cfg.Records, "records", 1000000, "Number of log records to generate")
	flag.IntVar(&cfg.BatchSize, "batch-size", 50000, "Batch size for ingestion")
	flag.IntVar(&cfg.Workers, "workers", 4, "Number of ingest workers")
	flag.IntVar(&cfg.Iterations, "iterations", 10, "Query iterations")
	flag.BoolVar(&cfg.IngestOnly, "ingest-only", false, "Only run ingestion")
	flag.BoolVar(&cfg.QueryOnly, "query-only", false, "Only run queries (skip ingestion)")
	flag.StringVar(&cfg.OutputFile, "output", "", "Output file for results (JSON)")

	flag.Parse()

	fmt.Println("Arc Bloom Filter Benchmark")
	fmt.Println("==========================")
	fmt.Printf("Host: %s:%d\n", cfg.Host, cfg.Port)
	fmt.Printf("Database: %s\n", cfg.Database)

	// Run ingestion if requested
	if !cfg.QueryOnly {
		fmt.Printf("\n--- INGESTION PHASE ---\n")
		fmt.Printf("Records: %d\n", cfg.Records)
		fmt.Printf("Batch size: %d\n", cfg.BatchSize)
		fmt.Printf("Workers: %d\n", cfg.Workers)

		if err := runIngest(cfg); err != nil {
			fmt.Printf("Ingestion error: %v\n", err)
			os.Exit(1)
		}

		// Give Arc a moment to flush buffers
		fmt.Println("\nWaiting 5s for buffer flush...")
		time.Sleep(5 * time.Second)
	}

	// Run queries if requested
	if !cfg.IngestOnly {
		fmt.Printf("\n--- QUERY PHASE ---\n")
		fmt.Printf("Iterations: %d\n", cfg.Iterations)

		results := runQueries(cfg)
		printResults(results)

		if cfg.OutputFile != "" {
			if err := saveResults(results, cfg.OutputFile); err != nil {
				fmt.Printf("Error saving results: %v\n", err)
			} else {
				fmt.Printf("\nResults saved to: %s\n", cfg.OutputFile)
			}
		}
	}

	fmt.Println("\nBenchmark complete!")
}
