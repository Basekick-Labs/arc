// Query benchmark suite for Arc
// Tests various query patterns to measure throughput
// Usage: go run benchmarks/query_suite.go [flags]
//
// Examples:
//   go run benchmarks/query_suite.go
//   go run benchmarks/query_suite.go --iterations 10
//   go run benchmarks/query_suite.go --table production.cpu

package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"time"
)

type Config struct {
	Table       string
	Measurement string // Just the table name without database prefix
	Iterations  int
	Host        string
	Port        int
	Token       string
	Database    string
	UseHeader   bool // If true, use x-arc-database header instead of db.table syntax
}

type QueryRequest struct {
	SQL string `json:"sql"`
}

type BenchmarkQuery struct {
	Name        string
	SQL         string
	ExpectedRows int64 // 0 means auto-detect or variable
}

type QueryResult struct {
	Name       string
	Latencies  []float64
	BytesRead  []int64
	RowCounts  []int64
	Errors     int
}

func (r *QueryResult) avgLatency() float64 {
	if len(r.Latencies) == 0 {
		return 0
	}
	var sum float64
	for _, v := range r.Latencies {
		sum += v
	}
	return sum / float64(len(r.Latencies))
}

func (r *QueryResult) p50Latency() float64 {
	return percentile(r.Latencies, 0.50)
}

func (r *QueryResult) avgRows() float64 {
	if len(r.RowCounts) == 0 {
		return 0
	}
	var sum int64
	for _, v := range r.RowCounts {
		sum += v
	}
	return float64(sum) / float64(len(r.RowCounts))
}

func (r *QueryResult) avgBytes() float64 {
	if len(r.BytesRead) == 0 {
		return 0
	}
	var sum int64
	for _, v := range r.BytesRead {
		sum += v
	}
	return float64(sum) / float64(len(r.BytesRead))
}

func percentile(data []float64, p float64) float64 {
	if len(data) == 0 {
		return 0
	}
	sorted := make([]float64, len(data))
	copy(sorted, data)
	sort.Float64s(sorted)
	idx := int(float64(len(sorted)) * p)
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func runQuery(cfg *Config, sql string, format string, client *http.Client) (latencyMs float64, respBytes int64, rows int64, err error) {
	var url string
	if format == "arrow" {
		url = fmt.Sprintf("http://%s:%d/api/v1/query/arrow", cfg.Host, cfg.Port)
	} else {
		url = fmt.Sprintf("http://%s:%d/api/v1/query", cfg.Host, cfg.Port)
	}

	reqBody := QueryRequest{SQL: sql}
	body, _ := json.Marshal(reqBody)

	start := time.Now()

	req, err := http.NewRequest("POST", url, bytes.NewReader(body))
	if err != nil {
		return 0, 0, 0, err
	}

	req.Header.Set("Content-Type", "application/json")
	// Only set header when using header mode (optimized path)
	if cfg.UseHeader {
		req.Header.Set("x-arc-database", cfg.Database)
	}
	if cfg.Token != "" {
		req.Header.Set("Authorization", "Bearer "+cfg.Token)
	}

	resp, err := client.Do(req)
	if err != nil {
		return 0, 0, 0, err
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, 0, 0, err
	}

	latencyMs = float64(time.Since(start).Microseconds()) / 1000.0
	respBytes = int64(len(respBody))

	if resp.StatusCode != 200 {
		return latencyMs, respBytes, 0, fmt.Errorf("status %d: %s", resp.StatusCode, string(respBody[:min(200, len(respBody))]))
	}

	// Extract row count from JSON response
	if format == "json" {
		var result map[string]interface{}
		if err := json.Unmarshal(respBody, &result); err == nil {
			if data, ok := result["data"].([]interface{}); ok {
				rows = int64(len(data))
			} else if rowCount, ok := result["row_count"].(float64); ok {
				rows = int64(rowCount)
			}
		}
	}

	return latencyMs, respBytes, rows, nil
}

func main() {
	cfg := Config{}

	flag.StringVar(&cfg.Measurement, "measurement", "cpu", "Measurement (table) name to benchmark")
	flag.IntVar(&cfg.Iterations, "iterations", 5, "Number of iterations per query")
	flag.StringVar(&cfg.Host, "host", "localhost", "Server host")
	flag.IntVar(&cfg.Port, "port", 8000, "Server port")
	flag.StringVar(&cfg.Database, "database", "production", "Database name")
	flag.BoolVar(&cfg.UseHeader, "use-header", false, "Use x-arc-database header instead of db.table syntax")
	flag.Parse()

	cfg.Token = os.Getenv("ARC_TOKEN")

	// Build table reference based on mode
	// Header mode: just "cpu" (simpler, header provides database)
	// Legacy mode: "production.cpu" (db.table syntax in SQL)
	if cfg.UseHeader {
		cfg.Table = cfg.Measurement
	} else {
		cfg.Table = fmt.Sprintf("%s.%s", cfg.Database, cfg.Measurement)
	}

	// Define benchmark queries
	queries := []BenchmarkQuery{
		// Count queries (full table scan)
		{
			Name: "COUNT(*) - Full Table",
			SQL:  fmt.Sprintf("SELECT count(*) FROM %s", cfg.Table),
		},

		// Small result sets
		{
			Name:         "SELECT LIMIT 1K",
			SQL:          fmt.Sprintf("SELECT * FROM %s LIMIT 1000", cfg.Table),
			ExpectedRows: 1000,
		},
		{
			Name:         "SELECT LIMIT 10K",
			SQL:          fmt.Sprintf("SELECT * FROM %s LIMIT 10000", cfg.Table),
			ExpectedRows: 10000,
		},
		{
			Name:         "SELECT LIMIT 100K",
			SQL:          fmt.Sprintf("SELECT * FROM %s LIMIT 100000", cfg.Table),
			ExpectedRows: 100000,
		},
		{
			Name:         "SELECT LIMIT 500K",
			SQL:          fmt.Sprintf("SELECT * FROM %s LIMIT 500000", cfg.Table),
			ExpectedRows: 500000,
		},
		{
			Name:         "SELECT LIMIT 1M",
			SQL:          fmt.Sprintf("SELECT * FROM %s LIMIT 1000000", cfg.Table),
			ExpectedRows: 1000000,
		},

		// Aggregation queries
		{
			Name: "AVG/MIN/MAX Aggregation",
			SQL:  fmt.Sprintf("SELECT AVG(value), MIN(value), MAX(value) FROM %s", cfg.Table),
		},
		{
			Name: "GROUP BY host (Top 10)",
			SQL:  fmt.Sprintf("SELECT host, COUNT(*), AVG(value) FROM %s GROUP BY host ORDER BY COUNT(*) DESC LIMIT 10", cfg.Table),
		},

		// Time-based queries
		{
			Name: "Last 1 hour",
			SQL:  fmt.Sprintf("SELECT * FROM %s WHERE time > NOW() - INTERVAL '1 hour' LIMIT 100000", cfg.Table),
		},
	}

	fmt.Println("================================================================================")
	fmt.Println("QUERY BENCHMARK SUITE - Arc")
	fmt.Println("================================================================================")
	fmt.Printf("Target: http://%s:%d\n", cfg.Host, cfg.Port)
	fmt.Printf("Table: %s\n", cfg.Table)
	fmt.Printf("Database: %s\n", cfg.Database)
	fmt.Printf("Mode: %s\n", map[bool]string{true: "Header (x-arc-database)", false: "Legacy (db.table)"}[cfg.UseHeader])
	fmt.Printf("Iterations: %d per query\n", cfg.Iterations)
	fmt.Println("================================================================================")
	fmt.Println()

	if cfg.Token != "" {
		fmt.Printf("Using auth token: %s...\n\n", cfg.Token[:min(8, len(cfg.Token))])
	}

	client := &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        10,
			MaxIdleConnsPerHost: 10,
			IdleConnTimeout:     30 * time.Second,
		},
		Timeout: 600 * time.Second,
	}

	// Test both formats
	formats := []string{"json", "arrow"}
	allResults := make(map[string]map[string]*QueryResult)

	for _, format := range formats {
		fmt.Printf("=== Testing %s format ===\n\n", format)
		allResults[format] = make(map[string]*QueryResult)

		for _, q := range queries {
			result := &QueryResult{Name: q.Name}
			fmt.Printf("Query: %s\n", q.Name)

			// Warmup
			_, _, _, err := runQuery(&cfg, q.SQL, format, client)
			if err != nil {
				fmt.Printf("  Warmup ERROR: %v\n\n", err)
				result.Errors++
				allResults[format][q.Name] = result
				continue
			}

			// Benchmark runs
			for i := 0; i < cfg.Iterations; i++ {
				latency, bytes, rows, err := runQuery(&cfg, q.SQL, format, client)
				if err != nil {
					result.Errors++
					continue
				}
				result.Latencies = append(result.Latencies, latency)
				result.BytesRead = append(result.BytesRead, bytes)

				// Use expected rows if specified, otherwise use detected
				if q.ExpectedRows > 0 {
					result.RowCounts = append(result.RowCounts, q.ExpectedRows)
				} else if rows > 0 {
					result.RowCounts = append(result.RowCounts, rows)
				}
			}

			if len(result.Latencies) > 0 {
				avgLatency := result.avgLatency()
				avgRows := result.avgRows()
				avgBytes := result.avgBytes()

				fmt.Printf("  Latency: %.2f ms (p50: %.2f ms)\n", avgLatency, result.p50Latency())
				fmt.Printf("  Response: %.2f MB\n", avgBytes/(1024*1024))
				if avgRows > 0 {
					throughput := (avgRows / avgLatency) * 1000
					fmt.Printf("  Rows: %.0f\n", avgRows)
					fmt.Printf("  🚀 Throughput: %.2f M rows/sec\n", throughput/1_000_000)
				}
			}
			fmt.Println()

			allResults[format][q.Name] = result
		}
	}

	// Summary comparison
	fmt.Println("================================================================================")
	fmt.Println("SUMMARY - Arrow vs JSON Comparison")
	fmt.Println("================================================================================")
	fmt.Printf("%-30s | %-15s | %-15s | %-10s\n", "Query", "Arrow (ms)", "JSON (ms)", "Speedup")
	fmt.Println("--------------------------------------------------------------------------------")

	for _, q := range queries {
		arrowResult := allResults["arrow"][q.Name]
		jsonResult := allResults["json"][q.Name]

		if arrowResult == nil || jsonResult == nil {
			continue
		}

		arrowLatency := arrowResult.avgLatency()
		jsonLatency := jsonResult.avgLatency()

		speedup := jsonLatency / arrowLatency
		if arrowLatency == 0 {
			speedup = 0
		}

		fmt.Printf("%-30s | %13.2f | %13.2f | %8.2fx\n",
			q.Name[:min(30, len(q.Name))], arrowLatency, jsonLatency, speedup)
	}

	// Best throughput results
	fmt.Println()
	fmt.Println("================================================================================")
	fmt.Println("BEST THROUGHPUT RESULTS")
	fmt.Println("================================================================================")

	for _, format := range formats {
		fmt.Printf("\n%s Format:\n", format)
		var bestThroughput float64
		var bestQuery string

		for name, result := range allResults[format] {
			avgRows := result.avgRows()
			avgLatency := result.avgLatency()
			if avgRows > 0 && avgLatency > 0 {
				throughput := (avgRows / avgLatency) * 1000
				if throughput > bestThroughput {
					bestThroughput = throughput
					bestQuery = name
				}
			}
		}

		if bestThroughput > 0 {
			fmt.Printf("  Best: %s\n", bestQuery)
			fmt.Printf("  🚀 %.2f M rows/sec\n", bestThroughput/1_000_000)
		}
	}

	fmt.Println()
	fmt.Println("================================================================================")
}
