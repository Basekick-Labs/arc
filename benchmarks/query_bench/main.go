// Query benchmark for Arc
// Usage: go run benchmarks/query_bench.go [flags]
//
// Examples:
//   go run benchmarks/query_bench.go --query "SELECT * FROM production.cpu LIMIT 100000"
//   go run benchmarks/query_bench.go --format arrow --query "SELECT * FROM production.cpu LIMIT 500000"
//   go run benchmarks/query_bench.go --iterations 10 --format json

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
	Query      string
	Format     string // "json" or "arrow"
	Iterations int
	Rows       int64 // Expected row count (for throughput calculation)
	Host       string
	Port       int
	Token      string
	Database   string
}

type QueryRequest struct {
	SQL string `json:"sql"`
}

type Stats struct {
	latencies    []float64
	bytesRead    []int64
	rowsReturned []int64
}

func (s *Stats) addResult(latencyMs float64, bytes int64, rows int64) {
	s.latencies = append(s.latencies, latencyMs)
	s.bytesRead = append(s.bytesRead, bytes)
	s.rowsReturned = append(s.rowsReturned, rows)
}

func (s *Stats) getPercentile(data []float64, p float64) float64 {
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

func (s *Stats) avg(data []int64) float64 {
	if len(data) == 0 {
		return 0
	}
	var sum int64
	for _, v := range data {
		sum += v
	}
	return float64(sum) / float64(len(data))
}

func (s *Stats) avgFloat(data []float64) float64 {
	if len(data) == 0 {
		return 0
	}
	var sum float64
	for _, v := range data {
		sum += v
	}
	return sum / float64(len(data))
}

func runQuery(cfg *Config, client *http.Client) (latencyMs float64, respBytes int64, rows int64, err error) {
	var url string
	var contentType string
	var body []byte

	if cfg.Format == "arrow" {
		url = fmt.Sprintf("http://%s:%d/api/v1/query/arrow", cfg.Host, cfg.Port)
		contentType = "application/json"
		reqBody := QueryRequest{SQL: cfg.Query}
		body, _ = json.Marshal(reqBody)
	} else {
		url = fmt.Sprintf("http://%s:%d/api/v1/query", cfg.Host, cfg.Port)
		contentType = "application/json"
		reqBody := QueryRequest{SQL: cfg.Query}
		body, _ = json.Marshal(reqBody)
	}

	start := time.Now()

	req, err := http.NewRequest("POST", url, bytes.NewReader(body))
	if err != nil {
		return 0, 0, 0, err
	}

	req.Header.Set("Content-Type", contentType)
	req.Header.Set("x-arc-database", cfg.Database)
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

	// Try to extract row count from JSON response
	if cfg.Format == "json" {
		var result map[string]interface{}
		if err := json.Unmarshal(respBody, &result); err == nil {
			if data, ok := result["data"].([]interface{}); ok {
				rows = int64(len(data))
			}
		}
	} else {
		// For Arrow, we can estimate from response header or just use a marker
		// The actual row count would require parsing the Arrow IPC format
		// For now, we'll rely on the query returning a known number of rows
		rows = -1 // Will be set from first JSON query or manually
	}

	return latencyMs, respBytes, rows, nil
}

func main() {
	cfg := Config{}

	flag.StringVar(&cfg.Query, "query", "SELECT * FROM production.cpu LIMIT 100000", "SQL query to execute")
	flag.StringVar(&cfg.Format, "format", "both", "Response format: json, arrow, or both")
	flag.IntVar(&cfg.Iterations, "iterations", 5, "Number of iterations per format")
	flag.Int64Var(&cfg.Rows, "rows", 0, "Expected row count (for throughput calculation, 0=auto-detect from JSON)")
	flag.StringVar(&cfg.Host, "host", "localhost", "Server host")
	flag.IntVar(&cfg.Port, "port", 8000, "Server port")
	flag.StringVar(&cfg.Database, "database", "production", "Database name")
	flag.Parse()

	cfg.Token = os.Getenv("ARC_TOKEN")

	fmt.Println("================================================================================")
	fmt.Println("QUERY BENCHMARK - GO CLIENT")
	fmt.Println("================================================================================")
	fmt.Printf("Target: http://%s:%d\n", cfg.Host, cfg.Port)
	fmt.Printf("Query: %s\n", cfg.Query)
	fmt.Printf("Iterations: %d per format\n", cfg.Iterations)
	fmt.Printf("Format: %s\n", cfg.Format)
	fmt.Println("================================================================================")
	fmt.Println()

	if cfg.Token != "" {
		fmt.Printf("Using auth token: %s...\n", cfg.Token[:min(8, len(cfg.Token))])
	} else {
		fmt.Println("No ARC_TOKEN set - authentication may fail")
	}

	client := &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        10,
			MaxIdleConnsPerHost: 10,
			IdleConnTimeout:     30 * time.Second,
		},
		Timeout: 300 * time.Second, // Long timeout for large queries
	}

	formats := []string{}
	if cfg.Format == "both" {
		formats = []string{"json", "arrow"}
	} else {
		formats = []string{cfg.Format}
	}

	results := make(map[string]*Stats)

	for _, format := range formats {
		fmt.Printf("\n--- Testing %s format ---\n", format)
		testCfg := cfg
		testCfg.Format = format

		stats := &Stats{}

		// Warmup
		fmt.Println("Warmup run...")
		_, _, _, err := runQuery(&testCfg, client)
		if err != nil {
			fmt.Printf("Warmup failed: %v\n", err)
			continue
		}

		// Actual runs
		for i := 0; i < cfg.Iterations; i++ {
			latency, bytes, rows, err := runQuery(&testCfg, client)
			if err != nil {
				fmt.Printf("  Run %d: ERROR - %v\n", i+1, err)
				continue
			}
			stats.addResult(latency, bytes, rows)
			fmt.Printf("  Run %d: %.2f ms, %.2f MB", i+1, latency, float64(bytes)/(1024*1024))
			if rows > 0 {
				fmt.Printf(", %d rows", rows)
			}
			fmt.Println()
		}

		results[format] = stats
	}

	// Summary
	fmt.Println()
	fmt.Println("================================================================================")
	fmt.Println("RESULTS")
	fmt.Println("================================================================================")

	for _, format := range formats {
		stats := results[format]
		if stats == nil || len(stats.latencies) == 0 {
			continue
		}

		avgLatency := stats.avgFloat(stats.latencies)
		avgBytes := stats.avg(stats.bytesRead)
		avgRows := stats.avg(stats.rowsReturned)

		// Use configured rows if provided and auto-detect failed
		if avgRows <= 0 && cfg.Rows > 0 {
			avgRows = float64(cfg.Rows)
		}

		fmt.Printf("\n%s Format:\n", format)
		fmt.Printf("  Avg latency:     %.2f ms\n", avgLatency)
		fmt.Printf("  p50 latency:     %.2f ms\n", stats.getPercentile(stats.latencies, 0.50))
		fmt.Printf("  p95 latency:     %.2f ms\n", stats.getPercentile(stats.latencies, 0.95))
		fmt.Printf("  p99 latency:     %.2f ms\n", stats.getPercentile(stats.latencies, 0.99))
		fmt.Printf("  Avg response:    %.2f MB\n", avgBytes/(1024*1024))

		if avgRows > 0 && avgLatency > 0 {
			rowsPerSec := (avgRows / avgLatency) * 1000
			fmt.Printf("  🚀 Throughput:   %.2f M rows/sec\n", rowsPerSec/1_000_000)
		}
	}

	// Comparison if both formats tested
	if len(formats) == 2 && results["json"] != nil && results["arrow"] != nil {
		jsonStats := results["json"]
		arrowStats := results["arrow"]

		if len(jsonStats.latencies) > 0 && len(arrowStats.latencies) > 0 {
			jsonAvg := jsonStats.avgFloat(jsonStats.latencies)
			arrowAvg := arrowStats.avgFloat(arrowStats.latencies)
			jsonBytes := jsonStats.avg(jsonStats.bytesRead)
			arrowBytes := arrowStats.avg(arrowStats.bytesRead)

			fmt.Println()
			fmt.Println("--- Comparison ---")
			fmt.Printf("  Arrow vs JSON latency: %.1fx %s\n",
				max(jsonAvg/arrowAvg, arrowAvg/jsonAvg),
				map[bool]string{true: "faster", false: "slower"}[arrowAvg < jsonAvg])
			fmt.Printf("  Arrow vs JSON size:    %.1fx %s\n",
				max(jsonBytes/arrowBytes, arrowBytes/jsonBytes),
				map[bool]string{true: "smaller", false: "larger"}[arrowBytes < jsonBytes])
		}
	}

	fmt.Println()
	fmt.Println("================================================================================")
}
