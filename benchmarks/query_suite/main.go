// Arc query benchmark suite — measures query latency across a set of queries
// to detect performance regression or improvement.
//
// Presets:
//   generic — auto-generated queries against any measurement (default)
//   logs    — log-specific queries (count, filter by level/service, full-text, etc.)
//
// Examples:
//   go run benchmarks/query_suite/main.go --database production --measurement cpu
//   go run benchmarks/query_suite/main.go --target arc-arrow --database production --measurement cpu
//   go run benchmarks/query_suite/main.go --preset logs --database logs --measurement logs

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
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow/ipc"
)

type Config struct {
	Target      string // "arc" or "arc-arrow"
	Preset      string // "generic" or "logs"
	Measurement string
	Database    string
	Iterations  int
	Host        string
	Port        int
	Token       string
}

type BenchmarkQuery struct {
	Name string
	SQL  string
}

type QueryResult struct {
	Name      string
	Latencies []float64
	RowCounts []int64
	Errors    int
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

func (r *QueryResult) p50Latency() float64 { return percentile(r.Latencies, 0.50) }
func (r *QueryResult) p99Latency() float64 { return percentile(r.Latencies, 0.99) }

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

// runArcQuery executes a query via the JSON endpoint.
func runArcQuery(cfg *Config, sql string, client *http.Client) (latencyMs float64, rows int64, err error) {
	url := fmt.Sprintf("http://%s:%d/api/v1/query", cfg.Host, cfg.Port)

	body, _ := json.Marshal(map[string]string{"sql": sql})
	start := time.Now()

	req, err := http.NewRequest("POST", url, bytes.NewReader(body))
	if err != nil {
		return 0, 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-arc-database", cfg.Database)
	if cfg.Token != "" {
		req.Header.Set("Authorization", "Bearer "+cfg.Token)
	}

	resp, err := client.Do(req)
	if err != nil {
		return 0, 0, err
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, 0, err
	}

	latencyMs = float64(time.Since(start).Microseconds()) / 1000.0

	if resp.StatusCode != 200 {
		return latencyMs, 0, fmt.Errorf("status %d: %s", resp.StatusCode, string(respBody[:min(200, len(respBody))]))
	}

	var result map[string]interface{}
	if err := json.Unmarshal(respBody, &result); err == nil {
		if data, ok := result["data"].([]interface{}); ok {
			rows = int64(len(data))
			// For count queries, extract the actual count from [[count]] format
			if len(data) == 1 {
				if row, ok := data[0].([]interface{}); ok && len(row) == 1 {
					if count, ok := row[0].(float64); ok {
						rows = int64(count)
					}
				}
			}
		}
	}

	return latencyMs, rows, nil
}

// runArcArrowQuery executes a query via the Arrow IPC streaming endpoint.
func runArcArrowQuery(cfg *Config, sql string, client *http.Client) (latencyMs float64, rows int64, err error) {
	url := fmt.Sprintf("http://%s:%d/api/v1/query/arrow", cfg.Host, cfg.Port)

	body, _ := json.Marshal(map[string]string{"sql": sql})
	start := time.Now()

	req, err := http.NewRequest("POST", url, bytes.NewReader(body))
	if err != nil {
		return 0, 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-arc-database", cfg.Database)
	if cfg.Token != "" {
		req.Header.Set("Authorization", "Bearer "+cfg.Token)
	}

	resp, err := client.Do(req)
	if err != nil {
		return 0, 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		respBody, _ := io.ReadAll(resp.Body)
		return 0, 0, fmt.Errorf("status %d: %s", resp.StatusCode, string(respBody[:min(200, len(respBody))]))
	}

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		latencyMs = float64(time.Since(start).Microseconds()) / 1000.0
		return latencyMs, 0, fmt.Errorf("failed to read response: %v", err)
	}

	reader, err := ipc.NewReader(bytes.NewReader(respBody))
	if err != nil {
		latencyMs = float64(time.Since(start).Microseconds()) / 1000.0
		return latencyMs, 0, fmt.Errorf("failed to create Arrow reader: %v", err)
	}
	defer reader.Release()

	for reader.Next() {
		record := reader.Record()
		rows += record.NumRows()
		if record.NumCols() == 1 && record.NumRows() == 1 {
			col := record.Column(0)
			if col.Len() > 0 && strings.Contains(strings.ToLower(sql), "count(") {
				if arr, ok := col.(interface{ Value(int) int64 }); ok {
					rows = arr.Value(0)
				}
			}
		}
	}

	latencyMs = float64(time.Since(start).Microseconds()) / 1000.0

	if err := reader.Err(); err != nil {
		return latencyMs, rows, fmt.Errorf("Arrow reader error: %v", err)
	}

	return latencyMs, rows, nil
}

func runQuery(cfg *Config, q BenchmarkQuery, client *http.Client) (float64, int64, error) {
	if cfg.Target == "arc-arrow" {
		return runArcArrowQuery(cfg, q.SQL, client)
	}
	return runArcQuery(cfg, q.SQL, client)
}

// genericQueries returns queries that work against any time-series measurement.
func genericQueries(m string) []BenchmarkQuery {
	return []BenchmarkQuery{
		{"Count All", fmt.Sprintf("SELECT count(*) FROM %s", m)},
		{"Select * LIMIT 1K", fmt.Sprintf("SELECT * FROM %s LIMIT 1000", m)},
		{"Select * LIMIT 10K", fmt.Sprintf("SELECT * FROM %s LIMIT 10000", m)},
		{"Select * LIMIT 100K", fmt.Sprintf("SELECT * FROM %s LIMIT 100000", m)},
		{"Select * LIMIT 500K", fmt.Sprintf("SELECT * FROM %s LIMIT 500000", m)},
		{"Select * LIMIT 1M", fmt.Sprintf("SELECT * FROM %s LIMIT 1000000", m)},
		{"Time Range (1h)", fmt.Sprintf("SELECT * FROM %s WHERE time > NOW() - INTERVAL '1 hour' LIMIT 10000", m)},
		{"Time Range (24h)", fmt.Sprintf("SELECT * FROM %s WHERE time > NOW() - INTERVAL '24 hours' LIMIT 10000", m)},
		{"Time Range (7d)", fmt.Sprintf("SELECT * FROM %s WHERE time > NOW() - INTERVAL '7 days' LIMIT 10000", m)},
		{"Time Bucket (1h, 24h)", fmt.Sprintf("SELECT time_bucket('1 hour', time) AS bucket, count(*) FROM %s WHERE time > NOW() - INTERVAL '24 hours' GROUP BY bucket ORDER BY bucket", m)},
		{"Time Bucket (1h, 7d)", fmt.Sprintf("SELECT time_bucket('1 hour', time) AS bucket, count(*) FROM %s WHERE time > NOW() - INTERVAL '7 days' GROUP BY bucket ORDER BY bucket", m)},
		{"Date Trunc (day, 30d)", fmt.Sprintf("SELECT date_trunc('day', time) AS d, count(*) FROM %s WHERE time > NOW() - INTERVAL '30 days' GROUP BY d ORDER BY d", m)},
		// Aggregations
		{"SUM/AVG/MIN/MAX", fmt.Sprintf("SELECT SUM(value), AVG(value), MIN(value), MAX(value) FROM %s", m)},
		{"Multi-column AGG", fmt.Sprintf("SELECT SUM(value), SUM(cpu_user), AVG(cpu_idle), COUNT(*) FROM %s", m)},
		{"GROUP BY host", fmt.Sprintf("SELECT host, COUNT(*), AVG(value), MAX(cpu_user) FROM %s GROUP BY host ORDER BY COUNT(*) DESC LIMIT 100", m)},
		{"GROUP BY host + hour", fmt.Sprintf("SELECT host, date_trunc('hour', time) AS h, AVG(value) FROM %s GROUP BY host, h ORDER BY h DESC LIMIT 1000", m)},
		{"DISTINCT hosts", fmt.Sprintf("SELECT DISTINCT host FROM %s", m)},
		{"Percentile (p95)", fmt.Sprintf("SELECT quantile_cont(value, 0.95) FROM %s", m)},
		{"Top 10 by AVG", fmt.Sprintf("SELECT host, AVG(value) AS avg_val FROM %s GROUP BY host ORDER BY avg_val DESC LIMIT 10", m)},
		{"HAVING filter", fmt.Sprintf("SELECT host, AVG(value) AS avg_v FROM %s GROUP BY host HAVING AVG(value) > 50 ORDER BY avg_v DESC", m)},
	}
}

// logsQueries returns queries specific to a logs measurement.
func logsQueries(m string) []BenchmarkQuery {
	return []BenchmarkQuery{
		{"Count All Logs", fmt.Sprintf("SELECT count(*) FROM %s", m)},
		{"Filter by Level (ERROR)", fmt.Sprintf("SELECT * FROM %s WHERE level = 'ERROR' LIMIT 1000", m)},
		{"Filter by Service (api)", fmt.Sprintf("SELECT * FROM %s WHERE service = 'api' LIMIT 1000", m)},
		{"Full-text Search (timeout)", fmt.Sprintf("SELECT * FROM %s WHERE message LIKE '%%timeout%%' LIMIT 1000", m)},
		{"Time Range (Last 1h)", fmt.Sprintf("SELECT * FROM %s WHERE time > NOW() - INTERVAL '1 hour' LIMIT 10000", m)},
		{"Top 10 Services", fmt.Sprintf("SELECT service, count(*) as cnt FROM %s GROUP BY service ORDER BY cnt DESC LIMIT 10", m)},
		{"Complex (api + ERROR)", fmt.Sprintf("SELECT * FROM %s WHERE service = 'api' AND level = 'ERROR' LIMIT 1000", m)},
		{"Select LIMIT 10K", fmt.Sprintf("SELECT * FROM %s LIMIT 10000", m)},
		{"Time Bucket (1h, 24h)", fmt.Sprintf("SELECT time_bucket('1 hour', time) AS bucket, count(*) FROM %s WHERE time > NOW() - INTERVAL '24 hours' GROUP BY bucket ORDER BY bucket", m)},
	}
}

func main() {
	cfg := Config{}

	flag.StringVar(&cfg.Target, "target", "arc", "Target: arc (JSON) or arc-arrow (Arrow IPC)")
	flag.StringVar(&cfg.Preset, "preset", "generic", "Query preset: generic or logs")
	flag.StringVar(&cfg.Measurement, "measurement", "cpu", "Measurement/table name")
	flag.StringVar(&cfg.Database, "database", "default", "Database name")
	flag.IntVar(&cfg.Iterations, "iterations", 5, "Number of iterations per query")
	flag.StringVar(&cfg.Host, "host", "localhost", "Server host")
	flag.IntVar(&cfg.Port, "port", 8000, "Server port")
	flag.Parse()

	cfg.Target = strings.ToLower(cfg.Target)
	cfg.Token = os.Getenv("ARC_TOKEN")

	if cfg.Target != "arc" && cfg.Target != "arc-arrow" {
		fmt.Printf("Unknown target: %s (use arc or arc-arrow)\n", cfg.Target)
		os.Exit(1)
	}

	var queries []BenchmarkQuery
	switch cfg.Preset {
	case "generic":
		queries = genericQueries(cfg.Measurement)
	case "logs":
		queries = logsQueries(cfg.Measurement)
	default:
		fmt.Printf("Unknown preset: %s (use generic or logs)\n", cfg.Preset)
		os.Exit(1)
	}

	targetLabel := "ARC (JSON)"
	if cfg.Target == "arc-arrow" {
		targetLabel = "ARC (Arrow IPC)"
	}

	fmt.Println("================================================================================")
	fmt.Printf("QUERY BENCHMARK SUITE — %s\n", targetLabel)
	fmt.Println("================================================================================")
	fmt.Printf("Target:      http://%s:%d\n", cfg.Host, cfg.Port)
	fmt.Printf("Database:    %s\n", cfg.Database)
	fmt.Printf("Measurement: %s\n", cfg.Measurement)
	fmt.Printf("Preset:      %s (%d queries)\n", cfg.Preset, len(queries))
	fmt.Printf("Iterations:  %d per query\n", cfg.Iterations)
	fmt.Println("================================================================================")
	fmt.Println()

	if cfg.Token != "" {
		fmt.Printf("Using auth token: %s...\n\n", cfg.Token[:min(8, len(cfg.Token))])
	} else {
		fmt.Println("No ARC_TOKEN set — authentication may fail")
		fmt.Println()
	}

	client := &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        10,
			MaxIdleConnsPerHost: 10,
			IdleConnTimeout:     30 * time.Second,
		},
		Timeout: 600 * time.Second,
	}

	results := make([]*QueryResult, 0, len(queries))

	for _, q := range queries {
		result := &QueryResult{Name: q.Name}
		fmt.Printf("Query: %s\n", q.Name)
		fmt.Printf("  SQL: %s\n", q.SQL)

		// Warmup
		_, _, err := runQuery(&cfg, q, client)
		if err != nil {
			fmt.Printf("  Warmup ERROR: %v\n\n", err)
			result.Errors++
			results = append(results, result)
			continue
		}

		// Benchmark runs
		for i := 0; i < cfg.Iterations; i++ {
			latency, rows, err := runQuery(&cfg, q, client)
			if err != nil {
				result.Errors++
				if result.Errors <= 3 {
					fmt.Printf("  Run %d ERROR: %v\n", i+1, err)
				}
				continue
			}
			result.Latencies = append(result.Latencies, latency)
			result.RowCounts = append(result.RowCounts, rows)
		}

		if len(result.Latencies) > 0 {
			fmt.Printf("  Avg: %.2f ms | p50: %.2f ms | p99: %.2f ms | Rows: %.0f\n",
				result.avgLatency(), result.p50Latency(), result.p99Latency(), result.avgRows())
		}
		if result.Errors > 0 {
			fmt.Printf("  Errors: %d/%d\n", result.Errors, cfg.Iterations)
		}
		fmt.Println()

		results = append(results, result)
	}

	// Summary table
	fmt.Println("================================================================================")
	fmt.Println("SUMMARY")
	fmt.Println("================================================================================")
	fmt.Printf("%-30s | %10s | %10s | %10s | %12s\n", "Query", "Avg (ms)", "p50 (ms)", "p99 (ms)", "Rows")
	fmt.Println("-------------------------------|------------|------------|------------|-------------")

	for _, r := range results {
		name := r.Name
		if len(name) > 30 {
			name = name[:30]
		}
		if len(r.Latencies) > 0 {
			fmt.Printf("%-30s | %10.2f | %10.2f | %10.2f | %12.0f\n",
				name, r.avgLatency(), r.p50Latency(), r.p99Latency(), r.avgRows())
		} else {
			fmt.Printf("%-30s | %10s | %10s | %10s | %12s\n",
				name, "ERROR", "ERROR", "ERROR", "ERROR")
		}
	}

	fmt.Println("================================================================================")
}
