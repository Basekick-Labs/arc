// Query benchmark suite for Arc vs Elasticsearch, ClickHouse, VictoriaLogs, Loki, Quickwit
// Tests various query patterns against the logs table populated by log_bench
// Usage: go run benchmarks/query_suite/main.go [flags]
//
// Examples:
//   go run benchmarks/query_suite/main.go --target arc
//   go run benchmarks/query_suite/main.go --target elastic
//   go run benchmarks/query_suite/main.go --target clickhouse
//   go run benchmarks/query_suite/main.go --target victorialogs
//   go run benchmarks/query_suite/main.go --target loki
//   go run benchmarks/query_suite/main.go --target quickwit

package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow/ipc"
)

// Target systems
const (
	TargetArc          = "arc"
	TargetArcArrow     = "arc-arrow"
	TargetElastic      = "elastic"
	TargetClickHouse   = "clickhouse"
	TargetVictoriaLogs = "victorialogs"
	TargetLoki         = "loki"
	TargetQuickwit     = "quickwit"
)

// Default ports per target
var defaultPorts = map[string]int{
	TargetArc:          8000,
	TargetArcArrow:     8000,
	TargetElastic:      9200,
	TargetClickHouse:   8123,
	TargetVictoriaLogs: 9428,
	TargetLoki:         3100,
	TargetQuickwit:     7280,
}

// Target labels for display
var targetLabels = map[string]string{
	TargetArc:          "ARC",
	TargetArcArrow:     "ARC (ARROW)",
	TargetElastic:      "ELASTICSEARCH",
	TargetClickHouse:   "CLICKHOUSE",
	TargetVictoriaLogs: "VICTORIALOGS",
	TargetLoki:         "GRAFANA LOKI",
	TargetQuickwit:     "QUICKWIT",
}

type Config struct {
	Target     string
	Index      string
	Iterations int
	Host       string
	Port       int
	Token      string
	Database   string // For Arc
}

type BenchmarkQuery struct {
	Name           string
	ArcSQL         string
	ClickHouseSQL  string
	ElasticDSL     map[string]interface{}
	VictoriaLogsQL string
	LokiQL         string
	QuickwitQuery  string
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

func (r *QueryResult) p50Latency() float64 {
	return percentile(r.Latencies, 0.50)
}

func (r *QueryResult) p99Latency() float64 {
	return percentile(r.Latencies, 0.99)
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

// Arc query executor
func runArcQuery(cfg *Config, sql string, client *http.Client) (latencyMs float64, rows int64, err error) {
	url := fmt.Sprintf("http://%s:%d/api/v1/query", cfg.Host, cfg.Port)

	reqBody := map[string]string{"sql": sql}
	body, _ := json.Marshal(reqBody)

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

	// Parse row count
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

// Arc Arrow query executor - uses Arrow IPC streaming format
func runArcArrowQuery(cfg *Config, sql string, client *http.Client) (latencyMs float64, rows int64, err error) {
	arrowURL := fmt.Sprintf("http://%s:%d/api/v1/query/arrow", cfg.Host, cfg.Port)

	reqBody := map[string]string{"sql": sql}
	body, _ := json.Marshal(reqBody)

	start := time.Now()

	req, err := http.NewRequest("POST", arrowURL, bytes.NewReader(body))
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

	// Read entire response into buffer first to avoid canceling server mid-stream
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		latencyMs = float64(time.Since(start).Microseconds()) / 1000.0
		return latencyMs, 0, fmt.Errorf("failed to read response: %v", err)
	}

	// Parse Arrow IPC stream from buffer
	reader, err := ipc.NewReader(bytes.NewReader(respBody))
	if err != nil {
		latencyMs = float64(time.Since(start).Microseconds()) / 1000.0
		return latencyMs, 0, fmt.Errorf("failed to create Arrow reader: %v", err)
	}
	defer reader.Release()

	for reader.Next() {
		record := reader.Record()
		rows += record.NumRows()
		// For count queries, extract the actual count value from the first row
		if record.NumCols() == 1 && record.NumRows() == 1 {
			col := record.Column(0)
			if col.Len() > 0 && strings.Contains(strings.ToLower(sql), "count(") {
				// Read the actual count value from the Arrow column
				// Arrow Int64 columns implement Value(int) int64
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

// Elasticsearch query executor
func runElasticQuery(cfg *Config, queryDSL map[string]interface{}, client *http.Client) (latencyMs float64, rows int64, err error) {
	url := fmt.Sprintf("http://%s:%d/%s/_search", cfg.Host, cfg.Port, cfg.Index)

	body, _ := json.Marshal(queryDSL)

	start := time.Now()

	req, err := http.NewRequest("POST", url, bytes.NewReader(body))
	if err != nil {
		return 0, 0, err
	}

	req.Header.Set("Content-Type", "application/json")

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

	// Parse row count from Elasticsearch response
	var result map[string]interface{}
	if err := json.Unmarshal(respBody, &result); err == nil {
		if hits, ok := result["hits"].(map[string]interface{}); ok {
			// Get total hits
			if total, ok := hits["total"].(map[string]interface{}); ok {
				if value, ok := total["value"].(float64); ok {
					rows = int64(value)
				}
			} else if total, ok := hits["total"].(float64); ok {
				rows = int64(total)
			}
			// If we got actual hits, use that count
			if hitsArray, ok := hits["hits"].([]interface{}); ok && len(hitsArray) > 0 {
				rows = int64(len(hitsArray))
			}
		}
		// For aggregation queries
		if aggs, ok := result["aggregations"].(map[string]interface{}); ok {
			for _, agg := range aggs {
				if aggMap, ok := agg.(map[string]interface{}); ok {
					if buckets, ok := aggMap["buckets"].([]interface{}); ok {
						rows = int64(len(buckets))
					}
				}
			}
		}
	}

	return latencyMs, rows, nil
}

// ClickHouse query executor
func runClickHouseQuery(cfg *Config, sql string, client *http.Client) (latencyMs float64, rows int64, err error) {
	queryURL := fmt.Sprintf("http://%s:%d/?query=%s+FORMAT+JSON", cfg.Host, cfg.Port, url.QueryEscape(sql))

	start := time.Now()

	req, err := http.NewRequest("GET", queryURL, nil)
	if err != nil {
		return 0, 0, err
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

	// Parse row count from ClickHouse JSON response
	var result map[string]interface{}
	if err := json.Unmarshal(respBody, &result); err == nil {
		if data, ok := result["data"].([]interface{}); ok {
			rows = int64(len(data))
			// For count queries, extract the actual count
			if len(data) == 1 {
				if row, ok := data[0].(map[string]interface{}); ok {
					for _, v := range row {
						if count, ok := v.(float64); ok {
							rows = int64(count)
							break
						}
						if countStr, ok := v.(string); ok {
							if count, err := strconv.ParseInt(countStr, 10, 64); err == nil {
								rows = count
								break
							}
						}
					}
				}
			}
		}
		if rowsVal, ok := result["rows"].(float64); ok && rows == 0 {
			rows = int64(rowsVal)
		}
	}

	return latencyMs, rows, nil
}

// VictoriaLogs query executor
func runVictoriaLogsQuery(cfg *Config, logsql string, client *http.Client) (latencyMs float64, rows int64, err error) {
	queryURL := fmt.Sprintf("http://%s:%d/select/logsql/query?query=%s", cfg.Host, cfg.Port, url.QueryEscape(logsql))

	start := time.Now()

	req, err := http.NewRequest("GET", queryURL, nil)
	if err != nil {
		return 0, 0, err
	}
	// VictoriaLogs uses chunked encoding that Go's client can have issues with
	req.Header.Set("Accept-Encoding", "identity")

	resp, err := client.Do(req)
	if err != nil {
		return 0, 0, err
	}
	defer resp.Body.Close()

	// Read response line by line to handle chunked encoding issues
	var lineCount int64
	buf := make([]byte, 0, 1024*1024) // 1MB buffer
	tmp := make([]byte, 32*1024)      // 32KB read chunks

	for {
		n, readErr := resp.Body.Read(tmp)
		if n > 0 {
			buf = append(buf, tmp[:n]...)
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			// Ignore chunked encoding errors and process what we have
			break
		}
	}

	latencyMs = float64(time.Since(start).Microseconds()) / 1000.0

	if resp.StatusCode != 200 {
		return latencyMs, 0, fmt.Errorf("status %d: %s", resp.StatusCode, string(buf[:min(200, len(buf))]))
	}

	// VictoriaLogs returns JSONL - count lines
	content := strings.TrimSpace(string(buf))
	lines := strings.Split(content, "\n")
	lineCount = int64(len(lines))
	if lineCount == 1 && lines[0] == "" {
		lineCount = 0
	}

	// For stats queries, try to extract the count from the JSON response
	// VictoriaLogs stats returns {"total":"123456"} as a string
	if lineCount == 1 && len(content) > 0 {
		var result map[string]interface{}
		if err := json.Unmarshal([]byte(content), &result); err == nil {
			// Try "total" field (from stats count() as total)
			if total, ok := result["total"]; ok {
				switch v := total.(type) {
				case float64:
					lineCount = int64(v)
				case string:
					if count, err := strconv.ParseInt(v, 10, 64); err == nil {
						lineCount = count
					}
				}
			}
		}
	}

	return latencyMs, lineCount, nil
}

// Loki query executor
func runLokiQuery(cfg *Config, logql string, client *http.Client) (latencyMs float64, rows int64, err error) {
	// Use query_range for log queries, query for metric queries
	endpoint := "query_range"
	if strings.Contains(logql, "count_over_time") || strings.Contains(logql, "sum") {
		endpoint = "query"
	}

	now := time.Now()
	start := now.Add(-24 * time.Hour)

	params := url.Values{}
	params.Set("query", logql)
	if endpoint == "query_range" {
		params.Set("start", strconv.FormatInt(start.UnixNano(), 10))
		params.Set("end", strconv.FormatInt(now.UnixNano(), 10))
		params.Set("limit", "5000") // Loki default max_entries_limit_per_query
	}

	queryURL := fmt.Sprintf("http://%s:%d/loki/api/v1/%s?%s", cfg.Host, cfg.Port, endpoint, params.Encode())

	startTime := time.Now()

	req, err := http.NewRequest("GET", queryURL, nil)
	if err != nil {
		return 0, 0, err
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

	latencyMs = float64(time.Since(startTime).Microseconds()) / 1000.0

	if resp.StatusCode != 200 {
		return latencyMs, 0, fmt.Errorf("status %d: %s", resp.StatusCode, string(respBody[:min(200, len(respBody))]))
	}

	// Parse Loki response
	var result map[string]interface{}
	if err := json.Unmarshal(respBody, &result); err == nil {
		if data, ok := result["data"].(map[string]interface{}); ok {
			if resultArr, ok := data["result"].([]interface{}); ok {
				// For streams, count total log entries
				for _, stream := range resultArr {
					if streamMap, ok := stream.(map[string]interface{}); ok {
						if values, ok := streamMap["values"].([]interface{}); ok {
							rows += int64(len(values))
						}
						// For metric queries
						if value, ok := streamMap["value"].([]interface{}); ok && len(value) >= 2 {
							if countStr, ok := value[1].(string); ok {
								if count, err := strconv.ParseFloat(countStr, 64); err == nil {
									rows += int64(count)
								}
							}
						}
					}
				}
			}
		}
	}

	return latencyMs, rows, nil
}

// Quickwit query executor
func runQuickwitQuery(cfg *Config, query string, client *http.Client) (latencyMs float64, rows int64, err error) {
	queryURL := fmt.Sprintf("http://%s:%d/api/v1/%s/search", cfg.Host, cfg.Port, cfg.Index)

	// Replace relative time placeholders with actual timestamps
	if strings.Contains(query, "{{NOW}}") || strings.Contains(query, "{{1H_AGO}}") {
		now := time.Now().UTC()
		oneHourAgo := now.Add(-1 * time.Hour)
		query = strings.ReplaceAll(query, "{{NOW}}", fmt.Sprintf("%d", now.UnixNano()))
		query = strings.ReplaceAll(query, "{{1H_AGO}}", fmt.Sprintf("%d", oneHourAgo.UnixNano()))
	}

	reqBody := map[string]interface{}{
		"query":    query,
		"max_hits": 10000,
	}
	body, _ := json.Marshal(reqBody)

	start := time.Now()

	req, err := http.NewRequest("POST", queryURL, bytes.NewReader(body))
	if err != nil {
		return 0, 0, err
	}

	req.Header.Set("Content-Type", "application/json")

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

	// Parse Quickwit response
	var result map[string]interface{}
	if err := json.Unmarshal(respBody, &result); err == nil {
		if numHits, ok := result["num_hits"].(float64); ok {
			rows = int64(numHits)
		}
		if hits, ok := result["hits"].([]interface{}); ok && rows == 0 {
			rows = int64(len(hits))
		}
	}

	return latencyMs, rows, nil
}

func runBenchmarkQuery(cfg *Config, q BenchmarkQuery, client *http.Client) (latencyMs float64, rows int64, err error) {
	switch cfg.Target {
	case TargetArc:
		return runArcQuery(cfg, q.ArcSQL, client)
	case TargetArcArrow:
		return runArcArrowQuery(cfg, q.ArcSQL, client)
	case TargetElastic:
		return runElasticQuery(cfg, q.ElasticDSL, client)
	case TargetClickHouse:
		return runClickHouseQuery(cfg, q.ClickHouseSQL, client)
	case TargetVictoriaLogs:
		return runVictoriaLogsQuery(cfg, q.VictoriaLogsQL, client)
	case TargetLoki:
		return runLokiQuery(cfg, q.LokiQL, client)
	case TargetQuickwit:
		return runQuickwitQuery(cfg, q.QuickwitQuery, client)
	default:
		return 0, 0, fmt.Errorf("unknown target: %s", cfg.Target)
	}
}

func main() {
	cfg := Config{}

	flag.StringVar(&cfg.Target, "target", "arc", "Target: arc, arc-arrow, elastic, clickhouse, victorialogs, loki, quickwit")
	flag.StringVar(&cfg.Index, "index", "logs", "Index/table name")
	flag.IntVar(&cfg.Iterations, "iterations", 5, "Number of iterations per query")
	flag.StringVar(&cfg.Host, "host", "localhost", "Server host")
	flag.IntVar(&cfg.Port, "port", 0, "Server port (0 = use default for target)")
	flag.StringVar(&cfg.Database, "database", "logs", "Database name (for Arc)")
	flag.Parse()

	cfg.Token = os.Getenv("ARC_TOKEN")

	// Normalize target name
	cfg.Target = strings.ToLower(cfg.Target)

	// Set default port if not specified
	if cfg.Port == 0 {
		port, ok := defaultPorts[cfg.Target]
		if !ok {
			fmt.Printf("Unknown target: %s\n", cfg.Target)
			fmt.Println("Valid targets: arc, elastic, clickhouse, victorialogs, loki, quickwit")
			os.Exit(1)
		}
		cfg.Port = port
	}

	// Define benchmark queries for logs table
	queries := []BenchmarkQuery{
		{
			Name:          "Count All Logs",
			ArcSQL:        fmt.Sprintf("SELECT count(*) FROM %s", cfg.Index),
			ClickHouseSQL: fmt.Sprintf("SELECT count(*) FROM %s", cfg.Index),
			ElasticDSL: map[string]interface{}{
				"query":            map[string]interface{}{"match_all": map[string]interface{}{}},
				"track_total_hits": true,
				"size":             0,
			},
			VictoriaLogsQL: "* | stats count() as total",
			LokiQL:         `count_over_time({service=~".+"} [24h])`,
			QuickwitQuery:  "*",
		},
		{
			Name:          "Filter by Level (ERROR)",
			ArcSQL:        fmt.Sprintf("SELECT * FROM %s WHERE level = 'ERROR' LIMIT 1000", cfg.Index),
			ClickHouseSQL: fmt.Sprintf("SELECT * FROM %s WHERE level = 'ERROR' LIMIT 1000", cfg.Index),
			ElasticDSL: map[string]interface{}{
				"query": map[string]interface{}{"term": map[string]interface{}{"level.keyword": "ERROR"}},
				"size":  1000,
			},
			VictoriaLogsQL: "level:ERROR | limit 1000",
			LokiQL:         `{level="ERROR"}`,
			QuickwitQuery:  "level:ERROR",
		},
		{
			Name:          "Filter by Service (api)",
			ArcSQL:        fmt.Sprintf("SELECT * FROM %s WHERE service = 'api' LIMIT 1000", cfg.Index),
			ClickHouseSQL: fmt.Sprintf("SELECT * FROM %s WHERE service = 'api' LIMIT 1000", cfg.Index),
			ElasticDSL: map[string]interface{}{
				"query": map[string]interface{}{"term": map[string]interface{}{"service.keyword": "api"}},
				"size":  1000,
			},
			VictoriaLogsQL: "service:api | limit 1000",
			LokiQL:         `{service="api"}`,
			QuickwitQuery:  "service:api",
		},
		{
			Name:          "Full-text Search (timeout)",
			ArcSQL:        fmt.Sprintf("SELECT * FROM %s WHERE message LIKE '%%timeout%%' LIMIT 1000", cfg.Index),
			ClickHouseSQL: fmt.Sprintf("SELECT * FROM %s WHERE message LIKE '%%timeout%%' LIMIT 1000", cfg.Index),
			ElasticDSL: map[string]interface{}{
				"query": map[string]interface{}{"match": map[string]interface{}{"message": "timeout"}},
				"size":  1000,
			},
			VictoriaLogsQL: "timeout | limit 1000",
			LokiQL:         `{service=~".+"} |= "timeout"`,
			QuickwitQuery:  "message:timeout",
		},
		{
			Name:          "Time Range (Last 1 Hour)",
			ArcSQL:        fmt.Sprintf("SELECT * FROM %s WHERE time > NOW() - INTERVAL '1 hour' LIMIT 10000", cfg.Index),
			ClickHouseSQL: fmt.Sprintf("SELECT * FROM %s WHERE timestamp > now() - INTERVAL 1 HOUR LIMIT 10000", cfg.Index),
			ElasticDSL: map[string]interface{}{
				"query": map[string]interface{}{
					"range": map[string]interface{}{
						"@timestamp": map[string]interface{}{"gte": "now-1h"},
					},
				},
				"size": 10000,
			},
			VictoriaLogsQL: "_time:1h | limit 10000",
			LokiQL:         `{service=~".+"}`,
			QuickwitQuery:  "timestamp:[{{1H_AGO}} TO {{NOW}}]",
		},
		{
			Name:          "Top 10 Services by Count",
			ArcSQL:        fmt.Sprintf("SELECT service, count(*) as cnt FROM %s GROUP BY service ORDER BY cnt DESC LIMIT 10", cfg.Index),
			ClickHouseSQL: fmt.Sprintf("SELECT service, count(*) as cnt FROM %s GROUP BY service ORDER BY cnt DESC LIMIT 10", cfg.Index),
			ElasticDSL: map[string]interface{}{
				"size": 0,
				"aggs": map[string]interface{}{
					"services": map[string]interface{}{
						"terms": map[string]interface{}{
							"field": "service.keyword",
							"size":  10,
						},
					},
				},
			},
			VictoriaLogsQL: "* | stats by(service) count() as cnt | sort by(cnt) desc | limit 10",
			LokiQL:         `sum by(service) (count_over_time({service=~".+"} [24h]))`,
			QuickwitQuery:  "*",
		},
		{
			Name:          "Complex Filter (api + ERROR)",
			ArcSQL:        fmt.Sprintf("SELECT * FROM %s WHERE service = 'api' AND level = 'ERROR' LIMIT 1000", cfg.Index),
			ClickHouseSQL: fmt.Sprintf("SELECT * FROM %s WHERE service = 'api' AND level = 'ERROR' LIMIT 1000", cfg.Index),
			ElasticDSL: map[string]interface{}{
				"query": map[string]interface{}{
					"bool": map[string]interface{}{
						"must": []map[string]interface{}{
							{"term": map[string]interface{}{"service.keyword": "api"}},
							{"term": map[string]interface{}{"level.keyword": "ERROR"}},
						},
					},
				},
				"size": 1000,
			},
			VictoriaLogsQL: "service:api AND level:ERROR | limit 1000",
			LokiQL:         `{service="api", level="ERROR"}`,
			QuickwitQuery:  "service:api AND level:ERROR",
		},
		{
			Name:          "SELECT LIMIT 10K",
			ArcSQL:        fmt.Sprintf("SELECT * FROM %s LIMIT 10000", cfg.Index),
			ClickHouseSQL: fmt.Sprintf("SELECT * FROM %s LIMIT 10000", cfg.Index),
			ElasticDSL: map[string]interface{}{
				"query": map[string]interface{}{"match_all": map[string]interface{}{}},
				"size":  10000,
			},
			VictoriaLogsQL: "* | limit 10000",
			LokiQL:         `{service=~".+"}`,
			QuickwitQuery:  "*",
		},
	}

	fmt.Println("================================================================================")
	fmt.Printf("QUERY BENCHMARK SUITE - %s\n", targetLabels[cfg.Target])
	fmt.Println("================================================================================")
	fmt.Printf("Target: http://%s:%d\n", cfg.Host, cfg.Port)
	fmt.Printf("Index: %s\n", cfg.Index)
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

	results := make([]*QueryResult, 0, len(queries))

	for _, q := range queries {
		result := &QueryResult{Name: q.Name}
		fmt.Printf("Query: %s\n", q.Name)

		// Warmup
		_, _, err := runBenchmarkQuery(&cfg, q, client)
		if err != nil {
			fmt.Printf("  Warmup ERROR: %v\n\n", err)
			result.Errors++
			results = append(results, result)
			continue
		}

		// Benchmark runs
		for i := 0; i < cfg.Iterations; i++ {
			latency, rows, err := runBenchmarkQuery(&cfg, q, client)
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
			fmt.Printf("  Latency: %.2f ms (p50: %.2f ms, p99: %.2f ms)\n",
				result.avgLatency(), result.p50Latency(), result.p99Latency())
			fmt.Printf("  Rows: %.0f\n", result.avgRows())
		}
		if result.Errors > 0 {
			fmt.Printf("  Errors: %d/%d\n", result.Errors, cfg.Iterations)
		}
		fmt.Println()

		results = append(results, result)
	}

	// Summary
	fmt.Println("================================================================================")
	fmt.Println("SUMMARY")
	fmt.Println("================================================================================")
	fmt.Printf("%-35s | %12s | %12s | %12s\n", "Query", "Avg (ms)", "p99 (ms)", "Rows")
	fmt.Println("--------------------------------------------------------------------------------")

	for _, r := range results {
		if len(r.Latencies) > 0 {
			fmt.Printf("%-35s | %12.2f | %12.2f | %12.0f\n",
				r.Name[:min(35, len(r.Name))],
				r.avgLatency(),
				r.p99Latency(),
				r.avgRows())
		} else {
			fmt.Printf("%-35s | %12s | %12s | %12s\n",
				r.Name[:min(35, len(r.Name))], "ERROR", "ERROR", "ERROR")
		}
	}

	fmt.Println("================================================================================")
}
