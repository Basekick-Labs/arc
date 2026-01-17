// overhead_bench.go - Measures Arc HTTP/transformation overhead vs raw DuckDB
//
// Usage: go run benchmarks/clickbench/overhead_bench.go
//
// This benchmark compares:
// 1. Direct DuckDB query execution (baseline)
// 2. Arc API response time (includes HTTP, transformation, JSON serialization)

package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
)

const (
	parquetPath = "./data/arc/clickbench/hits/hits.parquet"
	arcEndpoint = "http://localhost:8000/api/v1/query"
	numRuns     = 5
)

// Test queries of varying complexity
var testQueries = []struct {
	name      string
	duckSQL   string // SQL for direct DuckDB (with read_parquet)
	arcSQL    string // SQL for Arc API (table name)
}{
	{
		name:    "Q1: Simple COUNT",
		duckSQL: fmt.Sprintf("SELECT COUNT(*) FROM read_parquet('%s')", parquetPath),
		arcSQL:  "SELECT COUNT(*) FROM hits",
	},
	{
		name:    "Q3: SUM/COUNT/AVG",
		duckSQL: fmt.Sprintf("SELECT SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth) FROM read_parquet('%s')", parquetPath),
		arcSQL:  "SELECT SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth) FROM hits",
	},
	{
		name:    "Q8: COUNT DISTINCT",
		duckSQL: fmt.Sprintf("SELECT COUNT(DISTINCT SearchPhrase) FROM read_parquet('%s')", parquetPath),
		arcSQL:  "SELECT COUNT(DISTINCT SearchPhrase) FROM hits",
	},
	{
		name:    "Q10: Aggregation WHERE",
		duckSQL: fmt.Sprintf("SELECT SUM(ResolutionWidth), SUM(ResolutionHeight), COUNT(*), AVG(ResolutionWidth), AVG(ResolutionHeight) FROM read_parquet('%s') WHERE ResolutionWidth > 100", parquetPath),
		arcSQL:  "SELECT SUM(ResolutionWidth), SUM(ResolutionHeight), COUNT(*), AVG(ResolutionWidth), AVG(ResolutionHeight) FROM hits WHERE ResolutionWidth > 100",
	},
	{
		name:    "GROUP BY 10 rows",
		duckSQL: fmt.Sprintf("SELECT UserID, COUNT(*) FROM read_parquet('%s') GROUP BY UserID ORDER BY COUNT(*) DESC LIMIT 10", parquetPath),
		arcSQL:  "SELECT UserID, COUNT(*) FROM hits GROUP BY UserID ORDER BY COUNT(*) DESC LIMIT 10",
	},
	{
		name:    "GROUP BY 100 rows",
		duckSQL: fmt.Sprintf("SELECT UserID, COUNT(*) FROM read_parquet('%s') GROUP BY UserID ORDER BY COUNT(*) DESC LIMIT 100", parquetPath),
		arcSQL:  "SELECT UserID, COUNT(*) FROM hits GROUP BY UserID ORDER BY COUNT(*) DESC LIMIT 100",
	},
	{
		name:    "GROUP BY 1000 rows",
		duckSQL: fmt.Sprintf("SELECT UserID, COUNT(*) FROM read_parquet('%s') GROUP BY UserID ORDER BY COUNT(*) DESC LIMIT 1000", parquetPath),
		arcSQL:  "SELECT UserID, COUNT(*) FROM hits GROUP BY UserID ORDER BY COUNT(*) DESC LIMIT 1000",
	},
	{
		name:    "SELECT * LIMIT 100",
		duckSQL: fmt.Sprintf("SELECT * FROM read_parquet('%s') LIMIT 100", parquetPath),
		arcSQL:  "SELECT * FROM hits LIMIT 100",
	},
	{
		name:    "SELECT * LIMIT 1000",
		duckSQL: fmt.Sprintf("SELECT * FROM read_parquet('%s') LIMIT 1000", parquetPath),
		arcSQL:  "SELECT * FROM hits LIMIT 1000",
	},
}

func main() {
	fmt.Println("Arc Overhead Benchmark")
	fmt.Println("======================")
	fmt.Println()

	// Check if parquet file exists
	if _, err := os.Stat(parquetPath); os.IsNotExist(err) {
		fmt.Printf("Error: Parquet file not found at %s\n", parquetPath)
		fmt.Println("Please ensure the ClickBench data is loaded.")
		os.Exit(1)
	}

	// Initialize direct DuckDB connection
	db, err := sql.Open("duckdb", "")
	if err != nil {
		fmt.Printf("Error opening DuckDB: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	// Enable object cache for fair comparison
	db.Exec("SET enable_object_cache=true")

	// Warmup DuckDB
	fmt.Println("Warming up DuckDB...")
	for _, q := range testQueries {
		rows, err := db.Query(q.duckSQL)
		if err != nil {
			fmt.Printf("Warmup error for %s: %v\n", q.name, err)
			continue
		}
		rows.Close()
	}

	// Check if Arc is running
	fmt.Println("Checking Arc connectivity...")
	arcAvailable := checkArcConnection()
	if !arcAvailable {
		fmt.Println("Warning: Arc API not available. Skipping Arc benchmarks.")
		fmt.Println("Start Arc with: go run cmd/arc/main.go")
		fmt.Println()
	} else {
		// Warmup Arc
		fmt.Println("Warming up Arc API...")
		for _, q := range testQueries {
			queryArc(q.arcSQL)
		}
	}

	fmt.Println()
	fmt.Println("Running benchmarks...")
	fmt.Println()
	fmt.Printf("%-25s | %12s | %12s | %10s | %10s\n", "Query", "DuckDB (ms)", "Arc (ms)", "Overhead", "% Overhead")
	fmt.Println(repeatString("-", 85))

	for _, q := range testQueries {
		// Benchmark direct DuckDB
		duckTimes := make([]float64, numRuns)
		for i := 0; i < numRuns; i++ {
			start := time.Now()
			rows, err := db.Query(q.duckSQL)
			if err != nil {
				fmt.Printf("DuckDB error: %v\n", err)
				continue
			}
			// Drain results
			for rows.Next() {
			}
			rows.Close()
			duckTimes[i] = float64(time.Since(start).Microseconds()) / 1000.0
		}
		duckAvg := average(duckTimes)

		// Benchmark Arc API
		var arcAvg float64
		if arcAvailable {
			arcTimes := make([]float64, numRuns)
			for i := 0; i < numRuns; i++ {
				start := time.Now()
				resp, err := queryArc(q.arcSQL)
				if err != nil {
					fmt.Printf("Arc error: %v\n", err)
					continue
				}
				arcTimes[i] = float64(time.Since(start).Microseconds()) / 1000.0
				_ = resp
			}
			arcAvg = average(arcTimes)
		}

		// Calculate overhead
		overhead := arcAvg - duckAvg
		pctOverhead := 0.0
		if duckAvg > 0 {
			pctOverhead = (overhead / duckAvg) * 100
		}

		if arcAvailable {
			fmt.Printf("%-25s | %12.1f | %12.1f | %+9.1f | %+9.1f%%\n",
				q.name, duckAvg, arcAvg, overhead, pctOverhead)
		} else {
			fmt.Printf("%-25s | %12.1f | %12s | %10s | %10s\n",
				q.name, duckAvg, "N/A", "N/A", "N/A")
		}
	}

	fmt.Println()
	fmt.Println("Notes:")
	fmt.Println("- DuckDB times are raw query execution (no HTTP, no JSON)")
	fmt.Println("- Arc times include: HTTP parsing, SQL transformation, DuckDB query, JSON serialization, HTTP response")
	fmt.Println("- Overhead = Arc time - DuckDB time")
	fmt.Println("- All times are averages of", numRuns, "runs (after warmup)")
}

func checkArcConnection() bool {
	client := &http.Client{Timeout: 2 * time.Second}
	_, err := client.Get("http://localhost:8000/healthz")
	return err == nil
}

func queryArc(sql string) (map[string]interface{}, error) {
	body := map[string]string{"sql": sql}
	jsonBody, _ := json.Marshal(body)

	req, err := http.NewRequest("POST", arcEndpoint, bytes.NewBuffer(jsonBody))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-arc-database", "clickbench")

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var result map[string]interface{}
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, err
	}

	if success, ok := result["success"].(bool); !ok || !success {
		if errMsg, ok := result["error"].(string); ok {
			return nil, fmt.Errorf("query error: %s", errMsg)
		}
		return nil, fmt.Errorf("query failed")
	}

	return result, nil
}

func average(times []float64) float64 {
	if len(times) == 0 {
		return 0
	}
	var sum float64
	for _, t := range times {
		sum += t
	}
	return sum / float64(len(times))
}

func repeatString(s string, count int) string {
	result := ""
	for i := 0; i < count; i++ {
		result += s
	}
	return result
}
