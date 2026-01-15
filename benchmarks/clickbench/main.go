// ClickBench benchmark for Arc
// Tests standard ClickBench queries against the hits table
// Usage: go run benchmarks/clickbench/main.go [flags]

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
)

type Config struct {
	Host       string
	Port       int
	Database   string
	Iterations int
	Token      string
	OutputCSV  bool
}

type QueryRequest struct {
	SQL string `json:"sql"`
}

type BenchmarkQuery struct {
	ID   int
	Name string
	SQL  string
}

type QueryResult struct {
	ID         int
	Name       string
	Latencies  []float64
	Errors     int
	ErrorMsg   string
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

func (r *QueryResult) minLatency() float64 {
	if len(r.Latencies) == 0 {
		return 0
	}
	min := r.Latencies[0]
	for _, v := range r.Latencies[1:] {
		if v < min {
			min = v
		}
	}
	return min
}

func (r *QueryResult) maxLatency() float64 {
	if len(r.Latencies) == 0 {
		return 0
	}
	max := r.Latencies[0]
	for _, v := range r.Latencies[1:] {
		if v > max {
			max = v
		}
	}
	return max
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

func runQuery(cfg *Config, sql string, client *http.Client) (latencyMs float64, err error) {
	url := fmt.Sprintf("http://%s:%d/api/v1/query", cfg.Host, cfg.Port)

	reqBody := QueryRequest{SQL: sql}
	body, _ := json.Marshal(reqBody)

	start := time.Now()

	req, err := http.NewRequest("POST", url, bytes.NewReader(body))
	if err != nil {
		return 0, err
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-arc-database", cfg.Database)
	if cfg.Token != "" {
		req.Header.Set("Authorization", "Bearer "+cfg.Token)
	}

	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, err
	}

	latencyMs = float64(time.Since(start).Milliseconds())

	if resp.StatusCode != 200 {
		return latencyMs, fmt.Errorf("status %d: %s", resp.StatusCode, string(respBody[:min(300, len(respBody))]))
	}

	return latencyMs, nil
}

func main() {
	cfg := Config{}

	flag.StringVar(&cfg.Host, "host", "localhost", "Server host")
	flag.IntVar(&cfg.Port, "port", 8000, "Server port")
	flag.StringVar(&cfg.Database, "database", "clickbench", "Database name")
	flag.IntVar(&cfg.Iterations, "iterations", 3, "Number of iterations per query")
	flag.BoolVar(&cfg.OutputCSV, "csv", false, "Output results as CSV")
	flag.Parse()

	cfg.Token = os.Getenv("ARC_TOKEN")

	// ClickBench queries
	queries := []BenchmarkQuery{
		{1, "Q1: COUNT(*)", "SELECT COUNT(*) FROM hits"},
		{2, "Q2: COUNT(*) WHERE", "SELECT COUNT(*) FROM hits WHERE AdvEngineID <> 0"},
		{3, "Q3: SUM/COUNT/AVG", "SELECT SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth) FROM hits"},
		{4, "Q4: AVG(UserID)", "SELECT AVG(UserID) FROM hits"},
		{5, "Q5: COUNT DISTINCT UserID", "SELECT COUNT(DISTINCT UserID) FROM hits"},
		{6, "Q6: COUNT DISTINCT SearchPhrase", "SELECT COUNT(DISTINCT SearchPhrase) FROM hits"},
		{7, "Q7: MIN/MAX EventDate", "SELECT MIN(EventDate), MAX(EventDate) FROM hits"},
		{8, "Q8: GROUP BY AdvEngineID", "SELECT AdvEngineID, COUNT(*) FROM hits WHERE AdvEngineID <> 0 GROUP BY AdvEngineID ORDER BY COUNT(*) DESC"},
		{9, "Q9: GROUP BY RegionID DISTINCT", "SELECT RegionID, COUNT(DISTINCT UserID) AS u FROM hits GROUP BY RegionID ORDER BY u DESC LIMIT 10"},
		{10, "Q10: GROUP BY RegionID multi-agg", "SELECT RegionID, SUM(AdvEngineID), COUNT(*) AS c, AVG(ResolutionWidth), COUNT(DISTINCT UserID) FROM hits GROUP BY RegionID ORDER BY c DESC LIMIT 10"},
		{11, "Q11: MobilePhoneModel DISTINCT", "SELECT MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits WHERE MobilePhoneModel <> '' GROUP BY MobilePhoneModel ORDER BY u DESC LIMIT 10"},
		{12, "Q12: MobilePhone+Model GROUP BY", "SELECT MobilePhone, MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM hits WHERE MobilePhoneModel <> '' GROUP BY MobilePhone, MobilePhoneModel ORDER BY u DESC LIMIT 10"},
		{13, "Q13: SearchPhrase COUNT", "SELECT SearchPhrase, COUNT(*) AS c FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10"},
		{14, "Q14: SearchPhrase DISTINCT", "SELECT SearchPhrase, COUNT(DISTINCT UserID) AS u FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY u DESC LIMIT 10"},
		{15, "Q15: SearchEngine+Phrase", "SELECT SearchEngineID, SearchPhrase, COUNT(*) AS c FROM hits WHERE SearchPhrase <> '' GROUP BY SearchEngineID, SearchPhrase ORDER BY c DESC LIMIT 10"},
		{16, "Q16: UserID GROUP BY", "SELECT UserID, COUNT(*) FROM hits GROUP BY UserID ORDER BY COUNT(*) DESC LIMIT 10"},
		{17, "Q17: UserID+SearchPhrase ORDER", "SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, SearchPhrase ORDER BY COUNT(*) DESC LIMIT 10"},
		{18, "Q18: UserID+SearchPhrase", "SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, SearchPhrase LIMIT 10"},
		{19, "Q19: UserID+minute+Phrase", "SELECT UserID, (EventTime // 60) % 60 AS m, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, m, SearchPhrase ORDER BY COUNT(*) DESC LIMIT 10"},
		{20, "Q20: UserID point lookup", "SELECT UserID FROM hits WHERE UserID = 435090932899640449"},
		{21, "Q21: URL LIKE google", "SELECT COUNT(*) FROM hits WHERE URL LIKE '%google%'"},
		{22, "Q22: SearchPhrase+URL LIKE", "SELECT SearchPhrase, MIN(URL), COUNT(*) AS c FROM hits WHERE URL LIKE '%google%' AND SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10"},
		{23, "Q23: Title LIKE Google", "SELECT SearchPhrase, MIN(URL), MIN(Title), COUNT(*) AS c, COUNT(DISTINCT UserID) FROM hits WHERE Title LIKE '%Google%' AND URL NOT LIKE '%.google.%' AND SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10"},
		{24, "Q24: SELECT * LIKE ORDER", "SELECT * FROM hits WHERE URL LIKE '%google%' ORDER BY EventTime LIMIT 10"},
		{25, "Q25: SearchPhrase ORDER EventTime", "SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY EventTime LIMIT 10"},
		{26, "Q26: SearchPhrase ORDER self", "SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY SearchPhrase LIMIT 10"},
		{27, "Q27: SearchPhrase ORDER both", "SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' ORDER BY EventTime, SearchPhrase LIMIT 10"},
		{28, "Q28: AVG(STRLEN) HAVING", "SELECT CounterID, AVG(STRLEN(URL)) AS l, COUNT(*) AS c FROM hits WHERE URL <> '' GROUP BY CounterID HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25"},
		{29, "Q29: REGEXP_REPLACE", "SELECT REGEXP_REPLACE(Referer, '^https?://(?:www\\.)?([^/]+)/.*$', '\\1') AS k, AVG(STRLEN(Referer)) AS l, COUNT(*) AS c, MIN(Referer) FROM hits WHERE Referer <> '' GROUP BY k HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25"},
		{30, "Q30: Wide aggregation (90 SUMs)", "SELECT SUM(ResolutionWidth), SUM(ResolutionWidth + 1), SUM(ResolutionWidth + 2), SUM(ResolutionWidth + 3), SUM(ResolutionWidth + 4), SUM(ResolutionWidth + 5), SUM(ResolutionWidth + 6), SUM(ResolutionWidth + 7), SUM(ResolutionWidth + 8), SUM(ResolutionWidth + 9), SUM(ResolutionWidth + 10), SUM(ResolutionWidth + 11), SUM(ResolutionWidth + 12), SUM(ResolutionWidth + 13), SUM(ResolutionWidth + 14), SUM(ResolutionWidth + 15), SUM(ResolutionWidth + 16), SUM(ResolutionWidth + 17), SUM(ResolutionWidth + 18), SUM(ResolutionWidth + 19), SUM(ResolutionWidth + 20), SUM(ResolutionWidth + 21), SUM(ResolutionWidth + 22), SUM(ResolutionWidth + 23), SUM(ResolutionWidth + 24), SUM(ResolutionWidth + 25), SUM(ResolutionWidth + 26), SUM(ResolutionWidth + 27), SUM(ResolutionWidth + 28), SUM(ResolutionWidth + 29), SUM(ResolutionWidth + 30), SUM(ResolutionWidth + 31), SUM(ResolutionWidth + 32), SUM(ResolutionWidth + 33), SUM(ResolutionWidth + 34), SUM(ResolutionWidth + 35), SUM(ResolutionWidth + 36), SUM(ResolutionWidth + 37), SUM(ResolutionWidth + 38), SUM(ResolutionWidth + 39), SUM(ResolutionWidth + 40), SUM(ResolutionWidth + 41), SUM(ResolutionWidth + 42), SUM(ResolutionWidth + 43), SUM(ResolutionWidth + 44), SUM(ResolutionWidth + 45), SUM(ResolutionWidth + 46), SUM(ResolutionWidth + 47), SUM(ResolutionWidth + 48), SUM(ResolutionWidth + 49), SUM(ResolutionWidth + 50), SUM(ResolutionWidth + 51), SUM(ResolutionWidth + 52), SUM(ResolutionWidth + 53), SUM(ResolutionWidth + 54), SUM(ResolutionWidth + 55), SUM(ResolutionWidth + 56), SUM(ResolutionWidth + 57), SUM(ResolutionWidth + 58), SUM(ResolutionWidth + 59), SUM(ResolutionWidth + 60), SUM(ResolutionWidth + 61), SUM(ResolutionWidth + 62), SUM(ResolutionWidth + 63), SUM(ResolutionWidth + 64), SUM(ResolutionWidth + 65), SUM(ResolutionWidth + 66), SUM(ResolutionWidth + 67), SUM(ResolutionWidth + 68), SUM(ResolutionWidth + 69), SUM(ResolutionWidth + 70), SUM(ResolutionWidth + 71), SUM(ResolutionWidth + 72), SUM(ResolutionWidth + 73), SUM(ResolutionWidth + 74), SUM(ResolutionWidth + 75), SUM(ResolutionWidth + 76), SUM(ResolutionWidth + 77), SUM(ResolutionWidth + 78), SUM(ResolutionWidth + 79), SUM(ResolutionWidth + 80), SUM(ResolutionWidth + 81), SUM(ResolutionWidth + 82), SUM(ResolutionWidth + 83), SUM(ResolutionWidth + 84), SUM(ResolutionWidth + 85), SUM(ResolutionWidth + 86), SUM(ResolutionWidth + 87), SUM(ResolutionWidth + 88), SUM(ResolutionWidth + 89) FROM hits"},
		{31, "Q31: SearchEngine+ClientIP", "SELECT SearchEngineID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits WHERE SearchPhrase <> '' GROUP BY SearchEngineID, ClientIP ORDER BY c DESC LIMIT 10"},
		{32, "Q32: WatchID+ClientIP WHERE", "SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits WHERE SearchPhrase <> '' GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10"},
		{33, "Q33: WatchID+ClientIP all", "SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10"},
		{34, "Q34: URL GROUP BY", "SELECT URL, COUNT(*) AS c FROM hits GROUP BY URL ORDER BY c DESC LIMIT 10"},
		{35, "Q35: Constant+URL GROUP", "SELECT 1, URL, COUNT(*) AS c FROM hits GROUP BY 1, URL ORDER BY c DESC LIMIT 10"},
		{36, "Q36: ClientIP arithmetic", "SELECT ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3, COUNT(*) AS c FROM hits GROUP BY ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3 ORDER BY c DESC LIMIT 10"},
		{37, "Q37: URL filtered complex", "SELECT URL, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= 15888 AND EventDate <= 15917 AND DontCountHits = 0 AND IsRefresh = 0 AND URL <> '' GROUP BY URL ORDER BY PageViews DESC LIMIT 10"},
		{38, "Q38: Title filtered complex", "SELECT Title, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= 15888 AND EventDate <= 15917 AND DontCountHits = 0 AND IsRefresh = 0 AND Title <> '' GROUP BY Title ORDER BY PageViews DESC LIMIT 10"},
		{39, "Q39: URL OFFSET 1000", "SELECT URL, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= 15888 AND EventDate <= 15917 AND IsRefresh = 0 AND IsLink <> 0 AND IsDownload = 0 GROUP BY URL ORDER BY PageViews DESC LIMIT 10 OFFSET 1000"},
		{40, "Q40: Traffic source CASE", "SELECT TraficSourceID, SearchEngineID, AdvEngineID, CASE WHEN (SearchEngineID = 0 AND AdvEngineID = 0) THEN Referer ELSE '' END AS Src, URL AS Dst, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= 15888 AND EventDate <= 15917 AND IsRefresh = 0 GROUP BY TraficSourceID, SearchEngineID, AdvEngineID, Src, Dst ORDER BY PageViews DESC LIMIT 10 OFFSET 1000"},
		{41, "Q41: URLHash+EventDate", "SELECT URLHash, EventDate, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= 15888 AND EventDate <= 15917 AND IsRefresh = 0 AND TraficSourceID IN (-1, 6) AND RefererHash = 3594120000172545465 GROUP BY URLHash, EventDate ORDER BY PageViews DESC LIMIT 10 OFFSET 100"},
		{42, "Q42: Window dimensions", "SELECT WindowClientWidth, WindowClientHeight, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= 15888 AND EventDate <= 15917 AND IsRefresh = 0 AND DontCountHits = 0 AND URLHash = 2868770270353813622 GROUP BY WindowClientWidth, WindowClientHeight ORDER BY PageViews DESC LIMIT 10 OFFSET 10000"},
		{43, "Q43: Time bucket", "SELECT (EventTime // 60) * 60 AS M, COUNT(*) AS PageViews FROM hits WHERE CounterID = 62 AND EventDate >= 15888 AND EventDate <= 15917 AND IsRefresh = 0 AND DontCountHits = 0 GROUP BY (EventTime // 60) * 60 ORDER BY (EventTime // 60) * 60 LIMIT 10 OFFSET 1000"},
	}

	if !cfg.OutputCSV {
		fmt.Println("================================================================================")
		fmt.Println("CLICKBENCH BENCHMARK - Arc")
		fmt.Println("================================================================================")
		fmt.Printf("Target: http://%s:%d\n", cfg.Host, cfg.Port)
		fmt.Printf("Database: %s\n", cfg.Database)
		fmt.Printf("Iterations: %d per query (+ 1 warmup)\n", cfg.Iterations)
		fmt.Printf("Total queries: %d\n", len(queries))
		fmt.Println("================================================================================")
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
		result := &QueryResult{ID: q.ID, Name: q.Name}

		if !cfg.OutputCSV {
			fmt.Printf("[Q%02d] %s\n", q.ID, q.Name)
		}

		// Warmup
		_, err := runQuery(&cfg, q.SQL, client)
		if err != nil {
			if !cfg.OutputCSV {
				fmt.Printf("      WARMUP ERROR: %v\n", err)
			}
			result.Errors++
			result.ErrorMsg = err.Error()
			results = append(results, result)
			continue
		}

		// Benchmark runs
		for i := 0; i < cfg.Iterations; i++ {
			latency, err := runQuery(&cfg, q.SQL, client)
			if err != nil {
				result.Errors++
				result.ErrorMsg = err.Error()
				continue
			}
			result.Latencies = append(result.Latencies, latency)
		}

		if !cfg.OutputCSV && len(result.Latencies) > 0 {
			fmt.Printf("      Min: %8.0f ms | Avg: %8.0f ms | Max: %8.0f ms\n",
				result.minLatency(), result.avgLatency(), result.maxLatency())
		}

		results = append(results, result)
	}

	// Output results
	if cfg.OutputCSV {
		fmt.Println("query_id,query_name,min_ms,avg_ms,max_ms,p50_ms,errors")
		for _, r := range results {
			name := strings.ReplaceAll(r.Name, ",", ";")
			fmt.Printf("%d,%s,%.0f,%.0f,%.0f,%.0f,%d\n",
				r.ID, name, r.minLatency(), r.avgLatency(), r.maxLatency(),
				percentile(r.Latencies, 0.50), r.Errors)
		}
	} else {
		// Summary
		fmt.Println()
		fmt.Println("================================================================================")
		fmt.Println("SUMMARY")
		fmt.Println("================================================================================")
		fmt.Printf("%-5s | %-30s | %10s | %10s | %10s\n", "ID", "Query", "Min (ms)", "Avg (ms)", "Max (ms)")
		fmt.Println("--------------------------------------------------------------------------------")

		var totalAvg float64
		var successCount int

		for _, r := range results {
			if len(r.Latencies) > 0 {
				name := r.Name
				if len(name) > 30 {
					name = name[:27] + "..."
				}
				fmt.Printf("Q%-4d | %-30s | %10.0f | %10.0f | %10.0f\n",
					r.ID, name, r.minLatency(), r.avgLatency(), r.maxLatency())
				totalAvg += r.avgLatency()
				successCount++
			} else {
				fmt.Printf("Q%-4d | %-30s | %10s | %10s | %10s\n",
					r.ID, r.Name[:min(30, len(r.Name))], "ERROR", "ERROR", "ERROR")
			}
		}

		fmt.Println("--------------------------------------------------------------------------------")
		if successCount > 0 {
			fmt.Printf("Total time (sum of averages): %.0f ms\n", totalAvg)
			fmt.Printf("Average query time: %.0f ms\n", totalAvg/float64(successCount))
		}
		fmt.Printf("Successful queries: %d/%d\n", successCount, len(queries))
		fmt.Println("================================================================================")
	}
}
