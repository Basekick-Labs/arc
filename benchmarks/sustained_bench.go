// Sustained load benchmark for Arc ingestion
// Usage: go run benchmarks/sustained_bench.go [flags]
//
// Examples:
//   go run benchmarks/sustained_bench.go --duration 30
//   go run benchmarks/sustained_bench.go --duration 60 --workers 200 --compress zstd
//   go run benchmarks/sustained_bench.go --batch-size 5000 --compress gzip
//   go run benchmarks/sustained_bench.go --protocol lineprotocol --workers 20

package main

import (
	"bytes"
	"compress/gzip"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/vmihailenco/msgpack/v5"
)

type Config struct {
	Duration    int
	Workers     int
	BatchSize   int
	Pregenerate int
	Compress    string
	ZstdLevel   int
	DataType    string
	Protocol    string // "msgpack" or "lineprotocol"
	Host        string
	Port        int
	Token       string
}

type Stats struct {
	totalSent   atomic.Int64
	totalErrors atomic.Int64
	latencies   []float64
	latencyMu   sync.Mutex
	running     atomic.Bool
}

func (s *Stats) addLatency(ms float64) {
	s.latencyMu.Lock()
	s.latencies = append(s.latencies, ms)
	s.latencyMu.Unlock()
}

func (s *Stats) getPercentile(p float64) float64 {
	s.latencyMu.Lock()
	defer s.latencyMu.Unlock()

	if len(s.latencies) == 0 {
		return 0
	}

	sorted := make([]float64, len(s.latencies))
	copy(sorted, s.latencies)
	sort.Float64s(sorted)

	idx := int(float64(len(sorted)) * p)
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func generateIOTBatches(count, batchSize int, compress string, zstdLevel int) [][]byte {
	measurements := []string{"cpu", "mem", "disk", "net"}
	hosts := make([]string, 1000)
	for i := range hosts {
		hosts[i] = fmt.Sprintf("server%03d", i)
	}

	batches := make([][]byte, count)
	var zstdEncoder *zstd.Encoder
	if compress == "zstd" {
		var err error
		zstdEncoder, err = zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(zstdLevel)))
		if err != nil {
			panic(err)
		}
	}

	for i := 0; i < count; i++ {
		nowMicros := time.Now().UnixMicro()

		times := make([]int64, batchSize)
		hostVals := make([]string, batchSize)
		values := make([]float64, batchSize)
		cpuIdle := make([]float64, batchSize)
		cpuUser := make([]float64, batchSize)

		for j := 0; j < batchSize; j++ {
			times[j] = nowMicros + int64(j)
			hostVals[j] = hosts[rand.Intn(len(hosts))]
			values[j] = rand.Float64() * 100
			cpuIdle[j] = rand.Float64() * 100
			cpuUser[j] = rand.Float64() * 100
		}

		payload := map[string]interface{}{
			"m": measurements[rand.Intn(len(measurements))],
			"columns": map[string]interface{}{
				"time":     times,
				"host":     hostVals,
				"value":    values,
				"cpu_idle": cpuIdle,
				"cpu_user": cpuUser,
			},
		}

		data, err := msgpack.Marshal(payload)
		if err != nil {
			panic(err)
		}

		switch compress {
		case "gzip":
			var buf bytes.Buffer
			w, _ := gzip.NewWriterLevel(&buf, gzip.BestSpeed)
			w.Write(data)
			w.Close()
			data = buf.Bytes()
		case "zstd":
			data = zstdEncoder.EncodeAll(data, nil)
		}

		batches[i] = data

		if (i+1)%100 == 0 {
			fmt.Printf("  Progress: %d/%d\n", i+1, count)
		}
	}

	return batches
}

func generateLineProtocolBatches(count, batchSize int, compress string, zstdLevel int, dataType string) [][]byte {
	batches := make([][]byte, count)
	var zstdEncoder *zstd.Encoder
	if compress == "zstd" {
		var err error
		zstdEncoder, err = zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(zstdLevel)))
		if err != nil {
			panic(err)
		}
	}

	// IOT hosts or financial symbols
	var tags []string
	var measurement string
	if dataType == "financial" {
		measurement = "trades"
		tags = []string{
			"AAPL", "GOOGL", "MSFT", "AMZN", "META", "NVDA", "TSLA", "JPM", "V", "JNJ",
			"WMT", "PG", "UNH", "HD", "MA", "DIS", "PYPL", "BAC", "ADBE", "NFLX",
		}
	} else {
		measurement = "cpu"
		tags = make([]string, 1000)
		for i := range tags {
			tags[i] = fmt.Sprintf("server%03d", i)
		}
	}

	for i := 0; i < count; i++ {
		nowMicros := time.Now().UnixMicro()
		var buf bytes.Buffer

		for j := 0; j < batchSize; j++ {
			ts := nowMicros + int64(j)
			tag := tags[rand.Intn(len(tags))]

			if dataType == "financial" {
				exchanges := []string{"NYSE", "NASDAQ", "ARCA", "BATS", "IEX"}
				exchange := exchanges[rand.Intn(len(exchanges))]
				basePrice := 10 + rand.Float64()*490
				bid := basePrice - rand.Float64()*0.04 - 0.01
				ask := basePrice + rand.Float64()*0.04 + 0.01
				bidSize := 100 + rand.Intn(9900)
				askSize := 100 + rand.Intn(9900)
				volume := 1 + rand.Intn(999)
				tradeID := 1000000 + rand.Intn(8999999)

				fmt.Fprintf(&buf, "%s,symbol=%s,exchange=%s price=%.4f,bid=%.4f,ask=%.4f,bid_size=%di,ask_size=%di,volume=%di,trade_id=%di %d\n",
					measurement, tag, exchange, basePrice, bid, ask, bidSize, askSize, volume, tradeID, ts*1000)
			} else {
				value := rand.Float64() * 100
				cpuIdle := rand.Float64() * 100
				cpuUser := rand.Float64() * 100

				fmt.Fprintf(&buf, "%s,host=%s value=%.4f,cpu_idle=%.4f,cpu_user=%.4f %d\n",
					measurement, tag, value, cpuIdle, cpuUser, ts*1000)
			}
		}

		data := buf.Bytes()

		switch compress {
		case "gzip":
			var compBuf bytes.Buffer
			w, _ := gzip.NewWriterLevel(&compBuf, gzip.BestSpeed)
			w.Write(data)
			w.Close()
			data = compBuf.Bytes()
		case "zstd":
			data = zstdEncoder.EncodeAll(data, nil)
		}

		batches[i] = data

		if (i+1)%100 == 0 {
			fmt.Printf("  Progress: %d/%d\n", i+1, count)
		}
	}

	return batches
}

func generateFinancialBatches(count, batchSize int, compress string, zstdLevel int) [][]byte {
	symbols := []string{
		"AAPL", "GOOGL", "MSFT", "AMZN", "META", "NVDA", "TSLA", "JPM", "V", "JNJ",
		"WMT", "PG", "UNH", "HD", "MA", "DIS", "PYPL", "BAC", "ADBE", "NFLX",
	}
	exchanges := []string{"NYSE", "NASDAQ", "ARCA", "BATS", "IEX"}

	batches := make([][]byte, count)
	var zstdEncoder *zstd.Encoder
	if compress == "zstd" {
		var err error
		zstdEncoder, err = zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(zstdLevel)))
		if err != nil {
			panic(err)
		}
	}

	for i := 0; i < count; i++ {
		nowMicros := time.Now().UnixMicro()

		times := make([]int64, batchSize)
		symbolVals := make([]string, batchSize)
		exchangeVals := make([]string, batchSize)
		prices := make([]float64, batchSize)
		bids := make([]float64, batchSize)
		asks := make([]float64, batchSize)
		bidSizes := make([]int, batchSize)
		askSizes := make([]int, batchSize)
		volumes := make([]int, batchSize)
		tradeIDs := make([]int, batchSize)

		for j := 0; j < batchSize; j++ {
			times[j] = nowMicros + int64(j)
			symbolVals[j] = symbols[rand.Intn(len(symbols))]
			exchangeVals[j] = exchanges[rand.Intn(len(exchanges))]
			basePrice := 10 + rand.Float64()*490
			prices[j] = basePrice
			bids[j] = basePrice - rand.Float64()*0.04 - 0.01
			asks[j] = basePrice + rand.Float64()*0.04 + 0.01
			bidSizes[j] = 100 + rand.Intn(9900)
			askSizes[j] = 100 + rand.Intn(9900)
			volumes[j] = 1 + rand.Intn(999)
			tradeIDs[j] = 1000000 + rand.Intn(8999999)
		}

		payload := map[string]interface{}{
			"m": "trades",
			"columns": map[string]interface{}{
				"time":     times,
				"symbol":   symbolVals,
				"exchange": exchangeVals,
				"price":    prices,
				"bid":      bids,
				"ask":      asks,
				"bid_size": bidSizes,
				"ask_size": askSizes,
				"volume":   volumes,
				"trade_id": tradeIDs,
			},
		}

		data, err := msgpack.Marshal(payload)
		if err != nil {
			panic(err)
		}

		switch compress {
		case "gzip":
			var buf bytes.Buffer
			w, _ := gzip.NewWriterLevel(&buf, gzip.BestSpeed)
			w.Write(data)
			w.Close()
			data = buf.Bytes()
		case "zstd":
			data = zstdEncoder.EncodeAll(data, nil)
		}

		batches[i] = data

		if (i+1)%100 == 0 {
			fmt.Printf("  Progress: %d/%d\n", i+1, count)
		}
	}

	return batches
}

func worker(id int, cfg *Config, batches [][]byte, stats *Stats, client *http.Client) {
	var url string
	var contentType string

	if cfg.Protocol == "lineprotocol" {
		url = fmt.Sprintf("http://%s:%d/api/v1/write", cfg.Host, cfg.Port)
		contentType = "text/plain"
	} else {
		url = fmt.Sprintf("http://%s:%d/api/v1/write/msgpack", cfg.Host, cfg.Port)
		contentType = "application/msgpack"
	}

	batchIdx := 0

	headers := map[string]string{
		"Content-Type":   contentType,
		"x-arc-database": "production",
	}
	if cfg.Token != "" {
		headers["Authorization"] = "Bearer " + cfg.Token
	}
	if cfg.Compress == "gzip" {
		headers["Content-Encoding"] = "gzip"
	} else if cfg.Compress == "zstd" {
		headers["Content-Encoding"] = "zstd"
	}

	for stats.running.Load() {
		batch := batches[batchIdx%len(batches)]
		batchIdx++

		start := time.Now()

		req, err := http.NewRequest("POST", url, bytes.NewReader(batch))
		if err != nil {
			stats.totalErrors.Add(1)
			continue
		}

		for k, v := range headers {
			req.Header.Set(k, v)
		}

		resp, err := client.Do(req)
		if err != nil {
			stats.totalErrors.Add(1)
			if stats.totalErrors.Load() <= 3 {
				fmt.Printf("Error: %v\n", err)
			}
			continue
		}

		latencyMs := float64(time.Since(start).Microseconds()) / 1000.0

		if resp.StatusCode == 204 {
			stats.totalSent.Add(int64(cfg.BatchSize))
			stats.addLatency(latencyMs)
		} else {
			stats.totalErrors.Add(1)
			if stats.totalErrors.Load() <= 3 {
				body, _ := io.ReadAll(resp.Body)
				fmt.Printf("Error %d: %s\n", resp.StatusCode, string(body)[:min(100, len(body))])
			}
		}
		resp.Body.Close()
	}
}

func main() {
	cfg := Config{}

	flag.IntVar(&cfg.Duration, "duration", 60, "Test duration in seconds")
	flag.IntVar(&cfg.Workers, "workers", 100, "Number of concurrent workers")
	flag.IntVar(&cfg.BatchSize, "batch-size", 1000, "Records per batch")
	flag.IntVar(&cfg.Pregenerate, "pregenerate", 1000, "Number of batches to pre-generate")
	flag.StringVar(&cfg.Compress, "compress", "none", "Compression: none, gzip, zstd")
	flag.IntVar(&cfg.ZstdLevel, "zstd-level", 3, "Zstd compression level (1-22)")
	flag.StringVar(&cfg.DataType, "data-type", "iot", "Data type: iot, financial")
	flag.StringVar(&cfg.Protocol, "protocol", "msgpack", "Protocol: msgpack, lineprotocol")
	flag.StringVar(&cfg.Host, "host", "localhost", "Server host")
	flag.IntVar(&cfg.Port, "port", 8000, "Server port")
	flag.Parse()

	cfg.Token = os.Getenv("ARC_TOKEN")

	// Compression label
	compressLabel := "No Compression"
	if cfg.Compress == "gzip" {
		compressLabel = "WITH GZIP COMPRESSION"
	} else if cfg.Compress == "zstd" {
		compressLabel = fmt.Sprintf("WITH ZSTD COMPRESSION (level=%d)", cfg.ZstdLevel)
	}

	dataTypeLabel := "IOT (Server Metrics)"
	if cfg.DataType == "financial" {
		dataTypeLabel = "FINANCIAL (Stock Prices)"
	}

	protocolLabel := "MessagePack Columnar"
	endpoint := "/api/v1/write/msgpack"
	if cfg.Protocol == "lineprotocol" {
		protocolLabel = "Line Protocol"
		endpoint = "/api/v1/write"
	}

	fmt.Println("================================================================================")
	fmt.Printf("SUSTAINED LOAD TEST - GO CLIENT (%s)\n", compressLabel)
	fmt.Println("================================================================================")
	fmt.Printf("Target: http://%s:%d%s\n", cfg.Host, cfg.Port, endpoint)
	fmt.Printf("Protocol: %s\n", protocolLabel)
	fmt.Printf("Data type: %s\n", dataTypeLabel)
	fmt.Printf("Duration: %ds\n", cfg.Duration)
	fmt.Printf("Batch size: %d\n", cfg.BatchSize)
	fmt.Printf("Workers: %d\n", cfg.Workers)
	fmt.Printf("Pre-generate: %d batches\n", cfg.Pregenerate)
	fmt.Println("================================================================================")
	fmt.Println()

	// Generate batches
	fmt.Printf("Pre-generating %d batches...\n", cfg.Pregenerate)
	startGen := time.Now()

	var batches [][]byte
	if cfg.Protocol == "lineprotocol" {
		batches = generateLineProtocolBatches(cfg.Pregenerate, cfg.BatchSize, cfg.Compress, cfg.ZstdLevel, cfg.DataType)
	} else if cfg.DataType == "financial" {
		batches = generateFinancialBatches(cfg.Pregenerate, cfg.BatchSize, cfg.Compress, cfg.ZstdLevel)
	} else {
		batches = generateIOTBatches(cfg.Pregenerate, cfg.BatchSize, cfg.Compress, cfg.ZstdLevel)
	}

	genTime := time.Since(startGen)
	var totalSize int64
	for _, b := range batches {
		totalSize += int64(len(b))
	}
	avgSize := float64(totalSize) / float64(len(batches))

	sizeLabel := "uncompressed"
	if cfg.Compress != "none" {
		sizeLabel = cfg.Compress
	}
	fmt.Printf("✓ Generated %d batches in %.1fs\n", cfg.Pregenerate, genTime.Seconds())
	fmt.Printf("  Avg size: %.1f KB (%s)\n", avgSize/1024, sizeLabel)
	fmt.Println()

	if cfg.Token != "" {
		fmt.Printf("Using auth token: %s...\n", cfg.Token[:min(8, len(cfg.Token))])
	} else {
		fmt.Println("No ARC_TOKEN set - authentication may fail")
	}

	// Create HTTP client with connection pooling
	client := &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        cfg.Workers + 50,
			MaxIdleConnsPerHost: cfg.Workers + 50,
			MaxConnsPerHost:     cfg.Workers + 50,
			IdleConnTimeout:     30 * time.Second,
			DisableKeepAlives:   false,
		},
		Timeout: 120 * time.Second,
	}

	// Run benchmark
	fmt.Println("Starting test...")
	stats := &Stats{}
	stats.running.Store(true)

	var wg sync.WaitGroup
	for i := 0; i < cfg.Workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			worker(id, &cfg, batches, stats, client)
		}(i)
	}

	// Progress reporting
	startTime := time.Now()
	lastSent := int64(0)
	ticker := time.NewTicker(5 * time.Second)

	go func() {
		for range ticker.C {
			if !stats.running.Load() {
				return
			}
			elapsed := time.Since(startTime).Seconds()
			currentSent := stats.totalSent.Load()
			intervalRPS := float64(currentSent-lastSent) / 5.0
			fmt.Printf("[%6.1fs] RPS: %10.0f | Total: %12d | Errors: %6d\n",
				elapsed, intervalRPS, currentSent, stats.totalErrors.Load())
			lastSent = currentSent
		}
	}()

	// Wait for duration
	time.Sleep(time.Duration(cfg.Duration) * time.Second)
	stats.running.Store(false)
	ticker.Stop()

	// Wait for workers to finish
	wg.Wait()

	elapsed := time.Since(startTime).Seconds()
	totalSent := stats.totalSent.Load()
	totalErrors := stats.totalErrors.Load()

	// Results
	fmt.Println()
	fmt.Println("================================================================================")
	fmt.Println("RESULTS")
	fmt.Println("================================================================================")
	fmt.Printf("Duration:        %.1fs\n", elapsed)
	fmt.Printf("Total sent:      %d records\n", totalSent)
	fmt.Printf("Total errors:    %d\n", totalErrors)
	successRate := float64(totalSent) / float64(totalSent+max(totalErrors, 1)) * 100
	fmt.Printf("Success rate:    %.2f%%\n", successRate)
	fmt.Println()
	fmt.Printf("🚀 THROUGHPUT:   %d records/sec\n", int64(float64(totalSent)/elapsed))
	fmt.Println()
	fmt.Println("Latency percentiles:")
	fmt.Printf("  p50:  %.2f ms\n", stats.getPercentile(0.50))
	fmt.Printf("  p95:  %.2f ms\n", stats.getPercentile(0.95))
	fmt.Printf("  p99:  %.2f ms\n", stats.getPercentile(0.99))
	fmt.Printf("  p999: %.2f ms\n", stats.getPercentile(0.999))
	fmt.Println("================================================================================")
}
