// Log ingestion benchmark for Arc vs Elasticsearch, OpenSearch, ClickHouse, VictoriaLogs, Loki, Quickwit
// Usage: go run benchmarks/log_bench/main.go [flags]
//
// Examples:
//   go run benchmarks/log_bench/main.go --target arc --duration 60
//   go run benchmarks/log_bench/main.go --target elastic --workers 50 --compress gzip
//   go run benchmarks/log_bench/main.go --target opensearch --workers 50
//   go run benchmarks/log_bench/main.go --target clickhouse --duration 60
//   go run benchmarks/log_bench/main.go --target victorialogs --batch-size 1000
//   go run benchmarks/log_bench/main.go --target loki --duration 120
//   go run benchmarks/log_bench/main.go --target quickwit --compress gzip

package main

import (
	"bytes"
	"compress/gzip"
	"context"
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

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/google/uuid"
	"github.com/klauspost/compress/zstd"
	"github.com/vmihailenco/msgpack/v5"
)

// Target systems
const (
	TargetArc                = "arc"
	TargetElastic            = "elastic"
	TargetOpenSearch         = "opensearch"
	TargetClickHouse         = "clickhouse"
	TargetClickHouseNative   = "clickhouse-native"
	TargetVictoriaLogs       = "victorialogs"
	TargetLoki               = "loki"
	TargetQuickwit           = "quickwit"
)

// Default ports per target
var defaultPorts = map[string]int{
	TargetArc:                8000,
	TargetElastic:            9200,
	TargetOpenSearch:         9200,
	TargetClickHouse:         8123,
	TargetClickHouseNative:   9000,
	TargetVictoriaLogs:       9428,
	TargetLoki:               3100,
	TargetQuickwit:           7280,
}

// Success status codes per target
var successCodes = map[string]int{
	TargetArc:                204,
	TargetElastic:            200,
	TargetOpenSearch:         200,
	TargetClickHouse:         200,
	TargetClickHouseNative:   0, // Not HTTP-based
	TargetVictoriaLogs:       200,
	TargetLoki:               204,
	TargetQuickwit:           200,
}

type Config struct {
	Target      string
	Duration    int
	Workers     int
	BatchSize   int
	Pregenerate int
	Compress    string
	ZstdLevel   int
	Host        string
	Port        int
	Index       string
	Protocol    string // For Arc: msgpack or lineprotocol
	Token       string
	MaxLogs     int64  // Stop after ingesting this many logs (0 = unlimited, use duration)
}

type Stats struct {
	totalSent       atomic.Int64
	totalErrors     atomic.Int64
	running         atomic.Bool
	workerLatencies [][]float64
	latencyMu       sync.Mutex
}

func (s *Stats) initWorkers(n int) {
	s.workerLatencies = make([][]float64, n)
	for i := range s.workerLatencies {
		s.workerLatencies[i] = make([]float64, 0, 10000)
	}
}

func (s *Stats) addLatency(workerID int, ms float64) {
	s.workerLatencies[workerID] = append(s.workerLatencies[workerID], ms)
}

func (s *Stats) getPercentile(p float64) float64 {
	var total int
	for _, wl := range s.workerLatencies {
		total += len(wl)
	}
	if total == 0 {
		return 0
	}

	all := make([]float64, 0, total)
	for _, wl := range s.workerLatencies {
		all = append(all, wl...)
	}
	sort.Float64s(all)

	idx := int(float64(len(all)) * p)
	if idx >= len(all) {
		idx = len(all) - 1
	}
	return all[idx]
}

// LogRecord represents a single Web API log entry
type LogRecord struct {
	Timestamp  int64   // microseconds since epoch
	Level      string  // DEBUG, INFO, WARN, ERROR, FATAL
	Service    string  // api, auth, gateway, etc.
	Host       string  // server001, server002, etc.
	RequestID  string  // UUID
	TraceID    string  // UUID
	Method     string  // GET, POST, PUT, DELETE, PATCH
	Path       string  // /api/v1/users, etc.
	StatusCode int     // 200, 201, 400, 401, 404, 500, etc.
	LatencyMs  float64 // request latency
	UserID     string  // user_0001, etc.
	Message    string  // log message
	Error      string  // error details (only for ERROR/FATAL)
}

// Log generation constants
var (
	levels   = []string{"DEBUG", "INFO", "WARN", "ERROR", "FATAL"}
	services = []string{
		"api", "auth", "gateway", "users", "orders", "payments", "inventory",
		"notifications", "search", "analytics", "reporting", "scheduler",
		"cache", "queue", "storage", "cdn", "mailer", "sms", "webhook", "audit",
	}
	methods = []string{"GET", "POST", "PUT", "DELETE", "PATCH"}
	paths   = []string{
		"/api/v1/users", "/api/v1/users/{id}", "/api/v1/orders", "/api/v1/orders/{id}",
		"/api/v1/products", "/api/v1/products/{id}", "/api/v1/cart", "/api/v1/checkout",
		"/api/v1/payments", "/api/v1/auth/login", "/api/v1/auth/logout", "/api/v1/auth/refresh",
		"/api/v1/search", "/api/v1/notifications", "/api/v1/webhooks", "/api/v1/health",
	}
	statusCodes = []int{200, 200, 200, 200, 201, 204, 400, 401, 403, 404, 500, 502, 503}
	errorMsgs   = []string{
		"Connection timeout", "Database connection failed", "Invalid request payload",
		"Authentication failed", "Rate limit exceeded", "Service unavailable",
		"Internal server error", "Resource not found", "Permission denied",
	}
)

func generateLogRecords(batchSize int, nowMicros int64) []LogRecord {
	records := make([]LogRecord, batchSize)

	for i := 0; i < batchSize; i++ {
		// Level distribution: 60% INFO, 25% DEBUG, 10% WARN, 4% ERROR, 1% FATAL
		levelRoll := rand.Float64()
		var level string
		switch {
		case levelRoll < 0.25:
			level = "DEBUG"
		case levelRoll < 0.85:
			level = "INFO"
		case levelRoll < 0.95:
			level = "WARN"
		case levelRoll < 0.99:
			level = "ERROR"
		default:
			level = "FATAL"
		}

		service := services[rand.Intn(len(services))]
		method := methods[rand.Intn(len(methods))]
		path := paths[rand.Intn(len(paths))]
		statusCode := statusCodes[rand.Intn(len(statusCodes))]

		// Latency distribution: mostly fast, some slow
		var latency float64
		latencyRoll := rand.Float64()
		switch {
		case latencyRoll < 0.7:
			latency = 1 + rand.Float64()*50 // 1-51ms (fast)
		case latencyRoll < 0.9:
			latency = 50 + rand.Float64()*200 // 50-250ms (medium)
		case latencyRoll < 0.98:
			latency = 250 + rand.Float64()*750 // 250-1000ms (slow)
		default:
			latency = 1000 + rand.Float64()*4000 // 1-5s (very slow)
		}

		message := fmt.Sprintf("%s %s completed in %.0fms with status %d", method, path, latency, statusCode)

		var errorMsg string
		if level == "ERROR" || level == "FATAL" {
			errorMsg = errorMsgs[rand.Intn(len(errorMsgs))]
			message = fmt.Sprintf("%s %s failed: %s", method, path, errorMsg)
		}

		records[i] = LogRecord{
			Timestamp:  nowMicros + int64(i),
			Level:      level,
			Service:    service,
			Host:       fmt.Sprintf("server%03d", rand.Intn(100)),
			RequestID:  uuid.New().String(),
			TraceID:    uuid.New().String(),
			Method:     method,
			Path:       path,
			StatusCode: statusCode,
			LatencyMs:  latency,
			UserID:     fmt.Sprintf("user_%05d", rand.Intn(10000)),
			Message:    message,
			Error:      errorMsg,
		}
	}

	return records
}

// Format converters for each target

func formatForArc(records []LogRecord, compress string, zstdEncoder *zstd.Encoder) []byte {
	n := len(records)

	times := make([]int64, n)
	levels := make([]string, n)
	services := make([]string, n)
	hosts := make([]string, n)
	requestIDs := make([]string, n)
	traceIDs := make([]string, n)
	methods := make([]string, n)
	pathsCol := make([]string, n)
	statusCodes := make([]int, n)
	latencies := make([]float64, n)
	userIDs := make([]string, n)
	messages := make([]string, n)
	errors := make([]string, n)

	for i, r := range records {
		times[i] = r.Timestamp
		levels[i] = r.Level
		services[i] = r.Service
		hosts[i] = r.Host
		requestIDs[i] = r.RequestID
		traceIDs[i] = r.TraceID
		methods[i] = r.Method
		pathsCol[i] = r.Path
		statusCodes[i] = r.StatusCode
		latencies[i] = r.LatencyMs
		userIDs[i] = r.UserID
		messages[i] = r.Message
		errors[i] = r.Error
	}

	payload := map[string]interface{}{
		"m": "logs",
		"columns": map[string]interface{}{
			"time":        times,
			"level":       levels,
			"service":     services,
			"host":        hosts,
			"request_id":  requestIDs,
			"trace_id":    traceIDs,
			"method":      methods,
			"path":        pathsCol,
			"status_code": statusCodes,
			"latency_ms":  latencies,
			"user_id":     userIDs,
			"message":     messages,
			"error":       errors,
		},
	}

	data, err := msgpack.Marshal(payload)
	if err != nil {
		panic(err)
	}

	return compressData(data, compress, zstdEncoder)
}

func formatForElastic(records []LogRecord, index string, compress string, zstdEncoder *zstd.Encoder) []byte {
	var buf bytes.Buffer

	indexLine := fmt.Sprintf(`{"index":{"_index":"%s"}}`, index)

	for _, r := range records {
		buf.WriteString(indexLine)
		buf.WriteByte('\n')

		doc := map[string]interface{}{
			"@timestamp":  time.UnixMicro(r.Timestamp).UTC().Format(time.RFC3339Nano),
			"level":       r.Level,
			"service":     r.Service,
			"host":        r.Host,
			"request_id":  r.RequestID,
			"trace_id":    r.TraceID,
			"method":      r.Method,
			"path":        r.Path,
			"status_code": r.StatusCode,
			"latency_ms":  r.LatencyMs,
			"user_id":     r.UserID,
			"message":     r.Message,
		}
		if r.Error != "" {
			doc["error"] = r.Error
		}

		docBytes, _ := json.Marshal(doc)
		buf.Write(docBytes)
		buf.WriteByte('\n')
	}

	return compressData(buf.Bytes(), compress, zstdEncoder)
}

func formatForVictoriaLogs(records []LogRecord, compress string, zstdEncoder *zstd.Encoder) []byte {
	var buf bytes.Buffer

	for _, r := range records {
		doc := map[string]interface{}{
			"_time":       time.UnixMicro(r.Timestamp).UTC().Format(time.RFC3339Nano),
			"level":       r.Level,
			"service":     r.Service,
			"host":        r.Host,
			"request_id":  r.RequestID,
			"trace_id":    r.TraceID,
			"method":      r.Method,
			"path":        r.Path,
			"status_code": r.StatusCode,
			"latency_ms":  r.LatencyMs,
			"user_id":     r.UserID,
			"_msg":        r.Message,
		}
		if r.Error != "" {
			doc["error"] = r.Error
		}

		docBytes, _ := json.Marshal(doc)
		buf.Write(docBytes)
		buf.WriteByte('\n')
	}

	return compressData(buf.Bytes(), compress, zstdEncoder)
}

func formatForLoki(records []LogRecord, compress string, zstdEncoder *zstd.Encoder) []byte {
	// Group records by service+level for optimal Loki performance
	streams := make(map[string]*lokiStream)

	for _, r := range records {
		key := r.Service + "|" + r.Level
		stream, exists := streams[key]
		if !exists {
			stream = &lokiStream{
				Stream: map[string]string{
					"service": r.Service,
					"level":   r.Level,
				},
				Values: make([][2]string, 0),
			}
			streams[key] = stream
		}

		// Loki uses nanoseconds as string
		tsNano := fmt.Sprintf("%d", r.Timestamp*1000)

		// Build structured log line
		logLine := fmt.Sprintf("host=%s request_id=%s trace_id=%s method=%s path=%s status=%d latency=%.2f user=%s msg=%q",
			r.Host, r.RequestID, r.TraceID, r.Method, r.Path, r.StatusCode, r.LatencyMs, r.UserID, r.Message)
		if r.Error != "" {
			logLine += fmt.Sprintf(" error=%q", r.Error)
		}

		stream.Values = append(stream.Values, [2]string{tsNano, logLine})
	}

	// Build final payload
	streamsList := make([]*lokiStream, 0, len(streams))
	for _, s := range streams {
		streamsList = append(streamsList, s)
	}

	payload := lokiPushRequest{Streams: streamsList}
	data, _ := json.Marshal(payload)

	return compressData(data, compress, zstdEncoder)
}

type lokiStream struct {
	Stream map[string]string `json:"stream"`
	Values [][2]string       `json:"values"`
}

type lokiPushRequest struct {
	Streams []*lokiStream `json:"streams"`
}

func formatForQuickwit(records []LogRecord, compress string, zstdEncoder *zstd.Encoder) []byte {
	var buf bytes.Buffer

	for _, r := range records {
		doc := map[string]interface{}{
			"timestamp":   time.UnixMicro(r.Timestamp).UTC().Format(time.RFC3339Nano),
			"level":       r.Level,
			"service":     r.Service,
			"host":        r.Host,
			"request_id":  r.RequestID,
			"trace_id":    r.TraceID,
			"method":      r.Method,
			"path":        r.Path,
			"status_code": r.StatusCode,
			"latency_ms":  r.LatencyMs,
			"user_id":     r.UserID,
			"message":     r.Message,
		}
		if r.Error != "" {
			doc["error"] = r.Error
		}

		docBytes, _ := json.Marshal(doc)
		buf.Write(docBytes)
		buf.WriteByte('\n')
	}

	return compressData(buf.Bytes(), compress, zstdEncoder)
}

func formatForClickHouse(records []LogRecord, compress string, zstdEncoder *zstd.Encoder) []byte {
	var buf bytes.Buffer

	for _, r := range records {
		// ClickHouse expects DateTime64 as string in ISO format
		doc := map[string]interface{}{
			"timestamp":   time.UnixMicro(r.Timestamp).UTC().Format("2006-01-02 15:04:05.000000"),
			"level":       r.Level,
			"service":     r.Service,
			"host":        r.Host,
			"request_id":  r.RequestID,
			"trace_id":    r.TraceID,
			"method":      r.Method,
			"path":        r.Path,
			"status_code": r.StatusCode,
			"latency_ms":  r.LatencyMs,
			"user_id":     r.UserID,
			"message":     r.Message,
			"error":       r.Error,
		}

		docBytes, _ := json.Marshal(doc)
		buf.Write(docBytes)
		buf.WriteByte('\n')
	}

	return compressData(buf.Bytes(), compress, zstdEncoder)
}

func compressData(data []byte, compress string, zstdEncoder *zstd.Encoder) []byte {
	switch compress {
	case "gzip":
		var buf bytes.Buffer
		w, _ := gzip.NewWriterLevel(&buf, gzip.BestSpeed)
		w.Write(data)
		w.Close()
		return buf.Bytes()
	case "zstd":
		return zstdEncoder.EncodeAll(data, nil)
	default:
		return data
	}
}

func generateBatches(cfg *Config) [][]byte {
	batches := make([][]byte, cfg.Pregenerate)

	var zstdEncoder *zstd.Encoder
	if cfg.Compress == "zstd" {
		var err error
		zstdEncoder, err = zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(cfg.ZstdLevel)))
		if err != nil {
			panic(err)
		}
	}

	for i := 0; i < cfg.Pregenerate; i++ {
		nowMicros := time.Now().UnixMicro()
		records := generateLogRecords(cfg.BatchSize, nowMicros)

		switch cfg.Target {
		case TargetArc:
			batches[i] = formatForArc(records, cfg.Compress, zstdEncoder)
		case TargetElastic, TargetOpenSearch:
			batches[i] = formatForElastic(records, cfg.Index, cfg.Compress, zstdEncoder)
		case TargetClickHouse:
			batches[i] = formatForClickHouse(records, cfg.Compress, zstdEncoder)
		case TargetVictoriaLogs:
			batches[i] = formatForVictoriaLogs(records, cfg.Compress, zstdEncoder)
		case TargetLoki:
			batches[i] = formatForLoki(records, cfg.Compress, zstdEncoder)
		case TargetQuickwit:
			batches[i] = formatForQuickwit(records, cfg.Compress, zstdEncoder)
		}

		if (i+1)%100 == 0 {
			fmt.Printf("  Progress: %d/%d\n", i+1, cfg.Pregenerate)
		}
	}

	return batches
}

// generateRecordBatches generates raw LogRecord batches for native protocols
func generateRecordBatches(cfg *Config) [][]LogRecord {
	batches := make([][]LogRecord, cfg.Pregenerate)

	for i := 0; i < cfg.Pregenerate; i++ {
		nowMicros := time.Now().UnixMicro()
		batches[i] = generateLogRecords(cfg.BatchSize, nowMicros)

		if (i+1)%100 == 0 {
			fmt.Printf("  Progress: %d/%d\n", i+1, cfg.Pregenerate)
		}
	}

	return batches
}

func getEndpoint(cfg *Config) (string, string) {
	switch cfg.Target {
	case TargetArc:
		if cfg.Protocol == "lineprotocol" {
			return fmt.Sprintf("http://%s:%d/api/v1/write", cfg.Host, cfg.Port), "text/plain"
		}
		return fmt.Sprintf("http://%s:%d/api/v1/write/msgpack", cfg.Host, cfg.Port), "application/msgpack"
	case TargetElastic, TargetOpenSearch:
		return fmt.Sprintf("http://%s:%d/_bulk", cfg.Host, cfg.Port), "application/x-ndjson"
	case TargetClickHouse:
		// ClickHouse HTTP interface with JSONEachRow format
		return fmt.Sprintf("http://%s:%d/?query=INSERT+INTO+%s+FORMAT+JSONEachRow", cfg.Host, cfg.Port, cfg.Index), "application/json"
	case TargetClickHouseNative:
		// Native TCP protocol - not HTTP
		return fmt.Sprintf("clickhouse://%s:%d", cfg.Host, cfg.Port), "native"
	case TargetVictoriaLogs:
		return fmt.Sprintf("http://%s:%d/insert/jsonline", cfg.Host, cfg.Port), "application/json"
	case TargetLoki:
		return fmt.Sprintf("http://%s:%d/loki/api/v1/push", cfg.Host, cfg.Port), "application/json"
	case TargetQuickwit:
		return fmt.Sprintf("http://%s:%d/api/v1/%s/ingest", cfg.Host, cfg.Port, cfg.Index), "application/x-ndjson"
	default:
		panic("unknown target: " + cfg.Target)
	}
}

// clickhouseNativeWorker uses the native TCP protocol for ClickHouse
func clickhouseNativeWorker(id int, cfg *Config, recordBatches [][]LogRecord, stats *Stats) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)},
		Settings: clickhouse.Settings{
			"max_execution_time": 60,
		},
		DialTimeout:     10 * time.Second,
		MaxOpenConns:    1,
		MaxIdleConns:    1,
		ConnMaxLifetime: time.Hour,
	})
	if err != nil {
		fmt.Printf("Worker %d: failed to connect: %v\n", id, err)
		return
	}
	defer conn.Close()

	ctx := context.Background()
	batchIdx := 0

	for stats.running.Load() {
		records := recordBatches[batchIdx%len(recordBatches)]
		batchIdx++

		start := time.Now()

		batch, err := conn.PrepareBatch(ctx, "INSERT INTO "+cfg.Index)
		if err != nil {
			stats.totalErrors.Add(1)
			if stats.totalErrors.Load() <= 3 {
				fmt.Printf("Worker %d: PrepareBatch error: %v\n", id, err)
			}
			continue
		}

		for _, r := range records {
			err := batch.Append(
				time.UnixMicro(r.Timestamp),
				r.Level,
				r.Service,
				r.Host,
				r.RequestID,
				r.TraceID,
				r.Method,
				r.Path,
				int32(r.StatusCode),
				r.LatencyMs,
				r.UserID,
				r.Message,
				r.Error,
			)
			if err != nil {
				stats.totalErrors.Add(1)
				if stats.totalErrors.Load() <= 3 {
					fmt.Printf("Worker %d: Append error: %v\n", id, err)
				}
				break
			}
		}

		if err := batch.Send(); err != nil {
			stats.totalErrors.Add(1)
			if stats.totalErrors.Load() <= 3 {
				fmt.Printf("Worker %d: Send error: %v\n", id, err)
			}
			continue
		}

		latencyMs := float64(time.Since(start).Microseconds()) / 1000.0
		stats.totalSent.Add(int64(len(records)))
		stats.addLatency(id, latencyMs)
	}
}

func worker(id int, cfg *Config, batches [][]byte, stats *Stats, client *http.Client) {
	url, contentType := getEndpoint(cfg)
	successCode := successCodes[cfg.Target]
	batchIdx := 0

	headers := map[string]string{
		"Content-Type": contentType,
	}

	// Target-specific headers
	switch cfg.Target {
	case TargetArc:
		headers["x-arc-database"] = cfg.Index
		if cfg.Token != "" {
			headers["Authorization"] = "Bearer " + cfg.Token
		}
	case TargetElastic, TargetOpenSearch:
		// Elasticsearch/OpenSearch might need auth
		if cfg.Token != "" {
			headers["Authorization"] = "ApiKey " + cfg.Token
		}
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

		// Check for success (some targets return 200, others 204)
		if resp.StatusCode == successCode || resp.StatusCode == 200 || resp.StatusCode == 204 {
			stats.totalSent.Add(int64(cfg.BatchSize))
			stats.addLatency(id, latencyMs)
		} else {
			stats.totalErrors.Add(1)
			if stats.totalErrors.Load() <= 3 {
				body, _ := io.ReadAll(resp.Body)
				bodyStr := string(body)
				if len(bodyStr) > 200 {
					bodyStr = bodyStr[:200]
				}
				fmt.Printf("Error %d: %s\n", resp.StatusCode, bodyStr)
			}
		}
		resp.Body.Close()
	}
}

func main() {
	cfg := Config{}

	flag.StringVar(&cfg.Target, "target", "arc", "Target: arc, elastic, opensearch, clickhouse, clickhouse-native, victorialogs, loki, quickwit")
	flag.IntVar(&cfg.Duration, "duration", 60, "Test duration in seconds")
	flag.IntVar(&cfg.Workers, "workers", 100, "Number of concurrent workers")
	flag.IntVar(&cfg.BatchSize, "batch-size", 500, "Log records per batch")
	flag.IntVar(&cfg.Pregenerate, "pregenerate", 1000, "Number of batches to pre-generate")
	flag.StringVar(&cfg.Compress, "compress", "none", "Compression: none, gzip, zstd")
	flag.IntVar(&cfg.ZstdLevel, "zstd-level", 3, "Zstd compression level (1-22)")
	flag.StringVar(&cfg.Host, "host", "localhost", "Server host")
	flag.IntVar(&cfg.Port, "port", 0, "Server port (0 = use default for target)")
	flag.StringVar(&cfg.Index, "index", "logs", "Index/database name")
	flag.StringVar(&cfg.Protocol, "protocol", "msgpack", "For Arc: msgpack or lineprotocol")
	flag.Int64Var(&cfg.MaxLogs, "max-logs", 0, "Stop after ingesting N logs (0 = unlimited, use duration)")
	flag.Parse()

	cfg.Token = os.Getenv("ARC_TOKEN")
	if cfg.Token == "" {
		cfg.Token = os.Getenv("ELASTIC_TOKEN")
	}

	// Normalize target name
	cfg.Target = strings.ToLower(cfg.Target)

	// Set default port if not specified
	if cfg.Port == 0 {
		port, ok := defaultPorts[cfg.Target]
		if !ok {
			fmt.Printf("Unknown target: %s\n", cfg.Target)
			fmt.Println("Valid targets: arc, elastic, opensearch, clickhouse, clickhouse-native, victorialogs, loki, quickwit")
			os.Exit(1)
		}
		cfg.Port = port
	}

	// Compression label
	compressLabel := "No Compression"
	if cfg.Compress == "gzip" {
		compressLabel = "WITH GZIP COMPRESSION"
	} else if cfg.Compress == "zstd" {
		compressLabel = fmt.Sprintf("WITH ZSTD COMPRESSION (level=%d)", cfg.ZstdLevel)
	}

	// Target labels
	targetLabels := map[string]string{
		TargetArc:              "ARC",
		TargetElastic:          "ELASTICSEARCH",
		TargetOpenSearch:       "OPENSEARCH",
		TargetClickHouse:       "CLICKHOUSE (HTTP)",
		TargetClickHouseNative: "CLICKHOUSE (NATIVE)",
		TargetVictoriaLogs:     "VICTORIALOGS",
		TargetLoki:             "GRAFANA LOKI",
		TargetQuickwit:         "QUICKWIT",
	}

	url, _ := getEndpoint(&cfg)

	fmt.Println("================================================================================")
	fmt.Printf("LOG INGESTION BENCHMARK - %s (%s)\n", targetLabels[cfg.Target], compressLabel)
	fmt.Println("================================================================================")
	fmt.Printf("Target: %s\n", url)
	if cfg.MaxLogs > 0 {
		fmt.Printf("Max logs: %dM (%d)\n", cfg.MaxLogs/1_000_000, cfg.MaxLogs)
		fmt.Printf("Timeout: %ds\n", cfg.Duration)
	} else {
		fmt.Printf("Duration: %ds\n", cfg.Duration)
	}
	fmt.Printf("Batch size: %d logs\n", cfg.BatchSize)
	fmt.Printf("Workers: %d\n", cfg.Workers)
	fmt.Printf("Pre-generate: %d batches\n", cfg.Pregenerate)
	fmt.Printf("Index: %s\n", cfg.Index)
	fmt.Println("================================================================================")
	fmt.Println()

	// Generate batches
	fmt.Printf("Pre-generating %d batches of %d logs each...\n", cfg.Pregenerate, cfg.BatchSize)
	startGen := time.Now()

	var batches [][]byte
	var recordBatches [][]LogRecord

	if cfg.Target == TargetClickHouseNative {
		// Native protocol needs raw LogRecord batches
		recordBatches = generateRecordBatches(&cfg)
	} else {
		batches = generateBatches(&cfg)
	}

	genTime := time.Since(startGen)

	if cfg.Target == TargetClickHouseNative {
		fmt.Printf("Generated %d batches in %.1fs\n", cfg.Pregenerate, genTime.Seconds())
		fmt.Printf("  Batch size: %d records (native protocol)\n", cfg.BatchSize)
	} else {
		var totalSize int64
		for _, b := range batches {
			totalSize += int64(len(b))
		}
		avgSize := float64(totalSize) / float64(len(batches))

		sizeLabel := "uncompressed"
		if cfg.Compress != "none" {
			sizeLabel = cfg.Compress
		}
		fmt.Printf("Generated %d batches in %.1fs\n", cfg.Pregenerate, genTime.Seconds())
		fmt.Printf("  Avg batch size: %.1f KB (%s)\n", avgSize/1024, sizeLabel)
	}
	fmt.Println()

	if cfg.Token != "" {
		fmt.Printf("Using auth token: %s...\n", cfg.Token[:min(8, len(cfg.Token))])
	}

	// Create HTTP client with connection pooling (not used for native protocols)
	var client *http.Client
	if cfg.Target != TargetClickHouseNative {
		client = &http.Client{
			Transport: &http.Transport{
				MaxIdleConns:        cfg.Workers + 50,
				MaxIdleConnsPerHost: cfg.Workers + 50,
				MaxConnsPerHost:     cfg.Workers + 50,
				IdleConnTimeout:     30 * time.Second,
				DisableKeepAlives:   false,
			},
			Timeout: 120 * time.Second,
		}
	}

	// Run benchmark
	fmt.Println("Starting test...")
	stats := &Stats{}
	stats.initWorkers(cfg.Workers)
	stats.running.Store(true)

	var wg sync.WaitGroup
	for i := 0; i < cfg.Workers; i++ {
		wg.Add(1)
		if cfg.Target == TargetClickHouseNative {
			go func(id int) {
				defer wg.Done()
				clickhouseNativeWorker(id, &cfg, recordBatches, stats)
			}(i)
		} else {
			go func(id int) {
				defer wg.Done()
				worker(id, &cfg, batches, stats, client)
			}(i)
		}
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

	// Wait for duration or max-logs reached
	if cfg.MaxLogs > 0 {
		// Poll until max logs reached or duration exceeded
		deadline := time.After(time.Duration(cfg.Duration) * time.Second)
		poll := time.NewTicker(100 * time.Millisecond)
		defer poll.Stop()
	waitLoop:
		for {
			select {
			case <-deadline:
				break waitLoop
			case <-poll.C:
				if stats.totalSent.Load() >= cfg.MaxLogs {
					break waitLoop
				}
			}
		}
	} else {
		time.Sleep(time.Duration(cfg.Duration) * time.Second)
	}
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
	fmt.Printf("Total sent:      %d logs\n", totalSent)
	fmt.Printf("Total errors:    %d\n", totalErrors)
	successRate := float64(totalSent) / float64(totalSent+max(totalErrors, 1)) * 100
	fmt.Printf("Success rate:    %.2f%%\n", successRate)
	fmt.Println()
	fmt.Printf("THROUGHPUT:   %d logs/sec\n", int64(float64(totalSent)/elapsed))
	fmt.Println()
	fmt.Println("Latency percentiles:")
	fmt.Printf("  p50:  %.2f ms\n", stats.getPercentile(0.50))
	fmt.Printf("  p95:  %.2f ms\n", stats.getPercentile(0.95))
	fmt.Printf("  p99:  %.2f ms\n", stats.getPercentile(0.99))
	fmt.Printf("  p999: %.2f ms\n", stats.getPercentile(0.999))
	fmt.Println("================================================================================")
}
