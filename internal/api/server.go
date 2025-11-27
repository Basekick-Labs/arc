package api

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"syscall"
	"time"

	"github.com/basekick-labs/arc/internal/logger"
	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/gofiber/fiber/v2/middleware/pprof"
	"github.com/gofiber/fiber/v2/middleware/recover"
	"github.com/rs/zerolog"
)

// Server represents the HTTP API server
type Server struct {
	app    *fiber.App
	logger zerolog.Logger
	port   int
}

// ServerConfig holds server configuration
type ServerConfig struct {
	Port            int
	ReadTimeout     time.Duration
	WriteTimeout    time.Duration
	IdleTimeout     time.Duration
	ShutdownTimeout time.Duration
}

// DefaultServerConfig returns default server configuration
func DefaultServerConfig() *ServerConfig {
	return &ServerConfig{
		Port:            8000,
		ReadTimeout:     30 * time.Second,
		WriteTimeout:    30 * time.Second,
		IdleTimeout:     120 * time.Second,
		ShutdownTimeout: 30 * time.Second,
	}
}

// NewServer creates a new HTTP server with Fiber
func NewServer(config *ServerConfig, logger zerolog.Logger) *Server {
	if config == nil {
		config = DefaultServerConfig()
	}

	// Create Fiber app with config
	app := fiber.New(fiber.Config{
		AppName:               "Arc Time-Series Database",
		ReadTimeout:           config.ReadTimeout,
		WriteTimeout:          config.WriteTimeout,
		IdleTimeout:           config.IdleTimeout,
		DisableStartupMessage: true,
		ErrorHandler:          customErrorHandler(logger),
		// CRITICAL: Disable automatic request body parsing to preserve gzip-compressed payloads
		DisablePreParseMultipartForm: true,
		// StreamRequestBody allows us to handle compressed bodies manually
		StreamRequestBody: false, // Keep false to get full body at once
	})

	// Middleware
	app.Use(recover.New(recover.Config{
		EnableStackTrace: true,
	}))

	app.Use(cors.New(cors.Config{
		AllowOrigins: "*",
		AllowMethods: "GET,POST,PUT,DELETE,OPTIONS",
		AllowHeaders: "Origin,Content-Type,Accept,Authorization,x-api-key,x-arc-database,Content-Encoding",
	}))

	// Security headers middleware
	app.Use(securityHeaders())

	// NOTE: Compression middleware is disabled to allow manual decompression in handlers
	// This prevents double-decompression issues with gzip-compressed MessagePack payloads
	// Response compression is still available via Accept-Encoding handling

	// pprof profiling endpoints
	app.Use(pprof.New())

	// Request logging middleware
	app.Use(requestLogger(logger))

	return &Server{
		app:    app,
		logger: logger.With().Str("component", "api-server").Logger(),
		port:   config.Port,
	}
}

// RegisterRoutes registers all API routes
func (s *Server) RegisterRoutes() {
	// Health check
	s.app.Get("/health", s.healthHandler)

	// Readiness check (for Kubernetes)
	s.app.Get("/ready", s.readyHandler)

	// Metrics endpoint (Prometheus format)
	s.app.Get("/metrics", s.metricsHandler)

	// API v1 metrics endpoints (JSON format)
	s.app.Get("/api/v1/metrics", s.apiMetricsHandler)
	s.app.Get("/api/v1/metrics/memory", s.memoryMetricsHandler)
	s.app.Get("/api/v1/metrics/query-pool", s.queryPoolMetricsHandler)
	s.app.Get("/api/v1/metrics/endpoints", s.endpointMetricsHandler)
	s.app.Get("/api/v1/metrics/timeseries/:type", s.timeseriesMetricsHandler)

	// Application logs endpoint
	s.app.Get("/api/v1/logs", s.logsHandler)

	// API v1 routes will be added here
	// MessagePack endpoint will be registered separately
}

// healthHandler returns server health status
func (s *Server) healthHandler(c *fiber.Ctx) error {
	uptime := time.Since(startTime)
	return c.JSON(fiber.Map{
		"status":     "ok",
		"time":       time.Now().UTC().Format(time.RFC3339),
		"uptime":     uptime.String(),
		"uptime_sec": uptime.Seconds(),
	})
}

// readyHandler returns server readiness status (for Kubernetes readiness probes)
func (s *Server) readyHandler(c *fiber.Ctx) error {
	// Server is ready if it's responding to requests
	// Additional checks can be added here (e.g., database connectivity)
	uptime := time.Since(startTime).Seconds()

	return c.JSON(fiber.Map{
		"status":     "ready",
		"time":       time.Now().UTC().Format(time.RFC3339),
		"uptime_sec": uptime,
	})
}

// metricsHandler returns metrics in Prometheus format or JSON
func (s *Server) metricsHandler(c *fiber.Ctx) error {
	m := metrics.Get()

	// Check Accept header for format preference
	accept := c.Get("Accept")
	if accept == "application/json" {
		return c.JSON(m.Snapshot())
	}

	// Default to Prometheus text format
	c.Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	return c.SendString(m.PrometheusFormat())
}

// apiMetricsHandler returns all metrics in JSON format (API v1)
func (s *Server) apiMetricsHandler(c *fiber.Ctx) error {
	m := metrics.Get()
	snapshot := m.Snapshot()
	snapshot["timestamp"] = time.Now().UTC().Format(time.RFC3339)
	return c.JSON(snapshot)
}

// memoryMetricsHandler returns detailed memory metrics
func (s *Server) memoryMetricsHandler(c *fiber.Ctx) error {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	return c.JSON(fiber.Map{
		"timestamp": time.Now().UTC().Format(time.RFC3339),
		"memory": fiber.Map{
			// General
			"alloc_bytes":       memStats.Alloc,
			"total_alloc_bytes": memStats.TotalAlloc,
			"sys_bytes":         memStats.Sys,
			"lookups":           memStats.Lookups,
			"mallocs":           memStats.Mallocs,
			"frees":             memStats.Frees,

			// Heap
			"heap_alloc_bytes":    memStats.HeapAlloc,
			"heap_sys_bytes":      memStats.HeapSys,
			"heap_idle_bytes":     memStats.HeapIdle,
			"heap_inuse_bytes":    memStats.HeapInuse,
			"heap_released_bytes": memStats.HeapReleased,
			"heap_objects":        memStats.HeapObjects,

			// Stack
			"stack_inuse_bytes": memStats.StackInuse,
			"stack_sys_bytes":   memStats.StackSys,

			// Off-heap
			"mspan_inuse_bytes":   memStats.MSpanInuse,
			"mspan_sys_bytes":     memStats.MSpanSys,
			"mcache_inuse_bytes":  memStats.MCacheInuse,
			"mcache_sys_bytes":    memStats.MCacheSys,
			"buck_hash_sys_bytes": memStats.BuckHashSys,
			"gc_sys_bytes":        memStats.GCSys,
			"other_sys_bytes":     memStats.OtherSys,

			// GC
			"gc_cycles":         memStats.NumGC,
			"gc_pause_total_ns": memStats.PauseTotalNs,
			"gc_pause_ns":       memStats.PauseNs[(memStats.NumGC+255)%256], // Last GC pause
			"gc_cpu_fraction":   memStats.GCCPUFraction,
			"next_gc_bytes":     memStats.NextGC,
			"last_gc_time":      time.Unix(0, int64(memStats.LastGC)).UTC().Format(time.RFC3339),
		},
		"runtime": fiber.Map{
			"goroutines":  runtime.NumGoroutine(),
			"num_cpu":     runtime.NumCPU(),
			"gomaxprocs":  runtime.GOMAXPROCS(0),
			"go_version":  runtime.Version(),
			"go_os":       runtime.GOOS,
			"go_arch":     runtime.GOARCH,
			"cgo_calls":   runtime.NumCgoCall(),
			"uptime_secs": time.Since(startTime).Seconds(),
		},
	})
}

// queryPoolMetricsHandler returns DuckDB connection pool metrics
func (s *Server) queryPoolMetricsHandler(c *fiber.Ctx) error {
	m := metrics.Get()
	snapshot := m.Snapshot()

	return c.JSON(fiber.Map{
		"timestamp": time.Now().UTC().Format(time.RFC3339),
		"pool": fiber.Map{
			"connections_open":    snapshot["db_connections_open"],
			"connections_in_use":  snapshot["db_connections_in_use"],
			"connections_idle":    snapshot["db_connections_idle"],
			"queries_total":       snapshot["db_queries_total"],
			"query_errors_total":  snapshot["db_query_errors_total"],
			"query_requests":      snapshot["query_requests_total"],
			"query_success":       snapshot["query_success_total"],
			"query_errors":        snapshot["query_errors_total"],
			"query_rows_returned": snapshot["query_rows_total"],
			"query_latency_sum_us": snapshot["query_latency_sum_us"],
			"query_latency_count":  snapshot["query_latency_count"],
		},
	})
}

// endpointMetricsHandler returns per-endpoint statistics
func (s *Server) endpointMetricsHandler(c *fiber.Ctx) error {
	m := metrics.Get()
	snapshot := m.Snapshot()

	httpLatencyAvgMs := float64(0)
	if count, ok := snapshot["http_latency_count"].(int64); ok && count > 0 {
		if sum, ok := snapshot["http_latency_sum_us"].(int64); ok {
			httpLatencyAvgMs = float64(sum) / float64(count) / 1000.0
		}
	}

	queryLatencyAvgMs := float64(0)
	if count, ok := snapshot["query_latency_count"].(int64); ok && count > 0 {
		if sum, ok := snapshot["query_latency_sum_us"].(int64); ok {
			queryLatencyAvgMs = float64(sum) / float64(count) / 1000.0
		}
	}

	return c.JSON(fiber.Map{
		"timestamp": time.Now().UTC().Format(time.RFC3339),
		"http": fiber.Map{
			"requests_total":    snapshot["http_requests_total"],
			"requests_success":  snapshot["http_requests_success"],
			"requests_error":    snapshot["http_requests_error"],
			"latency_avg_ms":    httpLatencyAvgMs,
			"latency_sum_us":    snapshot["http_latency_sum_us"],
			"latency_count":     snapshot["http_latency_count"],
		},
		"ingestion": fiber.Map{
			"records_total":  snapshot["ingest_records_total"],
			"bytes_total":    snapshot["ingest_bytes_total"],
			"batches_total":  snapshot["ingest_batches_total"],
			"errors_total":   snapshot["ingest_errors_total"],
		},
		"msgpack": fiber.Map{
			"requests_total": snapshot["msgpack_requests_total"],
			"records_total":  snapshot["msgpack_records_total"],
			"bytes_total":    snapshot["msgpack_bytes_total"],
		},
		"lineprotocol": fiber.Map{
			"requests_total": snapshot["lineprotocol_requests_total"],
			"records_total":  snapshot["lineprotocol_records_total"],
			"bytes_total":    snapshot["lineprotocol_bytes_total"],
		},
		"query": fiber.Map{
			"requests_total":  snapshot["query_requests_total"],
			"success_total":   snapshot["query_success_total"],
			"errors_total":    snapshot["query_errors_total"],
			"rows_total":      snapshot["query_rows_total"],
			"latency_avg_ms":  queryLatencyAvgMs,
		},
		"buffer": fiber.Map{
			"records_buffered": snapshot["buffer_records_buffered"],
			"records_written":  snapshot["buffer_records_written"],
			"flushes_total":    snapshot["buffer_flushes_total"],
			"errors_total":     snapshot["buffer_errors_total"],
			"queue_depth":      snapshot["buffer_queue_depth"],
		},
		"storage": fiber.Map{
			"writes_total":      snapshot["storage_writes_total"],
			"write_bytes_total": snapshot["storage_write_bytes_total"],
			"reads_total":       snapshot["storage_reads_total"],
			"read_bytes_total":  snapshot["storage_read_bytes_total"],
			"errors_total":      snapshot["storage_errors_total"],
		},
		"compaction": fiber.Map{
			"jobs_total":      snapshot["compaction_jobs_total"],
			"jobs_success":    snapshot["compaction_jobs_success"],
			"jobs_failed":     snapshot["compaction_jobs_failed"],
			"files_compacted": snapshot["compaction_files_compacted"],
			"bytes_read":      snapshot["compaction_bytes_read"],
			"bytes_written":   snapshot["compaction_bytes_written"],
		},
		"auth": fiber.Map{
			"requests_total":  snapshot["auth_requests_total"],
			"cache_hits":      snapshot["auth_cache_hits"],
			"cache_misses":    snapshot["auth_cache_misses"],
			"failures_total":  snapshot["auth_failures_total"],
		},
	})
}

var startTime = time.Now()

// Start starts the HTTP server
func (s *Server) Start() error {
	s.logger.Info().
		Int("port", s.port).
		Msg("Starting Arc HTTP server")

	// Start server in goroutine
	go func() {
		addr := fmt.Sprintf(":%d", s.port)
		if err := s.app.Listen(addr); err != nil {
			s.logger.Fatal().Err(err).Msg("Failed to start server")
		}
	}()

	return nil
}

// Shutdown gracefully shuts down the server
func (s *Server) Shutdown(timeout time.Duration) error {
	s.logger.Info().Msg("Shutting down server gracefully...")

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	if err := s.app.ShutdownWithContext(ctx); err != nil {
		return fmt.Errorf("server shutdown failed: %w", err)
	}

	s.logger.Info().Msg("Server stopped")
	return nil
}

// WaitForShutdown blocks until shutdown signal is received
func (s *Server) WaitForShutdown(shutdownTimeout time.Duration) {
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	sig := <-quit
	s.logger.Info().Str("signal", sig.String()).Msg("Received shutdown signal")

	if err := s.Shutdown(shutdownTimeout); err != nil {
		s.logger.Error().Err(err).Msg("Shutdown error")
	}
}

// GetApp returns the underlying Fiber app (for registering custom routes)
func (s *Server) GetApp() *fiber.App {
	return s.app
}

// logsHandler returns recent application logs
func (s *Server) logsHandler(c *fiber.Ctx) error {
	// Parse query parameters
	limit := 100
	if l := c.Query("limit"); l != "" {
		if parsed, err := strconv.Atoi(l); err == nil && parsed > 0 && parsed <= 1000 {
			limit = parsed
		}
	}

	level := c.Query("level") // e.g., "error", "warn", "info", "debug"

	sinceMinutes := 60
	if sm := c.Query("since_minutes"); sm != "" {
		if parsed, err := strconv.Atoi(sm); err == nil && parsed > 0 && parsed <= 1440 {
			sinceMinutes = parsed
		}
	}

	// Get logs from buffer
	buffer := logger.GetBuffer()
	entries := buffer.GetRecent(limit, level, sinceMinutes)

	return c.JSON(fiber.Map{
		"timestamp":     time.Now().UTC().Format(time.RFC3339),
		"count":         len(entries),
		"limit":         limit,
		"level_filter":  level,
		"since_minutes": sinceMinutes,
		"logs":          entries,
	})
}

// timeseriesMetricsHandler returns time-series metrics data
func (s *Server) timeseriesMetricsHandler(c *fiber.Ctx) error {
	metricType := c.Params("type") // system, application, api

	durationMinutes := 30
	if dm := c.Query("duration_minutes"); dm != "" {
		if parsed, err := strconv.Atoi(dm); err == nil && parsed > 0 && parsed <= 1440 {
			durationMinutes = parsed
		}
	}

	collector := metrics.GetTimeSeriesCollector()
	var points []metrics.TimeSeriesPoint

	switch metricType {
	case "system":
		points = collector.GetSystem(durationMinutes)
	case "application":
		points = collector.GetApplication(durationMinutes)
	case "api":
		points = collector.GetAPI(durationMinutes)
	default:
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error":       "Invalid metric type",
			"valid_types": []string{"system", "application", "api"},
		})
	}

	return c.JSON(fiber.Map{
		"timestamp":        time.Now().UTC().Format(time.RFC3339),
		"type":             metricType,
		"duration_minutes": durationMinutes,
		"points_count":     len(points),
		"data":             points,
	})
}

// customErrorHandler handles Fiber errors
func customErrorHandler(logger zerolog.Logger) fiber.ErrorHandler {
	return func(c *fiber.Ctx, err error) error {
		code := fiber.StatusInternalServerError

		if e, ok := err.(*fiber.Error); ok {
			code = e.Code
		}

		logger.Error().
			Err(err).
			Int("status", code).
			Str("method", c.Method()).
			Str("path", c.Path()).
			Msg("Request error")

		return c.Status(code).JSON(fiber.Map{
			"error": err.Error(),
		})
	}
}

// securityHeaders adds security headers to all responses
func securityHeaders() fiber.Handler {
	return func(c *fiber.Ctx) error {
		// Prevent clickjacking - deny framing from any origin
		c.Set("X-Frame-Options", "DENY")

		// Prevent MIME type sniffing - browsers should trust Content-Type
		c.Set("X-Content-Type-Options", "nosniff")

		// XSS protection (legacy, but still useful for older browsers)
		c.Set("X-XSS-Protection", "1; mode=block")

		// Referrer policy - don't leak referrer to other origins
		c.Set("Referrer-Policy", "strict-origin-when-cross-origin")

		// Permissions policy - disable unnecessary browser features
		c.Set("Permissions-Policy", "geolocation=(), microphone=(), camera=()")

		// Content Security Policy - restrict resource loading
		// Note: API-only service, so restrictive CSP is appropriate
		c.Set("Content-Security-Policy", "default-src 'none'; frame-ancestors 'none'")

		// Note: HSTS (Strict-Transport-Security) should be set by reverse proxy/load balancer
		// when TLS is terminated there, not by the application

		return c.Next()
	}
}

// requestLogger logs errors only and collects metrics
func requestLogger(logger zerolog.Logger) fiber.Handler {
	return func(c *fiber.Ctx) error {
		start := time.Now()

		// Process request
		err := c.Next()

		// Collect metrics (always)
		duration := time.Since(start)
		status := c.Response().StatusCode()
		m := metrics.Get()

		m.IncHTTPRequests()
		m.RecordHTTPLatency(duration.Microseconds())

		if status >= 400 {
			m.IncHTTPError()
		} else {
			m.IncHTTPSuccess()
		}

		// OPTIMIZATION: Only log errors (status >= 400) to save 20% CPU
		// Profiling showed request logging consuming 19.62% of CPU time
		if status >= 400 {
			logEvent := logger.Warn()
			if status >= 500 {
				logEvent = logger.Error()
			}

			logEvent.
				Str("method", c.Method()).
				Str("path", c.Path()).
				Int("status", status).
				Dur("duration_ms", duration).
				Int("size", len(c.Response().Body())).
				Str("ip", c.IP()).
				Msg("HTTP request error")
		}

		return err
	}
}
