package api

import (
	"bytes"
	"fmt"
	"io"
	"regexp"
	"sync"
	"sync/atomic"

	"github.com/basekick-labs/arc/internal/cluster"
	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/gofiber/fiber/v2"
	"github.com/klauspost/compress/gzip"
	"github.com/rs/zerolog"
)

// Pool for gzip readers - avoids allocating ~32KB internal decompression state per request
// This significantly reduces GC pressure under high load with compressed payloads
var lpGzipReaderPool = sync.Pool{}

// LineProtocolHandler handles Line Protocol write requests
type LineProtocolHandler struct {
	buffer *ingest.ArrowBuffer
	parser *ingest.LineProtocolParser
	logger zerolog.Logger

	// RBAC support
	authManager AuthManager
	rbacManager RBACChecker

	// Cluster routing support
	router *cluster.Router

	// Stats
	totalRequests     atomic.Int64
	totalRecords      atomic.Int64
	totalBytes        atomic.Int64
	totalBytesGzipped atomic.Int64
	totalErrors       atomic.Int64
}

// Measurement name validation - must start with letter, contain only alphanumeric, underscore, or hyphen
// This prevents XML-illegal control characters from corrupting S3 ListObjectsV2 responses
var validMeasurementName = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9_-]*$`)

// isValidMeasurementName validates a measurement name
func isValidMeasurementName(name string) bool {
	if len(name) == 0 || len(name) > 128 {
		return false
	}
	return validMeasurementName.MatchString(name)
}

// NewLineProtocolHandler creates a new Line Protocol handler
func NewLineProtocolHandler(buffer *ingest.ArrowBuffer, logger zerolog.Logger) *LineProtocolHandler {
	return &LineProtocolHandler{
		buffer: buffer,
		parser: ingest.NewLineProtocolParser(),
		logger: logger.With().Str("component", "lineprotocol-handler").Logger(),
	}
}

// SetAuthAndRBAC sets the auth and RBAC managers for permission checking
func (h *LineProtocolHandler) SetAuthAndRBAC(authManager AuthManager, rbacManager RBACChecker) {
	h.authManager = authManager
	h.rbacManager = rbacManager
}

// SetRouter sets the cluster router for request forwarding.
// When set, write requests from reader nodes will be forwarded to writer nodes.
func (h *LineProtocolHandler) SetRouter(router *cluster.Router) {
	h.router = router
}

// checkWritePermissions checks if the token has write permission for the database and measurements
func (h *LineProtocolHandler) checkWritePermissions(c *fiber.Ctx, database string, measurements []string) error {
	return CheckWritePermissions(c, h.rbacManager, h.logger, database, measurements)
}

// RegisterRoutes registers Line Protocol routes
func (h *LineProtocolHandler) RegisterRoutes(app *fiber.App) {
	// InfluxDB 1.x compatible endpoint (matches InfluxDB API)
	app.Post("/write", h.WriteV1)

	// InfluxDB 2.x compatible endpoint (matches InfluxDB API)
	app.Post("/api/v2/write", h.WriteInfluxDB)

	// Arc native endpoint (no query parameters, uses x-arc-database header)
	app.Post("/api/v1/write/line-protocol", h.WriteSimple)

	// Stats endpoint
	app.Get("/api/v1/write/line-protocol/stats", h.Stats)

	// Health endpoint
	app.Get("/api/v1/write/line-protocol/health", h.Health)

	// Flush endpoint
	app.Post("/api/v1/write/line-protocol/flush", h.Flush)

	h.logger.Info().Msg("Line Protocol routes registered")
}

// WriteV1 handles InfluxDB 1.x compatible write requests
// POST /write?db=telegraf&rp=default&precision=ns
func (h *LineProtocolHandler) WriteV1(c *fiber.Ctx) error {
	h.totalRequests.Add(1)

	// Get query parameters
	db := c.Query("db", "default")
	// rp (retention policy) is ignored for compatibility
	// precision is ignored - we always expect nanoseconds and convert to microseconds

	// Get database from header if specified
	if headerDB := c.Get("x-arc-database"); headerDB != "" {
		db = headerDB
	}

	return h.handleWrite(c, db)
}

// WriteInfluxDB handles InfluxDB 2.x compatible write requests
// POST /api/v2/write?org=myorg&bucket=mybucket&precision=ns
func (h *LineProtocolHandler) WriteInfluxDB(c *fiber.Ctx) error {
	h.totalRequests.Add(1)

	// Get query parameters
	bucket := c.Query("bucket", "default")
	// org is ignored for now
	// precision is ignored - we always expect nanoseconds and convert to microseconds

	// Get database from header if specified (takes precedence)
	db := bucket
	if headerDB := c.Get("x-arc-database"); headerDB != "" {
		db = headerDB
	}

	return h.handleWrite(c, db)
}

// WriteSimple handles simple write requests without query parameters
// POST /api/v1/write/line-protocol
func (h *LineProtocolHandler) WriteSimple(c *fiber.Ctx) error {
	h.totalRequests.Add(1)

	// Get database from header
	db := c.Get("x-arc-database", "default")

	return h.handleWrite(c, db)
}

// handleWrite processes Line Protocol data and writes to the Arrow buffer
func (h *LineProtocolHandler) handleWrite(c *fiber.Ctx, database string) error {
	// Check if this request should be forwarded to a writer node
	// Reader nodes cannot process writes locally, so they forward to writers
	if h.router != nil && ShouldForwardWrite(h.router, c) {
		h.logger.Debug().Msg("Forwarding Line Protocol write request to writer node")

		httpReq, err := BuildHTTPRequest(c)
		if err != nil {
			h.logger.Error().Err(err).Msg("Failed to build HTTP request for forwarding")
			h.totalErrors.Add(1)
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"error": "Failed to prepare request for forwarding",
			})
		}

		resp, err := h.router.RouteWrite(c.Context(), httpReq)
		if err == cluster.ErrLocalNodeCanHandle {
			// Fall through to local processing
			goto localProcessing
		}
		if err != nil {
			h.logger.Error().Err(err).Msg("Failed to route write request")
			h.totalErrors.Add(1)
			return HandleRoutingError(c, err)
		}

		return CopyResponse(c, resp)
	}

localProcessing:
	// Get request body
	body := c.Body()
	originalSize := len(body)
	h.totalBytes.Add(int64(originalSize))

	// Check for gzip compression (magic number: 0x1f 0x8b)
	// Uses pooled gzip.Reader to avoid ~32KB allocation per request
	if len(body) >= 2 && body[0] == 0x1f && body[1] == 0x8b {
		h.totalBytesGzipped.Add(int64(originalSize))

		decompressed, err := h.decompressGzip(body)
		if err != nil {
			h.totalErrors.Add(1)
			h.logger.Error().Err(err).Msg("Failed to decompress gzip data")
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": "Failed to decompress gzip data: " + err.Error(),
			})
		}

		body = decompressed
	}

	// Check for empty body
	if len(body) == 0 {
		h.totalErrors.Add(1)
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "Empty request body",
		})
	}

	// Parse line protocol
	records := h.parser.ParseBatch(body)

	if len(records) == 0 {
		h.totalErrors.Add(1)
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "No valid records in request",
		})
	}

	h.totalRecords.Add(int64(len(records)))

	// Convert to columnar format grouped by measurement
	columnarByMeasurement := ingest.BatchToColumnar(records)

	// Check RBAC permissions for all measurements being written (only if RBAC is enabled)
	if h.rbacManager != nil && h.rbacManager.IsRBACEnabled() {
		measurements := make([]string, 0, len(columnarByMeasurement))
		for measurement := range columnarByMeasurement {
			measurements = append(measurements, measurement)
		}
		if err := h.checkWritePermissions(c, database, measurements); err != nil {
			h.totalErrors.Add(1)
			return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
				"error": err.Error(),
			})
		}
	}

	// Validate measurement names (prevent control chars that break S3 XML responses)
	for measurement := range columnarByMeasurement {
		if !isValidMeasurementName(measurement) {
			h.totalErrors.Add(1)
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": fmt.Sprintf("invalid measurement name %q: must start with a letter and contain only alphanumeric characters, underscores, or hyphens", measurement),
			})
		}
	}

	// Write each measurement to the buffer
	for measurement, columns := range columnarByMeasurement {
		err := h.buffer.WriteColumnarDirect(c.Context(), database, measurement, columns)
		if err != nil {
			h.totalErrors.Add(1)
			metrics.Get().IncIngestErrors()
			h.logger.Error().
				Err(err).
				Str("database", database).
				Str("measurement", measurement).
				Int("records", len(columns["time"])).
				Msg("Failed to write to Arrow buffer")
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"error": "Write failed: " + err.Error(),
			})
		}
	}

	// Record global metrics
	m := metrics.Get()
	m.IncLineProtocolRequests()
	m.IncLineProtocolRecords(int64(len(records)))
	m.IncLineProtocolBytes(int64(len(body)))
	m.IncIngestRecords(int64(len(records)))
	m.IncIngestBytes(int64(len(body)))
	m.IncIngestBatches()

	// InfluxDB returns 204 No Content on success
	return c.SendStatus(fiber.StatusNoContent)
}

// Stats returns Line Protocol handler statistics
func (h *LineProtocolHandler) Stats(c *fiber.Ctx) error {
	return c.JSON(fiber.Map{
		"status": "success",
		"stats": fiber.Map{
			"total_requests":      h.totalRequests.Load(),
			"total_records":       h.totalRecords.Load(),
			"total_bytes":         h.totalBytes.Load(),
			"total_bytes_gzipped": h.totalBytesGzipped.Load(),
			"total_errors":        h.totalErrors.Load(),
		},
	})
}

// Health returns health status
func (h *LineProtocolHandler) Health(c *fiber.Ctx) error {
	return c.JSON(fiber.Map{
		"status":  "healthy",
		"service": "line_protocol_writer",
	})
}

// Flush triggers a buffer flush
func (h *LineProtocolHandler) Flush(c *fiber.Ctx) error {
	err := h.buffer.FlushAll(c.Context())
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "Flush failed: " + err.Error(),
		})
	}

	return c.JSON(fiber.Map{
		"status":  "success",
		"message": "All buffers flushed",
	})
}

// GetStats returns stats as a map (for programmatic access)
func (h *LineProtocolHandler) GetStats() map[string]int64 {
	return map[string]int64{
		"total_requests":      h.totalRequests.Load(),
		"total_records":       h.totalRecords.Load(),
		"total_bytes":         h.totalBytes.Load(),
		"total_bytes_gzipped": h.totalBytesGzipped.Load(),
		"total_errors":        h.totalErrors.Load(),
	}
}

// decompressGzip decompresses gzip data using pooled readers to minimize allocations
// klauspost gzip.Reader has ~32KB internal state that can be reused via Reset()
func (h *LineProtocolHandler) decompressGzip(data []byte) ([]byte, error) {
	const maxDecompressedSize = 100 * 1024 * 1024 // 100MB

	// Get pooled gzip reader or create new one
	var reader *gzip.Reader
	var err error
	if pooled := lpGzipReaderPool.Get(); pooled != nil {
		reader = pooled.(*gzip.Reader)
		err = reader.Reset(bytes.NewReader(data))
	} else {
		reader, err = gzip.NewReader(bytes.NewReader(data))
	}
	if err != nil {
		if reader != nil {
			lpGzipReaderPool.Put(reader)
		}
		return nil, err
	}

	// Read with size limit
	result, err := io.ReadAll(io.LimitReader(reader, maxDecompressedSize+1))
	if err != nil {
		lpGzipReaderPool.Put(reader)
		return nil, err
	}

	// Check size limit
	if len(result) > maxDecompressedSize {
		lpGzipReaderPool.Put(reader)
		return nil, fmt.Errorf("decompressed payload exceeds 100MB limit")
	}

	// Return reader to pool for reuse
	lpGzipReaderPool.Put(reader)

	return result, nil
}
