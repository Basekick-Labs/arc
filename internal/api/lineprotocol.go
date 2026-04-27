package api

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"regexp"
	"sync"
	"sync/atomic"

	"github.com/basekick-labs/arc/internal/auth"
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

	// authManager holds the concrete *auth.AuthManager so route-level
	// auth.RequireWrite / RequireAdmin can be wired without the
	// silent-miss type-assert pattern. See MsgPackHandler.authManager
	// for full rationale. nil = OSS no-auth.
	authManager *auth.AuthManager
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

// SetAuthAndRBAC sets the auth and RBAC managers. See
// MsgPackHandler.SetAuthAndRBAC for the full rationale.
func (h *LineProtocolHandler) SetAuthAndRBAC(authManager *auth.AuthManager, rbacManager RBACChecker) {
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

// RegisterRoutes registers Line Protocol routes. All write endpoints
// are gated by auth.RequireWrite when auth is enabled; the /flush
// endpoint is admin-gated because it forces a global flush across
// all shards (operationally heavy + spammable). Per CLAUDE.md,
// mutating endpoints MUST have an explicit auth middleware.
func (h *LineProtocolHandler) RegisterRoutes(app *fiber.App) {
	writeAuth := withWriteAuth(h.authManager)
	adminAuth := withAdminAuth(h.authManager)

	// InfluxDB 1.x compatible endpoint
	app.Post("/write", writeAuth, h.WriteV1)
	// InfluxDB 2.x compatible endpoint
	app.Post("/api/v2/write", writeAuth, h.WriteInfluxDB)
	// Arc native endpoint (uses x-arc-database header)
	app.Post("/api/v1/write/line-protocol", writeAuth, h.WriteSimple)
	// Flush endpoint — admin-gated.
	app.Post("/api/v1/write/line-protocol/flush", adminAuth, h.Flush)

	// Stats and health remain at any-authenticated-token level.
	app.Get("/api/v1/write/line-protocol/stats", h.Stats)
	app.Get("/api/v1/write/line-protocol/health", h.Health)

	h.logger.Info().Msg("Line Protocol routes registered")
}

// WriteV1 handles InfluxDB 1.x compatible write requests
// POST /write?db=telegraf&rp=default&precision=ns
func (h *LineProtocolHandler) WriteV1(c *fiber.Ctx) error {
	h.totalRequests.Add(1)

	db := c.Query("db", "default")
	precision := c.Query("precision", "ns")
	// rp (retention policy) is ignored for compatibility

	// Get database from header if specified
	if headerDB := c.Get("x-arc-database"); headerDB != "" {
		db = headerDB
	}

	return h.handleWrite(c, db, precision)
}

// WriteInfluxDB handles InfluxDB 2.x compatible write requests
// POST /api/v2/write?org=myorg&bucket=mybucket&precision=ns
func (h *LineProtocolHandler) WriteInfluxDB(c *fiber.Ctx) error {
	h.totalRequests.Add(1)

	bucket := c.Query("bucket", "default")
	precision := c.Query("precision", "ns")
	// org is ignored for now

	// Get database from header if specified (takes precedence)
	db := bucket
	if headerDB := c.Get("x-arc-database"); headerDB != "" {
		db = headerDB
	}

	return h.handleWrite(c, db, precision)
}

// WriteSimple handles simple write requests without query parameters
// POST /api/v1/write/line-protocol
func (h *LineProtocolHandler) WriteSimple(c *fiber.Ctx) error {
	h.totalRequests.Add(1)

	db := c.Get("x-arc-database", "default")
	precision := c.Query("precision", "ns")

	return h.handleWrite(c, db, precision)
}

// handleWrite processes Line Protocol data and writes to the Arrow buffer.
// precision controls timestamp interpretation: "ns" (default), "us", "ms", "s".
func (h *LineProtocolHandler) handleWrite(c *fiber.Ctx, database string, precision string) error {
	// Validate database name to prevent path traversal
	if !isValidDatabaseName(database) {
		h.totalErrors.Add(1)
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "invalid database name: must start with a letter and contain only alphanumeric characters, underscores, or hyphens (max 64 characters)",
		})
	}

	// Validate precision parameter
	switch precision {
	case "ns", "us", "ms", "s":
		// valid
	default:
		h.totalErrors.Add(1)
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": fmt.Sprintf("invalid precision %q: must be ns, us, ms, or s", precision),
		})
	}

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
	// CRITICAL: use c.Request().Body() (raw) instead of c.Body() to avoid
	// fasthttp's transparent gunzip. fasthttp's BodyGunzip path is
	// uncapped — a 1MB gzip payload that decompresses to 50GB would OOM
	// the process before our handler-level size check fires. The
	// msgpack handler uses the same pattern; we now mirror it for LP.
	//
	// Defensive copy of the raw fasthttp body: c.Request().Body() returns
	// a slice owned by fasthttp and reused after the handler returns.
	// The current parser path produces fresh strings (no aliasing into
	// the byte slice), so today's behavior is safe — but documenting that
	// "no future change may retain a sub-slice of body" is brittle. One
	// memcpy at the boundary is dwarfed by per-record allocations and
	// closes the silent-aliasing footgun gemini flagged on PR #413.
	// The decompression branches below already return fresh slices.
	body := append([]byte(nil), c.Request().Body()...)
	originalSize := len(body)
	h.totalBytes.Add(int64(originalSize))

	// Check for gzip compression (magic number: 0x1f 0x8b).
	// decompressGzip applies a hard 100MB decompressed cap so an
	// adversarial gzip bomb is rejected before allocation.
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
	} else if len(body) >= 4 && body[0] == 0x28 && body[1] == 0xB5 && body[2] == 0x2F && body[3] == 0xFD {
		// zstd magic number 0x28 0xB5 0x2F 0xFD — same 100MB cap as gzip,
		// pooled decoder reused via the package-level zstdDecoderPool.
		h.totalBytesGzipped.Add(int64(originalSize))

		decompressed, err := decompressZstdPooled(body, 0)
		if err != nil {
			h.totalErrors.Add(1)
			h.logger.Error().Err(err).Msg("Failed to decompress zstd data")
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": "Failed to decompress zstd data: " + err.Error(),
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

	// Parse line protocol with timestamp precision
	records := h.parser.ParseBatchWithPrecision(body, precision)

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
	for measurement, record := range columnarByMeasurement {
		err := h.buffer.WriteColumnarRecord(c.Context(), database, record)
		if err != nil {
			h.totalErrors.Add(1)
			metrics.Get().IncIngestErrors()
			h.logger.Error().
				Err(err).
				Str("database", database).
				Str("measurement", measurement).
				Int("records", len(record.Columns["time"])).
				Msg("Failed to write to Arrow buffer")
			// Schema-churn rejection is retryable — surface as 503 so
			// upstream senders back off rather than treating this as a
			// permanent server error. See ingest.ErrSchemaChurnExceeded.
			if errors.Is(err, ingest.ErrSchemaChurnExceeded) {
				return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
					"error": "Write rejected (schema churn): " + err.Error(),
				})
			}
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
