package api

import (
	"errors"
	"fmt"
	"regexp"
	"sync"
	"sync/atomic"

	"github.com/basekick-labs/arc/internal/auth"
	"github.com/basekick-labs/arc/internal/cluster"
	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/gofiber/fiber/v2"
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
	// Shared decompressRequest (in tle.go) handles gzip/zstd/uncompressed
	// dispatch with a hard 100MB output cap. CRITICAL invariants kept by
	// that helper:
	//   1. fasthttp's transparent gunzip is bypassed — using rawBody
	//      avoids the uncapped BodyGunzip path that would OOM on a
	//      bomb (1MB compressed → 50GB decompressed).
	//   2. zstd path uses streaming Reset+LimitReader, not DecodeAll —
	//      WithDecoderMaxMemory only bounds decoder window state, not
	//      output buffer growth.
	//   3. Uncompressed branch defensively copies out of the fasthttp-
	//      owned buffer so retained sub-slices stay valid post-handler.
	rawBody := c.Request().Body()
	originalSize := len(rawBody)
	h.totalBytes.Add(int64(originalSize))

	body, codec, err := decompressRequest(rawBody, 0)
	if err != nil {
		h.totalErrors.Add(1)
		h.logger.Error().Err(err).Str("codec", codec).Msg("Failed to decompress request body")
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "Failed to decompress " + codec + " data: " + err.Error(),
		})
	}
	if codec != "" {
		h.totalBytesGzipped.Add(int64(originalSize))
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

// decompressGzip was the LP-specific gzip decoder. It duplicated the
// package-level decompressGzipPooled helper in tle.go (same pool,
// same shape) — gemini round 4 finding. The method is now removed
// and all call sites go through decompressRequest, which delegates
// to decompressGzipPooled. Keeping this comment as a tombstone so
// future code review sees the decision rather than wondering why LP
// has its own pooled-gzip helper next door (it doesn't anymore).
