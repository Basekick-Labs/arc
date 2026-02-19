package api

import (
	"bytes"
	"fmt"
	"io"
	"sync/atomic"

	"github.com/basekick-labs/arc/internal/cluster"
	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/gofiber/fiber/v2"
	"github.com/klauspost/compress/gzip"
	"github.com/rs/zerolog"
)

// TLEHandler handles streaming TLE (Two-Line Element) write requests.
// TLE data is parsed into the satellite_tle measurement with orbital elements
// as fields and satellite identifiers as tags.
type TLEHandler struct {
	buffer *ingest.ArrowBuffer
	parser *ingest.TLEParser
	logger zerolog.Logger

	// RBAC support
	authManager AuthManager
	rbacManager RBACChecker

	// Cluster routing support
	router *cluster.Router

	// Stats
	totalRequests atomic.Int64
	totalRecords  atomic.Int64
	totalBytes    atomic.Int64
	totalErrors   atomic.Int64
}

// NewTLEHandler creates a new TLE handler
func NewTLEHandler(buffer *ingest.ArrowBuffer, logger zerolog.Logger) *TLEHandler {
	return &TLEHandler{
		buffer: buffer,
		parser: ingest.NewTLEParser(),
		logger: logger.With().Str("component", "tle-handler").Logger(),
	}
}

// SetAuthAndRBAC sets the auth and RBAC managers for permission checking
func (h *TLEHandler) SetAuthAndRBAC(authManager AuthManager, rbacManager RBACChecker) {
	h.authManager = authManager
	h.rbacManager = rbacManager
}

// SetRouter sets the cluster router for request forwarding.
// When set, write requests from reader nodes will be forwarded to writer nodes.
func (h *TLEHandler) SetRouter(router *cluster.Router) {
	h.router = router
}

// RegisterRoutes registers TLE routes
func (h *TLEHandler) RegisterRoutes(app *fiber.App) {
	app.Post("/api/v1/write/tle", h.handleWrite)
	app.Get("/api/v1/write/tle/stats", h.Stats)

	h.logger.Info().Msg("TLE routes registered")
}

// handleWrite processes TLE data and writes to the Arrow buffer
func (h *TLEHandler) handleWrite(c *fiber.Ctx) error {
	h.totalRequests.Add(1)

	database := c.Get("x-arc-database", "default")

	if !isValidDatabaseName(database) {
		h.totalErrors.Add(1)
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "invalid database name: must start with a letter and contain only alphanumeric characters, underscores, or hyphens (max 64 characters)",
		})
	}

	// Check if this request should be forwarded to a writer node
	if h.router != nil && ShouldForwardWrite(h.router, c) {
		h.logger.Debug().Msg("Forwarding TLE write request to writer node")

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
	body := c.Body()
	originalSize := len(body)
	h.totalBytes.Add(int64(originalSize))

	// Check for gzip compression (magic number: 0x1f 0x8b)
	// Reuses the LP gzip reader pool to share pooled readers across handlers
	if len(body) >= 2 && body[0] == 0x1f && body[1] == 0x8b {
		decompressed, err := decompressGzipPooled(body)
		if err != nil {
			h.totalErrors.Add(1)
			h.logger.Error().Err(err).Msg("Failed to decompress gzip data")
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": "Failed to decompress gzip data: " + err.Error(),
			})
		}
		body = decompressed
	}

	if len(body) == 0 {
		h.totalErrors.Add(1)
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "Empty request body",
		})
	}

	// Measurement name from header (default: satellite_tle)
	measurement := c.Get("x-arc-measurement", "satellite_tle")
	if !isValidMeasurementName(measurement) {
		h.totalErrors.Add(1)
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": fmt.Sprintf("invalid measurement name %q: must start with a letter and contain only alphanumeric characters, underscores, or hyphens", measurement),
		})
	}

	// Parse TLE data
	tleRecords, warnings := h.parser.ParseTLEFile(body)
	if len(tleRecords) == 0 {
		h.totalErrors.Add(1)
		errMsg := "No valid TLE records in request"
		if len(warnings) > 0 {
			errMsg = fmt.Sprintf("No valid TLE records in request (warnings: %v)", warnings)
		}
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": errMsg,
		})
	}

	if len(warnings) > 0 {
		h.logger.Warn().Strs("warnings", warnings).Msg("TLE parse warnings")
	}

	// Convert directly to typed columnar format (single pass, pre-allocated, no []interface{})
	batch, numRecords := ingest.TLERecordsToTypedColumnar(tleRecords)
	h.totalRecords.Add(int64(numRecords))

	// Check RBAC permissions
	if h.rbacManager != nil && h.rbacManager.IsRBACEnabled() {
		if err := CheckWritePermissions(c, h.rbacManager, h.logger, database, []string{measurement}); err != nil {
			h.totalErrors.Add(1)
			return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
				"error": err.Error(),
			})
		}
	}

	// Write directly to the buffer (typed path — skips convertColumnsToTyped)
	if err := h.buffer.WriteTypedColumnarDirect(c.Context(), database, measurement, batch, numRecords); err != nil {
		h.totalErrors.Add(1)
		metrics.Get().IncIngestErrors()
		h.logger.Error().
			Err(err).
			Str("database", database).
			Str("measurement", measurement).
			Int("records", len(tleRecords)).
			Msg("Failed to write TLE data to Arrow buffer")
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "Write failed: " + err.Error(),
		})
	}

	// Record global metrics
	m := metrics.Get()
	m.IncIngestRecords(int64(len(tleRecords)))
	m.IncIngestBytes(int64(len(body)))
	m.IncIngestBatches()

	return c.SendStatus(fiber.StatusNoContent)
}

// Stats returns TLE handler statistics
func (h *TLEHandler) Stats(c *fiber.Ctx) error {
	return c.JSON(fiber.Map{
		"status": "success",
		"stats": fiber.Map{
			"total_requests": h.totalRequests.Load(),
			"total_records":  h.totalRecords.Load(),
			"total_bytes":    h.totalBytes.Load(),
			"total_errors":   h.totalErrors.Load(),
		},
	})
}

// decompressGzipPooled decompresses gzip data using pooled readers to minimize allocations.
// Shared across handlers — uses lpGzipReaderPool (declared in lineprotocol.go).
func decompressGzipPooled(data []byte) ([]byte, error) {
	const maxDecompressedSize = 100 * 1024 * 1024 // 100MB

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

	result, err := io.ReadAll(io.LimitReader(reader, int64(maxDecompressedSize)+1))
	if err != nil {
		lpGzipReaderPool.Put(reader)
		return nil, err
	}

	if len(result) > maxDecompressedSize {
		lpGzipReaderPool.Put(reader)
		return nil, fmt.Errorf("decompressed payload exceeds 100MB limit")
	}

	lpGzipReaderPool.Put(reader)
	return result, nil
}
