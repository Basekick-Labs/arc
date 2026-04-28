package api

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"sync/atomic"

	"github.com/basekick-labs/arc/internal/auth"
	"github.com/basekick-labs/arc/internal/cluster"
	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/gofiber/fiber/v2"
	"github.com/klauspost/compress/gzip"
	"github.com/klauspost/compress/zstd"
	"github.com/rs/zerolog"
)

// TLEHandler handles streaming TLE (Two-Line Element) write requests.
// TLE data is parsed into the satellite_tle measurement with orbital elements
// as fields and satellite identifiers as tags.
type TLEHandler struct {
	buffer *ingest.ArrowBuffer
	parser *ingest.TLEParser
	logger zerolog.Logger

	// authManager holds the concrete *auth.AuthManager. See
	// MsgPackHandler.authManager for the full rationale.
	authManager *auth.AuthManager
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

// SetAuthAndRBAC sets the auth and RBAC managers. See
// MsgPackHandler.SetAuthAndRBAC for the full rationale.
func (h *TLEHandler) SetAuthAndRBAC(authManager *auth.AuthManager, rbacManager RBACChecker) {
	h.authManager = authManager
	h.rbacManager = rbacManager
}

// SetRouter sets the cluster router for request forwarding.
// When set, write requests from reader nodes will be forwarded to writer nodes.
func (h *TLEHandler) SetRouter(router *cluster.Router) {
	h.router = router
}

// RegisterRoutes registers TLE routes. The write endpoint is gated by
// auth.RequireWrite when auth is enabled — per CLAUDE.md, mutating
// endpoints MUST have an explicit write-tier auth middleware.
func (h *TLEHandler) RegisterRoutes(app *fiber.App) {
	app.Post("/api/v1/write/tle", withWriteAuth(h.authManager), h.handleWrite)
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
	// CRITICAL: use c.Request().Body() (raw) instead of c.Body() to avoid
	// fasthttp's transparent gunzip. fasthttp's BodyGunzip path is
	// uncapped — a 1MB gzip payload that decompresses to 50GB would OOM
	// the process before our handler-level size check fires. The
	// msgpack handler uses the same pattern; we now mirror it for TLE.
	//
	// Defensive copy lives only in the uncompressed branch — see
	// lineprotocol.go for full rationale. The decompression branches
	// already return fresh slices, so doubling the memory by copying
	// the compressed input first is wasteful for large TLE catalog
	// refreshes.
	rawBody := c.Request().Body()
	originalSize := len(rawBody)
	h.totalBytes.Add(int64(originalSize))

	var body []byte
	switch {
	case len(rawBody) >= 2 && rawBody[0] == 0x1f && rawBody[1] == 0x8b:
		// gzip — pooled, hard decompressed-size cap (100MB).
		decompressed, err := decompressGzipPooled(rawBody, 0)
		if err != nil {
			h.totalErrors.Add(1)
			h.logger.Error().Err(err).Msg("Failed to decompress gzip data")
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": "Failed to decompress gzip data: " + err.Error(),
			})
		}
		body = decompressed

	case len(rawBody) >= 4 && rawBody[0] == 0x28 && rawBody[1] == 0xB5 && rawBody[2] == 0x2F && rawBody[3] == 0xFD:
		// zstd — same 100MB cap, mirrors LP and msgpack handlers.
		decompressed, err := decompressZstdPooled(rawBody, 0)
		if err != nil {
			h.totalErrors.Add(1)
			h.logger.Error().Err(err).Msg("Failed to decompress zstd data")
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": "Failed to decompress zstd data: " + err.Error(),
			})
		}
		body = decompressed

	default:
		// Uncompressed — defensive copy out of fasthttp-owned buffer.
		body = append([]byte(nil), rawBody...)
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
		// See lineprotocol.go for ErrSchemaChurnExceeded → 503 rationale.
		if errors.Is(err, ingest.ErrSchemaChurnExceeded) {
			return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
				"error": "Write rejected (schema churn): " + err.Error(),
			})
		}
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
// maxSize limits the decompressed output; pass 0 for the default 100MB limit.
func decompressGzipPooled(data []byte, maxSize int) ([]byte, error) {
	if maxSize <= 0 {
		maxSize = 100 * 1024 * 1024 // 100MB default
	}

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

	result, err := io.ReadAll(io.LimitReader(reader, int64(maxSize)+1))
	if err != nil {
		lpGzipReaderPool.Put(reader)
		return nil, err
	}

	if len(result) > maxSize {
		lpGzipReaderPool.Put(reader)
		return nil, fmt.Errorf("decompressed payload exceeds %dMB limit", maxSize/(1024*1024))
	}

	lpGzipReaderPool.Put(reader)
	return result, nil
}

// decompressZstdPooled decompresses zstd data using the package-level
// zstdDecoderPool (declared in msgpack.go). Returns a freshly-copied
// []byte (not a PooledBuffer) so LP and TLE callers can keep their
// existing flat-byte handler shape — the decode buffer itself is
// returned to decompressBufferPool inside this function.
//
// maxSize limits the decompressed output; pass 0 for the default 100MB.
// Mirrors the gzip helper above and the msgpack zstd path.
func decompressZstdPooled(data []byte, maxSize int) ([]byte, error) {
	if maxSize <= 0 {
		maxSize = 100 * 1024 * 1024 // 100MB default
	}

	var decoder *zstd.Decoder
	var err error
	if pooled := zstdDecoderPool.Get(); pooled != nil {
		decoder = pooled.(*zstd.Decoder)
	} else {
		decoder, err = zstd.NewReader(nil, zstd.WithDecoderMaxMemory(uint64(maxSize)+1024))
		if err != nil {
			return nil, fmt.Errorf("failed to create zstd decoder: %w", err)
		}
	}

	bufPtr := decompressBufferPool.Get().(*[]byte)
	buf := (*bufPtr)[:0]

	buf, err = decoder.DecodeAll(data, buf)
	if err != nil {
		zstdDecoderPool.Put(decoder)
		*bufPtr = (*bufPtr)[:0]
		decompressBufferPool.Put(bufPtr)
		return nil, fmt.Errorf("failed to decompress zstd: %w", err)
	}

	zstdDecoderPool.Put(decoder)

	if len(buf) > maxSize {
		*bufPtr = (*bufPtr)[:0]
		decompressBufferPool.Put(bufPtr)
		return nil, fmt.Errorf("decompressed payload exceeds %dMB limit", maxSize/(1024*1024))
	}

	// Copy out so the pooled buffer can be returned immediately — LP/TLE
	// callers operate on the result for the rest of the request and don't
	// follow the PooledBuffer.Release contract used by msgpack.
	out := make([]byte, len(buf))
	copy(out, buf)

	if cap(buf) > maxPooledBufferSize {
		// Don't pollute the pool with oversized buffers; mirrors msgpack.
		decompBufferDiscards.Add(1)
		metrics.Get().IncDecompBufferDiscards()
		freshBuf := make([]byte, 0, initialBufferSize)
		*bufPtr = freshBuf
	} else {
		*bufPtr = buf[:0]
	}
	decompressBufferPool.Put(bufPtr)

	return out, nil
}
