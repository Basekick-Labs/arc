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
	// Shared decompressRequest helper (below) handles gzip/zstd/
	// uncompressed dispatch with a hard 100MB output cap, the streaming
	// LimitReader-bounded zstd path, and the defensive copy of fasthttp-
	// owned bytes on the uncompressed branch. See lineprotocol.go for
	// the security rationale of each invariant.
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

// decompressRequest dispatches on the compression magic bytes at the
// head of rawBody and returns a fresh, hard-capped []byte plus a
// short codec label ("gzip", "zstd", or "" for uncompressed) suitable
// for structured logging.
//
// All three branches return slices that are safe to retain past
// handler return — the gzip and zstd helpers allocate fresh, and the
// uncompressed branch makes a defensive copy out of the fasthttp-
// owned rawBody (see lineprotocol.go for the silent-aliasing
// rationale).
//
// maxSize limits the decompressed output; pass 0 for the default
// 100MB. err carries the wrapped underlying decoder error so handlers
// can include it in the JSON response for client diagnosis.
func decompressRequest(rawBody []byte, maxSize int) (body []byte, codec string, err error) {
	switch {
	case len(rawBody) >= 2 && rawBody[0] == 0x1f && rawBody[1] == 0x8b:
		body, err = decompressGzipPooled(rawBody, maxSize)
		return body, "gzip", err
	case len(rawBody) >= 4 && rawBody[0] == 0x28 && rawBody[1] == 0xB5 && rawBody[2] == 0x2F && rawBody[3] == 0xFD:
		body, err = decompressZstdPooled(rawBody, maxSize)
		return body, "zstd", err
	default:
		// Uncompressed — defensive copy out of fasthttp-owned buffer.
		out := append([]byte(nil), rawBody...)
		return out, "", nil
	}
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
		return nil, fmt.Errorf("decompressed payload exceeds %s limit", formatBytes(int64(maxSize)))
	}

	lpGzipReaderPool.Put(reader)
	return result, nil
}

// decompressZstdPooled decompresses zstd data using the package-level
// zstdDecoderPool (declared in msgpack.go). Returns a freshly-copied
// []byte so LP and TLE callers can keep their flat-byte handler shape.
//
// maxSize is a hard ceiling on the *decompressed output*; pass 0 for
// the default 100MB. Critical: the bound is enforced by reading the
// decoder via io.LimitReader, NOT by checking len after DecodeAll.
// zstd.WithDecoderMaxMemory only bounds the decoder's per-frame
// window state, not the output buffer growth — DecodeAll on a high-
// ratio bomb (e.g. 100KB → 100GB) would OOM the process before any
// post-hoc length check could fire. The streaming Reset+LimitReader+
// ReadAll pattern matches the gzip helper above and is the same
// shape klauspost recommends for untrusted input.
//
// Reads up to maxSize+1 bytes; if exactly maxSize+1 bytes were read,
// the input was over-limit and we return an error (a single read of
// maxSize+1 is still bounded so the bomb cannot OOM).
func decompressZstdPooled(data []byte, maxSize int) ([]byte, error) {
	if maxSize <= 0 {
		maxSize = 100 * 1024 * 1024 // 100MB default
	}

	var decoder *zstd.Decoder
	var err error
	if pooled := zstdDecoderPool.Get(); pooled != nil {
		decoder = pooled.(*zstd.Decoder)
		if err = decoder.Reset(bytes.NewReader(data)); err != nil {
			// Pool entry's internal state is now poisoned — drop it.
			decoder.Close()
			return nil, fmt.Errorf("failed to reset zstd decoder: %w", err)
		}
	} else {
		decoder, err = zstd.NewReader(bytes.NewReader(data), zstd.WithDecoderMaxMemory(uint64(maxSize)+1024))
		if err != nil {
			return nil, fmt.Errorf("failed to create zstd decoder: %w", err)
		}
	}

	// Bounded read: io.LimitReader caps the bytes we will pull from the
	// decoder regardless of compression ratio. Reading maxSize+1 lets us
	// distinguish "exactly at limit" from "over limit" with a single
	// allocation — no unbounded buffer growth is possible.
	limited := io.LimitReader(decoder, int64(maxSize)+1)
	out, err := io.ReadAll(limited)
	if err != nil {
		// Drain any pending state so the decoder can be safely reused
		// by a future request. Reset(nil) is the documented release.
		_ = decoder.Reset(nil)
		zstdDecoderPool.Put(decoder)
		return nil, fmt.Errorf("failed to decompress zstd: %w", err)
	}

	// Release the decoder's internal buffers (it may be holding the
	// last decoded frame) before returning to the pool.
	_ = decoder.Reset(nil)
	zstdDecoderPool.Put(decoder)

	if len(out) > maxSize {
		return nil, fmt.Errorf("decompressed payload exceeds %s limit", formatBytes(int64(maxSize)))
	}

	return out, nil
}
