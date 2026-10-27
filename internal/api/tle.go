package api

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"
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

	// strings.Clone: c.Get is zero-copy (aliases the fasthttp header buffer);
	// this database name is retained past the handler by the async Arrow buffer
	// (WriteTypedColumnarDirect) as the storage-path DB directory, so a
	// concurrent request reusing the buffer could redirect the write. Copy at the
	// boundary. See the full explanation in msgpack.go. Only the request-provided
	// value aliases the buffer; the static "default" fallback does not, so we
	// clone only when the header was actually present.
	database := c.Get("x-arc-database")
	if database == "" {
		database = "default"
	} else {
		database = strings.Clone(database)
	}

	if !isValidDatabaseName(database) {
		h.totalErrors.Add(1)
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "invalid database name: must start with a letter and contain only alphanumeric characters, underscores, or hyphens (max 64 characters)",
		})
	}

	// Check if this request should be forwarded to a writer node
	switch WriteForwardDecision(h.router, c) {
	case ForwardAlreadyForwarded:
		// Already-forwarded marker on a node that cannot ingest locally:
		// a routing loop or a spoofed X-Arc-Forwarded-By header. Return a
		// deterministic error instead of a doomed local write attempt.
		h.totalErrors.Add(1)
		return RespondAlreadyForwarded(c)
	case ForwardToPeer:
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
			if !errors.Is(err, cluster.ErrWriteForwardUnavailable) && !errors.Is(err, cluster.ErrWriteForwardAmbiguous) {
				h.logger.Error().Err(err).Msg("Failed to route write request")
			}
			h.totalErrors.Add(1)
			return respondWriteForwardError(c, err)
		}

		return CopyResponse(c, resp)
	}

localProcessing:
	// A standby writer forwards the write to the primary instead of refusing
	// it, when the cluster names one it can trust (#1270). Before anything is
	// read or written here.
	if handled, err := forwardStandbyWrite(h.router, c); handled {
		return err
	}
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
		h.logger.Error().Err(err).Str("codec", codec).Msg("Failed to accept request body")
		errMsg := "Failed to read request body: " + err.Error()
		if codec != "" {
			errMsg = "Failed to decompress " + codec + " data: " + err.Error()
		}
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": errMsg,
		})
	}

	if len(body) == 0 {
		h.totalErrors.Add(1)
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "Empty request body",
		})
	}

	// Measurement name from header (default: satellite_tle). Clone for the same
	// reason as database above; clone only the request-provided value, not the
	// static default (which does not alias the request buffer).
	measurement := c.Get("x-arc-measurement")
	if measurement == "" {
		measurement = "satellite_tle"
	} else {
		measurement = strings.Clone(measurement)
	}
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
	if h.rbacManager != nil { // not license-gated; see CheckWritePermissions
		if err := CheckWritePermissions(c, h.rbacManager, h.logger, database, []string{measurement}); err != nil {
			h.totalErrors.Add(1)
			return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
				"error": err.Error(),
			})
		}
	}

	// Write directly to the buffer (typed path — skips convertColumnsToTyped)
	// Admit the whole request, its size reserved against the WAL volume's
	// free space (#676); a fence or disk pressure refuses it here.
	writeCtx, release, aerr := h.buffer.AdmitRequest(c.Context(), int64(len(body)))
	if aerr != nil {
		h.totalErrors.Add(1)
		return respondWriteRefused(c, aerr)
	}
	defer release()
	if err := h.buffer.WriteTypedColumnarDirect(writeCtx, database, measurement, batch, numRecords); err != nil {
		h.totalErrors.Add(1)
		// Logged by reason, not as an ingest error (#1268).
		if ingest.IsWriteRefusal(err) {
			return respondWriteRefused(c, err)
		}
		// A client error (#440), refused before the WAL.
		if errors.Is(err, ingest.ErrColumnTypeChange) {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
		}
		metrics.Get().IncIngestErrors()
		h.logger.Error().
			Err(err).
			Str("database", database).
			Str("measurement", measurement).
			Int("records", len(tleRecords)).
			Msg("Failed to write TLE data to Arrow buffer")
		// A write that arrives after its shard was flushed by shutdown is
		// refused, not lost: the record is still in the WAL. 503 so the client
		// retries (elsewhere, during a rolling restart) instead of treating a
		// shutdown race as a permanent server error.
		if errors.Is(err, ingest.ErrBufferClosing) {
			return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
				"error": "Write rejected (server shutting down): " + err.Error(),
			})
		}
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
// maxSize is a hard ceiling on the *post-decompression* size and is
// applied symmetrically to all three branches:
//   - gzip / zstd: enforced inside the streaming decoder via LimitReader
//     so a bomb is rejected before allocation.
//   - uncompressed: enforced before the defensive copy so an attacker
//     cannot force a multi-GB allocation by sending a giant raw body.
//
// Pass 0 for the default 100MB cap. err carries the wrapped underlying
// decoder error so handlers can include it in the JSON response for
// client diagnosis.
func decompressRequest(rawBody []byte, maxSize int) (body []byte, codec string, err error) {
	if maxSize <= 0 {
		maxSize = 100 * 1024 * 1024 // 100MB default — must match the codec helpers
	}
	switch {
	case len(rawBody) >= 2 && rawBody[0] == 0x1f && rawBody[1] == 0x8b:
		body, err = decompressGzipPooled(rawBody, maxSize)
		return body, "gzip", err
	case len(rawBody) >= 4 && rawBody[0] == 0x28 && rawBody[1] == 0xB5 && rawBody[2] == 0x2F && rawBody[3] == 0xFD:
		body, err = decompressZstdPooled(rawBody, maxSize)
		return body, "zstd", err
	default:
		// Uncompressed — enforce the same maxSize cap as the codec
		// branches before making the defensive copy. Without this
		// check, an uncompressed multi-GB body would be both accepted
		// AND copied (the defensive copy is the OOM vector). Symmetric
		// hardening per gemini round 6.
		if len(rawBody) > maxSize {
			return nil, "", fmt.Errorf("payload exceeds %s limit", formatBytes(int64(maxSize)))
		}
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

// zstdMaxWindow bounds the window a zstd frame may declare. The decoder
// allocates the declared window up front, so without it a 10-byte frame
// declaring a 512 MB window allocated 512 MB, and the pooled decoder kept it.
// zstd levels 1-19 use at most an 8 MB window and level 20 32 MB; levels 21
// and 22 (--ultra) and --long mode use larger ones, and a body compressed
// that way is refused (errZstdWindow).
const zstdMaxWindow = 32 << 20

// decompressZstdPooled decompresses an untrusted zstd request body with a
// pooled decoder for maxSize (zstdDecoderPool in msgpack.go), into a fresh
// []byte the caller owns.
//
// maxSize is a hard ceiling on the *decompressed output*; pass 0 for the
// default 100MB. DecodeAll enforces it while decoding: the decoder's
// WithDecoderMaxMemory is its output limit, checked against a frame's
// declared size before allocating and against the output as blocks decode,
// so a bomb (frames without a declared size, or many concatenated frames)
// stops at the limit (TestDecompressZstdBombsStopAtTheLimit).
//
// Everything runs on the caller's goroutine. A streaming decoder with the
// default concurrency decodes on a goroutine of its own, where a decoder
// panic cannot be recovered and ends the process: klauspost/compress
// <= 1.20.1 panics on a crafted frame (GHSA-fpmh-frq8-7rj8), and one request
// crashed Arc. Here a panic is recovered and returned as a decode error, and
// that decoder is dropped. DecodeAll is also faster than the stream was.
func decompressZstdPooled(data []byte, maxSize int) (out []byte, err error) {
	if maxSize <= 0 {
		maxSize = 100 * 1024 * 1024 // 100MB default
	}

	pool := zstdDecoderPool(maxSize)
	var decoder *zstd.Decoder
	if pooled := pool.Get(); pooled != nil {
		decoder = pooled.(*zstd.Decoder)
	} else {
		decoder, err = zstd.NewReader(nil,
			// DecodeAll never starts goroutines; concurrency 1 keeps a
			// streaming decode (Reset/Read) on the caller's goroutine too,
			// should one ever be added.
			zstd.WithDecoderConcurrency(1),
			zstd.WithDecoderMaxMemory(uint64(maxSize)),
			zstd.WithDecoderMaxWindow(zstdMaxWindow))
		if err != nil {
			return nil, fmt.Errorf("failed to create zstd decoder: %w", err)
		}
	}

	defer func() {
		if r := recover(); r != nil {
			// Not returned to the pool: its state is half-built.
			out, err = nil, fmt.Errorf("failed to decompress zstd: corrupt input (%v)", r)
		}
	}()

	out, err = decoder.DecodeAll(data, nil)
	// DecodeAll leaves the decoder reusable after an error (its block
	// decoders go back in a defer), so it returns to the pool either way.
	pool.Put(decoder)
	switch {
	case errors.Is(err, zstd.ErrWindowSizeExceeded):
		return nil, fmt.Errorf("failed to decompress zstd: %w", errZstdWindow)
	case errors.Is(err, zstd.ErrDecoderSizeExceeded):
		return nil, fmt.Errorf("decompressed payload exceeds %s limit", formatBytes(int64(maxSize)))
	case err != nil:
		return nil, fmt.Errorf("failed to decompress zstd: %w", err)
	}
	return out, nil
}

// errZstdWindow names the window limit, so a client compressing with
// --ultra or --long learns which setting to change.
var errZstdWindow = fmt.Errorf("the frame's window is larger than %d MB; compress with level 20 or lower, without --long", zstdMaxWindow>>20)
