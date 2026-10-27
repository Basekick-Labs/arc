package api

import (
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/basekick-labs/arc/internal/auth"
	"github.com/basekick-labs/arc/internal/cluster"
	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/basekick-labs/arc/pkg/models"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// Buffer pool configuration for memory management
const (
	// Initial buffer size for the pool (256KB covers most payloads)
	initialBufferSize = 256 * 1024
)

// NOTE: there is deliberately no oversized-buffer discard guard or counter
// here (#817). The cap constant, the package-local discard counter and its
// exported accessor were left behind when the per-handler pooled codecs were
// replaced by decompressGzipPooled / decompressZstdPooled — see the comment
// above msgPackStats. decompressBufferPool is never Get() from at runtime, so
// no buffer can be discarded and nothing could increment them. If a pooled
// decompression path returns, add the guard and its counter in the same
// change, so the metric never ships ahead of the behaviour it reports.

// Pool for decompression buffers - reduces GC pressure under high load
var decompressBufferPool = sync.Pool{
	New: func() any {
		// Pre-allocate 256KB buffer to cover most decompressed payloads
		// This avoids reallocations during decompression for typical payloads
		buf := make([]byte, 0, initialBufferSize)
		return &buf
	},
}

// Pool for gzip readers - avoids allocating internal decompression state per request
// klauspost gzip.Reader has ~32KB internal state that can be reused via Reset()
var gzipReaderPool = sync.Pool{
	// No New func - we create readers on-demand since gzip.NewReader requires valid data
}

// Pools for zstd decoders, one per size cap: a decoder keeps the
// WithDecoderMaxMemory it was created with, so a decoder made for a smaller
// cap would reject a valid payload of a caller with a larger one, and one
// made for a larger cap would let a smaller caller's limit be exceeded in
// memory. The caps come from configuration, so there are only a few.
var zstdDecoderPools sync.Map // int (cap) -> *sync.Pool

func zstdDecoderPool(maxSize int) *sync.Pool {
	if p, ok := zstdDecoderPools.Load(maxSize); ok {
		return p.(*sync.Pool)
	}
	p, _ := zstdDecoderPools.LoadOrStore(maxSize, &sync.Pool{})
	return p.(*sync.Pool)
}

// PooledBuffer wraps a decompression buffer that must be returned to pool after use
// This enables zero-copy decompression by returning the pooled buffer directly
type PooledBuffer struct {
	Data   []byte  // The decompressed data (slice of pooled buffer)
	bufPtr *[]byte // Pointer to the pooled buffer for returning to pool
}

// Release returns the buffer to the pool - MUST be called after use
// Safe to call multiple times (idempotent)
func (pb *PooledBuffer) Release() {
	if pb.bufPtr != nil {
		*pb.bufPtr = (*pb.bufPtr)[:0] // Reset length, keep capacity
		decompressBufferPool.Put(pb.bufPtr)
		pb.bufPtr = nil
		pb.Data = nil
	}
}

// MsgPackHandler handles MessagePack binary protocol endpoints
type MsgPackHandler struct {
	decoder        *ingest.MessagePackDecoder
	arrowBuffer    *ingest.ArrowBuffer
	logger         zerolog.Logger
	maxPayloadSize int64 // Maximum payload size in bytes (applies to both compressed and decompressed)

	// authManager holds the concrete *auth.AuthManager so we can wire
	// auth.RequireWrite as route-level middleware AND so RBAC checks
	// at request time use the same instance. Concrete (not the local
	// AuthManager interface) because auth.RequireWrite is implemented
	// against the struct; using the interface required a type
	// assertion that could silently miss on a mock and disable route
	// protection.
	authManager *auth.AuthManager
	rbacManager RBACChecker

	// Cluster routing support
	router *cluster.Router
}

// NewMsgPackHandler creates a new MessagePack handler
func NewMsgPackHandler(logger zerolog.Logger, arrowBuffer *ingest.ArrowBuffer, maxPayloadSize int64) *MsgPackHandler {
	decoder := ingest.NewMessagePackDecoder(logger)
	// Typed columnar fast path: enabled unless decimal columns are
	// configured anywhere (decimal conversion is config-driven inside the
	// generic conversion path; see ArrowBuffer.HasDecimalColumns).
	decoder.SetTypedDecodeEnabled(!arrowBuffer.HasDecimalColumns())
	return &MsgPackHandler{
		decoder:        decoder,
		arrowBuffer:    arrowBuffer,
		logger:         logger.With().Str("component", "msgpack-handler").Logger(),
		maxPayloadSize: maxPayloadSize,
	}
}

// SetAuthAndRBAC sets the auth and RBAC managers. Takes the concrete
// *auth.AuthManager (not the local AuthManager interface) so route-
// level auth.RequireWrite middleware can be wired without a type
// assertion that could silently miss on a mock and disable
// protection. nil = auth disabled (OSS default).
func (h *MsgPackHandler) SetAuthAndRBAC(authManager *auth.AuthManager, rbacManager RBACChecker) {
	h.authManager = authManager
	h.rbacManager = rbacManager
}

// SetRouter sets the cluster router for request forwarding.
// When set, write requests from reader nodes will be forwarded to writer nodes.
func (h *MsgPackHandler) SetRouter(router *cluster.Router) {
	h.router = router
}

// extractMeasurements extracts unique measurement names from decoded msgpack records.
//
// An empty name is collected rather than skipped, so the caller's
// isValidMeasurementName check rejects it. Skipping it used to mean a record
// with "m": "" passed validation and reached the writer, which builds
// "{database}//{year}/..." from it. filepath.Join silently collapsed the empty
// segment and the rows landed a directory level up, under the database itself.
// Since #741 the storage backend refuses that key instead, and a refused flush
// keeps its data in the WAL and retries forever, so one such record would wedge
// the buffer permanently.
func (h *MsgPackHandler) extractMeasurements(records interface{}) []string {
	seen := make(map[string]struct{})

	var extract func(v interface{})
	extract = func(v interface{}) {
		switch r := v.(type) {
		case *models.Record:
			seen[r.Measurement] = struct{}{}
		case *models.ColumnarRecord:
			seen[r.Measurement] = struct{}{}
		case *ingest.TypedColumnarRecord:
			// Typed decode fast path — MUST be listed here: a record type
			// missing from this switch silently skips measurement-name
			// validation and RBAC for its writes.
			seen[r.Measurement] = struct{}{}
		case []interface{}:
			for _, item := range r {
				extract(item)
			}
		}
	}

	extract(records)

	measurements := make([]string, 0, len(seen))
	for m := range seen {
		measurements = append(measurements, m)
	}
	return measurements
}

// checkWritePermissions checks if the token has write permission for the database and measurements
func (h *MsgPackHandler) checkWritePermissions(c *fiber.Ctx, database string, measurements []string) error {
	return CheckWritePermissions(c, h.rbacManager, h.logger, database, measurements)
}

// RegisterRoutes registers MessagePack endpoints. The write endpoint
// is gated by auth.RequireWrite when an auth manager is configured —
// per CLAUDE.md, mutating endpoints MUST have an explicit write-tier
// auth middleware. Stats and spec remain at any-authenticated-token
// level (gated by the global auth middleware in main.go).
func (h *MsgPackHandler) RegisterRoutes(app *fiber.App) {
	app.Post("/api/v1/write/msgpack", withWriteAuth(h.authManager), h.writeMsgPack)
	app.Get("/api/v1/write/msgpack/stats", h.msgPackStats)
	app.Get("/api/v1/write/msgpack/spec", h.msgPackSpec)
}

// writeMsgPack handles MessagePack binary write requests
func (h *MsgPackHandler) writeMsgPack(c *fiber.Ctx) error {
	// Check if this request should be forwarded to a writer node
	// Reader nodes cannot process writes locally, so they forward to writers
	switch WriteForwardDecision(h.router, c) {
	case ForwardAlreadyForwarded:
		// Already-forwarded marker on a node that cannot ingest locally:
		// a routing loop or a spoofed X-Arc-Forwarded-By header. Return a
		// deterministic error instead of a doomed local write attempt.
		return RespondAlreadyForwarded(c)
	case ForwardToPeer:
		h.logger.Debug().Msg("Forwarding write request to writer node")

		httpReq, err := BuildHTTPRequest(c)
		if err != nil {
			h.logger.Error().Err(err).Msg("Failed to build HTTP request for forwarding")
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
			if !errors.Is(err, cluster.ErrWriteForwardUnavailable) && !errors.Is(err, cluster.ErrWriteForwardAmbiguous) {
				h.logger.Error().Err(err).Msg("Failed to route write request")
			}
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
	// CRITICAL: Use BodyRaw() instead of Body() to avoid fasthttp's automatic gzip decompression
	// c.Body() triggers tryDecodeBodyInOrder → gunzipData which is NOT pooled and causes high latency.
	// rawBody is owned by fasthttp and reused after this handler returns; the
	// shared decompressRequest helper handles the defensive copy for the
	// uncompressed branch and produces fresh slices on the gzip/zstd paths,
	// so payload below is always safe to retain past handler return.
	rawBody := c.Request().Body()

	// Reject empty bodies up front.
	if len(rawBody) == 0 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "Empty payload",
		})
	}

	// Pre-decompression size check on the *compressed* payload — rejects
	// oversized request bodies before any decode work. The post-
	// decompression cap is applied inside decompressRequest.
	if int64(len(rawBody)) > h.maxPayloadSize {
		return c.Status(fiber.StatusRequestEntityTooLarge).JSON(fiber.Map{
			"error": fmt.Sprintf("Payload too large (max %s). Consider batching into smaller requests.", formatBytes(h.maxPayloadSize)),
		})
	}

	// HOT PATH: msgpack-columnar ingest sustains ~19M rec/s in benchmark,
	// so this dispatch is performance-critical. We do NOT use the shared
	// decompressRequest helper for the uncompressed branch because that
	// helper makes a defensive memcpy of rawBody — at 18M+ rec/s with
	// large batches that costs ~1M rec/s of throughput. The msgpack
	// decoder (Basekick-Labs/msgpack/v6) is *synchronous* and copies
	// every string field via `string(b)` and every []byte field via
	// `io.ReadFull` into a fresh slice — so by the time h.decoder.Decode
	// returns, no decoded value still aliases rawBody. fasthttp can
	// safely reuse the buffer once this handler returns. The compressed
	// branches still go through the shared helpers because those produce
	// fresh allocations regardless and the bomb-mitigation matters.
	//
	// LP and TLE use the helper unmodified — their throughput targets
	// are lower and the consistency wins.
	var (
		payload         []byte
		compressionType string
	)
	switch {
	case len(rawBody) >= 2 && rawBody[0] == 0x1f && rawBody[1] == 0x8b:
		decompressed, derr := decompressGzipPooled(rawBody, int(h.maxPayloadSize))
		if derr != nil {
			h.logger.Error().Err(derr).Msg("Failed to decompress gzip data")
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": fmt.Sprintf("Invalid gzip compression: %v", derr),
			})
		}
		payload = decompressed
		compressionType = "gzip"
	case len(rawBody) >= 4 && rawBody[0] == 0x28 && rawBody[1] == 0xB5 && rawBody[2] == 0x2F && rawBody[3] == 0xFD:
		decompressed, derr := decompressZstdPooled(rawBody, int(h.maxPayloadSize))
		if derr != nil {
			h.logger.Error().Err(derr).Msg("Failed to decompress zstd data")
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": fmt.Sprintf("Invalid zstd compression: %v", derr),
			})
		}
		payload = decompressed
		compressionType = "zstd"
	default:
		// Uncompressed: hand rawBody directly to the synchronous decoder.
		// No defensive copy because Decode finishes before the handler
		// returns. The pre-decompression maxPayloadSize check above
		// already capped the input size.
		payload = rawBody
	}
	if compressionType != "" {
		h.logger.Debug().
			Int("compressed_size", len(rawBody)).
			Int("decompressed_size", len(payload)).
			Str("compression", compressionType).
			Msg("Decompressed payload")
	}

	// Decode MessagePack. Typed row-format decoding drops points it cannot
	// write (line protocol's rules) and reports them; the rest are written.
	// The columns a measurement already has, for refusing a case twin
	// (#1342), for the database the write goes to (the header, as read again
	// and validated below; an invalid name is refused there before any answer
	// that depends on this), and only for measurements the token may write.
	knownDB := c.Get("x-arc-database")
	if knownDB == "" {
		knownDB = "default"
	}
	knownDB = strings.Clone(knownDB)
	known := writableKnownColumns(c, h.rbacManager, knownDB, h.arrowBuffer.KnownColumns(knownDB))
	records, recordCount, drops, err := h.decoder.DecodeWithDropsKnown(payload, known)
	if err != nil {
		h.logger.Error().Err(err).Msg("Failed to decode MessagePack")
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": fmt.Sprintf("Invalid MessagePack payload: %v", err),
		})
	}

	// Get database from header (optional).
	//
	// strings.Clone is REQUIRED, not cosmetic: Fiber's c.Get is zero-copy — it
	// returns a string aliasing the fasthttp request-header buffer, valid only
	// for this handler's lifetime (Fiber runs with Immutable=false). This string
	// is retained past the handler by the async Arrow-buffer flush (it is stored
	// in the flushTask as task.database and consumed later on a background
	// goroutine, where it becomes the top-level storage-path directory). Without
	// a copy, a concurrent request reusing the fasthttp buffer overwrites the
	// bytes, so a write tagged database=X silently lands under a different DB —
	// observed as ~60% of an energy_grid backfill scattering into other demos'
	// databases (names truncated to len("energy_grid")=11), a burst-triggered
	// data-corruption race. Copy at the boundary before it can escape.
	database := strings.Clone(c.Get("x-arc-database"))

	dropped := 0
	for _, d := range drops {
		dropped += d.Points
	}
	recordCount -= dropped // records written

	h.logger.Debug().
		Int("records", recordCount).
		Str("database", database).
		Str("compression", compressionType).
		Msg("Received MessagePack write request")

	// Write to Arrow buffer
	ctx := c.Context()
	if database == "" {
		database = "default"
	}

	// Validate database name to prevent path traversal
	if !isValidDatabaseName(database) {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "invalid database name: must start with a letter and contain only alphanumeric characters, underscores, or hyphens (max 64 characters)",
		})
	}

	// A record whose tag or field names Arc could not read back is dropped,
	// the rest written (#1342): a columnar record whole, since its names are
	// all its rows'. Before the WAL sees any of it.
	var nameDrops []ingest.LPDrop
	var removed int
	records, nameDrops, removed = ingest.FilterRecordNames(records, known)
	recordCount -= removed // in the decoder's unit: rows, or columnar items
	for _, d := range nameDrops {
		drops = append(drops, d)
		dropped += d.Points
	}

	if recordCount <= 0 && dropped > 0 {
		// Every point dropped: nothing to write (as line protocol answers).
		logPointDrops(h.logger, "MessagePack", database, drops)
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": partialWriteMessage(drops, dropped, false)})
	}

	// Extract measurements for validation and RBAC
	measurements := h.extractMeasurements(records)

	// Validate measurement names (prevent control chars that break S3 XML responses)
	for _, measurement := range measurements {
		if !isValidMeasurementName(measurement) {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": fmt.Sprintf("invalid measurement name %q: must start with a letter and contain only alphanumeric characters, underscores, or hyphens", measurement),
			})
		}
	}

	// Check RBAC permissions for all measurements being written (only if RBAC is enabled)
	if h.rbacManager != nil { // not license-gated; see CheckWritePermissions
		if err := h.checkWritePermissions(c, database, measurements); err != nil {
			return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
				"error": err.Error(),
			})
		}
	}

	// Admit the whole request, its size reserved against the WAL volume's
	// free space (#676); a fence or disk pressure refuses it here.
	writeCtx, release, aerr := h.arrowBuffer.AdmitRequest(ctx, int64(len(payload)))
	if aerr != nil {
		return respondWriteRefused(c, aerr)
	}
	defer release()
	err = h.arrowBuffer.Write(writeCtx, database, records)
	var rowDrops *ingest.PointsDroppedError
	if errors.As(err, &rowDrops) {
		// Rows the record path could not write (the others were written):
		// a partial write, like the typed path's drops.
		drops = append(drops, rowDrops.Drops...)
		for _, d := range rowDrops.Drops {
			dropped += d.Points
			recordCount -= d.Points
		}
		err = nil
	}
	if err != nil {
		// A fence refusal is expected during a failover: logged by reason,
		// not counted as an ingest error (#1268).
		if ingest.IsWriteRefusal(err) {
			return respondWriteRefused(c, err)
		}
		// #440: a client error, refused before the WAL; nothing was written.
		if errors.Is(err, ingest.ErrColumnTypeChange) {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
		}
		h.logger.Error().Err(err).Msg("Failed to write to Arrow buffer")
		metrics.Get().IncIngestErrors()
		// Schema-churn rejection is retryable — surface as 503 so
		// upstream senders back off, mirroring the LP and TLE handlers.
		// See ingest.ErrSchemaChurnExceeded.
		// A write that arrives after its shard was flushed by shutdown is
		// refused, not lost: the record is still in the WAL. 503 so the client
		// retries (elsewhere, during a rolling restart) instead of treating a
		// shutdown race as a permanent server error.
		if errors.Is(err, ingest.ErrBufferClosing) {
			return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
				"error": "Write rejected (server shutting down): " + err.Error(),
			})
		}
		if errors.Is(err, ingest.ErrSchemaChurnExceeded) {
			return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
				"error": "Write rejected (schema churn): " + err.Error(),
			})
		}
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "Failed to write records: " + err.Error(),
		})
	}

	// Record metrics
	m := metrics.Get()
	m.IncMsgPackRequests()
	m.IncMsgPackRecords(int64(recordCount))
	m.IncMsgPackBytes(int64(len(payload)))
	m.IncIngestRecords(int64(recordCount))
	m.IncIngestBytes(int64(len(payload)))
	m.IncIngestBatches()

	if dropped > 0 {
		logPointDrops(h.logger, "MessagePack", database, drops)
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": partialWriteMessage(drops, dropped, true)})
	}

	// Return 204 No Content (InfluxDB compatible)
	return c.SendStatus(fiber.StatusNoContent)
}

// MsgPackHandler.decompressGzip and decompressZstd were the per-handler
// pooled-buffer codecs. They are now replaced by the package-level
// decompressGzipPooled / decompressZstdPooled (in tle.go), used through
// the shared decompressRequest helper. This keeps the LP, TLE, and
// msgpack ingest paths on a single dispatch surface and consolidates
// the streaming-LimitReader bomb-mitigation in one place. Removed in
// gemini round 6.
//
// PooledBuffer remains in the package because TestPooledBufferRelease
// still exercises Release() idempotency. The decompressBufferPool /
// gzipReaderPool that PooledBuffer uses are unused at runtime now
// but remain available for future pooled paths.

// msgPackStats returns MessagePack decoder statistics
func (h *MsgPackHandler) msgPackStats(c *fiber.Ctx) error {
	decoderStats := h.decoder.GetStats()
	bufferStats := h.arrowBuffer.GetStats()

	return c.JSON(fiber.Map{
		"decoder": decoderStats,
		"buffer":  bufferStats,
	})
}

// msgPackSpec returns MessagePack protocol specification
func (h *MsgPackHandler) msgPackSpec(c *fiber.Ctx) error {
	spec := fiber.Map{
		"version":      "2.0",
		"protocol":     "MessagePack",
		"endpoint":     "/api/v1/write/msgpack",
		"content_type": "application/msgpack",
		"compression":  "gzip or zstd (optional, zstd recommended for best performance)",
		"authentication": fiber.Map{
			"header": "x-api-key",
			"note":   "Authentication not yet implemented",
		},
		"format": fiber.Map{
			"columnar (RECOMMENDED)": fiber.Map{
				"m":       "measurement (string)",
				"columns": "dict of column_name: [array of values]",
				"note":    "25-35% faster than row format, zero-copy passthrough",
			},
			"row (LEGACY)": fiber.Map{
				"m":      "measurement (string or int)",
				"t":      "timestamp (int64 milliseconds, seconds, or microseconds)",
				"h":      "host (string or int, optional)",
				"fields": "dict of field_name: value",
				"tags":   "dict of tag_name: value (optional)",
			},
			"batch": fiber.Map{
				"batch": "array of measurements (can mix columnar and row)",
			},
		},
		"example_columnar": fiber.Map{
			"m": "cpu",
			"columns": fiber.Map{
				"time":         []int64{1633024800000, 1633024801000, 1633024802000},
				"host":         []string{"server01", "server01", "server01"},
				"region":       []string{"us-east", "us-east", "us-east"},
				"usage_idle":   []float64{95.0, 94.5, 94.2},
				"usage_user":   []float64{3.2, 3.8, 4.1},
				"usage_system": []float64{1.8, 1.7, 1.7},
			},
		},
		"example_row": fiber.Map{
			"m": "cpu",
			"t": 1633024800000,
			"h": "server01",
			"fields": fiber.Map{
				"usage_idle":   95.0,
				"usage_user":   3.2,
				"usage_system": 1.8,
			},
			"tags": fiber.Map{
				"region":     "us-east",
				"datacenter": "aws-1a",
			},
		},
		"performance": fiber.Map{
			"expected_rps_columnar": "2.5M+ (columnar format, zero-copy)",
			"expected_rps_row":      "2.1M (row format, with conversion)",
			"columnar_advantage":    "25-35% faster (no flattening, no row→column conversion)",
			"parsing_speed":         "10-15x faster than text parsing",
			"serialization":         "Direct Arrow (2-3x faster than DataFrame)",
			"payload_size":          "50-70% smaller than Line Protocol",
			"wire_efficiency":       "Columnar sends field names once vs per-record",
		},
	}

	return c.JSON(spec)
}

// formatBytes formats a byte count into a human-readable string (e.g., "1GB", "500MB")
func formatBytes(bytes int64) string {
	const (
		gb = 1024 * 1024 * 1024
		mb = 1024 * 1024
		kb = 1024
	)

	switch {
	case bytes >= gb:
		if bytes%gb == 0 {
			return fmt.Sprintf("%dGB", bytes/gb)
		}
		return fmt.Sprintf("%.1fGB", float64(bytes)/float64(gb))
	case bytes >= mb:
		if bytes%mb == 0 {
			return fmt.Sprintf("%dMB", bytes/mb)
		}
		return fmt.Sprintf("%.1fMB", float64(bytes)/float64(mb))
	case bytes >= kb:
		if bytes%kb == 0 {
			return fmt.Sprintf("%dKB", bytes/kb)
		}
		return fmt.Sprintf("%.1fKB", float64(bytes)/float64(kb))
	default:
		return fmt.Sprintf("%dB", bytes)
	}
}
