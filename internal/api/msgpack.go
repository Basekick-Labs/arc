package api

import (
	"bytes"
	"fmt"
	"io"
	"sync"

	"github.com/basekick-labs/arc/internal/cluster"
	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/basekick-labs/arc/pkg/models"
	"github.com/gofiber/fiber/v2"
	"github.com/klauspost/compress/gzip"
	"github.com/klauspost/compress/zstd"
	"github.com/rs/zerolog"
)

// Pool for decompression buffers - reduces GC pressure under high load
var decompressBufferPool = sync.Pool{
	New: func() interface{} {
		// Pre-allocate 256KB buffer to cover most decompressed payloads
		// This avoids reallocations during decompression for typical payloads
		buf := make([]byte, 0, 256*1024)
		return &buf
	},
}

// Pool for gzip readers - avoids allocating internal decompression state per request
// klauspost gzip.Reader has ~32KB internal state that can be reused via Reset()
var gzipReaderPool = sync.Pool{
	// No New func - we create readers on-demand since gzip.NewReader requires valid data
}

// Pool for zstd decoders - zstd.Decoder is thread-safe and reusable
// klauspost zstd is 3-5x faster than gzip for decompression
var zstdDecoderPool = sync.Pool{
	// No New func - we create decoders on-demand
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

	// RBAC support
	authManager AuthManager
	rbacManager RBACChecker

	// Cluster routing support
	router *cluster.Router
}

// NewMsgPackHandler creates a new MessagePack handler
func NewMsgPackHandler(logger zerolog.Logger, arrowBuffer *ingest.ArrowBuffer, maxPayloadSize int64) *MsgPackHandler {
	return &MsgPackHandler{
		decoder:        ingest.NewMessagePackDecoder(logger),
		arrowBuffer:    arrowBuffer,
		logger:         logger.With().Str("component", "msgpack-handler").Logger(),
		maxPayloadSize: maxPayloadSize,
	}
}

// SetAuthAndRBAC sets the auth and RBAC managers for permission checking
func (h *MsgPackHandler) SetAuthAndRBAC(authManager AuthManager, rbacManager RBACChecker) {
	h.authManager = authManager
	h.rbacManager = rbacManager
}

// SetRouter sets the cluster router for request forwarding.
// When set, write requests from reader nodes will be forwarded to writer nodes.
func (h *MsgPackHandler) SetRouter(router *cluster.Router) {
	h.router = router
}

// extractMeasurements extracts unique measurement names from decoded msgpack records
func (h *MsgPackHandler) extractMeasurements(records interface{}) []string {
	seen := make(map[string]struct{})

	var extract func(v interface{})
	extract = func(v interface{}) {
		switch r := v.(type) {
		case *models.Record:
			if r.Measurement != "" {
				seen[r.Measurement] = struct{}{}
			}
		case *models.ColumnarRecord:
			if r.Measurement != "" {
				seen[r.Measurement] = struct{}{}
			}
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

// RegisterRoutes registers MessagePack endpoints
func (h *MsgPackHandler) RegisterRoutes(app *fiber.App) {
	app.Post("/api/v1/write/msgpack", h.writeMsgPack)
	app.Get("/api/v1/write/msgpack/stats", h.msgPackStats)
	app.Get("/api/v1/write/msgpack/spec", h.msgPackSpec)
}

// writeMsgPack handles MessagePack binary write requests
func (h *MsgPackHandler) writeMsgPack(c *fiber.Ctx) error {
	// Check if this request should be forwarded to a writer node
	// Reader nodes cannot process writes locally, so they forward to writers
	if h.router != nil && ShouldForwardWrite(h.router, c) {
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
			h.logger.Error().Err(err).Msg("Failed to route write request")
			return HandleRoutingError(c, err)
		}

		return CopyResponse(c, resp)
	}

localProcessing:
	// CRITICAL: Use BodyRaw() instead of Body() to avoid fasthttp's automatic gzip decompression
	// c.Body() triggers tryDecodeBodyInOrder → gunzipData which is NOT pooled and causes high latency
	// By using BodyRaw() we handle decompression ourselves with pooled gzip readers
	payload := c.Request().Body()

	// Validate payload size
	if len(payload) == 0 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "Empty payload",
		})
	}

	if int64(len(payload)) > h.maxPayloadSize {
		return c.Status(fiber.StatusRequestEntityTooLarge).JSON(fiber.Map{
			"error": fmt.Sprintf("Payload too large (max %s). Consider batching into smaller requests.", formatBytes(h.maxPayloadSize)),
		})
	}

	// Handle compression using pooled readers
	// We use c.Request().Body() above to get raw bytes and decompress ourselves
	// This avoids fasthttp's non-pooled gunzipData which causes high tail latency
	// Supported: gzip (0x1f 0x8b) and zstd (0x28 0xB5 0x2F 0xFD)
	var pooledBuf *PooledBuffer
	var compressionType string
	isGzip := len(payload) >= 2 && payload[0] == 0x1f && payload[1] == 0x8b
	isZstd := len(payload) >= 4 && payload[0] == 0x28 && payload[1] == 0xB5 && payload[2] == 0x2F && payload[3] == 0xFD

	if isZstd {
		compressionType = "zstd"
		var err error
		pooledBuf, err = h.decompressZstd(payload)
		if err != nil {
			h.logger.Error().Err(err).Msg("Failed to decompress zstd payload")
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": fmt.Sprintf("Invalid zstd compression: %v", err),
			})
		}
		defer pooledBuf.Release()

		compressedSize := len(payload)
		payload = pooledBuf.Data
		h.logger.Debug().
			Int("compressed_size", compressedSize).
			Int("decompressed_size", len(payload)).
			Str("compression", "zstd").
			Msg("Decompressed payload")
	} else if isGzip {
		compressionType = "gzip"
		var err error
		pooledBuf, err = h.decompressGzip(payload)
		if err != nil {
			h.logger.Error().Err(err).Msg("Failed to decompress gzip payload")
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": fmt.Sprintf("Invalid gzip compression: %v", err),
			})
		}
		defer pooledBuf.Release()

		compressedSize := len(payload)
		payload = pooledBuf.Data
		h.logger.Debug().
			Int("compressed_size", compressedSize).
			Int("decompressed_size", len(payload)).
			Str("compression", "gzip").
			Msg("Decompressed payload")
	}

	// Decode MessagePack
	records, err := h.decoder.Decode(payload)
	if err != nil {
		h.logger.Error().Err(err).Msg("Failed to decode MessagePack")
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": fmt.Sprintf("Invalid MessagePack payload: %v", err),
		})
	}

	// Get database from header (optional)
	database := c.Get("x-arc-database")

	// Get record count based on type
	var recordCount int
	switch v := records.(type) {
	case []interface{}:
		recordCount = len(v)
	default:
		recordCount = 1
	}

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
	if h.rbacManager != nil && h.rbacManager.IsRBACEnabled() {
		if err := h.checkWritePermissions(c, database, measurements); err != nil {
			return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
				"error": err.Error(),
			})
		}
	}

	if err := h.arrowBuffer.Write(ctx, database, records); err != nil {
		h.logger.Error().Err(err).Msg("Failed to write to Arrow buffer")
		metrics.Get().IncIngestErrors()
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "Failed to write records",
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

	// Return 204 No Content (InfluxDB compatible)
	return c.SendStatus(fiber.StatusNoContent)
}

// decompressGzip decompresses gzip data with size limits
// Uses sync.Pool for gzip reader and output buffer to minimize allocations
// ZERO-COPY: Returns a PooledBuffer that caller MUST Release() after use
func (h *MsgPackHandler) decompressGzip(data []byte) (*PooledBuffer, error) {
	maxDecompressedSize := h.maxPayloadSize
	const readChunkSize = 32 * 1024 // 32KB chunks

	// Get pooled gzip reader or create new one
	var reader *gzip.Reader
	var err error
	if pooled := gzipReaderPool.Get(); pooled != nil {
		reader = pooled.(*gzip.Reader)
		err = reader.Reset(bytes.NewReader(data))
	} else {
		// No reader in pool, create new one
		reader, err = gzip.NewReader(bytes.NewReader(data))
	}
	if err != nil {
		if reader != nil {
			gzipReaderPool.Put(reader)
		}
		return nil, fmt.Errorf("failed to initialize gzip reader: %w", err)
	}

	// Get output buffer from pool
	bufPtr := decompressBufferPool.Get().(*[]byte)
	buf := (*bufPtr)[:0] // Reset length but keep capacity (256KB)

	// Read directly into pooled buffer in chunks
	limitedReader := io.LimitReader(reader, maxDecompressedSize+1)
	for {
		// Ensure we have room to read
		if cap(buf)-len(buf) < readChunkSize {
			// Need to grow - double capacity
			newBuf := make([]byte, len(buf), cap(buf)*2+readChunkSize)
			copy(newBuf, buf)
			buf = newBuf
		}

		// Read into available capacity
		n, readErr := limitedReader.Read(buf[len(buf) : len(buf)+readChunkSize])
		buf = buf[:len(buf)+n]

		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			gzipReaderPool.Put(reader)
			*bufPtr = (*bufPtr)[:0]
			decompressBufferPool.Put(bufPtr)
			return nil, fmt.Errorf("failed to decompress: %w", readErr)
		}
	}

	// Return reader to pool (Close() is called internally by Reset on next use)
	gzipReaderPool.Put(reader)

	// Check size limit
	if int64(len(buf)) > maxDecompressedSize {
		*bufPtr = (*bufPtr)[:0]
		decompressBufferPool.Put(bufPtr)
		return nil, fmt.Errorf("decompressed payload exceeds %s limit", formatBytes(maxDecompressedSize))
	}

	// Update the pooled buffer pointer with potentially grown buffer
	*bufPtr = buf

	// ZERO-COPY: Return pooled buffer directly - caller MUST call Release()
	// This eliminates the ~45KB allocation per request that was causing GC pressure
	return &PooledBuffer{
		Data:   buf,
		bufPtr: bufPtr,
	}, nil
}

// decompressZstd decompresses zstd data with size limits
// Uses sync.Pool for zstd decoder and output buffer to minimize allocations
// Zstd is 3-5x faster than gzip for decompression
// ZERO-COPY: Returns a PooledBuffer that caller MUST Release() after use
func (h *MsgPackHandler) decompressZstd(data []byte) (*PooledBuffer, error) {
	maxDecompressedSize := h.maxPayloadSize

	// Get pooled zstd decoder or create new one
	var decoder *zstd.Decoder
	var err error
	if pooled := zstdDecoderPool.Get(); pooled != nil {
		decoder = pooled.(*zstd.Decoder)
	} else {
		// No decoder in pool, create new one with memory limit
		decoder, err = zstd.NewReader(nil, zstd.WithDecoderMaxMemory(uint64(maxDecompressedSize)+1024))
		if err != nil {
			return nil, fmt.Errorf("failed to create zstd decoder: %w", err)
		}
	}

	// Get output buffer from pool
	bufPtr := decompressBufferPool.Get().(*[]byte)
	buf := (*bufPtr)[:0] // Reset length but keep capacity (256KB)

	// Decompress directly into pooled buffer
	// zstd.Decoder.DecodeAll is very fast and handles buffer growth internally
	buf, err = decoder.DecodeAll(data, buf)
	if err != nil {
		zstdDecoderPool.Put(decoder)
		*bufPtr = (*bufPtr)[:0]
		decompressBufferPool.Put(bufPtr)
		return nil, fmt.Errorf("failed to decompress zstd: %w", err)
	}

	// Return decoder to pool
	zstdDecoderPool.Put(decoder)

	// Check size limit
	if int64(len(buf)) > maxDecompressedSize {
		*bufPtr = (*bufPtr)[:0]
		decompressBufferPool.Put(bufPtr)
		return nil, fmt.Errorf("decompressed payload exceeds %s limit", formatBytes(maxDecompressedSize))
	}

	// Update the pooled buffer pointer with potentially grown buffer
	*bufPtr = buf

	// ZERO-COPY: Return pooled buffer directly - caller MUST call Release()
	return &PooledBuffer{
		Data:   buf,
		bufPtr: bufPtr,
	}, nil
}

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
				"time":        []int64{1633024800000, 1633024801000, 1633024802000},
				"host":        []string{"server01", "server01", "server01"},
				"region":      []string{"us-east", "us-east", "us-east"},
				"usage_idle":  []float64{95.0, 94.5, 94.2},
				"usage_user":  []float64{3.2, 3.8, 4.1},
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
			"expected_rps_columnar":  "2.5M+ (columnar format, zero-copy)",
			"expected_rps_row":       "2.1M (row format, with conversion)",
			"columnar_advantage":     "25-35% faster (no flattening, no row→column conversion)",
			"parsing_speed":          "10-15x faster than text parsing",
			"serialization":          "Direct Arrow (2-3x faster than DataFrame)",
			"payload_size":           "50-70% smaller than Line Protocol",
			"wire_efficiency":        "Columnar sends field names once vs per-record",
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
