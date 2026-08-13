package api

import (
	"io"
	"strconv"
	"strings"
	"sync"

	"github.com/gofiber/fiber/v2"
	"github.com/klauspost/compress/gzip"
	"github.com/klauspost/compress/zstd"
)

// Streaming response compression for the JSON and msgpack query endpoints,
// negotiated via the standard Accept-Encoding request header (zstd preferred
// over gzip). Responses stream through SetBodyStreamWriter, which bypasses
// any body-rewriting middleware, so the compressor is wired directly into
// the stream callback: encoder output feeds the fasthttp chunk writer, and
// the endpoint's existing bufio layer sits in front of the encoder so
// per-primitive writes batch up before compression.
//
// Encoder choices are throughput-oriented: zstd SpeedFastest with
// concurrency 1 (no per-response goroutine fan-out) and gzip BestSpeed.
// Both encoder types are pooled and Reset per response — steady-state
// allocation cost is near zero, which is the point: the win is wire bytes,
// and the price must not be a per-request allocation storm.
//
// Without an Accept-Encoding match the request takes the exact pre-existing
// path: no wrapper, no extra buffer, no allocation.

var zstdRespEncPool = sync.Pool{
	New: func() interface{} {
		// Errors are impossible with these static options.
		enc, _ := zstd.NewWriter(nil,
			zstd.WithEncoderLevel(zstd.SpeedFastest),
			zstd.WithEncoderConcurrency(1))
		return enc
	},
}

var gzipRespEncPool = sync.Pool{
	New: func() interface{} {
		enc, _ := gzip.NewWriterLevel(nil, gzip.BestSpeed)
		return enc
	},
}

// negotiateResponseCompression returns "zstd", "gzip", or "" based on the
// request's Accept-Encoding header. zstd wins when both are acceptable.
// Minimal q-value handling: an encoding listed with q=0 is treated as not
// acceptable; any other q keeps list order irrelevant (we always prefer
// zstd for its better ratio at comparable speed).
func negotiateResponseCompression(c *fiber.Ctx) string {
	accept := c.Get(fiber.HeaderAcceptEncoding)
	if accept == "" {
		return ""
	}
	var zstdOK, gzipOK bool
	for _, part := range strings.Split(accept, ",") {
		token, params, _ := strings.Cut(strings.TrimSpace(part), ";")
		token = strings.ToLower(strings.TrimSpace(token))
		if params != "" {
			// First parameter only; q must be the first per RFC 7231. Parse
			// the value numerically so q=0, q=0., q=0.000 all mean "not
			// acceptable" (string matching missed legal zero spellings).
			qPart, _, _ := strings.Cut(params, ";")
			qPart = strings.TrimSpace(strings.ToLower(qPart))
			if rest, ok := strings.CutPrefix(qPart, "q="); ok {
				if qv, err := strconv.ParseFloat(strings.TrimSpace(rest), 64); err == nil && qv == 0 {
					continue
				}
			}
		}
		switch token {
		case "zstd":
			zstdOK = true
		case "gzip", "x-gzip":
			gzipOK = true
		}
	}
	switch {
	case zstdOK:
		return "zstd"
	case gzipOK:
		return "gzip"
	}
	return ""
}

// compressedSink wraps w with the negotiated encoder. It returns the writer
// the response body should be produced into and a finish func that flushes
// the encoder's trailing frame and returns it to its pool. encoding must be
// "", "zstd", or "gzip"; with "" the original writer and a no-op finish are
// returned (zero overhead).
//
// The caller MUST call finish exactly once after the last body byte and
// before the final flush of w.
func compressedSink(w io.Writer, encoding string) (io.Writer, func() error) {
	switch encoding {
	case "zstd":
		enc := zstdRespEncPool.Get().(*zstd.Encoder)
		enc.Reset(w)
		return enc, func() error {
			// Close flushes the final frame but keeps the encoder reusable
			// after Reset (klauspost zstd semantics). Reset(nil) drops the
			// reference to fasthttp's pooled writer before pooling.
			err := enc.Close()
			enc.Reset(nil)
			zstdRespEncPool.Put(enc)
			return err
		}
	case "gzip":
		enc := gzipRespEncPool.Get().(*gzip.Writer)
		enc.Reset(w)
		return enc, func() error {
			err := enc.Close()
			enc.Reset(nil)
			gzipRespEncPool.Put(enc)
			return err
		}
	default:
		return w, func() error { return nil }
	}
}
