package ingest

import (
	"context"
	"math"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/metrics"
)

// Buffered-bytes limit (#1468). A buffer whose flush the full flush queue
// keeps deferring grows for as long as storage stays slow; without a limit the
// process runs out of memory. Once the rows held for flushing (the estimate
// behind arc_buffer_bytes_buffered: buffered, queued and being written) reach
// ingest.max_buffered_bytes, new write requests are refused with a retryable
// 503 (ErrBufferFull) until flushes bring them down. Nothing accepted is
// dropped: the check is made once per request, before any of it is written
// (AdmitRequest), so a request is refused whole or not at all. WAL replay and
// continuous-query writes never call AdmitRequest and are never refused.
//
// It is a soft limit: requests admitted at once each add their own size, so
// one can overshoot by up to server.max_payload_size, and replay at startup
// can put a node over it until the replayed rows flush.
const (
	// bufferFullAutoPercent is the share of the memory limit "auto" resolves
	// to. A backstop, not a heap bound: the estimate is a MessagePack
	// request's payload size, and a flush's merge, Arrow and Parquet copies
	// are not in it, so the heap can be two or three times the estimate.
	bufferFullAutoPercent = 50
	// bufferFullClearPercent is where the buffer-full state clears once set:
	// below the limit, so a subscriber resumed as it clears (MQTT, whose
	// broker then delivers what it held) does not refill it at once and
	// suspend again.
	bufferFullClearPercent = 90
	// importAdmitPercent is where a bulk import is refused at admission: an
	// import paces its own writes below this (importPacer), and one started
	// above it would wait for a level it cannot reach.
	importAdmitPercent = 90
	// bufferFullWarnPercent is where the warning that writes will soon be
	// refused is logged; it re-arms below bufferFullRearmPercent.
	bufferFullWarnPercent  = 80
	bufferFullRearmPercent = 64
)

// resolveMaxBufferedBytes is the limit cfg asks for: an explicit value, half
// of memoryLimit for "auto", or 0 (off) when "auto" has no limit to use.
func resolveMaxBufferedBytes(cfg *config.IngestConfig, memoryLimit uint64) int64 {
	if cfg == nil {
		return 0
	}
	if !cfg.MaxBufferedBytesAuto {
		return max(cfg.MaxBufferedBytes, 0)
	}
	// Divided after multiplying, for an exact share of a limit that is not a
	// multiple of 100, unless the product would overflow.
	share := memoryLimit / 100 * bufferFullAutoPercent
	if memoryLimit <= math.MaxUint64/100 {
		share = memoryLimit * bufferFullAutoPercent / 100
	}
	return int64(min(share, math.MaxInt64))
}

// MaxBufferedBytes is the resolved ingest.max_buffered_bytes, 0 when off.
func (b *ArrowBuffer) MaxBufferedBytes() int64 { return b.maxBufferedBytes }

// OnBufferFullChange registers fn to be told when the buffer-full state
// changes, and tells it the current state at once: the state may already be
// set when the caller registers (the sampler runs from NewArrowBuffer). fn
// must not block; it can run on a write's goroutine. One listener; a second
// call replaces the first.
func (b *ArrowBuffer) OnBufferFullChange(fn func(full bool)) {
	b.bufferFullListener.Store(&fn)
	fn(b.bufferFull.Load())
}

func (b *ArrowBuffer) setBufferFull(full bool) {
	if !b.bufferFull.CompareAndSwap(!full, full) {
		return
	}
	if fn := b.bufferFullListener.Load(); fn != nil {
		(*fn)(full)
	}
}

// admitBufferedBytes refuses a request while the buffered bytes are at limit.
// setFull marks the buffer full on a refusal (the limit itself); an import's
// lower admission threshold does not.
func (b *ArrowBuffer) admitBufferedBytes(ctx context.Context, limit int64, setFull bool) error {
	if b.maxBufferedBytes <= 0 || b.bufferBytesBuffered.Load() < limit {
		return nil
	}
	if setFull {
		b.setBufferFull(true)
	}
	if counted, _ := ctx.Value(refusalCountedKey{}).(bool); !counted {
		metrics.Get().IncBufferFullRejections()
	}
	return ErrBufferFull
}

// AdmitImport is AdmitRequest for a bulk import, which is also refused, with
// ErrBufferFull, from importAdmitPercent of the limit: it would otherwise
// start writing and then wait, past importFlushWait, for its pacing level.
func (b *ArrowBuffer) AdmitImport(ctx context.Context, walBytes int64) (context.Context, func(), error) {
	if err := b.admitBufferedBytes(ctx, b.maxBufferedBytes/100*importAdmitPercent, false); err != nil {
		return ctx, noRelease, err
	}
	return b.AdmitRequest(ctx, walBytes)
}

// observeBufferedBytes runs on the 1 Hz sampler: it sets and clears the
// buffer-full state, and logs once as the bytes approach the limit.
func (b *ArrowBuffer) observeBufferedBytes(bytes int64) {
	limit := b.maxBufferedBytes
	if limit <= 0 {
		return
	}
	switch {
	case bytes >= limit:
		b.setBufferFull(true)
	case bytes < limit/100*bufferFullClearPercent:
		b.setBufferFull(false)
	}
	if bytes >= limit/100*bufferFullWarnPercent {
		if b.bufferFullWarning.CompareAndSwap(false, true) {
			b.logger.Warn().
				Int64("buffered_bytes", bytes).
				Int64("max_buffered_bytes", limit).
				Int("threshold_percent", bufferFullWarnPercent).
				Msg("Rows waiting to be flushed are near ingest.max_buffered_bytes: storage is not keeping up, and at the limit new writes are refused with 503 until flushes catch up")
		}
	} else if bytes < limit/100*bufferFullRearmPercent {
		b.bufferFullWarning.Store(false)
	}
}
