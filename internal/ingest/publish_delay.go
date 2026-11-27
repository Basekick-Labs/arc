package ingest

import (
	"time"

	"github.com/basekick-labs/arc/internal/metrics"
)

// observePublishDelay records arc_ingest_publish_delay_seconds for one file an
// ingest flush has just published: how long the oldest row of the flushed
// buffer waited, from the buffer being created (bufferedAt, the key's
// shard.bufferStartTimes entry, carried on the flush task and then on the
// merged batch) to the file being in storage.
//
// Every file of one flush is measured from the same start, the buffer's, so
// for a flush split into hour files or coverage classes the value is an upper
// bound on how long that file's own rows waited.
//
// Where it is called decides what it means:
//   - a single-hour file, and an hour file of a multi-hour flush that is
//     registered as soon as it is stored: right after storage.Write returns
//     nil.
//   - an hour file that waits for the rest of its flush (no WAL row map, or
//     deferred, #1697): when the flush registers it, after every hour is
//     stored. A query's glob can find such a file from its write, but until
//     the flush registers it a failed later hour can still take it away, so
//     the registration is what is timed.
//
// Files that reach storage and are not observed, all on a failed flush:
//   - a deferred hour file of a flush whose later hour failed, which the
//     ledger registers afterwards (ambiguous_writes.go);
//   - a file whose write returned an error and landed anyway (#1635).
//
// What it is not: the time until a query returns the row (query-side caches
// are not in it, #1757), nor the time until another node has the file. A
// buffer that WAL replay refilled starts again from the replay, so the time a
// failed flush cost is not in it either; arc_buffer_flush_failures_total says
// when that happened.
//
// bufferedAt is a wall-clock time (the buffer start is stored with .UTC(),
// which drops the monotonic reading), so a clock step can make the difference
// negative; the metric clamps that to zero. A zero bufferedAt is a batch that
// never sat in a buffer (claimed replay) and is not observed.
func observePublishDelay(bufferedAt time.Time) {
	if bufferedAt.IsZero() {
		return
	}
	metrics.Get().ObserveIngestPublishDelay(time.Since(bufferedAt))
}
