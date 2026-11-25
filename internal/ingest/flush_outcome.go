package ingest

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"
)

// Per-import flush outcome (#1888).
//
// An import answers once its rows are handed to the flush; their files follow
// as the flushes finish (#1296, #1429). With a WAL copy of every row that is
// a durable answer: a failed flush is replayed. A row without a WAL copy (the
// WAL off, or a queued-mode append the WAL dropped) is lost when its flush
// fails, and the import had already answered 200.
//
// A FlushOutcome, carried on the import's write context, rides on each batch
// the import buffers. It counts the rows buffered without a WAL copy, and the
// flush-failure sites add, per outcome, the rows of those batches a failure
// may have lost. The import, once it has handed everything to the flush,
// waits for those flushes (WaitFlushes) only when it buffered rows without a
// WAL copy, and fails only when rows of ITS OWN were at risk: another
// writer's failure, even on the same measurement, is not attributed to it.
type FlushOutcome struct {
	noWALRows atomic.Int64 // rows buffered without a WAL copy
	atRisk    atomic.Int64 // of those, rows in a flush that failed
}

// NoWALRows is how many rows were buffered without a WAL copy.
func (o *FlushOutcome) NoWALRows() int64 { return o.noWALRows.Load() }

// RowsAtRisk is how many rows without a WAL copy were in a flush that
// failed: lost, or, after a partial failure or a write that timed out and
// landed anyway, possibly stored. Never a count of rows known to be lost.
func (o *FlushOutcome) RowsAtRisk() int64 { return o.atRisk.Load() }

type flushOutcomeKey struct{}

// WithFlushOutcome is ctx carrying o: every batch a write under it buffers is
// counted in o.
func WithFlushOutcome(ctx context.Context, o *FlushOutcome) context.Context {
	return context.WithValue(ctx, flushOutcomeKey{}, o)
}

func flushOutcomeFrom(ctx context.Context) *FlushOutcome {
	if ctx == nil {
		return nil
	}
	o, _ := ctx.Value(flushOutcomeKey{}).(*FlushOutcome)
	return o
}

// outcomeRowsOf is, per outcome, the rows of records without a WAL copy: what
// a failure of their flush puts at risk. Read before the records are
// released. nil when no record carries an outcome, which is every live write.
func outcomeRowsOf(records []interface{}) map[*FlushOutcome]int64 {
	var out map[*FlushOutcome]int64
	for _, record := range records {
		batch, ok := record.(*TypedColumnBatch)
		if !ok || batch == nil || batch.outcome == nil || batch.hasWALCopy {
			continue
		}
		rows, known := flushRecordRows(record)
		if !known {
			continue
		}
		if out == nil {
			out = map[*FlushOutcome]int64{}
		}
		out[batch.outcome] += int64(rows)
	}
	return out
}

// noteOutcomeFailure adds a failed flush's at-risk rows to their outcomes:
// all its rows without a WAL copy, including any a partial flush stored
// before failing, since which outcome's rows those were is not known.
func noteOutcomeFailure(atRisk map[*FlushOutcome]int64, _ error) {
	for o, rows := range atRisk {
		o.atRisk.Add(rows)
	}
}

// countOutcome attaches o to an import's batch and counts its rows without a
// WAL copy. Called after the WAL commit, which can still clear hasWALCopy (a
// queued append the WAL dropped), under the shard lock.
func countOutcome(o *FlushOutcome, batch *TypedColumnBatch, rows int) {
	if o == nil {
		return
	}
	batch.outcome = o
	if !batch.hasWALCopy {
		o.noWALRows.Add(int64(rows))
	}
}

// OwnFlushErr filters a synchronous flush error an import got from flushing
// its measurements (#1888). Such a flush also takes other writers' rows of
// those measurements, so its error is the import's only when rows of its
// own were in it. With rows without a WAL copy, the outcome knows: the error
// is returned when the import's rows are at risk, or when the flush did not
// run (the caller's context, the buffer closing). Without any (the WAL on),
// it is returned as before.
func (o *FlushOutcome) OwnFlushErr(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if o == nil || o.NoWALRows() == 0 || ctx.Err() != nil || errors.Is(err, ErrBufferClosing) {
		return err
	}
	if n := o.RowsAtRisk(); n > 0 {
		return fmt.Errorf("%d of this import's rows had no WAL copy and were in a flush that failed: %w", n, err)
	}
	return nil
}

// ImportVerdict is an import's answer once its final flush returned flushErr
// (#1888): the flush error, filtered by OwnFlushErr; then, when the import
// buffered rows without a WAL copy, the wait for their flushes and whether
// any was at risk.
func (b *ArrowBuffer) ImportVerdict(ctx context.Context, o *FlushOutcome, database string, measurements []string, flushErr error) error {
	if err := o.OwnFlushErr(ctx, flushErr); err != nil {
		return err
	}
	if o == nil || o.NoWALRows() == 0 {
		return nil
	}
	if err := b.WaitFlushes(ctx, database, measurements); err != nil {
		return fmt.Errorf("the server stopped before this import's flushes finished, so whether its rows without a WAL copy were stored is not known: %w", err)
	}
	if n := o.RowsAtRisk(); n > 0 {
		return fmt.Errorf("%d of this import's rows had no WAL copy and were in a flush that failed", n)
	}
	return nil
}

// waitFlushesRecheck is how often WaitFlushes looks at the buffer closing,
// which no flush completion signals.
const waitFlushesRecheck = time.Second

// WaitFlushes waits until every flush task handed off so far for the given
// measurements of database has finished (#1888). An import calls it after
// its final flush handed everything to the flush: every task holding its
// rows was registered before then, so it is at or below the fence read here,
// or already done. No barrier gate and no shard lock are taken, so a DELETE
// or another barrier never waits behind it.
//
// It returns ctx's error (the server shutting down) or ErrBufferClosing once
// the buffer is closing; the import's outcome is then unknown.
func (b *ArrowBuffer) WaitFlushes(ctx context.Context, database string, measurements []string) error {
	if len(measurements) == 0 {
		return nil
	}
	want := make(map[string]struct{}, len(measurements))
	for _, m := range measurements {
		want[m] = struct{}{}
	}
	match := func(key string) bool {
		parts := splitBufferKey(key)
		if len(parts) != 2 || parts[0] != database {
			return false
		}
		_, ok := want[parts[1]]
		return ok
	}
	fence := b.asyncFlushFence()
	recheck := time.NewTicker(waitFlushesRecheck)
	defer recheck.Stop()
	for {
		// Closing first: Close abandons queued tasks it has no budget for,
		// and they complete without a flush.
		if b.closing.Load() {
			return ErrBufferClosing
		}
		pending, changed := b.asyncFlushState(fence, match)
		if !pending {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		case <-recheck.C:
		}
	}
}
