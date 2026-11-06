package ingest

import (
	"context"

	"github.com/basekick-labs/arc/internal/wal"
)

// walPreparer builds a write's tracked WAL entries before the shard lock and
// commits them under it (#1438): a write refused in between leaves nothing in
// the WAL. Queued acknowledgement only; see wal.(*Writer).PrepareTracked.
type walPreparer interface {
	PrepareTracked(database string, payload []byte) (*wal.PreparedAppend, error)
}

// walContextAppender is a tracked WAL append whose durable wait ends with the
// write's context (*wal.Writer, #1501).
type walContextAppender interface {
	AppendRawWithMetaTrackedContext(ctx context.Context, database string, payload []byte) ([]string, error)
}

// walCommitter is a prepared WAL append (*wal.PreparedAppend).
type walCommitter interface {
	Commit(cseq uint64) ([]string, error)
}

// walRetracter checkpoints a refused write's entries so recovery never
// replays them (*wal.Writer.RetractTracked).
type walRetracter interface {
	RetractTracked(hashes []string) bool
}

// prepareTrackedWAL prepares a queued write's WAL entries, or returns nil when
// this write appends before the lock instead: no WAL, a WAL that cannot
// prepare (a test double), or durable acknowledgement, whose sync wait cannot
// run under the shard lock.
func (b *ArrowBuffer) prepareTrackedWAL() walPreparer {
	if b.wal == nil || b.walDurableAck() {
		return nil
	}
	if _, ok := b.wal.(trackedWALWriter); !ok {
		return nil
	}
	p, _ := b.wal.(walPreparer)
	return p
}

// commitTrackedLocked enqueues a prepared uncovered write's WAL entries, under
// the shard lock, after every check that can refuse the write (#1438). On a
// full queue the entries that were enqueued keep their tokens, so the flush
// checkpoints them; without a complete copy the batch reports none.
// The caller checks prep for nil: a nil *wal.PreparedAppend in this
// interface is not a nil interface.
func commitTrackedLocked(prep walCommitter, batch *TypedColumnBatch) error {
	tokens, err := prep.Commit(0)
	batch.WALHashes = tokens
	batch.hasWALCopy = err == nil
	return err
}

// retractRefusedWAL takes back the WAL entries of a write refused after its
// append (#1438): durable acknowledgement appends before the shard lock, so a
// refusal under it (schema churn, a cancelled request, a swept shard) comes
// after the entries reached the WAL. Checkpointed, recovery never replays them,
// and the refusal stays true: nothing was stored. If the checkpoint cannot be
// written the WAL only releases them, as before, and counts and logs it (see
// wal.(*Writer).RetractTracked). Reports whether they were retracted. Never
// call it under a shard lock: a durable retraction waits for a sync.
func (b *ArrowBuffer) retractRefusedWAL(hashes []string) bool {
	if len(hashes) == 0 || b.wal == nil {
		return false
	}
	if r, ok := b.wal.(walRetracter); ok {
		return r.RetractTracked(hashes)
	}
	if tracked, ok := b.wal.(trackedWALWriter); ok {
		tracked.ForgetTracked(hashes)
	}
	return false
}

// appendUncoveredWAL writes the WAL entry of an uncovered live write. In
// queued mode it only prepares it: the caller commits it under the shard lock
// with commitTrackedLocked, or abandons it with the write (#1438). Otherwise
// (durable acknowledgement, or a WAL that cannot prepare) it appends now, as
// before, and a refusal after this point retracts it (retractRefusedWAL).
// hasCopy is whether the entry is already complete in the WAL; a prepared one
// is decided at commit.
func (b *ArrowBuffer) appendUncoveredWAL(ctx context.Context, database string, payload []byte) (prep *wal.PreparedAppend, hashes []string, hasCopy bool, err error) {
	if p := b.prepareTrackedWAL(); p != nil {
		prep, err = p.PrepareTracked(database, payload)
		return prep, nil, false, err
	}
	switch w := b.wal.(type) {
	case walContextAppender:
		hashes, err = w.AppendRawWithMetaTrackedContext(ctx, database, payload)
	case trackedWALWriter:
		hashes, err = w.AppendRawWithMetaTracked(database, payload)
	default:
		err = b.wal.AppendRawWithMeta(database, payload)
	}
	return nil, hashes, err == nil, err
}
