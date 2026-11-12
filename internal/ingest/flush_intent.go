package ingest

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/basekick-labs/arc/internal/wal"
)

// Flush intents (#1646, wal/intent.go): before a file is stored, the flush
// names it and the WAL identities its checkpoint will name, so that a crash
// between the storage write and the checkpoint does not replay rows that are
// already stored.

// flushIntentRecorder is implemented by *wal.Writer.
type flushIntentRecorder interface {
	RecordFlushIntent(wal.FlushIntent) error
}

// SetClusterMode tells the buffer it runs as a cluster node. A cluster node
// writes no flush intents and adopts none: there a file also needs its
// manifest registration, which a crash before the checkpoint can have
// skipped, and an unregistered file is a reconciliation orphan. Replay keeps
// the at-least-once behaviour there. Set before WAL recovery runs.
func (b *ArrowBuffer) SetClusterMode(enabled bool) {
	b.clusterMode.Store(enabled)
}

// recordFlushIntent appends the intent for a file about to be stored. It never
// waits and never fails the flush: no intent is the duplicate risk there was.
func (b *ArrowBuffer) recordFlushIntent(path string, size int64, ids []string) {
	if len(ids) == 0 || b.wal == nil || b.clusterMode.Load() {
		return
	}
	rec, ok := b.wal.(flushIntentRecorder)
	if !ok {
		return
	}
	if err := rec.RecordFlushIntent(wal.FlushIntent{Path: path, Size: size, IDs: ids}); err != nil {
		metrics.Get().IncWALFlushIntentsDropped()
		if !errors.Is(err, wal.ErrFlushIntentTooLarge) && b.warnOncePerMinute("flush-intent", "", "", time.Now()) {
			b.logger.Debug().Err(err).Str("storage_path", path).Msg("Flush intent not written; a crash before this flush's checkpoint replays its rows")
		}
		return
	}
	metrics.Get().IncWALFlushIntentsWritten()
}

// AdoptFlushedFile is wal.RecoveryOptions.AdoptIntent: it reports whether the
// file an intent names is in storage at the size the flush wrote, so the
// intent's entries are stored and need no replay. Storage writes are atomic
// (local: temp file and rename; object stores: one PUT), and a flush's file
// name is unique in its process, so the object at that path with that size
// is the file the flush wrote.
//
// The crash may also have cut the flush's field-schema registration short,
// which a replay would have redone: the adopted file's schema is read from
// its footer and registered. A tiering manager, when there is one, registers
// files present in storage itself when it starts.
func (b *ArrowBuffer) AdoptFlushedFile(ctx context.Context, intent wal.FlushIntent) bool {
	if b.clusterMode.Load() || b.storage == nil {
		return false
	}
	sctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	size, err := b.storage.StatFile(sctx, intent.Path)
	if err != nil || size != intent.Size {
		if err != nil {
			b.logger.Warn().Err(err).Str("storage_path", intent.Path).Msg("Could not check the file of an interrupted flush; replaying its rows")
		}
		return false
	}
	b.registerAdoptedSchema(sctx, intent.Path)
	if b.ambiguous != nil {
		b.ambiguous.forget(intent.Path) // #1635: this is the copy that holds the rows
	}
	metrics.Get().IncWALFlushIntentsAdopted()
	return true
}

// registerAdoptedSchema registers the schema of an adopted file, read from its
// footer. Best effort, as registerFieldSchema is.
func (b *ArrowBuffer) registerAdoptedSchema(ctx context.Context, path string) {
	if b.fieldSchema == nil {
		return
	}
	parts := strings.SplitN(path, "/", 3)
	if len(parts) < 3 {
		return
	}
	data, err := b.storage.Read(ctx, path)
	if err != nil {
		b.logger.Warn().Err(err).Str("storage_path", path).Msg("Could not read an adopted file to register its field schema")
		return
	}
	rdr, err := file.NewParquetReader(bytes.NewReader(data))
	if err != nil {
		b.logger.Warn().Err(err).Str("storage_path", path).Msg("Could not open an adopted file to register its field schema")
		return
	}
	defer rdr.Close()
	fr, err := pqarrow.NewFileReader(rdr, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	if err != nil {
		return
	}
	schema, err := fr.Schema()
	if err != nil {
		return
	}
	var tags []string
	if v := rdr.MetaData().KeyValueMetadata().FindValue("arc:tags"); v != nil && *v != "" {
		tags = strings.Split(*v, ",")
	}
	b.registerFieldSchema(ctx, parts[0], parts[1], schema, tags, path)
}
