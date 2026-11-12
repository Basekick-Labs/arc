package ingest

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/basekick-labs/arc/internal/storage"
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
// The crash may also have cut the flush's registrations short, which a replay
// would have redone: the adopted file's schema is read from its footer and
// registered, and the file is registered in tiering metadata when a tiering
// manager is set (it is not yet at startup recovery; the manager's own startup
// scan registers files present in storage). A file adopted by a periodic pass
// is usually a #1635 late-landed write, which never reached registration.
func (b *ArrowBuffer) AdoptFlushedFile(ctx context.Context, intent wal.FlushIntent) bool {
	if b.clusterMode.Load() || b.storage == nil {
		return false
	}
	sctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	size, err := b.storage.StatFile(sctx, intent.Path)
	cancel()
	if err != nil && !storage.IsNotFound(err) {
		b.logger.Warn().Err(err).Str("storage_path", intent.Path).Msg("Could not check the file of an interrupted flush; replaying its rows")
	}
	if err != nil || size != intent.Size {
		return false
	}
	database, measurement, partition, ok := partitionOfPath(intent.Path)
	if ok {
		rctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		b.registerAdoptedSchema(rctx, intent.Path, size, database, measurement)
		b.registerFileInTiering(rctx, database, measurement, intent.Path, partition, size, "", nil)
		cancel()
	}
	if b.ambiguous != nil {
		b.ambiguous.forget(intent.Path) // #1635: this is the copy that holds the rows
	}
	metrics.Get().IncWALFlushIntentsAdopted()
	return true
}

// registerAdoptedSchema registers the schema of an adopted file, read from
// its footer only (the tail of the object). Best effort, as
// registerFieldSchema is. The tags are the file's arc:tags, the subset of the
// flush's tag names that compaction can use.
func (b *ArrowBuffer) registerAdoptedSchema(ctx context.Context, path string, size int64, database, measurement string) {
	if b.fieldSchema == nil || size < 12 {
		return
	}
	tail := func(n int64) ([]byte, error) {
		var buf bytes.Buffer
		if err := b.storage.ReadToAt(ctx, path, &buf, size-n); err != nil {
			return nil, err
		}
		return buf.Bytes(), nil
	}
	end, err := tail(8)
	if err != nil || len(end) != 8 || string(end[4:]) != "PAR1" {
		b.logger.Warn().Err(err).Str("storage_path", path).Msg("Could not read an adopted file's footer to register its field schema")
		return
	}
	footer := int64(binary.LittleEndian.Uint32(end[:4])) + 8
	if footer > size-4 {
		return
	}
	data, err := tail(footer)
	if err != nil || int64(len(data)) != footer {
		return
	}
	rdr, err := file.NewParquetReader(&tailReader{tail: data, base: size - footer, size: size})
	if err != nil {
		b.logger.Warn().Err(err).Str("storage_path", path).Msg("Could not open an adopted file's footer to register its field schema")
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
	b.registerFieldSchema(ctx, database, measurement, schema, tags, path)
}

// tailReader serves the last bytes of a file as if it were the whole file:
// enough for a Parquet reader to read the footer, and an error for anything
// before it.
type tailReader struct {
	tail []byte
	base int64 // file offset of tail[0]
	size int64
	pos  int64
}

func (r *tailReader) ReadAt(p []byte, off int64) (int, error) {
	if off < r.base {
		return 0, errors.New("read before the footer")
	}
	if off >= r.size {
		return 0, io.EOF
	}
	n := copy(p, r.tail[off-r.base:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (r *tailReader) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		r.pos = offset
	case io.SeekCurrent:
		r.pos += offset
	case io.SeekEnd:
		r.pos = r.size + offset
	}
	return r.pos, nil
}

// partitionOfPath reads database, measurement and partition hour from a
// flush's storage path: {db}/{measurement}/{YYYY}/{MM}/{DD}/{HH}/{file}.
func partitionOfPath(path string) (database, measurement string, hour time.Time, ok bool) {
	parts := strings.Split(path, "/")
	if len(parts) != 7 {
		return "", "", time.Time{}, false
	}
	t, err := time.Parse("2006/01/02/15", strings.Join(parts[2:6], "/"))
	if err != nil {
		return "", "", time.Time{}, false
	}
	return parts[0], parts[1], t.UTC(), true
}

// *wal.Writer must keep writing intents for the buffer to find them.
var _ flushIntentRecorder = (*wal.Writer)(nil)
