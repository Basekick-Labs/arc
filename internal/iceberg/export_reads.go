package iceberg

import (
	"context"
	"io/fs"
	"sync"
	"time"

	iceio "github.com/apache/iceberg-go/io"
)

// What one export has already read from the store, so it does not ask again
// (#1778). On object storage an export is a chain of requests made one after
// another, and before this the same new data file was fetched three times
// over: its tail for the schema, then by iceberg-go the whole object (an
// un-ranged GET whose body it drops) and two more ranges of the footer.
//
// It lives for one export of one table: the scheduler puts it on the context
// of a reconcile pass or an append (withExportReads) and nothing outlives
// that call. Every export therefore still reads the table's metadata and
// hint from the store, which is how it learns a table was removed or
// republished under it.
type exportReads struct {
	mu sync.Mutex
	// tails are the footers of new data files, by physical path, as read
	// for their schema; buildDataFiles hands each to iceberg-go and drops it.
	tails     map[string]fileTail
	tailBytes int64
	// batch, when set, is the budget this export shares with the exports
	// that run beside it (withBatchReads).
	batch *batchReads
	// hints is version-hint.text as the adopt check read it, by metadata
	// directory, for notePassStart, which would read it again a moment later.
	hints map[string]int
}

// exportReadsMaxTailBytes bounds the tails held at once. What is held per
// file is what the schema read fetched: the last footerWindow bytes (64 KiB),
// or the whole footer when it is longer, so the bound is about a thousand
// files. A pass holds a file's tail from its schema read until the chunk that
// registers it, and a tail never used until the pass ends. Past the bound a
// tail is not kept and iceberg-go reads that file from the store itself.
const exportReadsMaxTailBytes = 64 << 20

// fileTail is the end of a data file: data is its bytes [base, size).
type fileTail struct {
	base int64
	size int64
	data []byte
}

type exportReadsKey struct{}

// withExportReads returns ctx carrying an exportReads, keeping one already
// there.
func withExportReads(ctx context.Context) context.Context {
	if exportReadsFrom(ctx) != nil {
		return ctx
	}
	return context.WithValue(ctx, exportReadsKey{}, &exportReads{})
}

// batchReads is what the exports of one batch share (#1786): together they
// hold the tails one export alone may hold, and make the storage reads the
// object-store client has idle connections for. A pass with many new files
// that shares a batch with tables that have none gets all of both.
type batchReads struct {
	mu        sync.Mutex
	tailBytes int64
	// gate bounds the footer reads and existence checks in flight.
	gate chan struct{}
}

// batchReadsAtOnce is the storage reads the exports of a batch make at once:
// the object-store client's idle connections per host.
const batchReadsAtOnce = 16

func newBatchReads() *batchReads {
	return &batchReads{gate: make(chan struct{}, batchReadsAtOnce)}
}

// withBatchReads is withExportReads for an export that runs beside others.
func withBatchReads(ctx context.Context, b *batchReads) context.Context {
	return context.WithValue(ctx, exportReadsKey{}, &exportReads{batch: b})
}

// acquireRead waits for one of the batch's storage reads, when the export
// runs in a batch, and returns its release. Outside a batch it does not
// wait: the caller's own limit is the bound.
func acquireRead(ctx context.Context) (release func()) {
	r := exportReadsFrom(ctx)
	if r == nil || r.batch == nil {
		return func() {}
	}
	select {
	case r.batch.gate <- struct{}{}:
		return func() { <-r.batch.gate }
	case <-ctx.Done():
		return func() {} // the read fails on the context
	}
}

// reserve counts n more tail bytes against the batch's budget, and reports
// whether they fit; release gives them back.
func (b *batchReads) reserve(n int64) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.tailBytes+n > exportReadsMaxTailBytes {
		return false
	}
	b.tailBytes += n
	return true
}

func (b *batchReads) release(n int64) {
	b.mu.Lock()
	b.tailBytes -= n
	b.mu.Unlock()
}

// exportReadsFrom returns the export's reads, or nil outside an export.
func exportReadsFrom(ctx context.Context) *exportReads {
	r, _ := ctx.Value(exportReadsKey{}).(*exportReads)
	return r
}

// putTail keeps a data file's tail. Safe on nil.
func (r *exportReads) putTail(physicalPath string, t fileTail) {
	if r == nil || physicalPath == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if old, ok := r.tails[physicalPath]; ok {
		r.dropLocked(physicalPath, old)
	}
	if r.tailBytes+int64(len(t.data)) > exportReadsMaxTailBytes {
		return
	}
	if r.batch != nil && !r.batch.reserve(int64(len(t.data))) {
		return
	}
	if r.tails == nil {
		r.tails = make(map[string]fileTail)
	}
	r.tails[physicalPath] = t
	r.tailBytes += int64(len(t.data))
}

// takeTail removes and returns a data file's tail. Safe on nil.
func (r *exportReads) takeTail(physicalPath string) (fileTail, bool) {
	if r == nil {
		return fileTail{}, false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	t, ok := r.tails[physicalPath]
	if ok {
		r.dropLocked(physicalPath, t)
	}
	return t, ok
}

// dropLocked forgets a held tail; r.mu is held.
func (r *exportReads) dropLocked(physicalPath string, t fileTail) {
	r.tailBytes -= int64(len(t.data))
	delete(r.tails, physicalPath)
	if r.batch != nil {
		r.batch.release(int64(len(t.data)))
	}
}

// putHint records the version version-hint.text named in a metadata
// directory (-1: none). Safe on nil.
func (r *exportReads) putHint(dirKey string, n int) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.hints == nil {
		r.hints = make(map[string]int)
	}
	r.hints[dirKey] = n
}

// takeHint removes and returns what putHint recorded. Safe on nil.
func (r *exportReads) takeHint(dirKey string) (int, bool) {
	if r == nil {
		return 0, false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	n, ok := r.hints[dirKey]
	delete(r.hints, dirKey)
	return n, ok
}

// dropHints forgets every recorded hint: something is about to write one, or
// moved a table's row. Safe on nil.
func (r *exportReads) dropHints() {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.hints = nil
}

// tailIO is an iceberg-go FileIO for one data file whose tail is already in
// memory. iceberg-go's FileToDataFile opens the file and reads its trailer
// and footer, all within the tail; a read before it (none is expected) goes
// to the store through under.
type tailIO struct {
	path  string
	tail  fileTail
	under iceio.IO
}

func (t *tailIO) Open(name string) (iceio.File, error) {
	if name != t.path {
		return t.under.Open(name)
	}
	return &tailFile{io: t, rd: tailReader{base: t.tail.base, data: t.tail.data, size: t.tail.size}}, nil
}

func (t *tailIO) Remove(name string) error { return t.under.Remove(name) }

// tailFile is the open file tailIO hands out.
type tailFile struct {
	io   *tailIO
	rd   tailReader
	real iceio.File // opened on the first read before the tail
}

func (f *tailFile) ReadAt(p []byte, off int64) (int, error) {
	if off >= f.rd.base {
		return f.rd.ReadAt(p, off)
	}
	if f.real == nil {
		real, err := f.io.under.Open(f.io.path)
		if err != nil {
			return 0, err
		}
		f.real = real
	}
	return f.real.ReadAt(p, off)
}

func (f *tailFile) Read(p []byte) (int, error) {
	n, err := f.ReadAt(p, f.rd.pos)
	f.rd.pos += int64(n)
	return n, err
}

func (f *tailFile) Seek(offset int64, whence int) (int64, error) { return f.rd.Seek(offset, whence) }

func (f *tailFile) Stat() (fs.FileInfo, error) {
	return tailInfo{name: f.io.path, size: f.rd.size}, nil
}

func (f *tailFile) Close() error {
	if f.real != nil {
		return f.real.Close()
	}
	return nil
}

type tailInfo struct {
	name string
	size int64
}

func (i tailInfo) Name() string       { return i.name }
func (i tailInfo) Size() int64        { return i.size }
func (i tailInfo) Mode() fs.FileMode  { return 0o444 }
func (i tailInfo) ModTime() time.Time { return time.Time{} }
func (i tailInfo) IsDir() bool        { return false }
func (i tailInfo) Sys() any           { return nil }
