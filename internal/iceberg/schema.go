package iceberg

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	iceberg "github.com/apache/iceberg-go"
	"golang.org/x/sync/errgroup"

	"github.com/basekick-labs/arc/internal/storage"
)

// SchemaFromParquet derives an ArcSchema (typed Iceberg columns) by reading a Parquet file's
// footer — pure Go, no DuckDB handle. Used by the reconciler to build/evolve the Iceberg
// table for a measurement. localPath must be a readable local file (Arc's hot tier is local;
// vortex/cold reads are out of scope — the reconciler samples a local hot file per measurement).
//
// Type mapping (Arc/Arrow physical -> Iceberg):
//
//	INT64                         -> long
//	TIMESTAMP (tz set, UTC-adj)   -> timestamptz   (Arc's `time`; MUST be tz — see eval doc)
//	TIMESTAMP (no tz)             -> timestamp
//	FLOAT64                       -> double
//	STRING                        -> string
//	BINARY (msgpack bin, #440)    -> binary
//	BOOL                          -> boolean
//	DECIMAL128(p,s)               -> decimal(p,s)
func SchemaFromParquet(localPath string) (ArcSchema, error) {
	f, err := os.Open(localPath)
	if err != nil {
		return ArcSchema{}, fmt.Errorf("open parquet %q: %w", localPath, err)
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return ArcSchema{}, fmt.Errorf("stat parquet %q: %w", localPath, err)
	}
	n, err := footerLen(f, st.Size(), localPath)
	if err != nil {
		return ArcSchema{}, err
	}
	if err := checkFooterLen(n, localPath); err != nil {
		return ArcSchema{}, err
	}
	return schemaFromReader(f, localPath)
}

// footerWindow is how much of a remote file's tail SchemaFromBackend reads
// first: Arc's footers are a few KiB, so one read normally holds the whole
// footer; a larger one costs a second read.
var footerWindow int64 = 64 << 10

// SchemaFromBackend derives a remote Parquet file's schema from its footer,
// reading only the file's tail through the backend (a ranged GET from the
// footer to the end of the object, ReadToAt), never the whole object. size
// is the file's size from the listing; a tail that does not match it (the
// file was replaced or truncated since) is an error, not a guess.
func SchemaFromBackend(ctx context.Context, backend storage.Backend, key string, size int64) (ArcSchema, error) {
	if size < parquetTrailerLen+int64(len(parquetMagic)) {
		return ArcSchema{}, fmt.Errorf("parquet %q: %d bytes is too small for a Parquet file", key, size)
	}
	off := size - footerWindow
	if off < 0 {
		off = 0
	}
	tail, err := readTail(ctx, backend, key, size, off)
	if err != nil {
		return ArcSchema{}, err
	}
	trailer := tail[len(tail)-parquetTrailerLen:]
	if string(trailer[4:]) != parquetMagic {
		return ArcSchema{}, fmt.Errorf("parquet %q: no Parquet magic at the end of the file", key)
	}
	need := int64(binary.LittleEndian.Uint32(trailer[:4])) + parquetTrailerLen
	if err := checkFooterLen(need-parquetTrailerLen, key); err != nil {
		return ArcSchema{}, err
	}
	if need > size-int64(len(parquetMagic)) {
		return ArcSchema{}, fmt.Errorf("parquet %q: footer length %d does not fit a %d-byte file", key, need-parquetTrailerLen, size)
	}
	if need > int64(len(tail)) {
		off = size - need
		if tail, err = readTail(ctx, backend, key, size, off); err != nil {
			return ArcSchema{}, err
		}
	}
	return schemaFromReader(&tailReader{base: off, data: tail, size: size}, key)
}

const (
	parquetMagic      = "PAR1"
	parquetTrailerLen = 8 // footer length (uint32 LE) + magic
)

// readTail reads key from off to its end and checks the result ends where
// the listing said the file does.
func readTail(ctx context.Context, backend storage.Backend, key string, size, off int64) ([]byte, error) {
	var buf bytes.Buffer
	buf.Grow(int(size - off))
	if err := backend.ReadToAt(ctx, key, &buf, off); err != nil {
		return nil, fmt.Errorf("read footer of %q: %w", key, err)
	}
	if got := int64(buf.Len()); got != size-off {
		return nil, fmt.Errorf("read footer of %q: read %d bytes from offset %d, want %d (the file changed since it was listed)", key, got, off, size-off)
	}
	return buf.Bytes(), nil
}

// tailReader serves reads within a file's tail [base, size) for the Parquet
// footer parser; a read before base is an error (the parser asked for more
// than the footer, which a schema read never needs).
type tailReader struct {
	base int64
	data []byte
	size int64
	pos  int64
}

func (r *tailReader) ReadAt(p []byte, off int64) (int, error) {
	if off < r.base {
		return 0, fmt.Errorf("parquet footer read at offset %d, before the %d bytes read from the tail", off, r.base)
	}
	if off >= r.size {
		return 0, io.EOF
	}
	n := copy(p, r.data[off-r.base:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (r *tailReader) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
	case io.SeekCurrent:
		offset += r.pos
	case io.SeekEnd:
		offset += r.size
	default:
		return 0, fmt.Errorf("tailReader: bad whence %d", whence)
	}
	if offset < 0 {
		return 0, fmt.Errorf("tailReader: negative position %d", offset)
	}
	r.pos = offset
	return offset, nil
}

// newParquetReader opens a Parquet file's footer; a test seam.
var newParquetReader = file.NewParquetReader

// maxFooterBytes bounds the footer schemaFromReader hands the Parquet
// library. Arc's footers are a few KiB and a file with thousands of row
// groups a few MiB; the bound keeps a malformed footer's nesting from
// recursing the library's schema builder past the stack limit, a fatal error
// no recover reaches.
var maxFooterBytes int64 = 16 << 20

// footerLen reads the footer length from a Parquet file's trailer.
func footerLen(f io.ReaderAt, size int64, name string) (int64, error) {
	if size < parquetTrailerLen+int64(len(parquetMagic)) {
		return 0, fmt.Errorf("parquet %q: %d bytes is too small for a Parquet file", name, size)
	}
	var trailer [parquetTrailerLen]byte
	if _, err := f.ReadAt(trailer[:], size-parquetTrailerLen); err != nil {
		return 0, fmt.Errorf("parquet %q: read trailer: %w", name, err)
	}
	if string(trailer[4:]) != parquetMagic {
		return 0, fmt.Errorf("parquet %q: no Parquet magic at the end of the file", name)
	}
	return int64(binary.LittleEndian.Uint32(trailer[:4])), nil
}

// checkFooterLen refuses a footer over maxFooterBytes.
func checkFooterLen(n int64, name string) error {
	if n > maxFooterBytes {
		return fmt.Errorf("parquet %q: footer of %d bytes is over the %d-byte limit", name, n, maxFooterBytes)
	}
	return nil
}

// schemaFromReader derives the ArcSchema from a Parquet file's footer. name
// labels errors.
//
// A malformed footer can panic inside the Parquet library rather than return
// an error (a length field it trusts for an allocation, for one). The files
// read here include ones Arc did not write itself, EdgeSync spokes' among
// them, and the reads run on the reconcile goroutine and on footer-reading
// worker goroutines, where a panic ends the process; and the file stays in
// storage, so every restart would reach it again. The panic is returned as
// this file's error instead, and the measurement fails its pass.
func schemaFromReader(f parquet.ReaderAtSeeker, name string) (sc ArcSchema, err error) {
	defer func() {
		if r := recover(); r != nil {
			sc, err = ArcSchema{}, fmt.Errorf("parquet %q: reading its footer panicked (%v); the file is malformed, and this measurement's export stays blocked until it is removed", name, r)
		}
	}()
	rdr, err := newParquetReader(f)
	if err != nil {
		return ArcSchema{}, fmt.Errorf("parquet reader %q: %w", name, err)
	}
	defer rdr.Close()

	fr, err := pqarrow.NewFileReader(rdr, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	if err != nil {
		return ArcSchema{}, fmt.Errorf("pqarrow reader %q: %w", name, err)
	}
	arrowSchema, err := fr.Schema()
	if err != nil {
		return ArcSchema{}, fmt.Errorf("arrow schema %q: %w", name, err)
	}

	fields := make([]ArcField, 0, len(arrowSchema.Fields()))
	for _, af := range arrowSchema.Fields() {
		it, err := arrowToIceberg(af.Type)
		if err != nil {
			return ArcSchema{}, fmt.Errorf("column %q: %w", af.Name, err)
		}
		fields = append(fields, ArcField{Name: af.Name, Type: it})
	}
	return ArcSchema{Fields: fields}, nil
}

// unionSchemaConcurrency bounds parallel footer reads in UnionSchema. Small enough to avoid
// file-descriptor exhaustion on a measurement with many files, large enough to hide per-file
// open+footer latency.
const unionSchemaConcurrency = 8

// UnionSchema derives the union of column schemas across multiple local Parquet files.
// Arc's ingest is schema-flexible: files for the same measurement can have different column
// sets over time (a metric added later). The Iceberg table must carry the SUPERSET so both
// narrow and wide files pass AddFiles (a column absent from a file reads as NULL, since all
// fields are optional). Column order follows first-seen; a name seen again must have a
// matching type (a genuine type conflict is returned as an error rather than silently picked).
//
// Footers are read in parallel (bounded) because on first-sight/full-derivation this reads EVERY
// local footer, which is sequential I/O that can exceed the reconciler's measurementTimeout on a
// measurement with thousands of files. ctx aborts the remaining reads early on the first failure
// or on reconciler shutdown/timeout, instead of grinding through every file. The merge stays
// deterministic: results are collected by index and folded in localPaths order, so column
// ordering and conflict detection are unaffected by scheduling. (The incremental path in the
// scheduler only passes newly-added files here.)
func UnionSchema(ctx context.Context, localPaths []string) (ArcSchema, error) {
	if len(localPaths) == 0 {
		return ArcSchema{}, nil
	}
	if len(localPaths) == 1 {
		return SchemaFromParquet(localPaths[0])
	}

	schemas := make([]ArcSchema, len(localPaths))
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(unionSchemaConcurrency)
	for i, p := range localPaths {
		i, p := i, p
		g.Go(func() error {
			if err := gctx.Err(); err != nil {
				return err // cancelled by an earlier failure or by the caller's timeout
			}
			sc, err := SchemaFromParquet(p)
			if err != nil {
				return fmt.Errorf("file %q: %w", p, err)
			}
			schemas[i] = sc
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return ArcSchema{}, err
	}

	seen := make(map[string]iceberg.Type)
	var order []string
	for i := range localPaths {
		for _, f := range schemas[i].Fields {
			if prev, ok := seen[f.Name]; ok {
				if !prev.Equals(f.Type) {
					return ArcSchema{}, fmt.Errorf("column %q has conflicting types across files: %s vs %s", f.Name, prev, f.Type)
				}
				continue
			}
			seen[f.Name] = f.Type
			order = append(order, f.Name)
		}
	}
	fields := make([]ArcField, len(order))
	for i, name := range order {
		fields[i] = ArcField{Name: name, Type: seen[name]}
	}
	return ArcSchema{Fields: fields}, nil
}

// MergeSchemas unions two already-derived ArcSchemas, preserving `base`'s column order and
// appending any names that appear only in `add`. A name present in both must carry the same
// type — a genuine conflict is an error (matching UnionSchema's semantics). This lets the
// reconciler extend a cached schema with only the newly-added files' schemas, avoiding an O(N)
// re-read of every footer when a measurement's file set grows.
func MergeSchemas(base, add ArcSchema) (ArcSchema, error) {
	seen := make(map[string]iceberg.Type, len(base.Fields)+len(add.Fields))
	order := make([]string, 0, len(base.Fields)+len(add.Fields))
	for _, f := range base.Fields {
		seen[f.Name] = f.Type
		order = append(order, f.Name)
	}
	for _, f := range add.Fields {
		if prev, ok := seen[f.Name]; ok {
			if !prev.Equals(f.Type) {
				return ArcSchema{}, fmt.Errorf("column %q has conflicting types across files: %s vs %s", f.Name, prev, f.Type)
			}
			continue
		}
		seen[f.Name] = f.Type
		order = append(order, f.Name)
	}
	fields := make([]ArcField, len(order))
	for i, name := range order {
		fields[i] = ArcField{Name: name, Type: seen[name]}
	}
	return ArcSchema{Fields: fields}, nil
}

// arrowToIceberg maps one Arrow type to its Iceberg type per the table above.
func arrowToIceberg(t arrow.DataType) (iceberg.Type, error) {
	switch dt := t.(type) {
	case *arrow.Int64Type:
		return iceberg.Int64Type{}, nil
	case *arrow.Int32Type:
		// Arc's own ingest only emits Int64/Float64/String/Boolean/Timestamp_us, but Parquet
		// files can also arrive via the bulk import path (internal/api/import.go) carrying
		// externally-produced int32/float32 columns. Without these, such a measurement fails
		// the whole export with "unsupported Arrow type". Iceberg has native 32-bit int/float.
		return iceberg.Int32Type{}, nil
	case *arrow.Float64Type:
		return iceberg.Float64Type{}, nil
	case *arrow.Float32Type:
		return iceberg.Float32Type{}, nil
	case *arrow.StringType:
		return iceberg.StringType{}, nil
	case *arrow.BinaryType:
		return iceberg.BinaryType{}, nil
	case *arrow.BooleanType:
		return iceberg.BooleanType{}, nil
	case *arrow.TimestampType:
		// A non-empty TimeZone means the column is UTC-adjusted (Arc writes Timestamp_us with
		// UTC), which Iceberg represents as timestamptz. Declaring plain `timestamp` for such a
		// column makes AddFiles fail with a schema mismatch (verified in Phase 0b).
		if dt.TimeZone != "" {
			return iceberg.TimestampTzType{}, nil
		}
		return iceberg.TimestampType{}, nil
	case *arrow.Decimal128Type:
		return iceberg.DecimalTypeOf(int(dt.Precision), int(dt.Scale)), nil
	default:
		return nil, fmt.Errorf("unsupported Arrow type for Iceberg export: %s", t)
	}
}
