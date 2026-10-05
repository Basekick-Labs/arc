// UNTAGGED on purpose. These helpers have no cgo and no arcx dependency — only
// arrow-go, which the stock build already links. Arc's CI never compiles the
// `arcx_engine` tag (.github/workflows/ci.yml), so anything that lives behind it is
// untested in CI. The truncation bug fixed here (see drainReaderToRecord) shipped
// precisely because its only possible regression test would also have been tagged
// out. Keeping the drain and its cap here means the test runs on every PR.
package arcxrouter

import (
	"errors"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// errShadowTruncated marks a shadow run abandoned because the result exceeded
// `ShadowMaxRows`. Reported as a SKIP, never a mismatch — see drainReaderToRecord.
var errShadowTruncated = errors.New("arcx shadow: result exceeded the shadow row cap")

// errShadowAssembly marks a HOST-side failure to assemble the arcx result (a
// concatenation error). It is neither "arcx is wrong" nor "arcx errored": we failed
// to build the comparison input. Reported as a SKIP so it cannot fire the mismatch
// alarm — gotcha #4 keeps decline, error and mismatch distinguishable, and a host
// bug must not be attributed to the engine.
var errShadowAssembly = errors.New("arcx shadow: could not assemble the arcx result")

// ShadowMaxRows bounds what shadow will materialize. Shadow holds the entire result in
// memory at once, so this is a memory ceiling, not a fairness knob.
//
// Sizing note (measured 2026-10-03 at 907,916 rows, the near-cap case): the drain plus
// concatenation costs ~20 MB (2 cols) to ~45 MB (5 cols) of Go heap, but the COMPARISON
// that follows renders both sides to strings and peaks at ~366 MB (2 cols) / ~749 MB
// (5 cols) of live heap. With `shadowSlots` = 2 that is ~1.5 GB of shadow-only live heap
// at the cap in the DEFAULT router mode. The cap is deliberately kept at 1,000,000: the
// alternative — lowering it — converts real comparisons into skips, and an abandoned
// reader holds its engine-side stream workers and decoded batches until a GC finalizer
// runs (Release is a no-op on the reader), so skipping early does not even reliably
// save engine resources.
const ShadowMaxRows = 1_000_000

// skipPrefix marks a comparator result that is a HOST-side failure to produce the
// comparison input — not an engine disagreement. runShadow reports these as a SKIP
// with a warning, never as ArcxShadowMismatch. Mirrors the existing "argdiff:"
// convention for agg-4 tie divergences.
//
// EVERY comparator's decode-error path must carry it. They all used to return the
// error as a plain diff, so a missing switch arm in Arc's own Go — say arcx grows a
// Decimal128 aggregate and `arcxCell` hits its `unhandled type` default — fired the
// one ERROR alarm that is supposed to mean "arcx computed a wrong answer". That is
// the gotcha-#4 violation this slice exists to close; closing it on one comparator
// and not the other four would have left it live on most shapes.
const skipPrefix = "skip:"

// drainReaderToRecord pulls every batch from a streaming reader and concatenates them into
// ONE arrow.Record (the shadow comparators take a single Record). Each batch is Retained on
// extraction because the reader auto-releases the previous batch on the next Next() (the
// arrow-go C-stream reader contract). The caller MUST runtime.KeepAlive(reader) until after
// this returns — the batches are FFI-backed and free on the reader's GC finalizer. Returns
// an empty-but-schema'd record for a zero-batch result.
//
// `maxRows` bounds the drain: shadow materializes the WHOLE result into one record, and
// an eligible shape need carry no LIMIT (`SELECT host FROM cpu` is eligible), so an
// unbounded drain on a large measurement is an OOM in Arc's own address space. Stopping
// early is safe here — shadow never serves, and a truncated compare is reported as a
// skip, never as a mismatch (a false mismatch alarm would be worse than no signal). The
// bound is enforced INSIDE the drain loop, before any concatenation, so the assembly can
// never exceed it.
//
// WHY per-column `array.Concatenate` and not a Table round-trip: the previous
// implementation built an `array.Table` from the batches and then took a SINGLE record
// from `array.NewTableReader(tbl, tbl.NumRows())`. `TableReader.Next()` clamps its chunk
// size to the current CHUNK boundary (arrow-go `array/table.go`), and
// `NewTableFromRecords` makes one chunk per record — so an N-batch result yielded N
// records and taking the first silently discarded N-1 of them. On the real corpus a
// 50-batch / 60,875-row scan was compared as its first 765 rows, and shadow reported
// `MISMATCH vs DuckDB` for a result arcx had computed correctly. Concatenating per column
// has no chunk semantics to get wrong.
func drainReaderToRecord(reader array.RecordReader, maxRows int) (arrow.Record, error) {
	schema := reader.Schema()
	var batches []arrow.Record
	releaseAll := func() {
		for _, b := range batches {
			b.Release()
		}
	}
	rows := 0
	for reader.Next() {
		b := reader.Record()
		if b == nil {
			break
		}
		if maxRows > 0 && rows+int(b.NumRows()) > maxRows {
			releaseAll()
			return nil, errShadowTruncated
		}
		// Zero-row batches carry nothing and are a known hazard for
		// `array.Concatenate` on dictionary columns; drop them here so the
		// concatenation only ever sees non-empty members.
		if b.NumRows() == 0 {
			continue
		}
		rows += int(b.NumRows())
		b.Retain()
		batches = append(batches, b)
	}
	if err := reader.Err(); err != nil {
		releaseAll()
		return nil, err
	}
	if len(batches) == 0 {
		return emptyRecord(schema), nil
	}
	if len(batches) == 1 {
		return batches[0], nil // caller releases
	}

	cols := make([]arrow.Array, schema.NumFields())
	built := 0
	defer func() {
		// On a failure part-way through, release what we already built.
		if built < len(cols) {
			for i := 0; i < built; i++ {
				cols[i].Release()
			}
		}
	}()
	for i := range cols {
		parts := make([]arrow.Array, len(batches))
		for j, b := range batches {
			parts[j] = b.Column(i)
		}
		c, err := array.Concatenate(parts, memory.DefaultAllocator)
		if err != nil {
			releaseAll()
			// Host-side assembly failure: a SKIP, never a mismatch.
			// %w twice keeps errors.Is(errShadowAssembly) working while reading as
			// one line — errors.Join would embed a newline into the log message.
			return nil, fmt.Errorf("%w: %w", errShadowAssembly, err)
		}
		cols[i] = c
		built++
	}
	releaseAll()
	// Safe against NewRecord's validate(): every column was concatenated from the
	// same field index of records that all carry `schema`, so the types match and
	// the lengths are equal by construction.
	rec := array.NewRecord(schema, cols, int64(rows))
	// NewRecord retains each column, so our refs are handed over here. The defer's
	// guard (`built < len(cols)`) is already false at this point, so it correctly
	// does nothing — the releases below are the only ones that run.
	for _, c := range cols {
		c.Release()
	}
	return rec, nil
}

// emptyRecord builds a zero-row record carrying `schema` (for a fully-filtered result).
// Each column is built via NewBuilder(f.Type) — its empty array's type is EXACTLY the
// schema field's type, so NewRecord's internal validate() cannot panic on a type mismatch
// (the invariant that keeps this off the "no panics in the query path" list). arcx's scan
// result schema is plain columns (dict encoding is reconciled to Utf8 before export), so
// there is no dictionary/extension type here for which an empty builder could disagree.
func emptyRecord(schema *arrow.Schema) arrow.Record {
	cols := make([]arrow.Array, schema.NumFields())
	for i, f := range schema.Fields() {
		b := array.NewBuilder(memory.DefaultAllocator, f.Type)
		cols[i] = b.NewArray()
		b.Release()
	}
	rec := array.NewRecord(schema, cols, 0)
	for _, col := range cols {
		col.Release()
	}
	return rec
}
