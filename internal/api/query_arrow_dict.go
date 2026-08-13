//go:build duckdb_arrow

package api

import (
	"fmt"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
)

// Arrow IPC dictionary encoding for low-cardinality string columns (opt-in
// via the x-arc-arrow-dictionary request header).
//
// DuckDB's Arrow export materializes VARCHAR columns as plain utf8 arrays, so
// repeated tag-like values (symbols, hostnames, sides) are re-sent in full for
// every row — measured 39 bytes/row on the trades egress benchmark vs 18.8
// for engines that dictionary-encode the stream. This transformer re-encodes
// qualifying string columns as dictionary<int32, utf8> with ONE persistent
// dictionary builder per column for the whole stream. The IPC writer re-sends
// a column's dictionary only when it has grown since the previous batch
// (replacement dictionaries — NOT delta dictionaries: polars cannot read
// delta batches, verified against polars 1.43; replacements are read
// correctly by pyarrow, polars, and pandas). Once the dictionary stabilizes
// it is transmitted no further, and every batch carries 4-byte indices
// instead of repeated strings.
//
// Column selection is adaptive and decided ONCE on the first record batch
// (the IPC schema is fixed for the stream): a string column qualifies when
// its unique-value count in the first batch stays under both an absolute cap
// and a fraction of the batch rows — high-cardinality columns (URLs, IDs)
// would make dictionaries larger than the plain encoding and are left alone.
//
// COST HONESTY: if a qualifying column's cardinality explodes later in the
// stream (typical shape: ORDER BY on a medium-cardinality column, whose
// first batch shows few uniques), correctness is preserved but the persistent
// dictionary grows without bound for the stream's lifetime, every batch
// re-materializes and re-compares the full dictionary (O(batches × uniques)
// CPU), and the client receives the cumulative dictionary. transform() logs
// one warning per stream when growth blows past the qualification cap; the
// release notes carry the operator-facing caveat.
//
// Only utf8 (String) columns are considered. LargeString is deliberately
// EXCLUDED: arrow-go v18 has no LargeString dictionary builder — constructing
// one panics — and the pinned duckdb-go never emits LargeString. The stream
// closure also carries a recover() as insurance, since it runs on a bare
// fasthttp goroutine where an unrecovered panic kills the process.

// isTruthyHeader reports whether an opt-in header value is enabled.
func isTruthyHeader(v string) bool {
	return strings.EqualFold(v, "true") || v == "1"
}

// dictMaxCardinality is the absolute unique-count cap for a column to
// qualify on the first batch.
const dictMaxCardinality = 4096

// dictMinRows is the minimum first-batch row count to bother analyzing —
// tiny result sets gain nothing from dictionary encoding.
const dictMinRows = 256

// dictGrowthWarnCap: transform() logs one warning per stream when any
// column's persistent dictionary exceeds this size — the signal that a
// column qualified on an unrepresentative first batch (see COST HONESTY
// above).
const dictGrowthWarnCap = 16 * dictMaxCardinality

// arrowDictTransformer rewrites qualifying string columns of each record
// batch into dictionary arrays with stream-persistent dictionaries.
type arrowDictTransformer struct {
	schema     *arrow.Schema
	builders   map[int]array.DictionaryBuilder // column index -> persistent builder
	logger     *zerolog.Logger
	growthWarn bool
}

// newArrowDictTransformer analyzes the first batch and returns a transformer
// for qualifying string columns, or nil if no column qualifies (stream is
// then written untouched). logger may be nil (tests).
func newArrowDictTransformer(first arrow.Record, logger *zerolog.Logger) *arrowDictTransformer {
	rows := int(first.NumRows())
	if rows < dictMinRows {
		return nil
	}
	// A column qualifies when its uniques fit the absolute cap AND repeat
	// enough to pay for the dictionary (uniques <= rows/4).
	limit := rows / 4
	if limit > dictMaxCardinality {
		limit = dictMaxCardinality
	}

	schema := first.Schema()
	var dictCols map[int]array.DictionaryBuilder

	for i := 0; i < int(first.NumCols()); i++ {
		f := schema.Field(i)
		var uniques int
		switch col := first.Column(i).(type) {
		case *array.String:
			uniques = countUniqueStrings(col, limit)
		default:
			// LargeString deliberately excluded — no dictionary builder for
			// it in arrow-go v18 (constructing one panics). See file header.
			continue
		}
		if uniques < 0 {
			continue // exceeded limit — leave plain
		}
		if dictCols == nil {
			dictCols = make(map[int]array.DictionaryBuilder)
		}
		dt := &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int32, ValueType: f.Type}
		dictCols[i] = array.NewDictionaryBuilder(memory.DefaultAllocator, dt)
	}

	if dictCols == nil {
		return nil
	}

	fields := make([]arrow.Field, schema.NumFields())
	for i := 0; i < schema.NumFields(); i++ {
		f := schema.Field(i)
		if _, ok := dictCols[i]; ok {
			f = arrow.Field{
				Name:     f.Name,
				Type:     &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int32, ValueType: f.Type},
				Nullable: f.Nullable,
				Metadata: f.Metadata,
			}
		}
		fields[i] = f
	}
	md := schema.Metadata()
	return &arrowDictTransformer{
		schema:   arrow.NewSchema(fields, &md),
		builders: dictCols,
		logger:   logger,
	}
}

// transform returns a new record with the qualifying string columns
// dictionary-encoded. The caller owns the returned record and must Release
// it after writing.
func (t *arrowDictTransformer) transform(batch arrow.Record) (arrow.Record, error) {
	cols := make([]arrow.Array, batch.NumCols())
	var toRelease []arrow.Array

	for i := 0; i < int(batch.NumCols()); i++ {
		b, ok := t.builders[i]
		if !ok {
			cols[i] = batch.Column(i)
			continue
		}
		if err := b.AppendArray(batch.Column(i)); err != nil {
			for _, a := range toRelease {
				a.Release()
			}
			return nil, fmt.Errorf("dictionary-encode column %d: %w", i, err)
		}
		arr := b.NewDictionaryArray()
		cols[i] = arr
		toRelease = append(toRelease, arr)

		// One warning per stream when a column's dictionary blows past the
		// qualification cap — the first batch was unrepresentative and this
		// stream is now paying the growth cost described in the file header.
		if !t.growthWarn && t.logger != nil && arr.Dictionary().Len() > dictGrowthWarnCap {
			t.growthWarn = true
			t.logger.Warn().
				Str("column", t.schema.Field(i).Name).
				Int("dictionary_size", arr.Dictionary().Len()).
				Int("qualify_cap", dictMaxCardinality).
				Msg("Arrow dictionary column grew far beyond its first-batch cardinality; consider omitting x-arc-arrow-dictionary for this query shape")
		}
	}

	rec := array.NewRecord(t.schema, cols, batch.NumRows())
	for _, a := range toRelease {
		a.Release()
	}
	return rec, nil
}

// release frees the persistent builders. Call once when the stream ends.
func (t *arrowDictTransformer) release() {
	for _, b := range t.builders {
		b.Release()
	}
}

// countUniqueStrings returns the unique count, or -1 as soon as it exceeds
// limit (early abort keeps first-batch analysis cheap for high-cardinality
// columns).
func countUniqueStrings(col *array.String, limit int) int {
	seen := make(map[string]struct{}, limit+1)
	for i := 0; i < col.Len(); i++ {
		if col.IsNull(i) {
			continue
		}
		seen[col.Value(i)] = struct{}{}
		if len(seen) > limit {
			return -1
		}
	}
	return len(seen)
}
