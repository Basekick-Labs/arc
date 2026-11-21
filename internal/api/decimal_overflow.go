//go:build duckdb_arrow

package api

import (
	"errors"
	"fmt"
	"math"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// decimalOverflowError is a scale-0 decimal result value that does not fit the
// int64 Arc sends such columns as (normalizeDecimalSchema): DuckDB's sum() of
// any integer column is a HUGEINT, exported as decimal(38,0), and a column
// stored with ingest.decimal_columns can be wider than 18 digits too. It is the
// client's query that asks for the value, so it is answered as a 400 when the
// response has not started yet. The text names the column and the SQL cast
// that returns the value, and never the query (plannings#49).
type decimalOverflowError struct {
	Column    string
	Precision int32
	Row       int64 // 0-based, counted from the first row of the result
}

func (e *decimalOverflowError) Error() string {
	return fmt.Sprintf(`%s: column "%s" (DECIMAL(%d,0)/HUGEINT) at row %d; cast it in SQL: ::DOUBLE (approximate) or ::VARCHAR (exact digits)`,
		decimalOverflowPrefix, e.Column, e.Precision, e.Row)
}

// wideScaleZeroDecimal reports whether a field is a decimal128 with scale 0
// and more than 18 digits: the only decimals whose values can fall outside
// int64. Other decimal widths keep the plain cast path.
func wideScaleZeroDecimal(dt arrow.DataType) bool {
	d, ok := dt.(*arrow.Decimal128Type)
	return ok && d.Scale == 0 && d.Precision > 18
}

// hasWideScaleZeroDecimal reports whether any field of the schema is one.
func hasWideScaleZeroDecimal(schema *arrow.Schema) bool {
	for _, f := range schema.Fields() {
		if wideScaleZeroDecimal(f.Type) {
			return true
		}
	}
	return false
}

// firstDecimalOverflow scans the wide scale-0 decimal columns of the first
// limit rows of batch for a value outside int64. It reads the 128-bit values
// in place: nothing is allocated, and a null slot is skipped whatever bytes
// lie under it.
func firstDecimalOverflow(batch arrow.Record, limit int64) (*decimalOverflowError, bool) {
	if limit > batch.NumRows() {
		limit = batch.NumRows()
	}
	schema := batch.Schema()
	for c := 0; c < int(batch.NumCols()); c++ {
		f := schema.Field(c)
		if !wideScaleZeroDecimal(f.Type) {
			continue
		}
		col, ok := batch.Column(c).(*array.Decimal128)
		if !ok {
			continue
		}
		for i := 0; i < int(limit); i++ {
			if col.IsNull(i) {
				continue
			}
			v := col.Value(i)
			hi, lo := v.HighBits(), v.LowBits()
			if (hi == 0 && lo <= math.MaxInt64) || (hi == -1 && lo >= 1<<63) {
				continue
			}
			return &decimalOverflowError{
				Column:    f.Name,
				Precision: f.Type.(*arrow.Decimal128Type).Precision,
				Row:       int64(i),
			}, true
		}
	}
	return nil, false
}

// castDecimalBatchAt is castDecimalBatch for a batch whose first row is row
// offset of the result. A failure caused by a value outside int64 comes back
// as a decimalOverflowError with the value's row; any other failure is
// returned as castDecimalBatch returned it.
func castDecimalBatchAt(batch arrow.Record, info *decimalCastInfo, offset int64) (arrow.Record, error) {
	rec, err := castDecimalBatch(batch, info)
	if err == nil {
		return rec, nil
	}
	if overflow, ok := firstDecimalOverflow(batch, batch.NumRows()); ok {
		overflow.Row += offset
		return nil, overflow
	}
	return nil, err
}

// wrapDecimalCastError leaves a decimalOverflowError as it is, since it
// already names its row, and wraps any other cast failure the way the caller
// always has.
func wrapDecimalCastError(err error, format string, args ...any) error {
	var overflow *decimalOverflowError
	if errors.As(err, &overflow) {
		return err
	}
	return fmt.Errorf(format+": %w", append(args, err)...)
}

// peekedRecordReader replays the outcome of a Next its constructor already
// called, then passes every call through. The batch it returns first is the
// reader's current one, which the underlying reader keeps until the next
// Next, so nothing has to be retained.
type peekedRecordReader struct {
	array.RecordReader
	pending bool
	first   bool
}

func (r *peekedRecordReader) Next() bool {
	if r.pending {
		r.pending = false
		return r.first
	}
	return r.RecordReader.Next()
}

// peekDecimalOverflow checks the first batch of a result that has a wide
// scale-0 decimal column for a value outside int64, before anything about the
// response is set, so the query can be answered as a 400 instead of a stream
// that is cut after it started (#1784). Only the rows the governance cap lets
// through are checked (maxRows <= 0: no cap). The returned reader replays the
// first batch, so the stream sees exactly what it would have; on a
// decimalOverflowError the caller owns the reader and must release it.
//
// Anything else the first Next meets (the end of the result, a deadline, a
// conversion error) is left for the stream to meet as it always has. A value
// that first appears in a later batch is not seen here and still cuts the
// stream, which then names this error.
func peekDecimalOverflow(reader array.RecordReader, maxRows int) (array.RecordReader, error) {
	if reader == nil || !hasWideScaleZeroDecimal(reader.Schema()) {
		return reader, nil
	}
	ok := reader.Next()
	peeked := &peekedRecordReader{RecordReader: reader, pending: true, first: ok}
	if !ok {
		return peeked, nil
	}
	batch := reader.RecordBatch()
	if batch == nil {
		return peeked, nil
	}
	limit := batch.NumRows()
	if maxRows > 0 && int64(maxRows) < limit {
		limit = int64(maxRows)
	}
	if overflow, found := firstDecimalOverflow(batch, limit); found {
		return peeked, overflow
	}
	return peeked, nil
}
