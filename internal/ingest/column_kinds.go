package ingest

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/basekick-labs/arc/pkg/models"
)

// ErrColumnTypeChange is returned for a write that would change a column
// between string and binary (#440). DuckDB binds a VARCHAR column and a BLOB
// column of the same name across files as BLOB, so one such write would turn
// every stored string of the column into bytes for every query; it is refused
// instead, before the WAL, so a refused write is never replayed.
var ErrColumnTypeChange = errors.New("column type change refused")

// textBinarySource is implemented by a registrar that can tell, without I/O,
// which of a measurement's stored STRING and BINARY columns are BINARY.
type textBinarySource interface {
	KnownTextBinaryColumns(database, measurement string) map[string]bool
}

// knownTextBinary is the stored string/binary kind of the measurement's
// columns (true = binary), or nil when there is no registry that can answer
// or the measurement's anchor is not in memory: the check is best-effort.
func (b *ArrowBuffer) knownTextBinary(database, measurement string) map[string]bool {
	src, ok := b.fieldSchema.(textBinarySource)
	if !ok || src == nil {
		return nil
	}
	return src.KnownTextBinaryColumns(database, measurement)
}

func columnTypeChangeError(measurement, name string, storedBinary bool) error {
	stored, incoming := "string", "binary"
	if storedBinary {
		stored, incoming = "binary", "string"
	}
	return fmt.Errorf("%w: measurement %q column %s is stored as %s; a %s value would change it for every row already written",
		ErrColumnTypeChange, measurement, strconv.Quote(name), stored, incoming)
}

// valueTextBinary is a value's kind: isBinary, and ok when it is a string
// or binary at all.
func valueTextBinary(v interface{}) (isBinary, ok bool) {
	switch v.(type) {
	case string:
		return false, true
	case []byte:
		return true, true
	}
	return false, false
}

// checkColumnsTextBinary refuses a generic columnar write that would change a
// column between string and binary. A column's kind is its first non-NULL
// value's; values that disagree within one column are the conversion's
// mixed-type error, as for any other type.
func checkColumnsTextBinary(known map[string]bool, measurement string, columns map[string][]interface{}) error {
	if len(known) == 0 {
		return nil
	}
	for name, col := range columns {
		storedBinary, tracked := known[name]
		if !tracked {
			continue
		}
		for _, v := range col {
			if v == nil {
				continue
			}
			if isBinary, ok := valueTextBinary(v); ok && isBinary != storedBinary {
				return columnTypeChangeError(measurement, name, storedBinary)
			}
			break
		}
	}
	return nil
}

// checkTypedTextBinary is checkColumnsTextBinary for a typed batch.
func checkTypedTextBinary(known map[string]bool, measurement string, batch *TypedColumnBatch) error {
	if len(known) == 0 || batch == nil {
		return nil
	}
	for name, col := range batch.Data {
		storedBinary, tracked := known[name]
		if !tracked {
			continue
		}
		switch col.(type) {
		case []string:
			if storedBinary {
				return columnTypeChangeError(measurement, name, storedBinary)
			}
		case [][]byte:
			if !storedBinary {
				return columnTypeChangeError(measurement, name, storedBinary)
			}
		}
	}
	return nil
}

// checkBatchTextBinary checks every record of a Write batch before any of it
// is written, so a refused batch writes nothing.
func (b *ArrowBuffer) checkBatchTextBinary(database string, records []interface{}) error {
	if _, ok := b.fieldSchema.(textBinarySource); !ok {
		return nil
	}
	for _, record := range records {
		switch r := record.(type) {
		case *models.ColumnarRecord:
			if err := checkColumnsTextBinary(b.knownTextBinary(database, r.Measurement), r.Measurement, r.Columns); err != nil {
				return err
			}
		case *TypedColumnarRecord:
			if err := checkTypedTextBinary(b.knownTextBinary(database, r.Measurement), r.Measurement, r.Batch); err != nil {
				return err
			}
		case *models.Record:
			known := b.knownTextBinary(database, r.Measurement)
			if len(known) == 0 {
				continue
			}
			for name, v := range r.Fields {
				storedBinary, tracked := known[name]
				if !tracked || v == nil {
					continue
				}
				if isBinary, ok := valueTextBinary(v); ok && isBinary != storedBinary {
					return columnTypeChangeError(r.Measurement, name, storedBinary)
				}
			}
			for name := range r.Tags {
				if known[name] {
					return columnTypeChangeError(r.Measurement, name, true)
				}
			}
		}
	}
	return nil
}
