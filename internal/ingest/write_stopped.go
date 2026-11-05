package ingest

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/basekick-labs/arc/pkg/models"
)

// WriteStoppedError is a multi-measurement write that failed after part of it
// was buffered (#1437). The cause alone would read as "nothing was written":
// a refusal says so in its header, and a 503 tells clients to resend, which
// writes the buffered part again. Handlers answer it with what was written
// instead.
//
// It deliberately has no Unwrap: errors.Is/As must not see the cause through
// it. IsWriteRefusal(cause) is true for a not-durable WAL entry, and callers
// that resend a refused write whole (the MQTT subscriber) would then duplicate
// the buffered part. Read the cause with Cause.
type WriteStoppedError struct {
	Err         error
	RowsWritten int64
	// RecordsWritten is what was written in the unit the request's record
	// count uses: rows, or for MessagePack columnar records, records (the
	// decoder's unit). For line protocol it equals RowsWritten.
	RecordsWritten int64
	// Written are the measurements with rows buffered, sorted, each once. A
	// MessagePack batch can hold several records of one measurement, so
	// StoppedAt can be in Written too.
	Written []string
	// StoppedAt is the measurement of the part whose write failed: a line-
	// protocol measurement, or one MessagePack record. That part and every
	// part after it in the request were not buffered.
	StoppedAt string
	// Drops are points the write left out for good: refused for their content,
	// or rows that could not be converted. Not written, and not to be resent.
	Drops []LPDrop
}

func (e *WriteStoppedError) Error() string {
	return fmt.Sprintf("write stopped part-way at measurement %q: %v. %d rows of %s were written; "+
		"the part of the request it stopped at, and every part after it, were not buffered "+
		"(with the WAL enabled, the part it stopped at may still come back from the WAL at the next start)",
		e.StoppedAt, e.Err, e.RowsWritten, strings.Join(e.Written, ", "))
}

// Cause is the error the write stopped on.
func (e *WriteStoppedError) Cause() error { return e.Err }

// AsWriteStopped reports whether err is a write that stopped part-way.
func AsWriteStopped(err error) (*WriteStoppedError, bool) {
	var ws *WriteStoppedError
	return ws, errors.As(err, &ws)
}

// isContentRefusal reports an error the buffer raises for a measurement's
// content before its WAL append: a string/binary column change (#440), a
// column name that cannot be stored (#1342), a column value that cannot be
// converted to its column's type (#1424), or a measurement name its keys
// cannot hold (#1475). Only that measurement is
// refused; nothing of it reached the WAL, so the rest of a request can be
// written and the measurement reported as dropped.
func isContentRefusal(err error) bool {
	return errors.Is(err, ErrColumnTypeChange) || errors.Is(err, ErrUnstorableColumnName) ||
		errors.Is(err, ErrUnstorableColumnValue) || errors.Is(err, ErrUnstorableMeasurementName)
}

// IsContentRefusal is isContentRefusal for the write handlers.
func IsContentRefusal(err error) bool { return isContentRefusal(err) }

// batchUnit returns the measurement and row count of write unit i of a Write
// batch: the columnar and typed records of recordList by index, then the
// converted row records. A row record in recordList is not a unit of its own.
func batchUnit(recordList []interface{}, converted []*models.ColumnarRecord, i int) (string, int) {
	if i >= len(recordList) {
		c := converted[i-len(recordList)]
		return c.Measurement, len(c.Columns["time"])
	}
	switch r := recordList[i].(type) {
	case *models.ColumnarRecord:
		return r.Measurement, len(r.Columns["time"])
	case *TypedColumnarRecord:
		return r.Measurement, r.NumRecords
	}
	return "", 0
}

// writtenUnits returns, for a Write that reached unit stop, the measurements
// buffered before it (sorted, each once), their rows, and the records in the
// decoder's unit (see unitRecords). Units refused for their content are not
// written. Called on the failure path only.
func writtenUnits(recordList []interface{}, converted []*models.ColumnarRecord, stop int, contentDropped []int) ([]string, int64, int64) {
	dropped := make(map[int]bool, len(contentDropped))
	for _, i := range contentDropped {
		dropped[i] = true
	}
	seen := map[string]bool{}
	var names []string
	var rows, records int64
	for i := 0; i < stop; i++ {
		if dropped[i] {
			continue
		}
		m, n := batchUnit(recordList, converted, i)
		if m == "" {
			continue // a row record, written as part of converted
		}
		rows += int64(n)
		records += unitRecords(recordList, i, n)
		if !seen[m] {
			seen[m] = true
			names = append(names, m)
		}
	}
	sort.Strings(names)
	return names, rows, records
}

// unitRecords is unit i's size in the unit the MessagePack decoder counts a
// request in: one per columnar or typed record, a row per converted row
// record (rows is the unit's row count).
func unitRecords(recordList []interface{}, i, rows int) int64 {
	if i < len(recordList) {
		return 1
	}
	return int64(rows)
}
