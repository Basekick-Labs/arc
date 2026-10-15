package api

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/gofiber/fiber/v2"
)

// A line-protocol import is parsed and written in chunks of about
// lpChunkBytes, so its memory does not grow with the file: a parsed line costs
// about 760 bytes, and a whole 164 MB file parsed at once took 3.8 GB (#1070
// follow-up). A var for tests.
var lpChunkBytes = 4 << 20

// lpKind is the type a column gets (ingest.LPKind): a field's column is
// typed by its first value in the file, as it was when the whole file was one
// write.
type lpKind = ingest.LPKind

const (
	lpNone   = ingest.LPKindNone
	lpInt    = ingest.LPKindInt
	lpFloat  = ingest.LPKindFloat
	lpString = ingest.LPKindString
	lpBool   = ingest.LPKindBool
)

// lpScan is what the validation pass learned about a line-protocol file.
type lpScan struct {
	bytes        int64
	records      int64
	measurements []string
	kinds        map[string]map[string]lpKind // measurement -> column -> kind
}

// scanLPImport is the validation pass: it parses the file chunk by chunk,
// keeping nothing but the measurements and each column's kind, and refuses
// before anything is written what the write pass would fail on part-way.
// Columns are named as BatchToColumnar names them: a field that shares its
// record's tag name is "<name>_value", and tags are string columns. A column
// isDecimal reports is converted to decimal from any value, so it is not
// typed here.
func scanLPImport(upload *importUpload, precision, filter string, isDecimal func(measurement, column string) bool) (*lpScan, *importError) {
	r, err := upload.openMaybeGzip()
	if err != nil {
		return nil, &importError{StatusCode: fiber.StatusBadRequest, Message: err.Error()}
	}
	scan := &lpScan{kinds: map[string]map[string]lpKind{}}
	parser := ingest.NewLineProtocolParser()
	var bad *importError
	refuse := func(format string, args ...interface{}) error {
		bad = &importError{StatusCode: fiber.StatusBadRequest, Message: fmt.Sprintf(format, args...)}
		return errStopScan
	}
	err = forEachLineChunk(r, lpChunkBytes, func(chunk []byte) error {
		scan.bytes += int64(len(chunk))
		return parser.LexBatch(chunk, precision, func(l *ingest.LPLine) error {
			if filter != "" && l.Measurement != filter {
				return nil
			}
			scan.records++
			if kind, found := ingest.LPLineTimeKey(l); found {
				return refuse("measurement %q has a %s named \"time\"; time is the timestamp and cannot be a tag or field key", l.Measurement, kind)
			}
			for _, t := range l.Tags {
				if ingest.IsWALTargetColumnName(t.Key) {
					return refuse("measurement %q has a tag named %q, a name kept for internal use", l.Measurement, t.Key)
				}
			}
			for _, f := range l.Fields {
				if ingest.IsWALTargetColumnName(f.Key) {
					return refuse("measurement %q has a field named %q, a name kept for internal use", l.Measurement, f.Key)
				}
			}
			if column, dup := ingest.LPLineColumnClash(l); dup {
				return refuse("measurement %q has two keys on one line that both go to column %q (a field named like a tag goes to \"<name>_value\")", l.Measurement, column)
			}
			columns, seen := scan.kinds[l.Measurement]
			if !seen {
				if !isValidMeasurementName(l.Measurement) {
					return refuse("invalid measurement name %q in LP data: must start with a letter and contain only alphanumeric characters, underscores, or hyphens", l.Measurement)
				}
				columns = map[string]lpKind{}
				scan.kinds[l.Measurement] = columns
				scan.measurements = append(scan.measurements, l.Measurement)
			}
			observe := func(column string, v interface{}) error {
				if isDecimal != nil && isDecimal(l.Measurement, column) {
					return nil
				}
				k, ok := columns[column]
				if !ok {
					k = ingest.LPKindOf(v)
					columns[column] = k
				}
				if !ingest.LPValueFits(k, v) {
					return refuse("column %q of measurement %q has %s values and a %T value %v that cannot be stored with them", column, l.Measurement, k, v, v)
				}
				return nil
			}
			for _, t := range l.Tags {
				if err := observe(t.Key, t.Value); err != nil {
					return err
				}
			}
			for _, f := range l.Fields {
				if err := observe(ingest.LPFieldColumn(l, f.Key), f.Value); err != nil {
					return err
				}
			}
			return nil
		})
	})
	switch {
	case bad != nil:
		return nil, bad
	case errors.Is(err, errImportLineTooLong):
		return nil, &importError{StatusCode: fiber.StatusBadRequest, Message: fmt.Sprintf("a line is longer than %d bytes", maxImportLineBytes)}
	case errors.Is(err, errImportTooLarge):
		return nil, &importError{StatusCode: fiber.StatusRequestEntityTooLarge, Message: "file exceeds 500MB limit"}
	case err != nil:
		return nil, &importError{StatusCode: fiber.StatusBadRequest, Message: "failed to read the uploaded file", Err: err}
	case scan.bytes == 0:
		return nil, &importError{StatusCode: fiber.StatusBadRequest, Message: "file is empty"}
	case scan.records == 0 && filter != "":
		return nil, &importError{StatusCode: fiber.StatusBadRequest, Message: fmt.Sprintf("no records found for measurement %q", filter)}
	case scan.records == 0:
		return nil, &importError{StatusCode: fiber.StatusBadRequest, Message: "no valid line protocol records found in file"}
	}
	return scan, nil
}

var errStopScan = errors.New("stop scan")

// writeLPImport is the write pass: chunk by chunk, parse, group by
// measurement, write, then wait for flushes to keep up.
func writeLPImport(ctx context.Context, w *importWriter, upload *importUpload, database, precision, filter string, kinds map[string]map[string]lpKind) error {
	r, err := upload.openMaybeGzip()
	if err != nil {
		return err
	}
	parser := ingest.NewLineProtocolParser()
	if !w.buf.HasDecimalColumns() {
		// Parsed straight into typed columns: no per-line maps or boxed
		// values (a line used to cost about 760 bytes).
		err = forEachLineChunk(r, lpChunkBytes, func(chunk []byte) error {
			batches, rows, err := parser.ParseLPTyped(chunk, precision, filter, kinds)
			if err != nil {
				return err
			}
			names := make([]string, 0, len(batches))
			for m := range batches {
				names = append(names, m)
			}
			sort.Strings(names)
			for _, m := range names {
				if err := w.add(ctx, m, batches[m], rows[m]); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
		return w.end(ctx)
	}
	// Decimal columns (ingest.decimal_columns) are converted by the buffer's
	// typing, from any value: records written as they are, chunk by chunk,
	// not grouped by hour. Grouping writes typed batches, whose WAL entries
	// hold a decimal as a float; written as records, the WAL keeps the
	// value as sent.
	return forEachLineChunk(r, lpChunkBytes, func(chunk []byte) error {
		records := parser.ParseBatchWithPrecision(chunk, precision)
		if filter != "" {
			kept := records[:0]
			for _, rec := range records {
				if rec.Measurement == filter {
					kept = append(kept, rec)
				}
			}
			records = kept
		}
		if len(records) == 0 {
			return nil
		}
		byMeasurement := ingest.BatchToColumnar(records)
		names := make([]string, 0, len(byMeasurement))
		for m := range byMeasurement {
			names = append(names, m)
		}
		sort.Strings(names)
		for _, m := range names {
			rec := byMeasurement[m]
			ingest.CoerceFirstValues(rec, kinds[m])
			if err := w.buf.WriteColumnarRecord(ctx, database, rec); err != nil {
				if w.started() {
					return w.partial(err, m)
				}
				return err
			}
			w.wrote(m, int64(len(rec.Columns["time"])))
		}
		if err := w.pace(ctx); err != nil {
			return w.partial(err, "")
		}
		return nil
	})
}
