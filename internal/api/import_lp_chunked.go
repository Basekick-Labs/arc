package api

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/big"
	"sort"

	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/basekick-labs/arc/pkg/models"
	"github.com/gofiber/fiber/v2"
)

// A line-protocol import is parsed and written in chunks of about
// lpChunkBytes, so its memory does not grow with the file: a parsed line costs
// about 760 bytes, and a whole 164 MB file parsed at once took 3.8 GB (#1070
// follow-up). A var for tests.
var lpChunkBytes = 4 << 20

// lpKind is the type a field's column gets: the buffer types a column from
// its first value, so the first value in the file decides it, as it did when
// the whole file was one write.
type lpKind uint8

const (
	lpNone lpKind = iota
	lpInt
	lpFloat
	lpString
	lpBool
)

func (k lpKind) String() string {
	switch k {
	case lpInt:
		return "integer"
	case lpFloat:
		return "float"
	case lpString:
		return "string"
	case lpBool:
		return "boolean"
	}
	return "none"
}

func lpKindOf(v interface{}) lpKind {
	switch v.(type) {
	case int64, uint64:
		return lpInt
	case float64:
		return lpFloat
	case string:
		return lpString
	case bool:
		return lpBool
	}
	return lpNone
}

// lpValueFits reports whether v can be stored in a column of kind k, with
// the conversions the buffer applies (a float in an integer column is
// truncated, an integer in a float column widened).
func lpValueFits(k lpKind, v interface{}) bool {
	switch k {
	case lpInt:
		switch x := v.(type) {
		case int64:
			return true
		case uint64:
			return x <= math.MaxInt64
		case float64:
			return !math.IsNaN(x) && !math.IsInf(x, 0) && x < 1<<63 && x >= -1<<63
		}
	case lpFloat:
		switch v.(type) {
		case int64, uint64, float64:
			return true
		}
	case lpString:
		_, ok := v.(string)
		return ok
	case lpBool:
		_, ok := v.(bool)
		return ok
	}
	return false
}

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
	err = forEachLineChunk(r, lpChunkBytes, func(chunk []byte) error {
		scan.bytes += int64(len(chunk))
		for _, rec := range parser.ParseBatchWithPrecision(chunk, precision) {
			if filter != "" && rec.Measurement != filter {
				continue
			}
			scan.records++
			fields, seen := scan.kinds[rec.Measurement]
			if !seen {
				if !isValidMeasurementName(rec.Measurement) {
					bad = &importError{StatusCode: fiber.StatusBadRequest, Message: fmt.Sprintf("invalid measurement name %q in LP data: must start with a letter and contain only alphanumeric characters, underscores, or hyphens", rec.Measurement)}
					return errStopScan
				}
				fields = map[string]lpKind{}
				scan.kinds[rec.Measurement] = fields
				scan.measurements = append(scan.measurements, rec.Measurement)
			}
			observe := func(column string, v interface{}) bool {
				if isDecimal != nil && isDecimal(rec.Measurement, column) {
					return true
				}
				k, ok := fields[column]
				if !ok {
					k = lpKindOf(v)
					fields[column] = k
				}
				if !lpValueFits(k, v) {
					bad = &importError{StatusCode: fiber.StatusBadRequest, Message: fmt.Sprintf("column %q of measurement %q has %s values and a %T value %v that cannot be stored with them", column, rec.Measurement, k, v, v)}
					return false
				}
				return true
			}
			for name, v := range rec.Tags {
				if !observe(name, v) {
					return errStopScan
				}
			}
			for name, v := range rec.Fields {
				column := name
				if _, isTag := rec.Tags[name]; isTag {
					column = name + "_value"
				}
				if !observe(column, v) {
					return errStopScan
				}
			}
		}
		return nil
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
			coerceFirstValues(rec, kinds[m])
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

// coerceFirstValues converts the first value of each field column to the kind
// the file's first value of that field had: the buffer types a column from
// its first value, and a chunk must type it as the whole file would.
func coerceFirstValues(rec *models.ColumnarRecord, kinds map[string]lpKind) {
	for name, k := range kinds {
		col := rec.Columns[name]
	first:
		for i, v := range col {
			if v == nil {
				continue
			}
			switch k {
			case lpInt:
				// The validation pass checked the values fit; the bounds are
				// checked again at the conversion. A value that does not fit is
				// left as is, and the buffer refuses it.
				switch x := v.(type) {
				case float64:
					// float64(MaxInt64) is 2^63, so "<" is the exact bound;
					// NaN fails both comparisons. big.Float.Int64 truncates
					// toward zero as int64(x) does in range.
					if x < math.MaxInt64 && x >= math.MinInt64 {
						col[i], _ = new(big.Float).SetFloat64(x).Int64()
					}
				case uint64:
					if x <= math.MaxInt64 {
						col[i] = int64(x)
					}
				}
			case lpFloat:
				switch x := v.(type) {
				case int64:
					col[i] = float64(x)
				case uint64:
					col[i] = float64(x)
				}
			}
			break first
		}
	}
}
