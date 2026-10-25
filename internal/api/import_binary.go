package api

import (
	"encoding/json"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
)

// binaryKind is how a Parquet import stores a binary column (#440).
type binaryKind int

const (
	// binaryUndecided: decided by the column's values over the whole file,
	// in the validation pass, before anything is written.
	binaryUndecided binaryKind = iota
	// binaryAsString: every value is valid UTF-8 text, written as a string
	// (Spark and Hive write string columns without a UTF8 annotation).
	binaryAsString
	// binaryAsBytes: written as a binary column, byte for byte.
	binaryAsBytes
	// binaryAsUUIDText: a Parquet UUID column, written as canonical text, as
	// Arc shows a UUID everywhere else.
	binaryAsUUIDText
)

// Extension types a Parquet import reads by their storage. Any other
// extension type is refused as before.
const (
	geoArrowWKBExtension = "geoarrow.wkb"
	arrowUUIDExtension   = "arrow.uuid"
)

// binaryFieldKind reports whether a Parquet import column holds raw bytes
// (Binary, LargeBinary, FixedSizeBinary, GeoArrow WKB) and, for a UUID
// column, that it is one.
func binaryFieldKind(t arrow.DataType) (isBinary bool, kind binaryKind) {
	if ext, ok := t.(arrow.ExtensionType); ok {
		switch ext.ExtensionName() {
		case arrowUUIDExtension:
			return true, binaryAsUUIDText
		case geoArrowWKBExtension:
			return true, binaryAsBytes
		default:
			return false, 0
		}
	}
	switch t.ID() {
	case arrow.BINARY, arrow.LARGE_BINARY, arrow.FIXED_SIZE_BINARY:
		return true, binaryUndecided
	}
	return false, 0
}

// writersThatMarkStrings are Parquet writers (the start of a file's
// created_by) that annotate every string column as UTF8, so an unannotated
// BYTE_ARRAY they wrote is binary data, not text. Arc's own files come from
// two of them: arrow-go ("parquet-go") at ingest and DuckDB at compaction and
// DELETE rewrites, which carry no ARROW:schema.
var writersThatMarkStrings = []string{"parquet-go ", "parquet-cpp-arrow ", "duckdb "}

// parquetBinaryKinds decides how each binary column of a Parquet import is
// stored, from the file's metadata and the measurement's stored columns. Until
// #440 every one became a string, and a column of WKB geometries (or any bytes
// that are not UTF-8) then failed every query that read it.
//
// Bytes, whatever the values: a column with Parquet's GEOMETRY or GEOGRAPHY
// logical type, one the file's GeoParquet "geo" metadata names with WKB
// encoding, any binary column of a file that carries ARROW:schema or was
// written by a writer that annotates its strings, and one the measurement
// already stores as binary. A "geo" column in another encoding is refused.
// Anything else is binaryUndecided, for the validation pass to decide.
//
// "geo" is read from the Parquet file's own metadata: the Arrow schema's
// metadata is empty for files with no ARROW:schema (DuckDB, GDAL). A "geo"
// that is not valid JSON is ignored, as it was before.
func parquetBinaryKinds(fr *pqarrow.FileReader, sc *arrow.Schema, stored map[string]bool) (map[string]binaryKind, *importError) {
	kinds := map[string]binaryKind{}
	for _, f := range sc.Fields() {
		if ok, kind := binaryFieldKind(f.Type); ok {
			kinds[f.Name] = kind
		}
	}
	if len(kinds) == 0 {
		return kinds, nil
	}
	md := fr.ParquetReader().MetaData()
	allBytes := false
	if kv := md.KeyValueMetadata(); kv != nil && kv.FindValue("ARROW:schema") != nil {
		allBytes = true
	}
	createdBy := strings.ToLower(md.GetCreatedBy())
	for _, w := range writersThatMarkStrings {
		if strings.HasPrefix(createdBy, w) {
			allBytes = true
		}
	}
	for name, kind := range kinds {
		if kind != binaryUndecided {
			continue
		}
		switch {
		case allBytes, stored[name]:
			kinds[name] = binaryAsBytes
		default:
			if leaf := md.Schema.ColumnIndexByName(name); leaf >= 0 {
				switch md.Schema.Column(leaf).LogicalType().(type) {
				case schema.GeometryLogicalType, schema.GeographyLogicalType:
					kinds[name] = binaryAsBytes
				}
			}
		}
	}
	kv := md.KeyValueMetadata()
	if kv == nil {
		return kinds, nil
	}
	geo := kv.FindValue("geo")
	if geo == nil {
		return kinds, nil
	}
	var meta struct {
		Columns map[string]struct {
			Encoding string `json:"encoding"`
		} `json:"columns"`
	}
	if err := json.Unmarshal([]byte(*geo), &meta); err != nil {
		return kinds, nil
	}
	for name, c := range meta.Columns {
		if _, ok := kinds[name]; !ok {
			continue // not a binary column of this file (or not in it)
		}
		if !strings.EqualFold(c.Encoding, "WKB") {
			return nil, &importError{StatusCode: fiber.StatusUnprocessableEntity,
				Message: fmt.Sprintf("parquet column %q is a GeoParquet geometry with encoding %q; only WKB is supported", name, c.Encoding)}
		}
		kinds[name] = binaryAsBytes
	}
	return kinds, nil
}

// binaryValue is the i-th value of a binary import chunk, unwrapping an
// extension array to its storage.
func binaryValue(chunk arrow.Array, i int) []byte {
	if ext, ok := chunk.(array.ExtensionArray); ok {
		chunk = ext.Storage()
	}
	switch a := chunk.(type) {
	case *array.Binary:
		return a.Value(i)
	case *array.LargeBinary:
		return a.Value(i)
	case *array.FixedSizeBinary:
		return a.Value(i)
	}
	return nil
}

func checkBinaryChunks(chunks []arrow.Array) error {
	for _, chunk := range chunks {
		c := chunk
		if ext, ok := c.(array.ExtensionArray); ok {
			c = ext.Storage()
		}
		switch c.(type) {
		case *array.Binary, *array.LargeBinary, *array.FixedSizeBinary:
		default:
			return fmt.Errorf("mixed chunk types in binary column: %s", chunk.DataType())
		}
	}
	return nil
}

// binaryColumnHasNonText reports whether a batch of an undecided binary
// column holds a non-NULL value that is not valid UTF-8. The validation pass
// calls it for every batch: one such value makes the column binary.
func binaryColumnHasNonText(rec arrow.RecordBatch, idx int) bool {
	col := rec.Column(idx)
	for i := 0; i < col.Len(); i++ {
		if !col.IsNull(i) && !utf8.Valid(binaryValue(col, i)) {
			return true
		}
	}
	return false
}

// binaryColumnToTyped converts one batch of a binary import column of a
// decided kind (an undecided one is written as text: the validation pass saw
// only text, or only NULLs, in it).
func binaryColumnToTyped(col *arrow.Column, kind binaryKind) (interface{}, []bool, error) {
	if col == nil || col.Data() == nil || len(col.Data().Chunks()) == 0 {
		return nil, nil, fmt.Errorf("column has no data")
	}
	chunks := col.Data().Chunks()
	if err := checkBinaryChunks(chunks); err != nil {
		return nil, nil, err
	}
	length := col.Len()
	var validity []bool
	if col.Data().NullN() > 0 {
		validity = make([]bool, length)
	}
	idx := 0
	if kind == binaryAsBytes {
		res := make([][]byte, length)
		for _, chunk := range chunks {
			for i := 0; i < chunk.Len(); i++ {
				if !chunk.IsNull(i) {
					// Copied: the batch's buffers belong to the reader and
					// are released once the batch is converted, while the
					// rows stay buffered until they are flushed.
					res[idx] = append([]byte{}, binaryValue(chunk, i)...)
					if validity != nil {
						validity[idx] = true
					}
				}
				idx++
			}
		}
		return res, validity, nil
	}
	res := make([]string, length)
	for _, chunk := range chunks {
		for i := 0; i < chunk.Len(); i++ {
			if !chunk.IsNull(i) {
				v := binaryValue(chunk, i)
				if kind == binaryAsUUIDText {
					if len(v) != 16 {
						return nil, nil, fmt.Errorf("UUID value at row %d has %d bytes, want 16", idx+1, len(v))
					}
					res[idx] = uuid.UUID(v).String()
				} else {
					res[idx] = string(v)
				}
				if validity != nil {
					validity[idx] = true
				}
			}
			idx++
		}
	}
	return res, validity, nil
}
