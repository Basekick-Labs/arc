package api

import (
	"encoding/json"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/gofiber/fiber/v2"
)

// binaryKind is how a Parquet import stores a binary column (#440).
type binaryKind int

const (
	// binaryUndecided: decided by the column's first non-NULL values.
	binaryUndecided binaryKind = iota
	// binaryAsString: every value is valid UTF-8 text, written as a string
	// (Spark and Hive write string columns without a UTF8 annotation).
	binaryAsString
	// binaryAsBytes: written as a binary column, byte for byte.
	binaryAsBytes
)

// isBinaryField reports whether a Parquet import column holds raw bytes:
// Binary, LargeBinary or FixedSizeBinary, or an extension type (GeoArrow
// WKB) stored as one.
func isBinaryField(t arrow.DataType) bool {
	if ext, ok := t.(arrow.ExtensionType); ok {
		t = ext.StorageType()
	}
	switch t.ID() {
	case arrow.BINARY, arrow.LARGE_BINARY, arrow.FIXED_SIZE_BINARY:
		return true
	}
	return false
}

// parquetBinaryKinds decides, before anything is written, how each binary
// column of a Parquet import is stored. Until #440 every one became a string,
// and a column of WKB geometries (or any bytes that are not UTF-8) then failed
// every query that read it.
//
//   - A column the file's GeoParquet "geo" metadata names with WKB encoding
//     is bytes; one with another encoding (GeoArrow native) is refused.
//   - A file that carries ARROW:schema (every file Arc and pyarrow write) has
//     its string columns typed as strings already, so a binary column is
//     bytes: Arc's files round-trip.
//   - Any other is decided by its first non-NULL values (binaryUndecided).
//
// The "geo" key is read from the Parquet file's own metadata: the Arrow
// schema's metadata is empty for files with no ARROW:schema (DuckDB, GDAL).
func parquetBinaryKinds(fr *pqarrow.FileReader, schema *arrow.Schema) (map[string]binaryKind, *importError) {
	kinds := map[string]binaryKind{}
	for _, f := range schema.Fields() {
		if isBinaryField(f.Type) {
			kinds[f.Name] = binaryUndecided
		}
	}
	if len(kinds) == 0 {
		return kinds, nil
	}
	kv := fr.ParquetReader().MetaData().KeyValueMetadata()
	if kv != nil && kv.FindValue("ARROW:schema") != nil {
		for name := range kinds {
			kinds[name] = binaryAsBytes
		}
	}
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
		return nil, &importError{StatusCode: fiber.StatusUnprocessableEntity, Message: "the file's GeoParquet \"geo\" metadata is not valid JSON", Err: err}
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

// binaryColumnToTyped converts one batch of a binary import column. An
// undecided kind is decided here by the batch's non-NULL values and written
// back: valid UTF-8 throughout is a string column, anything else bytes. A
// batch with no non-NULL value returns nil values (the caller leaves the
// column out of that batch, so it reads NULL) and stays undecided. A string
// column whose later value is not valid UTF-8 is an error: written as a string
// it could not be read back.
func binaryColumnToTyped(col *arrow.Column, kind *binaryKind) (interface{}, []bool, error) {
	if col == nil || col.Data() == nil || len(col.Data().Chunks()) == 0 {
		return nil, nil, fmt.Errorf("column has no data")
	}
	chunks := col.Data().Chunks()
	value := func(chunk arrow.Array, i int) []byte {
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
	for _, chunk := range chunks {
		c := chunk
		if ext, ok := c.(array.ExtensionArray); ok {
			c = ext.Storage()
		}
		switch c.(type) {
		case *array.Binary, *array.LargeBinary, *array.FixedSizeBinary:
		default:
			return nil, nil, fmt.Errorf("mixed chunk types in binary column: %s", chunk.DataType())
		}
	}
	if *kind == binaryUndecided {
		seen, text := false, true
		for _, chunk := range chunks {
			for i := 0; i < chunk.Len() && text; i++ {
				if chunk.IsNull(i) {
					continue
				}
				seen = true
				text = utf8.Valid(value(chunk, i))
			}
		}
		if !seen {
			return nil, nil, nil
		}
		if text {
			*kind = binaryAsString
		} else {
			*kind = binaryAsBytes
		}
	}
	length := col.Len()
	var validity []bool
	if col.Data().NullN() > 0 {
		validity = make([]bool, length)
	}
	idx := 0
	if *kind == binaryAsString {
		res := make([]string, length)
		for _, chunk := range chunks {
			for i := 0; i < chunk.Len(); i++ {
				if !chunk.IsNull(i) {
					v := value(chunk, i)
					if !utf8.Valid(v) {
						return nil, nil, fmt.Errorf("value at row %d is not valid UTF-8 text, and earlier rows of the column were; a column must be all text or all binary", idx+1)
					}
					res[idx] = string(v)
					if validity != nil {
						validity[idx] = true
					}
				}
				idx++
			}
		}
		return res, validity, nil
	}
	res := make([][]byte, length)
	for _, chunk := range chunks {
		for i := 0; i < chunk.Len(); i++ {
			if !chunk.IsNull(i) {
				// Copied: the batch's buffers are released after conversion.
				res[idx] = append([]byte{}, value(chunk, i)...)
				if validity != nil {
					validity[idx] = true
				}
			}
			idx++
		}
	}
	return res, validity, nil
}
