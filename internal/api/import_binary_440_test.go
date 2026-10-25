package api

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/stretchr/testify/require"

	// Linked into cmd/arc through iceberg-go: it registers the geoarrow
	// extension types, which changes how pqarrow reads a file declaring them.
	// Imported here so these tests read files as the server does.
	_ "github.com/geoarrow/geoarrow-go"
)

// duckdbParquet writes the result of a SELECT to a Parquet file with DuckDB:
// no ARROW:schema, a BLOB column is BYTE_ARRAY without a UTF8 annotation
// (as Spark and GDAL write), and kv is the file's key-value metadata.
func duckdbParquet(t *testing.T, sel, kv string) []byte {
	t.Helper()
	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	defer db.Close()
	p := filepath.Join(t.TempDir(), "f.parquet")
	opts := "FORMAT PARQUET"
	if kv != "" {
		opts += ", KV_METADATA {" + kv + "}"
	}
	_, err = db.Exec(fmt.Sprintf("COPY (%s) TO '%s' (%s)", sel, p, opts))
	require.NoError(t, err)
	data, err := os.ReadFile(p)
	require.NoError(t, err)
	return data
}

// storedColumn flushes and returns every stored value of column name, as
// Go values (string, []byte or nil), and the Arrow types it was written with.
func (ci *chunkedImport) storedColumn(t *testing.T, name string) ([]interface{}, map[string]bool) {
	t.Helper()
	require.NoError(t, ci.buf.FlushAllAndWait(context.Background()))
	var vals []interface{}
	types := map[string]bool{}
	require.NoError(t, filepath.WalkDir(ci.dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(p, ".parquet") {
			return err
		}
		f, err := os.Open(p)
		require.NoError(t, err)
		defer f.Close()
		pf, err := file.NewParquetReader(f)
		require.NoError(t, err)
		defer pf.Close()
		fr, err := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
		require.NoError(t, err)
		tbl, err := fr.ReadTable(context.Background())
		require.NoError(t, err)
		defer tbl.Release()
		idx := tbl.Schema().FieldIndices(name)
		if len(idx) == 0 {
			return nil
		}
		col := tbl.Column(idx[0])
		types[col.DataType().String()] = true
		for _, chunk := range col.Data().Chunks() {
			for i := 0; i < chunk.Len(); i++ {
				switch {
				case chunk.IsNull(i):
					vals = append(vals, nil)
				case chunk.DataType().ID() == arrow.STRING:
					vals = append(vals, chunk.(*array.String).Value(i))
				default:
					vals = append(vals, append([]byte{}, chunk.(*array.Binary).Value(i)...))
				}
			}
		}
		return nil
	}))
	return vals, types
}

const wkbPoint = "0101000000000000000000F03F0000000000000040"

// A GeoParquet file's WKB geometry column is imported as binary, byte for
// byte (#440). It used to become a string of invalid UTF-8 that every query
// reading the column failed on.
func TestParquetImportKeepsGeoParquetGeometryAsBinary(t *testing.T) {
	ci := newChunkedImport(t, 1000)
	data := duckdbParquet(t, "SELECT TIMESTAMP '2026-10-25 10:00:00' AS time, from_hex('"+wkbPoint+"') AS geom, 'depot' AS name",
		`'geo': '{"version":"1.1.0","primary_column":"geom","columns":{"geom":{"encoding":"WKB","geometry_types":["Point"]}}}'`)
	status, body := ci.post(t, "/api/v1/import/parquet?db=d&measurement=places", data)
	require.Equal(t, http.StatusOK, status, body)
	vals, types := ci.storedColumn(t, "geom")
	require.Equal(t, map[string]bool{"binary": true}, types)
	want := []byte{0x01, 0x01, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xf0, 0x3f, 0, 0, 0, 0, 0, 0, 0, 0x40}
	require.Len(t, vals, 1)
	require.True(t, bytes.Equal(vals[0].([]byte), want), "geom = %x", vals[0])
	_, nameTypes := ci.storedColumn(t, "name")
	require.Equal(t, map[string]bool{"utf8": true}, nameTypes)
}

// The "geo" metadata decides, not the bytes: WKB for POINT(0 0) is all bytes
// below 0x80, valid UTF-8, and without the metadata would import as text.
func TestParquetImportGeoParquetPointAtOriginIsBinary(t *testing.T) {
	ci := newChunkedImport(t, 1000)
	data := duckdbParquet(t, "SELECT TIMESTAMP '2026-10-25 10:00:00' AS time, from_hex('010100000000000000000000000000000000000000') AS geom",
		`'geo': '{"version":"1.1.0","primary_column":"geom","columns":{"geom":{"encoding":"WKB","geometry_types":[]}}}'`)
	// A writer Arc does not know (same length, the footer stays valid): the
	// "geo" metadata alone decides.
	data = bytes.Replace(data, []byte("DuckDB version"), []byte("GDALxx version"), 1)
	status, body := ci.post(t, "/api/v1/import/parquet?db=d&measurement=places", data)
	require.Equal(t, http.StatusOK, status, body)
	_, types := ci.storedColumn(t, "geom")
	require.Equal(t, map[string]bool{"binary": true}, types)
}

// unmarkedParquet writes a Parquet file the way Spark does: no ARROW:schema,
// strings as BYTE_ARRAY without a UTF8 annotation (written here as Binary),
// and a created_by Arc does not know to mark its strings. rows of raw are
// given per row; nil is NULL. Row groups of rg rows.
func unmarkedParquet(t *testing.T, raw [][]byte, rg int) []byte {
	t.Helper()
	sc := arrow.NewSchema([]arrow.Field{{Name: "time", Type: arrow.FixedWidthTypes.Timestamp_us}, {Name: "raw", Type: arrow.BinaryTypes.Binary, Nullable: true}}, nil)
	b := array.NewRecordBuilder(memory.DefaultAllocator, sc)
	defer b.Release()
	for i, v := range raw {
		b.Field(0).(*array.TimestampBuilder).Append(arrow.Timestamp(1761386400000000 + int64(i)*1e6))
		if v == nil {
			b.Field(1).(*array.BinaryBuilder).AppendNull()
		} else {
			b.Field(1).(*array.BinaryBuilder).Append(v)
		}
	}
	rec := b.NewRecord()
	defer rec.Release()
	var out bytes.Buffer
	props := parquet.NewWriterProperties(parquet.WithCreatedBy("parquet-mr version 1.13.1 (build abc)"), parquet.WithMaxRowGroupLength(int64(rg)))
	w, err := pqarrow.NewFileWriter(sc, &out, props, pqarrow.DefaultWriterProps())
	require.NoError(t, err)
	require.NoError(t, w.Write(rec))
	require.NoError(t, w.Close())
	return out.Bytes()
}

// From a writer that does not mark its strings (Spark, Hive), a binary
// column of text throughout is a string, as before; one value that is not
// UTF-8 anywhere in the file makes it binary, decided before anything is
// written so no batch is refused part-way. All NULLs stays a string column.
func TestParquetImportDecidesTextOrBinaryOverTheFile(t *testing.T) {
	setVar(t, &parquetBatchRows, 2)
	cases := []struct {
		name string
		raw  [][]byte
		want string
	}{
		{"text", [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d")}, "utf8"},
		{"text then bytes", [][]byte{[]byte("a"), []byte("b"), []byte("c"), {0x00, 0xff}}, "binary"},
		{"nulls then bytes", [][]byte{nil, nil, {0x00, 0xff}, {0x80}}, "binary"},
		{"nulls only", [][]byte{nil, nil, nil, nil}, "utf8"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ci := newChunkedImport(t, 1)
			status, body := ci.post(t, "/api/v1/import/parquet?db=d&measurement=m", unmarkedParquet(t, tc.raw, 2))
			require.Equal(t, http.StatusOK, status, body)
			vals, types := ci.storedColumn(t, "raw")
			require.Equal(t, map[string]bool{tc.want: true}, types)
			require.Len(t, vals, len(tc.raw))
		})
	}
}

// A measurement that already stores the column as binary keeps it binary,
// even when the file's values happen to be text.
func TestParquetImportFollowsAStoredBinaryColumn(t *testing.T) {
	ci := newChunkedImport(t, 1000)
	ci.buf.SetFieldSchema(lpTextBinary{"d/m": {"raw": true}})
	status, body := ci.post(t, "/api/v1/import/parquet?db=d&measurement=m", unmarkedParquet(t, [][]byte{[]byte("text")}, 10))
	require.Equal(t, http.StatusOK, status, body)
	_, types := ci.storedColumn(t, "raw")
	require.Equal(t, map[string]bool{"binary": true}, types)
}

// Files Arc writes at compaction and DELETE rewrites come from DuckDB and
// carry no ARROW:schema; DuckDB marks its strings, so an unmarked column of
// text-like bytes in them is binary, and Arc's files round-trip.
func TestParquetImportOfADuckDBFileKeepsBinary(t *testing.T) {
	ci := newChunkedImport(t, 1000)
	data := duckdbParquet(t, "SELECT TIMESTAMP '2026-10-25 10:00:00' AS time, 'plain text'::BLOB AS raw, 'label' AS label", "")
	status, body := ci.post(t, "/api/v1/import/parquet?db=d&measurement=m", data)
	require.Equal(t, http.StatusOK, status, body)
	raw, rawTypes := ci.storedColumn(t, "raw")
	require.Equal(t, map[string]bool{"binary": true}, rawTypes)
	require.Equal(t, []byte("plain text"), raw[0])
	_, labelTypes := ci.storedColumn(t, "label")
	require.Equal(t, map[string]bool{"utf8": true}, labelTypes)
}

// A Parquet UUID column is imported as canonical text, as Arc shows a UUID
// everywhere else (it was refused as an unsupported type).
func TestParquetImportOfAUUIDColumnIsText(t *testing.T) {
	ci := newChunkedImport(t, 1000)
	data := duckdbParquet(t, "SELECT TIMESTAMP '2026-10-25 10:00:00' AS time, '6f1c2c1e-8a3b-4d2e-9f00-1234567890ab'::UUID AS id", "")
	status, body := ci.post(t, "/api/v1/import/parquet?db=d&measurement=m", data)
	require.Equal(t, http.StatusOK, status, body)
	vals, types := ci.storedColumn(t, "id")
	require.Equal(t, map[string]bool{"utf8": true}, types)
	require.Equal(t, "6f1c2c1e-8a3b-4d2e-9f00-1234567890ab", vals[0])
}

// A file Arc (or pyarrow) wrote carries ARROW:schema, which types its string
// columns as strings: a binary column in it stays binary even when its bytes
// happen to be text, so Arc's own files round-trip between instances.
// LargeBinary is imported like Binary.
func TestParquetImportOfAnArrowSchemaFileKeepsBinary(t *testing.T) {
	for _, typ := range []arrow.DataType{arrow.BinaryTypes.Binary, arrow.BinaryTypes.LargeBinary} {
		t.Run(typ.String(), func(t *testing.T) {
			ci := newChunkedImport(t, 1000)
			sc := arrow.NewSchema([]arrow.Field{{Name: "time", Type: arrow.FixedWidthTypes.Timestamp_us}, {Name: "token", Type: typ, Nullable: true}}, nil)
			b := array.NewRecordBuilder(memory.DefaultAllocator, sc)
			defer b.Release()
			b.Field(0).(*array.TimestampBuilder).Append(arrow.Timestamp(1700000000000000))
			switch tb := b.Field(1).(type) {
			case *array.BinaryBuilder:
				tb.Append([]byte("ascii"))
			}
			rec := b.NewRecord()
			defer rec.Release()
			var out bytes.Buffer
			// A writer Arc does not know: the Arrow schema alone decides.
			props := parquet.NewWriterProperties(parquet.WithCreatedBy("parquet-mr version 1.13.1 (build abc)"))
			w, err := pqarrow.NewFileWriter(sc, &out, props, pqarrow.NewArrowWriterProperties(pqarrow.WithStoreSchema()))
			require.NoError(t, err)
			require.NoError(t, w.Write(rec))
			require.NoError(t, w.Close())
			status, body := ci.post(t, "/api/v1/import/parquet?db=d&measurement=m", out.Bytes())
			require.Equal(t, http.StatusOK, status, body)
			vals, types := ci.storedColumn(t, "token")
			require.Equal(t, map[string]bool{"binary": true}, types)
			require.Equal(t, []byte("ascii"), vals[0])
		})
	}
}

// A GeoParquet column in an encoding other than WKB (GeoArrow native) is
// refused before anything is written.
func TestParquetImportRefusesNonWKBGeoParquet(t *testing.T) {
	ci := newChunkedImport(t, 1000)
	data := duckdbParquet(t, "SELECT TIMESTAMP '2026-10-25 10:00:00' AS time, from_hex('"+wkbPoint+"') AS geom",
		`'geo': '{"version":"1.1.0","primary_column":"geom","columns":{"geom":{"encoding":"point","geometry_types":[]}}}'`)
	status, body := ci.post(t, "/api/v1/import/parquet?db=d&measurement=places", data)
	require.Equal(t, http.StatusUnprocessableEntity, status, body)
	require.Contains(t, fmt.Sprint(body), "only WKB")
	require.Empty(t, ci.writes)
}

// A file that uses Parquet's own GEOMETRY type (written by pyarrow, GDAL,
// DuckDB) is imported as plain binary even without "geo" metadata and from a
// writer Arc does not know: the logical type decides, not the bytes. WKB for
// POINT(100 200) is valid UTF-8 and would otherwise import as text. The
// stored file reads back as BLOB in DuckDB.
func TestParquetImportOfParquetGeometryTypeIsPlainBinary(t *testing.T) {
	ci := newChunkedImport(t, 1000)
	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	defer db.Close()
	p := filepath.Join(t.TempDir(), "none.parquet")
	_, err = db.Exec(fmt.Sprintf(`COPY (SELECT TIMESTAMP '2026-10-25 10:00:00' AS time, 'POINT(100 200)'::GEOMETRY AS geom) TO '%s' (FORMAT PARQUET, GEOPARQUET_VERSION 'NONE')`, p))
	require.NoError(t, err)
	data, err := os.ReadFile(p)
	require.NoError(t, err)
	// A writer Arc does not know marks its strings: same length, so the
	// footer stays valid.
	require.Equal(t, 1, bytes.Count(data, []byte("DuckDB version")))
	data = bytes.Replace(data, []byte("DuckDB version"), []byte("GDALxx version"), 1)
	status, body := ci.post(t, "/api/v1/import/parquet?db=d&measurement=places", data)
	require.Equal(t, http.StatusOK, status, body)
	_, types := ci.storedColumn(t, "geom")
	require.Equal(t, map[string]bool{"binary": true}, types)
	require.NoError(t, ci.buf.FlushAllAndWait(context.Background()))
	var files []string
	require.NoError(t, filepath.WalkDir(ci.dir, func(p string, d fs.DirEntry, err error) error {
		if err == nil && !d.IsDir() && strings.HasSuffix(p, ".parquet") {
			files = append(files, p)
		}
		return err
	}))
	require.NotEmpty(t, files)
	var typ string
	require.NoError(t, db.QueryRow(fmt.Sprintf("SELECT typeof(geom) FROM read_parquet('%s')", files[0])).Scan(&typ))
	require.Equal(t, "BLOB", typ)
}
