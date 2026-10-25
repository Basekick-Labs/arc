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
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/stretchr/testify/require"
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

// Without "geo": bytes that are not UTF-8 are binary; a column whose values
// are all text stays a string (Spark and Hive write strings that way).
func TestParquetImportDecidesTextOrBinary(t *testing.T) {
	ci := newChunkedImport(t, 1000)
	data := duckdbParquet(t, "SELECT TIMESTAMP '2026-10-25 10:00:00' AS time, from_hex('00FF80') AS raw, 'plain text'::BLOB AS label", "")
	status, body := ci.post(t, "/api/v1/import/parquet?db=d&measurement=m", data)
	require.Equal(t, http.StatusOK, status, body)
	raw, rawTypes := ci.storedColumn(t, "raw")
	require.Equal(t, map[string]bool{"binary": true}, rawTypes)
	require.Equal(t, []byte{0x00, 0xff, 0x80}, raw[0])
	label, labelTypes := ci.storedColumn(t, "label")
	require.Equal(t, map[string]bool{"utf8": true}, labelTypes)
	require.Equal(t, "plain text", label[0])
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
			w, err := pqarrow.NewFileWriter(sc, &out, nil, pqarrow.NewArrowWriterProperties(pqarrow.WithStoreSchema()))
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

// The kind is fixed by the first batch with a value: a batch of NULLs leaves
// it open, and a later value that is not text in a column decided as text is
// refused (written as a string it could not be read), reporting what was
// written.
func TestParquetImportBinaryKindAcrossBatches(t *testing.T) {
	setVar(t, &parquetBatchRows, 2)
	t.Run("nulls first", func(t *testing.T) {
		ci := newChunkedImport(t, 1)
		data := duckdbParquet(t, `SELECT TIMESTAMP '2026-10-25 10:00:00' + to_seconds(i) AS time,
			CASE WHEN i < 2 THEN NULL ELSE from_hex('00FF') END AS raw FROM range(4) t(i) ORDER BY i`, "")
		status, body := ci.post(t, "/api/v1/import/parquet?db=d&measurement=m", data)
		require.Equal(t, http.StatusOK, status, body)
		vals, types := ci.storedColumn(t, "raw")
		require.Equal(t, map[string]bool{"binary": true}, types)
		nonNull := 0
		for _, v := range vals {
			if v != nil {
				nonNull++
				require.Equal(t, []byte{0x00, 0xff}, v)
			}
		}
		require.Equal(t, 2, nonNull)
	})
	t.Run("text then bytes", func(t *testing.T) {
		ci := newChunkedImport(t, 1)
		data := duckdbParquet(t, `SELECT TIMESTAMP '2026-10-25 10:00:00' + to_seconds(i) AS time,
			CASE WHEN i < 2 THEN 'text'::BLOB ELSE from_hex('00FF') END AS raw FROM range(4) t(i) ORDER BY i`, "")
		status, body := ci.post(t, "/api/v1/import/parquet?db=d&measurement=m", data)
		require.NotEqual(t, http.StatusOK, status, body)
		require.Contains(t, fmt.Sprint(body), "not valid UTF-8")
	})
}
