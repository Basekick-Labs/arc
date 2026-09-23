package ingest

import (
	"bytes"
	"context"
	"testing"

	"github.com/Basekick-Labs/msgpack/v6"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/pkg/models"
	"github.com/rs/zerolog"
)

func TestBinaryColumnarRoundTripIssue440(t *testing.T) {
	payload, err := msgpack.Marshal(map[string]interface{}{
		"m": "geo",
		"columns": map[string]interface{}{
			"time": []interface{}{
				int64(1700000000000000),
				int64(1700000000000001),
				int64(1700000000000002),
			},
			"geom": []interface{}{
				[]byte{0x00, 0xff, 0x80},
				nil,
				[]byte{},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	decoder := NewMessagePackDecoder(zerolog.Nop())
	decoder.SetTypedDecodeEnabled(true)

	decoded, err := decoder.Decode(payload)
	if err != nil {
		t.Fatal(err)
	}
	results, ok := decoded.([]interface{})
	if !ok || len(results) != 1 {
		t.Fatalf("unexpected decode result: %T", decoded)
	}
	rec, ok := results[0].(*models.ColumnarRecord)
	if !ok {
		t.Fatalf("expected generic fallback, got %T", results[0])
	}

	// Binary is deliberately decoded by the generic path.
	if decoder.typedHits.Load() != 0 {
		t.Fatal("binary payload unexpectedly used the typed fast path")
	}

	buffer := createTestArrowBuffer(t)
	batch, count, err := buffer.convertColumnsToTyped(rec.Measurement, rec.Columns)
	if err != nil {
		t.Fatalf("binary conversion failed: %v", err)
	}
	if count != 3 {
		t.Fatalf("record count = %d, want 3", count)
	}

	values, ok := batch.Data["geom"].([][]byte)
	if !ok {
		t.Fatalf("binary column type = %T, want [][]byte", batch.Data["geom"])
	}
	if !bytes.Equal(values[0], []byte{0x00, 0xff, 0x80}) {
		t.Fatalf("binary content changed: %x", values[0])
	}
	if got := batch.Validity["geom"]; len(got) != 3 || !got[0] || got[1] || !got[2] {
		t.Fatalf("binary validity = %v, want [true false true]", got)
	}

	writer := NewArrowWriter(&config.IngestConfig{
		Compression: "snappy",
	}, zerolog.Nop())

	ctx := context.Background()
	data, err := writer.WriteParquetColumnar(
		ctx, "geo", batch.Data, batch.Validity, nil, false, nil,
	)
	if err != nil {
		t.Fatalf("Parquet write failed: %v", err)
	}

	parquetReader, err := file.NewParquetReader(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	defer parquetReader.Close()

	arrowReader, err := pqarrow.NewFileReader(
		parquetReader, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator,
	)
	if err != nil {
		t.Fatal(err)
	}

	records, err := arrowReader.GetRecordReader(ctx, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer records.Release()

	if !records.Next() {
		t.Fatalf("missing Parquet record batch: %v", records.Err())
	}

	record := records.Record()
	indices := record.Schema().FieldIndices("geom")
	if len(indices) != 1 {
		t.Fatalf("missing geom field: %v", record.Schema())
	}

	field := record.Schema().Field(indices[0])
	if field.Type.ID() != arrow.BINARY {
		t.Fatalf("geom Arrow type = %s, want binary", field.Type)
	}

	binary, ok := record.Column(indices[0]).(*array.Binary)
	if !ok {
		t.Fatalf("geom array type = %T", record.Column(indices[0]))
	}

	if binary.Len() != 3 ||
		!bytes.Equal(binary.Value(0), []byte{0x00, 0xff, 0x80}) ||
		!binary.IsNull(1) ||
		binary.IsNull(2) ||
		len(binary.Value(2)) != 0 {
		t.Fatalf("binary Parquet round-trip changed values or nulls: %v", binary)
	}
}

func TestBinaryBatchOperationsIssue440(t *testing.T) {
	buffer := createTestArrowBuffer(t)

	first, _, err := buffer.convertColumnsToTyped("geo", map[string][]interface{}{
		"time": {int64(3), int64(1)},
		"geom": {[]byte{0x03}, nil},
	})
	if err != nil {
		t.Fatal(err)
	}

	second, _, err := buffer.convertColumnsToTyped("geo", map[string][]interface{}{
		"time": {int64(2)},
		"geom": {[]byte{0x02}},
	})
	if err != nil {
		t.Fatal(err)
	}

	merged, err := buffer.mergeBatches([]interface{}{first, second})
	if err != nil {
		t.Fatal(err)
	}

	sorted := sortTypedColumnBatchByKeys(merged, []string{"time"})
	values, ok := sorted.Data["geom"].([][]byte)
	if !ok || len(values) != 3 {
		t.Fatalf("sorted binary column = %T", sorted.Data["geom"])
	}
	if values[0] != nil ||
		!bytes.Equal(values[1], []byte{0x02}) ||
		!bytes.Equal(values[2], []byte{0x03}) {
		t.Fatalf("binary values misaligned after sorting: %v", values)
	}
	if got := sorted.Validity["geom"]; len(got) != 3 || got[0] || !got[1] || !got[2] {
		t.Fatalf("validity misaligned after sorting: %v", got)
	}

	sliced := sliceTypedColumnBatchByIndices(sorted, []int{0, 2})
	subset, ok := sliced.Data["geom"].([][]byte)
	if !ok || len(subset) != 2 ||
		subset[0] != nil ||
		!bytes.Equal(subset[1], []byte{0x03}) {
		t.Fatalf("binary hour-slice changed values: %v", sliced.Data["geom"])
	}

	binarySignature := getColumnSignature(map[string]interface{}{
		"geom": [][]byte{{0x01}},
	})
	stringSignature := getColumnSignature(map[string]interface{}{
		"geom": []string{"x"},
	})
	if binarySignature == stringSignature {
		t.Fatal("binary and string columns have identical schema signatures")
	}
}

func TestBinarySparseAndMixedIssue440(t *testing.T) {
	buffer := createTestArrowBuffer(t)

	first, _, err := buffer.convertColumnsToTyped(
		"geo", map[string][]interface{}{
			"time": {int64(1)},
			"geom": {[]byte{0x01}},
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	second, _, err := buffer.convertColumnsToTyped(
		"geo", map[string][]interface{}{
			"time": {int64(2)},
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	merged, err := buffer.mergeBatches([]interface{}{first, second})
	if err != nil {
		t.Fatal(err)
	}

	values, ok := merged.Data["geom"].([][]byte)
	if !ok || len(values) != 2 ||
		!bytes.Equal(values[0], []byte{0x01}) ||
		values[1] != nil {
		t.Fatalf("sparse binary values = %v", merged.Data["geom"])
	}

	valid := merged.Validity["geom"]
	if len(valid) != 2 || !valid[0] || valid[1] {
		t.Fatalf("sparse binary validity = %v", valid)
	}

	_, _, err = buffer.convertColumnsToTyped(
		"geo", map[string][]interface{}{
			"time": {int64(1), int64(2)},
			"geom": {[]byte{0x01}, "wrong type"},
		},
	)
	if err == nil {
		t.Fatal("mixed binary and string values must be rejected")
	}

	typ, err := inferArrowType("geom", []byte{0xff})
	if err != nil || typ.ID() != arrow.BINARY {
		t.Fatalf("inferred type = %v, error = %v", typ, err)
	}
}

func TestBinarySortKeyIssue440(t *testing.T) {
	columns := map[string]interface{}{
		"time": []int64{1, 2},
		"geom": [][]byte{{0x02}, {0x01}},
	}

	sorted, _, err := sortColumnsByKeysWithPermutation(
		columns, []string{"geom"},
	)
	if err != nil {
		t.Fatal(err)
	}

	times := sorted["time"].([]int64)
	if times[0] != 2 || times[1] != 1 {
		t.Fatalf("binary sort key ignored: %v", times)
	}
}
