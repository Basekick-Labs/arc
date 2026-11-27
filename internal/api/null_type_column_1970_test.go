//go:build duckdb_arrow

package api

import (
	"encoding/json"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
)

// #1970: an Arrow NULL-typed column is named "null" and must carry nulls.
// An *array.Null has no validity bitmap, so IsNull(i) is false for every
// row and an encoder without a case for it rendered ValueStr, "(null)".
//
// Batch counts 1 and 4 cover both encoders' serial and parallel paths
// (parallelEncodeAfterBatches = 2).

func nullColumnBatches(t *testing.T, alloc memory.Allocator, nBatches int) (*arrow.Schema, []arrow.Record) {
	t.Helper()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "nothing", Type: arrow.Null, Nullable: true},
		{Name: "n", Type: arrow.PrimitiveTypes.Int64},
	}, nil)
	recs := make([]arrow.Record, nBatches)
	for b := range recs {
		recs[b] = buildArrowBatch(alloc, schema, [][]interface{}{
			{nil, int64(b*2 + 1)},
			{nil, int64(b*2 + 2)},
		})
	}
	return schema, recs
}

func TestNullTypeColumnJSON_1970(t *testing.T) {
	for _, nBatches := range []int{1, 4} {
		alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
		schema, recs := nullColumnBatches(t, alloc, nBatches)
		reader := newSimpleRecordReader(schema, recs)
		raw, rc := arrowStreamToBytes(reader, 0, nil)
		reader.Release()
		alloc.AssertSize(t, 0)

		require.Equal(t, 2*nBatches, rc)
		var out struct {
			Types []string `json:"types"`
			Data  [][]any  `json:"data"`
		}
		require.NoError(t, json.Unmarshal(raw, &out), "%s", raw)
		require.Equal(t, []string{wireTypeNull, wireTypeInt64}, out.Types)
		require.Len(t, out.Data, 2*nBatches)
		for i, row := range out.Data {
			require.Nil(t, row[0], "batches=%d row %d: a null-typed value must be JSON null: %s", nBatches, i, raw)
			require.Equal(t, float64(i+1), row[1], "batches=%d row %d", nBatches, i)
		}
	}
}

func TestNullTypeColumnMsgpack_1970(t *testing.T) {
	for _, nBatches := range []int{1, 4} {
		alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
		schema, recs := nullColumnBatches(t, alloc, nBatches)
		reader := newSimpleRecordReader(schema, recs)
		raw, _ := msgpackStreamToBytes(reader, 0)
		reader.Release()
		alloc.AssertSize(t, 0)

		out := decodeMsgpack(t, raw)
		require.Equal(t, []any{wireTypeNull, wireTypeInt64}, out["types"])
		cols := colsOf(t, out)
		require.Len(t, cols[0], 2*nBatches)
		for i, v := range cols[0] {
			require.Nil(t, v, "batches=%d row %d: a null-typed value must be msgpack nil", nBatches, i)
		}
		require.Len(t, cols[1], 2*nBatches)
		for i, v := range cols[1] {
			require.EqualValues(t, i+1, v, "batches=%d row %d", nBatches, i)
		}
	}
}

// The governance cap slices the last kept batch; a sliced null column still
// matches the array header and its neighbour.
func TestNullTypeColumnMsgpackCapped_1970(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	schema, recs := nullColumnBatches(t, alloc, 4)
	reader := newSimpleRecordReader(schema, recs)
	raw, _ := msgpackStreamToBytes(reader, 3)
	reader.Release()
	alloc.AssertSize(t, 0)

	out := decodeMsgpack(t, raw)
	cols := colsOf(t, out)
	require.Len(t, cols[0], 3)
	require.Len(t, cols[1], 3)
	for i, v := range cols[0] {
		require.Nil(t, v, "row %d", i)
	}
}
