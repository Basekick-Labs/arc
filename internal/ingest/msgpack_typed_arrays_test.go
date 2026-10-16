package ingest

// A top-level array of columnar maps (Telegraf's Arc output) and
// {"batch": [...]} (the Python client's encode_batch) take the typed path
// item by item, with the same result as the generic path, and each item's
// RawPayload is its own bytes.

import (
	"bytes"
	"context"
	"reflect"
	"runtime"
	"testing"

	"github.com/Basekick-Labs/msgpack/v6"
	"github.com/basekick-labs/arc/pkg/models"
	"github.com/stretchr/testify/require"
)

// telegrafPayload is what Telegraf's Arc output sends for several
// measurements: []interface{} columns, ms time, nil where a metric lacks a
// field.
func telegrafPayload() []interface{} {
	cols := func(n int, extra string) map[string]interface{} {
		time, host, v, x := []interface{}{}, []interface{}{}, []interface{}{}, []interface{}{}
		for i := 0; i < n; i++ {
			time = append(time, int64(1700000000000+i))
			host = append(host, "h"+string(rune('a'+i%3)))
			v = append(v, float64(i)+0.5)
			if i%2 == 0 {
				x = append(x, int64(i))
			} else {
				x = append(x, nil)
			}
		}
		return map[string]interface{}{"time": time, "host": host, "v": v, extra: x}
	}
	return []interface{}{
		map[string]interface{}{"m": "cpu", "columns": cols(5, "n")},
		map[string]interface{}{"m": "mem", "columns": cols(3, "free"), "tag_keys": []interface{}{"host"}},
		map[string]interface{}{"m": int64(7), "columns": cols(2, "k")},
		map[string]interface{}{"m": "cpu", "columns": cols(4, "n")},
	}
}

// decodeItems decodes payload with the typed path on and off.
func decodeItems(t *testing.T, payload []byte) (typed, boxed []interface{}, typedErr, boxedErr error, hit bool) {
	t.Helper()
	dt, db := newTypedTestDecoder(true), newTypedTestDecoder(false)
	tr, typedErr := dt.Decode(payload)
	br, boxedErr := db.Decode(payload)
	typed, _ = tr.([]interface{})
	boxed, _ = br.([]interface{})
	return typed, boxed, typedErr, boxedErr, dt.typedHits.Load() == 1
}

// assertItemsEquivalent: typed hit, one record per item, each equal to the
// generic path's conversion of that item, RawPayload = the item's bytes.
func assertItemsEquivalent(t *testing.T, payload []byte, items []interface{}) {
	t.Helper()
	typed, boxed, terr, berr, hit := decodeItems(t, payload)
	require.NoError(t, terr)
	require.NoError(t, berr)
	require.True(t, hit, "typed path fell back")
	require.Len(t, typed, len(boxed))
	require.Len(t, typed, len(items))
	for i := range typed {
		tr, ok := typed[i].(*TypedColumnarRecord)
		require.True(t, ok, "item %d is %T", i, typed[i])
		br, ok := boxed[i].(*models.ColumnarRecord)
		require.True(t, ok, "boxed item %d is %T", i, boxed[i])
		ref, n := boxedToTyped(t, br)
		require.Equal(t, br.Measurement, tr.Measurement, "item %d", i)
		require.Equal(t, n, tr.NumRecords, "item %d", i)
		require.Equal(t, ref.Signature, tr.Batch.Signature, "item %d", i)
		require.True(t, reflect.DeepEqual(ref.Data, tr.Batch.Data), "item %d data", i)
		require.True(t, reflect.DeepEqual(map[string][]bool(ref.Validity), map[string][]bool(tr.Batch.Validity)), "item %d validity", i)
		require.Equal(t, br.TagColumns, tr.Batch.TagColumns, "item %d tag keys", i)
		// RawPayload is this item's own bytes inside the payload.
		require.True(t, bytes.Contains(payload, tr.RawPayload), "item %d", i)
		var got, want interface{}
		require.NoError(t, msgpack.Unmarshal(tr.RawPayload, &got))
		require.NoError(t, msgpack.Unmarshal(mustMarshal(t, items[i]), &want))
		require.Equal(t, want, got, "item %d RawPayload", i)
	}
}

func TestTypedDecodeArrayOfColumnarMaps(t *testing.T) {
	items := telegrafPayload()
	assertItemsEquivalent(t, mustMarshal(t, items), items)
}

func TestTypedDecodeBatch(t *testing.T) {
	items := telegrafPayload()
	assertItemsEquivalent(t, mustMarshal(t, map[string]interface{}{"batch": items}), items)
	// Other keys beside "batch" are ignored, as the generic path ignores them.
	assertItemsEquivalent(t, mustMarshal(t, map[string]interface{}{"m": "wrong", "batch": items, "columns": map[string]interface{}{"x": []interface{}{1}}}), items)
}

// Anything the typed path can't take sends the WHOLE payload to the generic
// path: same answer, no typed hit.
func TestTypedDecodeArrayFallsBack(t *testing.T) {
	good := telegrafPayload()[0]
	row := map[string]interface{}{"m": "cpu", "t": int64(1700000000000), "fields": map[string]interface{}{"v": 1.0}}
	noTime := map[string]interface{}{"m": "cpu", "columns": map[string]interface{}{"v": []interface{}{1.0}}}
	nested := map[string]interface{}{"batch": []interface{}{good}}
	cases := map[string][]byte{
		"row item":           mustMarshal(t, []interface{}{good, row}),
		"item without time":  mustMarshal(t, []interface{}{good, noTime}),
		"empty array":        mustMarshal(t, []interface{}{}),
		"empty batch":        mustMarshal(t, map[string]interface{}{"batch": []interface{}{}}),
		"nested batch":       mustMarshal(t, []interface{}{good, nested}),
		"non-map item":       mustMarshal(t, []interface{}{good, int64(3)}),
		"batch not an array": mustMarshal(t, map[string]interface{}{"batch": "x", "m": "cpu", "columns": map[string]interface{}{"time": []interface{}{int64(1)}}}),
		"trailing bytes":     append(mustMarshal(t, []interface{}{good}), 0xc0),
		"truncated":          mustMarshal(t, []interface{}{good, good})[:20],
	}
	// Duplicate "batch" key: written by hand (a Go map cannot hold it).
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	require.NoError(t, enc.EncodeMapLen(2))
	for i := 0; i < 2; i++ {
		require.NoError(t, enc.EncodeString("batch"))
		require.NoError(t, enc.Encode([]interface{}{good}))
	}
	cases["duplicate batch key"] = buf.Bytes()

	for name, payload := range cases {
		t.Run(name, func(t *testing.T) {
			typed, boxed, terr, berr, hit := decodeItems(t, payload)
			require.False(t, hit, "typed path took it")
			require.Equal(t, berr == nil, terr == nil, "typed %v, boxed %v", terr, berr)
			require.Equal(t, len(boxed), len(typed))
		})
	}
}

// Typed on and off agree on accept/reject for any input, and on every item
// when the typed path takes it.
func FuzzTypedDecodeArrays(f *testing.F) {
	items := telegrafPayload()
	f.Add(mustMarshal(f, items))
	f.Add(mustMarshal(f, map[string]interface{}{"batch": items}))
	f.Add(mustMarshal(f, items[:1]))
	f.Add(mustMarshal(f, map[string]interface{}{"x": 1, "batch": items}))
	f.Add(append(append([]byte{0x82, 0xa1, 'x', 0xd4, 0x30, 0x00}, 0xa5), append([]byte("batch"), mustMarshal(f, items)...)...))
	f.Add(append([]byte{0x81, 0xd9, 5}, append([]byte("batch"), mustMarshal(f, items)...)...))
	f.Fuzz(func(t *testing.T, payload []byte) {
		typed, boxed, terr, berr, hit := decodeItems(t, payload)
		if (terr == nil) != (berr == nil) {
			t.Fatalf("accept/reject divergence: typed %v, boxed %v", terr, berr)
		}
		if !hit || terr != nil {
			return
		}
		require.Len(t, typed, len(boxed))
		for i := range typed {
			tr := typed[i].(*TypedColumnarRecord)
			br, ok := boxed[i].(*models.ColumnarRecord)
			require.True(t, ok)
			b := &ArrowBuffer{}
			ref, n, err := b.convertColumnsToTyped(br.Measurement, br.Columns)
			if err != nil {
				t.Fatalf("typed took item %d the generic path rejects at write: %v", i, err)
			}
			require.Equal(t, n, tr.NumRecords)
			// normalize: NaN compares equal to NaN.
			require.Equal(t, normalize(ref, n).cols, normalize(tr.Batch, n).cols, "item %d", i)
			require.True(t, reflect.DeepEqual(map[string][]bool(ref.Validity), map[string][]bool(tr.Batch.Validity)), "item %d", i)
		}
	})
}

// A map with a nil key (91 81 c0 30: [{nil: 48}]) panicked inside the msgpack
// library's generic decode;
// it is a malformed payload (an error), not a panic.
func TestDecodeNilMapKeyIsAnError(t *testing.T) {
	for _, typed := range []bool{false, true} {
		_, err := newTypedTestDecoder(typed).Decode([]byte("\x91\x81\xc00"))
		require.ErrorContains(t, err, "malformed payload")
		_, err = newTypedTestDecoder(typed).Decode([]byte("\x82\xc00\xa1m\xa3cpu"))
		require.Error(t, err)
	}
}

// A key repeated in "columns" keeps only its last value in the generic path
// (dropped if not an array); the typed path kept the first array. Any
// repeated column key now goes to the generic path.
func TestTypedDecodeDuplicateColumnKeyFallsBack(t *testing.T) {
	in := []byte("\x82\xa1m0\xa7columns\x84\xa4time\x9500000\xa2000\xa10\x95\xcb000000000000\xa100")
	typed, boxed, terr, berr, hit := decodeItems(t, in)
	require.NoError(t, terr)
	require.NoError(t, berr)
	require.False(t, hit)
	require.Equal(t, boxed, typed)
}

// A value the typed path ignores but the generic path refuses (an unknown
// extension, a map with a nil key) is left to the generic path: refused.
func TestTypedDecodeLeavesNonScalarExtrasToGeneric(t *testing.T) {
	for name, in := range map[string][]byte{
		"ext as a column value":   []byte("\x82\xa1m0\xa7columns\x84\xa4time\x9500000\xa2000\xa110\xa10\xd5000"),
		"ext under another key":   []byte("\x83\xa1m\xa3cpu\xa1x\xd4\x30\x00\xa7columns\x81\xa4time\x91\x01"),
		"nil-key map in a column": []byte("\x82\xa1m\xa3cpu\xa7columns\x82\xa4time\x91\x01\xa1x\x81\xc0\x01"),
	} {
		t.Run(name, func(t *testing.T) {
			_, _, terr, berr, hit := decodeItems(t, in)
			require.Error(t, berr)
			require.Error(t, terr)
			require.False(t, hit)
		})
	}
}

// The batch map is read as the generic path reads it: keys of any string
// encoding, every key a string, other values plain, exactly one "batch".
func TestTypedDecodeBatchMapLikeGeneric(t *testing.T) {
	item := telegrafPayload()[0]
	enc := func(build func(e *msgpack.Encoder)) []byte {
		var buf bytes.Buffer
		e := msgpack.NewEncoder(&buf)
		build(e)
		return buf.Bytes()
	}
	refused := map[string][]byte{
		"int key": enc(func(e *msgpack.Encoder) {
			_ = e.EncodeMapLen(2)
			_ = e.EncodeInt(1)
			_ = e.EncodeInt(2)
			_ = e.EncodeString("batch")
			_ = e.Encode([]interface{}{item})
		}),
		"nil key": enc(func(e *msgpack.Encoder) {
			_ = e.EncodeMapLen(2)
			_ = e.EncodeNil()
			_ = e.EncodeInt(2)
			_ = e.EncodeString("batch")
			_ = e.Encode([]interface{}{item})
		}),
		"ext value": append(append([]byte{0x82, 0xa1, 'x', 0xd4, 0x30, 0x00}, 0xa5), append([]byte("batch"), mustMarshal(t, []interface{}{item})...)...),
		"batch then str8 batch": enc(func(e *msgpack.Encoder) {
			_ = e.EncodeMapLen(2)
			_ = e.EncodeString("batch")
			_ = e.Encode([]interface{}{item})
			_, _ = e.Writer().Write([]byte{0xd9, 5})
			_, _ = e.Writer().Write([]byte("batch"))
			_ = e.EncodeInt(1)
		}),
	}
	for name, in := range refused {
		t.Run(name, func(t *testing.T) {
			_, _, terr, berr, hit := decodeItems(t, in)
			require.False(t, hit, "typed path took it")
			require.Equal(t, berr == nil, terr == nil, "typed %v, generic %v", terr, berr)
		})
	}
	// "batch" spelled as str8 is still the batch key.
	str8 := enc(func(e *msgpack.Encoder) {
		_ = e.EncodeMapLen(1)
		_, _ = e.Writer().Write([]byte{0xd9, 5})
		_, _ = e.Writer().Write([]byte("batch"))
		_ = e.Encode([]interface{}{item})
	})
	assertItemsEquivalent(t, str8, []interface{}{item})
}

// A column declaring more values than the payload has bytes is refused
// before anything is allocated for it (a 25-byte payload declaring a
// 1M-element column allocated 16 MB).
func TestTypedDecodeRefusesForgedColumnCount(t *testing.T) {
	// {"m": "cpu", "columns": {"s": array32(1<<20) "x" ...}}: the forged
	// column first, so no other column's length refuses it before.
	in := []byte{0x82, 0xa1, 'm', 0xa3, 'c', 'p', 'u', 0xa7, 'c', 'o', 'l', 'u', 'm', 'n', 's', 0x82,
		0xa1, 's', 0xdd, 0x00, 0x10, 0x00, 0x00, 0xa1, 'x'}
	for _, typed := range []bool{true, false} {
		var a, b runtime.MemStats
		runtime.ReadMemStats(&a)
		_, err := newTypedTestDecoder(typed).Decode(in)
		runtime.ReadMemStats(&b)
		require.Error(t, err)
		require.Less(t, b.TotalAlloc-a.TotalAlloc, uint64(1<<20), "typed=%v allocated %d", typed, b.TotalAlloc-a.TotalAlloc)
	}
}

// Nesting past msgpackMaxDepth is refused before the generic decode recurses
// (millions of levels overflowed the stack, which recover cannot catch).
func TestDecodeRefusesDeepNesting(t *testing.T) {
	ok := append(bytes.Repeat([]byte{0x91}, msgpackMaxDepth), 0xc0)
	_, valid := msgpackValueEnd(ok, 0)
	require.True(t, valid)
	deep := append(bytes.Repeat([]byte{0x91}, msgpackMaxDepth+1), 0xc0)
	_, valid = msgpackValueEnd(deep, 0)
	require.False(t, valid)
	huge := append(bytes.Repeat([]byte{0x91}, 1<<20), 0xc0)
	for _, typed := range []bool{true, false} {
		_, err := newTypedTestDecoder(typed).Decode(huge)
		require.Error(t, err)
	}
}

// Items are counted as the generic path counts them, one per item.
func TestTypedDecodeCountsItems(t *testing.T) {
	items := telegrafPayload()
	d := newTypedTestDecoder(true)
	_, err := d.Decode(mustMarshal(t, items))
	require.NoError(t, err)
	require.EqualValues(t, len(items), d.totalDecoded.Load())
	require.EqualValues(t, 1, d.typedHits.Load())
}

// A typed item's buffered size is at least 8 bytes a cell: its msgpack is
// smaller than its columns when they are mostly NULL (Telegraf's are).
func TestTypedItemBufferedBytesFloor(t *testing.T) {
	cols := map[string]interface{}{"time": []interface{}{}}
	for c := 0; c < 20; c++ {
		cols[string(rune('a'+c))] = []interface{}{}
	}
	for i := 0; i < 500; i++ {
		cols["time"] = append(cols["time"].([]interface{}), int64(1700000000000000+i))
		for c := 0; c < 20; c++ {
			name := string(rune('a' + c))
			if i == 0 {
				cols[name] = append(cols[name].([]interface{}), 1.5)
			} else {
				cols[name] = append(cols[name].([]interface{}), nil)
			}
		}
	}
	payload := mustMarshal(t, []interface{}{map[string]interface{}{"m": "cpu", "columns": cols}})
	recs, err := newTypedTestDecoder(true).Decode(payload)
	require.NoError(t, err)
	b := diffBuffer(t)
	require.NoError(t, b.Write(context.Background(), "d", recs))
	require.GreaterOrEqual(t, b.BufferedBytes(), int64(500*21*8))
	require.Less(t, int64(len(payload)), int64(500*21*8), "the payload must be smaller than the floor for this test to mean anything")
}

// A non-array value then an array under one column name: the generic path
// keeps the last (the array), and so does the typed path.
func TestTypedDecodeNonArrayThenArraySameName(t *testing.T) {
	var buf bytes.Buffer
	e := msgpack.NewEncoder(&buf)
	_ = e.EncodeMapLen(2)
	_ = e.EncodeString("m")
	_ = e.EncodeString("cpu")
	_ = e.EncodeString("columns")
	_ = e.EncodeMapLen(3)
	_ = e.EncodeString("time")
	_ = e.Encode([]interface{}{int64(1700000000000000), int64(1700000000000001)})
	_ = e.EncodeString("v")
	_ = e.EncodeInt(5)
	_ = e.EncodeString("v")
	_ = e.Encode([]interface{}{1.5, 2.5})
	assertEquivalent(t, "non-array then array", buf.Bytes(), false)
}
