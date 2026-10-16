package ingest

// Row-format msgpack decoded straight into typed columns gives what the
// record path (decodeRow, rowsToColumnar, the buffer's typing) gives for the
// same rows, and drops, as line protocol does, the points the record path
// could not write.

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/Basekick-Labs/msgpack/v6"
	"github.com/basekick-labs/arc/pkg/models"
	"github.com/stretchr/testify/require"
)

// rowsGeneric is the record path for a row payload: generic decode, rows
// grouped by measurement in arrival order, rowsToColumnar, typing.
func rowsGeneric(t *testing.T, payload []byte) (map[string]normBatch, map[string]int, error) {
	t.Helper()
	recs, err := newTypedTestDecoder(false).Decode(payload)
	if err != nil {
		return nil, nil, err
	}
	byM := map[string][]*models.Record{}
	for _, r := range recs.([]interface{}) {
		rec, ok := r.(*models.Record)
		if !ok {
			return nil, nil, fmt.Errorf("not a row: %T", r)
		}
		byM[rec.Measurement] = append(byM[rec.Measurement], rec)
	}
	b := diffBuffer(t)
	out, rows := map[string]normBatch{}, map[string]int{}
	for m, rs := range byM {
		col, _ := b.rowsToColumnar(m, rs)
		for name, vals := range col.Columns {
			if len(vals) != len(col.Columns["time"]) {
				return nil, nil, fmt.Errorf("column %q has %d cells for %d rows", name, len(vals), len(col.Columns["time"]))
			}
		}
		batch, n, err := b.convertColumnsToTyped(m, col.Columns)
		if err != nil {
			return nil, nil, err
		}
		nb := normalize(batch, n)
		nb.tags = nil // rowsToColumnar marks no tag columns
		out[m], rows[m] = nb, n
	}
	return out, rows, nil
}

// rowsTyped is the typed decode of a row payload.
func rowsTyped(t *testing.T, payload []byte) (map[string]normBatch, map[string]int, []LPDrop, bool) {
	t.Helper()
	d := newTypedTestDecoder(true)
	recs, _, drops, err := d.DecodeWithDrops(payload)
	require.NoError(t, err)
	if d.typedHits.Load() != 1 {
		return nil, nil, nil, false
	}
	out, rows := map[string]normBatch{}, map[string]int{}
	for _, r := range recs.([]interface{}) {
		rec := r.(*TypedColumnarRecord)
		nb := normalize(rec.Batch, rec.NumRecords)
		nb.tags = nil
		out[rec.Measurement], rows[rec.Measurement] = nb, rec.NumRecords
	}
	return out, rows, drops, true
}

// randomRows is a conflict-free row payload: each field keeps one type per
// measurement, tag values are strings, some fields are NULL, some field
// names are another row's tag (written to "<name>_value").
func randomRows(rng *rand.Rand, n int) []interface{} {
	units := []int64{1, 1000, 1_000_000, 1_000_000_000}
	var rows []interface{}
	for i := 0; i < n; i++ {
		m := []interface{}{"cpu", "mem", int64(7)}[rng.Intn(3)]
		base := int64(1700000000 + i)
		u := units[rng.Intn(len(units))]
		var tval interface{} = base * u
		if rng.Intn(4) == 0 {
			tval = uint64(base * u)
		}
		row := map[string]interface{}{"m": m, "t": tval}
		tags := map[string]interface{}{"region": []string{"us", "eu"}[rng.Intn(2)]}
		if rng.Intn(3) == 0 {
			tags["dc"] = "d1"
		}
		if rng.Intn(3) == 0 {
			tags["host"] = "tagged"
		}
		if rng.Intn(4) == 0 {
			row["h"] = []interface{}{"srv", int64(42), ""}[rng.Intn(3)]
		}
		row["tags"] = tags
		fields := map[string]interface{}{"v": rng.Float64()}
		if rng.Intn(2) == 0 {
			fields["n"] = int64(rng.Intn(100))
		}
		if rng.Intn(3) == 0 {
			fields["s"] = fmt.Sprintf("s%d", rng.Intn(5))
		}
		if rng.Intn(4) == 0 {
			fields["ok"] = rng.Intn(2) == 0
		}
		if rng.Intn(5) == 0 {
			fields["n"] = nil // a NULL
		}
		if rng.Intn(6) == 0 {
			fields["gone"] = nil // only ever NULL
		}
		if rng.Intn(6) == 0 {
			fields["dc"] = 1.5 // a field named like a tag of other rows
		}
		row["fields"] = fields
		rows = append(rows, row)
	}
	return rows
}

func TestRowsTypedMatchesRecordPath(t *testing.T) {
	rng := rand.New(rand.NewSource(3))
	for iter := 0; iter < 300; iter++ {
		rows := randomRows(rng, 1+rng.Intn(30))
		var payload []byte
		switch iter % 3 {
		case 0:
			payload = mustMarshal(t, rows)
		case 1:
			payload = mustMarshal(t, map[string]interface{}{"batch": rows})
		default:
			payload = mustMarshal(t, rows[0])
		}
		want, wantRows, err := rowsGeneric(t, payload)
		require.NoError(t, err, "iteration %d", iter)
		got, gotRows, drops, hit := rowsTyped(t, payload)
		require.True(t, hit, "iteration %d fell back", iter)
		require.Empty(t, drops, "iteration %d", iter)
		require.Equal(t, wantRows, gotRows, "iteration %d", iter)
		require.Equal(t, want, got, "iteration %d", iter)
	}
}

// A point the record path could not write is dropped and counted; the
// others are written.
func TestRowsTypedDropsPoints(t *testing.T) {
	row := func(fields, tags map[string]interface{}) map[string]interface{} {
		r := map[string]interface{}{"m": "cpu", "t": int64(1700000000), "fields": fields}
		if tags != nil {
			r["tags"] = tags
		}
		return r
	}
	cases := []struct {
		name   string
		rows   []interface{}
		reason string
		kept   int
	}{
		{"kind misfit", []interface{}{row(map[string]interface{}{"v": 1.5}, nil), row(map[string]interface{}{"v": "x"}, nil), row(map[string]interface{}{"v": 2.5}, nil)}, LPDropKind, 2},
		{"time field", []interface{}{row(map[string]interface{}{"v": 1.5, "time": int64(5)}, nil), row(map[string]interface{}{"v": 2.5}, nil)}, LPDropTime, 1},
		{"time tag", []interface{}{row(map[string]interface{}{"v": 1.5}, map[string]interface{}{"time": "x"}), row(map[string]interface{}{"v": 2.5}, nil)}, LPDropTime, 1},
		{"x and x_value in one row without tags, x a tag elsewhere", []interface{}{
			row(map[string]interface{}{"v": 1.0}, map[string]interface{}{"x": "a"}),
			func() map[string]interface{} {
				r := row(map[string]interface{}{"x": 1.0, "x_value": 2.0}, nil)
				r["h"] = "" // no host tag either: a point with fields only
				return r
			}(),
		}, LPDropSameColumn, 1},
		{"x and x_value in one row, x a tag elsewhere", []interface{}{
			row(map[string]interface{}{"v": 1.0}, map[string]interface{}{"x": "a"}),
			row(map[string]interface{}{"x": 1.0, "x_value": 2.0}, nil),
		}, LPDropSameColumn, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, rows, drops, hit := rowsTyped(t, mustMarshal(t, tc.rows))
			require.True(t, hit)
			require.Len(t, drops, 1)
			require.Equal(t, tc.reason, drops[0].Reason)
			require.Equal(t, 1, drops[0].Points)
			require.Equal(t, tc.kept, rows["cpu"])
		})
	}
	// Decode (no drops reported) refuses instead of dropping silently.
	_, err := newTypedTestDecoder(true).Decode(mustMarshal(t, cases[0].rows))
	require.ErrorContains(t, err, "1 points cannot be written")
}

// Host: h when sent; the row's own host tag when not (it used to be
// replaced by "unknown": the Python client never sends h); "unknown" when
// neither. The same on the typed and the record path.
func TestRowsHost(t *testing.T) {
	cases := []struct {
		name string
		row  map[string]interface{}
		want string
	}{
		{"tag only", map[string]interface{}{"tags": map[string]interface{}{"host": "s1"}}, "s1"},
		{"h wins", map[string]interface{}{"h": "srv", "tags": map[string]interface{}{"host": "s1"}}, "srv"},
		{"int h", map[string]interface{}{"h": int64(7)}, "host_7"},
		{"neither", map[string]interface{}{}, "unknown"},
		{"empty h keeps the tag", map[string]interface{}{"h": "", "tags": map[string]interface{}{"host": "s1"}}, "s1"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tc.row["m"], tc.row["t"], tc.row["fields"] = "cpu", int64(1700000000), map[string]interface{}{"v": 1.0}
			payload := mustMarshal(t, []interface{}{tc.row})
			got, _, _, hit := rowsTyped(t, payload)
			require.True(t, hit)
			require.Equal(t, []interface{}{tc.want}, got["cpu"].cols["host"].vals)
			want, _, err := rowsGeneric(t, payload)
			require.NoError(t, err)
			require.Equal(t, []interface{}{tc.want}, want["cpu"].cols["host"].vals, "record path")
		})
	}
	// An unusable h type on the record path keeps the tag too.
	payload := mustMarshal(t, map[string]interface{}{"m": "cpu", "t": int64(1700000000), "h": 1.5,
		"tags": map[string]interface{}{"host": "s1"}, "fields": map[string]interface{}{"v": 1.0}})
	want, _, err := rowsGeneric(t, payload)
	require.NoError(t, err)
	require.Equal(t, []interface{}{"s1"}, want["cpu"].cols["host"].vals)
}

// Rows the typed decoder does not handle exactly send the whole payload to
// the record path, which answers as before.
func TestRowsTypedFallsBack(t *testing.T) {
	good := map[string]interface{}{"m": "cpu", "t": int64(1700000000), "fields": map[string]interface{}{"v": 1.0}}
	with := func(k string, v interface{}) map[string]interface{} {
		r := map[string]interface{}{}
		for kk, vv := range good {
			r[kk] = vv
		}
		r[k] = v
		return r
	}
	f32 := mustMarshal(t, map[string]interface{}{"m": "cpu", "t": int64(1700000000), "fields": map[string]interface{}{"v": float32(1.5)}})
	cases := map[string][]byte{
		"float32 field":        f32,
		"nested field":         mustMarshal(t, with("fields", map[string]interface{}{"v": []interface{}{1}})),
		"int tag value":        mustMarshal(t, with("tags", map[string]interface{}{"port": int64(80)})),
		"binary tag value":     mustMarshal(t, with("tags", map[string]interface{}{"id": []byte{1, 2, 3}})),
		"compact f":            mustMarshal(t, map[string]interface{}{"m": "cpu", "f": []interface{}{1.0}}),
		"float t":              mustMarshal(t, with("t", 1.7e9)),
		"zero t":               mustMarshal(t, with("t", int64(0))),
		"negative t":           mustMarshal(t, with("t", int64(-5))),
		"missing m":            mustMarshal(t, map[string]interface{}{"t": int64(1700000000), "fields": map[string]interface{}{"v": 1.0}}),
		"missing fields":       mustMarshal(t, map[string]interface{}{"m": "cpu", "t": int64(1700000000)}),
		"tags not a map":       mustMarshal(t, with("tags", "x")),
		"float h":              mustMarshal(t, with("h", 1.5)),
		"uint64 above int64 t": mustMarshal(t, with("t", uint64(1)<<63)),
		"rows and columnar":    mustMarshal(t, []interface{}{good, map[string]interface{}{"m": "cpu", "columns": map[string]interface{}{"time": []interface{}{int64(1)}}}}),
	}
	for name, payload := range cases {
		t.Run(name, func(t *testing.T) {
			d := newTypedTestDecoder(true)
			_, _, _, terr := d.DecodeWithDrops(payload)
			require.Zero(t, d.typedHits.Load(), "typed path took it")
			_, berr := newTypedTestDecoder(false).Decode(payload)
			require.Equal(t, berr == nil, terr == nil, "typed %v, generic %v", terr, berr)
		})
	}
}

// An empty fields map is a row of time and tags; a missing t is now, per row.
func TestRowsTypedEmptyFieldsAndMissingTime(t *testing.T) {
	old := lpNow
	now := int64(1700000000000000)
	lpNow = func() int64 { now++; return now }
	defer func() { lpNow = old }()
	payload := mustMarshal(t, []interface{}{
		map[string]interface{}{"m": "cpu", "fields": map[string]interface{}{}, "tags": map[string]interface{}{"a": "x"}},
		map[string]interface{}{"m": "cpu", "fields": map[string]interface{}{"v": 1.0}},
	})
	got, rows, drops, hit := rowsTyped(t, payload)
	require.True(t, hit)
	require.Empty(t, drops)
	require.Equal(t, 2, rows["cpu"])
	require.Equal(t, []interface{}{int64(1700000000000001), int64(1700000000000002)}, got["cpu"].cols["time"].vals)
	require.Equal(t, []interface{}{nil, 1.0}, got["cpu"].cols["v"].vals)
}

// The record path (fallbacks, decimal deployments, MQTT) leaves out the rows
// it cannot write as one row each, writes the others, and reports the drops,
// instead of buffering a column longer than the others, which every flush
// then refused (or panicked on). Collisions across rows included.
func TestRowsToColumnarDropsUnwritableRows(t *testing.T) {
	ts := func(i int) int64 { return 1700000000000000 + int64(i) }
	rec := func(i int, tags map[string]string, fields map[string]interface{}) *models.Record {
		fields["i"] = int64(i)
		return &models.Record{Measurement: "cpu", Timestamp: ts(i), Tags: tags, Fields: fields}
	}
	cases := []struct {
		name string
		recs []interface{}
		kept []interface{}
		drop string
	}{
		{"x and x_value in one row, x a tag of another", []interface{}{
			rec(0, map[string]string{"x": "a"}, map[string]interface{}{}),
			rec(1, map[string]string{}, map[string]interface{}{"x": 2.0, "x_value": 3.0}),
			rec(2, map[string]string{}, map[string]interface{}{}),
		}, []interface{}{int64(0), int64(2)}, LPDropSameColumn},
		{"field time", []interface{}{
			rec(0, map[string]string{}, map[string]interface{}{"time": int64(5)}),
			rec(1, map[string]string{}, map[string]interface{}{}),
		}, []interface{}{int64(1)}, LPDropTime},
		{"tag time", []interface{}{
			rec(0, map[string]string{"time": "x"}, map[string]interface{}{}),
			rec(1, map[string]string{}, map[string]interface{}{}),
		}, []interface{}{int64(1)}, LPDropTime},
		// Across rows: one value per row and column, so nothing is dropped
		// and every column has one cell per row.
		{"x, x_value literal and tag x in different rows", []interface{}{
			rec(0, map[string]string{"x": "a"}, map[string]interface{}{}),
			rec(1, map[string]string{}, map[string]interface{}{"x": 2.0}),
			rec(2, map[string]string{}, map[string]interface{}{"x_value": 3.0}),
		}, []interface{}{int64(0), int64(1), int64(2)}, ""},
		{"tag x_value and field x in different rows", []interface{}{
			rec(0, map[string]string{"x": "a"}, map[string]interface{}{}),
			rec(1, map[string]string{"x_value": "b"}, map[string]interface{}{}),
			rec(2, map[string]string{}, map[string]interface{}{"x": "s"}),
		}, []interface{}{int64(0), int64(1), int64(2)}, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := newFlushRig(t, nil)
			err := r.buf.Write(context.Background(), "d", tc.recs)
			if tc.drop == "" {
				require.NoError(t, err)
			} else {
				var pde *PointsDroppedError
				require.ErrorAs(t, err, &pde)
				require.Len(t, pde.Drops, 1)
				require.Equal(t, tc.drop, pde.Drops[0].Reason)
				require.Equal(t, 1, pde.Drops[0].Points)
			}
			files := r.files(t)
			require.Len(t, files, 1)
			require.Equal(t, tc.kept, files[0].cols["i"])
			_, hasTime := files[0].types["time"]
			require.True(t, hasTime)
		})
	}
}

// Mixed rows across measurements in one request: each measurement's rows
// are typed on their own (first value per measurement).
func TestRowsTypedPerMeasurementKinds(t *testing.T) {
	payload := mustMarshal(t, []interface{}{
		map[string]interface{}{"m": "a", "t": int64(1700000000), "fields": map[string]interface{}{"v": int64(1)}},
		map[string]interface{}{"m": "b", "t": int64(1700000000), "fields": map[string]interface{}{"v": "x"}},
	})
	got, _, drops, hit := rowsTyped(t, payload)
	require.True(t, hit)
	require.Empty(t, drops)
	require.Equal(t, "[]int64", got["a"].cols["v"].typ)
	require.Equal(t, "[]string", got["b"].cols["v"].typ)
}

// Typed rows and the record path agree on accept/reject for any input, and
// on the data when nothing is dropped.
func FuzzRowsTypedMatchesRecordPath(f *testing.F) {
	rng := rand.New(rand.NewSource(9))
	for i := 0; i < 8; i++ {
		f.Add(mustMarshal(f, randomRows(rng, 1+i)))
	}
	f.Add(mustMarshal(f, map[string]interface{}{"batch": randomRows(rng, 3)}))
	f.Fuzz(func(t *testing.T, payload []byte) {
		d := newTypedTestDecoder(true)
		_, _, drops, terr := d.DecodeWithDrops(payload)
		_, berr := newTypedTestDecoder(false).Decode(payload)
		if (terr == nil) != (berr == nil) {
			t.Fatalf("accept/reject divergence: typed %v, generic %v", terr, berr)
		}
		if terr != nil || d.typedHits.Load() == 0 || len(drops) > 0 {
			return
		}
		got, gotRows, _, _ := rowsTyped(t, payload)
		if got == nil {
			return // columnar: covered by FuzzTypedDecodeArrays
		}
		want, wantRows, err := rowsGeneric(t, payload)
		if err != nil {
			return // the record path's write fails; the typed path wrote (a fix)
		}
		require.Equal(t, wantRows, gotRows)
		for m := range want {
			wb, gb := want[m], got[m]
			keys := make([]string, 0, len(wb.cols))
			for k := range wb.cols {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			for _, k := range keys {
				if k == "time" {
					continue // a row without t is "now" on each path's own clock
				}
				// The typed path makes tag strings valid UTF-8, as it does
				// fields (the record path sanitized fields only).
				wc := wb.cols[k]
				for i, v := range wc.vals {
					if sv, ok := v.(string); ok {
						wc.vals[i], _ = SanitizeUTF8(sv)
					}
				}
				require.Equal(t, wc, gb.cols[k], "measurement %q column %q", m, k)
			}
		}
	})
}

// The record count a write reports is its rows, not its measurements.
func TestRowsTypedCountsRows(t *testing.T) {
	payload := mustMarshal(t, randomRows(rand.New(rand.NewSource(1)), 25))
	d := newTypedTestDecoder(true)
	recs, count, drops, err := d.DecodeWithDrops(payload)
	require.NoError(t, err)
	require.Empty(t, drops)
	require.Equal(t, 25, count)
	require.EqualValues(t, 25, d.totalDecoded.Load())
	require.Less(t, len(recs.([]interface{})), 25, "one record per measurement")
	_, gcount, _, err := newTypedTestDecoder(false).DecodeWithDrops(payload)
	require.NoError(t, err)
	require.Equal(t, count, gcount, "the record path counts rows too")
}

// A repeated key in a row's fields or tags: the record path's map keeps the
// last value; the typed decoder leaves such a row to it.
func TestRowsTypedRepeatedKeysFallBack(t *testing.T) {
	enc := func(build func(e *msgpack.Encoder)) []byte {
		var buf bytes.Buffer
		e := msgpack.NewEncoder(&buf)
		build(e)
		return buf.Bytes()
	}
	for name, field := range map[string]string{"fields": "v", "tags": "a"} {
		payload := enc(func(e *msgpack.Encoder) {
			_ = e.EncodeMapLen(3)
			_ = e.EncodeString("m")
			_ = e.EncodeString("cpu")
			_ = e.EncodeString("fields")
			if name == "fields" {
				_ = e.EncodeMapLen(2)
				_ = e.EncodeString(field)
				_ = e.EncodeFloat64(1)
				_ = e.EncodeString(field)
				_ = e.EncodeFloat64(2)
			} else {
				_ = e.EncodeMapLen(1)
				_ = e.EncodeString("v")
				_ = e.EncodeFloat64(1)
			}
			_ = e.EncodeString("tags")
			if name == "tags" {
				_ = e.EncodeMapLen(2)
				_ = e.EncodeString(field)
				_ = e.EncodeString("x")
				_ = e.EncodeString(field)
				_ = e.EncodeString("y")
			} else {
				_ = e.EncodeMapLen(0)
			}
		})
		d := newTypedTestDecoder(true)
		_, _, _, err := d.DecodeWithDrops(payload)
		require.NoError(t, err, name)
		require.Zero(t, d.typedHits.Load(), "repeated %s key taken by the typed path", name)
	}
}

// Tag values are made valid UTF-8, as field values are.
func TestRowsTypedSanitizesTags(t *testing.T) {
	payload := mustMarshal(t, map[string]interface{}{"m": "cpu", "t": int64(1700000000),
		"tags": map[string]interface{}{"region": "us\xff"}, "fields": map[string]interface{}{"v": 1.0}})
	got, _, _, hit := rowsTyped(t, payload)
	require.True(t, hit)
	want, _ := SanitizeUTF8("us\xff")
	require.Equal(t, []interface{}{want}, got["cpu"].cols["region"].vals)
	require.NotEqual(t, "us\xff", want)
}

// A row with very many keys decodes in linear time (a pairwise repeated-key
// check took 7 s for 100,000 keys).
func TestRowsTypedManyKeysLinear(t *testing.T) {
	fields := make(map[string]interface{}, 100000)
	tags := make(map[string]interface{}, 100000)
	for i := 0; i < 100000; i++ {
		fields[fmt.Sprintf("f%d", i)] = 1.0
		tags[fmt.Sprintf("t%d", i)] = "x"
	}
	payload := mustMarshal(t, map[string]interface{}{"m": "cpu", "t": int64(1700000000), "fields": fields, "tags": tags})
	start := time.Now()
	_, rows, _, hit := rowsTyped(t, payload)
	require.True(t, hit)
	require.Equal(t, 1, rows["cpu"])
	require.Less(t, time.Since(start), 2*time.Second)
}
