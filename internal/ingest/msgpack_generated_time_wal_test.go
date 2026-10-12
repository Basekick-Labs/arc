package ingest

import (
	"context"
	"fmt"

	"testing"

	"github.com/Basekick-Labs/msgpack/v6"
	"github.com/basekick-labs/arc/internal/wal"
	"github.com/basekick-labs/arc/pkg/models"
	"github.com/rs/zerolog"
)

// #1207: a payload without a time column gets the ingest time. The WAL must
// keep that time, so the raw payload (which has no time) is not stored: the
// WAL entry is built from the decoded columns instead. Both decode paths.
func TestGeneratedTimeIsNotLostToTheRawWALPayload(t *testing.T) {
	raw, err := msgpack.Marshal(map[string]interface{}{"m": "cpu", "columns": map[string]interface{}{"v": []interface{}{int64(1), nil}}})
	if err != nil {
		t.Fatal(err)
	}
	for _, typed := range []bool{false, true} {
		d := NewMessagePackDecoder(zerolog.Nop())
		d.SetTypedDecodeEnabled(typed)
		out, err := d.Decode(raw)
		if err != nil {
			t.Fatal(err)
		}
		rec, ok := out.([]interface{})[0].(*models.ColumnarRecord)
		if !ok {
			t.Fatalf("typed=%v: decoded %T; a payload without time must take the generic path", typed, out.([]interface{})[0])
		}
		if rec.RawPayload != nil {
			t.Fatalf("typed=%v: the raw payload (no time) would be written to the WAL", typed)
		}
		if len(rec.Columns["time"]) != 2 || rec.Columns["v"][1] != nil {
			t.Fatalf("typed=%v: columns %v", typed, rec.Columns)
		}
	}

	// With a time column the raw payload is still used (zero copy).
	withTime, err := msgpack.Marshal(map[string]interface{}{"m": "cpu", "columns": map[string]interface{}{"time": []interface{}{int64(1700000000000000)}, "v": []interface{}{int64(1)}}})
	if err != nil {
		t.Fatal(err)
	}
	d := NewMessagePackDecoder(zerolog.Nop())
	out, err := d.Decode(withTime)
	if err != nil {
		t.Fatal(err)
	}
	if rec := out.([]interface{})[0].(*models.ColumnarRecord); len(rec.RawPayload) == 0 {
		t.Fatal("raw payload dropped for a payload with time")
	}
}

// End to end on both WAL paths (covered and tracked): the WAL entry of a
// write without time carries the time ingest generated.
func TestGeneratedTimeReachesTheWAL(t *testing.T) {
	raw, err := msgpack.Marshal(map[string]interface{}{"m": "cpu", "columns": map[string]interface{}{"v": []interface{}{int64(1)}}})
	if err != nil {
		t.Fatal(err)
	}
	for _, covered := range []bool{true, false} {
		var reg *coverageRegistrar
		if covered {
			reg = &coverageRegistrar{}
		}
		b, w := newCoverageBuffer(t, 1000, reg)
		out, err := NewMessagePackDecoder(zerolog.Nop()).Decode(raw)
		if err != nil {
			t.Fatal(err)
		}
		rec := out.([]interface{})[0].(*models.ColumnarRecord)
		generated := rec.Columns["time"][0]
		if err := b.WriteColumnarRecord(context.Background(), "db", rec); err != nil {
			t.Fatal(err)
		}
		file := w.CurrentFile()
		if err := w.Rotate(); err != nil {
			t.Fatal(err)
		}
		entries, err := wal.NewReader(file, zerolog.Nop()).ReadAll()
		if err != nil || len(entries) != 1 {
			t.Fatalf("covered=%v: %d entries, %v", covered, len(entries), err)
		}
		rows := entries[0].Records
		if len(rows) != 1 || fmt.Sprint(rows[0]["time"]) != fmt.Sprint(generated) {
			t.Fatalf("covered=%v: WAL rows %v, want time %v", covered, rows, generated)
		}
		if covered && entries[0].Coverage == nil {
			t.Fatal("covered entry lost its coverage")
		}
	}
}
