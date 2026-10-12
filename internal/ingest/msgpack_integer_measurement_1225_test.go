package ingest

import (
	"testing"

	"github.com/Basekick-Labs/msgpack/v6"
	"github.com/basekick-labs/arc/internal/wal"
	"github.com/basekick-labs/arc/pkg/models"
	"github.com/rs/zerolog"
)

// #1225: the WAL keeps the payload the client sent, so the WAL reader must
// name an integer measurement exactly as ingest did, on both decode paths.
func TestWALNamesIntegerMeasurementAsIngestDoes(t *testing.T) {
	for _, m := range []interface{}{int8(7), int64(-3), uint64(1 << 40), 300, uint8(255)} {
		raw, err := msgpack.Marshal(map[string]interface{}{
			"m":       m,
			"columns": map[string]interface{}{"time": []interface{}{int64(1700000000000000)}, "v": []interface{}{int64(1)}},
		})
		if err != nil {
			t.Fatal(err)
		}
		var decoded map[string]interface{}
		if err := msgpack.Unmarshal(raw, &decoded); err != nil {
			t.Fatal(err)
		}
		walName, ok := wal.MeasurementName(decoded["m"])
		if !ok {
			t.Fatalf("WAL cannot name %#v", m)
		}
		for _, typed := range []bool{false, true} {
			d := NewMessagePackDecoder(zerolog.Nop())
			d.SetTypedDecodeEnabled(typed)
			out, err := d.Decode(raw)
			if err != nil {
				t.Fatal(err)
			}
			var live string
			switch r := out.([]interface{})[0].(type) {
			case *models.ColumnarRecord:
				live = r.Measurement
			case *TypedColumnarRecord:
				live = r.Measurement
			default:
				t.Fatalf("decoded %T", r)
			}
			if live != walName {
				t.Errorf("m=%#v typed=%v: ingest %q, WAL %q", m, typed, live, walName)
			}
		}
	}
}
