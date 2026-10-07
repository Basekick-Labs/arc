package ingest

import (
	"math"
	"testing"

	"github.com/basekick-labs/arc/pkg/models"
)

// A rejected float must fall back to the generic write-time error, rather
// than become an already-converted integer in the typed fast path. Assert
// rejection, not the result of an invalid Go conversion (architecture-dependent).
func TestTypedDecodeFloatToInt64Rejections(t *testing.T) {
	values := []struct {
		name  string
		value interface{}
	}{
		{"float32 upper bound", float32(1 << 63)},
		{"float64 upper bound", float64(1 << 63)},
		{"float32 NaN", float32(math.NaN())},
		{"float64 NaN", math.NaN()},
		// Infinities and values beyond the boundaries were already rejected;
		// keep them as controls when sharing the conversion helper.
		{"float32 positive infinity", float32(math.Inf(1))},
		{"float32 negative infinity", float32(math.Inf(-1))},
		{"float64 positive infinity", math.Inf(1)},
		{"float64 negative infinity", math.Inf(-1)},
		{"float32 below lower bound", math.Nextafter32(-1<<63, float32(math.Inf(-1)))},
		{"float64 below lower bound", math.Nextafter(-1<<63, math.Inf(-1))},
		{"float32 above upper bound", math.Nextafter32(1<<63, float32(math.Inf(1)))},
		{"float64 above upper bound", math.Nextafter(1<<63, math.Inf(1))},
	}
	for _, tc := range values {
		t.Run(tc.name, func(t *testing.T) {
			columnar := map[string]interface{}{
				"m": "cpu", "columns": map[string]interface{}{
					"time": []interface{}{int64(1700000000000000), int64(1700000000000001)},
					"v":    []interface{}{int64(1), tc.value},
				},
			}
			for format, payload := range map[string]interface{}{
				"single": columnar,
				"batch":  map[string]interface{}{"batch": []interface{}{columnar}},
				"array":  []interface{}{columnar},
			} {
				t.Run(format, func(t *testing.T) {
					data := mustMarshal(t, payload)
					for _, enabled := range []bool{false, true} {
						d := newTypedTestDecoder(enabled)
						decoded, err := d.Decode(data)
						if err != nil {
							t.Fatalf("typed=%v: decode failed before write-time validation: %v", enabled, err)
						}
						if d.typedHits.Load() != 0 {
							t.Fatalf("typed=%v: invalid float bypassed write-time validation", enabled)
						}
						records, ok := decoded.([]interface{})
						if !ok || len(records) != 1 {
							t.Fatalf("unexpected decoded records: %T", decoded)
						}
						rec, ok := records[0].(*models.ColumnarRecord)
						if !ok {
							t.Fatalf("expected generic fallback, got %T", records[0])
						}
						if _, _, err := (&ArrowBuffer{}).convertColumnsToTyped(rec.Measurement, rec.Columns); err == nil {
							t.Fatalf("typed=%v: invalid float accepted as integer", enabled)
						}
					}
				})
			}
		})
	}
}

// Keep accepted boundaries and truncation on the fast path: always falling
// back would pass rejection tests while silently disabling typed decoding.
func TestTypedDecodeFloatToInt64ValidValues(t *testing.T) {
	for name, value := range map[string]interface{}{
		"float32 lower":             float32(-1 << 63),
		"float64 lower":             float64(-1 << 63),
		"float32 upper neighbor":    math.Nextafter32(1<<63, 0),
		"float64 upper neighbor":    math.Nextafter(1<<63, 0),
		"float32 positive fraction": float32(1.75),
		"float32 negative fraction": float32(-1.75),
		"float64 positive fraction": 1.75,
		"float64 negative fraction": -1.75,
	} {
		t.Run(name, func(t *testing.T) {
			payload := mustMarshal(t, map[string]interface{}{
				"m": "cpu", "columns": map[string]interface{}{
					"time": []interface{}{int64(1700000000000000), int64(1700000000000001)},
					"v":    []interface{}{int64(1), value},
				},
			})
			assertEquivalent(t, name, payload, false)
		})
	}
}
