package ingest

import (
	"math"
	"reflect"
	"strings"
	"testing"

	"github.com/Basekick-Labs/msgpack/v6"
	"github.com/basekick-labs/arc/pkg/models"
	"github.com/rs/zerolog"
)

// mustMarshal encodes a payload with the same msgpack library clients use.
func mustMarshal(t *testing.T, v interface{}) []byte {
	t.Helper()
	b, err := msgpack.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return b
}

func newTypedTestDecoder(typed bool) *MessagePackDecoder {
	d := NewMessagePackDecoder(zerolog.Nop())
	d.SetTypedDecodeEnabled(typed)
	return d
}

// boxedToTyped runs the generic path's conversion, producing the reference
// TypedColumnBatch the typed decoder must match. A zero-value ArrowBuffer is
// sufficient: convertColumnsToTyped only consults decimal config (nil here)
// and pure helpers.
func boxedToTyped(t *testing.T, rec *models.ColumnarRecord) (*TypedColumnBatch, int) {
	t.Helper()
	b := &ArrowBuffer{}
	batch, n, err := b.convertColumnsToTyped(rec.Measurement, rec.Columns)
	if err != nil {
		t.Fatalf("reference conversion failed: %v", err)
	}
	return batch, n
}

// decodeBoth runs Decode with the typed path on and off. Returns the typed
// record (nil if the typed path fell back) and the boxed record.
func decodeBoth(t *testing.T, payload []byte) (*TypedColumnarRecord, *models.ColumnarRecord) {
	t.Helper()
	dTyped := newTypedTestDecoder(true)
	dBoxed := newTypedTestDecoder(false)

	typedRes, typedErr := dTyped.Decode(payload)
	boxedRes, boxedErr := dBoxed.Decode(payload)

	if (typedErr == nil) != (boxedErr == nil) {
		t.Fatalf("accept/reject divergence: typed err=%v boxed err=%v", typedErr, boxedErr)
	}
	if typedErr != nil {
		return nil, nil
	}

	var typedRec *TypedColumnarRecord
	if list, ok := typedRes.([]interface{}); ok && len(list) == 1 {
		typedRec, _ = list[0].(*TypedColumnarRecord)
	}
	var boxedRec *models.ColumnarRecord
	if list, ok := boxedRes.([]interface{}); ok && len(list) == 1 {
		boxedRec, _ = list[0].(*models.ColumnarRecord)
	}
	return typedRec, boxedRec
}

// assertEquivalent checks the typed decoder's output matches the generic
// path's conversion exactly: measurement, record count, typed data, validity
// (presence AND contents), and signature.
func assertEquivalent(t *testing.T, name string, payload []byte, skipTimeValues bool) {
	t.Helper()
	typedRec, boxedRec := decodeBoth(t, payload)
	if typedRec == nil {
		t.Fatalf("%s: typed path fell back (expected hit)", name)
	}
	if boxedRec == nil {
		t.Fatalf("%s: boxed path did not produce a ColumnarRecord", name)
	}

	refBatch, refN := boxedToTyped(t, boxedRec)

	if typedRec.Measurement != boxedRec.Measurement {
		t.Errorf("%s: measurement %q != %q", name, typedRec.Measurement, boxedRec.Measurement)
	}
	if typedRec.NumRecords != refN {
		t.Errorf("%s: numRecords %d != %d", name, typedRec.NumRecords, refN)
	}
	if typedRec.Batch.Signature != refBatch.Signature {
		t.Errorf("%s: signature %q != %q", name, typedRec.Batch.Signature, refBatch.Signature)
	}

	gotData := typedRec.Batch.Data
	wantData := refBatch.Data
	if skipTimeValues {
		gotData = copyWithoutKey(gotData, "time")
		wantData = copyWithoutKey(wantData, "time")
		gt, gok := typedRec.Batch.Data["time"].([]int64)
		wt, wok := refBatch.Data["time"].([]int64)
		if !gok || !wok || len(gt) != len(wt) {
			t.Errorf("%s: generated time column shape mismatch", name)
		}
	}
	if !reflect.DeepEqual(gotData, wantData) {
		t.Errorf("%s: data mismatch\n got: %#v\nwant: %#v", name, gotData, wantData)
	}
	if !reflect.DeepEqual(map[string][]bool(typedRec.Batch.Validity), map[string][]bool(refBatch.Validity)) {
		t.Errorf("%s: validity mismatch (presence matters)\n got: %#v\nwant: %#v", name, typedRec.Batch.Validity, refBatch.Validity)
	}
	// RawPayload must be the original client bytes (zero-copy WAL contract),
	// same as the generic path's ColumnarRecord.RawPayload.
	if !reflect.DeepEqual(typedRec.RawPayload, payload) {
		t.Errorf("%s: RawPayload does not equal original payload bytes", name)
	}
	if !reflect.DeepEqual(boxedRec.RawPayload, payload) {
		t.Errorf("%s: reference RawPayload does not equal original payload bytes", name)
	}
}

func copyWithoutKey(m map[string]interface{}, key string) map[string]interface{} {
	out := make(map[string]interface{}, len(m))
	for k, v := range m {
		if k != key {
			out[k] = v
		}
	}
	return out
}

func TestTypedDecodeEquivalence(t *testing.T) {
	cases := []struct {
		name           string
		payload        map[string]interface{}
		skipTimeValues bool
	}{
		{"int64_columns", map[string]interface{}{
			"m": "cpu", "columns": map[string]interface{}{
				"time": []interface{}{int64(1700000000000000), int64(1700000000000001)},
				"v":    []interface{}{int64(1), int64(2)},
			}}, false},
		{"float_columns", map[string]interface{}{
			"m": "cpu", "columns": map[string]interface{}{
				"time": []interface{}{int64(1700000000000000)},
				"v":    []interface{}{3.14},
			}}, false},
		{"string_columns_with_sanitize", map[string]interface{}{
			"m": "cpu", "columns": map[string]interface{}{
				"time": []interface{}{int64(1700000000000000), int64(1700000000000001)},
				"host": []interface{}{"h1", "bad\xff\xfeutf8"},
			}}, false},
		{"bool_columns", map[string]interface{}{
			"m": "cpu", "columns": map[string]interface{}{
				"time": []interface{}{int64(1700000000000000)},
				"up":   []interface{}{true},
			}}, false},
		{"int_first_then_float_truncates", map[string]interface{}{
			"m": "cpu", "columns": map[string]interface{}{
				"time": []interface{}{int64(1700000000000000), int64(1700000000000001)},
				"v":    []interface{}{int64(1), 2.7},
			}}, false},
		{"float_first_then_int_coerces", map[string]interface{}{
			"m": "cpu", "columns": map[string]interface{}{
				"time": []interface{}{int64(1700000000000000), int64(1700000000000001)},
				"v":    []interface{}{2.5, int64(7)},
			}}, false},
		{"nils_build_validity", map[string]interface{}{
			"m": "cpu", "columns": map[string]interface{}{
				"time": []interface{}{int64(1700000000000000), int64(1700000000000001), int64(1700000000000002)},
				"v":    []interface{}{int64(1), nil, int64(3)},
				"s":    []interface{}{nil, "x", nil},
			}}, false},
		{"all_nil_column_becomes_string", map[string]interface{}{
			"m": "cpu", "columns": map[string]interface{}{
				"time": []interface{}{int64(1700000000000000), int64(1700000000000001)},
				"v":    []interface{}{nil, nil},
			}}, false},
		{"leading_nils_then_int", map[string]interface{}{
			"m": "cpu", "columns": map[string]interface{}{
				"time": []interface{}{int64(1700000000000000), int64(1700000000000001), int64(1700000000000002)},
				"v":    []interface{}{nil, nil, int64(9)},
			}}, false},
		{"time_seconds_unit", map[string]interface{}{
			"m": "cpu", "columns": map[string]interface{}{
				"time": []interface{}{int64(1700000000), int64(1700000001)},
				"v":    []interface{}{int64(1), int64(2)},
			}}, false},
		{"time_millis_unit", map[string]interface{}{
			"m": "cpu", "columns": map[string]interface{}{
				"time": []interface{}{int64(1700000000000)},
				"v":    []interface{}{int64(1)},
			}}, false},
		{"time_nanos_unit", map[string]interface{}{
			"m": "cpu", "columns": map[string]interface{}{
				"time": []interface{}{int64(1700000000000000000)},
				"v":    []interface{}{int64(1)},
			}}, false},
		{"time_float_values", map[string]interface{}{
			"m": "cpu", "columns": map[string]interface{}{
				"time": []interface{}{1.7e15, 1.7e15},
				"v":    []interface{}{int64(1), int64(2)},
			}}, false},
		{"missing_time_generated", map[string]interface{}{
			"m": "cpu", "columns": map[string]interface{}{
				"v": []interface{}{int64(1), int64(2)},
			}}, true},
		{"small_fixints", map[string]interface{}{
			"m": "cpu", "columns": map[string]interface{}{
				"time": []interface{}{int64(1700000000000000)},
				"v":    []interface{}{int8(5)},
			}}, false},
		{"negative_values", map[string]interface{}{
			"m": "cpu", "columns": map[string]interface{}{
				"time": []interface{}{int64(1700000000000000)},
				"v":    []interface{}{int64(-42)},
			}}, false},
		{"negative_fixint_values", map[string]interface{}{
			// -1..-32 encode as negative fixint codes (0xe0-0xff) — a distinct
			// decode branch from Int8 (-42 encodes as Int8, not neg-fixint).
			"m": "cpu", "columns": map[string]interface{}{
				"time": []interface{}{int64(1700000000000000), int64(1700000000000001)},
				"v":    []interface{}{int64(-1), int64(-32)},
			}}, false},
		{"uint64_wrap_in_time_column", map[string]interface{}{
			// toInt64Timestamp wraps uint64 unconditionally; typed must match.
			"m": "cpu", "columns": map[string]interface{}{
				"time": []interface{}{uint64(math.MaxInt64) + 12345},
				"v":    []interface{}{int64(1)},
			}}, false},
		{"uint64_in_float_column", map[string]interface{}{
			"m": "cpu", "columns": map[string]interface{}{
				"time": []interface{}{int64(1700000000000000), int64(1700000000000001)},
				"v":    []interface{}{1.5, uint64(math.MaxInt64) + 7},
			}}, false},
		{"float32_time_values", map[string]interface{}{
			"m": "cpu", "columns": map[string]interface{}{
				"time": []interface{}{float32(1.7e9)},
				"v":    []interface{}{int64(1)},
			}}, false},
		{"negative_fixint_measurement", map[string]interface{}{
			"m": int64(-7), "columns": map[string]interface{}{
				"time": []interface{}{int64(1700000000000000)},
				"v":    []interface{}{int64(1)},
			}}, false},
		{"float32_values", map[string]interface{}{
			"m": "cpu", "columns": map[string]interface{}{
				"time": []interface{}{int64(1700000000000000)},
				"v":    []interface{}{float32(1.5)},
			}}, false},
		{"uint64_below_max", map[string]interface{}{
			"m": "cpu", "columns": map[string]interface{}{
				"time": []interface{}{int64(1700000000000000)},
				"v":    []interface{}{uint64(math.MaxInt64)},
			}}, false},
		{"int_measurement", map[string]interface{}{
			"m": int64(7), "columns": map[string]interface{}{
				"time": []interface{}{int64(1700000000000000)},
				"v":    []interface{}{int64(1)},
			}}, false},
		{"extra_keys_ignored", map[string]interface{}{
			"m": "cpu", "t": int64(123), "h": "srv", "tags": map[string]interface{}{"a": "b"},
			"columns": map[string]interface{}{
				"time": []interface{}{int64(1700000000000000)},
				"v":    []interface{}{int64(1)},
			}}, false},
		{"dropped_non_array_column_value", map[string]interface{}{
			"m": "cpu", "columns": map[string]interface{}{
				"time":    []interface{}{int64(1700000000000000)},
				"v":       []interface{}{int64(1)},
				"ignored": int64(5), // non-array value: silently dropped today
			}}, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assertEquivalent(t, tc.name, mustMarshal(t, tc.payload), tc.skipTimeValues)
		})
	}
}

// TestTypedDecodeRejectEquivalence: payloads the generic path rejects must be
// rejected identically with the typed path enabled (which falls back to the
// generic path for the actual rejection — asserting err presence on both).
func TestTypedDecodeRejectEquivalence(t *testing.T) {
	cases := []struct {
		name    string
		payload map[string]interface{}
	}{
		{"string_time", map[string]interface{}{
			"m": "cpu", "columns": map[string]interface{}{
				"time": []interface{}{"2026-01-01"},
				"v":    []interface{}{int64(1)},
			}}},
		{"nil_in_time", map[string]interface{}{
			"m": "cpu", "columns": map[string]interface{}{
				"time": []interface{}{int64(1700000000000000), nil},
				"v":    []interface{}{int64(1), int64(2)},
			}}},
		{"all_nil_time", map[string]interface{}{
			"m": "cpu", "columns": map[string]interface{}{
				"time": []interface{}{nil, nil},
				"v":    []interface{}{int64(1), int64(2)},
			}}},
		{"length_mismatch", map[string]interface{}{
			"m": "cpu", "columns": map[string]interface{}{
				"time": []interface{}{int64(1700000000000000), int64(1700000000000001)},
				"v":    []interface{}{int64(1)},
			}}},
		{"missing_measurement", map[string]interface{}{
			"columns": map[string]interface{}{
				"time": []interface{}{int64(1700000000000000)},
				"v":    []interface{}{int64(1)},
			}}},
		{"empty_columns", map[string]interface{}{
			"m": "cpu", "columns": map[string]interface{}{}}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			payload := mustMarshal(t, tc.payload)
			dTyped := newTypedTestDecoder(true)
			dBoxed := newTypedTestDecoder(false)
			_, typedErr := dTyped.Decode(payload)
			_, boxedErr := dBoxed.Decode(payload)
			if boxedErr == nil {
				t.Fatalf("expected the generic path to reject %s", tc.name)
			}
			if typedErr == nil {
				t.Fatalf("typed-enabled decode accepted %s that generic path rejects", tc.name)
			}
			// Exact-text equality is wrong for errors that embed a column
			// name chosen by map-iteration order (length_mismatch reports
			// whichever column the validation loop visited first — two
			// independent Decode calls can legitimately name different
			// columns). Compare the deterministic prefix instead.
			te, be := typedErr.Error(), boxedErr.Error()
			if te != be {
				common := "columnar format: array length mismatch"
				if !(strings.Contains(te, common) && strings.Contains(be, common)) {
					t.Errorf("error text diverged:\n typed: %v\n boxed: %v", typedErr, boxedErr)
				}
			}
		})
	}
}

// TestTypedDecodeWriteTimeRejects: payloads that pass Decode today but are
// rejected later at write time by convertColumnsToTyped (uint64 overflow,
// bin columns, mid-column type mismatches, nested values). The typed path
// must FALL BACK on these — never produce a typed record — so the write-time
// rejection is preserved unchanged.
func TestTypedDecodeWriteTimeRejects(t *testing.T) {
	cases := []struct {
		name    string
		payload map[string]interface{}
	}{
		{"uint64_overflow_in_int_column", map[string]interface{}{
			"m": "cpu", "columns": map[string]interface{}{
				"time": []interface{}{int64(1700000000000000), int64(1700000000000001)},
				"v":    []interface{}{int64(1), uint64(math.MaxInt64) + 1},
			}}},
		{"bin_column", map[string]interface{}{
			"m": "cpu", "columns": map[string]interface{}{
				"time": []interface{}{int64(1700000000000000)},
				"v":    []interface{}{[]byte{0x01, 0x02}},
			}}},
		{"string_in_int_column", map[string]interface{}{
			"m": "cpu", "columns": map[string]interface{}{
				"time": []interface{}{int64(1700000000000000), int64(1700000000000001)},
				"v":    []interface{}{int64(1), "oops"},
			}}},
		{"nested_array_column", map[string]interface{}{
			"m": "cpu", "columns": map[string]interface{}{
				"time": []interface{}{int64(1700000000000000)},
				"v":    []interface{}{[]interface{}{int64(1)}},
			}}},
		{"bool_then_int_column", map[string]interface{}{
			"m": "cpu", "columns": map[string]interface{}{
				"time": []interface{}{int64(1700000000000000), int64(1700000000000001)},
				"v":    []interface{}{true, int64(1)},
			}}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			payload := mustMarshal(t, tc.payload)
			d := newTypedTestDecoder(true)
			res, err := d.Decode(payload)
			if err != nil {
				t.Fatalf("decode should succeed (rejection happens at write time): %v", err)
			}
			if d.typedHits.Load() != 0 {
				t.Fatal("typed path must fall back on write-time-rejected payloads")
			}
			// The boxed record it fell back to must still be rejected by the
			// write-time conversion, same as today.
			list, _ := res.([]interface{})
			if len(list) != 1 {
				t.Fatalf("expected one record, got %d", len(list))
			}
			rec, ok := list[0].(*models.ColumnarRecord)
			if !ok {
				t.Fatalf("expected ColumnarRecord fallback, got %T", list[0])
			}
			b := &ArrowBuffer{}
			if _, _, convErr := b.convertColumnsToTyped(rec.Measurement, rec.Columns); convErr == nil {
				t.Fatal("write-time conversion should reject this payload")
			}
		})
	}
}

// TestTypedDecodeFallbackFormats: formats outside the typed scope must still
// decode via the generic path (batch, array, row) and produce identical
// results with the typed flag on.
func TestTypedDecodeFallbackFormats(t *testing.T) {
	payloads := map[string]interface{}{
		"batch_format": map[string]interface{}{
			"batch": []interface{}{
				map[string]interface{}{
					"m": "cpu", "columns": map[string]interface{}{
						"time": []interface{}{int64(1700000000000000)},
						"v":    []interface{}{int64(1)},
					}},
			}},
		"row_format": map[string]interface{}{
			"m": "cpu", "t": int64(1700000000000), "h": "srv",
			"fields": map[string]interface{}{"v": int64(1)},
		},
		"batch_key_wins_over_columns": map[string]interface{}{
			"batch": []interface{}{
				map[string]interface{}{
					"m": "cpu", "columns": map[string]interface{}{
						"time": []interface{}{int64(1700000000000000)},
						"v":    []interface{}{int64(1)},
					}},
			},
			"columns": map[string]interface{}{
				"time":    []interface{}{int64(1700000000000000)},
				"ignored": []interface{}{int64(9)},
			},
			"m": "wrong",
		},
	}
	for name, p := range payloads {
		t.Run(name, func(t *testing.T) {
			payload := mustMarshal(t, p)
			dTyped := newTypedTestDecoder(true)
			res, err := dTyped.Decode(payload)
			if err != nil {
				t.Fatalf("decode failed: %v", err)
			}
			if dTyped.typedHits.Load() != 0 {
				t.Fatalf("typed path claimed a hit on out-of-scope format %s", name)
			}
			if res == nil {
				t.Fatal("nil result")
			}
		})
	}
}

// TestUnwiredRecordTypeFailsLoudly pins the write-dispatch guard: a record
// type the dispatch doesn't know must produce an ERROR, not a silent drop
// with a success response (which would also mean measurement validation and
// RBAC were skipped for it in the handler).
func TestUnwiredRecordTypeFailsLoudly(t *testing.T) {
	b := &ArrowBuffer{}
	type mysteryRecord struct{}
	err := b.Write(nil, "db", []interface{}{&mysteryRecord{}})
	if err == nil {
		t.Fatal("unknown record type must fail the write, not be silently dropped")
	}
}

// TestTypedDecodeHitCounter ensures the hot-path payload shape actually takes
// the typed path (the equivalence suite would silently pass via fallback
// otherwise).
func TestTypedDecodeHitCounter(t *testing.T) {
	payload := mustMarshal(t, map[string]interface{}{
		"m": "cpu", "columns": map[string]interface{}{
			"time": []interface{}{int64(1700000000000000)},
			"host": []interface{}{"h1"},
			"v":    []interface{}{1.5},
		}})
	d := newTypedTestDecoder(true)
	if _, err := d.Decode(payload); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if d.typedHits.Load() != 1 || d.typedMisses.Load() != 0 {
		t.Fatalf("expected typed hit: hits=%d misses=%d", d.typedHits.Load(), d.typedMisses.Load())
	}
}
