// agg-6b' companion, tagged: tolerance / oraclediff sets, Boolean decoding,
// and the Inf-ness check the tolerance test could not see.

//go:build cgo && arcx_engine

package arcxrouter

import (
	"math"
	"math/big"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

func TestNumericAggSets(t *testing.T) {
	for _, item := range []string{"product(f)", "fsum(f)", "sumkahan(f)", "kahan_sum(f)", "favg(f)"} {
		if !aggItemTolerant(item) {
			t.Fatalf("aggItemTolerant(%q) = false", item)
		}
	}
	for _, item := range []string{"bit_or(i)", "bool_and(b)", "count_if(b)", "countif(b)"} {
		if aggItemTolerant(item) {
			t.Fatalf("aggItemTolerant(%q) = true (must compare exact)", item)
		}
	}
	for _, item := range []string{"fsum(f)", "sumkahan(f)", "kahan_sum(f)", "favg(f)"} {
		if !isOracleUnstableItem(item) {
			t.Fatalf("isOracleUnstableItem(%q) = false", item)
		}
	}
	if isOracleUnstableItem("product(f)") {
		t.Fatal("product is band-bounded and stable — not oraclediff")
	}
}

func TestCompareAggBooleanAndInf(t *testing.T) {
	fields := []arrow.Field{
		{Name: "bool_or(b)", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
		{Name: "product(f)", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	}
	items := []string{"bool_or(b)", "product(f)"}
	rec := func(b bool, f float64) arrow.Record {
		return oneRowRecord(t, fields, func(rb *array.RecordBuilder) {
			rb.Field(0).(*array.BooleanBuilder).Append(b)
			rb.Field(1).(*array.Float64Builder).Append(f)
		})
	}
	or := func(b bool, f float64) []aggCell { return []aggCell{{isBool: true, b: b}, {f: f}} }
	if d := compareAgg(rec(true, 2.5), or(true, 2.5), items); d != "" {
		t.Fatalf("expected match, got %q", d)
	}
	if d := compareAgg(rec(true, 2.5), or(false, 2.5), items); d == "" {
		t.Fatal("boolean difference must alarm")
	}
	if d := compareAgg(rec(true, 2.5), []aggCell{{isInt: true, i: big.NewInt(1)}, {f: 2.5}}, items); d == "" {
		t.Fatal("bool vs int kind difference must alarm")
	}
	// The blind spot: finite vs ±Inf used to pass the tolerance test.
	if d := compareAgg(rec(true, 1.0), or(true, math.Inf(1)), items); !strings.Contains(d, "Inf-ness") {
		t.Fatalf("finite vs +Inf must alarm as Inf-ness, got %q", d)
	}
	if d := compareAgg(rec(true, math.Inf(1)), or(true, math.Inf(-1)), items); !strings.Contains(d, "Inf-ness") {
		t.Fatalf("+Inf vs -Inf must alarm, got %q", d)
	}
	if d := compareAgg(rec(true, math.Inf(1)), or(true, math.Inf(1)), items); d != "" {
		t.Fatalf("+Inf vs +Inf must match, got %q", d)
	}
}
