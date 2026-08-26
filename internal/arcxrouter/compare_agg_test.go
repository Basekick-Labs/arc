//go:build cgo && arcx_engine

package arcxrouter

import (
	"math"
	"math/big"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// aggRec builds a 1-row record: each cell is nil (NULL), int64, or float64.
func aggRec(t *testing.T, cells ...interface{}) arrow.Record {
	t.Helper()
	pool := memory.NewGoAllocator()
	fields := make([]arrow.Field, len(cells))
	cols := make([]arrow.Array, len(cells))
	for i, c := range cells {
		switch v := c.(type) {
		case int64:
			b := array.NewInt64Builder(pool)
			b.Append(v)
			cols[i] = b.NewArray()
			fields[i] = arrow.Field{Name: "c", Type: arrow.PrimitiveTypes.Int64, Nullable: true}
		case float64:
			b := array.NewFloat64Builder(pool)
			b.Append(v)
			cols[i] = b.NewArray()
			fields[i] = arrow.Field{Name: "c", Type: arrow.PrimitiveTypes.Float64, Nullable: true}
		case nil:
			b := array.NewFloat64Builder(pool)
			b.AppendNull()
			cols[i] = b.NewArray()
			fields[i] = arrow.Field{Name: "c", Type: arrow.PrimitiveTypes.Float64, Nullable: true}
		default:
			t.Fatalf("unsupported cell %T", c)
		}
	}
	return array.NewRecord(arrow.NewSchema(fields, nil), cols, 1)
}

func TestCompareAggMatches(t *testing.T) {
	cases := []struct {
		name   string
		rec    arrow.Record
		oracle []aggCell
		items  []string
	}{
		{
			"count and int sum exact (HUGEINT big.Int path)",
			aggRec(t, int64(5), int64(1500)),
			[]aggCell{{isInt: true, i: big.NewInt(5)}, {isInt: true, i: big.NewInt(1500)}},
			[]string{"count(*)", "sum(x)"},
		},
		{
			"float sum within 1e-9 relative tolerance",
			aggRec(t, 73469570793.69745),
			[]aggCell{{f: 73469570793.63945}},
			[]string{"sum(value)"},
		},
		{
			"min/max signed zero: -0.0 == 0.0 numerically",
			aggRec(t, math.Copysign(0, -1), 0.0),
			[]aggCell{{f: 0.0}, {f: math.Copysign(0, -1)}},
			[]string{"min(f)", "max(f)"},
		},
		{
			"both NaN agree",
			aggRec(t, math.NaN()),
			[]aggCell{{f: math.NaN()}},
			[]string{"sum(f)"},
		},
		{
			"empty-set NULLs agree",
			aggRec(t, nil),
			[]aggCell{{isNull: true}},
			[]string{"avg(x)"},
		},
	}
	for _, c := range cases {
		if diff := compareAgg(c.rec, c.oracle, c.items); diff != "" {
			t.Fatalf("%s: unexpected diff %q", c.name, diff)
		}
	}
}

func TestCompareAggMismatches(t *testing.T) {
	cases := []struct {
		name   string
		rec    arrow.Record
		oracle []aggCell
		items  []string
	}{
		{
			"int value differs",
			aggRec(t, int64(5)),
			[]aggCell{{isInt: true, i: big.NewInt(6)}},
			[]string{"count(*)"},
		},
		{
			"min/max floats are EXACT (no tolerance)",
			aggRec(t, 1.0000000000001),
			[]aggCell{{f: 1.0}},
			[]string{"min(f)"},
		},
		{
			"float sum beyond tolerance",
			aggRec(t, 1.001),
			[]aggCell{{f: 1.0}},
			[]string{"sum(f)"},
		},
		{
			"one-sided NaN is a divergence",
			aggRec(t, math.NaN()),
			[]aggCell{{f: 1.0}},
			[]string{"sum(f)"},
		},
		{
			"null-ness differs",
			aggRec(t, nil),
			[]aggCell{{f: 1.0}},
			[]string{"avg(f)"},
		},
	}
	for _, c := range cases {
		diff := compareAgg(c.rec, c.oracle, c.items)
		if diff == "" {
			t.Fatalf("%s: expected a diff, got match", c.name)
		}
		// Value-free discipline: the diff class may name kinds, never values.
		for _, leak := range []string{"5", "6", "1.0", "73469"} {
			if strings.Contains(diff, leak) {
				t.Fatalf("%s: diff leaks a value: %q", c.name, diff)
			}
		}
	}
}
