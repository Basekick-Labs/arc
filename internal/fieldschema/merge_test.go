package fieldschema

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
)

func fields(kv ...interface{}) *arrow.Schema {
	var fs []arrow.Field
	for i := 0; i < len(kv); i += 2 {
		fs = append(fs, arrow.Field{Name: kv[i].(string), Type: kv[i+1].(arrow.DataType), Nullable: true})
	}
	return arrow.NewSchema(fs, nil)
}

var (
	tBool   = arrow.FixedWidthTypes.Boolean
	tInt32  = arrow.PrimitiveTypes.Int32
	tInt64  = arrow.PrimitiveTypes.Int64
	tFloat  = arrow.PrimitiveTypes.Float64
	tStr    = arrow.BinaryTypes.String
	tTsTZ   = arrow.FixedWidthTypes.Timestamp_us
	tTs     = &arrow.TimestampType{Unit: arrow.Microsecond}
	tDec186 = &arrow.Decimal128Type{Precision: 18, Scale: 6}
	tDec102 = &arrow.Decimal128Type{Precision: 10, Scale: 2}
)

func TestNormalizeOrdersTimeTagsFields(t *testing.T) {
	s := arrow.NewSchema([]arrow.Field{
		{Name: "value", Type: tFloat}, {Name: "host", Type: tStr}, {Name: "time", Type: tTsTZ}, {Name: "region", Type: tStr}, {Name: "count", Type: tInt64},
	}, nil)
	n := Normalize(s, []string{"region", "host"})
	var names []string
	for _, f := range n.Fields() {
		names = append(names, f.Name)
		if !f.Nullable {
			t.Errorf("%s not nullable", f.Name)
		}
	}
	want := []string{"time", "host", "region", "count", "value"}
	if len(names) != len(want) {
		t.Fatalf("names=%v", names)
	}
	for i := range want {
		if names[i] != want[i] {
			t.Fatalf("names=%v want %v", names, want)
		}
	}
	// arc:tags metadata is honored when no tag list is passed.
	md := arrow.NewMetadata([]string{"arc:tags"}, []string{"host,region"})
	s2 := arrow.NewSchema(s.Fields(), &md)
	n2 := Normalize(s2, nil)
	if Fingerprint(n2) != Fingerprint(n) {
		t.Fatalf("metadata tags ignored: %v vs %v", n2, n)
	}
}

func TestMergeNarrowsNeverWidens(t *testing.T) {
	cases := []struct {
		name         string
		stored, in   arrow.DataType
		want         arrow.DataType
		wantConflict bool
	}{
		{"same", tInt64, tInt64, tInt64, false},
		{"bigint then double keeps bigint", tInt64, tFloat, tInt64, false},
		{"double then bigint narrows", tFloat, tInt64, tInt64, false},
		{"varchar then bigint narrows", tStr, tInt64, tInt64, false},
		{"bigint then varchar keeps bigint", tInt64, tStr, tInt64, false},
		{"bool vs bigint keeps bool", tBool, tInt64, tBool, false},
		{"bigint then bool narrows", tInt64, tBool, tBool, false},
		{"int32 vs bigint keeps int32", tInt32, tInt64, tInt32, false},
		{"double then decimal narrows", tFloat, tDec186, tDec186, false},
		{"decimal then double keeps decimal", tDec186, tFloat, tDec186, false},
		{"tz then naive narrows", tTsTZ, tTs, tTs, false},
		{"naive then tz keeps naive", tTs, tTsTZ, tTs, false},
		{"bigint vs decimal falls to bottom", tInt64, tDec186, tBool, true},
		{"decimal then smaller decimal narrows", tDec186, tDec102, tDec102, false},
		{"smaller decimal then larger keeps", tDec102, tDec186, tDec102, false},
		{"decimals with crossed precision and scale fall to bottom", &arrow.Decimal128Type{Precision: 12, Scale: 2}, &arrow.Decimal128Type{Precision: 10, Scale: 6}, tBool, true},
		{"tinyint below integer", tInt32, arrow.PrimitiveTypes.Int8, arrow.PrimitiveTypes.Int8, false},
		{"bottom stays bottom against anything", tBool, tDec186, tBool, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			merged, _, conflicts := Merge(fields("x", c.stored), fields("x", c.in))
			got := merged.Field(0).Type
			if !arrow.TypeEqual(got, c.want) {
				t.Fatalf("got %s want %s", got, c.want)
			}
			if (len(conflicts) > 0) != c.wantConflict {
				t.Fatalf("conflicts=%v", conflicts)
			}
			// Covers agrees with Merge: no change means covered.
			m2, changed, _ := Merge(fields("x", c.stored), fields("x", c.in))
			if Covers(fields("x", c.stored), fields("x", c.in)) == changed {
				t.Fatalf("Covers disagrees with Merge (changed=%v) for %v", changed, m2)
			}
		})
	}
}

func TestMergeAppendsNewFieldsAndKeepsOrder(t *testing.T) {
	stored := fields("time", tTsTZ, "host", tStr, "stable", tInt64)
	in := Normalize(fields("weeks_later", tInt64, "time", tTsTZ, "stable", tInt64), nil)
	merged, changed, _ := Merge(stored, in)
	if !changed {
		t.Fatal("new field not detected")
	}
	var names []string
	for _, f := range merged.Fields() {
		names = append(names, f.Name)
	}
	if got := len(names); got != 4 || names[3] != "weeks_later" || names[0] != "time" || names[1] != "host" {
		t.Fatalf("order=%v", names)
	}
	if Covers(stored, in) {
		t.Fatal("Covers must be false when a field is new")
	}
	if !Covers(merged, in) || !Covers(merged, stored) {
		t.Fatal("merged must cover both inputs")
	}
	if _, changed, _ := Merge(nil, in); !changed {
		t.Fatal("nil stored must report a change")
	}
}
