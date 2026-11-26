//go:build duckdb_arrow

package api

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"google.golang.org/grpc/metadata"
)

func TestNormalizeDecimalSchemaForMode(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "wide", Type: &arrow.Decimal128Type{Precision: 38, Scale: 0}},
		{Name: "narrow", Type: &arrow.Decimal128Type{Precision: 18, Scale: 0}},
		{Name: "fractional", Type: &arrow.Decimal128Type{Precision: 10, Scale: 2}},
		{Name: "boundary", Type: &arrow.Decimal128Type{Precision: 19, Scale: 0}},
	}, nil)

	defaultInfo := normalizeDecimalSchemaForMode(schema, decimalModeDefault)
	if defaultInfo.schema.Field(0).Type != arrow.PrimitiveTypes.Int64 || defaultInfo.schema.Field(1).Type != arrow.PrimitiveTypes.Int64 || defaultInfo.schema.Field(2).Type != arrow.PrimitiveTypes.Float64 {
		t.Fatalf("default schema changed: %s", defaultInfo.schema)
	}

	exactInfo := normalizeDecimalSchemaForMode(schema, decimalModeExact)
	if exactInfo.schema.Field(0).Type != schema.Field(0).Type {
		t.Fatalf("wide scale-0 decimal should be retained: got %s", exactInfo.schema.Field(0).Type)
	}
	if exactInfo.schema.Field(1).Type != arrow.PrimitiveTypes.Int64 || exactInfo.schema.Field(2).Type != arrow.PrimitiveTypes.Float64 {
		t.Fatalf("exact mode should retain historical conversions for other decimals: %s", exactInfo.schema)
	}
	if exactInfo.targets[0] != nil || exactInfo.targets[1] != arrow.PrimitiveTypes.Int64 || exactInfo.targets[2] != arrow.PrimitiveTypes.Float64 {
		t.Fatalf("unexpected exact cast targets: %#v", exactInfo.targets)
	}
	// Precision 19 is the first one int64 cannot hold: retained under exact,
	// int64 by default.
	if exactInfo.schema.Field(3).Type != schema.Field(3).Type || exactInfo.targets[3] != nil {
		t.Fatalf("decimal(19,0) should be retained under exact: %s", exactInfo.schema.Field(3).Type)
	}
	if defaultInfo.schema.Field(3).Type != arrow.PrimitiveTypes.Int64 {
		t.Fatalf("decimal(19,0) should be int64 by default: %s", defaultInfo.schema.Field(3).Type)
	}

	// Only retained decimals: nothing to cast, so no cast info (each batch
	// streams as DuckDB built it). By default the same schema is cast.
	wideOnly := arrow.NewSchema([]arrow.Field{
		{Name: "s", Type: &arrow.Decimal128Type{Precision: 38, Scale: 0}},
		{Name: "tag", Type: arrow.BinaryTypes.String},
	}, nil)
	if info := normalizeDecimalSchemaForMode(wideOnly, decimalModeExact); info != nil {
		t.Fatalf("exact with only wide decimals: cast info %s, want nil", info.schema)
	}
	if info := normalizeDecimalSchemaForMode(wideOnly, decimalModeDefault); info == nil || info.targets[0] != arrow.PrimitiveTypes.Int64 {
		t.Fatal("default must cast the wide decimal")
	}
}

// decimalModeOf is the rule both protocols share; the cross-protocol test is
// TestDecimalsHeaderRuleIsTheSameOnHTTPAndFlight1784.
func TestDecimalModeOf(t *testing.T) {
	for _, c := range []struct {
		values  []string
		mode    string
		present bool
		err     bool
	}{
		{nil, decimalModeDefault, false, false},
		{[]string{"", " \t"}, decimalModeDefault, false, false},
		{[]string{" exact "}, decimalModeExact, true, false},
		{[]string{"default"}, decimalModeDefault, true, false},
		{[]string{"", "exact", ""}, decimalModeExact, true, false},
		{[]string{"exact", "exact"}, "", true, true},
		{[]string{"Exact"}, "", true, true},
		{[]string{"exact,default"}, "", true, true},
	} {
		mode, present, err := decimalModeOf(c.values)
		if mode != c.mode || present != c.present || (err != nil) != c.err {
			t.Errorf("%q: got %q %v %v", c.values, mode, present, err)
		}
	}
}

func TestFlightSQLDecimalMode(t *testing.T) {
	ctx := context.Background()
	if mode, present, err := flightSQLDecimalMode(ctx); err != nil || present || mode != decimalModeDefault {
		t.Fatalf("absent mode = %q, %v, %v", mode, present, err)
	}
	exactCtx := metadata.NewIncomingContext(ctx, metadata.Pairs(decimalsHeader, decimalModeExact))
	if mode, present, err := flightSQLDecimalMode(exactCtx); err != nil || !present || mode != decimalModeExact {
		t.Fatalf("exact mode = %q, %v, %v", mode, present, err)
	}
	if _, err := flightStatementDecimalMode(ctx, decimalModeExact); err == nil {
		t.Fatal("expected statement execution with a different mode to fail")
	}
	duplicateCtx := metadata.NewIncomingContext(ctx, metadata.Pairs(decimalsHeader, decimalModeExact, decimalsHeader, decimalModeDefault))
	if _, _, err := flightSQLDecimalMode(duplicateCtx); err == nil {
		t.Fatal("expected duplicate mode headers to fail")
	}
}
