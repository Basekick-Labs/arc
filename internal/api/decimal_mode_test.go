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
