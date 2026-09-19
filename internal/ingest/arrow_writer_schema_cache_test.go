package ingest

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/rs/zerolog"
)

func TestSchemaCacheIdentityIssue750(t *testing.T) {
	newWriter := func() *ArrowWriter {
		return NewArrowWriter(
			&config.IngestConfig{Compression: "snappy"},
			zerolog.Nop(),
		)
	}

	t.Run("decimal precision changes schema", func(t *testing.T) {
		writer := newWriter()
		columns := map[string]interface{}{
			"price": []decimal128.Num{},
		}

		first, err := writer.getSchema(
			"trades", columns, nil, false,
			map[string]config.DecimalSpec{
				"price": {Precision: 18, Scale: 8},
			},
		)
		if err != nil {
			t.Fatal(err)
		}

		second, err := writer.getSchema(
			"trades", columns, nil, false,
			map[string]config.DecimalSpec{
				"price": {Precision: 20, Scale: 8},
			},
		)
		if err != nil {
			t.Fatal(err)
		}

		firstType, ok := first.Field(0).Type.(*arrow.Decimal128Type)
		if !ok {
			t.Fatalf("first type = %T", first.Field(0).Type)
		}
		secondType, ok := second.Field(0).Type.(*arrow.Decimal128Type)
		if !ok {
			t.Fatalf("second type = %T", second.Field(0).Type)
		}

		if firstType.Precision != 18 || secondType.Precision != 20 {
			t.Fatalf(
				"decimal precisions = %d, %d; want 18, 20",
				firstType.Precision, secondType.Precision,
			)
		}

		metadata := second.Metadata()
		idx := metadata.FindKey("arc:decimals")
		if idx < 0 || metadata.Values()[idx] != "price:20,8" {
			t.Fatalf("stale decimal metadata: %+v", metadata)
		}
	})

	t.Run("missing decimal spec differs from explicit spec", func(t *testing.T) {
		writer := newWriter()
		columns := map[string]interface{}{
			"price": []decimal128.Num{},
		}

		_, err := writer.getSchema(
			"trades", columns, nil, false, nil,
		)
		if err != nil {
			t.Fatal(err)
		}

		schema, err := writer.getSchema(
			"trades", columns, nil, false,
			map[string]config.DecimalSpec{
				"price": {Precision: 18, Scale: 8},
			},
		)
		if err != nil {
			t.Fatal(err)
		}

		typ, ok := schema.Field(0).Type.(*arrow.Decimal128Type)
		if !ok || typ.Precision != 18 || typ.Scale != 8 {
			t.Fatalf("explicit decimal spec ignored: %v", schema.Field(0).Type)
		}
	})

	t.Run("reordered tags reuse equivalent schema", func(t *testing.T) {
		writer := newWriter()
		columns := map[string]interface{}{
			"host":   []string{},
			"region": []string{},
		}

		first, err := writer.getSchema(
			"cpu", columns, []string{"host", "region"}, false, nil,
		)
		if err != nil {
			t.Fatal(err)
		}

		second, err := writer.getSchema(
			"cpu", columns, []string{"region", "host"}, false, nil,
		)
		if err != nil {
			t.Fatal(err)
		}

		if first != second {
			t.Fatal("equivalent tag sets produced different cached schemas")
		}
	})

	t.Run("column names containing spaces stay distinct", func(t *testing.T) {
		firstColumns := map[string]interface{}{
			"a b": []int64{},
			"c":   []int64{},
		}
		secondColumns := map[string]interface{}{
			"a":   []int64{},
			"b c": []int64{},
		}

		// The old cache key also depends on random map iteration order.
		// Repeated fresh caches make the ambiguous ordering observable.
		for attempt := 0; attempt < 128; attempt++ {
			writer := newWriter()

			_, err := writer.getSchema(
				"cpu", firstColumns, nil, false, nil,
			)
			if err != nil {
				t.Fatal(err)
			}

			schema, err := writer.getSchema(
				"cpu", secondColumns, nil, false, nil,
			)
			if err != nil {
				t.Fatal(err)
			}

			if len(schema.FieldIndices("a")) != 1 ||
				len(schema.FieldIndices("b c")) != 1 ||
				len(schema.FieldIndices("a b")) != 0 {
				t.Fatalf(
					"schema cache collision on attempt %d: %v",
					attempt, schema.Fields(),
				)
			}
		}
	})
}
