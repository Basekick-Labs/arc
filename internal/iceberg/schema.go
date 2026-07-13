package iceberg

import (
	"fmt"
	"os"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	iceberg "github.com/apache/iceberg-go"
)

// SchemaFromParquet derives an ArcSchema (typed Iceberg columns) by reading a Parquet file's
// footer — pure Go, no DuckDB handle. Used by the reconciler to build/evolve the Iceberg
// table for a measurement. localPath must be a readable local file (Arc's hot tier is local;
// vortex/cold reads are out of scope — the reconciler samples a local hot file per measurement).
//
// Type mapping (Arc/Arrow physical -> Iceberg):
//
//	INT64                         -> long
//	TIMESTAMP (tz set, UTC-adj)   -> timestamptz   (Arc's `time`; MUST be tz — see eval doc)
//	TIMESTAMP (no tz)             -> timestamp
//	FLOAT64                       -> double
//	STRING                        -> string
//	BOOL                          -> boolean
//	DECIMAL128(p,s)               -> decimal(p,s)
func SchemaFromParquet(localPath string) (ArcSchema, error) {
	f, err := os.Open(localPath)
	if err != nil {
		return ArcSchema{}, fmt.Errorf("open parquet %q: %w", localPath, err)
	}
	defer f.Close()

	rdr, err := file.NewParquetReader(f)
	if err != nil {
		return ArcSchema{}, fmt.Errorf("parquet reader %q: %w", localPath, err)
	}
	defer rdr.Close()

	fr, err := pqarrow.NewFileReader(rdr, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	if err != nil {
		return ArcSchema{}, fmt.Errorf("pqarrow reader %q: %w", localPath, err)
	}
	arrowSchema, err := fr.Schema()
	if err != nil {
		return ArcSchema{}, fmt.Errorf("arrow schema %q: %w", localPath, err)
	}

	fields := make([]ArcField, 0, len(arrowSchema.Fields()))
	for _, af := range arrowSchema.Fields() {
		it, err := arrowToIceberg(af.Type)
		if err != nil {
			return ArcSchema{}, fmt.Errorf("column %q: %w", af.Name, err)
		}
		fields = append(fields, ArcField{Name: af.Name, Type: it})
	}
	return ArcSchema{Fields: fields}, nil
}

// UnionSchema derives the union of column schemas across multiple local Parquet files.
// Arc's ingest is schema-flexible: files for the same measurement can have different column
// sets over time (a metric added later). The Iceberg table must carry the SUPERSET so both
// narrow and wide files pass AddFiles (a column absent from a file reads as NULL, since all
// fields are optional). Column order follows first-seen; a name seen again must have a
// matching type (a genuine type conflict is returned as an error rather than silently picked).
func UnionSchema(localPaths []string) (ArcSchema, error) {
	seen := make(map[string]iceberg.Type)
	var order []string
	for _, p := range localPaths {
		sc, err := SchemaFromParquet(p)
		if err != nil {
			return ArcSchema{}, err
		}
		for _, f := range sc.Fields {
			if prev, ok := seen[f.Name]; ok {
				if prev.String() != f.Type.String() {
					return ArcSchema{}, fmt.Errorf("column %q has conflicting types across files: %s vs %s", f.Name, prev, f.Type)
				}
				continue
			}
			seen[f.Name] = f.Type
			order = append(order, f.Name)
		}
	}
	fields := make([]ArcField, len(order))
	for i, name := range order {
		fields[i] = ArcField{Name: name, Type: seen[name]}
	}
	return ArcSchema{Fields: fields}, nil
}

// arrowToIceberg maps one Arrow type to its Iceberg type per the table above.
func arrowToIceberg(t arrow.DataType) (iceberg.Type, error) {
	switch dt := t.(type) {
	case *arrow.Int64Type:
		return iceberg.Int64Type{}, nil
	case *arrow.Float64Type:
		return iceberg.Float64Type{}, nil
	case *arrow.StringType:
		return iceberg.StringType{}, nil
	case *arrow.BooleanType:
		return iceberg.BooleanType{}, nil
	case *arrow.TimestampType:
		// A non-empty TimeZone means the column is UTC-adjusted (Arc writes Timestamp_us with
		// UTC), which Iceberg represents as timestamptz. Declaring plain `timestamp` for such a
		// column makes AddFiles fail with a schema mismatch (verified in Phase 0b).
		if dt.TimeZone != "" {
			return iceberg.TimestampTzType{}, nil
		}
		return iceberg.TimestampType{}, nil
	case *arrow.Decimal128Type:
		return iceberg.DecimalTypeOf(int(dt.Precision), int(dt.Scale)), nil
	default:
		return nil, fmt.Errorf("unsupported Arrow type for Iceberg export: %s", t)
	}
}
