package iceberg

import (
	"fmt"
	"os"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	iceberg "github.com/apache/iceberg-go"
	"golang.org/x/sync/errgroup"
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

// unionSchemaConcurrency bounds parallel footer reads in UnionSchema. Small enough to avoid
// file-descriptor exhaustion on a measurement with many files, large enough to hide per-file
// open+footer latency.
const unionSchemaConcurrency = 8

// UnionSchema derives the union of column schemas across multiple local Parquet files.
// Arc's ingest is schema-flexible: files for the same measurement can have different column
// sets over time (a metric added later). The Iceberg table must carry the SUPERSET so both
// narrow and wide files pass AddFiles (a column absent from a file reads as NULL, since all
// fields are optional). Column order follows first-seen; a name seen again must have a
// matching type (a genuine type conflict is returned as an error rather than silently picked).
//
// Footers are read in parallel (bounded) because on first-sight/full-derivation this reads EVERY
// local footer, which is sequential I/O that can exceed the reconciler's measurementTimeout on a
// measurement with thousands of files. The merge stays deterministic: results are collected by
// index and folded in localPaths order, so column ordering and conflict detection are unaffected
// by scheduling. (The incremental path in the scheduler only passes newly-added files here.)
func UnionSchema(localPaths []string) (ArcSchema, error) {
	if len(localPaths) == 0 {
		return ArcSchema{}, nil
	}
	if len(localPaths) == 1 {
		return SchemaFromParquet(localPaths[0])
	}

	schemas := make([]ArcSchema, len(localPaths))
	errs := make([]error, len(localPaths))
	g := new(errgroup.Group)
	g.SetLimit(unionSchemaConcurrency)
	for i, p := range localPaths {
		i, p := i, p
		g.Go(func() error {
			schemas[i], errs[i] = SchemaFromParquet(p)
			return nil // collect all errors; fold below in deterministic order
		})
	}
	_ = g.Wait()

	seen := make(map[string]iceberg.Type)
	var order []string
	for i, p := range localPaths {
		if errs[i] != nil {
			return ArcSchema{}, fmt.Errorf("file %q: %w", p, errs[i])
		}
		for _, f := range schemas[i].Fields {
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

// MergeSchemas unions two already-derived ArcSchemas, preserving `base`'s column order and
// appending any names that appear only in `add`. A name present in both must carry the same
// type — a genuine conflict is an error (matching UnionSchema's semantics). This lets the
// reconciler extend a cached schema with only the newly-added files' schemas, avoiding an O(N)
// re-read of every footer when a measurement's file set grows.
func MergeSchemas(base, add ArcSchema) (ArcSchema, error) {
	seen := make(map[string]iceberg.Type, len(base.Fields)+len(add.Fields))
	order := make([]string, 0, len(base.Fields)+len(add.Fields))
	for _, f := range base.Fields {
		seen[f.Name] = f.Type
		order = append(order, f.Name)
	}
	for _, f := range add.Fields {
		if prev, ok := seen[f.Name]; ok {
			if prev.String() != f.Type.String() {
				return ArcSchema{}, fmt.Errorf("column %q has conflicting types across files: %s vs %s", f.Name, prev, f.Type)
			}
			continue
		}
		seen[f.Name] = f.Type
		order = append(order, f.Name)
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
