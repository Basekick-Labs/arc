package iceberg

import (
	"context"
	"fmt"

	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	icetable "github.com/apache/iceberg-go/table"
)

// Why a file is left out of its table. A table is partitioned by day(time), and a file is
// registered under one day; a file that cannot be given one is not registered (#1754).
const (
	leftOutSpansDays = "its rows span more than one UTC day"
	leftOutNoTime    = "its time column holds no value to take a day from"
)

// leftOutFile is a file a pass did not register, and why.
type leftOutFile struct {
	path   string
	reason string
}

func leftOutPaths(files []leftOutFile) []string {
	out := make([]string, len(files))
	for i, f := range files {
		out[i] = f.path
	}
	return out
}

// leftOutLines is the files with their reasons, for a log line.
func leftOutLines(files []leftOutFile) []string {
	out := make([]string, len(files))
	for i, f := range files {
		out[i] = f.path + " (" + f.reason + ")"
	}
	return out
}

// missingPartitionValue reports that df has no value for a partition field of spec.
// iceberg-go takes the value from the footer's min/max of the source column and returns none,
// without an error, when the footer has no such statistics (a file written with
// ingest.write_statistics = false before #1754, or one from elsewhere). Registered like that,
// the file is under a null day: a reader that filters by time prunes it, and its rows are
// missing from every such query while an unfiltered read returns them.
func missingPartitionValue(spec iceberg.PartitionSpec, df iceberg.DataFile) bool {
	part := df.Partition()
	for _, f := range spec.Fields() {
		if v, ok := part[f.FieldID]; !ok || v == nil {
			return true
		}
	}
	return false
}

// withPartitionFromData is df with its partition value taken from the file's rows instead of
// its footer statistics: the source column is read once, and the value is the transform of its
// smallest and largest value when both give the same one. reason is set, and the file is left
// out, when they give different values or the column holds no value.
//
// The spec this package creates has one field, day(time); a spec of another shape is not
// repaired here and the file is left out.
func withPartitionFromData(ctx context.Context, fio iceio.IO, path string, tbl *icetable.Table, df iceberg.DataFile) (fixed iceberg.DataFile, reason string, err error) {
	spec := tbl.Spec()
	if spec.NumFields() != 1 {
		return nil, leftOutNoTime, nil
	}
	field := spec.Field(0)
	src, ok := tbl.Schema().FindFieldByID(field.SourceID())
	if !ok {
		return nil, "", fmt.Errorf("partition field %s: its source column is not in the table schema", field.Name)
	}
	lo, hi, found, err := columnMicrosRange(ctx, fio, path, src.Name)
	if err != nil {
		return nil, "", err
	}
	if !found {
		return nil, leftOutNoTime, nil
	}
	low, high := partitionOfMicros(field, src.Type, lo), partitionOfMicros(field, src.Type, hi)
	if !low.Valid || !high.Valid {
		return nil, leftOutNoTime, nil
	}
	if !low.Val.Equals(high.Val) {
		return nil, leftOutSpansDays, nil
	}
	b, err := iceberg.NewDataFileBuilder(spec, df.ContentType(), df.FilePath(), df.FileFormat(),
		map[int]any{field.FieldID: low.Val.Any()}, partitionLogicalTypes(tbl), nil, df.Count(), df.FileSizeBytes())
	if err != nil {
		return nil, "", err
	}
	b = b.ColumnSizes(df.ColumnSizes()).ValueCounts(df.ValueCounts()).NullValueCounts(df.NullValueCounts()).
		NaNValueCounts(df.NaNValueCounts()).LowerBoundValues(df.LowerBoundValues()).UpperBoundValues(df.UpperBoundValues()).
		SplitOffsets(df.SplitOffsets())
	if id := df.SortOrderID(); id != nil {
		b = b.SortOrderID(*id)
	}
	return b.Build(), "", nil
}

// partitionOfMicros applies field's transform to a timestamp in microseconds, as iceberg-go
// does to a footer's min and max.
func partitionOfMicros(field iceberg.PartitionField, srcType iceberg.Type, micros int64) iceberg.Optional[iceberg.Literal] {
	var lit iceberg.Literal
	switch srcType.(type) {
	case iceberg.TimestampTzType, iceberg.TimestampType:
		lit = iceberg.NewLiteral(iceberg.Timestamp(micros))
	default:
		return iceberg.Optional[iceberg.Literal]{}
	}
	return field.Transform.Apply(iceberg.Optional[iceberg.Literal]{Valid: true, Val: lit})
}

// columnMicrosRange reads a timestamp column of a Parquet file and returns its smallest and
// largest value in microseconds. found is false when the column is absent, is not an int64
// timestamp, or holds only nulls.
func columnMicrosRange(ctx context.Context, fio iceio.IO, path, column string) (lo, hi int64, found bool, err error) {
	f, err := fio.Open(path)
	if err != nil {
		return 0, 0, false, fmt.Errorf("read the time column of %s: %w", path, err)
	}
	defer f.Close()
	rdr, err := file.NewParquetReader(f)
	if err != nil {
		return 0, 0, false, fmt.Errorf("read the time column of %s: %w", path, err)
	}
	defer rdr.Close()

	sc := rdr.MetaData().Schema
	idx := sc.ColumnIndexByName(column)
	if idx < 0 || sc.Column(idx).PhysicalType() != parquet.Types.Int64 {
		return 0, 0, false, nil
	}
	ts, ok := sc.Column(idx).LogicalType().(schema.TimestampLogicalType)
	if !ok {
		return 0, 0, false, nil
	}
	toMicros := func(v int64) int64 { return v }
	switch ts.TimeUnit() {
	case schema.TimeUnitMillis:
		toMicros = func(v int64) int64 { return v * 1000 }
	case schema.TimeUnitNanos:
		toMicros = func(v int64) int64 { return floorDiv(v, 1000) }
	}

	const batch = 8192
	vals := make([]int64, batch)
	defs := make([]int16, batch)
	for g := 0; g < rdr.NumRowGroups(); g++ {
		if err := ctx.Err(); err != nil {
			return 0, 0, false, err
		}
		col, err := rdr.RowGroup(g).Column(idx)
		if err != nil {
			return 0, 0, false, fmt.Errorf("read the time column of %s: %w", path, err)
		}
		cr, ok := col.(*file.Int64ColumnChunkReader)
		if !ok {
			return 0, 0, false, nil
		}
		for cr.HasNext() {
			_, n, err := cr.ReadBatch(batch, vals, defs, nil)
			if err != nil {
				return 0, 0, false, fmt.Errorf("read the time column of %s: %w", path, err)
			}
			for _, v := range vals[:n] {
				m := toMicros(v)
				if !found {
					lo, hi, found = m, m, true
					continue
				}
				lo, hi = min(lo, m), max(hi, m)
			}
		}
	}
	return lo, hi, found, nil
}

// floorDiv is a/b rounded toward negative infinity, so a time before 1970 falls in the unit
// that holds it.
func floorDiv(a, b int64) int64 {
	q := a / b
	if a%b != 0 && (a < 0) != (b < 0) {
		q--
	}
	return q
}
