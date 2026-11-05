package iceberg

import (
	"context"
	"fmt"
	"path"
	"strings"
	"sync"

	iceberg "github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	icetable "github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/utils"
	"github.com/aws/aws-sdk-go-v2/aws"

	"github.com/basekick-labs/arc/internal/storage"
)

// The exporter's side of the S3 cold tier (#939 B2; the source side is cold.go).
//
// A file migrated to cold keeps its key and its bytes, so its DataFile is the hot one with the
// cold URI: cloneDataFile builds it from the table's own entry and the cold object is never read.
// Only a cold file the table never held hot (cold data that predates the export) has its footer
// read, through a FileIO of its own, with the cold tier's credentials: the table's FileIO is bound
// to the warehouse's bucket (or to local disk), and iceberg-go takes one aws.Config per context.

type coldExport struct {
	root  string // cold tier root URI, e.g. s3://cold-bucket/prefix
	cfg   *aws.Config
	props iceberg.Properties

	once sync.Once
	io   iceio.IO
	err  error
}

// SetColdTier tells the exporter where the cold tier's files live, so their DataFiles can be
// built (#939 B2). The cold backend must be S3; config refuses any other with Iceberg.
func (e *Exporter) SetColdTier(cold storage.Backend) error {
	cfg, props := awsConfigOf(cold)
	if cfg == nil {
		return fmt.Errorf("the Iceberg export tracks an S3 cold tier only (got %s)", cold.Type())
	}
	e.cold = &coldExport{root: strings.TrimSuffix(DefaultWarehouse(cold), "/"), cfg: cfg, props: props}
	return nil
}

// fileIOFor returns the FileIO that can open uri: the cold tier's for a cold URI, else the
// table's.
func (e *Exporter) fileIOFor(ctx context.Context, tbl *icetable.Table, uri string) (iceio.IO, error) {
	if c := e.cold; c != nil && strings.HasPrefix(uri, c.root+"/") {
		c.once.Do(func() {
			c.io, c.err = iceio.LoadFS(utils.WithAwsConfig(ctx, c.cfg), c.props, c.root)
		})
		if c.err != nil {
			return nil, fmt.Errorf("open the cold tier's filesystem: %w", c.err)
		}
		return c.io, nil
	}
	return tbl.FS(ctx)
}

// cloneFromTable builds, for each added path, the DataFile of a live entry holding the same file
// under another URI — the same storage key and the same size: a hot file now registered under its
// cold URI, or the reverse. Those adds never open their file. keyOf maps an added URI to its
// storage key. Returns the clones and the paths still to build.
func cloneFromTable(tbl *icetable.Table, live map[string]iceberg.DataFile, toAdd []string, keyOf map[string]string, want map[string]int64) (cloned []iceberg.DataFile, rest []string, err error) {
	byBase := make(map[string][]iceberg.DataFile, len(live))
	for p, df := range live {
		byBase[path.Base(p)] = append(byBase[path.Base(p)], df)
	}
	for _, p := range toAdd {
		key := keyOf[p]
		var src iceberg.DataFile
		if key != "" {
			for _, df := range byBase[path.Base(key)] {
				if df.FilePath() != p && strings.HasSuffix(df.FilePath(), "/"+key) && df.FileSizeBytes() == want[p] {
					src = df
					break
				}
			}
		}
		if src == nil {
			rest = append(rest, p)
			continue
		}
		df, err := cloneDataFile(tbl, src, p)
		if err != nil {
			return nil, nil, err
		}
		cloned = append(cloned, df)
	}
	return cloned, rest, nil
}

// cloneDataFile is src under newPath: the same record count, size, partition and column stats,
// built as iceberg-go builds a DataFile from a footer.
func cloneDataFile(tbl *icetable.Table, src iceberg.DataFile, newPath string) (iceberg.DataFile, error) {
	spec := tbl.Spec()
	if int(src.SpecID()) != spec.ID() {
		return nil, fmt.Errorf("clone %s: written under partition spec %d, the table's is %d", src.FilePath(), src.SpecID(), spec.ID())
	}
	b, err := iceberg.NewDataFileBuilder(spec, src.ContentType(), newPath, src.FileFormat(),
		src.Partition(), partitionLogicalTypes(tbl), nil, src.Count(), src.FileSizeBytes())
	if err != nil {
		return nil, fmt.Errorf("clone %s: %w", src.FilePath(), err)
	}
	b = b.ColumnSizes(src.ColumnSizes()).ValueCounts(src.ValueCounts()).NullValueCounts(src.NullValueCounts()).
		NaNValueCounts(src.NaNValueCounts()).LowerBoundValues(src.LowerBoundValues()).UpperBoundValues(src.UpperBoundValues()).
		SplitOffsets(src.SplitOffsets())
	if id := src.SortOrderID(); id != nil {
		b = b.SortOrderID(*id)
	}
	return b.Build(), nil
}

// partitionLogicalTypes maps each partition field to the Avro logical type of its value, as
// iceberg-go does when it builds a DataFile from a footer (table/internal/utils.go): day(time) is
// a "date".
func partitionLogicalTypes(tbl *icetable.Table) map[int]string {
	out := map[int]string{}
	sc := tbl.Schema()
	spec := tbl.Spec()
	for _, field := range spec.Fields() {
		src, ok := sc.FindFieldByID(field.SourceID())
		if !ok {
			continue
		}
		switch field.Transform.ResultType(src.Type).(type) {
		case iceberg.DateType:
			out[field.FieldID] = "date"
		case iceberg.TimeType:
			out[field.FieldID] = "time-micros"
		case iceberg.TimestampType, iceberg.TimestampTzType:
			out[field.FieldID] = "timestamp-micros"
		case iceberg.DecimalType:
			out[field.FieldID] = "decimal"
		case iceberg.UUIDType:
			out[field.FieldID] = "uuid"
		}
	}
	return out
}
