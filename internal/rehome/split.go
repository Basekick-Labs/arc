// Package rehome moves the rows of a misfiled file to the directories they
// belong to (#1825).
//
// A file is misfiled when it holds a row whose time is outside the hour (a
// file in an hour directory) or the UTC day (a file in a day directory) of the
// directory it sits in. Arc's time-filtered queries derive the directories to
// read from the time range, so they do not read such a row; a file whose rows
// span more than one UTC day cannot be registered in its Iceberg table; and
// compaction leaves the file alone (internal/compaction/misfiled.go).
//
// This package holds the split: reading a file's rows into one file per
// destination and checking the result. What it produces is not published
// here. The design, and the order in which a swap is made safe on a single
// node, on a cluster and for a cold file, is in the plannings repository
// (Arc/Arc_design_rehome_misfiled_files_plan.md).
package rehome

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/basekick-labs/arc/internal/compaction"
	sqlutil "github.com/basekick-labs/arc/internal/sql"
	"github.com/basekick-labs/arc/internal/storage"
)

// Shape is the kind of directory a file sits in, which decides the shape of
// the files its rows are split into.
type Shape string

const (
	// ShapeHour: {database}/{measurement}/YYYY/MM/DD/HH/file.parquet. Its
	// rows are split one file per UTC hour.
	ShapeHour Shape = "hour"
	// ShapeDay: {database}/{measurement}/YYYY/MM/DD/file.parquet. Its rows
	// are split one file per UTC day.
	ShapeDay Shape = "day"
)

// Source is a data file's storage key, taken apart.
type Source struct {
	Key         string
	Database    string
	Measurement string
	Shape       Shape
	// Partition is the start of the hour or day of the directory.
	Partition time.Time
	// Dir is the directory, without a trailing slash.
	Dir string
	// File is the file name.
	File string
}

// ErrRefused is a request the job does not take, with the reason as its text.
type ErrRefused struct{ Reason string }

func (e *ErrRefused) Error() string { return e.Reason }

func refuse(format string, args ...any) error {
	return &ErrRefused{Reason: fmt.Sprintf(format, args...)}
}

// Refusef is a refusal with a formatted reason.
func Refusef(format string, args ...any) error { return refuse(format, args...) }

// IsRefused reports whether err is a refusal, and its reason.
func IsRefused(err error) (string, bool) {
	var r *ErrRefused
	if errors.As(err, &r) {
		return r.Reason, true
	}
	return "", false
}

// ParseKey takes a data file's storage key apart. It accepts exactly the two
// shapes Arc writes for a database and a measurement,
// {database}/{measurement}/YYYY/MM/DD/HH/file.parquet and
// {database}/{measurement}/YYYY/MM/DD/file.parquet, and refuses everything
// else: a reserved root, a key that is not a storage key, a spoke-namespace
// key of an edge-sync hub (one level deeper, and not supported yet), a
// directory that is not a calendar hour or day.
func ParseKey(key string) (Source, error) {
	if err := storage.ValidateKey(key); err != nil {
		return Source{}, refuse("%q is not a storage key: %v", key, err)
	}
	parts := strings.Split(key, "/")
	for _, p := range parts {
		if p == "" || p == "." || p == ".." {
			return Source{}, refuse("%q is not a storage key", key)
		}
	}
	if storage.IsReservedRootDir(parts[0]) {
		return Source{}, refuse("%q is not a data file: %q is a reserved directory", key, parts[0])
	}
	if !strings.HasSuffix(parts[len(parts)-1], ".parquet") {
		return Source{}, refuse("%q is not a Parquet file", key)
	}
	var shape Shape
	switch len(parts) {
	case 7:
		shape = ShapeHour
	case 6:
		shape = ShapeDay
	default:
		return Source{}, refuse("%q is not {database}/{measurement}/YYYY/MM/DD[/HH]/file.parquet (files of an edge-sync spoke namespace are not supported)", key)
	}
	num := func(s string, width, lo, hi int) (int, bool) {
		if len(s) != width {
			return 0, false
		}
		n, err := strconv.Atoi(s)
		return n, err == nil && n >= lo && n <= hi
	}
	y, okY := num(parts[2], 4, 1, 9999)
	mo, okM := num(parts[3], 2, 1, 12)
	d, okD := num(parts[4], 2, 1, 31)
	h, okH := 0, true
	if shape == ShapeHour {
		h, okH = num(parts[5], 2, 0, 23)
	}
	if !okY || !okM || !okD || !okH {
		return Source{}, refuse("%q is not in a year/month/day[/hour] directory", key)
	}
	t := time.Date(y, time.Month(mo), d, h, 0, 0, 0, time.UTC)
	if t.Day() != d || int(t.Month()) != mo {
		return Source{}, refuse("%q is in a directory that is not a calendar day", key)
	}
	return Source{
		Key: key, Database: parts[0], Measurement: parts[1], Shape: shape, Partition: t,
		Dir: strings.Join(parts[:len(parts)-1], "/"), File: parts[len(parts)-1],
	}, nil
}

// dirFor is the directory a row at the start of a destination belongs to.
func (s Source) dirFor(start time.Time) string {
	start = start.UTC()
	dir := fmt.Sprintf("%s/%s/%04d/%02d/%02d", s.Database, s.Measurement, start.Year(), int(start.Month()), start.Day())
	if s.Shape == ShapeHour {
		dir += fmt.Sprintf("/%02d", start.Hour())
	}
	return dir
}

func (s Source) span() time.Duration {
	if s.Shape == ShapeHour {
		return time.Hour
	}
	return 24 * time.Hour
}

func (s Source) tier() string {
	if s.Shape == ShapeHour {
		return compaction.TierHourly
	}
	return compaction.TierDaily
}

// Destination is one hour or day that holds rows of the source.
type Destination struct {
	// Dir is the directory those rows belong to.
	Dir string `json:"dir"`
	// Start is the start of the hour or day.
	Start time.Time `json:"start"`
	Rows  int64     `json:"rows"`
	// Own: Dir is the directory the source already sits in.
	Own bool `json:"own,omitempty"`
}

// Plan is what a source file holds and where its rows belong.
type Plan struct {
	Source   Source
	Rows     int64
	TimeType string
	// Destinations, by Start.
	Destinations []Destination
	// RemainderRows have no destination: their time is NULL or infinite.
	// A split keeps them in one file in the source's own directory.
	RemainderRows int64
	// kv is the source's Parquet key/value metadata a split carries over.
	kv [][2]string
}

// Misfiled reports whether any row belongs to another directory.
func (p Plan) Misfiled() bool {
	for _, d := range p.Destinations {
		if !d.Own {
			return true
		}
	}
	return false
}

// MisfiledRows is how many rows belong to another directory.
func (p Plan) MisfiledRows() int64 {
	var n int64
	for _, d := range p.Destinations {
		if !d.Own {
			n += d.Rows
		}
	}
	return n
}

// carriedKV are the Parquet key/value metadata entries a split carries into
// every output, as a DELETE rewrite does (api.rewriteKVKeys): the dedup
// columns, the exact-dedup marker, the decimal specs and the legacy dedup
// marker. Never ARROW:schema, which describes the writer's Arrow schema and
// not the file DuckDB writes.
var carriedKV = map[string]bool{
	"arc:tags":        true,
	"arc:dedup_exact": true,
	"arc:decimals":    true,
	"arc:dedup_time":  true,
}

// timeTypes are the "time" column types a split takes: microsecond
// timestamps, which every range comparison and every output keeps exactly.
// Anything else is refused. Compaction's own normalisation would let them
// through, and change them: it drops the nanoseconds of a TIMESTAMP_NS, reads
// an integer of seconds as microseconds, and turns a string it cannot parse
// into NULL. A repair must not change a value it was not asked to change.
// A Parquet timestamp in milliseconds or seconds reads as a TIMESTAMP with
// the same values, and is taken; its outputs store microseconds.
var timeTypes = map[string]bool{
	"TIMESTAMP":                true,
	"TIMESTAMP WITH TIME ZONE": true,
}

// read is the read_parquet of one local file, as every statement here reads
// it: no hive partitioning, so a directory named "time=..." in the path is
// not taken for the column.
func read(localPath string) string {
	return sqlutil.ReadParquet(sqlutil.QuoteStringLiteral(localPath))
}

// MakePlan reads the file at localPath, which is the source's content, and
// reports where its rows belong. db must read times in UTC on every
// connection (database.OpenRewriteDB does).
func MakePlan(ctx context.Context, db *sql.DB, src Source, localPath string) (Plan, error) {
	if err := storage.ValidateGlobSafe(localPath); err != nil {
		return Plan{}, fmt.Errorf("the local path of %s cannot be read as one file: %w", src.Key, err)
	}
	p := Plan{Source: src}
	renamedTime := false
	var zone string
	if err := db.QueryRowContext(ctx, `SELECT current_setting('TimeZone')`).Scan(&zone); err != nil {
		return Plan{}, fmt.Errorf("read the session time zone: %w", err)
	}
	if zone != "UTC" {
		return Plan{}, fmt.Errorf("the split must read times in UTC; the session's time zone is %q", zone)
	}

	rows, err := db.QueryContext(ctx, fmt.Sprintf(`SELECT column_name, column_type FROM (DESCRIBE SELECT * FROM %s)`, read(localPath)))
	if err != nil {
		return Plan{}, fmt.Errorf("read the schema of %s: %w", src.Key, err)
	}
	for rows.Next() {
		var name, typ string
		if err := rows.Scan(&name, &typ); err != nil {
			rows.Close()
			return Plan{}, fmt.Errorf("read the schema of %s: %w", src.Key, err)
		}
		// DuckDB binds "time" to a column of any case.
		if strings.EqualFold(name, "time") {
			p.TimeType = typ
		}
		if renamedTimeColumn(name) {
			renamedTime = true
		}
	}
	if err := rows.Close(); err != nil {
		return Plan{}, fmt.Errorf("read the schema of %s: %w", src.Key, err)
	}
	if err := rows.Err(); err != nil {
		return Plan{}, fmt.Errorf("read the schema of %s: %w", src.Key, err)
	}
	if p.TimeType == "" {
		return Plan{}, refuse("%s has no \"time\" column", src.Key)
	}
	// Two columns named "time" in different cases: DuckDB reads the second
	// under a new name ("Time_1"), and a split would write that name.
	var sameName int
	if err := db.QueryRowContext(ctx, fmt.Sprintf(`SELECT count(*) FROM parquet_schema(%s) WHERE lower(name) = 'time'`, sqlutil.QuoteStringLiteral(localPath))).Scan(&sameName); err != nil {
		return Plan{}, fmt.Errorf("read the schema of %s: %w", src.Key, err)
	}
	if sameName > 1 && renamedTime {
		return Plan{}, refuse("%s has more than one column named \"time\" (in different cases); a split would rename one of them", src.Key)
	}
	if !timeTypes[p.TimeType] {
		return Plan{}, refuse("%s stores \"time\" as %s; only a microsecond TIMESTAMP or TIMESTAMP WITH TIME ZONE can be split without changing its values", src.Key, p.TimeType)
	}

	unit := "hour"
	if src.Shape == ShapeDay {
		unit = "day"
	}
	// A NULL or infinite time has no epoch, and so no destination.
	q := fmt.Sprintf(`SELECT epoch_us(date_trunc('%s', "time")) AS b, count(*) FROM %s GROUP BY 1 ORDER BY 1 NULLS FIRST`, unit, read(localPath))
	grouped, err := db.QueryContext(ctx, q)
	if err != nil {
		return Plan{}, fmt.Errorf("read the times of %s: %w", src.Key, err)
	}
	defer grouped.Close()
	for grouped.Next() {
		var bucket sql.NullInt64
		var n int64
		if err := grouped.Scan(&bucket, &n); err != nil {
			return Plan{}, fmt.Errorf("read the times of %s: %w", src.Key, err)
		}
		p.Rows += n
		if !bucket.Valid {
			p.RemainderRows += n
			continue
		}
		start := time.UnixMicro(bucket.Int64).UTC()
		// A directory is a four-digit year, and so is a time in a report.
		// The first hour of year 1 is Go's zero time, which the partition
		// window does not take. Such a time is almost always a wrong unit
		// (nanoseconds stored as microseconds) or an unset value.
		if y := start.Year(); y < 1 || y > 9999 || start.IsZero() {
			return Plan{}, refuse("%s holds %d row(s) at a time outside years 0001 to 9999 or at the very start of year 1 (first at %d µs since 1970); there is no directory for them", src.Key, n, bucket.Int64)
		}
		dir := src.dirFor(start)
		p.Destinations = append(p.Destinations, Destination{Dir: dir, Start: start, Rows: n, Own: dir == src.Dir})
	}
	if err := grouped.Err(); err != nil {
		return Plan{}, fmt.Errorf("read the times of %s: %w", src.Key, err)
	}

	kv, err := readKV(ctx, db, localPath)
	if err != nil {
		return Plan{}, fmt.Errorf("read the Parquet metadata of %s: %w", src.Key, err)
	}
	p.kv = kv
	return p, nil
}

// renamedTimeColumn reports a name DuckDB gives a second column called
// "time": "Time_1", "time_2".
func renamedTimeColumn(name string) bool {
	rest, ok := strings.CutPrefix(strings.ToLower(name), "time_")
	if !ok || rest == "" {
		return false
	}
	for _, r := range rest {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

// readKV returns the file's carried key/value metadata, by key. Unlike a
// DELETE rewrite, a failure is an error: an output without arc:tags is
// deduplicated differently by a later compaction than the source would be.
func readKV(ctx context.Context, db *sql.DB, localPath string) ([][2]string, error) {
	rows, err := db.QueryContext(ctx, fmt.Sprintf(`SELECT key, value FROM parquet_kv_metadata(%s)`, sqlutil.QuoteStringLiteral(localPath)))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var kv [][2]string
	seen := map[string]bool{}
	for rows.Next() {
		// Scanned raw: a cast to VARCHAR would turn a quote in a tag name
		// into an escape.
		var k, v []byte
		if err := rows.Scan(&k, &v); err != nil {
			return nil, err
		}
		if !carriedKV[string(k)] || seen[string(k)] {
			continue
		}
		if !utf8.Valid(v) {
			return nil, fmt.Errorf("the value of %s is not valid UTF-8", k)
		}
		seen[string(k)] = true
		kv = append(kv, [2]string{string(k), string(v)})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	sort.Slice(kv, func(i, j int) bool { return kv[i][0] < kv[j][0] })
	return kv, nil
}

func kvOption(kv [][2]string) string {
	if len(kv) == 0 {
		return ""
	}
	parts := make([]string, len(kv))
	for i, e := range kv {
		parts[i] = sqlutil.QuoteStringLiteral(e[0]) + ": " + sqlutil.QuoteStringLiteral(e[1])
	}
	return ", KV_METADATA {" + strings.Join(parts, ", ") + "}"
}

// Output is one file a split wrote.
type Output struct {
	// Dir is the directory the file belongs in; LocalPath is where the split
	// wrote it.
	Dir       string    `json:"dir"`
	Start     time.Time `json:"start"`
	Rows      int64     `json:"rows"`
	Bytes     int64     `json:"bytes"`
	MinTime   time.Time `json:"min_time"`
	MaxTime   time.Time `json:"max_time"`
	LocalPath string    `json:"-"`
	// Remainder: the rows with no destination; Dir is the source's own.
	Remainder bool `json:"remainder,omitempty"`
}

// rowGroupRows is the row group size of an output, as compaction and a
// DELETE rewrite write.
const rowGroupRows = 122880

// timeLiteral is a microsecond epoch as a literal of the column's type.
func timeLiteral(timeType string, t time.Time) string {
	if timeType == "TIMESTAMP" {
		return fmt.Sprintf("make_timestamp(%d)", t.UnixMicro())
	}
	return fmt.Sprintf("make_timestamptz(%d)", t.UnixMicro())
}

// MaxSplitDestinations bounds the files one split writes. Each destination
// is one pass over the source, so the work is destinations times the file.
const MaxSplitDestinations = 500

// Split writes the plan's rows into dir, one file per destination and one for
// the remainder, in the source's row order, and checks the result (Check). It
// publishes nothing. The caller removes dir.
func Split(ctx context.Context, db *sql.DB, p Plan, localPath, dir string) ([]Output, error) {
	if err := storage.ValidateGlobSafe(dir); err != nil {
		return nil, fmt.Errorf("the split's directory cannot be written to: %w", err)
	}
	if len(p.Destinations) > MaxSplitDestinations {
		return nil, refuse("%s holds rows of %d hours or days; a split writes at most %d files", p.Source.Key, len(p.Destinations), MaxSplitDestinations)
	}
	var outs []Output
	write := func(i int, where string) (string, error) {
		out := filepath.Join(dir, fmt.Sprintf("out-%06d.parquet", i))
		q := fmt.Sprintf(`COPY (SELECT * FROM %s WHERE %s) TO %s (FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL 3, ROW_GROUP_SIZE %d%s)`,
			read(localPath), where, sqlutil.QuoteStringLiteral(out), rowGroupRows, kvOption(p.kv))
		if _, err := db.ExecContext(ctx, q); err != nil {
			return "", err
		}
		return out, nil
	}
	for i, d := range p.Destinations {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		end := d.Start.Add(p.Source.span())
		where := fmt.Sprintf(`"time" >= %s AND "time" < %s`, timeLiteral(p.TimeType, d.Start), timeLiteral(p.TimeType, end))
		out, err := write(i, where)
		if err != nil {
			return nil, fmt.Errorf("write the rows of %s: %w", d.Dir, err)
		}
		outs = append(outs, Output{Dir: d.Dir, Start: d.Start, LocalPath: out})
	}
	if p.RemainderRows > 0 {
		out, err := write(len(p.Destinations), `epoch_us("time") IS NULL`)
		if err != nil {
			return nil, fmt.Errorf("write the rows with no destination: %w", err)
		}
		outs = append(outs, Output{Dir: p.Source.Dir, Start: p.Source.Partition, LocalPath: out, Remainder: true})
	}
	if err := Check(ctx, db, p, outs); err != nil {
		return nil, err
	}
	return outs, nil
}

// Check verifies a split before anything is published, and fills in each
// output's rows, size and time range:
//   - every output holds the rows the plan counted for it, so together they
//     hold every row of the source, once;
//   - every output's times are inside the hour or day of its directory, by
//     the same read compaction's input check uses, so no output would be
//     refused as misfiled where it lands;
//   - the remainder holds no time at all.
func Check(ctx context.Context, db *sql.DB, p Plan, outs []Output) error {
	want := len(p.Destinations)
	if p.RemainderRows > 0 {
		want++
	}
	if len(outs) != want {
		return fmt.Errorf("the split wrote %d files for %d destinations", len(outs), want)
	}
	var total int64
	for i := range outs {
		o := &outs[i]
		if err := db.QueryRowContext(ctx, fmt.Sprintf(`SELECT num_rows FROM parquet_file_metadata(%s)`, sqlutil.QuoteStringLiteral(o.LocalPath))).Scan(&o.Rows); err != nil {
			return fmt.Errorf("count the rows written for %s: %w", o.Dir, err)
		}
		st, err := os.Stat(o.LocalPath)
		if err != nil {
			return fmt.Errorf("stat the file written for %s: %w", o.Dir, err)
		}
		o.Bytes = st.Size()
		total += o.Rows
		lo, hi, found, err := compaction.FileTimeRange(ctx, db, o.LocalPath)
		if err != nil {
			return fmt.Errorf("read the times written for %s: %w", o.Dir, err)
		}
		if o.Remainder {
			if o.Rows != p.RemainderRows {
				return fmt.Errorf("the file of rows with no destination holds %d rows, the source has %d", o.Rows, p.RemainderRows)
			}
			if found {
				return fmt.Errorf("the file of rows with no destination holds times (%s to %s)", lo.Format(time.RFC3339Nano), hi.Format(time.RFC3339Nano))
			}
			continue
		}
		if o.Rows != p.Destinations[i].Rows {
			return fmt.Errorf("the file for %s holds %d rows, the source has %d there", o.Dir, o.Rows, p.Destinations[i].Rows)
		}
		start, end, ok := compaction.PartitionWindow(p.Source.tier(), o.Start)
		if !ok || !found || lo.Before(start) || !hi.Before(end) {
			return fmt.Errorf("the file for %s holds times %s to %s, outside its directory", o.Dir, lo.Format(time.RFC3339Nano), hi.Format(time.RFC3339Nano))
		}
		o.MinTime, o.MaxTime = lo, hi
	}
	if total != p.Rows {
		return fmt.Errorf("the split wrote %d rows, the source has %d", total, p.Rows)
	}
	return nil
}
