package api

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/basekick-labs/arc/internal/pruning"
	"github.com/duckdb/duckdb-go/v2"
)

// A DELETE reads only the files its time range can touch, and counts them in
// bounded chunks. Before, it listed every file of the measurement and counted
// them in one read_parquet over all of them, so its memory grew with the
// table's file count times its width, not with the window: a DELETE of one
// minute on a table of 300k wide files needed terabytes.

// deleteScanChunk is the most files one counting query reads, hot or cold.
// A count over a wide file costs memory for its schema whatever its row count:
// up to ~31 MB a file on the writer at 14k columns (a probe measured >18 MB,
// the writer 1.2-1.7x that), so 64 files is about 2 GB per query there, and a
// count that needs a schema donor (countInChunks) reads two chunks.
var deleteScanChunk = 64 // a var for tests only

// deleteWindowPad widens a DELETE's time range on each bounded side. The
// range is read from the WHERE a moment before DuckDB evaluates it, so a
// now()-relative bound moves in between; an hour covers that for any DELETE
// shorter than an hour, at the cost of reading at most one hour directory
// more on each side. The window is read again on every scan.
const deleteWindowPad = time.Hour

// deleteWindow is the time range a DELETE's WHERE bounds, as the partition
// directories it can touch. A nil window keeps every file.
type deleteWindow struct {
	prefix           string // database/measurement/
	start, end       time.Time
	hasStart, hasEnd bool
}

// windowFor reads the window of where, with now() read as now. nil when
// pruning is off or the WHERE has no range the extractor can prove: a
// top-level OR, a subquery, a time column compared with anything but a
// literal or now() plus or minus one interval, or time = / IN (point deletes
// read every file).
//
// It relies on a file's directory holding only rows of that hour (or day,
// for a daily compaction output). Ingest splits every batch by row hour since
// 26.01.1, compaction writes into the directory it compacted, a DELETE
// rewrite into the file's own, and the query path's partition pruning makes
// the same assumption. Arc 25.12 and earlier placed a file by the hour it was
// flushed: delete.time_pruning is off for a deployment holding such data.
func (h *DeleteHandler) windowFor(database, measurement, where string, now time.Time) *deleteWindow {
	if h.config == nil || !h.config.TimePruning {
		return nil
	}
	tr := pruning.ExtractTimeRangeAt("SELECT * FROM t WHERE "+where, now)
	if tr == nil {
		return nil
	}
	w := &deleteWindow{prefix: database + "/" + measurement + "/"}
	// An assumed side is open: an end-only WHERE reaches the oldest file and
	// a start-only one a file stamped in the future, not the floor or
	// now+1 day the query path assumes.
	if !tr.StartAssumed {
		w.start, w.hasStart = tr.Start.UTC().Add(-deleteWindowPad), true
	}
	if !tr.EndAssumed {
		w.end, w.hasEnd = tr.End.UTC().Add(deleteWindowPad), true
	}
	if !w.hasStart && !w.hasEnd {
		return nil
	}
	return w
}

// keeps reports whether the file at path (relative to the storage root) may
// hold rows in the window. A path whose partition cannot be read, outside the
// DELETE's own prefix or in a layout this does not know, is kept.
func (w *deleteWindow) keeps(path string) bool {
	if w == nil {
		return true
	}
	rest, ok := strings.CutPrefix(path, w.prefix)
	if !ok {
		return true
	}
	segs := strings.Split(rest, "/")
	var from time.Time
	var span time.Duration
	switch len(segs) {
	case 4: // YYYY/MM/DD/file: a daily compaction output
		from, ok = partitionStart(segs[0], segs[1], segs[2], "00")
		span = 24 * time.Hour
	case 5: // YYYY/MM/DD/HH/file
		from, ok = partitionStart(segs[0], segs[1], segs[2], segs[3])
		span = time.Hour
	default:
		return true
	}
	if !ok {
		return true
	}
	// The end is exclusive (the extractor moves an inclusive one past its
	// literal), and so is the partition's.
	if w.hasStart && !from.Add(span).After(w.start) {
		return false
	}
	if w.hasEnd && !from.Before(w.end) {
		return false
	}
	return true
}

// partitionStart reads a partition directory's start in UTC. Only exact
// digit widths and a real date and hour: February 30 is not read as March 2.
func partitionStart(year, month, day, hour string) (time.Time, bool) {
	num := func(s string, width int) (int, bool) {
		if len(s) != width {
			return 0, false
		}
		for _, c := range s {
			if c < '0' || c > '9' {
				return 0, false
			}
		}
		n, err := strconv.Atoi(s)
		return n, err == nil
	}
	y, ok1 := num(year, 4)
	m, ok2 := num(month, 2)
	d, ok3 := num(day, 2)
	hh, ok4 := num(hour, 2)
	if !ok1 || !ok2 || !ok3 || !ok4 {
		return time.Time{}, false
	}
	t := time.Date(y, time.Month(m), d, hh, 0, 0, 0, time.UTC)
	if t.Year() != y || int(t.Month()) != m || t.Day() != d || t.Hour() != hh {
		return time.Time{}, false
	}
	return t, true
}

// errScanFileChanged: a file the scan listed was gone when it was counted (a
// compaction or a move replaced it). Retryable: the next scan lists what
// replaced it.
var errScanFileChanged = errors.New("files changed under the delete while it counted them")

// scanFileError: a file could not be counted. The DELETE stops before it
// rewrites anything rather than leave the file's matching rows in place.
type scanFileError struct {
	path string
	err  error
}

func (e *scanFileError) Error() string {
	return fmt.Sprintf("could not count the rows of %s: %v", e.path, e.err)
}

func (e *scanFileError) Unwrap() error { return e.err }

// whereBindError: a file cannot bind the WHERE even read together with files
// that do (a schema donor), so the DELETE cannot tell which of its rows
// match.
type whereBindError struct {
	path string
	err  error
}

func (e *whereBindError) Error() string {
	return fmt.Sprintf("the WHERE clause cannot be applied to %s, even read together with files it can be applied to: %v", e.path, e.err)
}

func (e *whereBindError) Unwrap() error { return e.err }

func isBinderErr(err error) bool {
	var de *duckdb.Error
	return errors.As(err, &de) && de.Type == duckdb.ErrorTypeBinder
}

// countInChunks counts n files in chunks of deleteScanChunk with count, which
// runs one union_by_name query over the files at the given indices and
// returns a result per index it found rows for.
//
// A file read alone, or with others that lack a column the WHERE names,
// fails to bind (a Binder error), where the measurement's union of columns
// would have read that column as NULL, or as its widest type. Such a file is
// counted again together with a chunk that did bind: the schema donor. Its
// results are dropped, so the file is counted with the columns a read over
// every file would have given it, as one query over all of them did before.
// A file that fails to bind even with the donor is a whereBindError.
//
// With no donor (no file in the scan set has the columns the WHERE names, a
// column newer than all of them say), those files match nothing, as a cold
// file (#1363) and a DELETE record (delete_records.go) that cannot bind do:
// otherwise every DELETE naming a newer column would fail on the older
// files. The one predicate that reads differently is one a NULL matches
// (col IS NULL), on a column no file in the time range has.
//
// Any other failure of a file goes to fail; a non-nil return stops the
// count. When a count with the donor fails, the donor is counted alone first,
// so a donor that went (a compaction replaced it) is the file reported, not
// the one it was read with. A cancelled context stops the count with
// ctx.Err(). It returns the donor's indices, nil when there is none.
func countInChunks[T any](ctx context.Context, n int, name func(int) string, count func(context.Context, []int) (map[int]T, error), fail func(i int, err error) error) (map[int]T, []int, error) {
	out := make(map[int]T)
	var donor, deferred []int
	add := func(got map[int]T, keep []int) {
		for _, i := range keep {
			if v, ok := got[i]; ok {
				out[i] = v
			}
		}
	}
	// failWith reports a failed count with the donor: the donor's fault if
	// it no longer counts alone.
	failWith := func(i int, err error) error {
		if _, derr := count(ctx, donor); derr != nil && !isBinderErr(derr) {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fail(donor[0], derr)
		}
		return fail(i, err)
	}
	// one counts each file alone, or with the donor when with is set.
	one := func(idx []int, with bool) error {
		for _, i := range idx {
			files := []int{i}
			if with {
				files = append(files, donor...)
			}
			got, err := count(ctx, files)
			switch {
			case err == nil:
				add(got, []int{i})
				if donor == nil {
					donor = []int{i}
				}
			case ctx.Err() != nil:
				return ctx.Err()
			case isBinderErr(err) && with:
				return &whereBindError{path: name(i), err: err}
			case isBinderErr(err):
				deferred = append(deferred, i)
			case with:
				if ferr := failWith(i, err); ferr != nil {
					return ferr
				}
			default:
				if ferr := fail(i, err); ferr != nil {
					return ferr
				}
			}
		}
		return nil
	}
	chunks := func(idx []int, fn func([]int) error) error {
		for start := 0; start < len(idx); start += deleteScanChunk {
			if err := fn(idx[start:min(start+deleteScanChunk, len(idx))]); err != nil {
				return err
			}
		}
		return nil
	}
	all := make([]int, n)
	for i := range all {
		all[i] = i
	}
	err := chunks(all, func(chunk []int) error {
		got, err := count(ctx, chunk)
		switch {
		case err == nil:
			add(got, chunk)
			if donor == nil {
				donor = chunk
			}
			return nil
		case ctx.Err() != nil:
			return ctx.Err()
		case isBinderErr(err):
			deferred = append(deferred, chunk...)
			return nil
		default:
			// One bad file fails the whole chunk: find it.
			return one(chunk, false)
		}
	})
	if err != nil {
		return nil, nil, err
	}
	if len(deferred) == 0 {
		return out, donor, nil
	}
	if donor == nil {
		// No chunk bound: a file that binds alone is the donor.
		pending := deferred
		deferred = nil
		if err := one(pending, false); err != nil {
			return nil, nil, err
		}
		if donor == nil {
			return out, nil, nil
		}
	}
	pending := deferred
	deferred = nil
	err = chunks(pending, func(chunk []int) error {
		got, err := count(ctx, append(append([]int(nil), chunk...), donor...))
		switch {
		case err == nil:
			add(got, chunk)
			return nil
		case ctx.Err() != nil:
			return ctx.Err()
		default:
			return one(chunk, true)
		}
	})
	if err != nil {
		return nil, nil, err
	}
	return out, donor, nil
}

// countConn is a connection for a DELETE's counting queries with DuckDB's
// Parquet metadata cache off. Arc turns that cache on for the whole database
// (internal/database/duckdb.go), so every footer a count read stayed in memory until the
// DELETE ended: on wide files the footers are most of the cost, and a DELETE
// over many files held them all however small its chunks (measured: 1,416
// files of 1,000 columns, +1.8 GB with the cache, +0.4 GB without). The
// release discards the connection rather than handing it back with the
// override (RESET reads DuckDB's default, false, not Arc's), so the pool
// opens a fresh one, which takes the database-wide settings.
func (h *DeleteHandler) countConn(ctx context.Context) (*sql.Conn, func(), error) {
	conn, err := h.db.DB().Conn(ctx)
	if err != nil {
		return nil, nil, err
	}
	release := func() {
		_ = conn.Raw(func(any) error { return driver.ErrBadConn })
		_ = conn.Close()
	}
	if _, err := conn.ExecContext(ctx, "SET SESSION parquet_metadata_cache=false"); err != nil {
		release()
		return nil, nil, err
	}
	return conn, release, nil
}
