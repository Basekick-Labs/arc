package api

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/basekick-labs/arc/internal/rehome"
	sqlutil "github.com/basekick-labs/arc/internal/sql"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/basekick-labs/arc/internal/tiering"
)

// A file that is already in cold storage can be misfiled too: a daily file
// that spans days and has since been migrated (#1825). Its job differs from
// a hot file's in where the old file is, and in nothing else:
//
//   - the old file is read from cold storage, and checked there by tiering
//     (CheckRehomeColdSource) under the DELETE rewrite lock;
//   - once reads use the new files, the old one is left out by its address
//     in cold storage (rehome.Registry.ExcludedTiered, readExclusions);
//   - it is removed from cold storage (RemoveRehomedColdFile): the read
//     caches are dropped, the object deleted, cold storage asked that it is
//     gone, its tier row retired. The job does not end before that.
//
// The new files are written to the hot tier, like any job's, with hot tier
// rows; day files among them migrate on a later tiering cycle.
//
// The job's record names the cold store the file was in. Taken up against
// another store, or with no cold store at all, the job is kept as recorded:
// "not there" is not "removed".

// coldNameProbeTimeout bounds the question a job puts to the query engine
// about a cold file's name.
const coldNameProbeTimeout = 2 * time.Minute

// ColdFileNamesFromEngine returns RehomeSwap.ColdFileNames over the query
// engine's own connection, which has the cold store's credentials: the names
// DuckDB reports for the files a glob matches, read as a query reads them.
func ColdFileNamesFromEngine(db *sql.DB) func(ctx context.Context, glob string) ([]string, error) {
	return func(ctx context.Context, glob string) ([]string, error) {
		rows, err := db.QueryContext(ctx,
			"SELECT DISTINCT "+openCompactionFileColumn+" FROM "+
				sqlutil.ReadParquet(sqlutil.QuoteStringLiteral(glob), "union_by_name=true", "filename='"+openCompactionFileColumn+"'"))
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		var names []string
		for rows.Next() {
			var n string
			if err := rows.Scan(&n); err != nil {
				return nil, err
			}
			names = append(names, n)
		}
		return names, rows.Err()
	}
}

// inColdStorage reports whether this node's cold storage holds key. It is
// only what makes a request a job on a cold file; the checks are the job's.
func (h *RehomeHandler) inColdStorage(ctx context.Context, key string) (bool, error) {
	t := h.tieringManager()
	if t == nil {
		return false, nil
	}
	cold := t.ColdBackend()
	if cold == nil {
		return false, nil
	}
	n, err := cold.StatFile(ctx, key)
	if err != nil {
		return false, err
	}
	return n >= 0, nil
}

// coldSource checks a file in cold storage for a job and returns its size,
// the cold store's identity and the backend to read it from.
func (h *RehomeHandler) coldSource(ctx context.Context, key string) (size int64, store string, cold storage.Backend, err error) {
	t := h.tieringManager()
	if t == nil {
		return 0, "", nil, rehome.Refusef("%s is not on the hot tier, and tiered storage is not running on this node", key)
	}
	if cold = t.ColdBackend(); cold == nil {
		return 0, "", nil, rehome.Refusef("%s is not on the hot tier, and this node has no usable cold tier", key)
	}
	size, store, err = t.CheckRehomeColdSource(ctx, key)
	switch {
	case err == nil && store == "":
		// The record must say which cold store the file is in; a record
		// without it could not be read back.
		return 0, "", nil, rehome.Refusef("this node's cold store has no identity (no bucket or container configured?), so a job on %s could not be recorded", key)
	case err == nil:
		return size, store, cold, nil
	case errors.Is(err, tiering.ErrRehomeNotCold):
		return 0, "", nil, rehome.Refusef("%v", err)
	}
	return 0, "", nil, rehome.Refusef("cold storage could not be asked about %s (%v); retry once it can", key, err)
}

// coldSourceSize is the size of a job's source in the cold store the job
// recorded, -1 when it is not there. An error means it could not be asked:
// no tiering, no cold tier, another cold store, or cold storage unreachable.
func (h *RehomeHandler) coldSourceSize(ctx context.Context, job *rehome.Job) (int64, error) {
	t := h.tieringManager()
	if t == nil {
		return 0, fmt.Errorf("%s is in cold storage and tiered storage is not running on this node", job.Source.Key)
	}
	return t.ColdSourceStat(ctx, job.Source.Key, job.Source.ColdStore)
}

// removeColdSource removes a job's source from cold storage, with its row.
func (h *RehomeHandler) removeColdSource(ctx context.Context, job *rehome.Job) error {
	t := h.tieringManager()
	if t == nil {
		return errJobKept{fmt.Sprintf("%s is in cold storage and tiered storage is not running on this node", job.Source.Key)}
	}
	if err := t.RemoveRehomedColdFile(ctx, job.Source.Key, job.Source.ColdStore); err != nil {
		return fmt.Errorf("remove %s from cold storage: %w; the file is kept and left out of reads", job.Source.Key, err)
	}
	return nil
}

// coldNameCheck is why the job's cold source cannot be left out of reads by
// name, or "". A read names files through globs and filters on the name the
// engine reports; the filter's literal is storage.ObjectURI on the cold
// backend. The two must be the same string, and that is the engine's to say,
// per backend and per configuration: asked through the two glob shapes a read
// uses, the file's directory and a recursive one.
func (h *RehomeHandler) coldNameCheck(ctx context.Context, job *rehome.Job) string {
	t := h.tieringManager()
	if t == nil || t.ColdBackend() == nil {
		return "tiered storage is not running on this node"
	}
	if h.swap.ColdFileNames == nil {
		return "this node cannot ask its query engine how it names a file in cold storage"
	}
	want, err := storage.ObjectURI(t.ColdBackend(), job.Source.Key)
	if err != nil {
		return fmt.Sprintf("%s has no address in cold storage: %v", job.Source.Key, err)
	}
	slash := strings.LastIndex(want, "/")
	if slash < 0 {
		return fmt.Sprintf("%s has no directory in cold storage", want)
	}
	dir, base := want[:slash], want[slash+1:]
	// Bounded: the probe reads the footers of one directory in cold storage.
	ctx, cancel := context.WithTimeout(ctx, coldNameProbeTimeout)
	defer cancel()
	for _, glob := range []string{dir + "/*.parquet", dir + "/**/*.parquet"} {
		names, err := h.swap.ColdFileNames(ctx, glob)
		if err != nil {
			return fmt.Sprintf("the query engine could not read %s in cold storage: %v", glob, err)
		}
		found, other := false, ""
		for _, n := range names {
			switch {
			case n == want:
				found = true
			case strings.HasSuffix(n, "/"+base):
				// The same file under another spelling, or, through the
				// recursive glob, a file of that name in a subdirectory.
				other = n
			}
		}
		if !found && other != "" {
			return fmt.Sprintf("the query engine names the file %q where Arc would leave out %q: reads could not leave it out once its rows are in new files. Not supported on this cold storage configuration", other, want)
		}
		if !found {
			return fmt.Sprintf("the query engine does not list %s under %s in cold storage", want, glob)
		}
	}
	return ""
}
