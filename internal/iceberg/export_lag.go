package iceberg

import (
	"path"
	"strings"
	"sync"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/basekick-labs/arc/internal/storage"
)

// Export lag (#1750): how long an ingest file waited between being written
// and the table version that holds it being published.
//
// A file's lag is known only when the version that holds it is published,
// which is not when it is committed: a pass can commit in several chunks and
// publishes once at its end, and a pass whose publish fails leaves its files
// to be published by a later one that commits nothing. So the write times of
// committed ingest files are kept per table (noteExported) and observed at
// the one place a publish succeeds (drainExportLag, from writeVersionHint).
//
// What counts as an ingest file: a name storage.IngestFileTime reads, that is
// not a DELETE rewrite (its name carries the time of the file it rewrote).
// Compacted and daily files do not parse, and their rows were exported
// before. A file written before this process started is existing data being
// exported, not a lag of the running export: it is counted apart
// (arc_iceberg_export_backlog_files_total) and kept out of the histogram.
type exportLag struct {
	mu      sync.Mutex
	pending map[string][]time.Time // table identifier -> write times of committed, unpublished ingest files
	started time.Time
	// last is, per table, the largest lag of its last publish that covered
	// ingest files (#1772, export_lag_tables.go).
	last map[string]lastLag
}

// lastLag is a table's last publish: the largest lag among its files, and
// when it was published.
type lastLag struct {
	database, measurement string
	lag                   time.Duration
	at                    time.Time
}

func tableLagKey(ident []string) string { return strings.Join(ident, "\x00") }

// noteExported records the ingest files among files, which a commit has just
// added to the table. skip names paths to leave out (files re-registered at
// the same path, which are not new).
func (e *Exporter) noteExported(ident []string, files []iceberg.DataFile, skip map[string]struct{}) {
	var times []time.Time
	backlog := 0
	for _, df := range files {
		p := df.FilePath()
		if _, ok := skip[p]; ok {
			continue
		}
		name := path.Base(p)
		if storage.StripRewriteSuffix(name) != name {
			continue // a DELETE rewrite
		}
		t, ok := storage.IngestFileTime(name)
		if !ok {
			continue
		}
		if t.Before(e.lag.started) {
			backlog++
			continue
		}
		times = append(times, t)
	}
	metrics.Get().AddIcebergExportBacklogFiles(backlog)
	if len(times) == 0 {
		return
	}
	key := tableLagKey(ident)
	e.lag.mu.Lock()
	if e.lag.pending == nil {
		e.lag.pending = make(map[string][]time.Time)
	}
	e.lag.pending[key] = append(e.lag.pending[key], times...)
	e.lag.mu.Unlock()
}

// drainExportLag observes the lag of every ingest file of the table that was
// committed and is published only now, and returns the largest. Called where
// a table version has just been published.
func (e *Exporter) drainExportLag(ident []string) time.Duration {
	key := tableLagKey(ident)
	e.lag.mu.Lock()
	times := e.lag.pending[key]
	delete(e.lag.pending, key)
	e.lag.mu.Unlock()
	if len(times) == 0 {
		// A publish that covered no ingest file (a pass that committed
		// nothing new, a heal) says nothing of the table's lag and leaves
		// its last one as it was.
		return 0
	}
	now := time.Now()
	var oldest time.Duration
	m := metrics.Get()
	for _, t := range times {
		d := now.Sub(t)
		m.ObserveIcebergExportLag(d)
		if d > oldest {
			oldest = d
		}
	}
	e.noteLastLag(key, ident, oldest, now)
	if oldest > e.passLag {
		e.passLag = oldest
	}
	return oldest
}

// forgetExportLag drops the write times kept for a table whose committed
// files this exporter will not publish: it lost the writer role or was
// fenced during the pass (another node publishes from here), or the table
// was dropped. Kept, they would be observed as an hours-long lag the next
// time this node publishes a table of that name.
func (e *Exporter) forgetExportLag(ident []string) {
	if len(ident) == 0 {
		return
	}
	e.lag.mu.Lock()
	delete(e.lag.pending, tableLagKey(ident))
	delete(e.lag.last, tableLagKey(ident))
	e.lag.mu.Unlock()
}

// forgetExportLagIfNotPublisher is what a pass runs at its end: forget the
// table's write times when this exporter was fenced during the pass or is no
// longer the writer. Called while the pass's gate is still installed.
func (e *Exporter) forgetExportLagIfNotPublisher(database, measurement string) {
	if e.fencedErr() != nil || !e.passMayCommit() {
		e.forgetExportLag(e.tableIdent(database, measurement))
	}
}

// forgetExportLagNamespace is forgetExportLag for every table of a namespace.
func (e *Exporter) forgetExportLagNamespace(namespace []string) {
	if len(namespace) == 0 {
		return
	}
	prefix := tableLagKey(namespace) + "\x00"
	e.lag.mu.Lock()
	for key := range e.lag.pending {
		if strings.HasPrefix(key, prefix) {
			delete(e.lag.pending, key)
		}
	}
	for key := range e.lag.last {
		if strings.HasPrefix(key, prefix) {
			delete(e.lag.last, key)
		}
	}
	e.lag.mu.Unlock()
}
