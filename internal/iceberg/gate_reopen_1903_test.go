package iceberg

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

// #1903: a node that loses the export role and gets it back without a
// restart must not append onto the table as it left it. While another node
// exported, this node's ingest reports were dropped and files it had exported
// went away (compaction inputs, retention, DELETE); appending only the new
// file would publish a table that names the gone files and misses the ones
// written in between. The first export after the role returns must be a pass.
// Both ingest paths: serial (concurrency 1) and batched (the default, 8).
func TestGateReopenDoesNotAppendOntoAStaleTable(t *testing.T) {
	for _, conc := range []int{1, 8} {
		t.Run("concurrency="+strconv.Itoa(conc), func(t *testing.T) {
			r := newAppendRig(t)
			gate := &flagGate{on: true}
			r.sched = NewScheduler(SchedulerConfig{Exporter: r.exp, Source: r.src, Interval: time.Hour, Logger: r.sched.logger,
				IngestInterval: time.Millisecond, IngestAppend: true, Gate: gate, IngestConcurrency: conc})
			r.sched.started = time.Now().Add(-time.Second)
			first := r.settled()

			// Another node exports: this node's report is dropped, and a
			// file its table names goes away meanwhile.
			gate.on = false
			gap := r.newFile()
			r.report(gap)
			r.serve()
			if err := os.Remove(filepath.Join(r.root, filepath.FromSlash(appendDir+first))); err != nil {
				t.Fatal(err)
			}

			// The role comes back to this node, which has not restarted.
			gate.on = true
			fresh := r.newFile()
			before := r.listings()
			r.report(fresh)
			r.serve()
			r.wantOnce(gap, fresh)
			if got := r.listings() - before; got != 1 {
				t.Fatalf("the first export after the role returned listed the measurement %d times, want 1 (a pass)", got)
			}
		})
	}
}
