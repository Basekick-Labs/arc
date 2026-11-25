package iceberg

import (
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
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

type atomicGate struct{ on atomic.Bool }

func (g *atomicGate) CanRun() bool { return g.on.Load() }

// #1903: with no traffic at all, the loop still sees the role leave and come
// back, and runs a pass at once rather than at the next tick (an hour here),
// so the table the REST catalog serves again is brought in line promptly.
func TestGateReopenRunsAPassWithoutWaitingForTheTimer(t *testing.T) {
	r := newAppendRig(t)
	gate := &atomicGate{}
	gate.on.Store(true)
	r.sched = NewScheduler(SchedulerConfig{Exporter: r.exp, Source: r.src, Interval: time.Hour, Logger: r.sched.logger,
		IngestInterval: time.Millisecond, IngestAppend: true, Gate: gate})
	r.sched.started = time.Now().Add(-time.Second)
	r.sched.gatePoll = 10 * time.Millisecond
	r.newFile()
	r.sched.Start()
	t.Cleanup(r.sched.Stop)
	waitFor := func(what string, cond func() bool) {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for !cond() {
			if time.Now().After(deadline) {
				t.Fatal(what)
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
	waitFor("the start-up pass did not run", func() bool { return r.listings() >= 1 })
	afterStart := r.listings()

	gate.on.Store(false)
	time.Sleep(100 * time.Millisecond) // several polls see it closed
	if got := r.listings(); got != afterStart {
		t.Fatalf("listed %d times while another node exported", got-afterStart)
	}
	gate.on.Store(true)
	waitFor("no pass after the role came back (the next tick is an hour away)", func() bool { return r.listings() > afterStart })
	time.Sleep(100 * time.Millisecond)
	if got := r.listings() - afterStart; got != 1 {
		t.Fatalf("%d passes after one reopen, want 1", got)
	}
}
