package cluster

import (
	"strings"
	"sync"
	"testing"
)

// #1728: what the exporter node's coordinator tells the Iceberg export.

type pathRecorder struct {
	mu              sync.Mutex
	now, late, cold []string
}

func (p *pathRecorder) wire(c *Coordinator) {
	c.SetIcebergPathCallbacks(
		func(k []string) { p.mu.Lock(); p.now = append(p.now, k...); p.mu.Unlock() },
		func(k []string) { p.mu.Lock(); p.late = append(p.late, k...); p.mu.Unlock() },
		func(k []string) { p.mu.Lock(); p.cold = append(p.cold, k...); p.mu.Unlock() },
	)
}

func (p *pathRecorder) gotCold() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return strings.Join(p.cold, ",")
}

func (p *pathRecorder) got() (string, string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return strings.Join(p.now, ","), strings.Join(p.late, ",")
}

func TestDirsOfCollapsesToDirectories(t *testing.T) {
	got := dirsOf([]string{"db/m/2026/11/16/14/a.parquet", "db/m/2026/11/16/14/b.parquet", "db/n/2026/11/16/15/c.parquet", "x.parquet"})
	if strings.Join(got, ",") != "db/m/2026/11/16/14/,db/n/2026/11/16/15/" {
		t.Fatalf("dirsOf = %v", got)
	}
}

// A local unlink is reported once its batch ends; a DELETE rewrite's original
// unlinked before its rewrite arrived is not (a reconcile then would publish
// neither file), and is reported when the rewrite lands instead.
func TestLocalUnlinkReportsToTheExport(t *testing.T) {
	t.Run("retention unlink", func(t *testing.T) {
		r := newRewriteUnlinkRig(t)
		rec := &pathRecorder{}
		rec.wire(r.c)
		r.c.unlinkBatch([]deleteRequest{{path: r.old, reason: "retention:1"}}, nil)
		if r.exists(r.old) {
			t.Fatal("not unlinked")
		}
		if now, _ := rec.got(); now != stHour {
			t.Fatalf("reported %q, want the file's directory %q", now, stHour)
		}
	})
	t.Run("rewrite original unlinked before its rewrite (no deferral)", func(t *testing.T) {
		r := newRewriteUnlinkRig(t)
		r.c.stale.seen.Store(nil) // no journal: the unlink is not deferred
		rec := &pathRecorder{}
		rec.wire(r.c)
		r.c.unlinkBatch([]deleteRequest{{path: r.old, reason: deleteRewriteReason}}, nil)
		if r.exists(r.old) {
			t.Fatal("expected the unlink without deferral")
		}
		if now, _ := rec.got(); now != "" {
			t.Fatalf("reported %q before the rewrite arrived: the table would lose the surviving rows", now)
		}
		r.write(r.rw, r.rwBody)
		r.c.recordPulledFileInTiering(r.rw, int64(len(r.rwBody)))
		// Asked after a delay: rewrites arrive one by one, and the original
		// may still be in its grace before the unlink.
		if now, late := rec.got(); now != "" || late != stHour {
			t.Fatalf("rewrite landed: now %q later %q, want later %q", now, late, stHour)
		}
	})
	t.Run("rewrite original after its rewrite (deferred)", func(t *testing.T) {
		r := newRewriteUnlinkRig(t)
		rec := &pathRecorder{}
		rec.wire(r.c)
		r.unlink() // deferred: rewrite not here
		r.write(r.rw, r.rwBody)
		r.c.recordPulledFileInTiering(r.rw, int64(len(r.rwBody)))
		r.c.unlinkBatch([]deleteRequest{{path: r.old, reason: deleteRewriteReason}}, nil)
		if now, late := rec.got(); now != stHour || late != stHour {
			t.Fatalf("now %q later %q, want the unlink now and the pull later", now, late)
		}
	})
	t.Run("database delete", func(t *testing.T) {
		// The drop removes the database's tables itself; a reconcile racing
		// it could recreate one.
		r := newRewriteUnlinkRig(t)
		rec := &pathRecorder{}
		rec.wire(r.c)
		r.c.unlinkBatch([]deleteRequest{{path: r.old, reason: databaseDeleteReason}}, nil)
		if r.exists(r.old) {
			t.Fatal("not unlinked")
		}
		if now, late := rec.got(); now != "" || late != "" {
			t.Fatalf("a database delete asked: now %q later %q", now, late)
		}
	})
	t.Run("nothing wired", func(t *testing.T) {
		r := newRewriteUnlinkRig(t)
		r.c.unlinkBatch([]deleteRequest{{path: r.old, reason: "retention:1"}}, nil)
		if len(r.c.iceberg.unlinked) != 0 {
			t.Fatal("unlinks collected with no export wired")
		}
	})
}

// A shared bucket's manifest delete, by reason: a rewrite's original now
// (reads already leave it out), retention and a whole-file DELETE later (the
// object goes after the manifest), the rest never (a database delete drops
// its tables itself; compaction and tiering ask on their own).
func TestSharedManifestDeleteAsksByReason(t *testing.T) {
	r := newRewriteUnlinkRig(t)
	rec := &pathRecorder{}
	rec.wire(r.c)
	const p = "db/m/2026/11/16/14/f.parquet"
	// Through the FSM's shared-backend delete observer, as in service.
	for _, reason := range []string{deleteRewriteReason, "retention:7", "delete", "database:delete", "compaction:j1", "tiering:cold", "snapshot:removed"} {
		r.c.observeSharedManifestDelete(p, reason)
	}
	now, late := rec.got()
	if now != "db/m/2026/11/16/14/" || late != "db/m/2026/11/16/14/,db/m/2026/11/16/14/" {
		t.Fatalf("now %q later %q", now, late)
	}
}
