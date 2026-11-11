package iceberg

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/rs/zerolog"
)

// #1613: a catalog restored behind its local warehouse. A whole-instance
// backup lists the warehouse after the SQLite snapshot, without pausing the
// export, so the warehouse it copies can be ahead of the rows, and its
// version hint, read last, can name a v<N> it never listed. A restore in
// place (merge-only, SQLite applied at the next boot) leaves the same shape:
// the restored row behind versions the live warehouse still holds.

const restoreRetain = 10 // keeps every NNNNN-<uuid>.metadata.json these tests address

// exportedRestoreRig exports `passes` passes through one node and returns the
// rig, the node's catalog, and the catalog location of each committed
// version still on disk.
func exportedRestoreRig(t *testing.T, passes int) (*warehouseRig, *sql.DB, map[int]string) {
	t.Helper()
	r := newWarehouseRig(t)
	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "arc.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	exp := restoredNode(t, r, db)
	for i := 0; i < passes; i++ {
		r.addFile()
		if err := r.reconcile(exp); err != nil {
			t.Fatal(err)
		}
	}
	cur := rowLocation(t, db)
	prefix := cur[:strings.LastIndex(cur, "/")+1]
	entries, err := os.ReadDir(r.metaDir())
	if err != nil {
		t.Fatal(err)
	}
	rows := map[int]string{}
	for _, e := range entries {
		if name := e.Name(); strings.HasSuffix(name, ".metadata.json") && !strings.HasPrefix(name, "v") {
			rows[versionOf(t, name)] = prefix + name
		}
	}
	if _, ok := rows[versionOf(t, cur)]; !ok {
		t.Fatalf("current row %s not on disk", cur)
	}
	return r, db, rows
}

// restoredNode is a fresh process over db: nothing in memory about what it
// published. Every warehouse is authoritative since #1613.
func restoredNode(t *testing.T, r *warehouseRig, db *sql.DB) *Exporter {
	t.Helper()
	exp, err := NewExporter(db, r.backend, DefaultWarehouse(r.backend), "arc", restoreRetain, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	exp.SetWarehouseAuthoritative(true)
	return exp
}

func rowLocation(t *testing.T, db *sql.DB) string {
	t.Helper()
	var loc string
	if err := db.QueryRow(`SELECT metadata_location FROM iceberg_tables WHERE table_name = 'cpu'`).Scan(&loc); err != nil {
		t.Fatal(err)
	}
	return loc
}

func versionOf(t *testing.T, loc string) int {
	t.Helper()
	base := loc[strings.LastIndex(loc, "/")+1:]
	head, _, ok := strings.Cut(base, "-")
	if !ok {
		head = strings.TrimSuffix(strings.TrimPrefix(base, "v"), ".metadata.json")
	}
	n, err := strconv.Atoi(head)
	if err != nil {
		t.Fatalf("version of %s: %v", loc, err)
	}
	return n
}

func restoreRow(t *testing.T, db *sql.DB, loc string) {
	t.Helper()
	if _, err := db.Exec(`UPDATE iceberg_tables SET metadata_location = ? WHERE table_name = 'cpu'`, loc); err != nil {
		t.Fatal(err)
	}
}

// removeVersion deletes version n's committed metadata and its v<n> copy:
// a commit the backup's listing never saw.
func removeVersion(t *testing.T, r *warehouseRig, n int, rowLoc string) {
	t.Helper()
	if err := os.Remove(filepath.Join(r.metaDir(), "v"+strconv.Itoa(n)+".metadata.json")); err != nil {
		t.Fatal(err)
	}
	if rowLoc != "" {
		if err := os.Remove(filepath.Join(r.metaDir(), rowLoc[strings.LastIndex(rowLoc, "/")+1:])); err != nil {
			t.Fatal(err)
		}
	}
}

// Restore in place: the catalog row goes back to an older version while the
// live warehouse keeps the newer v<N>. A merge restore also writes the
// backup's hint over the live one (seen in a binary run), so the hint can
// agree with the stale row. Committing from it forked the table: on a quiet
// table the hint rolled back and the orphan sweep took the newer manifests;
// with new files the stale lineage published its own versions until it ran
// into the newer v<N>. Adoption moves the row forward first, from the newest
// v<N> present, not only the hint. These set the exporter authoritative
// directly; that every node is wired so is pinned by TestIcebergTopologyFor.
func TestRestoreBehind_AdoptsForward(t *testing.T) {
	for _, tc := range []struct {
		name         string
		hintRestored bool // the backup's hint written over the live one
		newFiles     bool
	}{
		{"quiet table, live hint kept", false, false},
		{"quiet table, hint restored with the catalog", true, false},
		{"new files, hint restored with the catalog", true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, db, rows := exportedRestoreRig(t, 5)
			head := r.hint()
			restoreRow(t, db, rows[head-3])
			if tc.hintRestored {
				if err := os.WriteFile(filepath.Join(r.metaDir(), "version-hint.text"), []byte(strconv.Itoa(head-3)), 0o600); err != nil {
					t.Fatal(err)
				}
				// The backup held v<head-3>: a v<N> copy is published for it.
				body, err := os.ReadFile(filepath.Join(r.metaDir(), rows[head-3][strings.LastIndex(rows[head-3], "/")+1:]))
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(r.metaDir(), "v"+strconv.Itoa(head-3)+".metadata.json"), body, 0o600); err != nil {
					t.Fatal(err)
				}
			}
			exp := restoredNode(t, r, db)
			if tc.newFiles {
				r.addFile()
			}
			if err := r.reconcile(exp); err != nil {
				t.Fatalf("reconcile after the restore: %v", err)
			}
			if got := r.hint(); got < head {
				t.Fatalf("version hint at %d, below the published %d: the table continued from the restored row", got, head)
			}
			if got := versionOf(t, rowLocation(t, db)); got < head {
				t.Fatalf("catalog row at version %d, want at least the published %d", got, head)
			}
			if f := r.forkedVersions(); len(f) != 0 {
				t.Fatalf("forked versions %v", f)
			}
			if n := r.live(exp); n != len(r.files()) {
				t.Fatalf("table holds %d files, want %d", n, len(r.files()))
			}
		})
	}
}

// The backup's hint names a version whose files it does not hold. The row,
// relative to the newest v<N> the restore does hold: behind adopts it, equal
// keeps the row, ahead never adopts backward. Every case exports cleanly.
func TestRestoreBehind_HintNamesAMissingVersion(t *testing.T) {
	for _, tc := range []struct {
		name string
		// missing: how many of the newest versions the restore lacks (the
		// hint names the newest). rowBack: the restored row's distance below
		// the hint.
		missing, rowBack int
	}{
		{"row behind the newest version present", 1, 3},
		{"row at the newest version present", 1, 1},
		{"row between the newest present and the hint", 2, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, db, rows := exportedRestoreRig(t, 6)
			head := r.hint()
			for v := head; v > head-tc.missing; v-- {
				loc := rows[v]
				if v == head-tc.rowBack {
					loc = "" // the row's own metadata is restored; only v<N> is lost
				}
				removeVersion(t, r, v, loc)
			}
			restoreRow(t, db, rows[head-tc.rowBack])
			exp := restoredNode(t, r, db)
			r.addFile()
			if err := r.reconcile(exp); err != nil {
				t.Fatalf("reconcile after the restore: %v", err)
			}
			if f := r.forkedVersions(); len(f) != 0 {
				t.Fatalf("forked versions %v", f)
			}
			if n := r.live(exp); n != len(r.files()) {
				t.Fatalf("table holds %d files, want %d", n, len(r.files()))
			}
		})
	}
}

// The restored row's own metadata is gone too (pruned before the backup
// copied it), and the hint names a version the backup lacks. The row's v<N>
// is present, so adoption's missing-hint fallback recovers it before the
// load (publishedMatchesRow: published, own file gone); recoverDeadRow's
// fallback is pinned by DeadRowAboveEveryVersionTheBackupHolds.
func TestRestoreBehind_DeadRowWithAHintAheadOfTheBackupIsAdopted(t *testing.T) {
	r, db, rows := exportedRestoreRig(t, 5)
	head := r.hint()
	removeVersion(t, r, head, rows[head])
	dead := rows[head-1]
	if err := os.Remove(filepath.Join(r.metaDir(), dead[strings.LastIndex(dead, "/")+1:])); err != nil {
		t.Fatal(err)
	}
	restoreRow(t, db, dead)
	exp := restoredNode(t, r, db)
	r.addFile()
	if err := r.reconcile(exp); err != nil {
		t.Fatalf("reconcile: %v; want the dead row recovered onto v%d", err, head-1)
	}
	if n := r.live(exp); n != len(r.files()) {
		t.Fatalf("table holds %d files, want %d", n, len(r.files()))
	}
}

// #637 on a real local backend: the row's metadata and every v<N> are gone,
// but the hint survived. Nothing is published to recover onto, so the
// actionable metadata-missing error stands, not a read error on v<hint>.
func TestRestoreBehind_HintWithNoVersionsKeepsThe637Error(t *testing.T) {
	r, db, rows := exportedRestoreRig(t, 3)
	entries, err := os.ReadDir(r.metaDir())
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".metadata.json") {
			if err := os.Remove(filepath.Join(r.metaDir(), e.Name())); err != nil {
				t.Fatal(err)
			}
		}
	}
	restoreRow(t, db, rows[r.hint()])
	exp := restoredNode(t, r, db)
	r.addFile()
	err = r.reconcile(exp)
	if err == nil || !errors.Is(err, os.ErrNotExist) || strings.Contains(err.Error(), "read published Iceberg metadata") {
		t.Fatalf("reconcile: %v; want the #637 metadata-missing error", err)
	}
}

// The dead row's own v<N> is gone as well, so the newest version the backup
// holds is BELOW the dead row: adoption declines (never backward on a live
// row), the load fails, and recoverDeadRow moves the dead row down onto it.
func TestRestoreBehind_DeadRowAboveEveryVersionTheBackupHolds(t *testing.T) {
	r, db, rows := exportedRestoreRig(t, 5)
	head := r.hint()
	removeVersion(t, r, head, rows[head])
	removeVersion(t, r, head-1, rows[head-1])
	restoreRow(t, db, rows[head-1])
	exp := restoredNode(t, r, db)
	r.addFile()
	if err := r.reconcile(exp); err != nil {
		t.Fatalf("reconcile: %v; want the dead row recovered onto v%d", err, head-2)
	}
	if f := r.forkedVersions(); len(f) != 0 {
		t.Fatalf("forked versions %v", f)
	}
	if n := r.live(exp); n != len(r.files()) {
		t.Fatalf("table holds %d files, want %d", n, len(r.files()))
	}
}

// No catalog row (a database being dropped: DropDatabase removes the rows,
// then the v<N> files, then the hint) and a hint naming a missing version:
// the pass fails rather than adopt an older version into a new row for the
// table being dropped.
func TestRestoreBehind_NoRowNeverFallsBackBelowTheHint(t *testing.T) {
	r, db, rows := exportedRestoreRig(t, 4)
	head := r.hint()
	removeVersion(t, r, head, rows[head])
	if _, err := db.Exec(`DELETE FROM iceberg_tables WHERE table_name = 'cpu'`); err != nil {
		t.Fatal(err)
	}
	exp := restoredNode(t, r, db)
	if _, err := exp.adoptPublished(context.Background(), exp.tableIdent("mydb", "cpu"), "arc_mydb", "cpu"); err == nil {
		t.Fatal("adoptPublished with no row and a hint naming a missing version: nil error, want the read failure")
	}
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM iceberg_tables WHERE table_name = 'cpu'`).Scan(&n); err != nil || n != 0 {
		t.Fatalf("catalog rows for the table: %d (%v), want none", n, err)
	}
}

// A database DROP in progress on a shared bucket deletes v<N> files before
// the hint, and the table carries a drop marker: the missing-hint fallbacks
// do not follow an arbitrary leftover v<N>. The pass fails and retries.
func TestRestoreBehind_NoFallbackWhileTheTableIsBeingDropped(t *testing.T) {
	r, db, rows := exportedRestoreRig(t, 5)
	head := r.hint()
	removeVersion(t, r, head, rows[head])
	restoreRow(t, db, rows[head-3])
	exp := restoredNode(t, r, db)
	exp.SetForgetDroppedRows(true)
	if err := r.backend.Write(context.Background(), droppedMarkerKey("arc_mydb", "cpu"), []byte("x")); err != nil {
		t.Fatal(err)
	}
	r.addFile()
	if err := r.reconcile(exp); err == nil {
		t.Fatal("reconcile: nil error; want the pass to fail while the drop marker is set")
	}
	if got := rowLocation(t, db); got != rows[head-3] {
		t.Fatalf("catalog row moved to %s during a drop", got)
	}
}
