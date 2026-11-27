package iceberg

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"strconv"
	"testing"

	icetable "github.com/apache/iceberg-go/table"
	"github.com/rs/zerolog"
)

// refuseExpiry makes every snapshot-expiry commit fail without committing.
func refuseExpiry(context.Context, *icetable.Transaction) (*icetable.Table, error) {
	return nil, errors.New("expiry refused")
}

// overRetention builds a table and leaves it with more snapshots than
// retain_snapshots = 2: every pass commits, and its expiry is refused.
func overRetention(t *testing.T) (*rig1686, []FileRef, ArcSchema) {
	t.Helper()
	r := newHealRig(t, 2)
	r.exp.expireCommit = refuseExpiry
	var files []FileRef
	var res reconcileResult
	for i := 0; i < 4; i++ {
		files = append(files, r.write(0, "e"+strconv.Itoa(i)))
		res = r.passResult1791(files, r.schema(files[0]), nil)
	}
	if res.settled {
		t.Fatal("a pass whose snapshot expiry failed is settled")
	}
	if got := r.snapshots(); got <= 2 {
		t.Fatalf("%d snapshots after four passes whose expiry was refused: nothing is over retain_snapshots = 2, the test shows nothing", got)
	}
	return r, files, r.schema(files[0])
}

func (r *rig1686) passResult1791(files []FileRef, sc ArcSchema, canCommit func() bool) reconcileResult {
	r.t.Helper()
	res, err := r.exp.reconcile(context.Background(), reconcileRequest{database: "mydb", measurement: "cpu", schema: sc, current: files, canCommit: canCommit})
	if err != nil {
		r.t.Fatal(err)
	}
	return res
}

// A snapshot expiry that failed is tried again by the table's next pass, though
// that pass commits nothing (#1791): it used to settle, and the snapshots
// over retain_snapshots stayed until the table's next commit.
func TestExpiry1791_AFailedExpiryIsRetriedByAPassThatCommitsNothing(t *testing.T) {
	r, files, sc := overRetention(t)

	// Still failing: the pass that commits nothing is not settled either.
	if res := r.passResult1791(files, sc, nil); res.settled || res.commits != 0 {
		t.Fatalf("expiry still refused: settled %v after %d commits; want an unsettled pass, so the scheduler runs the next one", res.settled, res.commits)
	}
	// It works again: expired, and settled.
	r.exp.expireCommit = nil
	res := r.passResult1791(files, sc, nil)
	if got := r.snapshots(); got != 2 {
		t.Fatalf("%d snapshots after a pass whose expiry worked, want retain_snapshots = 2", got)
	}
	if !res.settled {
		t.Fatal("the pass that expired is not settled")
	}
}

// An expiry commit that landed but reported an error: the next pass finds
// the table within retain_snapshots and settles without another commit. (On
// the code before #1791 this fails earlier, for the first test's reason: the
// pass did not try to expire at all.)
func TestExpiry1791_AnExpiryThatLandedButReportedAnErrorIsNotRepeated(t *testing.T) {
	r, files, sc := overRetention(t)
	r.exp.expireCommit = func(ctx context.Context, txn *icetable.Transaction) (*icetable.Table, error) {
		if _, err := txn.Commit(ctx); err != nil {
			t.Errorf("the expiry commit itself failed: %v", err)
		}
		return nil, errors.New("connection lost after the commit")
	}
	if res := r.passResult1791(files, sc, nil); res.settled {
		t.Fatal("a pass whose expiry reported an error is settled")
	}
	if got := r.snapshots(); got != 2 {
		t.Fatalf("%d snapshots after the expiry that landed, want 2", got)
	}
	expiries := 0
	r.exp.expireCommit = func(ctx context.Context, txn *icetable.Transaction) (*icetable.Table, error) {
		expiries++
		return txn.Commit(ctx)
	}
	if res := r.passResult1791(files, sc, nil); !res.settled || expiries != 0 {
		t.Fatalf("the next pass: settled %v, %d expiry commits; want settled with none", res.settled, expiries)
	}
}

// A node that lost the writer role before its pass could expire makes no
// expiry commit then, and expires at its next pass once the role is back.
func TestExpiry1791_AnExpirySkippedWithoutTheWriterRoleIsMadeUpLater(t *testing.T) {
	r, files, sc := overRetention(t)
	r.exp.expireCommit = nil
	before := r.snapshots()
	// The role goes after the pass's own check and before its end.
	calls := 0
	lost := func() bool { calls++; return calls <= 1 }
	gated := r.passResult1791(files, sc, lost)
	if gated.settled {
		t.Fatal("a pass that ended without the writer role is settled")
	}
	if gated.writerLost {
		t.Fatal("the pass stopped at its own check of the writer role: it did not get to its end without the role, the test shows nothing")
	}
	if got := r.snapshots(); got != before {
		t.Fatalf("%d snapshots, was %d: a node without the writer role committed an expiry", got, before)
	}
	res := r.passResult1791(files, sc, nil)
	if got := r.snapshots(); got != 2 || !res.settled {
		t.Fatalf("role back: %d snapshots, settled %v; want retain_snapshots = 2 and a settled pass", got, res.settled)
	}
}

// Another node committed to the table and stopped before it expired. The
// node that takes over has reconciled the table before in this process, so
// nothing told it an expiry was due; its next pass, which commits nothing,
// expires all the same.
func TestExpiry1791_APassExpiresWhatAnotherNodeLeft(t *testing.T) {
	r := newHealRig(t, 2)
	r.exp.SetWarehouseAuthoritative(true)
	files := []FileRef{r.write(0, "n0")}
	sc := r.schema(files[0])
	if res := r.passResult1791(files, sc, nil); !res.settled {
		t.Fatal("the first node's pass is not settled")
	}
	first := r.exp

	// The other node: its own catalog over the same warehouse. Its passes
	// commit, and none of its expiries lands.
	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "other.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	other, err := NewExporter(db, first.backend, "file://"+r.root, "arc", 2, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	other.SetWarehouseAuthoritative(true)
	other.chunkMax = 16
	other.expireCommit = refuseExpiry
	r.exp = other
	for i := 1; i <= 4; i++ {
		files = append(files, r.write(0, "n"+strconv.Itoa(i)))
		r.passResult1791(files, sc, nil)
	}
	if got := r.snapshots(); got <= 2 {
		t.Fatalf("%d snapshots after the other node's passes: nothing is over retain_snapshots = 2, the test shows nothing", got)
	}

	// Back on the first node: it adopts what was published, finds every
	// file in the table, and expires.
	r.exp = first
	res := r.passResult1791(files, sc, nil)
	if res.commits != 0 {
		t.Fatalf("%d data commits on the first node's pass, want none: the test is about a pass that commits nothing", res.commits)
	}
	if got := r.snapshots(); got != 2 || !res.settled {
		t.Fatalf("%d snapshots, settled %v after the first node's pass; want retain_snapshots = 2 and a settled pass", got, res.settled)
	}
}
