package edgesync

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

func receiptState(t *testing.T, idx *HubIndex, spoke, path string) (HeldFile, bool) {
	t.Helper()
	held, err := idx.Lookup(context.Background(), spoke, []string{path})
	if err != nil {
		t.Fatal(err)
	}
	hf, ok := held[path]
	return hf, ok
}

// A file the hub removed on purpose keeps its receipt, marked removed, and
// reconcile keeps answering "present": the spoke does not re-send what was
// deleted (#1186). Without the mark the existence check forgets the receipt
// and the spoke re-sends, which is what retention's delete loop was.
func TestReconcile_RemovedReceiptStaysPresent(t *testing.T) {
	ctx := context.Background()
	rec, idx, backend := newTestReconcilerWithBackend(t)
	const kept, gone = "metrics/cpu/2026/01/01/00/kept.parquet", "metrics/cpu/2026/01/01/00/gone.parquet"
	shaA, shaB := sha256Hex([]byte("a")), sha256Hex([]byte("b"))
	seedFile(t, idx, backend, "rocket-01", kept, shaA)
	seedFile(t, idx, backend, "rocket-01", gone, shaB)

	if err := idx.MarkRemoved(ctx, "rocket-01", []string{gone}, RemovedByRetention); err != nil {
		t.Fatal(err)
	}
	if err := backend.Delete(ctx, NamespacedPath("rocket-01", gone)); err != nil {
		t.Fatal(err)
	}
	res, err := rec.Reconcile(ctx, "rocket-01", []ReconcileEntry{{Path: kept, SHA256: shaA, SizeBytes: 100}, {Path: gone, SHA256: shaB, SizeBytes: 100}})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Present) != 2 || len(res.Missing) != 0 {
		t.Fatalf("present=%v missing=%v, want both present", res.Present, res.Missing)
	}
	if hf, ok := receiptState(t, idx, "rocket-01", gone); !ok || !hf.Removed || hf.Compacted {
		t.Fatalf("receipt = %+v ok=%v, want kept and removed", hf, ok)
	}
}

// A re-sent copy of removed content (a stale air-gap bundle, a pruned-ledger
// rediscovery) is answered already-present and nothing lands in storage.
func TestReceiver_RemovedReceiptAnswersAlreadyPresent(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	backend, err := storage.NewLocalBackend(dir, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	idx := newTestHubIndex(t)
	r, err := NewReceiver(ReceiverConfig{Backend: backend, Index: idx, Logger: zerolog.Nop()})
	if err != nil {
		t.Fatal(err)
	}
	content := []byte("payload")
	digest := sha256Hex(content)
	if res, err := r.Receive(ctx, "rocket-01", testPath, digest, int64(len(content)), 0, bytes.NewReader(content)); err != nil || res.Outcome != OutcomeCommitted {
		t.Fatalf("first receive: %v %v", res, err)
	}
	if err := idx.MarkSpokeRemoved(ctx, "rocket-01", RemovedByOperatorDelete); err != nil {
		t.Fatal(err)
	}
	if err := backend.Delete(ctx, "rocket-01/"+testPath); err != nil {
		t.Fatal(err)
	}
	res, err := r.Receive(ctx, "rocket-01", testPath, digest, int64(len(content)), 0, bytes.NewReader(content))
	if err != nil || res.Outcome != OutcomeAlreadyPresent {
		t.Fatalf("re-receive = %v %v, want already-present", res, err)
	}
	if present, _ := backend.Exists(ctx, "rocket-01/"+testPath); present {
		t.Fatal("a re-sent copy of deleted content landed in storage")
	}

	// After an explicit forget the same file is accepted again, and the new
	// receipt is clean.
	n, err := idx.ForgetRemoved(ctx, "rocket-01", "metrics")
	if err != nil || n != 1 {
		t.Fatalf("ForgetRemoved = %d, %v; want 1", n, err)
	}
	res, err = r.Receive(ctx, "rocket-01", testPath, digest, int64(len(content)), 0, bytes.NewReader(content))
	if err != nil || res.Outcome != OutcomeCommitted {
		t.Fatalf("receive after forget = %v %v, want committed", res, err)
	}
	if hf, ok := receiptState(t, idx, "rocket-01", testPath); !ok || hf.Removed || hf.Compacted {
		t.Fatalf("receipt after re-delivery = %+v, want clean", hf)
	}
}

// MarkRemoved marks the path's own receipt and, for a removed compacted
// output whose path matches no receipt, the compacted receipts under its
// partition directory. Uncompacted receipts in that directory stay untouched
// (their files are their own), and the directory match is exact at a
// boundary: "_" in a database name is not a wildcard and "cpu" does not
// match "cpu2".
func TestMarkRemoved_PathsAndCompactedInputsUnderTheDirectory(t *testing.T) {
	ctx := context.Background()
	idx := newTestHubIndex(t)
	const (
		dir         = "my_db/cpu/2026/01/01/00/"
		raw         = dir + "raw.parquet"                   // its own file: marked only when named
		input       = dir + "in.parquet"                    // compacted into the output below
		deeper      = dir + "sub/in2.parquet"               // compacted, in a subdirectory
		sibling     = "my_db/cpu2/2026/01/01/00/in.parquet" // compacted, other measurement
		wildcard    = "myXdb/cpu/2026/01/01/00/in.parquet"  // compacted, "_" must not match X
		otherSpoke  = dir + "in.parquet"
		outputGone  = dir + "compacted_output.parquet" // no receipt: hub-side output
		explicitRaw = "my_db/mem/2026/01/01/00/raw.parquet"
	)
	const memInput = "my_db/mem/2026/01/01/00/in.parquet" // compacted, beside a named delivered file
	for _, p := range []string{raw, input, deeper, sibling, wildcard, explicitRaw, memInput} {
		recordFile(t, idx, "rocket-01", p, "aa")
	}
	recordFile(t, idx, "rocket-02", otherSpoke, "aa")
	if err := idx.MarkCompacted(ctx, "rocket-01", []string{input, deeper, sibling, wildcard, memInput}); err != nil {
		t.Fatal(err)
	}
	if err := idx.MarkCompacted(ctx, "rocket-02", []string{otherSpoke}); err != nil {
		t.Fatal(err)
	}

	if err := idx.MarkRemoved(ctx, "rocket-01", []string{outputGone, explicitRaw}, RemovedByRetention); err != nil {
		t.Fatal(err)
	}
	// memInput stays: explicitRaw has its own receipt, so it is a delivered
	// file and only its own receipt is marked, not its compacted neighbours.
	for p, want := range map[string]bool{raw: false, input: true, deeper: true, sibling: false, wildcard: false, explicitRaw: true, memInput: false} {
		if hf, _ := receiptState(t, idx, "rocket-01", p); hf.Removed != want {
			t.Errorf("%s removed = %v, want %v", p, hf.Removed, want)
		}
	}
	if hf, _ := receiptState(t, idx, "rocket-02", otherSpoke); hf.Removed {
		t.Error("another spoke's receipt was marked")
	}
}

// MarkSpokeRemoved marks every receipt of the spoke, across batches, and no
// other spoke's. ForgetRemoved forgets only removed receipts under the named
// database, at a boundary, also across batches.
func TestMarkSpokeRemovedAndForgetRemoved_Batched(t *testing.T) {
	ctx := context.Background()
	idx := newTestHubIndex(t)
	n := removalChunk + 50
	tx, err := idx.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		if _, err := tx.Exec(`INSERT INTO sync_received (spoke_id, source_path, hub_path, sha256, size_bytes, received_at) VALUES (?,?,?,?,?,CURRENT_TIMESTAMP)`,
			"rocket-01", fmt.Sprintf("db/m/2026/01/01/00/f%05d.parquet", i), "x", "aa", 1); err != nil {
			t.Fatal(err)
		}
	}
	tx.Commit()
	recordFile(t, idx, "rocket-01", "db2/m/2026/01/01/00/f.parquet", "aa") // sibling database
	recordFile(t, idx, "rocket-02", "db/m/2026/01/01/00/f00000.parquet", "aa")

	if err := idx.MarkSpokeRemoved(ctx, "rocket-01", RemovedByOperatorDelete); err != nil {
		t.Fatal(err)
	}
	var removed, other int
	idx.db.QueryRow(`SELECT COUNT(*) FROM sync_received WHERE spoke_id='rocket-01' AND removed_at IS NOT NULL AND removed_reason='operator_delete'`).Scan(&removed)
	idx.db.QueryRow(`SELECT COUNT(*) FROM sync_received WHERE spoke_id='rocket-02' AND removed_at IS NOT NULL`).Scan(&other)
	if removed != n+1 || other != 0 {
		t.Fatalf("removed=%d (want %d) other spoke=%d (want 0)", removed, n+1, other)
	}

	// A compacted-only receipt under the database survives a forget.
	recordFile(t, idx, "rocket-01", "db/m/2026/01/01/01/kept.parquet", "aa")
	if err := idx.MarkCompacted(ctx, "rocket-01", []string{"db/m/2026/01/01/01/kept.parquet"}); err != nil {
		t.Fatal(err)
	}
	got, err := idx.ForgetRemoved(ctx, "rocket-01", "db")
	if err != nil || got != int64(n) {
		t.Fatalf("ForgetRemoved = %d, %v; want %d (db2 and compacted-only kept)", got, err, n)
	}
	if _, ok := receiptState(t, idx, "rocket-01", "db2/m/2026/01/01/00/f.parquet"); !ok {
		t.Error("ForgetRemoved crossed the database boundary into db2")
	}
	if hf, ok := receiptState(t, idx, "rocket-01", "db/m/2026/01/01/01/kept.parquet"); !ok || !hf.Compacted {
		t.Error("ForgetRemoved forgot a compacted-only receipt")
	}
}

// A hub whose arc.db predates the removed columns gains them at startup.
func TestHubIndex_MigratesRemovedColumns(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "old-*.db")
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	db, err := sql.Open("sqlite3", f.Name())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	if _, err := db.Exec(`CREATE TABLE sync_received (
		id INTEGER PRIMARY KEY AUTOINCREMENT, spoke_id TEXT NOT NULL, source_path TEXT NOT NULL,
		hub_path TEXT NOT NULL, sha256 TEXT NOT NULL, size_bytes INTEGER NOT NULL,
		received_at TIMESTAMP NOT NULL, compacted_at TIMESTAMP, UNIQUE(spoke_id, source_path));
		INSERT INTO sync_received (spoke_id, source_path, hub_path, sha256, size_bytes, received_at, compacted_at)
		VALUES ('rocket-01','db/m/f.parquet','rocket-01/db/m/f.parquet','aa',1,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)`); err != nil {
		t.Fatal(err)
	}
	idx, err := NewHubIndex(db, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewHubIndex(db, zerolog.Nop()); err != nil { // idempotent
		t.Fatal(err)
	}
	if hf, ok := receiptState(t, idx, "rocket-01", "db/m/f.parquet"); !ok || !hf.Compacted || hf.Removed {
		t.Fatalf("migrated receipt = %+v ok=%v", hf, ok)
	}
	if err := idx.MarkRemoved(context.Background(), "rocket-01", []string{"db/m/f.parquet"}, RemovedByRetention); err != nil {
		t.Fatal(err)
	}
	if hf, _ := receiptState(t, idx, "rocket-01", "db/m/f.parquet"); !hf.Removed {
		t.Fatal("mark on a migrated table did not stick")
	}
}
