package edgesync

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/basekick-labs/arc/internal/storage"
	_ "github.com/mattn/go-sqlite3"
	"github.com/rs/zerolog"
)

func newTestHubIndex(t *testing.T) *HubIndex {
	t.Helper()

	f, err := os.CreateTemp("", "hub-index-*.db")
	if err != nil {
		t.Fatalf("temp file: %v", err)
	}
	f.Close()

	db, err := sql.Open("sqlite3", f.Name())
	if err != nil {
		os.Remove(f.Name())
		t.Fatalf("open sqlite: %v", err)
	}
	if _, err := db.Exec("PRAGMA synchronous = OFF"); err != nil {
		t.Fatalf("pragma: %v", err)
	}

	idx, err := NewHubIndex(db, zerolog.Nop())
	if err != nil {
		db.Close()
		os.Remove(f.Name())
		t.Fatalf("new hub index: %v", err)
	}
	t.Cleanup(func() {
		db.Close()
		os.Remove(f.Name())
	})
	return idx
}

// newTestReconciler returns a reconciler plus the backend behind it, so a test
// can delete a file out from under the index.
func newTestReconciler(t *testing.T) (*Reconciler, *HubIndex) {
	t.Helper()
	rec, idx, _ := newTestReconcilerWithBackend(t)
	return rec, idx
}

func newTestReconcilerWithBackend(t *testing.T) (*Reconciler, *HubIndex, storage.Backend) {
	t.Helper()
	idx := newTestHubIndex(t)
	dir, err := os.MkdirTemp("", "reconcile-backend-*")
	if err != nil {
		t.Fatalf("temp dir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	backend := newLocalBackendForTest(t, dir)

	rec, err := NewReconciler(ReconcilerConfig{Index: idx, Backend: backend})
	if err != nil {
		t.Fatalf("new reconciler: %v", err)
	}
	return rec, idx, backend
}

// seedFile records a file in the index AND writes it to storage, so reconcile's
// existence confirmation finds it.
func seedFile(t *testing.T, idx *HubIndex, backend storage.Backend, spokeID, path, sha string) {
	t.Helper()
	recordFile(t, idx, spokeID, path, sha)
	if err := backend.Write(context.Background(), NamespacedPath(spokeID, path), []byte("x")); err != nil {
		t.Fatalf("seed storage %s: %v", path, err)
	}
}

func recordFile(t *testing.T, idx *HubIndex, spokeID, path, sha string) {
	t.Helper()
	if err := idx.Record(context.Background(), &ReceivedRecord{
		SpokeID:    spokeID,
		SourcePath: path,
		HubPath:    NamespacedPath(spokeID, path),
		SHA256:     sha,
		SizeBytes:  100,
	}); err != nil {
		t.Fatalf("record %s: %v", path, err)
	}
}

func TestReconcile_PartitionsTheBatch(t *testing.T) {
	ctx := context.Background()
	rec, idx, backend := newTestReconcilerWithBackend(t)

	missing := ReconcileEntry{Path: "metrics/cpu/2026/08/07/14/a.parquet", SHA256: sha256Hex([]byte("a"))}
	present := ReconcileEntry{Path: "metrics/cpu/2026/08/07/14/b.parquet", SHA256: sha256Hex([]byte("b"))}
	conflict := ReconcileEntry{Path: "metrics/cpu/2026/08/07/14/c.parquet", SHA256: sha256Hex([]byte("spoke-version"))}

	seedFile(t, idx, backend, "rocket-01", present.Path, present.SHA256)
	seedFile(t, idx, backend, "rocket-01", conflict.Path, sha256Hex([]byte("hub-version")))

	res, err := rec.Reconcile(ctx, "rocket-01", []ReconcileEntry{missing, present, conflict})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if err := res.Validate(); err != nil {
		t.Fatalf("the hub built a result its own validator rejects: %v", err)
	}

	if len(res.Missing) != 1 || res.Missing[0] != missing.Path {
		t.Errorf("missing = %v, want [%s]", res.Missing, missing.Path)
	}
	if len(res.Present) != 1 || res.Present[0] != present.Path {
		t.Errorf("present = %v, want [%s]", res.Present, present.Path)
	}
	if len(res.Conflicts) != 1 {
		t.Fatalf("conflicts = %v, want one", res.Conflicts)
	}
	// The hub's digest is the operator's only evidence for telling a
	// spoke_id collision from corruption; echoing the spoke's own would make
	// the conflict unactionable.
	if res.Conflicts[0].TheirSHA256 == conflict.SHA256 {
		t.Error("the conflict reports the spoke's digest; it must report the hub's")
	}
	if res.Conflicts[0].TheirSHA256 != sha256Hex([]byte("hub-version")) {
		t.Errorf("TheirSHA256 = %q, want the hub's", res.Conflicts[0].TheirSHA256)
	}
}

func TestReconcile_IsSpokeScoped(t *testing.T) {
	ctx := context.Background()
	rec, idx, backend := newTestReconcilerWithBackend(t)

	// Two rockets legitimately produce the same native path. One having sent
	// it must not make the hub claim it holds the other's.
	const p = "metrics/cpu/2026/08/07/14/a.parquet"
	sha := sha256Hex([]byte("payload"))
	seedFile(t, idx, backend, "rocket-01", p, sha)

	res, err := rec.Reconcile(ctx, "rocket-02", []ReconcileEntry{{Path: p, SHA256: sha}})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if len(res.Present) != 0 {
		t.Errorf("present = %v, want empty — one spoke's file was credited to another", res.Present)
	}
	if len(res.Missing) != 1 {
		t.Errorf("missing = %v, want the file", res.Missing)
	}
}

func TestReconcile_LostAckRecovery(t *testing.T) {
	ctx := context.Background()
	rec, idx, backend := newTestReconcilerWithBackend(t)

	// The whole point of `present`: a transfer completed but its ack never
	// arrived, so the spoke still believes the file is pending. Reconcile
	// tells it the truth in bulk, and it advances without re-sending bytes.
	entries := make([]ReconcileEntry, 0, 50)
	for i := 0; i < 50; i++ {
		p := fmt.Sprintf("metrics/cpu/2026/08/07/14/f_%d.parquet", i)
		sha := sha256Hex([]byte(fmt.Sprintf("payload %d", i)))
		seedFile(t, idx, backend, "rocket-01", p, sha)
		entries = append(entries, ReconcileEntry{Path: p, SHA256: sha})
	}

	res, err := rec.Reconcile(ctx, "rocket-01", entries)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if len(res.Present) != 50 {
		t.Errorf("present = %d, want all 50 — the spoke would re-send files the hub already holds", len(res.Present))
	}
	if len(res.Missing) != 0 {
		t.Errorf("missing = %d, want 0", len(res.Missing))
	}
}

func TestReconcile_RejectsOversizedBatch(t *testing.T) {
	ctx := context.Background()
	idx := newTestHubIndex(t)
	rec, err := NewReconciler(ReconcilerConfig{Index: idx, Backend: newLocalBackendForTest(t, t.TempDir()), MaxEntries: 10})
	if err != nil {
		t.Fatalf("new reconciler: %v", err)
	}

	// The cap exists because the request body is buffered before auth runs;
	// an unbounded batch is a pre-auth memory claim.
	entries := make([]ReconcileEntry, 11)
	for i := range entries {
		entries[i] = ReconcileEntry{
			Path:   fmt.Sprintf("metrics/cpu/2026/08/07/14/f_%d.parquet", i),
			SHA256: sha256Hex([]byte(fmt.Sprint(i))),
		}
	}

	_, err = rec.Reconcile(ctx, "rocket-01", entries)
	if !errors.Is(err, ErrReconcileTooLarge) {
		t.Errorf("err = %v, want ErrReconcileTooLarge", err)
	}

	// Exactly at the cap must be accepted — an off-by-one here would make the
	// documented limit a lie.
	if _, err := rec.Reconcile(ctx, "rocket-01", entries[:10]); err != nil {
		t.Errorf("a batch exactly at the cap was rejected: %v", err)
	}
}

func TestReconcile_RejectsMalformedEntries(t *testing.T) {
	ctx := context.Background()
	rec, _ := newTestReconciler(t)
	good := sha256Hex([]byte("payload"))

	// A spoke is semi-trusted, so its declared paths are untrusted input even
	// though the batch is HMAC-signed: the MAC proves who is asking, not that
	// what they ask is well-formed.
	tests := []struct {
		name  string
		entry ReconcileEntry
	}{
		{"traversal", ReconcileEntry{Path: "../../../etc/passwd.parquet", SHA256: good}},
		{"absolute", ReconcileEntry{Path: "/etc/passwd.parquet", SHA256: good}},
		{"staging prefix", ReconcileEntry{Path: ".sync-staging/rocket-02/f.parquet", SHA256: good}},
		{"not parquet", ReconcileEntry{Path: "metrics/cpu/evil.sh", SHA256: good}},
		{"empty path", ReconcileEntry{Path: "", SHA256: good}},
		{"digest not hex", ReconcileEntry{Path: "metrics/cpu/2026/08/07/14/a.parquet", SHA256: strings.Repeat("z", 64)}},
		{"digest wrong length", ReconcileEntry{Path: "metrics/cpu/2026/08/07/14/a.parquet", SHA256: "abc"}},
		{"digest empty", ReconcileEntry{Path: "metrics/cpu/2026/08/07/14/a.parquet", SHA256: ""}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := rec.Reconcile(ctx, "rocket-01", []ReconcileEntry{tt.entry}); err == nil {
				t.Errorf("entry %+v was accepted", tt.entry)
			}
		})
	}
}

func TestReconcile_RejectsDuplicatePaths(t *testing.T) {
	ctx := context.Background()
	rec, _ := newTestReconciler(t)

	// The same path cannot be both present and conflicted, so a duplicate has
	// no correct answer and would produce a result ReconcileResult.Validate
	// rejects. Refusing the batch is clearer than answering it arbitrarily.
	const p = "metrics/cpu/2026/08/07/14/a.parquet"
	entries := []ReconcileEntry{
		{Path: p, SHA256: sha256Hex([]byte("one"))},
		{Path: p, SHA256: sha256Hex([]byte("two"))},
	}
	_, err := rec.Reconcile(ctx, "rocket-01", entries)
	if err == nil {
		t.Fatal("a batch with a duplicate path was accepted")
	}
	// Assert WHICH guard fired. The input check and the output res.Validate()
	// both reject this, so testing only `err != nil` lets either be deleted
	// silently — they mask each other. The input check is the one that should
	// fire: rejecting a malformed request beats building a result and then
	// discovering it is invalid.
	if !strings.Contains(err.Error(), "twice") {
		t.Errorf("err = %v; want the input duplicate check to reject it, not the output validator", err)
	}
}

func TestReconcile_RejectsMaliciousSpokeIDs(t *testing.T) {
	ctx := context.Background()
	rec, _ := newTestReconciler(t)
	entry := ReconcileEntry{Path: "metrics/cpu/2026/08/07/14/a.parquet", SHA256: sha256Hex([]byte("p"))}

	for _, id := range []string{"", "..", "rocket/../other", ".sync-staging", "rocket\x00-01"} {
		t.Run(id, func(t *testing.T) {
			if _, err := rec.Reconcile(ctx, id, []ReconcileEntry{entry}); err == nil {
				t.Errorf("spoke ID %q was accepted", id)
			}
		})
	}
}

func TestReconcile_EmptyBatchIsValid(t *testing.T) {
	ctx := context.Background()
	rec, _ := newTestReconciler(t)

	// A spoke with nothing pending still reconciles — it is how it discovers
	// files the hub holds that its own ledger has lost.
	res, err := rec.Reconcile(ctx, "rocket-01", nil)
	if err != nil {
		t.Fatalf("empty batch: %v", err)
	}
	if len(res.Missing) != 0 || len(res.Present) != 0 || len(res.Conflicts) != 0 {
		t.Errorf("an empty batch produced a non-empty result: %+v", res)
	}
}

func TestReconcile_LookupChunksBeyondSQLiteParameterLimit(t *testing.T) {
	ctx := context.Background()
	idx := newTestHubIndex(t)
	backend := newLocalBackendForTest(t, t.TempDir())
	rec, err := NewReconciler(ReconcilerConfig{Index: idx, Backend: backend, MaxEntries: 5000})
	if err != nil {
		t.Fatalf("new reconciler: %v", err)
	}

	// SQLite's default parameter limit is 999. A naive IN (?,?,...) over a
	// realistic backlog would blow past it, so Lookup chunks — this proves the
	// chunking both works and stitches results back together.
	const n = 2500
	entries := make([]ReconcileEntry, 0, n)
	for i := 0; i < n; i++ {
		p := fmt.Sprintf("metrics/cpu/2026/08/07/14/f_%d.parquet", i)
		sha := sha256Hex([]byte(fmt.Sprint(i)))
		entries = append(entries, ReconcileEntry{Path: p, SHA256: sha})
		if i%2 == 0 {
			seedFile(t, idx, backend, "rocket-01", p, sha)
		}
	}

	res, err := rec.Reconcile(ctx, "rocket-01", entries)
	if err != nil {
		t.Fatalf("reconcile %d entries: %v", n, err)
	}
	if len(res.Present) != n/2 {
		t.Errorf("present = %d, want %d", len(res.Present), n/2)
	}
	if len(res.Missing) != n/2 {
		t.Errorf("missing = %d, want %d", len(res.Missing), n/2)
	}
}

func TestHubIndex_RecordIsIdempotentAndUpdates(t *testing.T) {
	ctx := context.Background()
	idx := newTestHubIndex(t)

	const p = "metrics/cpu/2026/08/07/14/a.parquet"
	recordFile(t, idx, "rocket-01", p, sha256Hex([]byte("v1")))
	recordFile(t, idx, "rocket-01", p, sha256Hex([]byte("v1")))

	n, err := idx.CountForSpoke(ctx, "rocket-01")
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 1 {
		t.Errorf("count = %d, want 1 — re-recording created a duplicate row", n)
	}

	// A legitimate replacement updates the digest. Only verified content
	// reaches Record, so a differing digest here means the file was genuinely
	// replaced, not that two spokes collided.
	recordFile(t, idx, "rocket-01", p, sha256Hex([]byte("v2")))
	held, err := idx.Lookup(ctx, "rocket-01", []string{p})
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if held[p].SHA256 != sha256Hex([]byte("v2")) {
		t.Errorf("digest = %q, want the updated one", held[p].SHA256)
	}
}

func TestHubIndex_ForgetStopsReportingPresent(t *testing.T) {
	ctx := context.Background()
	rec, idx, backend := newTestReconcilerWithBackend(t)

	const p = "metrics/cpu/2026/08/07/14/a.parquet"
	sha := sha256Hex([]byte("payload"))
	seedFile(t, idx, backend, "rocket-01", p, sha)

	if err := idx.Forget(ctx, "rocket-01", p); err != nil {
		t.Fatalf("forget: %v", err)
	}

	// Critical: after retention deletes a file from hub storage, reconcile
	// must stop calling it present. Otherwise the spoke marks it synced and
	// could delete its only copy of data the hub no longer has.
	res, err := rec.Reconcile(ctx, "rocket-01", []ReconcileEntry{{Path: p, SHA256: sha}})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if len(res.Present) != 0 {
		t.Error("a forgotten file is still reported as present; the spoke could delete its only copy")
	}
	if len(res.Missing) != 1 {
		t.Errorf("missing = %v, want the forgotten file", res.Missing)
	}
}

func TestHubIndex_RequiresDB(t *testing.T) {
	if _, err := NewHubIndex(nil, zerolog.Nop()); err == nil {
		t.Error("a nil database was accepted")
	}
}

func TestNewReconciler_RequiresIndex(t *testing.T) {
	if _, err := NewReconciler(ReconcilerConfig{}); err == nil {
		t.Error("a reconciler was built without an index")
	}
}

func TestReceiver_RecordsCommittedFilesForReconcile(t *testing.T) {
	ctx := context.Background()

	dir, err := os.MkdirTemp("", "sync-recv-idx-*")
	if err != nil {
		t.Fatalf("temp dir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })

	backend := newLocalBackendForTest(t, dir)
	idx := newTestHubIndex(t)

	r, err := NewReceiver(ReceiverConfig{Backend: backend, Index: idx, Logger: zerolog.Nop()})
	if err != nil {
		t.Fatalf("receiver: %v", err)
	}
	rec, err := NewReconciler(ReconcilerConfig{Index: idx, Backend: backend})
	if err != nil {
		t.Fatalf("reconciler: %v", err)
	}

	// End to end: a file that was actually received must then reconcile as
	// present. Without this wiring the hub would report every file missing and
	// spokes would re-send their whole backlog on every contact.
	content := []byte("parquet payload")
	digest := sha256Hex(content)
	if _, err := r.Receive(ctx, "rocket-01", testPath, digest, int64(len(content)), 0, bytes.NewReader(content)); err != nil {
		t.Fatalf("receive: %v", err)
	}

	res, err := rec.Reconcile(ctx, "rocket-01", []ReconcileEntry{{Path: testPath, SHA256: digest}})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if len(res.Present) != 1 {
		t.Errorf("present = %v, want the received file — receive did not record it", res.Present)
	}
}

// newLocalBackendForTest builds a local backend rooted at dir.
func newLocalBackendForTest(t *testing.T, dir string) storage.Backend {
	t.Helper()
	b, err := storage.NewLocalBackend(dir, zerolog.Nop())
	if err != nil {
		t.Fatalf("local backend: %v", err)
	}
	t.Cleanup(func() { b.Close() })
	return b
}

func TestReconcile_NeverVouchesForADeletedFile(t *testing.T) {
	ctx := context.Background()
	rec, idx, backend := newTestReconcilerWithBackend(t)

	// The index records what the hub RECEIVED; it never learns about
	// deletions. Arc retention lists on an operator-chosen `policy.Database +
	// "/"` prefix (api/retention.go), so a policy naming a spoke ID sweeps
	// that spoke's namespace — and cold-tier migration and a human with rm do
	// the same thing.
	//
	// If reconcile trusted the index alone it would report the file present,
	// the spoke would mark it synced per §5.1, and a spoke reclaiming space
	// would delete its only copy. Silent, permanent data loss with no attacker
	// involved.
	const p = "metrics/cpu/2026/08/07/14/only_copy.parquet"
	sha := sha256Hex([]byte("the spoke's only copy"))
	seedFile(t, idx, backend, "rocket-01", p, sha)

	if err := backend.Delete(ctx, NamespacedPath("rocket-01", p)); err != nil {
		t.Fatalf("delete: %v", err)
	}

	res, err := rec.Reconcile(ctx, "rocket-01", []ReconcileEntry{{Path: p, SHA256: sha}})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if len(res.Present) != 0 {
		t.Fatalf("present = %v — the hub vouched for a file it no longer holds", res.Present)
	}
	if len(res.Missing) != 1 || res.Missing[0] != p {
		t.Errorf("missing = %v, want the deleted file so the spoke re-sends it", res.Missing)
	}

	// The stale row is dropped, so the next reconcile skips the stat rather
	// than re-confirming a file that is gone.
	held, err := idx.Lookup(ctx, "rocket-01", []string{p})
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if _, stillThere := held[p]; stillThere {
		t.Error("the stale index row survived; every future reconcile pays a wasted stat")
	}
}

// failingExistsBackend fails the existence check for one specific path.
type failingExistsBackend struct {
	storage.Backend
	failFor string
}

func (f *failingExistsBackend) Exists(ctx context.Context, p string) (bool, error) {
	if strings.Contains(p, f.failFor) {
		return false, errors.New("simulated backend failure")
	}
	return f.Backend.Exists(ctx, p)
}

func TestReconcile_ExistenceFailureRejectsTheWholeBatch(t *testing.T) {
	ctx := context.Background()
	idx := newTestHubIndex(t)
	local := newLocalBackendForTest(t, t.TempDir())
	backend := &failingExistsBackend{Backend: local, failFor: "unlucky"}

	rec, err := NewReconciler(ReconcilerConfig{Index: idx, Backend: backend})
	if err != nil {
		t.Fatalf("reconciler: %v", err)
	}

	// One entry's confirmation fails; the rest would succeed.
	entries := make([]ReconcileEntry, 0, 3)
	for _, name := range []string{"fine_a", "unlucky", "fine_b"} {
		p := fmt.Sprintf("metrics/cpu/2026/08/07/14/%s.parquet", name)
		sha := sha256Hex([]byte(name))
		seedFile(t, idx, local, "rocket-01", p, sha)
		entries = append(entries, ReconcileEntry{Path: p, SHA256: sha})
	}

	// The whole batch must fail rather than return a partial answer. A
	// partial result would be indistinguishable from a complete one: the spoke
	// cannot tell "the hub does not have this" from "the hub could not check",
	// and treating the second as the first means re-sending files needlessly —
	// or worse, if the polarity ever flipped, marking unsent files as synced.
	_, err = rec.Reconcile(ctx, "rocket-01", entries)
	if err == nil {
		t.Fatal("a failed existence check produced a result instead of an error")
	}
	if !errors.Is(err, ErrReceiveInternal) {
		t.Errorf("err = %v, want ErrReceiveInternal so the handler answers 503 and the spoke retries", err)
	}
}

// #619 receipt integrity: a receipt whose FILE the hub's own compaction
// consumed is exempt from the existence check — reconcile keeps answering
// present (the content lives inside a compacted output), and the receipt is
// never forgotten. Without the exemption, the stale-sweep would forget it
// and the spoke would re-upload the raw next to the compacted output.
func TestReconcile_CompactedReceiptsStayPresent(t *testing.T) {
	ctx := context.Background()
	rec, idx, backend := newTestReconcilerWithBackend(t)

	const p = "metrics/cpu/2026/08/07/14/f.parquet"
	sha := sha256Hex([]byte("payload"))
	if err := backend.Write(ctx, "rocket-01/"+p, []byte("payload")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := idx.Record(ctx, &ReceivedRecord{
		SpokeID: "rocket-01", SourcePath: p, HubPath: "rocket-01/" + p,
		SHA256: sha, SizeBytes: 7,
	}); err != nil {
		t.Fatalf("record: %v", err)
	}

	// Compaction consumes the file: mark, then delete.
	if err := idx.MarkCompacted(ctx, "rocket-01", []string{p}); err != nil {
		t.Fatalf("mark: %v", err)
	}
	if err := backend.Delete(ctx, "rocket-01/"+p); err != nil {
		t.Fatalf("delete: %v", err)
	}

	res, err := rec.Reconcile(ctx, "rocket-01", []ReconcileEntry{{Path: p, SHA256: sha, SizeBytes: 7}})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if len(res.Present) != 1 || res.Present[0] != p {
		t.Fatalf("present = %v; a compacted receipt must stay present", res.Present)
	}
	if len(res.Missing) != 0 || len(res.Conflicts) != 0 {
		t.Fatalf("missing=%v conflicts=%v, want none", res.Missing, res.Conflicts)
	}

	// The receipt survived (was not forgotten by the stale sweep).
	held, err := idx.Lookup(ctx, "rocket-01", []string{p})
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if hf, ok := held[p]; !ok || !hf.Compacted {
		t.Fatalf("receipt = %+v, want present and compacted", held)
	}

	// Different content at a compacted path is still a conflict.
	res, err = rec.Reconcile(ctx, "rocket-01", []ReconcileEntry{{Path: p, SHA256: sha256Hex([]byte("other")), SizeBytes: 5}})
	if err != nil {
		t.Fatalf("reconcile 2: %v", err)
	}
	if len(res.Conflicts) != 1 {
		t.Fatalf("conflicts = %v, want 1", res.Conflicts)
	}
}

// MarkCompacted is an UPDATE, never an INSERT: unknown paths and foreign
// namespaces no-op, and a verified re-receive (legitimate replacement)
// clears the mark.
func TestMarkCompacted_UpdateOnlyAndClearedByRecord(t *testing.T) {
	ctx := context.Background()
	idx := newTestHubIndex(t)

	// Unknown path: no receipt appears.
	if err := idx.MarkCompacted(ctx, "rocket-01", []string{"metrics/cpu/none.parquet"}); err != nil {
		t.Fatalf("mark unknown: %v", err)
	}
	held, err := idx.Lookup(ctx, "rocket-01", []string{"metrics/cpu/none.parquet"})
	if err != nil || len(held) != 0 {
		t.Fatalf("phantom receipt: %v %v", held, err)
	}

	const p = "metrics/cpu/2026/08/07/14/f.parquet"
	if err := idx.Record(ctx, &ReceivedRecord{
		SpokeID: "rocket-01", SourcePath: p, HubPath: "rocket-01/" + p,
		SHA256: "aa", SizeBytes: 2,
	}); err != nil {
		t.Fatalf("record: %v", err)
	}
	if err := idx.MarkCompacted(ctx, "rocket-01", []string{p}); err != nil {
		t.Fatalf("mark: %v", err)
	}
	// Idempotent re-fire (recovery path).
	if err := idx.MarkCompacted(ctx, "rocket-01", []string{p}); err != nil {
		t.Fatalf("re-mark: %v", err)
	}
	held, _ = idx.Lookup(ctx, "rocket-01", []string{p})
	if !held[p].Compacted {
		t.Fatal("mark did not stick")
	}

	// A verified replacement clears the mark: the path holds a real file again.
	if err := idx.Record(ctx, &ReceivedRecord{
		SpokeID: "rocket-01", SourcePath: p, HubPath: "rocket-01/" + p,
		SHA256: "bb", SizeBytes: 2,
	}); err != nil {
		t.Fatalf("re-record: %v", err)
	}
	held, _ = idx.Lookup(ctx, "rocket-01", []string{p})
	if held[p].Compacted {
		t.Fatal("re-record left the compacted mark in place")
	}
}
