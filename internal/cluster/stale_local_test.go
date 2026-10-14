package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

type staleRigT struct {
	t    *testing.T
	c    *Coordinator
	root string
}

// staleRig: a local-storage node with the stale-local sweep set up as
// startStaleLocalLocked would (no loops: tests run passes), confirmations
// immediate unless a test says otherwise.
func staleRig(t *testing.T) *staleRigT {
	t.Helper()
	c := originRig(t, "reader-1", RoleReader)
	root := t.TempDir()
	b, err := storage.NewLocalBackend(root, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	c.storage = b
	c.cfg.ReplicationEnabled = true
	c.cfg.ReplicationCatchUpEnabled = true
	c.cfg.StaleLocalSweep = true
	c.cfg.RaftDataDir = t.TempDir()
	c.raftFSM = c.raftNode.FSM()
	seen, err := openSeenLocal(c.cfg.RaftDataDir, c.cfg.ClusterName)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(seen.close)
	c.stale.seen.Store(seen)
	c.stale.base = root
	c.stale.feed = make(chan string, staleLocalFeedQueue)
	c.stale.firstUnlisted = map[string]time.Time{}
	c.stale.every, c.stale.confirm = time.Hour, 0
	c.SetStaleLocalPending(nil)
	return &staleRigT{t: t, c: c, root: root}
}

const staleHour = "testdb/cpu/2026/10/14/03/"

func (r *staleRigT) write(key, body string) {
	r.t.Helper()
	full := filepath.Join(r.root, key)
	if err := os.MkdirAll(filepath.Dir(full), 0o700); err != nil {
		r.t.Fatal(err)
	}
	if err := os.WriteFile(full, []byte(body), 0o600); err != nil {
		r.t.Fatal(err)
	}
}

func (r *staleRigT) register(key, body string) {
	r.t.Helper()
	r.write(key, body)
	e := raft.FileEntry{Path: key, SHA256: strings.Repeat("a", 64), SizeBytes: int64(len(body)), Database: "testdb",
		Measurement: "cpu", OriginNodeID: "writer-1", Tier: "hot", CreatedAt: time.Now()}
	if err := r.c.raftNode.RegisterFile(e, 5*time.Second); err != nil {
		r.t.Fatal(err)
	}
}

// unregister deletes key from the manifest; no delete callback is wired, as
// on a node that was down when the delete applied.
func (r *staleRigT) unregister(key string) {
	r.t.Helper()
	raw, _ := json.Marshal(raft.DeleteFilePayload{Path: key, Reason: "retention"})
	if err := r.c.raftNode.Apply(&raft.Command{Type: raft.CommandDeleteFile, Payload: raw}, 5*time.Second); err != nil {
		r.t.Fatal(err)
	}
}

func (r *staleRigT) exists(key string) bool {
	_, err := os.Stat(filepath.Join(r.root, key))
	return err == nil
}

// walk is the replication walk's hook over the whole manifest.
func (r *staleRigT) walk() {
	for _, e := range r.c.raftNode.FSM().GetAllFiles() {
		r.c.observeListed(e)
	}
}

// pass: the replication walk, then a sweep pass (as they run on a node).
func (r *staleRigT) pass() {
	r.walk()
	r.c.staleLocalPass(context.Background())
}

// A delete this node missed: the copy it saw registered goes, on the second
// pass after the manifest dropped it, and leaves the set.
func TestStaleLocalRemovesAMissedDelete(t *testing.T) {
	r := staleRig(t)
	rec := &fakeTierRecorder{}
	r.c.SetTierRecorder(rec)
	key := staleHour + "a.parquet"
	keep := staleHour + "b.parquet"
	r.register(key, "rows a")
	r.register(keep, "rows b")
	r.pass()
	if _, ok := r.c.stale.seen.Load().get(key); !ok {
		t.Fatal("a registered local file is not tracked")
	}
	r.unregister(key)
	r.pass()
	if !r.exists(key) {
		t.Fatal("removed on the first pass that found it unlisted")
	}
	r.pass()
	if r.exists(key) {
		t.Fatal("a copy the cluster deleted is still on disk")
	}
	if !r.exists(keep) {
		t.Fatal("a registered copy was removed")
	}
	if _, ok := r.c.stale.seen.Load().get(key); ok {
		t.Fatal("a removed copy is still tracked")
	}
	if left, _ := filepath.Glob(filepath.Join(r.root, staleLocalAsideDir, "*")); len(left) != 0 {
		t.Fatalf("copies left aside: %v", left)
	}
	if st := r.c.StaleLocalStatus(); st.Unlinked != 1 || st.Tracked != 1 {
		t.Fatalf("status %+v", st)
	}
	// Reported to tiering with a reason it treats as possibly a migration.
	if got := rec.unlinkReports(); len(got) != 1 || got[0] != (unlinkReport{key, "stale-local", 6}) ||
		UnlinkReasonStaleLocal != "stale-local" {
		t.Fatalf("tier reports %+v", got)
	}
}

// A file this node never saw registered is never removed: one on disk the
// manifest never listed, and one listed while it was not here yet.
func TestStaleLocalLeavesFilesNeverSeenRegistered(t *testing.T) {
	r := staleRig(t)
	never := staleHour + "never.parquet"
	r.write(never, "unregistered rows")
	late := staleHour + "late.parquet"
	e := raft.FileEntry{Path: late, SHA256: strings.Repeat("b", 64), SizeBytes: 9, Database: "testdb",
		Measurement: "cpu", OriginNodeID: "writer-1", Tier: "hot", CreatedAt: time.Now()}
	if err := r.c.raftNode.RegisterFile(e, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	wrong := staleHour + "wrong.parquet"
	r.register(wrong, "rows")
	if err := os.WriteFile(filepath.Join(r.root, wrong), []byte("other size rows"), 0o600); err != nil {
		t.Fatal(err)
	}
	r.pass()
	r.unregister(late)
	r.write(late, "late rows") // arrives after the manifest dropped it
	r.unregister(wrong)
	r.pass()
	r.pass()
	for _, k := range []string{never, late, wrong} {
		if !r.exists(k) {
			t.Fatalf("%s removed", k)
		}
	}
}

// Each part of the identity tells another file apart on its own.
func TestStaleLocalIdentityParts(t *testing.T) {
	for name, change := range map[string]func(t *testing.T, full string){
		"inode": func(t *testing.T, full string) {
			// Written while the old file still exists, so the inode differs
			// (a file system may reuse a freed inode number at once).
			fi, _ := os.Stat(full)
			_ = os.WriteFile(full+".new", []byte("rows a"), 0o600)
			_ = os.Chtimes(full+".new", fi.ModTime(), fi.ModTime())
			if err := os.Rename(full+".new", full); err != nil {
				t.Fatal(err)
			}
			if nf, _ := os.Stat(full); fileInode(nf) == fileInode(fi) {
				t.Skip("same inode")
			}
		},
		"size": func(t *testing.T, full string) {
			fi, _ := os.Stat(full)
			f, _ := os.OpenFile(full, os.O_WRONLY|os.O_APPEND, 0o600)
			_, _ = f.WriteString("more")
			_ = f.Close()
			_ = os.Chtimes(full, fi.ModTime(), fi.ModTime())
		},
	} {
		t.Run(name, func(t *testing.T) {
			r := staleRig(t)
			key := staleHour + "a.parquet"
			r.register(key, "rows a")
			r.pass()
			r.unregister(key)
			r.pass()
			change(t, filepath.Join(r.root, key))
			r.pass()
			if !r.exists(key) {
				t.Fatal("another file at the path was removed")
			}
		})
	}
}

// Another file at the path (a restore, a pull of a re-registered path) is
// never removed, and is no longer tracked.
func TestStaleLocalKeepsAReplacedFile(t *testing.T) {
	r := staleRig(t)
	key := staleHour + "a.parquet"
	r.register(key, "rows a")
	r.pass()
	r.unregister(key)
	r.pass()
	full := filepath.Join(r.root, key)
	if err := os.Remove(full); err != nil {
		t.Fatal(err)
	}
	r.write(key, "rows a") // same size, new file
	r.pass()
	if !r.exists(key) {
		t.Fatal("a file written to the path after the delete was removed")
	}
	if _, ok := r.c.stale.seen.Load().get(key); ok {
		t.Fatal("a replaced file is still tracked")
	}
}

// Same size and inode, rewritten in place: the mtime tells them apart.
func TestStaleLocalKeepsAFileRewrittenInPlace(t *testing.T) {
	r := staleRig(t)
	key := staleHour + "a.parquet"
	r.register(key, "rows a")
	r.pass()
	r.unregister(key)
	r.pass()
	full := filepath.Join(r.root, key)
	later := time.Now().Add(time.Hour)
	if err := os.Chtimes(full, later, later); err != nil {
		t.Fatal(err)
	}
	r.pass()
	if !r.exists(key) {
		t.Fatal("a file modified after it was seen was removed")
	}
}

// Paths waiting to register here, held by a producer, cold on this node, or
// at an index the manifest has not applied yet are kept.
func TestStaleLocalKeepsHeldPaths(t *testing.T) {
	r := staleRig(t)
	pend := staleHour + "pending.parquet"
	held := staleHour + "held.parquet"
	cold := staleHour + "cold.parquet"
	ahead := staleHour + "ahead.parquet"
	for _, k := range []string{pend, held, cold, ahead} {
		r.register(k, "rows "+k)
	}
	r.pass()
	rec, _ := r.c.stale.seen.Load().get(ahead)
	rec.lsn = r.c.raftNode.FSM().AppliedDone() + 1000
	r.c.stale.seen.Load().add(ahead, rec)
	r.c.SetStaleLocalPending(func(p string) bool { return p == pend })
	r.c.SetColdPathCheck(func(_ context.Context, p string) bool { return p == cold })
	release := r.c.HoldLocalPaths([]string{held})
	for _, k := range []string{pend, held, cold, ahead} {
		r.unregister(k)
	}
	r.pass()
	r.pass()
	for _, k := range []string{pend, held, cold, ahead} {
		if !r.exists(k) {
			t.Fatalf("%s removed", k)
		}
	}
	release()
	r.pass()
	r.pass()
	if r.exists(held) {
		t.Fatal("a released path was not removed")
	}
	if !r.exists(pend) || !r.exists(cold) || !r.exists(ahead) {
		t.Fatal("a held path was removed after another was released")
	}
}

// A pending check that turns true between the pass's candidates and the
// unlink still keeps the file.
func TestStaleLocalRechecksPendingAtTheUnlink(t *testing.T) {
	r := staleRig(t)
	key := staleHour + "a.parquet"
	r.register(key, "rows a")
	r.pass()
	r.unregister(key)
	r.pass()
	r.c.SetStaleLocalPending(func(string) bool { return true })
	if r.c.unlinkStale(context.Background(), key) || !r.exists(key) {
		t.Fatal("unlinked a path waiting to register")
	}
}

// A path the manifest lists again before the unlink is kept.
func TestStaleLocalRechecksTheManifestAtTheUnlink(t *testing.T) {
	r := staleRig(t)
	key := staleHour + "a.parquet"
	r.register(key, "rows a")
	r.pass()
	r.unregister(key)
	r.pass()
	e := raft.FileEntry{Path: key, SHA256: strings.Repeat("a", 64), SizeBytes: 6, Database: "testdb",
		Measurement: "cpu", OriginNodeID: "writer-1", Tier: "hot", CreatedAt: time.Now()}
	if err := r.c.raftNode.RegisterFile(e, 5*time.Second); err != nil { // the file itself untouched
		t.Fatal(err)
	}
	if r.c.unlinkStale(context.Background(), key) || !r.exists(key) {
		t.Fatal("unlinked a listed path")
	}
}

// The confirmation interval: a path goes only once it has been unlisted for
// staleLocalConfirm.
func TestStaleLocalConfirmInterval(t *testing.T) {
	r := staleRig(t)
	r.c.stale.confirm = 10 * time.Minute
	key := staleHour + "a.parquet"
	r.register(key, "rows a")
	r.pass()
	r.unregister(key)
	now := time.Now()
	if got := r.c.staleCandidates(context.Background(), r.c.stale.seen.Load(), now); len(got) != 0 {
		t.Fatalf("confirmed on first sight: %v", got)
	}
	if got := r.c.staleCandidates(context.Background(), r.c.stale.seen.Load(), now.Add(9*time.Minute)); len(got) != 0 {
		t.Fatalf("confirmed early: %v", got)
	}
	if got := r.c.staleCandidates(context.Background(), r.c.stale.seen.Load(), now.Add(10*time.Minute)); len(got) != 1 {
		t.Fatalf("not confirmed after the interval: %v", got)
	}
	// Listed again: the clock restarts.
	r.register(key, "rows a")
	_ = r.c.staleCandidates(context.Background(), r.c.stale.seen.Load(), now.Add(11*time.Minute))
	r.unregister(key)
	if got := r.c.staleCandidates(context.Background(), r.c.stale.seen.Load(), now.Add(12*time.Minute)); len(got) != 0 {
		t.Fatalf("an unlisted path kept its old first sight: %v", got)
	}
}

// The breaker: a pass that would remove more than the allowed fraction (and
// at least staleLocalBreakerMin) removes nothing.
func TestStaleLocalBreaker(t *testing.T) {
	r := staleRig(t)
	keys := make([]string, 0, staleLocalBreakerMin+10)
	for i := 0; i < staleLocalBreakerMin+10; i++ {
		k := fmt.Sprintf("%sf%04d.parquet", staleHour, i)
		r.write(k, "rows")
		keys = append(keys, k)
	}
	ops := make([]raft.BatchFileOp, 0, len(keys))
	for _, k := range keys {
		raw, _ := json.Marshal(raft.RegisterFilePayload{File: raft.FileEntry{Path: k, SHA256: strings.Repeat("a", 64), SizeBytes: 4,
			Database: "testdb", Measurement: "cpu", OriginNodeID: "writer-1", Tier: "hot", CreatedAt: time.Now()}})
		ops = append(ops, raft.BatchFileOp{Type: raft.CommandRegisterFile, Payload: raw})
	}
	applyBatch(t, r.c, ops)
	r.pass()
	ops = ops[:0]
	for _, k := range keys {
		raw, _ := json.Marshal(raft.DeleteFilePayload{Path: k, Reason: "retention"})
		ops = append(ops, raft.BatchFileOp{Type: raft.CommandDeleteFile, Payload: raw})
	}
	applyBatch(t, r.c, ops)
	r.pass()
	r.pass()
	if st := r.c.StaleLocalStatus(); !st.Blocked || st.Unlinked != 0 {
		t.Fatalf("status %+v", st)
	}
	for _, k := range keys {
		if !r.exists(k) {
			t.Fatal("the breaker removed files")
		}
	}
	r.c.cfg.StaleLocalSweepMaxFraction = 1
	r.pass()
	if st := r.c.StaleLocalStatus(); st.Blocked || st.Unlinked != len(keys) {
		t.Fatalf("status with the limit raised %+v", st)
	}
}

func applyBatch(t *testing.T, c *Coordinator, ops []raft.BatchFileOp) {
	t.Helper()
	raw, _ := json.Marshal(raft.BatchFileOpsPayload{Ops: ops})
	if err := c.raftNode.Apply(&raft.Command{Type: raft.CommandBatchFileOps, Payload: raw}, 10*time.Second); err != nil {
		t.Fatal(err)
	}
}

// Nothing runs while a compaction pause (a restore) is in force, or before
// main has wired the pending check.
func TestStaleLocalWaitsForPauseAndWiring(t *testing.T) {
	r := staleRig(t)
	key := staleHour + "a.parquet"
	r.register(key, "rows a")
	r.pass()
	r.unregister(key)
	r.c.stale.ready.Store(false)
	r.pass()
	r.pass()
	if !r.exists(key) {
		t.Fatal("swept before the pending check was wired")
	}
	if _, ok := r.c.stale.firstUnlisted[key]; ok {
		t.Fatal("a pass ran before the pending check was wired")
	}
	r.c.SetStaleLocalPending(nil)
	now := time.Now().UTC()
	if err := r.c.raftNode.SetCompactionPause(raft.SetCompactionPausePayload{Paused: true, Generation: 1,
		RequestedBy: "writer-1", Reason: "restore", Now: now, ExpiresAt: now.Add(time.Hour)}, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	r.pass()
	r.pass()
	if !r.exists(key) {
		t.Fatal("swept during a compaction pause")
	}
	if err := r.c.raftNode.SetCompactionPause(raft.SetCompactionPausePayload{Paused: false, Generation: 1,
		RequestedBy: "writer-1", Now: time.Now().UTC()}, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	r.pass()
	if !r.exists(key) {
		t.Fatal("a pass during the pause counted as the first sight")
	}
	r.pass()
	if r.exists(key) {
		t.Fatal("not swept after the pause")
	}
}

// A hold waits for an unlink in progress, and an unlink skips a held path.
func TestHoldLocalPathsExcludesTheUnlink(t *testing.T) {
	r := staleRig(t)
	if !r.c.stale.beginUnlink("p") {
		t.Fatal("an unheld path refused")
	}
	got := make(chan struct{})
	go func() {
		release := r.c.HoldLocalPaths([]string{"p"})
		close(got)
		release()
	}()
	select {
	case <-got:
		t.Fatal("a hold did not wait for the unlink in progress")
	case <-time.After(50 * time.Millisecond):
	}
	r.c.stale.endUnlink("p")
	<-got
	release := r.c.HoldLocalPaths([]string{"p", "p"})
	if r.c.stale.beginUnlink("p") {
		t.Fatal("a held path was unlinked")
	}
	release()
	release() // idempotent
	if !r.c.stale.beginUnlink("p") {
		t.Fatal("a released path stays held")
	}
	r.c.stale.endUnlink("p")
}

// The delete workers and abandoned pulls forget what they removed; a pull
// feeds the set.
func TestStaleLocalFeedsAndForgets(t *testing.T) {
	r := staleRig(t)
	key := staleHour + "a.parquet"
	r.register(key, "rows a")
	if !r.c.recordPulledFileInTiering(key, 6) && len(r.c.stale.feed) != 1 {
		t.Fatal("a pulled file was not fed to the set")
	}
	p := <-r.c.stale.feed
	e, _ := r.c.raftNode.FSM().GetFile(p)
	r.c.observeLocal(e, true)
	if _, ok := r.c.stale.seen.Load().get(key); !ok {
		t.Fatal("not tracked")
	}
	r.c.recordAbandonedFileInTiering(key, 6)
	if _, ok := r.c.stale.seen.Load().get(key); ok {
		t.Fatal("an abandoned pull is still tracked")
	}
	r.c.observeLocal(e, true)
	r.c.unlinkOne(deleteRequest{path: key, reason: "retention"}, nil)
	if _, ok := r.c.stale.seen.Load().get(key); ok {
		t.Fatal("a copy the delete workers removed is still tracked")
	}
}

// Reserved roots are never tracked.
func TestStaleLocalIgnoresReservedRoots(t *testing.T) {
	for _, p := range []string{"_compaction_staged/j/x.parquet", "_refused/a/b.parquet", ".x/y", "", "../a", "/abs"} {
		if sweepablePath(p) {
			t.Fatalf("%q sweepable", p)
		}
	}
	if !sweepablePath(staleHour + "a.parquet") {
		t.Fatal("a data path is not sweepable")
	}
}

// Copies a crash left moved aside are removed at startup, except those kept
// for an operator.
func TestRemoveStaleAsides(t *testing.T) {
	r := staleRig(t)
	dir := filepath.Join(r.root, staleLocalAsideDir)
	_ = os.MkdirAll(dir, 0o700)
	aside := filepath.Join(dir, "abc")
	keep := filepath.Join(dir, staleLocalKeepAside+"abc")
	_ = os.WriteFile(aside, []byte("x"), 0o600)
	_ = os.WriteFile(keep, []byte("x"), 0o600)
	r.write(staleHour+"x.parquet", "x")
	r.c.removeStaleAsides()
	if _, err := os.Stat(aside); err == nil {
		t.Fatal("aside copy left")
	}
	if _, err := os.Stat(keep); err != nil {
		t.Fatal("a copy kept for an operator was removed")
	}
	if !r.exists(staleHour + "x.parquet") {
		t.Fatal("data removed")
	}
}

// A path registered again with other content while this node kept its old
// copy (a snapshot install fires no callback for it): the walk has the copy
// re-checked, once.
func TestStaleLocalRechecksARegisteredAgainCopy(t *testing.T) {
	r := staleRig(t)
	var verified []string
	fn := func(e *raft.FileEntry) { verified = append(verified, e.Path) }
	r.c.stale.verifyContent.Store(&fn)
	key := staleHour + "a.parquet"
	r.register(key, "rows a")
	r.walk()
	e := raft.FileEntry{Path: key, SHA256: strings.Repeat("c", 64), SizeBytes: 6, Database: "testdb",
		Measurement: "cpu", OriginNodeID: "writer-1", Tier: "hot", CreatedAt: time.Now()}
	r.unregister(key)
	if err := r.c.raftNode.RegisterFile(e, 5*time.Second); err != nil { // the old copy left in place
		t.Fatal(err)
	}
	r.walk()
	r.walk()
	if len(verified) != 1 || verified[0] != key {
		t.Fatalf("verified %v", verified)
	}
	// A new index with the same checksum (a tier update) is not re-checked.
	e.Tier = "hot"
	raw, _ := json.Marshal(raft.UpdateFilePayload{File: e})
	if err := r.c.raftNode.Apply(&raft.Command{Type: raft.CommandUpdateFile, Payload: raw}, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	r.walk()
	if len(verified) != 1 {
		t.Fatalf("a same-checksum update was re-checked: %v", verified)
	}
	// A copy replaced by a pull is recorded afresh, not re-checked.
	r.write(key+".tmp", "rows b")
	_ = os.Rename(filepath.Join(r.root, key+".tmp"), filepath.Join(r.root, key))
	r.unregister(key)
	if err := r.c.raftNode.RegisterFile(e, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	r.walk()
	if len(verified) != 1 {
		t.Fatalf("a replaced copy was re-checked: %v", verified)
	}
}

// Cold is checked again at the unlink, and a record whose file is gone is
// dropped.
func TestStaleLocalUnlinkRechecksColdAndDropsGone(t *testing.T) {
	r := staleRig(t)
	key := staleHour + "a.parquet"
	gone := staleHour + "g.parquet"
	r.register(key, "rows a")
	r.register(gone, "rows g")
	r.pass()
	r.unregister(key)
	r.unregister(gone)
	_ = os.Remove(filepath.Join(r.root, gone))
	r.pass()
	if _, ok := r.c.stale.seen.Load().get(gone); ok {
		t.Fatal("a record whose file is gone is still tracked")
	}
	r.c.SetColdPathCheck(func(context.Context, string) bool { return true })
	if r.c.unlinkStale(context.Background(), key) || !r.exists(key) {
		t.Fatal("unlinked a cold path")
	}
}

// Status: nothing before the sweep runs; the open error when the journal
// cannot be opened.
func TestStaleLocalStatusOnOpenFailure(t *testing.T) {
	r := staleRig(t)
	r.c.stale.status = nil
	if r.c.StaleLocalStatus() != nil {
		t.Fatal("status before any pass")
	}
	r.c.stale.seen.Store(nil)
	file := filepath.Join(t.TempDir(), "not-a-dir")
	_ = os.WriteFile(file, nil, 0o600)
	r.c.cfg.RaftDataDir = file
	r.c.stale.wg.Add(1)
	r.c.staleLocalRun(context.Background())
	st := r.c.StaleLocalStatus()
	if st == nil || !strings.Contains(st.Error, "tracking not started") || st.LastRun != nil {
		t.Fatalf("status %+v", st)
	}
}

// The sweep runs only where the delete workers own local unlinks.
func TestStaleLocalEnabled(t *testing.T) {
	r := staleRig(t)
	if !r.c.staleLocalEnabled() {
		t.Fatal("off on a local replica node")
	}
	for name, mod := range map[string]func(c *Coordinator){
		"sweep off":      func(c *Coordinator) { c.cfg.StaleLocalSweep = false },
		"no replication": func(c *Coordinator) { c.cfg.ReplicationEnabled = false },
		"catch-up off":   func(c *Coordinator) { c.cfg.ReplicationCatchUpEnabled = false },
		"no raft dir":    func(c *Coordinator) { c.cfg.RaftDataDir = "" },
		"object storage": func(c *Coordinator) { c.storage = &fakeObjectStore{LocalBackend: c.storage.(*storage.LocalBackend)} },
	} {
		cfg := *r.c.cfg
		st := r.c.storage
		mod(r.c)
		if r.c.staleLocalEnabled() {
			t.Fatalf("on with %s", name)
		}
		*r.c.cfg = cfg
		r.c.storage = st
	}
}

// A path that waited to register here starts its confirmation again once it
// no longer does.
func TestStaleLocalPendingRestartsTheClock(t *testing.T) {
	r := staleRig(t)
	key := staleHour + "a.parquet"
	r.register(key, "rows a")
	r.pass()
	r.unregister(key)
	pending := true
	r.c.SetStaleLocalPending(func(string) bool { return pending })
	r.pass()
	r.pass()
	pending = false
	r.pass()
	if !r.exists(key) {
		t.Fatal("removed on the first pass after it stopped waiting")
	}
	r.pass()
	if r.exists(key) {
		t.Fatal("not removed")
	}
}

// pauseRecorder applies a compaction pause when the first copy is reported
// removed: the rest of the pass must stop.
type pauseRecorder struct {
	fakeTierRecorder
	c *Coordinator
}

func (p *pauseRecorder) RecordUnlinkedFile(path, reason string, size int64) {
	p.fakeTierRecorder.RecordUnlinkedFile(path, reason, size)
	now := time.Now().UTC()
	_ = p.c.raftNode.SetCompactionPause(raft.SetCompactionPausePayload{Paused: true, Generation: 1,
		RequestedBy: "writer-1", Reason: "restore", Now: now, ExpiresAt: now.Add(time.Hour)}, 5*time.Second)
}

// A pause that begins during a pass stops its removals; the cap bounds them.
func TestStaleLocalPassStopsAtAPauseAndTheCap(t *testing.T) {
	r := staleRig(t)
	keys := []string{staleHour + "a.parquet", staleHour + "b.parquet", staleHour + "c.parquet", staleHour + "d.parquet"}
	for _, k := range keys {
		r.register(k, "rows")
	}
	r.pass()
	for _, k := range keys {
		r.unregister(k)
	}
	r.pass()
	old := staleLocalMaxPerPass
	staleLocalMaxPerPass = 2
	t.Cleanup(func() { staleLocalMaxPerPass = old })
	rec := &pauseRecorder{c: r.c}
	r.c.SetTierRecorder(rec)
	r.pass()
	if st := r.c.StaleLocalStatus(); st.Unlinked != 1 {
		t.Fatalf("removed %d after a pause began", st.Unlinked)
	}
	if err := r.c.raftNode.SetCompactionPause(raft.SetCompactionPausePayload{Paused: false, Generation: 1,
		RequestedBy: "writer-1", Now: time.Now().UTC()}, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	r.c.SetTierRecorder(&fakeTierRecorder{})
	r.pass()
	if st := r.c.StaleLocalStatus(); st.Unlinked != 2 {
		t.Fatalf("a pass removed %d with a cap of 2", st.Unlinked)
	}
}

// No pass acts without a manifest barrier: a node that cannot reach the
// leader may be behind.
func TestStaleLocalNeedsTheBarrier(t *testing.T) {
	r := staleRig(t)
	key := staleHour + "a.parquet"
	r.register(key, "rows a")
	r.pass()
	r.unregister(key)
	r.pass()
	r.c.cfg.ReplicationCatchUpBarrierTimeoutMs = 300
	_ = r.c.raftNode.Stop()
	r.pass()
	if !r.exists(key) {
		t.Fatal("removed without a barrier")
	}
	if st := r.c.StaleLocalStatus(); st.Error == "" {
		t.Fatalf("status %+v", st)
	}
}

// The sweep goroutine opens the journal and records what the FSM register
// callback hands it; after the stop, nothing is written.
func TestStaleLocalRunFeeds(t *testing.T) {
	r := staleRig(t)
	r.c.stale.seen.Store(nil)
	key := staleHour + "a.parquet"
	r.register(key, "rows a")
	ctx, cancel := context.WithCancel(context.Background())
	r.c.stale.wg.Add(1)
	go r.c.staleLocalRun(ctx)
	waitFSM(t, func() bool { return r.c.stale.seen.Load() != nil })
	r.c.noteLocalRegistered(key)
	waitFSM(t, func() bool { _, ok := r.c.stale.seen.Load().get(key); return ok })
	cancel()
	r.c.stopStaleLocal()
	r.c.forgetLocal(key) // a late delete worker: no panic, nothing written
	if _, ok := r.c.stale.seen.Load().get(key); !ok {
		t.Fatal("changed after the stop")
	}
}

// A pass skipped for a pause forgets first sightings: the copy goes only
// after two passes that ran.
func TestStaleLocalSkippedPassRestartsTheClock(t *testing.T) {
	r := staleRig(t)
	key := staleHour + "a.parquet"
	r.register(key, "rows a")
	r.pass()
	r.unregister(key)
	r.pass() // first sight
	now := time.Now().UTC()
	if err := r.c.raftNode.SetCompactionPause(raft.SetCompactionPausePayload{Paused: true, Generation: 1,
		RequestedBy: "writer-1", Reason: "restore", Now: now, ExpiresAt: now.Add(time.Hour)}, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	r.pass()
	if err := r.c.raftNode.SetCompactionPause(raft.SetCompactionPausePayload{Paused: false, Generation: 1,
		RequestedBy: "writer-1", Now: time.Now().UTC()}, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	r.pass()
	if !r.exists(key) {
		t.Fatal("confirmed across a skipped pass")
	}
	r.pass()
	if r.exists(key) {
		t.Fatal("not removed")
	}
}

// A tier update of a copy recorded at registration is not re-checked: after
// an upgrade the leader moves every legacy compaction output to "hot", and
// re-reading each one on every node would compete with queries.
func TestStaleLocalRetierIsNotReChecked(t *testing.T) {
	r := staleRig(t)
	var verified []string
	fn := func(e *raft.FileEntry) { verified = append(verified, e.Path) }
	r.c.stale.verifyContent.Store(&fn)
	key := staleHour + "a.parquet"
	r.register(key, "rows a")
	r.walk()
	e, _ := r.c.raftNode.FSM().GetFile(key)
	e.Tier = "hot"
	raw, _ := json.Marshal(raft.UpdateFilePayload{File: *e})
	if err := r.c.raftNode.Apply(&raft.Command{Type: raft.CommandUpdateFile, Payload: raw}, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	r.walk()
	if len(verified) != 0 {
		t.Fatalf("re-checked %v", verified)
	}
}
