package cluster

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"hash"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/filereplication"
	"github.com/basekick-labs/arc/internal/cluster/protocol"
	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/compaction"
	"github.com/basekick-labs/arc/internal/storage"
	hraft "github.com/hashicorp/raft"
	"github.com/rs/zerolog"
)

// #1207 d2a2a: the staged compaction commit, cluster side.

const stHour = "testdb/cpu/2026/10/13/10/"

type stagedRigT struct {
	t       *testing.T
	c       *Coordinator
	root    string
	backend *storage.LocalBackend
}

// stagedRig: this node is writer-2 (the compactor) with local storage;
// compaction commit on; reader-1 keeps local replicas.
func stagedRig(t *testing.T) *stagedRigT {
	t.Helper()
	c := originRig(t, "writer-2", RoleWriter)
	root := t.TempDir()
	b, err := storage.NewLocalBackend(root, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	c.storage = b
	c.cfg.ReplicationEnabled = true
	c.staged = stagedReplica{fetching: map[string]bool{}, wake: make(chan struct{}, 1), goneSince: map[string]time.Time{}}
	c.staged.replicaReady.Store(true)
	all := []string{raft.CapabilityCompactionCommit}
	capNodes(t, c, map[string][]string{"raft-1": all, "writer-1": all, "writer-2": all,
		"reader-1": {raft.CapabilityCompactionCommit, CapabilityLocalReplica}})
	c.maybeEnableCompactionCommit()
	waitFSM(t, c.CompactionCommitActive)
	return &stagedRigT{t: t, c: c, root: root, backend: b}
}

func (r *stagedRigT) write(key, body string) string {
	r.t.Helper()
	full := filepath.Join(r.root, key)
	if err := os.MkdirAll(filepath.Dir(full), 0o700); err != nil {
		r.t.Fatal(err)
	}
	if err := os.WriteFile(full, []byte(body), 0o600); err != nil {
		r.t.Fatal(err)
	}
	sum := sha256.Sum256([]byte(body))
	return hex.EncodeToString(sum[:])
}

func (r *stagedRigT) exists(key string) bool {
	_, err := os.Stat(filepath.Join(r.root, key))
	return err == nil
}

// register writes and registers name in the hour, returning the input.
func (r *stagedRigT) register(name string) compaction.StagedInput {
	r.t.Helper()
	key := stHour + name
	sha := r.write(key, "rows of "+name)
	e := raft.FileEntry{Path: key, SHA256: sha, SizeBytes: int64(len("rows of " + name)), Database: "testdb",
		Measurement: "cpu", OriginNodeID: "writer-1", Tier: "hot", CreatedAt: time.Now()}
	if err := r.c.raftNode.RegisterFile(e, 5*time.Second); err != nil {
		r.t.Fatal(err)
	}
	return compaction.StagedInput{Path: key, SizeBytes: e.SizeBytes, SHA256: sha}
}

// job builds a staged job over two fresh inputs, its output staged here.
func (r *stagedRigT) job(id string) compaction.StagedJob {
	r.t.Helper()
	ins := []compaction.StagedInput{r.register(id + "-a.parquet"), r.register(id + "-b.parquet")}
	out := stHour + id + "-o.parquet"
	key := compaction.StagedKey(id, out)
	sha := r.write(key, "output of "+id)
	return compaction.StagedJob{JobID: id, StagedKey: key, Inputs: ins, Output: compaction.CompactedOutput{
		Path: out, SHA256: sha, SizeBytes: int64(len("output of " + id)), Database: "testdb", Measurement: "cpu",
		// The compaction tier, as compactPartition fills it: not the storage tier.
		Tier: "hourly", CreatedAt: time.Now()}}
}

func (r *stagedRigT) held(job, node string, sha string) {
	r.t.Helper()
	raw, _ := json.Marshal(raft.CompactionHeldPayload{Job: job, Node: node, SHA256: sha})
	if err := r.c.raftNode.Apply(&raft.Command{Type: raft.CommandCompactionHeld, Payload: raw}, 5*time.Second); err != nil {
		r.t.Fatal(err)
	}
}

func TestStagedKeyMatchesTheFSM(t *testing.T) {
	if compaction.StagedRoot != raft.StagedCompactionRoot ||
		compaction.StagedKey("j1", "db/cpu/2026/10/13/10/o.parquet") != raft.StagedCompactionKey("j1", "db/cpu/2026/10/13/10/o.parquet") {
		t.Fatal("the compactor and the FSM disagree on where a staged output lives")
	}
}

func TestStagedCommitterRoundTrip(t *testing.T) {
	r := stagedRig(t)
	sc := r.c.StagedCommitter()
	ctx := context.Background()
	job := r.job("j1")
	if out, err := sc.Stage(ctx, job); out != compaction.StagedOK {
		t.Fatalf("stage: %v %v", out, err)
	}
	st := sc.Status(job)
	if !st.Staged || st.MinHolders != 1 || st.Holders != 0 || st.Committed {
		t.Fatalf("status %+v", st)
	}
	if own := sc.OwnStaged(); len(own) != 1 || own[0] != "j1" {
		t.Fatalf("own %v", own)
	}
	if out, _ := sc.Commit(ctx, job); out != compaction.StagedRetry {
		t.Fatalf("commit without a holder: %v", out)
	}
	r.held("j1", "reader-1", job.Output.SHA256)
	if out, err := sc.Commit(ctx, job); out != compaction.StagedOK {
		t.Fatalf("commit: %v %v", out, err)
	}
	if st := sc.Status(job); !st.Committed || st.Staged {
		t.Fatalf("after the commit: %+v", st)
	}
	if out, err := sc.Commit(ctx, job); out != compaction.StagedOK && out != compaction.StagedCommitted {
		t.Fatalf("commit retried: %v %v", out, err)
	}
	if sc.Registered(job.Inputs[0].Path) || !sc.Registered(job.Output.Path) {
		t.Fatal("manifest after the commit")
	}
	// Registered as a hot file, whatever the compaction tier: a peer moves a
	// non-hot output's staged copy away instead of into place.
	if e, _ := r.c.raftNode.FSM().GetFile(job.Output.Path); e.Tier != "hot" {
		t.Fatalf("output registered with tier %q", e.Tier)
	}
	if out, _ := sc.Stage(ctx, job); out != compaction.StagedCommitted {
		t.Fatalf("stage after the commit: %v", out)
	}

	// An input that changed: refused, never retried.
	j2 := r.job("j2")
	j2.Inputs[0].SHA256 = "old"
	if out, _ := sc.Stage(ctx, j2); out != compaction.StagedRefused {
		t.Fatalf("changed input: %v", out)
	}
	// A pause: retried.
	j3 := r.job("j3")
	pause, _ := json.Marshal(raft.SetCompactionPausePayload{Paused: true, Generation: 1, RequestedBy: "writer-1",
		Now: time.Now(), ExpiresAt: time.Now().Add(time.Minute)})
	if err := r.c.raftNode.Apply(&raft.Command{Type: raft.CommandSetCompactionPause, Payload: pause}, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	if out, _ := sc.Stage(ctx, j3); out != compaction.StagedRetry {
		t.Fatalf("paused: %v", out)
	}
	// A commit of a job the cluster dropped: refused.
	if err := sc.Abort(ctx, "j1", "test"); err != nil {
		t.Fatal(err)
	}
	ghost := r.job("j4")
	if out, _ := sc.Commit(ctx, ghost); out != compaction.StagedRefused {
		t.Fatalf("unknown job: %v", out)
	}
}

// MinHolders is 0 when no other member keeps local replicas.
func TestStagedMinHoldersFollowsTheCapability(t *testing.T) {
	r := stagedRig(t)
	if n := r.c.stagedMinHolders(); n != 1 {
		t.Fatalf("with a replica: %d", n)
	}
	capNodes(t, r.c, map[string][]string{"reader-1": {raft.CapabilityCompactionCommit}})
	if n := r.c.stagedMinHolders(); n != 0 {
		t.Fatalf("without one: %d", n)
	}
	if !hasCapability(r.c.localCapabilities(), CapabilityLocalReplica) {
		t.Fatal("a local node with replication does not advertise local replicas")
	}
	r.c.staged.replicaReady.Store(false)
	if hasCapability(r.c.localCapabilities(), CapabilityLocalReplica) {
		t.Fatal("advertised local replicas before its puller runs")
	}
	r.c.staged.replicaReady.Store(true)
	r.c.cfg.ReplicationEnabled = false
	if hasCapability(r.c.localCapabilities(), CapabilityLocalReplica) {
		t.Fatal("advertised local replicas with replication off")
	}
}

// commitJob stages and commits job (a holder reported).
func (r *stagedRigT) commitJob(job compaction.StagedJob) {
	r.t.Helper()
	sc := r.c.StagedCommitter()
	if out, err := sc.Stage(context.Background(), job); out != compaction.StagedOK {
		r.t.Fatalf("stage: %v %v", out, err)
	}
	r.held(job.JobID, "reader-1", job.Output.SHA256)
	if out, err := sc.Commit(context.Background(), job); out != compaction.StagedOK {
		r.t.Fatalf("commit: %v %v", out, err)
	}
}

func TestAdoptStaged(t *testing.T) {
	r := stagedRig(t)
	ctx := context.Background()
	job := r.job("j1")
	r.commitJob(job)
	e, _ := r.c.raftNode.FSM().GetFile(job.Output.Path)
	if ok, err := r.c.adoptStaged(ctx, e); !ok || err != nil {
		t.Fatalf("adopt: %v %v", ok, err)
	}
	if !r.exists(job.Output.Path) || r.exists(compaction.StagedJobDir("j1")) {
		t.Fatal("not in place, or the staged directory kept")
	}
	// Moved after the output left the manifest: removed again.
	j2 := r.job("j2")
	r.commitJob(j2)
	e2, _ := r.c.raftNode.FSM().GetFile(j2.Output.Path)
	if err := r.c.raftNode.DeleteFile(j2.Output.Path, "delete", 5*time.Second); err != nil {
		t.Fatal(err)
	}
	if ok, _ := r.c.adoptStaged(ctx, e2); ok || r.exists(j2.Output.Path) {
		t.Fatal("an output that left the manifest was moved into place")
	}
	// No staged copy: nothing to adopt.
	j3 := r.job("j3")
	r.commitJob(j3)
	_ = os.Remove(filepath.Join(r.root, j3.StagedKey))
	e3, _ := r.c.raftNode.FSM().GetFile(j3.Output.Path)
	if ok, _ := r.c.adoptStaged(ctx, e3); ok {
		t.Fatal("adopted a missing copy")
	}
	// The compactor still has a manifest for the job: its directory stays.
	j4 := r.job("j4")
	r.commitJob(j4)
	r.c.SetStagedCompactionHooks(func(job string) bool { return job == "j4" }, nil)
	r.write(compaction.StagedJobDir("j4")+"/marker", "x")
	e4, _ := r.c.raftNode.FSM().GetFile(j4.Output.Path)
	if ok, _ := r.c.adoptStaged(ctx, e4); !ok || !r.exists(compaction.StagedJobDir("j4")+"/marker") {
		t.Fatal("the compactor's directory was removed")
	}
}

func TestEndStagedCopy(t *testing.T) {
	r := stagedRig(t)
	ctx := context.Background()
	sc := r.c.StagedCommitter()
	// Aborted: the copy is removed.
	j1 := r.job("j1")
	if out, _ := sc.Stage(ctx, j1); out != compaction.StagedOK {
		t.Fatal("stage")
	}
	s1, _ := r.c.raftNode.FSM().StagedCompactionByJob("j1")
	r.c.endStagedCopy(s1)
	if r.exists(compaction.StagedJobDir("j1")) {
		t.Fatal("copy of an aborted job kept")
	}
	// Dropped by a snapshot taken after its commit: moved into place.
	j2 := r.job("j2")
	if out, _ := sc.Stage(ctx, j2); out != compaction.StagedOK {
		t.Fatal("stage")
	}
	s2, _ := r.c.raftNode.FSM().StagedCompactionByJob("j2")
	r.held("j2", "reader-1", j2.Output.SHA256)
	if out, _ := sc.Commit(ctx, j2); out != compaction.StagedOK {
		t.Fatal("commit")
	}
	r.c.endStagedCopy(s2)
	if !r.exists(j2.Output.Path) {
		t.Fatal("a committed job's copy was not moved into place")
	}
	// The compactor still uses the directory: kept.
	j3 := r.job("j3")
	s3 := raft.StagedCompaction{Job: "j3", Output: raft.FileEntry{Path: j3.Output.Path}}
	r.c.SetStagedCompactionHooks(func(job string) bool { return job == "j3" }, nil)
	r.c.endStagedCopy(s3)
	if !r.exists(j3.StagedKey) {
		t.Fatal("the compactor's staged copy was removed")
	}
}

func TestSweepStagedDirs(t *testing.T) {
	r := stagedRig(t)
	ctx := context.Background()
	sc := r.c.StagedCommitter()
	local := r.job("local")
	staged := r.job("staged")
	if out, _ := sc.Stage(ctx, staged); out != compaction.StagedOK {
		t.Fatal("stage")
	}
	committed := r.job("committed")
	r.commitJob(committed)
	r.write(compaction.StagedKey("stray", stHour+"x.parquet"), "junk")
	r.c.SetStagedCompactionHooks(func(job string) bool { return job == "local" }, nil)
	r.c.sweepStagedDirs(ctx)
	if !r.exists(local.StagedKey) || !r.exists(staged.StagedKey) {
		t.Fatal("a directory in use was removed")
	}
	if !r.exists(committed.Output.Path) || r.exists(compaction.StagedJobDir("committed")) {
		t.Fatal("a committed job's copy was not moved into place")
	}
	if r.exists(compaction.StagedJobDir("stray")) {
		t.Fatal("a stray directory was kept")
	}
}

func TestWaitStagedAdoption(t *testing.T) {
	r := stagedRig(t)
	r.write(compaction.StagedKey("j1", stHour+"o.parquet"), "x")
	go func() {
		time.Sleep(200 * time.Millisecond)
		_ = r.backend.RemoveAll(context.Background(), compaction.StagedJobDir("j1"))
	}()
	start := time.Now()
	r.c.waitStagedAdoption("compaction:j1")
	if d := time.Since(start); d < 150*time.Millisecond || d > stagedDeleteWait {
		t.Fatalf("waited %s", d)
	}
	start = time.Now()
	r.c.waitStagedAdoption("retention")
	r.c.waitStagedAdoption("compaction:nothing")
	if d := time.Since(start); d > 100*time.Millisecond {
		t.Fatalf("waited %s with nothing staged", d)
	}
}

func TestCompactionInputLookupAndHeld(t *testing.T) {
	r := stagedRig(t)
	in := r.register("a.parquet")
	if size, sha, ok := r.c.CompactionInputLookup(in.Path); !ok || size != in.SizeBytes || sha != in.SHA256 {
		t.Fatalf("lookup: %d %s %v", size, sha, ok)
	}
	if _, _, ok := r.c.CompactionInputLookup(stHour + "nope.parquet"); ok {
		t.Fatal("an unregistered file")
	}
	cold := raft.FileEntry{Path: stHour + "cold.parquet", SHA256: "c", SizeBytes: 1, Database: "testdb", Measurement: "cpu",
		OriginNodeID: "writer-1", Tier: "cold", CreatedAt: time.Now()}
	if err := r.c.raftNode.RegisterFile(cold, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	if _, _, ok := r.c.CompactionInputLookup(cold.Path); ok {
		t.Fatal("a cold file")
	}
	job := r.job("j1")
	if _, err := r.c.StagedCommitter().Stage(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	if _, _, ok := r.c.CompactionInputLookup(job.Inputs[0].Path); ok {
		t.Fatal("an input of a staged job")
	}
	r.held("j1", "reader-1", job.Output.SHA256)
	if _, err := r.c.StagedCommitter().Commit(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	if !r.c.CompactionOutputHeld(job.Output.Path) {
		t.Fatal("an output with its staged copy here is not held")
	}
	_ = r.backend.RemoveAll(context.Background(), compaction.StagedJobDir("j1"))
	if r.c.CompactionOutputHeld(job.Output.Path) || r.c.CompactionOutputHeld(in.Path) {
		t.Fatal("held without a staged copy")
	}
}

// A peer pulls a staged output from the compactor's fetch server, makes it
// durable and reports holding it; the fetch server serves no other key.
func TestHoldStagedOutputThroughTheFetchServer(t *testing.T) {
	const secret, clusterName = "auth", "test-cluster"
	r := stagedRig(t)
	r.c.cfg.SharedSecret, r.c.cfg.ClusterName = secret, clusterName
	ctx := context.Background()

	// The compactor (writer-1): its own FSM with the job staged, and the
	// staged bytes in its storage.
	originFSM := raft.NewClusterFSM(zerolog.Nop())
	idx := uint64(0)
	apply := func(typ raft.CommandType, p any) {
		raw, _ := json.Marshal(p)
		data, _ := json.Marshal(raft.Command{Type: typ, Payload: raw})
		idx++
		if res := originFSM.Apply(&hraft.Log{Index: idx, Data: data}); res != nil {
			if err, ok := res.(error); ok {
				t.Fatalf("origin apply %d: %v", typ, err)
			}
		}
	}
	for _, n := range []string{"writer-1", "writer-2"} {
		apply(raft.CommandAddNode, raft.AddNodePayload{Node: raft.NodeInfo{ID: n, Role: "writer", State: "healthy"}})
	}
	apply(raft.CommandEnableCompactionCommit, raft.EnableCompactionCommitPayload{Nodes: []string{"writer-1", "writer-2"}})
	in := raft.FileEntry{Path: stHour + "a.parquet", SHA256: "sa", SizeBytes: 3, Database: "testdb", Measurement: "cpu",
		OriginNodeID: "writer-1", Tier: "hot", CreatedAt: time.Now()}
	apply(raft.CommandRegisterFile, raft.RegisterFilePayload{File: in})
	body := []byte("the staged output")
	sum := sha256.Sum256(body)
	out := raft.FileEntry{Path: stHour + "o.parquet", SHA256: hex.EncodeToString(sum[:]), SizeBytes: int64(len(body)),
		Database: "testdb", Measurement: "cpu", Tier: "hot", CreatedAt: time.Now()}
	stage := raft.StageCompactionPayload{Job: "j1", Origin: "writer-1", Output: out, MinHolders: 1, Now: time.Now(),
		Inputs: []raft.CompactionInput{{Path: in.Path, SizeBytes: 3, SHA256: "sa"}}}
	apply(raft.CommandStageCompaction, stage)
	originBackend := newMemBackend()
	key := raft.StagedCompactionKey("j1", out.Path)
	if err := originBackend.Write(ctx, key, body); err != nil {
		t.Fatal(err)
	}
	origin := startOriginServer(t, originBackend, originFSM, secret, clusterName, "writer-1")
	defer origin.stop()
	if ack := fetchAck(t, origin, secret, clusterName, "writer-2", raft.StagedCompactionKey("j1", stHour+"other.parquet")); ack.Code != protocol.AckCodeManifest {
		t.Fatalf("another key under the job: %+v", ack)
	}
	if ack := fetchAck(t, origin, secret, clusterName, "writer-2", raft.StagedCompactionKey("j9", out.Path)); ack.Code != protocol.AckCodeManifest {
		t.Fatalf("an unknown job: %+v", ack)
	}

	// This node (writer-2) sees the same job staged in its cluster.
	if err := r.c.raftNode.RegisterFile(in, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(stage)
	if err := r.c.raftNode.Apply(&raft.Command{Type: raft.CommandStageCompaction, Payload: raw}, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	if err := r.c.registry.Register(func() *Node {
		n := NewNode("writer-1", "writer-1", RoleWriter, clusterName)
		n.SetAddresses(origin.addr(), "")
		n.UpdateState(StateHealthy)
		return n
	}()); err != nil {
		t.Fatal(err)
	}
	fc, err := filereplication.NewFetchClient(filereplication.FetchClient{SelfNodeID: "writer-2", ClusterName: clusterName,
		SharedSecret: secret, DialTimeout: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	r.c.fetcher = fc
	s, _ := r.c.raftNode.FSM().StagedCompactionByJob("j1")
	if err := r.c.holdStagedOutput(ctx, s); err != nil {
		t.Fatalf("hold: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(r.root, key))
	if err != nil || string(got) != string(body) {
		t.Fatalf("staged copy %q %v", got, err)
	}
	if s, _ := r.c.raftNode.FSM().StagedCompactionByJob("j1"); len(s.Holders) != 1 || s.Holders[0] != "writer-2" {
		t.Fatalf("holders %v", s.Holders)
	}
	// A copy here of the right size but another content is fetched again
	// (the fetch fails: nothing to fetch from now).
	stale := strings.Repeat("?", len(body))
	if err := os.WriteFile(filepath.Join(r.root, key), []byte(stale), 0o600); err != nil {
		t.Fatal(err)
	}
	bad0 := NewNode("writer-1", "writer-1", RoleWriter, clusterName)
	bad0.SetAddresses("127.0.0.1:1", "")
	bad0.UpdateState(StateHealthy)
	_ = r.c.registry.Register(bad0)
	if err := r.c.holdStagedOutput(ctx, s); err == nil {
		t.Fatal("a same-size copy of another content counted as held")
	}
	if err := os.WriteFile(filepath.Join(r.root, key), body, 0o600); err != nil {
		t.Fatal(err)
	}
	// A copy already here is reported without a fetch.
	bad := NewNode("writer-1", "writer-1", RoleWriter, clusterName)
	bad.SetAddresses("127.0.0.1:1", "")
	bad.UpdateState(StateHealthy)
	if err := r.c.registry.Register(bad); err != nil {
		t.Fatal(err)
	}
	if err := r.c.holdStagedOutput(ctx, s); err != nil {
		t.Fatalf("hold with a local copy: %v", err)
	}
}

// The FSM refusing a stage outright ends the job; no answer is retried.
func TestStagedOutcomeOfAnFSMRejection(t *testing.T) {
	r := stagedRig(t)
	sc := r.c.StagedCommitter()
	job := r.job("j1")
	job.JobID = "../escape"
	if out, err := sc.Stage(context.Background(), job); out != compaction.StagedRefused {
		t.Fatalf("an invalid job id: %v %v", out, err)
	}
	if r.c.fsmRejected(ErrNoLeaderKnown) || r.c.fsmRejected(context.DeadlineExceeded) ||
		r.c.fsmRejected(errors.New("failed to apply command: leadership lost")) {
		t.Fatal("no answer read as a rejection")
	}
	if !r.c.fsmRejected(&ForwardRejectedError{Code: protocol.ForwardCodeApplyFailed, Message: "stage compaction: bad"}) ||
		r.c.fsmRejected(&ForwardRejectedError{Code: protocol.ForwardCodeAuth, Message: "x"}) ||
		r.c.fsmRejected(&ForwardRejectedError{Code: protocol.ForwardCodeInvalidCommand, Message: "x"}) {
		t.Fatal("forwarded answers")
	}
}

// The puller's adoption moves the staged copy even over a file at the path
// (a forced refresh); a cold output is never brought back hot.
func TestAdoptStagedForPullAndTier(t *testing.T) {
	r := stagedRig(t)
	ctx := context.Background()
	job := r.job("j1")
	r.commitJob(job)
	r.write(job.Output.Path, strings.Repeat("?", int(job.Output.SizeBytes)))
	e, _ := r.c.raftNode.FSM().GetFile(job.Output.Path)
	if ok, err := r.c.adoptStagedForPull(ctx, e); !ok || err != nil {
		t.Fatalf("adopt for pull: %v %v", ok, err)
	}
	if got, _ := os.ReadFile(filepath.Join(r.root, job.Output.Path)); string(got) != "output of j1" {
		t.Fatalf("content %q", got)
	}
	j2 := r.job("j2")
	r.commitJob(j2)
	e2, _ := r.c.raftNode.FSM().GetFile(j2.Output.Path)
	e2.Tier = "cold"
	if ok, _ := r.c.adoptStaged(ctx, e2); ok || r.exists(j2.Output.Path) || r.exists(compaction.StagedJobDir("j2")) {
		t.Fatal("a cold output moved into place")
	}
}

// The delete worker waits once per job, not once per input.
func TestWaitStagedAdoptionOncePerJob(t *testing.T) {
	r := stagedRig(t)
	r.write(compaction.StagedKey("j1", stHour+"o.parquet"), "x")
	r.c.staged.waitUntil.Store("j1", time.Now().Add(300*time.Millisecond))
	start := time.Now()
	r.c.waitStagedAdoption("compaction:j1")
	r.c.waitStagedAdoption("compaction:j1")
	if d := time.Since(start); d > time.Second {
		t.Fatalf("waited %s for two inputs of one job", d)
	}
}

// A job that ended while its output was being pulled is not reported held,
// and the copy is removed.
func TestHoldStagedOutputOfAJobThatEnded(t *testing.T) {
	r := stagedRig(t)
	ctx := context.Background()
	job := r.job("j1")
	if out, _ := r.c.StagedCommitter().Stage(ctx, job); out != compaction.StagedOK {
		t.Fatal("stage")
	}
	s, _ := r.c.raftNode.FSM().StagedCompactionByJob("j1")
	if err := r.c.StagedCommitter().Abort(ctx, "j1", "test"); err != nil {
		t.Fatal(err)
	}
	if err := r.c.holdStagedOutput(ctx, s); err != nil {
		t.Fatalf("hold: %v", err)
	}
	if r.exists(compaction.StagedJobDir("j1")) {
		t.Fatal("the copy of an ended job was kept")
	}
}

type recordFetcher struct{ paths chan string }

func (f *recordFetcher) Fetch(_ context.Context, _ string, e *raft.FileEntry, _ io.Writer, _ int64, _ hash.Hash) (int64, error) {
	f.paths <- e.Path
	return 0, errors.New("no peer in this test")
}

type oneResolver struct{}

func (oneResolver) ResolvePeers(string, string) []string { return []string{"peer:1"} }

// A holder whose staged fetch the commit cut short (the staged key is no
// longer served) pulls the committed output itself.
func TestEndStagedCopyPullsWhenTheCopyIsIncomplete(t *testing.T) {
	r := stagedRig(t)
	ctx := context.Background()
	job := r.job("j1")
	r.commitJob(job)
	r.write(job.StagedKey, "partial") // the wrong size: a fetch cut short
	f := &recordFetcher{paths: make(chan string, 8)}
	p, err := filereplication.New(filereplication.Config{
		SelfNodeID: "writer-2", Backend: r.backend, Fetcher: f, PeerResolver: oneResolver{},
		Workers: 1, QueueSize: 8, RetryMaxAttempts: 1, RetryInitialBackoff: time.Millisecond,
		FetchTimeout: time.Second, RepullMissingSelfOrigin: true, AdoptStaged: r.c.adoptStagedForPull,
		ManifestEntry: func(path string) (raft.FileEntry, bool) {
			e, ok := r.c.raftNode.FSM().GetFile(path)
			if !ok {
				return raft.FileEntry{}, false
			}
			return *e, true
		},
		Logger: zerolog.Nop(),
	})
	if err != nil {
		t.Fatal(err)
	}
	p.Start(ctx)
	defer p.Stop()
	r.c.puller = p
	r.c.endStagedCopy(raft.StagedCompaction{Job: "j1", Output: raft.FileEntry{Path: job.Output.Path}})
	select {
	case got := <-f.paths:
		if got != job.Output.Path {
			t.Fatalf("pulled %s", got)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the committed output was not pulled")
	}
}
