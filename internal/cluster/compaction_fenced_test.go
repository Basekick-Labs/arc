package cluster

import (
	"context"
	"encoding/json"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/raft"
)

// #1852: a fenced object job, finished against a real FSM and a fake bucket.

func (r *objectRigT) enableFences() {
	r.t.Helper()
	fsm := r.c.raftNode.FSM()
	var nodes []string
	for _, n := range fsm.GetAllNodes() {
		nodes = append(nodes, n.ID)
	}
	if ids, err := r.c.raftNode.ConfigurationServerIDs(); err == nil {
		for _, id := range ids {
			if _, ok := fsm.GetNode(id); !ok {
				r.t.Fatalf("raft server %s not in the node table: the fence wait would block", id)
			}
		}
	}
	raw, _ := json.Marshal(raft.EnableCompactionFencesPayload{Nodes: nodes, Now: time.Now()})
	if err := r.c.raftNode.Apply(&raft.Command{Type: raft.CommandEnableCompactionFences, Payload: raw}, 5*time.Second); err != nil {
		r.t.Fatal(err)
	}
}

// fencer raises the fences of every node but skip to the applied index, and
// records, per phase it sees, what a read here leaves out.
type fencer struct {
	mu        sync.Mutex
	excluded  map[string][]string
	skip      string
	stop      chan struct{}
	stopped   chan struct{}
	prefix    string
	rehomeKey string
}

func (r *objectRigT) startFencer(prefix, skip string) *fencer {
	fc := &fencer{excluded: map[string][]string{}, skip: skip, stop: make(chan struct{}), stopped: make(chan struct{}), prefix: prefix}
	fsm := r.c.raftNode.FSM()
	go func() {
		defer close(fc.stopped)
		for {
			select {
			case <-fc.stop:
				return
			case <-time.After(10 * time.Millisecond):
			}
			for _, s := range fsm.FinalizingObjectJobsUnder("") {
				ex := r.c.OpenExcludedFor(context.Background(), fc.prefix)
				sort.Strings(ex)
				fc.mu.Lock()
				if _, seen := fc.excluded[s.Phase]; !seen {
					fc.excluded[s.Phase] = ex
				}
				fc.mu.Unlock()
			}
			w := fsm.AppliedDone()
			for _, n := range fsm.GetAllNodes() {
				if n.ID == fc.skip || fsm.ReadFence(n.ID) >= w {
					continue
				}
				raw, _ := json.Marshal(raft.ReadFencePayload{Node: n.ID, Index: w})
				_ = r.c.raftNode.Apply(&raft.Command{Type: raft.CommandReadFence, Payload: raw}, 5*time.Second)
			}
		}
	}()
	return fc
}

func (fc *fencer) halt() {
	close(fc.stop)
	<-fc.stopped
}

func TestFinishFencedObjectJob(t *testing.T) {
	r := objectRig(t)
	r.enableFences()
	job := r.objectJob("j1")
	s, _ := r.staged("j1")
	if !s.Acked || s.Phase != raft.PhaseCommitted {
		t.Fatalf("committed job %+v", s)
	}
	prefix := job.Output.Path[:strings.Index(job.Output.Path, "/")+1]
	fc := r.startFencer(prefix, "")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := r.c.finishObjectJob(ctx, "j1"); err != nil {
		t.Fatal(err)
	}
	fc.halt()
	if _, ok := r.staged("j1"); ok {
		t.Fatal("job not ended")
	}
	if size, sha, _ := r.store.ObjectSHA(ctx, job.Output.Path); size != job.Output.SizeBytes || sha != job.Output.SHA256 {
		t.Fatalf("output at its path: %d %q", size, sha)
	}
	for _, in := range job.Inputs {
		if r.exists(in.Path) {
			t.Fatalf("input %s still in the bucket", in.Path)
		}
	}
	fc.mu.Lock()
	defer fc.mu.Unlock()
	for _, ph := range []string{raft.PhaseCommitted, raft.PhasePublished} {
		if ex := fc.excluded[ph]; len(ex) != 1 || ex[0] != job.Output.Path {
			t.Fatalf("%s leaves out %v, want the output only", ph, ex)
		}
	}
	var inputs []string
	for _, in := range job.Inputs {
		inputs = append(inputs, in.Path)
	}
	sort.Strings(inputs)
	if ex := fc.excluded[raft.PhaseSwitched]; strings.Join(ex, ",") != strings.Join(inputs, ",") {
		t.Fatalf("switched leaves out %v, want the inputs %v", ex, inputs)
	}
}

// A node that never fences holds the job before its copy; a waived fence
// lets it go on.
func TestFencedJobWaitsForEveryNode(t *testing.T) {
	r := objectRig(t)
	r.enableFences()
	job := r.objectJob("j1")
	fsm := r.c.raftNode.FSM()
	var other string
	for _, n := range fsm.GetAllNodes() {
		if n.ID != r.c.LocalNodeID() {
			other = n.ID
		}
	}
	if other == "" {
		t.Skip("the rig has one node")
	}
	fc := r.startFencer("", other)
	defer fc.halt()
	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	err := r.c.finishObjectJob(ctx, "j1")
	cancel()
	if err == nil {
		t.Fatal("finished while a node had no fence")
	}
	if r.store.copies != 0 || r.exists(job.Output.Path) {
		t.Fatal("copied before every node's fence")
	}
	raw, _ := json.Marshal(raft.ReadFencePayload{Node: other, Index: fsm.AppliedDone(), Waived: true})
	if err := r.c.raftNode.Apply(&raft.Command{Type: raft.CommandReadFence, Payload: raw}, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	// Each later phase needs a fence from it again: keep waiving.
	stop := make(chan struct{})
	go func() {
		for {
			select {
			case <-stop:
				return
			case <-time.After(20 * time.Millisecond):
			}
			raw, _ := json.Marshal(raft.ReadFencePayload{Node: other, Index: fsm.AppliedDone(), Waived: true})
			_ = r.c.raftNode.Apply(&raft.Command{Type: raft.CommandReadFence, Payload: raw}, 5*time.Second)
		}
	}()
	defer close(stop)
	ctx, cancel = context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := r.c.finishObjectJob(ctx, "j1"); err != nil {
		t.Fatal(err)
	}
	if _, ok := r.staged("j1"); ok {
		t.Fatal("job not ended")
	}
}

// The leader's record of silent peers: only a recent failure whose last
// contact is old enough counts; a resumption or a membership change clears it.
func TestSilentNodes(t *testing.T) {
	sn := &silentNodes{since: map[string]silentPeer{}}
	now := time.Now()
	sn.observe("a", true, now.Add(-time.Minute))
	sn.observe("b", true, now.Add(-5*time.Second))
	if got := sn.silent(now, silentNodeAfter); !got["a"] || got["b"] {
		t.Fatalf("silent %v", got)
	}
	sn.observe("a", false, time.Time{}) // resumed, or left/joined the configuration
	if got := sn.silent(now, silentNodeAfter); got["a"] {
		t.Fatal("a resumed peer is still silent")
	}
	sn.observe("c", true, now.Add(-time.Hour))
	if got := sn.silent(now.Add(2*silentObservedWithin), silentNodeAfter); got["c"] {
		t.Fatal("an old observation still counts")
	}
}

// fencingManifest is the outbox's manifest with a fenced compaction that owns
// one path.
type fencingManifest struct {
	*fakeOutboxManifest
	owned atomic.Value // string; read by the outbox's goroutine
}

func (m *fencingManifest) CompactionFencedOwns(path string) bool {
	owned, _ := m.owned.Load().(string)
	return path == owned
}

// A pending registration of a path a fenced compaction owns is deferred, not
// sent (the cluster would refuse the whole batch) and not settled.
func TestOutboxDefersAFencedPath(t *testing.T) {
	r := loadPending(t, t.TempDir(), map[string]int64{outPathA: 10, outPathB: 20})
	m := &fencingManifest{fakeOutboxManifest: newFakeOutboxManifest()}
	m.owned.Store(outPathA)
	r.HandOff(m, &fakeOutboxStorage{sizes: map[string]int64{outPathA: 10, outPathB: 20}}, "writer-1")
	waitOutbox(t, "the unrelated file registered", func() bool { return m.isRegistered(outPathB) })
	if m.isRegistered(outPathA) || r.Pending() != 1 {
		t.Fatalf("fenced path: registered %v, pending %d", m.isRegistered(outPathA), r.Pending())
	}
	m.owned.Store("")
	waitOutbox(t, "the deferred file registered once the job ended", func() bool { return m.isRegistered(outPathA) })
}

// A fenced job's files are owned for DELETE, retention, database delete and
// backup (RehomeSource), until removed.
func TestRehomeSourceOwnsFencedPaths(t *testing.T) {
	r := objectRig(t)
	r.enableFences()
	job := r.objectJob("j1")
	src := r.c.RehomeSource()
	if !src.Owns(job.Output.Path) || !src.Owns(job.Inputs[0].Path) {
		t.Fatal("a fenced job's files are not owned")
	}
	if !src.OwnsUnder(job.Output.Path[:strings.Index(job.Output.Path, "/")+1]) {
		t.Fatal("not owned under the database")
	}
	if src.Owns("other/x.parquet") {
		t.Fatal("an unrelated file is owned")
	}
}

// A node fences without its read lease: it serves no reads then.
func TestReadFenceWithoutTheLease(t *testing.T) {
	r := objectRig(t)
	r.enableFences()
	r.objectJob("j1")
	r.c.readLease = &readLease{why: "not synced"}
	ran := 0
	r.c.SetReadFenceFunc(func(context.Context, []raft.FileEntry) error { ran++; return nil })
	if err := r.c.readFencePass(context.Background()); err != nil {
		t.Fatal(err)
	}
	fsm := r.c.raftNode.FSM()
	if ran != 1 || fsm.ReadFence(r.c.LocalNodeID()) < fsm.FenceTop() {
		t.Fatalf("ran %d, fence %d, top %d", ran, fsm.ReadFence(r.c.LocalNodeID()), fsm.FenceTop())
	}
}
