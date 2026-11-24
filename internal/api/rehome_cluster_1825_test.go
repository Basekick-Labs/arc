package api

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gofiber/fiber/v2"
	hraft "github.com/hashicorp/raft"
	"github.com/rs/zerolog"

	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/readgate"
	"github.com/basekick-labs/arc/internal/storage"
)

// #1825 slice 4b: the job on a cluster sharing one object store, against a
// real cluster record (raft.ClusterFSM) and a local backend standing for the
// bucket.

// objStore is a local backend that answers as an object store: it records
// the checksum an object was written with.
type objStore struct {
	*storage.LocalBackend
	mu   sync.Mutex
	shas map[string]string
}

func (s *objStore) Type() string { return "s3" }
func (s *objStore) CopyObject(context.Context, string, string, int64, string) error {
	return fmt.Errorf("unused")
}
func (s *objStore) WriteReaderWithSHA(ctx context.Context, p string, rd io.Reader, n int64, sha string) error {
	if err := s.LocalBackend.WriteReader(ctx, p, rd, n); err != nil {
		return err
	}
	s.mu.Lock()
	s.shas[p] = sha
	s.mu.Unlock()
	return nil
}
func (s *objStore) ObjectSHA(ctx context.Context, p string) (int64, string, error) {
	n, err := s.LocalBackend.StatFile(ctx, p)
	if err != nil {
		return 0, "", err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if n < 0 {
		delete(s.shas, p)
		return -1, "", nil
	}
	return n, s.shas[p], nil
}

// fakeCluster is the cluster as the driver sees it: a real record, applied
// to in order, with this node (w1) the primary writer.
type fakeCluster struct {
	t   *testing.T
	mu  sync.Mutex
	f   *raft.ClusterFSM
	idx uint64
	// acking: the other nodes (and this one) ack every state they are
	// waited for.
	acking atomic.Bool
	// skipSelf: w1 (this node) is not acked by the fake; a test acks it.
	skipSelf atomic.Bool
	// stopAt: the driver's proposals of this state fail (a stand-in for a
	// driver that stopped).
	stopAt atomic.Value
	// failOnce: the first proposal of this state fails with a transient
	// error.
	failOnce atomic.Value
	onFail   func()
}

var fakeClusterNodes = []string{"w1", "w2", "r1"}

func newFakeCluster(t *testing.T) *fakeCluster {
	c := &fakeCluster{t: t, f: raft.NewClusterFSM(zerolog.Nop())}
	c.stopAt.Store("")
	c.failOnce.Store("")
	for _, n := range fakeClusterNodes {
		c.must(raft.CommandAddNode, raft.AddNodePayload{Node: raft.NodeInfo{ID: n, Role: "writer", State: "healthy",
			Capabilities: []string{raft.CapabilityRehome, raft.CapabilityRehomeInScope}}})
	}
	c.must(raft.CommandEnableCompactionCommit, raft.EnableCompactionCommitPayload{Nodes: fakeClusterNodes, Now: time.Now()})
	c.must(raft.CommandEnableRehome, raft.EnableRehomePayload{Nodes: fakeClusterNodes, Now: time.Now()})
	c.acking.Store(true)
	done := make(chan struct{})
	t.Cleanup(func() { close(done) })
	go func() {
		for {
			select {
			case <-done:
				return
			case <-time.After(5 * time.Millisecond):
			}
			if !c.acking.Load() {
				continue
			}
			for _, j := range c.f.RehomeJobs() {
				for _, n := range j.MissingAcks() {
					if n == "w1" && c.skipSelf.Load() {
						continue
					}
					_ = c.apply(raft.CommandAckRehome, raft.AckRehomePayload{Job: j.ID, Node: n, State: j.State})
				}
			}
		}
	}()
	return c
}

func (c *fakeCluster) apply(t raft.CommandType, p any) error {
	raw, err := json.Marshal(p)
	if err != nil {
		return err
	}
	data, err := json.Marshal(raft.Command{Type: t, Payload: raw})
	if err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.idx++
	if err, ok := c.f.Apply(&hraft.Log{Index: c.idx, Data: data}).(error); ok {
		return err
	}
	return nil
}

func (c *fakeCluster) must(t raft.CommandType, p any) {
	c.t.Helper()
	if err := c.apply(t, p); err != nil {
		c.t.Fatalf("command %d: %v", t, err)
	}
}

func (c *fakeCluster) LocalNodeID() string    { return "w1" }
func (c *fakeCluster) IsPrimaryWriter() bool  { return true }
func (c *fakeCluster) IsLeader() bool         { return true }
func (c *fakeCluster) RehomeRunnable() string { return c.f.RehomeOutOfScope() }
func (c *fakeCluster) RehomeStage(_ context.Context, p raft.StageRehomePayload) error {
	p.Origin = "w1"
	return c.apply(raft.CommandStageRehome, p)
}
func (c *fakeCluster) RehomeAdvance(_ context.Context, id, to string) error {
	if s, _ := c.stopAt.Load().(string); s == to {
		// As if another node had taken the job: the driver stops at once.
		return &raft.RehomeConflictError{Job: id, Reason: raft.RehomeNotFinisher}
	}
	if s, _ := c.failOnce.Load().(string); s == to && c.failOnce.CompareAndSwap(s, "") {
		if c.onFail != nil {
			c.onFail()
		}
		return fmt.Errorf("leader changed")
	}
	return c.apply(raft.CommandAdvanceRehome, raft.AdvanceRehomePayload{Job: id, By: "w1", To: to, Now: time.Now().UTC()})
}
func (c *fakeCluster) RehomeClaim(_ context.Context, id string) error {
	return c.apply(raft.CommandClaimRehomeFinisher, raft.ClaimRehomeFinisherPayload{Job: id, By: "w1", Now: time.Now().UTC()})
}
func (c *fakeCluster) RehomeEnd(_ context.Context, id string) error {
	now := time.Now().UTC()
	if j, ok := c.f.RehomeJob(id); ok && j.State == raft.RehomeReverting {
		// The grace already over, so a test need not wait for it.
		now = now.Add(-raft.RehomeRevertedGrace - time.Second)
	}
	return c.apply(raft.CommandEndRehome, raft.EndRehomePayload{Job: id, By: "w1", Now: now})
}
func (c *fakeCluster) RehomeJob(id string) (raft.RehomeJob, bool) { return c.f.RehomeJob(id) }
func (c *fakeCluster) RehomeJobs() []raft.RehomeJob               { return c.f.RehomeJobs() }
func (c *fakeCluster) RehomeConfigBlockers() ([]string, error)    { return nil, nil }
func (c *fakeCluster) ManifestFile(p string) (*raft.FileEntry, bool) {
	return c.f.GetFile(p)
}

// clusterRig is the swap rig's file in a bucket, a cluster record that has
// it registered, and the handler running jobs through the record.
type clusterRig struct {
	*swapRig
	store *objStore
	c     *fakeCluster
}

func newClusterRig(t *testing.T) *clusterRig {
	t.Helper()
	r := &clusterRig{swapRig: newSwapRig(t)}
	local := r.backend.(*storage.LocalBackend)
	r.store = &objStore{LocalBackend: local, shas: map[string]string{}}
	r.backend = r.store
	r.h = NewRehomeHandler(r.store, nil, r.scratch, zerolog.Nop())
	t.Cleanup(func() { _ = r.h.Shutdown(context.Background()) })
	r.gate = &readgate.Gate{}
	r.swap = &RehomeSwap{Registry: r.reg, Reads: r.gate}
	r.h.SetSwap(r.swap)
	r.c = newFakeCluster(t)
	r.h.SetCluster(r.c, func() (bool, string) { return true, "" })
	r.app = fiber.New()
	r.h.RegisterRoutes(r.app)
	raw, err := os.ReadFile(filepath.Join(r.root, r.key))
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(raw)
	r.c.must(raft.CommandRegisterFile, raft.RegisterFilePayload{File: raft.FileEntry{
		Path: r.key, SHA256: hex.EncodeToString(sum[:]), SizeBytes: int64(len(raw)), Database: "db", Measurement: "cpu",
		PartitionTime: time.Date(2026, 11, 18, 23, 0, 0, 0, time.UTC), OriginNodeID: "w1", Tier: "hot", CreatedAt: time.Now()}})
	return r
}

// clusterReads is a read of the measurement as any node makes it: every
// file in the bucket, less what the cluster record leaves out.
func (r *clusterRig) clusterReads() (rows, sum int, files []string) {
	r.t.Helper()
	ctx := context.Background()
	all, err := r.store.List(ctx, "db/cpu/")
	if err != nil {
		r.t.Fatal(err)
	}
	keys, _ := r.c.f.RehomeExcludedFor("db/cpu/")
	out := map[string]bool{}
	for _, k := range keys {
		out[k] = true
	}
	db, err := sql.Open("duckdb", "")
	if err != nil {
		r.t.Fatal(err)
	}
	defer db.Close()
	for _, k := range all {
		if out[k] || !strings.HasSuffix(k, ".parquet") {
			continue
		}
		files = append(files, k)
		var n, s int
		if err := db.QueryRow(fmt.Sprintf(`SELECT count(*), coalesce(sum(n), 0) FROM read_parquet('%s')`, filepath.Join(r.root, k))).Scan(&n, &s); err != nil {
			r.t.Fatal(err)
		}
		rows, sum = rows+n, sum+s
	}
	return rows, sum, files
}

func (r *clusterRig) requireOnce(when string) {
	r.t.Helper()
	if rows, sum, files := r.clusterReads(); rows != 6 || sum != 63 {
		r.t.Fatalf("%s: a read sees %d rows (sum %d), want the six rows once (sum 63); files %v", when, rows, sum, files)
	}
}

func TestClusterSwapMovesTheRows1825(t *testing.T) {
	r := newClusterRig(t)
	var steps []string
	var mu sync.Mutex
	r.h.afterStep = func(s string) {
		mu.Lock()
		steps = append(steps, s)
		mu.Unlock()
		r.requireOnce("after step " + s)
	}
	cleared := ""
	r.swap.ClearMisfiled = func(p string) { cleared = p }
	id, code, why := r.start()
	if code != 202 {
		t.Fatalf("start: %d %s", code, why)
	}
	job := r.wait(id)
	if job["state"] != "done" {
		t.Fatalf("job: %v", job)
	}
	r.requireOnce("after the swap")
	_, _, files := r.clusterReads()
	if len(files) != 3 {
		t.Fatalf("files after the swap: %v", files)
	}
	if n, _ := r.store.StatFile(context.Background(), r.key); n >= 0 {
		t.Fatal("the moved file is still in the bucket")
	}
	if _, reg := r.c.f.GetFile(r.key); reg {
		t.Fatal("the moved file is still in the manifest")
	}
	for _, f := range files {
		if e, reg := r.c.f.GetFile(f); !reg || e.Tier != "hot" {
			t.Fatalf("output %s not registered: %+v", f, e)
		}
	}
	if len(r.c.f.RehomeJobs()) != 0 || r.reg.Active() || cleared != r.key {
		t.Fatalf("left behind: jobs %v, local registry active %v, cleared %q", r.c.f.RehomeJobs(), r.reg.Active(), cleared)
	}
	mu.Lock()
	defer mu.Unlock()
	want := []string{"staged", "upload", "upload", "upload", "uploaded", "latched", "removed"}
	if strings.Join(steps, ",") != strings.Join(want, ",") {
		t.Fatalf("steps %v, want %v", steps, want)
	}
}

// A job taken up before its outputs were written (another node, or this one
// after a restart: no split files) is reverted, and reads never change.
func TestClusterTakeoverBeforePublishReverts1825(t *testing.T) {
	r := newClusterRig(t)
	r.c.stopAt.Store(raft.RehomePublished)
	r.h.afterStep = func(s string) { r.requireOnce("after step " + s) }
	id, code, why := r.start()
	if code != 202 {
		t.Fatalf("start: %d %s", code, why)
	}
	if job := r.wait(id); job["state"] != "stopped" {
		t.Fatalf("job: %v", job)
	}
	if j, ok := r.c.f.RehomeJob(id); !ok || j.State != raft.RehomeCommitted {
		t.Fatalf("job after the stop: %+v %v", j, ok)
	}
	r.requireOnce("stopped at committed, outputs uploaded")
	r.c.stopAt.Store("")
	r.h.takeOverOnce(context.Background())
	for i := 0; i < 500; i++ {
		if _, ok := r.c.f.RehomeJob(id); !ok {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if j, ok := r.c.f.RehomeJob(id); ok {
		t.Fatalf("the job did not end: %+v", j)
	}
	r.requireOnce("after the revert")
	_, _, files := r.clusterReads()
	all, _ := r.store.List(context.Background(), "db/cpu/")
	if len(files) != 1 || files[0] != r.key || len(all) != 1 {
		t.Fatalf("after the revert: read %v, bucket %v", files, all)
	}
}

// A job published when its driver stopped goes forward when taken up.
func TestClusterTakeoverAfterPublishFinishes1825(t *testing.T) {
	r := newClusterRig(t)
	r.c.stopAt.Store(raft.RehomeLatched)
	id, _, _ := r.start()
	if job := r.wait(id); job["state"] != "stopped" {
		t.Fatalf("job: %v", job)
	}
	r.requireOnce("stopped at published")
	r.c.stopAt.Store("")
	r.h.takeOverOnce(context.Background())
	for i := 0; i < 500; i++ {
		if _, ok := r.c.f.RehomeJob(id); !ok {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, ok := r.c.f.RehomeJob(id); ok {
		t.Fatal("the job did not end")
	}
	r.requireOnce("after the takeover")
	if _, _, files := r.clusterReads(); len(files) != 3 {
		t.Fatalf("files %v", files)
	}
}

// An output key that holds something else when the upload comes reverts the
// job; the object is left alone.
func TestClusterOutputKeyTakenReverts1825(t *testing.T) {
	r := newClusterRig(t)
	planted := ""
	r.h.afterStep = func(s string) {
		if s != "staged" {
			return
		}
		planted = r.c.f.RehomeJobs()[0].Outputs[0].Key
		if err := r.store.WriteReader(context.Background(), planted, strings.NewReader("not the job's"), 13); err != nil {
			t.Error(err)
		}
	}
	id, _, _ := r.start()
	if job := r.wait(id); job["state"] != "failed" {
		t.Fatalf("job: %v", job)
	}
	if n, _ := r.store.StatFile(context.Background(), planted); n != 13 {
		t.Fatalf("the planted object was touched (size %d)", n)
	}
	if n, _ := r.store.StatFile(context.Background(), r.key); n < 0 {
		t.Fatal("the source is gone")
	}
	r.h.takeOverOnce(context.Background())
	for i := 0; i < 500 && len(r.c.f.RehomeJobs()) != 0; i++ {
		time.Sleep(10 * time.Millisecond)
	}
	if n, _ := r.store.StatFile(context.Background(), planted); n != 13 {
		t.Fatalf("the planted object was touched by the cleanup (size %d)", n)
	}
	if len(r.c.f.RehomeJobs()) != 0 {
		t.Fatal("the job did not end")
	}
}

// A node that does not ack in time makes the job revert before anything
// changes for readers.
func TestClusterAckTimeoutReverts1825(t *testing.T) {
	r := newClusterRig(t)
	old := rehomeClusterAckWait
	rehomeClusterAckWait = 200 * time.Millisecond
	t.Cleanup(func() { rehomeClusterAckWait = old })
	r.c.acking.Store(false)
	sawReverted := make(chan struct{})
	go func() {
		// Every node but r1 acks, until the job is reverted: r1 comes
		// back then, and the reverted state waits for it too.
		for i := 0; i < 2000; i++ {
			for _, j := range r.c.f.RehomeJobs() {
				if j.State == raft.RehomeReverted {
					r.c.acking.Store(true)
					close(sawReverted)
					return
				}
				for _, n := range j.MissingAcks() {
					if n != "r1" {
						_ = r.c.apply(raft.CommandAckRehome, raft.AckRehomePayload{Job: j.ID, Node: n, State: j.State})
					}
				}
			}
			time.Sleep(5 * time.Millisecond)
		}
	}()
	r.h.afterStep = func(s string) { r.requireOnce("after step " + s) }
	id, _, _ := r.start()
	job := r.wait(id)
	if job["state"] != "failed" || !strings.Contains(fmt.Sprint(job["reason"]), "reverted") {
		t.Fatalf("job: %v", job)
	}
	// The rest of a reverted job runs in the background.
	r.h.takeOverOnce(context.Background())
	for i := 0; i < 500 && len(r.c.f.RehomeJobs()) != 0; i++ {
		time.Sleep(10 * time.Millisecond)
	}
	select {
	case <-sawReverted:
	default:
		t.Fatal("the job ended without passing through reverted")
	}
	if len(r.c.f.RehomeJobs()) != 0 {
		t.Fatalf("jobs %v", r.c.f.RehomeJobs())
	}
	r.requireOnce("after the revert")
	if all, _ := r.store.List(context.Background(), "db/cpu/"); len(all) != 1 {
		t.Fatalf("bucket %v", all)
	}
}

// A node removed without acking holds the next step for rehomeDroppedWait
// after the Raft configuration is clean.
func TestClusterDroppedNodeWait1825(t *testing.T) {
	h := &RehomeHandler{}
	old := rehomeDroppedWait
	rehomeDroppedWait = 100 * time.Millisecond
	t.Cleanup(func() { rehomeDroppedWait = old })
	j := raft.RehomeJob{ID: "j1", Dropped: []string{"r1"}}
	if w := h.droppedWait(j, false); w != rehomeDroppedWait {
		t.Fatalf("configuration not clean: wait %v", w)
	}
	if w := h.droppedWait(j, true); w <= 0 {
		t.Fatalf("clean: no wait (%v)", w)
	}
	time.Sleep(120 * time.Millisecond)
	if w := h.droppedWait(j, true); w > 0 {
		t.Fatalf("still waiting %v", w)
	}
	if w := h.droppedWait(raft.RehomeJob{ID: "j2"}, true); w != 0 {
		t.Fatal("a job without dropped nodes waits")
	}
}

// A node's part of a state: its caches dropped before and after the wait,
// and the wait for the reads from before.
func TestParticipateClusterWaitsForReads1825(t *testing.T) {
	gate := &readgate.Gate{}
	var drops atomic.Int32
	h := &RehomeHandler{swap: &RehomeSwap{Reads: gate, InvalidateLocalCaches: func() { drops.Add(1) }}}
	j := raft.RehomeJob{ID: "j1", State: raft.RehomePublished, StateIndex: 7}
	leave := gate.Enter()
	done := make(chan error, 1)
	go func() { done <- h.ParticipateCluster(context.Background(), j, true) }()
	select {
	case err := <-done:
		t.Fatalf("returned with a read from before in flight: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	if drops.Load() != 1 {
		t.Fatalf("caches dropped %d times before the wait", drops.Load())
	}
	leave()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if drops.Load() != 2 {
		t.Fatalf("caches dropped %d times", drops.Load())
	}
	// Not waited for: the cache step only, no wait.
	leave = gate.Enter()
	defer leave()
	if err := h.ParticipateCluster(context.Background(), raft.RehomeJob{ID: "j2", State: raft.RehomeRemoved, StateIndex: 9}, false); err != nil {
		t.Fatal(err)
	}
	// A retry of a step waits for the reads of its first flip, not later
	// ones: a read entered after the first attempt does not hold it.
	j3 := raft.RehomeJob{ID: "j3", State: raft.RehomeCommitted, StateIndex: 11}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	_ = h.ParticipateCluster(ctx, j3, true) // the read entered above holds it
	cancel()
	leave()
	later := gate.Enter()
	defer later()
	if err := h.ParticipateCluster(context.Background(), j3, true); err != nil {
		t.Fatalf("a retry waited for a later read: %v", err)
	}
}

// A takeover pass while the request path is between its stage and its drive
// does not take the job for one left by a restart.
func TestClusterTakeoverDuringStageLeavesTheJob1825(t *testing.T) {
	r := newClusterRig(t)
	r.h.afterStep = func(s string) {
		if s == "staged" {
			r.h.takeOverOnce(context.Background())
		}
	}
	id, _, _ := r.start()
	if job := r.wait(id); job["state"] != "done" {
		t.Fatalf("job: %v", job)
	}
	r.requireOnce("after the swap")
}

// A transient failure of a step is retried where it is, not reverted.
func TestClusterTransientFailureRetried1825(t *testing.T) {
	r := newClusterRig(t)
	var failed atomic.Int32
	r.c.failOnce.Store(raft.RehomePublished)
	r.c.onFail = func() { failed.Add(1) }
	id, _, _ := r.start()
	if job := r.wait(id); job["state"] != "done" {
		t.Fatalf("job: %v", job)
	}
	if failed.Load() != 1 {
		t.Fatalf("failures injected: %d", failed.Load())
	}
	r.requireOnce("after the swap")
}

// #1859: a node that goes out of scope mid-job makes the next step refused;
// the driver reverts the job at once, and reads never change.
func TestClusterNodeOutOfScopeReverts1859(t *testing.T) {
	r := newClusterRig(t)
	r.h.afterStep = func(s string) {
		if s == "staged" {
			r.c.must(raft.CommandAddNode, raft.AddNodePayload{Node: raft.NodeInfo{ID: "r9", Role: "reader", State: "healthy",
				Capabilities: []string{raft.CapabilityRehome}}})
		}
		r.requireOnce("after step " + s)
	}
	id, _, _ := r.start()
	job := r.wait(id)
	if job["state"] != "failed" || !strings.Contains(fmt.Sprint(job["reason"]), "reverted") {
		t.Fatalf("job: %v", job)
	}
	r.h.takeOverOnce(context.Background())
	for i := 0; i < 500 && len(r.c.f.RehomeJobs()) != 0; i++ {
		time.Sleep(10 * time.Millisecond)
	}
	if len(r.c.f.RehomeJobs()) != 0 {
		t.Fatalf("jobs %v", r.c.f.RehomeJobs())
	}
	r.requireOnce("after the revert")
}

// #1857: a DELETE rewrite of the source that the cluster never registered (a
// DELETE that started on a former primary) is removed before the source.
func TestClusterRemovesAnUnregisteredRewrite1857(t *testing.T) {
	r := newClusterRig(t)
	rewrite := strings.TrimSuffix(r.key, ".parquet") + "_rewrite_1.parquet"
	r.h.afterStep = func(s string) {
		if s == "latched" {
			if err := r.store.WriteReader(context.Background(), rewrite, strings.NewReader("stale rows"), 10); err != nil {
				t.Error(err)
			}
		}
	}
	id, _, _ := r.start()
	if job := r.wait(id); job["state"] != "done" {
		t.Fatalf("job: %v", job)
	}
	if n, _ := r.store.StatFile(context.Background(), rewrite); n >= 0 {
		t.Fatal("the unregistered rewrite is still in the bucket")
	}
	r.requireOnce("after the swap")
}

// A registered sibling of the source is not touched by the sweep.
func TestClusterKeepsARegisteredSibling1857(t *testing.T) {
	r := newClusterRig(t)
	h := &RehomeHandler{storage: r.store, logger: r.h.logger, swap: r.swap}
	sib := strings.TrimSuffix(r.key, ".parquet") + "_rewrite_2.parquet"
	if err := r.store.WriteReader(context.Background(), sib, strings.NewReader("kept"), 4); err != nil {
		t.Fatal(err)
	}
	r.c.must(raft.CommandRegisterFile, raft.RegisterFilePayload{File: raft.FileEntry{
		Path: sib, SHA256: "s", SizeBytes: 4, Database: "db", Measurement: "cpu",
		PartitionTime: time.Date(2026, 11, 18, 23, 0, 0, 0, time.UTC), OriginNodeID: "w1", Tier: "hot", CreatedAt: time.Now()}})
	if err := h.removeUnregisteredRewrites(context.Background(), r.key); err != nil {
		t.Fatal(err)
	}
	if n, _ := r.store.StatFile(context.Background(), sib); n != 4 {
		t.Fatal("a registered sibling was removed")
	}
}
