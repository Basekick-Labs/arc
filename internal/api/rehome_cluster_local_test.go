package api

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"sync"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/readgate"
	"github.com/basekick-labs/arc/internal/rehome"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// localFake is the fake cluster with the local-storage part: a fetch copies
// a staged output from the origin's disk (peer).
type localFake struct {
	*fakeCluster
	peer    *storage.LocalBackend
	local   *storage.LocalBackend
	fetches int
	// beforeFetch runs at the start of each fetch.
	beforeFetch func()
}

func (l *localFake) FetchRehomeStaged(ctx context.Context, _ raft.RehomeJob, out raft.RehomeOutput) error {
	l.fetches++
	if l.beforeFetch != nil {
		l.beforeFetch()
	}
	data, err := l.peer.Read(ctx, out.StagedKey)
	if err != nil {
		return err
	}
	return l.local.WriteReader(ctx, out.StagedKey, bytes.NewReader(data), int64(len(data)))
}

func (l *localFake) SyncManifest(context.Context) error { return nil }

type localRig struct {
	t     *testing.T
	local *storage.LocalBackend
	peer  *storage.LocalBackend
	reg   *rehome.Registry
	c     *localFake
	h     *RehomeHandler
	job   raft.StageRehomePayload
}

const lrSource = "db/cpu/2026/11/18/23/cpu_x.parquet"

func shaOf(b []byte) string { s := sha256.Sum256(b); return hex.EncodeToString(s[:]) }

func newLocalRig(t *testing.T) *localRig {
	t.Helper()
	local, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	peer, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	src := []byte("source rows")
	if err := local.Write(ctx, lrSource, src); err != nil {
		t.Fatal(err)
	}
	r := &localRig{t: t, local: local, peer: peer}
	r.reg = rehome.NewRegistry(local)
	if err := r.reg.Seed(ctx); err != nil {
		t.Fatal(err)
	}
	r.c = &localFake{fakeCluster: newFakeCluster(t), peer: peer, local: local}
	r.h = NewRehomeHandler(local, nil, t.TempDir(), zerolog.Nop())
	r.h.SetSwap(&RehomeSwap{Registry: r.reg, Reads: &readgate.Gate{}})
	r.h.SetCluster(r.c, func() (bool, string) { return true, "" })
	r.c.must(raft.CommandRegisterFile, raft.RegisterFilePayload{File: raft.FileEntry{
		Path: lrSource, SHA256: shaOf(src), SizeBytes: int64(len(src)), Database: "db", Measurement: "cpu",
		PartitionTime: time.Date(2026, 11, 18, 23, 0, 0, 0, time.UTC), OriginNodeID: "w1", Tier: "hot", CreatedAt: time.Now()}})
	r.job = raft.StageRehomePayload{Job: "ab12", Database: "db", Measurement: "cpu",
		Source: raft.CompactionInput{Path: lrSource, SizeBytes: int64(len(src)), SHA256: shaOf(src)}, Now: time.Now().UTC()}
	for i, body := range []string{"rows of hour 22", "rows of hour 21"} {
		key := raft.RehomeStagedKey("ab12", i)
		if err := peer.Write(ctx, key, []byte(body)); err != nil {
			t.Fatal(err)
		}
		r.job.Outputs = append(r.job.Outputs, raft.RehomeOutput{
			Key: []string{"db/cpu/2026/11/18/22/cpu_o0.parquet", "db/cpu/2026/11/18/21/cpu_o1.parquet"}[i], StagedKey: key,
			SizeBytes: int64(len(body)), SHA256: shaOf([]byte(body)), PartitionTime: time.Date(2026, 11, 18, 22-i, 0, 0, 0, time.UTC)})
	}
	if err := r.c.RehomeStage(ctx, r.job); err != nil {
		t.Fatal(err)
	}
	return r
}

// advance moves the job on (every node acks) and runs this node's part of the
// state it reached.
func (r *localRig) advance(to string) {
	r.t.Helper()
	ctx := context.Background()
	deadline := time.Now().Add(5 * time.Second)
	for {
		err := r.c.RehomeAdvance(ctx, "ab12", to)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			r.t.Fatalf("advance to %s: %v", to, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	r.part()
}

// end retries EndRehome until every node acked (or, from reverting, moves it
// to reverted).
func (r *localRig) end() {
	r.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		err := r.c.RehomeEnd(context.Background(), "ab12")
		if err == nil {
			return
		}
		if time.Now().After(deadline) {
			r.t.Fatalf("end: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// advanceOnly is advance without this node's part: it retries until every
// node acked the state the job leaves.
func (r *localRig) advanceOnly(to string) {
	r.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		err := r.c.RehomeAdvance(context.Background(), "ab12", to)
		if err == nil {
			return
		}
		if time.Now().After(deadline) {
			r.t.Fatalf("advance to %s: %v", to, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (r *localRig) part() {
	r.t.Helper()
	j, ok := r.c.RehomeJob("ab12")
	if !ok {
		r.t.Fatal("no job")
	}
	if err := r.h.ParticipateLocal(context.Background(), j, true); err != nil {
		r.t.Fatalf("%s: %v", j.State, err)
	}
}

func (r *localRig) has(key string) bool {
	n, _ := r.local.StatFile(context.Background(), key)
	return n >= 0
}

func (r *localRig) excluded() []string { return r.reg.ExcludedFor(context.Background(), "db/cpu/") }

// The whole job on one node: outputs held staged at committed, recorded and
// in place at published, the source gone at latched, everything settled at
// the end.
func TestLocalParticipantSwap1860(t *testing.T) {
	r := newLocalRig(t)
	r.advance(raft.RehomeCommitted)
	for _, o := range r.job.Outputs {
		if !r.has(o.StagedKey) || r.has(o.Key) {
			t.Fatalf("committed: staged %v final %v", r.has(o.StagedKey), r.has(o.Key))
		}
	}
	if len(r.excluded()) != 0 {
		t.Fatalf("committed: this node's record leaves out %v", r.excluded())
	}
	r.advance(raft.RehomePublished)
	for _, o := range r.job.Outputs {
		if !r.has(o.Key) {
			t.Fatalf("published: %s not in place", o.Key)
		}
	}
	if ex := r.excluded(); len(ex) != 2 {
		t.Fatalf("published: leaves out %v, want both outputs", ex)
	}
	// A restart now: the record seeds back and leaves the outputs out.
	again := rehome.NewRegistry(r.local)
	if err := again.Seed(context.Background()); err != nil {
		t.Fatal(err)
	}
	if ex := again.ExcludedFor(context.Background(), "db/cpu/"); len(ex) != 2 {
		t.Fatalf("after a restart: leaves out %v", ex)
	}
	if _, err := r.c.f.RehomeJob("ab12"); !err {
		t.Fatal("no job")
	}
	if err := r.c.RehomeClaim(context.Background(), "ab12"); err != nil {
		t.Fatal(err)
	}
	r.advance(raft.RehomeLatched)
	if r.has(lrSource) {
		t.Fatal("latched: the source is still here")
	}
	if ex := r.excluded(); len(ex) != 1 || ex[0] != lrSource {
		t.Fatalf("latched: leaves out %v", ex)
	}
	r.advance(raft.RehomeRemoved)
	// Removed ends this node's record: the source is gone and the outputs
	// in place, which is right whatever the cluster's record says.
	if r.reg.Active() || r.has(raft.RehomeStagedKey("ab12", 0)) {
		t.Fatalf("removed: record %v, staged left %v", r.reg.Active(), r.has(raft.RehomeStagedKey("ab12", 0)))
	}
	r.end()
	if err := r.h.SweepLocal(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, o := range r.job.Outputs {
		if !r.has(o.Key) {
			t.Fatalf("after the end: %s gone", o.Key)
		}
	}
}

// A revert while this node is still fetching: nothing it fetched is placed,
// and the revert leaves nothing behind.
func TestLocalParticipantRevertDuringFetch1860(t *testing.T) {
	r := newLocalRig(t)
	if err := r.c.RehomeAdvance(context.Background(), "ab12", raft.RehomeCommitted); err != nil {
		t.Fatal(err)
	}
	committed, _ := r.c.RehomeJob("ab12")
	reverted := make(chan struct{})
	r.c.beforeFetch = func() {
		r.c.beforeFetch = nil
		go func() {
			defer close(reverted)
			_ = r.c.RehomeAdvance(context.Background(), "ab12", raft.RehomeReverting)
			_ = r.c.RehomeEnd(context.Background(), "ab12") // to reverted
			j, _ := r.c.RehomeJob("ab12")
			if err := r.h.ParticipateLocal(context.Background(), j, true); err != nil {
				t.Error(err)
			}
		}()
		time.Sleep(50 * time.Millisecond) // the revert's step waits for this one
	}
	if err := r.h.ParticipateLocal(context.Background(), committed, true); err != nil {
		t.Fatal(err)
	}
	<-reverted
	if j, _ := r.c.RehomeJob("ab12"); j.State != raft.RehomeReverted {
		t.Fatalf("state %s", j.State)
	}
	for _, o := range r.job.Outputs {
		if r.has(o.StagedKey) || r.has(o.Key) {
			t.Fatalf("left behind: staged %v final %v", r.has(o.StagedKey), r.has(o.Key))
		}
	}
	if !r.has(lrSource) || r.reg.Active() {
		t.Fatal("the source went, or a record is left")
	}
}

// A node that missed the end of a job it recorded: the sweep settles it by
// what the manifest says (here: reverted, the source still registered).
func TestLocalSweepSettlesAMissedRevert1860(t *testing.T) {
	r := newLocalRig(t)
	r.advance(raft.RehomeCommitted)
	r.advance(raft.RehomePublished)
	if !r.reg.Active() {
		t.Fatal("no record")
	}
	if err := r.c.RehomeAdvance(context.Background(), "ab12", raft.RehomeReverting); err != nil {
		t.Fatal(err)
	}
	r.end() // to reverted; this node misses its step
	r.end()
	if err := r.h.SweepLocal(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, o := range r.job.Outputs {
		if r.has(o.Key) || r.has(o.StagedKey) {
			t.Fatalf("%s left behind", o.Key)
		}
	}
	if !r.has(lrSource) || r.reg.Active() {
		t.Fatal("source gone or record left")
	}
}

// A node with no record when the job latched (it joined after published):
// it records the job committed, so the outputs its puller brings are left out
// until all are here, and keeps the source until then.
func TestLocalParticipantLateJoinerAtLatch1860(t *testing.T) {
	r := newLocalRig(t)
	ctx := context.Background()
	for _, to := range []string{raft.RehomeCommitted, raft.RehomePublished} {
		r.advanceOnly(to)
	}
	if err := r.c.RehomeClaim(ctx, "ab12"); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for r.c.RehomeAdvance(ctx, "ab12", raft.RehomeLatched) != nil {
		if time.Now().After(deadline) {
			t.Fatal("no latch")
		}
		time.Sleep(10 * time.Millisecond)
	}
	j, _ := r.c.RehomeJob("ab12")
	if err := r.h.ParticipateLocal(ctx, j, false); err == nil {
		t.Fatal("latched without the outputs here")
	}
	if ex := r.excluded(); len(ex) != 2 || !r.has(lrSource) {
		t.Fatalf("leaves out %v, source here %v", ex, r.has(lrSource))
	}
	// The puller brings them.
	for _, o := range r.job.Outputs {
		data, _ := r.peer.Read(ctx, o.StagedKey)
		if err := r.local.Write(ctx, o.Key, data); err != nil {
			t.Fatal(err)
		}
	}
	if err := r.h.ParticipateLocal(ctx, j, false); err != nil {
		t.Fatal(err)
	}
	if ex := r.excluded(); len(ex) != 1 || ex[0] != lrSource || r.has(lrSource) {
		t.Fatalf("latched: leaves out %v, source here %v", ex, r.has(lrSource))
	}
}

// A node that missed the end of a job that finished: its outputs stay, the
// source goes, the record ends.
func TestLocalSweepSettlesAMissedFinish1860(t *testing.T) {
	r := newLocalRig(t)
	r.advance(raft.RehomeCommitted)
	r.advance(raft.RehomePublished)
	ctx := context.Background()
	if err := r.c.RehomeClaim(ctx, "ab12"); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for r.c.RehomeAdvance(ctx, "ab12", raft.RehomeLatched) != nil {
		if time.Now().After(deadline) {
			t.Fatal("no latch")
		}
		time.Sleep(10 * time.Millisecond)
	}
	deadline = time.Now().Add(5 * time.Second)
	for r.c.RehomeAdvance(ctx, "ab12", raft.RehomeRemoved) != nil {
		if time.Now().After(deadline) {
			t.Fatal("no removed")
		}
		time.Sleep(10 * time.Millisecond)
	}
	r.end() // this node took no part since published
	if err := r.h.SweepLocal(ctx); err != nil {
		t.Fatal(err)
	}
	for _, o := range r.job.Outputs {
		if !r.has(o.Key) {
			t.Fatalf("%s removed", o.Key)
		}
	}
	if r.has(lrSource) || r.reg.Active() {
		t.Fatalf("source here %v, record %v", r.has(lrSource), r.reg.Active())
	}
}

// A node that skipped latched (it joined since) does it at removed: the source
// goes only once the outputs are here.
func TestLocalParticipantRemovedLatchesFirst1860(t *testing.T) {
	r := newLocalRig(t)
	ctx := context.Background()
	for _, to := range []string{raft.RehomeCommitted, raft.RehomePublished} {
		r.advanceOnly(to)
	}
	if err := r.c.RehomeClaim(ctx, "ab12"); err != nil {
		t.Fatal(err)
	}
	for _, to := range []string{raft.RehomeLatched, raft.RehomeRemoved} {
		deadline := time.Now().Add(5 * time.Second)
		for r.c.RehomeAdvance(ctx, "ab12", to) != nil {
			if time.Now().After(deadline) {
				t.Fatalf("no %s", to)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	j, _ := r.c.RehomeJob("ab12")
	if err := r.h.ParticipateLocal(ctx, j, true); err == nil || !r.has(lrSource) {
		t.Fatalf("removed without the outputs: err %v, source here %v", err, r.has(lrSource))
	}
	for _, o := range r.job.Outputs {
		data, _ := r.peer.Read(ctx, o.StagedKey)
		if err := r.local.Write(ctx, o.Key, data); err != nil {
			t.Fatal(err)
		}
	}
	if err := r.h.ParticipateLocal(ctx, j, true); err != nil {
		t.Fatal(err)
	}
	if r.has(lrSource) || r.reg.Active() {
		t.Fatalf("source here %v, record %v", r.has(lrSource), r.reg.Active())
	}
}

// The driver on local storage, end to end on one node (#1860 5b): this node
// does its own part of each state (as the participant loop would) and acks
// it; every other node acks. A read sees every row once at every step.
func TestLocalClusterSwapMovesTheRows1860(t *testing.T) {
	r := newSwapRig(t)
	fake := &localFake{fakeCluster: newFakeCluster(t), peer: r.backend.(*storage.LocalBackend), local: r.backend.(*storage.LocalBackend)}
	fake.skipSelf.Store(true)
	r.swap.LocalCluster = true
	r.h.SetCluster(fake, func() (bool, string) { return true, "" })
	raw, err := r.backend.Read(context.Background(), r.key)
	if err != nil {
		t.Fatal(err)
	}
	fake.must(raft.CommandRegisterFile, raft.RegisterFilePayload{File: raft.FileEntry{
		Path: r.key, SHA256: shaOf(raw), SizeBytes: int64(len(raw)), Database: "db", Measurement: "cpu",
		PartitionTime: time.Date(2026, 11, 18, 23, 0, 0, 0, time.UTC), OriginNodeID: "w1", Tier: "hot", CreatedAt: time.Now()}})
	r.h.afterStep = func(s string) { r.requireOnce("after driver step " + s) }
	stop := make(chan struct{})
	var parts sync.WaitGroup
	parts.Add(1)
	go func() {
		defer parts.Done()
		for {
			select {
			case <-stop:
				return
			case <-time.After(5 * time.Millisecond):
			}
			for _, j := range fake.RehomeJobs() {
				missing := false
				for _, n := range j.MissingAcks() {
					missing = missing || n == "w1"
				}
				if !missing {
					continue
				}
				if err := r.h.ParticipateLocal(context.Background(), j, true); err != nil {
					continue
				}
				r.requireOnce("after this node's " + j.State)
				_ = fake.apply(raft.CommandAckRehome, raft.AckRehomePayload{Job: j.ID, Node: "w1", State: j.State})
			}
		}
	}()
	id, code, reason := r.start()
	if code != 202 {
		t.Fatalf("start: %d %s", code, reason)
	}
	job := r.wait(id)
	close(stop)
	parts.Wait()
	if job["state"] != "done" {
		t.Fatalf("job: %v", job)
	}
	r.requireOnce("after the job")
	if n, _ := r.backend.StatFile(context.Background(), r.key); n >= 0 {
		t.Fatal("the source is still here")
	}
	if r.reg.Active() {
		t.Fatal("a record is left")
	}
}
