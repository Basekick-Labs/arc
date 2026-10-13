package cluster

// #1207 S4b1c2: the claimant, against the real manifest, standby store,
// buffer and outbox.

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/originstream"
	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/basekick-labs/arc/internal/wal"
	"github.com/rs/zerolog"
)

const (
	clOrigin = "writer-2"
	clInst   = uint64(0x500)
	clKey    = "db/cpu"
)

type claimantRigT struct {
	c       *Coordinator
	cl      *claimant
	store   *originstream.Store
	outbox  *RegistrationOutbox
	buffer  *ingest.ArrowBuffer
	backend storage.Backend
	wal     *wal.Writer
	hour    time.Time
}

// newClaimantRig: writer-1 (local) is the designated primary with stream
// takeover on; writer-2 announced instance clInst and is not heard from.
func newClaimantRig(t *testing.T) *claimantRigT {
	t.Helper()
	c, _ := takeoverOriginRig(t, true)
	propose(t, c, raft.CommandAnnounceInstance, raft.AnnounceInstancePayload{Origin: clOrigin, Instance: clInst, FreshStart: true})
	logger := zerolog.Nop()
	backend, err := storage.NewLocalBackend(t.TempDir(), logger)
	if err != nil {
		t.Fatal(err)
	}
	c.storage = backend
	w, err := wal.NewWriter(&wal.WriterConfig{WALDir: t.TempDir(), SyncMode: wal.SyncModeAsync, Logger: logger})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.Close() })
	buffer := ingest.NewArrowBuffer(&config.IngestConfig{MaxBufferSize: 1000000, MaxBufferAgeMS: 600000,
		Compression: "snappy", ShardCount: 4, FlushWorkers: 2, FlushQueueSize: 8}, backend, logger)
	buffer.SetWAL(w)
	outbox := newTestOutbox(t, t.TempDir())
	buffer.SetFileRegistrar(outbox)
	if err := buffer.EnableCoverage(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = buffer.Close() })
	c.raftFSM = c.raftNode.FSM()
	outbox.HandOff(rigManifest{c}, backend, "writer-1")
	store, err := originstream.OpenStore(t.TempDir(), 0, logger)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	st := &originStream{present: map[string]int64{}}
	cl := newClaimant(c, store, outbox, buffer, func(ctx context.Context, origin string, keys []string) bool {
		tk := make([]originstream.TrimKey, len(keys))
		for i, k := range keys {
			tk[i] = originstream.TrimKey{Origin: origin, Key: k}
		}
		got := st.keysLocal(ctx, c, tk, func(originstream.TrimKey) bool { return false })
		for _, k := range tk {
			if !got[k] {
				return false
			}
		}
		return true
	})
	cl.grace = time.Minute
	c.claimant.Store(cl)
	return &claimantRigT{c: c, cl: cl, store: store, outbox: outbox, buffer: buffer, backend: backend, wal: w,
		hour: time.Date(2026, 10, 12, 10, 0, 0, 0, time.UTC)}
}

// stored appends one batch of writer-2 to the standby store: n rows of the
// rig's hour.
func (r *claimantRigT) stored(t *testing.T, cseq uint64, n int) {
	t.Helper()
	times, vals := make([]any, n), make([]any, n)
	for i := range times {
		times[i] = r.hour.Add(time.Duration(int(cseq)*100+i) * time.Second).UnixMicro()
		vals[i] = float64(i)
	}
	p := logicalPayload(t, "db", map[string]any{"m": "cpu", "columns": map[string]any{"time": times, "value": vals}})
	if err := r.store.Append(clOrigin, &originstream.Entry{Instance: clInst, Key: clKey, CSeq: cseq, Chunks: 1, Payload: p}); err != nil {
		t.Fatal(err)
	}
}

// afterGrace runs a pass (the claimant sees writer-2's instance), then
// moves its clock past the grace.
func (r *claimantRigT) afterGrace() {
	r.cl.pass(context.Background())
	at := time.Now().Add(2 * r.cl.grace)
	r.cl.now = func() time.Time { return at }
}

func (r *claimantRigT) claim(t *testing.T) (raft.StreamClaim, bool) {
	t.Helper()
	for _, k := range r.c.raftNode.FSM().StreamClaims() {
		if k.Instance == clInst {
			return k, true
		}
	}
	return raft.StreamClaim{}, false
}

func TestClaimantPublishesADeadPrimarysBatchesOnce(t *testing.T) {
	r := newClaimantRig(t)
	ctx := context.Background()
	r.stored(t, 1, 3)
	r.stored(t, 2, 2)

	// Within the grace: nothing is claimed.
	r.cl.pass(ctx)
	if _, ok := r.claim(t); ok {
		t.Fatal("claimed within the grace")
	}

	r.afterGrace()
	r.cl.pass(ctx)
	k, ok := r.claim(t)
	if !ok || k.By != "writer-1" || k.Set[clKey].Count() != 2 {
		t.Fatalf("claim %+v", k)
	}
	waitFSM(t, func() bool { return r.outbox.Pending() == 0 })
	for i := 0; i < 3; i++ {
		r.cl.pass(ctx)
	}
	k, _ = r.claim(t)
	if !k.Done || len(k.Unreplayable) != 0 {
		t.Fatalf("claim not finished cleanly: %+v", k)
	}
	fsm := r.c.raftNode.FSM()
	for cseq := uint64(1); cseq <= 2; cseq++ {
		if st, _, _, _ := fsm.CoveredRows(clInst, clKey, cseq, 0, 1, r.hour); st != raft.CoveredAll {
			t.Fatalf("batch %d not registered whole: %v", cseq, st)
		}
	}
	files := fsm.GetFilesByDatabase("db")
	if len(files) == 0 {
		t.Fatal("no file registered")
	}
	// Finished: the store copy goes (every file is local).
	r.cl.pass(ctx)
	if r.store.Held(clOrigin, clInst) != nil {
		t.Fatal("store copy of a finished claim kept")
	}
	// Nothing is published twice.
	before := len(fsm.GetFilesByDatabase("db"))
	r.cl.pass(ctx)
	if after := len(fsm.GetFilesByDatabase("db")); after != before {
		t.Fatalf("files %d -> %d after another pass", before, after)
	}
}

// Rows the origin registered before it died are not published again.
func TestClaimantSkipsRowsTheOriginRegistered(t *testing.T) {
	r := newClaimantRig(t)
	ctx := context.Background()
	r.stored(t, 1, 2)
	// Pulled here: a claim finishes only once every file of the instance is.
	if err := r.backend.Write(ctx, "db/cpu/origin.parquet", []byte("x")); err != nil {
		t.Fatal(err)
	}
	propose(t, r.c, raft.CommandRegisterFile, raft.RegisterFilePayload{
		File: raft.FileEntry{Path: "db/cpu/origin.parquet", SHA256: "x", SizeBytes: 1, Database: "db", Measurement: "cpu",
			PartitionTime: r.hour, OriginNodeID: clOrigin, CreatedAt: time.Now()},
		Cov2: []raft.InstanceCoverage{{Instance: clInst, Key: clKey, Partial: []raft.CoveragePiece{{CSeq: 1, RowLo: 0, RowHi: 1, RowTotal: 2}}}}})
	r.afterGrace()
	r.cl.pass(ctx)
	waitFSM(t, func() bool { return r.outbox.Pending() == 0 })
	r.cl.pass(ctx)
	r.cl.pass(ctx)
	k, _ := r.claim(t)
	if !k.Done || len(k.Unreplayable) != 0 {
		t.Fatalf("claim %+v", k)
	}
	// Row 0 was the origin's: an overlap would have been refused (parked,
	// then unreplayable). Row 1 is the claimant's.
	st, ranges, total, err := r.c.raftNode.FSM().CoveredRows(clInst, clKey, 1, 0, 1, r.hour)
	if err != nil || st != raft.CoveredRanges || total != 2 || len(complementRows(ranges, 2)) != 0 || len(ranges) != 2 {
		t.Fatalf("batch 1: %v %v %d %v", st, ranges, total, err)
	}
	if n := len(r.outbox.InstanceFiles(clInst)); n != 0 {
		t.Fatalf("%d claimant files parked or pending", n)
	}
}

func TestClaimantTriggerConditions(t *testing.T) {
	ctx := context.Background()
	t.Run("origin healthy", func(t *testing.T) {
		r := newClaimantRig(t)
		r.stored(t, 1, 1)
		peer := NewNode(clOrigin, clOrigin, RoleWriter, "test-cluster")
		peer.UpdateState(StateHealthy)
		if err := r.c.registry.Register(peer); err != nil {
			t.Fatal(err)
		}
		r.afterGrace()
		r.cl.pass(ctx)
		if _, ok := r.claim(t); ok {
			t.Fatal("claimed a live writer's instance")
		}
	})
	t.Run("not the designated primary", func(t *testing.T) {
		r := newClaimantRig(t)
		r.stored(t, 1, 1)
		if err := r.c.raftNode.PromoteWriter("writer-2", "writer-1", 5*time.Second); err != nil {
			t.Fatal(err)
		}
		waitFSM(t, func() bool { return r.c.designatedPrimaryWriterID() == "writer-2" })
		r.afterGrace()
		r.cl.pass(ctx)
		if _, ok := r.claim(t); ok || len(r.cl.backoff) != 0 {
			t.Fatalf("a standby proposed a first claim of a current instance (backoff %v)", r.cl.backoff)
		}
	})
	t.Run("closed instance: any writer, no grace", func(t *testing.T) {
		r := newClaimantRig(t)
		r.stored(t, 1, 1)
		// writer-2 restarts without holding clInst: it is orphaned.
		propose(t, r.c, raft.CommandAnnounceInstance, raft.AnnounceInstancePayload{Origin: clOrigin, Instance: 0x600, FreshStart: true})
		if err := r.c.raftNode.PromoteWriter("writer-2", "writer-1", 5*time.Second); err != nil {
			t.Fatal(err)
		}
		waitFSM(t, func() bool { return r.c.designatedPrimaryWriterID() == "writer-2" })
		r.cl.pass(ctx)
		if k, ok := r.claim(t); !ok || k.By != "writer-1" {
			t.Fatalf("orphaned instance not claimed: %+v", k)
		}
	})
	t.Run("orphaned, nothing here: reported", func(t *testing.T) {
		r := newClaimantRig(t)
		propose(t, r.c, raft.CommandAnnounceInstance, raft.AnnounceInstancePayload{Origin: clOrigin, Instance: 0x600, FreshStart: true})
		r.cl.pass(ctx)
		if in := streamInstance(r.c, clInst); in != nil {
			found := false
			for _, by := range in.ReportedBy {
				found = found || by == "writer-1"
			}
			if !found {
				t.Fatalf("no report: %+v", in)
			}
		}
		if _, ok := r.claim(t); ok {
			t.Fatal("claimed with nothing held")
		}
	})
}

// A claim whose store copy is gone is released, so another writer can take
// the batches over.
func TestClaimantReleasesAClaimItCannotServe(t *testing.T) {
	r := newClaimantRig(t)
	ctx := context.Background()
	r.stored(t, 1, 1)
	r.afterGrace()
	// Claim without replaying: a replayer that is never reached.
	propose(t, r.c, raft.CommandTakeOverStreamV2, raft.TakeOverStreamV2Payload{Origin: clOrigin, By: "writer-1", Now: time.Now(),
		Claims: []raft.ClaimSet{{Instance: clInst, Keys: map[string]raft.Intervals{clKey: {{1, 1}}}}}})
	if err := r.store.DropInstance(clOrigin, clInst); err != nil {
		t.Fatal(err)
	}
	r.cl.pass(ctx)
	if k, _ := r.claim(t); !k.Released {
		t.Fatalf("claim %+v not released", k)
	}
}

// fakeReplayer answers ReplayClaimed with a scripted error or outcome.
type fakeReplayer struct {
	calls   [][]uint64
	err     func(n int) error
	outcome ingest.ReplayOutcome
}

func (f *fakeReplayer) ReplayClaimed(_ context.Context, r ingest.ClaimedReplay) ([]ingest.HourOutcome, error) {
	var cseqs []uint64
	for _, rec := range r.Records {
		cseqs = append(cseqs, rec.CSeq)
	}
	f.calls = append(f.calls, cseqs)
	if f.err != nil {
		if err := f.err(len(r.Records)); err != nil {
			return nil, err
		}
	}
	out := make([]ingest.HourOutcome, len(r.Records))
	for i := range out {
		out[i] = ingest.HourOutcome{Record: i, Outcome: f.outcome}
	}
	return out, nil
}

func TestClaimantReplayOutcomes(t *testing.T) {
	ctx := context.Background()
	setup := func(t *testing.T, f *fakeReplayer) *claimantRigT {
		r := newClaimantRig(t)
		r.cl.replay = f
		for c := uint64(1); c <= 4; c++ {
			r.stored(t, c, 1)
		}
		r.afterGrace()
		return r
	}
	t.Run("too large halves, a single record is unreplayable", func(t *testing.T) {
		f := &fakeReplayer{outcome: ingest.ReplayCovered, err: func(n int) error {
			if n > 1 {
				return ingest.ErrReplayTooLarge
			}
			return nil
		}}
		r := setup(t, f)
		r.cl.pass(ctx)
		r.cl.pass(ctx)
		if k, _ := r.claim(t); !k.Done || len(k.Unreplayable) != 0 {
			t.Fatalf("halving did not finish the claim: %+v (calls %v)", k, f.calls)
		}
		f2 := &fakeReplayer{err: func(int) error { return ingest.ErrReplayTooLarge }}
		r2 := setup(t, f2)
		r2.cl.pass(ctx)
		r2.cl.pass(ctx)
		if k, _ := r2.claim(t); !k.Done || k.Unreplayable[clKey].Count() != 4 {
			t.Fatalf("records too large alone: %+v", k)
		}
	})
	t.Run("failed stays open", func(t *testing.T) {
		f := &fakeReplayer{outcome: ingest.ReplayFailed}
		r := setup(t, f)
		for i := 0; i < replayFailedAlarm+2; i++ {
			r.cl.pass(ctx)
		}
		if k, _ := r.claim(t); k.Done {
			t.Fatalf("a failing flush finished the claim: %+v", k)
		}
	})
	t.Run("written stays open until registered", func(t *testing.T) {
		f := &fakeReplayer{outcome: ingest.ReplayWritten}
		r := setup(t, f)
		for i := 0; i < 3; i++ {
			r.cl.pass(ctx)
		}
		if k, _ := r.claim(t); k.Done {
			t.Fatalf("finished with written files never registered: %+v", k)
		}
	})
	t.Run("unreplayable", func(t *testing.T) {
		f := &fakeReplayer{outcome: ingest.ReplayUnreplayable}
		r := setup(t, f)
		r.cl.pass(ctx)
		r.cl.pass(ctx)
		if k, _ := r.claim(t); !k.Done || k.Unreplayable[clKey].Count() != 4 {
			t.Fatalf("claim %+v", k)
		}
	})
	t.Run("closing buffer ends the pass", func(t *testing.T) {
		f := &fakeReplayer{err: func(int) error { return ingest.ErrReplayClosed }}
		r := setup(t, f)
		r.cl.pass(ctx)
		r.cl.pass(ctx)
		if k, _ := r.claim(t); k.Done {
			t.Fatalf("claim finished while the buffer was closing: %+v", k)
		}
	})
}

var _ = errors.New

// E1: a claim on this process's instance refuses writes and asks for an
// exit, once; a claim on an earlier instance it adopted does not.
func TestClaimedInstanceExits(t *testing.T) {
	c, d := takeoverOriginRig(t, true)
	if err := c.raftNode.PromoteWriter("writer-2", "writer-1", 5*time.Second); err != nil {
		t.Fatal(err)
	}
	waitFSM(t, func() bool { return c.designatedPrimaryWriterID() == "writer-2" })
	var exits atomic.Int32
	o := d.origin(0x100, func() ([]uint64, error) { return []uint64{0x90}, nil }, nil)
	o.OnClaimed = func() { exits.Add(1) }
	var paused atomic.Bool
	c.OnIngestFenceChange(func(p bool) { paused.Store(p) })
	// 0x90: an earlier instance of writer-1, held by this one.
	propose(t, c, raft.CommandAnnounceInstance, raft.AnnounceInstancePayload{Origin: "writer-1", Instance: 0x90, FreshStart: true})
	c.SetTakeoverOrigin(o)
	c.originPass(t)
	if streamInstance(c, 0x100) == nil || streamInstance(c, 0x90).Status != raft.InstanceAdopted {
		t.Fatalf("setup: %+v", streamInstance(c, 0x90))
	}
	// writer-2 is the primary here, so the designation gate refuses; that
	// is not the claimed exit.
	if err := c.IngestGate()(); err == nil || errors.Is(err, errTakeoverClaimed) {
		t.Fatalf("designation gate: %v", err)
	}
	claimAs := func(inst uint64, keys map[string]raft.Intervals, adopter ...uint64) {
		cs := []raft.ClaimSet{{Instance: inst, Keys: keys}}
		for _, a := range adopter {
			cs = append([]raft.ClaimSet{{Instance: a}}, cs...)
		}
		propose(t, c, raft.CommandTakeOverStreamV2, raft.TakeOverStreamV2Payload{Origin: "writer-1", By: "writer-2", Now: time.Now(), Claims: cs})
	}
	claimAs(0x90, map[string]raft.Intervals{"db/cpu": {{1, 1}}}, 0x100)
	c.originPass(t)
	if c.claimedExit.Load() {
		t.Fatal("a claim on an adopted instance stopped this process")
	}
	claimAs(0x100, map[string]raft.Intervals{"db/cpu": {{1, 1}}})
	c.originPass(t)
	c.originPass(t)
	waitFSM(t, func() bool { return exits.Load() == 1 })
	if err := c.IngestGate()(); !errors.Is(err, errTakeoverClaimed) {
		t.Fatalf("gate after the claim: %v", err)
	}
	// MQTT listeners hear the pause.
	c.ingestPauseChanged()
	waitFSM(t, func() bool { return paused.Load() })
	time.Sleep(50 * time.Millisecond)
	if n := exits.Load(); n != 1 {
		t.Fatalf("exit requested %d times", n)
	}
}

func TestDesignationGate(t *testing.T) {
	c, d := takeoverOriginRig(t, true)
	c.SetTakeoverOrigin(d.origin(0x100, noHolds, nil))
	c.originPass(t)
	if err := c.IngestGate()(); err != nil {
		t.Fatalf("the designated primary is refused: %v", err)
	}
	if err := c.raftNode.PromoteWriter("writer-2", "writer-1", 5*time.Second); err != nil {
		t.Fatal(err)
	}
	waitFSM(t, func() bool { return c.designatedPrimaryWriterID() == "writer-2" })
	if err := c.IngestGate()(); !errors.Is(err, errNotDesignated) {
		t.Fatalf("a standby accepted a write: %v", err)
	}
	// Takeover not known on here: the gate is the fence's business.
	off, d2 := takeoverOriginRig(t, false)
	off.SetTakeoverOrigin(d2.origin(0x100, noHolds, nil))
	if err := off.raftNode.PromoteWriter("writer-2", "writer-1", 5*time.Second); err != nil {
		t.Fatal(err)
	}
	waitFSM(t, func() bool { return off.designatedPrimaryWriterID() == "writer-2" })
	if err := off.IngestGate()(); err != nil {
		t.Fatalf("flag off: %v", err)
	}
}

// Another writer finished a claim whose copy lacked a batch this node holds:
// this node claims the rest (a follow-up claim), though not the primary.
func TestClaimantFollowUpClaim(t *testing.T) {
	r := newClaimantRig(t)
	ctx := context.Background()
	if err := r.c.raftNode.AddNode(&raft.NodeInfo{ID: "writer-3", Name: "writer-3", Role: string(RoleWriter), ClusterName: "test-cluster", Address: "writer-3:9100", State: string(StateHealthy)}, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	if err := r.c.raftNode.PromoteWriter("writer-3", "writer-1", 5*time.Second); err != nil {
		t.Fatal(err)
	}
	waitFSM(t, func() bool { return r.c.designatedPrimaryWriterID() == "writer-3" })
	propose(t, r.c, raft.CommandTakeOverStreamV2, raft.TakeOverStreamV2Payload{Origin: clOrigin, By: "writer-3", Now: time.Now(),
		Claims: []raft.ClaimSet{{Instance: clInst, Keys: map[string]raft.Intervals{clKey: {{1, 1}}}}}})
	k, _ := r.claim(t)
	propose(t, r.c, raft.CommandClaimDone, raft.ClaimDonePayload{Instance: clInst, By: "writer-3", Epoch: k.Epoch})
	r.stored(t, 1, 1)
	r.stored(t, 2, 1)
	r.afterGrace()
	r.cl.pass(ctx)
	k, _ = r.claim(t)
	if k.By != "writer-1" || len(k.FinishedBy) != 1 || k.FinishedBy[0] != "writer-3" || !k.Set[clKey].Contains(2) {
		t.Fatalf("follow-up claim %+v", k)
	}
	waitFSM(t, func() bool { return r.outbox.Pending() == 0 })
	r.cl.pass(ctx)
	r.cl.pass(ctx)
	if k, _ = r.claim(t); !k.Done || len(k.Unreplayable) != 0 {
		t.Fatalf("follow-up claim not finished: %+v", k)
	}
	// Finished: not claimed again by this node.
	r.stored(t, 3, 1)
	r.cl.pass(ctx)
	if k2, _ := r.claim(t); k2.Epoch != k.Epoch || len(r.cl.backoff) != 0 {
		t.Fatalf("claimed again (or proposed: backoff %v): %+v", r.cl.backoff, k2)
	}
}

func TestAutoReleaseOfAGoneClaimant(t *testing.T) {
	c, _ := takeoverOriginRig(t, true)
	propose(t, c, raft.CommandAnnounceInstance, raft.AnnounceInstancePayload{Origin: "writer-1", Instance: 0x100, FreshStart: true})
	propose(t, c, raft.CommandAnnounceInstance, raft.AnnounceInstancePayload{Origin: "writer-1", Instance: 0x101, FreshStart: true})
	// 0x100 is orphaned: writer-2 may claim it.
	propose(t, c, raft.CommandTakeOverStreamV2, raft.TakeOverStreamV2Payload{Origin: "writer-1", By: "writer-2", Now: time.Now(),
		Claims: []raft.ClaimSet{{Instance: 0x100, Keys: map[string]raft.Intervals{"db/cpu": {{1, 1}}}}}})
	gone := map[[2]uint64]time.Time{}
	now := time.Now()
	released := func() bool {
		for _, k := range c.raftNode.FSM().StreamClaims() {
			if k.Instance == 0x100 {
				return k.Released
			}
		}
		return false
	}
	c.autoReleasePass(now, time.Minute, gone)
	c.autoReleasePass(now.Add(30*time.Second), time.Minute, gone)
	if released() {
		t.Fatal("released within the grace")
	}
	// Back in time: the observation restarts.
	peer := NewNode("writer-2", "writer-2", RoleWriter, "test-cluster")
	peer.UpdateState(StateHealthy)
	if err := c.registry.Register(peer); err != nil {
		t.Fatal(err)
	}
	c.autoReleasePass(now.Add(2*time.Minute), time.Minute, gone)
	if released() || len(gone) != 0 {
		t.Fatal("a healthy claimant's claim released")
	}
	c.registry.Unregister("writer-2")
	c.autoReleasePass(now.Add(3*time.Minute), time.Minute, gone)
	c.autoReleasePass(now.Add(3*time.Minute+59*time.Second), time.Minute, gone)
	if released() {
		t.Fatal("released before a whole grace gone")
	}
	c.autoReleasePass(now.Add(4*time.Minute+time.Second), time.Minute, gone)
	if !released() {
		t.Fatal("a gone claimant's claim kept")
	}
}

func TestReleaseStreamClaimAPI(t *testing.T) {
	c, _ := takeoverOriginRig(t, true)
	propose(t, c, raft.CommandAnnounceInstance, raft.AnnounceInstancePayload{Origin: "writer-2", Instance: 0x100, FreshStart: true})
	propose(t, c, raft.CommandAnnounceInstance, raft.AnnounceInstancePayload{Origin: "writer-2", Instance: 0x101, FreshStart: true})
	propose(t, c, raft.CommandTakeOverStreamV2, raft.TakeOverStreamV2Payload{Origin: "writer-2", By: "writer-1", Now: time.Now(),
		Claims: []raft.ClaimSet{{Instance: 0x100, Keys: map[string]raft.Intervals{"db/cpu": {{1, 1}}}}}})
	k := c.raftNode.FSM().ClaimsBy("writer-1")[0]
	if err := c.ReleaseStreamClaim(0x100, k.Epoch+1, false); !errors.Is(err, ErrStreamClaimNotFound) {
		t.Fatalf("wrong epoch: %v", err)
	}
	if err := c.ReleaseStreamClaim(0x100, k.Epoch, false); err != nil {
		t.Fatal(err)
	}
	if err := c.ReleaseStreamClaim(0x100, k.Epoch, false); !errors.Is(err, ErrStreamClaimNotFound) {
		t.Fatalf("released twice: %v", err)
	}
	// A finished claim (of the current instance: it is kept).
	propose(t, c, raft.CommandTakeOverStreamV2, raft.TakeOverStreamV2Payload{Origin: "writer-2", By: "writer-1", Now: time.Now(),
		Claims: []raft.ClaimSet{{Instance: 0x101, Keys: map[string]raft.Intervals{"db/cpu": {{1, 1}}}}}})
	for _, x := range c.raftNode.FSM().ClaimsBy("writer-1") {
		if x.Instance == 0x101 {
			k = x
		}
	}
	propose(t, c, raft.CommandClaimDone, raft.ClaimDonePayload{Instance: 0x101, By: "writer-1", Epoch: k.Epoch})
	if err := c.ReleaseStreamClaim(0x101, k.Epoch, false); !errors.Is(err, ErrStreamClaimFinished) {
		t.Fatalf("finished: %v", err)
	}
	if err := c.ReleaseStreamClaim(0x101, k.Epoch, true); !errors.Is(err, ErrStreamClaimFinished) {
		t.Fatalf("abandon a finished claim: %v", err)
	}
	// Abandon an unfinished one (0x100, released above, claimed again).
	propose(t, c, raft.CommandTakeOverStreamV2, raft.TakeOverStreamV2Payload{Origin: "writer-2", By: "writer-1", Now: time.Now(),
		Claims: []raft.ClaimSet{{Instance: 0x100, Keys: map[string]raft.Intervals{"db/cpu": {{1, 1}}}}}})
	var open raft.StreamClaim
	for _, x := range c.raftNode.FSM().StreamClaims() {
		if x.Instance == 0x100 && !x.Released {
			open = x
		}
	}
	if err := c.ReleaseStreamClaim(0x100, open.Epoch, true); err != nil {
		t.Fatal(err)
	}
	for _, x := range c.raftNode.FSM().StreamClaims() {
		if x.Instance == 0x100 && (!x.Done || !x.Abandoned || x.Unreplayable["db/cpu"].Count() != 1) {
			t.Fatalf("abandoned %+v", x)
		}
	}
}

// A claimed batch not in the store: done when its origin's floors say it is
// registered, unreplayable otherwise.
func TestClaimantAbsentBatches(t *testing.T) {
	r := newClaimantRig(t)
	ctx := context.Background()
	r.stored(t, 1, 1)
	// The origin's floors (as last received): batch 3 is registered.
	if err := r.store.AcceptFloors(clOrigin, &originstream.Floors{Instance: clInst, AppliedIndex: 1,
		Keys: map[string]originstream.KeyFloor{clKey: {Settled: [][2]uint64{{3, 3}}}}}); err != nil {
		t.Fatal(err)
	}
	propose(t, r.c, raft.CommandTakeOverStreamV2, raft.TakeOverStreamV2Payload{Origin: clOrigin, By: "writer-1", Now: time.Now(),
		Claims: []raft.ClaimSet{{Instance: clInst, Keys: map[string]raft.Intervals{clKey: {{1, 3}}}}}})
	r.cl.pass(ctx)
	waitFSM(t, func() bool { return r.outbox.Pending() == 0 })
	r.cl.pass(ctx)
	r.cl.pass(ctx)
	k, _ := r.claim(t)
	if !k.Done || k.Unreplayable[clKey].Count() != 1 || !k.Unreplayable[clKey].Contains(2) {
		t.Fatalf("claim %+v: want batch 2 unreplayable only", k)
	}
}

// A writer this node hears from, with nothing claimable left: its frozen
// copy goes while this node is the designated primary. A writer it does not
// hear from keeps its copy.
func TestClaimantDropsAFrozenCopy(t *testing.T) {
	r := newClaimantRig(t)
	ctx := context.Background()
	r.stored(t, 1, 1)
	propose(t, r.c, raft.CommandSettleCoverage, raft.SettleCoveragePayload{Origin: clOrigin, Instance: clInst,
		Keys: map[string]raft.KeySettlement{clKey: {Floor: 1}}})
	r.cl.pass(ctx)
	if r.store.Held(clOrigin, clInst) == nil {
		t.Fatal("dropped the copy of a writer this node does not hear from")
	}
	peer := NewNode(clOrigin, clOrigin, RoleWriter, "test-cluster")
	peer.UpdateState(StateHealthy)
	if err := r.c.registry.Register(peer); err != nil {
		t.Fatal(err)
	}
	// A current file of the key is not here yet: the copy waits.
	propose(t, r.c, raft.CommandRegisterFile, raft.RegisterFilePayload{File: raft.FileEntry{Path: "db/cpu/remote.parquet", SHA256: "x",
		SizeBytes: 3, Database: "db", Measurement: "cpu", PartitionTime: r.hour, OriginNodeID: clOrigin, CreatedAt: time.Now()}})
	r.cl.pass(ctx)
	if r.store.Held(clOrigin, clInst) == nil {
		t.Fatal("dropped a copy while a file of its key is not here")
	}
	if err := r.backend.Write(ctx, "db/cpu/remote.parquet", []byte("abc")); err != nil {
		t.Fatal(err)
	}
	r.cl.pass(ctx)
	if r.store.Held(clOrigin, clInst) != nil {
		t.Fatal("kept a frozen copy with nothing claimable")
	}
}

// A claim that opened with part of what this node holds grows to the rest.
func TestClaimantExtendsItsClaim(t *testing.T) {
	r := newClaimantRig(t)
	ctx := context.Background()
	for c := uint64(1); c <= 3; c++ {
		r.stored(t, c, 1)
	}
	r.cl.replay = &fakeReplayer{outcome: ingest.ReplayWritten}
	propose(t, r.c, raft.CommandTakeOverStreamV2, raft.TakeOverStreamV2Payload{Origin: clOrigin, By: "writer-1", Now: time.Now(),
		Claims: []raft.ClaimSet{{Instance: clInst, Keys: map[string]raft.Intervals{clKey: {{1, 1}}}}}})
	r.cl.pass(ctx)
	if k, _ := r.claim(t); k.Set[clKey].Count() != 3 {
		t.Fatalf("claim not extended to what this node holds: %+v", k)
	}
}

// fakeOutbox reports scripted files of an instance.
type fakeOutbox struct{ files []InstanceFile }

func (f *fakeOutbox) InstanceFiles(uint64) []InstanceFile { return f.files }

// A file of this claim the manifest refused for good: its batches are
// unreplayable, and the claim finishes.
func TestClaimantParkedFileOfItsClaim(t *testing.T) {
	r := newClaimantRig(t)
	ctx := context.Background()
	r.stored(t, 1, 1)
	r.stored(t, 2, 1)
	propose(t, r.c, raft.CommandTakeOverStreamV2, raft.TakeOverStreamV2Payload{Origin: clOrigin, By: "writer-1", Now: time.Now(),
		Claims: []raft.ClaimSet{{Instance: clInst, Keys: map[string]raft.Intervals{clKey: {{1, 2}}}}}})
	k, _ := r.claim(t)
	r.cl.replay = &fakeReplayer{outcome: ingest.ReplayCovered}
	r.cl.outbox = &fakeOutbox{files: []InstanceFile{{Path: "p", Key: clKey, Epoch: k.Epoch, Ranges: [][2]uint64{{2, 2}}, Parked: true, Kind: raft.RefusedFull}}}
	r.cl.pass(ctx)
	r.cl.pass(ctx)
	if k, _ = r.claim(t); !k.Done || k.Unreplayable[clKey].Count() != 1 || !k.Unreplayable[clKey].Contains(2) {
		t.Fatalf("claim %+v", k)
	}
}

// The copy of an instance removed with its claim (every writer answered)
// goes once its files are here.
func TestClaimantDropsATombstonedCopy(t *testing.T) {
	r := newClaimantRig(t)
	ctx := context.Background()
	r.stored(t, 1, 1)
	// writer-2 restarts on the same disks without it: closed; writer-1
	// claims, publishes.
	propose(t, r.c, raft.CommandAnnounceInstance, raft.AnnounceInstancePayload{Origin: clOrigin, Instance: 0x600, FreshStart: true,
		Continuity: raft.Continuity{WAL: clInst, Journal: clInst, Storage: clInst}})
	r.cl.pass(ctx)
	waitFSM(t, func() bool { return r.outbox.Pending() == 0 })
	r.cl.pass(ctx)
	r.cl.pass(ctx)
	if r.store.Held(clOrigin, clInst) != nil {
		// Finished by this node: dropped already (files local).
		t.Fatal("copy of a finished claim kept")
	}
	// Stored again (say, still arriving) after the drop; the rig's other
	// writer ("raft-1") answers: the instance goes.
	r.stored(t, 1, 1)
	for _, n := range r.c.raftNode.FSM().GetNodesByRole(string(RoleWriter)) {
		if n.ID != "writer-1" && n.ID != clOrigin {
			propose(t, r.c, raft.CommandTakeOverStreamV2, raft.TakeOverStreamV2Payload{Origin: clOrigin, By: n.ID, Now: time.Now(),
				Claims: []raft.ClaimSet{{Instance: clInst}}})
		}
	}
	if streamInstance(r.c, clInst) != nil || !r.c.raftNode.FSM().Tombstoned(clInst) {
		k, _ := r.claim(t)
		t.Fatalf("instance not removed: %+v claim %+v", streamInstance(r.c, clInst), k)
	}
	r.cl.pass(ctx)
	if r.store.Held(clOrigin, clInst) != nil {
		t.Fatal("copy of a removed instance kept")
	}
}

// A claim larger than one replay call is read and replayed in slices.
func TestClaimantReplaysInSlices(t *testing.T) {
	r := newClaimantRig(t)
	ctx := context.Background()
	const n = replayCallRecords + 40
	for c := uint64(1); c <= n; c++ {
		r.stored(t, c, 1)
	}
	f := &fakeReplayer{outcome: ingest.ReplayCovered}
	r.cl.replay = f
	r.afterGrace()
	r.cl.pass(ctx)
	r.cl.pass(ctx)
	if k, _ := r.claim(t); !k.Done || len(k.Unreplayable) != 0 {
		t.Fatalf("claim %+v", k)
	}
	total := 0
	for _, c := range f.calls {
		if len(c) > replayCallRecords {
			t.Fatalf("a call of %d records", len(c))
		}
		total += len(c)
	}
	if len(f.calls) < 2 || total != n {
		t.Fatalf("%d calls, %d records", len(f.calls), total)
	}
}

// A released claim on this process's instance does not stop it.
func TestReleasedClaimDoesNotExit(t *testing.T) {
	c, d := takeoverOriginRig(t, true)
	if err := c.raftNode.PromoteWriter("writer-2", "writer-1", 5*time.Second); err != nil {
		t.Fatal(err)
	}
	waitFSM(t, func() bool { return c.designatedPrimaryWriterID() == "writer-2" })
	c.SetTakeoverOrigin(d.origin(0x100, noHolds, nil))
	c.originPass(t)
	propose(t, c, raft.CommandTakeOverStreamV2, raft.TakeOverStreamV2Payload{Origin: "writer-1", By: "writer-2", Now: time.Now(),
		Claims: []raft.ClaimSet{{Instance: 0x100, Keys: map[string]raft.Intervals{"db/cpu": {{1, 1}}}}}})
	e := c.raftNode.FSM().ClaimsBy("writer-2")[0].Epoch
	propose(t, c, raft.CommandReleaseStreamClaim, raft.ReleaseStreamClaimPayload{Instance: 0x100, Epoch: e})
	c.originPass(t)
	if c.claimedExit.Load() {
		t.Fatal("a released claim stopped the process")
	}
}
