package cluster

import (
	"context"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/filereplication"
	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/rs/zerolog"
)

// #1831: a committed compaction output moved into place from this node's
// staged copy by the sweep — because the pull the commit queued held the
// path's slot — reached disk and the manifest but never the tiering index.

func (r *fakeTierRecorder) pulledReports() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.pulled...)
}

// The reported race, with a real puller: the commit's pull is queued (and
// holds the slot), the staged sweep adopts the output directly, then the
// queued pull finds the file at its size and skips it. The file must end up
// reported to tiering.
func TestSweptAdoptionIsRecordedWhileThePullHoldsTheSlot1831(t *testing.T) {
	r := stagedRig(t)
	ctx := context.Background()
	rec := &fakeTierRecorder{}
	r.c.SetTierRecorder(rec)
	job := r.job("j1")
	r.commitJob(job)
	e, _ := r.c.raftNode.FSM().GetFile(job.Output.Path)

	f := &recordFetcher{paths: make(chan string, 8)}
	p, err := filereplication.New(filereplication.Config{
		SelfNodeID: "writer-2", Backend: r.backend, Fetcher: f, PeerResolver: oneResolver{},
		Workers: 1, QueueSize: 8, RetryMaxAttempts: 1, RetryInitialBackoff: time.Millisecond,
		FetchTimeout: time.Second, RepullMissingSelfOrigin: true, AdoptStaged: r.c.adoptStagedForPull,
		RecordPulledFile: r.c.recordPulledFileInTiering,
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
	r.c.puller = p
	// The commit's registration queued a pull; the puller has not run it
	// yet (a backlog), so it holds the path's slot.
	// As on a writer: the compactor is the output's origin.
	queued := *e
	queued.OriginNodeID = "compactor-1"
	p.Enqueue(&queued)
	if p.AdoptNow(e) {
		t.Fatal("setup: the queued pull does not hold the slot")
	}
	r.c.sweepStagedDirs(ctx)
	if !r.exists(job.Output.Path) {
		t.Fatal("the sweep did not move the output into place")
	}
	p.Start(ctx)
	defer p.Stop()
	deadline := time.Now().Add(3 * time.Second)
	for p.Pending(job.Output.Path) && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	got := rec.pulledReports()
	n := 0
	for _, path := range got {
		if path == job.Output.Path {
			n++
		}
	}
	if n == 0 {
		t.Fatalf("the adopted output was never reported to tiering (reports %v)", got)
	}
	select {
	case fetched := <-f.paths:
		t.Fatalf("the output was fetched again (%s) although it was in place", fetched)
	default:
	}
}

// The direct adopt reports; the puller's path (adoptStagedForPull) does not,
// because the puller reports its own adoptions.
func TestOnlyTheDirectAdoptReports1831(t *testing.T) {
	r := stagedRig(t)
	ctx := context.Background()
	rec := &fakeTierRecorder{}
	r.c.SetTierRecorder(rec)

	j1 := r.job("j1")
	r.commitJob(j1)
	e1, _ := r.c.raftNode.FSM().GetFile(j1.Output.Path)
	if ok, err := r.c.adoptStagedForPull(ctx, e1); !ok || err != nil {
		t.Fatalf("adopt for pull: %v %v", ok, err)
	}
	if got := rec.pulledReports(); len(got) != 0 {
		t.Fatalf("the puller's path reported %v; the puller reports it, so this would be twice", got)
	}

	j2 := r.job("j2")
	r.commitJob(j2)
	e2, _ := r.c.raftNode.FSM().GetFile(j2.Output.Path)
	if ok, err := r.c.adoptStaged(ctx, e2); !ok || err != nil {
		t.Fatalf("adopt: %v %v", ok, err)
	}
	if got := rec.pulledReports(); len(got) != 1 || got[0] != j2.Output.Path {
		t.Fatalf("direct adopt reported %v, want [%s]", got, j2.Output.Path)
	}
	// Present already: still the output, reported again (idempotent upsert).
	if ok, _ := r.c.adoptStaged(ctx, e2); !ok {
		t.Fatal("an output already in place was not adopted")
	}
	if got := rec.pulledReports(); len(got) != 2 {
		t.Fatalf("present output not reported: %v", got)
	}
	// Present at its size but no longer in the manifest: not adopted, not
	// reported.
	if err := r.c.raftNode.DeleteFile(j2.Output.Path, "delete", 5*time.Second); err != nil {
		t.Fatal(err)
	}
	if ok, _ := r.c.adoptStaged(ctx, e2); ok {
		t.Fatal("a file that left the manifest was counted as adopted")
	}
	if got := rec.pulledReports(); len(got) != 2 {
		t.Fatalf("a file that left the manifest was reported: %v", got)
	}
}
