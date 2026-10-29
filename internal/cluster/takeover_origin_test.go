package cluster

// #1207 S4b: the origin side of stream takeover, against a real Raft FSM.

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/ingest"
)

type originDirs struct{ wal, journal, storage string }

func takeoverOriginRig(t *testing.T, enable bool) (*Coordinator, originDirs) {
	t.Helper()
	c := originRig(t, "writer-1", RoleWriter)
	c.cfg.WALCoverage, c.cfg.StreamTakeover = true, true
	if enable {
		capNodes(t, c, map[string][]string{"raft-1": capList, "writer-1": capList, "writer-2": capList, "reader-1": capList})
		c.maybeEnableStreamClaims()
		waitFSM(t, c.StreamClaimsEnabled)
	}
	return c, originDirs{t.TempDir(), t.TempDir(), t.TempDir()}
}

func (d originDirs) origin(inst uint64, holds func() ([]uint64, error), progress func(map[string]uint64) (uint64, map[string]ingest.KeySettlement, bool)) TakeoverOrigin {
	return TakeoverOrigin{Instance: inst, WALDir: d.wal, JournalDir: d.journal, StorageBase: d.storage, Holds: holds, Progress: progress}
}

func noHolds() ([]uint64, error) { return nil, nil }

func (c *Coordinator) originPass(t *testing.T) {
	t.Helper()
	var a, b time.Time
	c.takeoverOriginPass(context.Background(), c.takeoverOrigin.Load(), &a, &b)
}

func streamInstance(c *Coordinator, id uint64) *raft.StreamInstance {
	for _, in := range c.raftNode.FSM().StreamInstances() {
		if in.Instance == id {
			return &in
		}
	}
	return nil
}

// Not known on: writes are admitted. Once the flag is seen, writes wait for
// the announcement, which then opens them and records the markers.
func TestAnnouncementGatesWrites(t *testing.T) {
	c, d := takeoverOriginRig(t, false)
	c.SetTakeoverOrigin(d.origin(0x100, noHolds, nil))
	if err := c.IngestGate()(); err != nil {
		t.Fatalf("gated before the cluster turned takeover on: %v", err)
	}
	capNodes(t, c, map[string][]string{"raft-1": capList, "writer-1": capList, "writer-2": capList, "reader-1": capList})
	c.maybeEnableStreamClaims()
	waitFSM(t, c.StreamClaimsEnabled)
	c.originPass(t)
	if err := c.IngestGate()(); err != nil {
		t.Fatalf("still gated after the announcement: %v", err)
	}
	in := streamInstance(c, 0x100)
	if in == nil || in.Status != raft.InstanceCurrent || in.Untracked {
		t.Fatalf("instance %+v: nothing was sent before the announcement, so it is a fresh start", in)
	}
	for _, p := range []string{filepath.Join(d.wal, takeoverMarkerName), filepath.Join(d.journal, takeoverMarkerName),
		filepath.Join(d.storage, takeoverInstancesDir, "writer-1."+takeoverMarkerName)} {
		if readInstanceMarker(p) != 0x100 {
			t.Fatalf("marker %s not written", p)
		}
	}
	if _, err := os.Stat(filepath.Join(d.journal, takeoverOnMarkerName)); err != nil {
		t.Fatal("takeover-on marker not written")
	}
}

// The next start knows takeover is on from its marker: writes and
// registrations wait from the first moment, and the instance announces as a
// fresh start with continuity and its holds.
func TestRestartAnnouncesWithContinuityAndHolds(t *testing.T) {
	c, d := takeoverOriginRig(t, true)
	c.SetTakeoverOrigin(d.origin(0x100, noHolds, nil))
	c.originPass(t)

	c2 := c // same cluster and node; a new process
	c2.takeoverOrigin.Store(nil)
	c2.SetTakeoverOrigin(d.origin(0x200, func() ([]uint64, error) { return []uint64{0x100, 0x200}, nil }, nil))
	if err := c2.IngestGate()(); err == nil || !errors.Is(err, ingest.ErrIngestFenced) {
		t.Fatalf("writes admitted before the announcement: %v", err)
	}
	if c2.TakeoverSendGate()() {
		t.Fatal("registrations sent before the announcement")
	}
	c2.originPass(t)
	if c2.IngestGate()() != nil || !c2.TakeoverSendGate()() {
		t.Fatal("gates still closed after the announcement")
	}
	if in := streamInstance(c2, 0x200); in == nil || in.Untracked {
		t.Fatalf("restart with the marker is a fresh start: %+v", in)
	}
	if in := streamInstance(c2, 0x100); in == nil || in.Status != raft.InstanceAdopted || in.AdoptedBy != 0x200 {
		t.Fatalf("held earlier instance: %+v", in)
	}

	// A third start holding nothing, same disks: the earlier ones close.
	c2.takeoverOrigin.Store(nil)
	c2.SetTakeoverOrigin(d.origin(0x300, noHolds, nil))
	c2.originPass(t)
	if in := streamInstance(c2, 0x200); in == nil || in.Status != raft.InstanceClosed {
		t.Fatalf("same disks, nothing held: %+v", in)
	}
}

// A listing failure stops the announcement: an incomplete Holds would
// declare held batches lost. Writes keep waiting.
func TestAnnouncementWaitsForACompleteHoldsList(t *testing.T) {
	c, d := takeoverOriginRig(t, true)
	if err := writeOnMarker(d.journal); err != nil {
		t.Fatal(err)
	}
	c.SetTakeoverOrigin(d.origin(0x100, func() ([]uint64, error) { return nil, errors.New("disk error") }, nil))
	c.originPass(t)
	if streamInstance(c, 0x100) != nil || c.IngestGate()() == nil {
		t.Fatal("announced without its holds")
	}
}

func TestSettleCoverageAndInstanceSettled(t *testing.T) {
	c, d := takeoverOriginRig(t, true)
	if err := writeOnMarker(d.journal); err != nil {
		t.Fatal(err)
	}
	c.SetTakeoverOrigin(d.origin(0x100, noHolds, nil))
	c.originPass(t)
	floors := map[string]ingest.KeySettlement{"db/cpu": {Floor: 7, Lost: [][2]uint64{{3, 3}}}}
	held := []uint64{0x100}
	c.takeoverOrigin.Store(nil)
	c.SetTakeoverOrigin(d.origin(0x200, func() ([]uint64, error) { return held, nil },
		func(map[string]uint64) (uint64, map[string]ingest.KeySettlement, bool) { return 0x200, floors, true }))
	c.originPass(t)
	fsm := c.raftNode.FSM()
	if fsm.CoverageCovered(0x200, "db/cpu", raft.CoveragePiece{CSeq: 3}, time.Now()) || !fsm.CoverageCovered(0x200, "db/cpu", raft.CoveragePiece{CSeq: 5}, time.Now()) {
		t.Fatal("settled floors not applied (5 settled, 3 lost)")
	}
	// Still held: not settled.
	if in := streamInstance(c, 0x100); in == nil || in.Status != raft.InstanceAdopted {
		t.Fatalf("held instance: %+v", in)
	}
	held = nil
	c.settleInstances(context.Background(), c.takeoverOrigin.Load())
	waitFSM(t, func() bool { return streamInstance(c, 0x100) == nil })
}

// An instance that already sent a covered registration before announcing
// (takeover turned on mid-run) is not a fresh start: never trusted.
func TestAnnouncementAfterCoveredSendsIsUntracked(t *testing.T) {
	c, d := takeoverOriginRig(t, true)
	o := d.origin(0x100, noHolds, nil)
	o.SentCovered = func() bool { return true }
	c.SetTakeoverOrigin(o)
	c.originPass(t)
	if in := streamInstance(c, 0x100); in == nil || !in.Untracked {
		t.Fatalf("instance %+v", in)
	}
}

// The marker says takeover is on, the caught-up manifest says it is not: the
// marker was wrong; writes are admitted.
func TestStaleTakeoverMarkerIsCleared(t *testing.T) {
	c, d := takeoverOriginRig(t, false)
	if err := writeOnMarker(d.journal); err != nil {
		t.Fatal(err)
	}
	c.SetTakeoverOrigin(d.origin(0x100, noHolds, nil))
	if c.IngestGate()() == nil {
		t.Fatal("the marker did not close the gate")
	}
	c.originPass(t)
	if err := c.IngestGate()(); err != nil {
		t.Fatalf("still gated after the manifest showed takeover off: %v", err)
	}
	if _, err := os.Stat(filepath.Join(d.journal, takeoverOnMarkerName)); err == nil {
		t.Fatal("stale marker kept")
	}
}

// Batches of another writer's instance held here (a claimant's replay files)
// are not announced as this origin's holds.
func TestAnnouncementLeavesOutOtherWritersInstances(t *testing.T) {
	c, d := takeoverOriginRig(t, true)
	raw, _ := json.Marshal(raft.AnnounceInstancePayload{Origin: "writer-2", Instance: 0x900, FreshStart: true})
	if err := c.applyOrForward(context.Background(), &raft.Command{Type: raft.CommandAnnounceInstance, Payload: raw}); err != nil {
		t.Fatal(err)
	}
	if err := writeOnMarker(d.journal); err != nil {
		t.Fatal(err)
	}
	c.SetTakeoverOrigin(d.origin(0x100, noHolds, nil))
	c.originPass(t)
	c.takeoverOrigin.Store(nil)
	c.SetTakeoverOrigin(d.origin(0x200, func() ([]uint64, error) { return []uint64{0x100, 0x900, 0x200}, nil }, nil))
	holds, err := c.announcedHolds(c.takeoverOrigin.Load())
	if err != nil || len(holds) != 1 || holds[0] != 0x100 {
		t.Fatalf("announced holds %v, %v", holds, err)
	}
}
