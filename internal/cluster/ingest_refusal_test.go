package cluster

// #1268: every refused live write is counted once, under its reason, and the
// reason survives the wrapping so the API can log refusals by reason.

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/basekick-labs/arc/internal/metrics"
)

func TestIngestRefusalReason(t *testing.T) {
	for want, err := range map[string]error{
		RefusalClaimed:       fmt.Errorf("%w: %w", ingest.ErrIngestFenced, errTakeoverClaimed),
		RefusalAnnouncing:    fmt.Errorf("%w: %w", ingest.ErrIngestFenced, errTakeoverAnnouncing),
		RefusalNotDesignated: fmt.Errorf("%w: %w", ingest.ErrIngestFenced, errNotDesignated),
		RefusalDeleteAck:     fmt.Errorf("%w: %w", ingest.ErrIngestFenced, errDeleteAckPending),
		"other":              ingest.ErrIngestFenced,
		"":                   errors.New("disk full"),
	} {
		if got := IngestRefusalReason(err); got != want {
			t.Errorf("IngestRefusalReason(%v) = %q, want %q", err, got, want)
		}
	}
	if IngestRefusalReason(nil) != "" {
		t.Error("nil classified as a refusal")
	}
	// Every reason is a label the counter exports, not folded into "other".
	known := map[string]bool{}
	for _, r := range metrics.IngestFencedReasons {
		known[r] = true
	}
	for _, r := range []string{RefusalContact, RefusalStandby, RefusalClaimed, RefusalAnnouncing, RefusalNotDesignated, RefusalDeleteAck} {
		if !known[r] {
			t.Errorf("reason %q is not an arc_cluster_ingest_fenced_writes_total label", r)
		}
	}
}

// The fence's own refusals carry standby or contact.
func TestIngestFenceRefusalClass(t *testing.T) {
	r := newFenceRig()
	r.primary = "w2"
	r.f.evaluate()
	if got := IngestRefusalReason(r.f.gate()); got != RefusalStandby {
		t.Fatalf("standby refusal classified %q", got)
	}
	r = newFenceRig()
	r.now = r.now.Add(8 * time.Second)
	r.f.evaluate()
	err := r.f.gate()
	if got := IngestRefusalReason(err); got != RefusalContact {
		t.Fatalf("contact refusal classified %q (%v)", got, err)
	}
	if !errors.Is(err, ingest.ErrIngestFenced) {
		t.Fatalf("%v does not wrap ErrIngestFenced", err)
	}
}

// IngestGate counts each refusal exactly once, under its reason; the fence's
// refusals (which used to be the only ones counted) included.
func TestIngestGateCountsEachRefusalOnce(t *testing.T) {
	m := metrics.Get()
	total := func() int64 {
		var n int64
		for _, r := range metrics.IngestFencedReasons {
			n += m.IngestFencedWritesFor(r)
		}
		return n
	}

	c := &Coordinator{}
	r := newFenceRig()
	r.primary = "w2"
	r.f.evaluate()
	c.ingestFence.Store(r.f)
	before, beforeTotal := m.IngestFencedWritesFor(RefusalStandby), total()
	if err := c.IngestGate()(); IngestRefusalReason(err) != RefusalStandby {
		t.Fatalf("gate: %v", err)
	}
	if d := m.IngestFencedWritesFor(RefusalStandby) - before; d != 1 {
		t.Fatalf("standby refusal counted %d times", d)
	}
	if d := total() - beforeTotal; d != 1 {
		t.Fatalf("one refusal added %d to the counter", d)
	}

	c.claimedExit.Store(true)
	before, beforeTotal = m.IngestFencedWritesFor(RefusalClaimed), total()
	if err := c.IngestGate()(); !errors.Is(err, errTakeoverClaimed) {
		t.Fatalf("gate: %v", err)
	}
	if d := m.IngestFencedWritesFor(RefusalClaimed) - before; d != 1 {
		t.Fatalf("claimed refusal counted %d times", d)
	}
	if d := total() - beforeTotal; d != 1 {
		t.Fatalf("one refusal added %d to the counter", d)
	}

	// An admitted write counts nothing.
	open := &Coordinator{}
	beforeTotal = total()
	if err := open.IngestGate()(); err != nil {
		t.Fatal(err)
	}
	if total() != beforeTotal {
		t.Fatal("an admitted write was counted as refused")
	}
}
