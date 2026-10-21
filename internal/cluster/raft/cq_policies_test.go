package raft

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

func enabledCQFSM(t *testing.T) *tkFSM {
	t.Helper()
	x := cpFSM(t)
	x.ok(CommandEnableCQ, EnableCQPayload{Nodes: allNodes, Now: t0})
	x.ok(CommandPromoteWriter, PromoteWriterPayload{NodeID: "w1"})
	x.ok(CommandCollectPolicies, CollectPoliciesPayload{Kind: PolicyKindCQ, Leader: "w1", Now: t0})
	x.ok(CommandSeedCQ, SeedCQPayload{Epoch: x.idx, Floor: 1, Now: t0, Entries: []CQPolicyEntry{{ID: 1, Name: "hourly", Database: "db", SourceMeasurement: "cpu", DestinationMeasurement: "cpu_hourly", Query: "SELECT count(*) FROM db.cpu", Interval: "1h", IsActive: true, Version: 1}}})
	return x
}

func TestCQCompletionFencesDefinitionPrimaryAndWatermark(t *testing.T) {
	x := enabledCQFSM(t)
	cq, _ := x.f.CQ(1)
	epoch := x.f.PrimaryEpoch()
	end := t0.Add(time.Hour)
	p := CompleteCQPayload{ID: 1, Version: cq.Version, AuthorityEpoch: epoch, Executor: "w1", ExpectedStart: time.Time{}, End: end}
	x.ok(CommandCompleteCQ, p)
	got, _ := x.f.CQ(1)
	if got.LastProcessedTime == nil || !got.LastProcessedTime.Equal(end) {
		t.Fatalf("watermark=%v", got.LastProcessedTime)
	}
	if res := x.apply(CommandCompleteCQ, p); !errors.Is(asErr(res), ErrCQConflict) {
		t.Fatalf("duplicate old completion=%v", res)
	}
	p.ExpectedStart = end
	p.End = end.Add(time.Hour)
	p.AuthorityEpoch = epoch + 1
	if res := x.apply(CommandCompleteCQ, p); !errors.Is(asErr(res), ErrCQConflict) {
		t.Fatalf("stale epoch=%v", res)
	}
	// The definition version is checked before progress, so an old run cannot
	// complete after an administrator updates the query.
	x.ok(CommandUpdateCQ, UpdateCQPayload{ID: 1, ExpectedVersion: 1, Entry: CQPolicyEntry{Name: "hourly", Database: "db", SourceMeasurement: "cpu", DestinationMeasurement: "cpu_hourly", Query: "SELECT sum(v) FROM db.cpu", Interval: "1h", IsActive: true}, Now: t1})
	p.Version = 1
	p.AuthorityEpoch = epoch
	p.Executor = "w1"
	p.ExpectedStart = end
	p.End = end.Add(time.Hour)
	if res := x.apply(CommandCompleteCQ, p); !errors.Is(asErr(res), ErrCQConflict) {
		t.Fatalf("stale definition=%v", res)
	}
}

func TestCQCRUDUsesVersionsUniqueNamesAndMonotonicIDs(t *testing.T) {
	x := enabledCQFSM(t)
	if res := x.apply(CommandCreateCQ, CreateCQPayload{Entry: CQPolicyEntry{Name: "hourly", Database: "db", SourceMeasurement: "cpu", DestinationMeasurement: "out", Query: "SELECT 1", Interval: "1h"}, Now: t1}); !errors.Is(asErr(res), ErrCQNameTaken) {
		t.Fatalf("duplicate create=%v", res)
	}
	x.ok(CommandUpdateCQ, UpdateCQPayload{ID: 1, ExpectedVersion: 1, Entry: CQPolicyEntry{Name: "hourly-v2", Database: "db", SourceMeasurement: "cpu", DestinationMeasurement: "out", Query: "SELECT 1", Interval: "1h"}, Now: t1})
	updated, ok := x.f.CQ(1)
	if !ok || updated.Version != 2 || updated.Name != "hourly-v2" {
		t.Fatalf("updated CQ=%+v", updated)
	}
	if res := x.apply(CommandUpdateCQ, UpdateCQPayload{ID: 1, ExpectedVersion: 1, Entry: updated, Now: t1}); !errors.Is(asErr(res), ErrCQConflict) {
		t.Fatalf("stale update=%v", res)
	}
	x.ok(CommandDeleteCQ, DeleteCQPayload{ID: 1, ExpectedVersion: 2})
	x.ok(CommandCreateCQ, CreateCQPayload{Entry: CQPolicyEntry{Name: "next", Database: "db", SourceMeasurement: "cpu", DestinationMeasurement: "out", Query: "SELECT 1", Interval: "1h"}, Now: t1})
	if _, ok := x.f.CQ(1); ok {
		t.Fatal("deleted CQ was recreated with its old ID")
	}
	if next, ok := x.f.CQByName("next"); !ok || next.ID <= 1 {
		t.Fatalf("recreated CQ identity=%+v ok=%v", next, ok)
	}
}

func TestCQAdoptionChoiceIsAcceptedOnlyForEnabledUnseededKind(t *testing.T) {
	x := cpFSM(t)
	x.ok(CommandEnableCQ, EnableCQPayload{Nodes: allNodes, Now: t0})
	x.ok(CommandChoosePolicyAdoption, ChoosePolicyAdoptionPayload{Kind: PolicyKindCQ, Node: "w2", Hash: "inventory-hash", By: "admin", Now: t0})
	choice, ok := x.f.PolicyAdoptChoiceFor(PolicyKindCQ)
	if !ok || choice.Node != "w2" || choice.Hash != "inventory-hash" {
		t.Fatalf("CQ choice=%+v ok=%v", choice, ok)
	}
	x.ok(CommandCollectPolicies, CollectPoliciesPayload{Kind: PolicyKindCQ, Leader: "w1", Now: t0})
	x.ok(CommandSeedCQ, SeedCQPayload{Epoch: x.idx, Floor: 0, Now: t0})
	if _, ok := x.f.PolicyAdoptChoiceFor(PolicyKindCQ); ok {
		t.Fatal("successful CQ adoption retained its pending choice")
	}
	if res := x.apply(CommandChoosePolicyAdoption, ChoosePolicyAdoptionPayload{Kind: PolicyKindCQ, Node: "w2", Hash: "another", Now: t1}); asErr(res) == nil {
		t.Fatal("accepted a CQ adoption choice after seeding")
	}
}

func TestCQSeedRejectsOversizedForwardedPayload(t *testing.T) {
	x := cpFSM(t)
	res := x.apply(CommandSeedCQ, SeedCQPayload{Entries: []CQPolicyEntry{{ID: 1, Name: "large", Database: "db", SourceMeasurement: "cpu", DestinationMeasurement: "out", Query: strings.Repeat("x", MaxSeedCQPayloadBytes), Interval: "1h"}}})
	if asErr(res) == nil || !strings.Contains(asErr(res).Error(), "exceeds") {
		t.Fatalf("oversized CQ seed result=%v", res)
	}
}

func TestCQSnapshotKeepsAuthorityAndWatermark(t *testing.T) {
	x := enabledCQFSM(t)
	cq, _ := x.f.CQ(1)
	end := t0.Add(time.Hour)
	x.ok(CommandCompleteCQ, CompleteCQPayload{ID: 1, Version: cq.Version, AuthorityEpoch: x.f.PrimaryEpoch(), Executor: "w1", End: end})
	snap, err := x.f.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	var b bytes.Buffer
	if err := snap.Persist(&testSnapshotSink{Writer: &b}); err != nil {
		t.Fatal(err)
	}
	f := NewClusterFSM(zerolog.Nop())
	if err := f.Restore(io.NopCloser(bytes.NewReader(b.Bytes()))); err != nil {
		t.Fatal(err)
	}
	got, ok := f.CQ(1)
	if !ok || got.LastProcessedTime == nil || !got.LastProcessedTime.Equal(end) || f.PrimaryEpoch() != x.f.PrimaryEpoch() || !f.CQsSeeded() || !f.PoliciesSeeded(PolicyKindCQ) {
		t.Fatalf("restored CQ=%+v ok=%v epoch=%d", got, ok, f.PrimaryEpoch())
	}
}
