package raft

import (
	"errors"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// #1207 d1: DELETE records.

func (x *tkFSM) recordDelete(origin string, inst uint64, m, where string, marks map[string]uint64) uint64 {
	x.t.Helper()
	x.ok(CommandRecordDelete, RecordDeletePayload{Origin: origin, Instance: inst, Database: "db", Measurement: m, Where: where, Marks: marks, Now: t0})
	return x.idx
}

func (x *tkFSM) record(id uint64) *DeleteRecord {
	for _, r := range x.f.DeleteRecords() {
		if r.ID == id {
			return &r
		}
	}
	return nil
}

// recordsFSM: w1 (primary) current 1, w2 current 2, w3's earlier process 3
// (closed: same disks) and current 4.
func recordsFSM(t *testing.T) *tkFSM {
	x := takeoverFSM(t)
	x.announce("w1", 1, Continuity{})
	x.announce("w2", 2, Continuity{})
	x.announce("w3", 3, Continuity{})
	x.announce("w3", 4, Continuity{WAL: 3, Journal: 3, Storage: 3})
	return x
}

// reportEmpty: writers report holding nothing of inst (removed once all did).
func (x *tkFSM) reportEmpty(inst uint64, by ...string) {
	x.t.Helper()
	origin := x.inst(inst).Origin
	for _, b := range by {
		x.ok(CommandTakeOverStreamV2, TakeOverStreamV2Payload{Origin: origin, By: b, Now: t0, Claims: []ClaimSet{{Instance: inst}}})
	}
}

func TestRecordDeleteScope(t *testing.T) {
	x := recordsFSM(t)
	id := x.recordDelete("w1", 1, "cpu", "v > 1", map[string]uint64{"db/cpu": 5, "db/mem": 9, "other/cpu": 9})
	r := x.record(id)
	if r == nil {
		t.Fatal("no record")
	}
	if len(r.OwnMarks) != 1 || r.OwnMarks["db/cpu"] != 5 {
		t.Fatalf("own marks keep only keys of the record: %v", r.OwnMarks)
	}
	if len(r.Before) != 3 || r.Before[0].Instance != 2 || r.Before[2].Instance != 4 {
		t.Fatalf("before: %+v", r.Before)
	}
	cases := []struct {
		inst uint64
		key  string
		cseq uint64
		want bool
	}{
		{1, "db/cpu", 5, true},       // own, at the mark
		{1, "db/cpu", 6, false},      // own, acknowledged after
		{1, "db/mem", 1, false},      // another measurement
		{2, "db/cpu", 1 << 40, true}, // unacknowledged: every batch
		{3, "db/cpu", 7, true},       // an earlier process never acknowledges
		{9, "db/cpu", 1, false},      // unknown when recorded: created after
		{2, "dbx/cpu", 1, false},     // another database with a common prefix
	}
	for _, c := range cases {
		if got := r.InScope(c.inst, c.key, c.cseq); got != c.want {
			t.Errorf("InScope(%d, %s, %d) = %v", c.inst, c.key, c.cseq, got)
		}
	}
	// A whole-database record (DROP DATABASE) matches every measurement.
	db := x.record(x.recordDelete("w1", 1, "", "", nil))
	if !db.MatchesKey("db/mem") || db.MatchesKey("dbx/mem") || db.InScope(1, "db/cpu", 1) {
		t.Fatalf("database record %+v", db)
	}
}

func TestRecordDeleteRefusals(t *testing.T) {
	x := recordsFSM(t)
	ok := RecordDeletePayload{Origin: "w1", Instance: 1, Database: "db", Measurement: "cpu", Now: t0}
	for name, mut := range map[string]func(p *RecordDeletePayload){
		"not the primary":   func(p *RecordDeletePayload) { p.Origin, p.Instance = "w2", 2 },
		"not current":       func(p *RecordDeletePayload) { p.Instance = 2 },
		"a reader":          func(p *RecordDeletePayload) { p.Origin = "r1" },
		"no time":           func(p *RecordDeletePayload) { p.Now = time.Time{} },
		"no database":       func(p *RecordDeletePayload) { p.Database = "" },
		"slash in database": func(p *RecordDeletePayload) { p.Database = "a/b" },
		"huge where":        func(p *RecordDeletePayload) { p.Where = string(make([]byte, maxDeleteWhereLen+1)) },
	} {
		p := ok
		mut(&p)
		if res := x.apply(CommandRecordDelete, p); res == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	x.ok(CommandRecordDelete, ok)
	// A writer whose instance the table had no room for (no current one).
	y := takeoverFSM(t)
	y.ok(CommandRecordDelete, RecordDeletePayload{Origin: "w1", Instance: 0x77, Database: "db", Now: t0})
	if res := y.apply(CommandRecordDelete, RecordDeletePayload{Origin: "w1", Instance: 0, Database: "db", Now: t0}); res != nil {
		t.Fatalf("no instance at all: %v", res)
	}
	off := takeoverFSM(t)
	off.f.streamClaims = false
	if res := off.apply(CommandRecordDelete, ok); res == nil {
		t.Fatal("recorded with stream takeover off")
	}
}

func TestRecordDeleteAck(t *testing.T) {
	x := recordsFSM(t)
	id := x.recordDelete("w1", 1, "cpu", "", map[string]uint64{"db/cpu": 3})
	if !x.f.OwesDeleteAck(2) || !x.f.OwesDeleteAck(4) || !x.f.OwesDeleteAck(3) || x.f.OwesDeleteAck(1) {
		t.Fatal("owed acknowledgements")
	}
	// Only the writer of a current instance acknowledges it.
	x.refused(CommandRecordDeleteAck, RecordDeleteAckPayload{Origin: "w2", Record: id, Instance: 4})
	x.refused(CommandRecordDeleteAck, RecordDeleteAckPayload{Origin: "w3", Record: id, Instance: 3})
	x.refused(CommandRecordDeleteAck, RecordDeleteAckPayload{Origin: "w2", Record: id + 99, Instance: 2})
	x.ok(CommandRecordDeleteAck, RecordDeleteAckPayload{Origin: "w2", Record: id, Instance: 2, Marks: map[string]uint64{"db/cpu": 7, "db/mem": 9}})
	if x.f.OwesDeleteAck(2) {
		t.Fatal("still owed after the acknowledgement")
	}
	r := x.record(id)
	if !r.InScope(2, "db/cpu", 7) || r.InScope(2, "db/cpu", 8) || len(r.scopeOf(2).Marks) != 1 {
		t.Fatalf("acknowledged scope %+v", r.scopeOf(2))
	}
	// A second acknowledgement changes nothing.
	x.ok(CommandRecordDeleteAck, RecordDeleteAckPayload{Origin: "w2", Record: id, Instance: 2, Marks: map[string]uint64{"db/cpu": 99}})
	if x.record(id).InScope(2, "db/cpu", 8) {
		t.Fatal("a second acknowledgement moved the mark")
	}
	// The owed set survives a snapshot.
	y := roundTrip(t, x.f)
	if !y.OwesDeleteAck(4) || y.OwesDeleteAck(2) || len(y.DeleteRecords()) != 1 {
		t.Fatal("records or owed set lost in a snapshot")
	}
}

func TestDeletedRefusal(t *testing.T) {
	x := recordsFSM(t)
	id := x.recordDelete("w1", 1, "cpu", "v > 1", map[string]uint64{"db/cpu": 5})
	reg := func(path string, seen uint64, c InstanceCoverage) string {
		p := regPayload(path, "w1", hourA, c)
		p.DeleteSeen = seen
		return x.register(p)
	}
	// The primary's own pending file of rows before the DELETE.
	if k := reg("a", 0, cov(1, "db/cpu", 4, 5)); k != RefusedDeleted {
		t.Fatalf("own file before the mark: %q", k)
	}
	if k := reg("a", id, cov(1, "db/cpu", 4, 5)); k != "" {
		t.Fatalf("rewritten with the record: %q", k)
	}
	if k := reg("b", 0, cov(1, "db/cpu", 6, 9)); k != "" {
		t.Fatalf("own file after the mark: %q", k)
	}
	// Another writer's batches: all of them, until it acknowledges.
	if k := reg("c", 0, cov(2, "db/cpu", 100, 100)); k != RefusedDeleted {
		t.Fatalf("an unacknowledged instance: %q", k)
	}
	if k := reg("d", 0, cov(2, "db/mem", 1, 1)); k != "" {
		t.Fatalf("another measurement: %q", k)
	}
	tr := cov(3, "db/cpu", 0, 0)
	tr.Ranges, tr.Truncated = nil, true
	if k := reg("e", 0, tr); k != RefusedDeleted {
		t.Fatalf("a truncated coverage of an instance in scope: %q", k)
	}
	x.ok(CommandRecordDeleteAck, RecordDeleteAckPayload{Origin: "w2", Record: id, Instance: 2, Marks: map[string]uint64{"db/cpu": 10}})
	if k := reg("f", 0, cov(2, "db/cpu", 11, 12)); k != "" {
		t.Fatalf("after the acknowledged mark: %q", k)
	}
	// A rewrite of a registered file (InheritFrom) is the DELETE's own work.
	if k := reg("g", 0, cov(9, "db/cpu", 1, 1)); k != "" {
		t.Fatalf("an instance unknown to the record: %q", k)
	}
	// In a batch too.
	p := regPayload("h", "w1", hourA, cov(2, "db/cpu", 3, 3))
	if ops := x.batch(regOp(t, p)); len(ops) != 1 || ops[0].Kind != RefusedDeleted {
		t.Fatalf("batch: %+v", ops)
	}
}

func TestDeleteRecordsCollected(t *testing.T) {
	x := recordsFSM(t)
	id := x.recordDelete("w1", 1, "cpu", "", map[string]uint64{"db/cpu": 5})
	x.ok(CommandRecordDeleteAck, RecordDeleteAckPayload{Origin: "w2", Record: id, Instance: 2})
	x.ok(CommandRecordDeleteAck, RecordDeleteAckPayload{Origin: "w3", Record: id, Instance: 4, Marks: map[string]uint64{"db/cpu": 2}})
	// Kept by w1's floor (below its mark), w3's floor, and w3's earlier
	// process 3 (never acknowledges).
	settle := func(origin string, inst, floor uint64, lost Intervals) {
		x.ok(CommandSettleCoverage, SettleCoveragePayload{Origin: origin, Instance: inst, Keys: map[string]KeySettlement{"db/cpu": {Floor: floor, Lost: lost}}})
	}
	settle("w1", 1, 5, Intervals{{2, 2}})
	settle("w3", 4, 2, nil)
	if x.record(id) == nil {
		t.Fatal("collected while an earlier process and a lost batch hold it")
	}
	x.reportEmpty(3, "w1", "w2")
	if x.inst(3) != nil {
		t.Fatal("instance 3 not removed")
	}
	if x.record(id) == nil {
		t.Fatal("collected while a lost batch below the mark is claimable")
	}
	// The lost batch never stops being claimable while 1 is current: only
	// the bound drops such a record (TestDeleteRecordsBound).
	pins, lostOnly := x.f.recordPinsLocked(x.f.deleteRecordLocked(id))
	if len(pins) != 1 || pins[0] != 1 || !lostOnly {
		t.Fatalf("pins %v lost-only %v", pins, lostOnly)
	}
	// Without the lost batch it goes as soon as the floors pass the marks.
	y := recordsFSM(t)
	id = y.recordDelete("w1", 1, "cpu", "", map[string]uint64{"db/cpu": 5})
	for _, a := range []RecordDeleteAckPayload{{Origin: "w2", Record: id, Instance: 2}, {Origin: "w3", Record: id, Instance: 4}} {
		y.ok(CommandRecordDeleteAck, a)
	}
	y.reportEmpty(3, "w1", "w2")
	y.ok(CommandSettleCoverage, SettleCoveragePayload{Origin: "w1", Instance: 1, Keys: map[string]KeySettlement{"db/cpu": {Floor: 4}}})
	if y.record(id) == nil {
		t.Fatal("collected below the mark")
	}
	y.ok(CommandSettleCoverage, SettleCoveragePayload{Origin: "w1", Instance: 1, Keys: map[string]KeySettlement{"db/cpu": {Floor: 5}}})
	if y.record(id) != nil {
		t.Fatal("kept once nothing can publish a batch of it")
	}
}

func TestDeleteRecordsBound(t *testing.T) {
	x, _ := orphanFinished(t) // 7 retired
	x.announce("w1", 1, Continuity{})
	for i := 0; i < maxDeleteRecords; i++ {
		x.recordDelete("w1", 1, "cpu", "", nil)
		for _, in := range []uint64{8} {
			if x.f.OwesDeleteAck(in) {
				x.ok(CommandRecordDeleteAck, RecordDeleteAckPayload{Origin: "w2", Record: x.idx, Instance: in})
			}
		}
	}
	if n := len(x.f.DeleteRecords()); n != maxDeleteRecords {
		t.Fatalf("%d records", n)
	}
	// Full, every record kept only by the retired instance 7: the next one
	// removes it, and the records it held go.
	x.recordDelete("w1", 1, "cpu", "", nil)
	if x.inst(7) != nil || !x.f.Tombstoned(7) {
		t.Fatal("retired instance pinning the oldest record not removed")
	}
	if n := len(x.f.DeleteRecords()); n != 1 {
		t.Fatalf("%d records left, want the new one", n)
	}
	// Full with a live, unacknowledged writer: refused.
	y := recordsFSM(t)
	for i := 0; i < maxDeleteRecords; i++ {
		y.recordDelete("w1", 1, "cpu", "", nil)
	}
	res := y.apply(CommandRecordDelete, RecordDeletePayload{Origin: "w1", Instance: 1, Database: "db", Now: t0})
	if err, _ := res.(error); !errors.Is(err, ErrDeleteRecordsFull) {
		t.Fatalf("full: %v", res)
	}
}

func TestSettleDeleted(t *testing.T) {
	x := takeoverFSM(t)
	x.announce("w1", 1, Continuity{})
	x.announce("w2", 7, Continuity{})
	x.announce("w2", 8, Continuity{}) // 7 orphaned
	e := x.claim("w1", "w2", 7, "db/cpu", 1, 5)
	id := x.recordDelete("w1", 1, "cpu", "", nil)
	pt := mustTime(hourA).Unix()
	c := cov(7, "db/cpu", 2, 3)
	c.ClaimEpoch = e
	p := SettleDeletedPayload{By: "w1", Hour: pt, Coverage: c, Records: []uint64{id}}
	for name, mut := range map[string]func(p *SettleDeletedPayload){
		"not the claimant":  func(p *SettleDeletedPayload) { p.By = "w3" },
		"no records":        func(p *SettleDeletedPayload) { p.Records = nil },
		"unknown record":    func(p *SettleDeletedPayload) { p.Records = []uint64{id + 1} },
		"outside the claim": func(p *SettleDeletedPayload) { p.Coverage.Ranges = [][2]uint64{{4, 9}} },
		"wrong epoch":       func(p *SettleDeletedPayload) { p.Coverage.ClaimEpoch = e + 1 },
		"truncated":         func(p *SettleDeletedPayload) { p.Coverage.Truncated = true },
	} {
		q := p
		q.Coverage.Ranges = append([][2]uint64(nil), p.Coverage.Ranges...)
		mut(&q)
		if res := x.apply(CommandSettleDeleted, q); res == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	// A record not covering the batch (instance 8 did not exist; a later
	// record of another measurement) does not settle it.
	other := x.recordDelete("w1", 1, "mem", "", nil)
	q := p
	q.Records = []uint64{other}
	x.refused(CommandSettleDeleted, q)
	x.ok(CommandSettleDeleted, p)
	if st, _, _, _ := x.f.CoveredRows(7, "db/cpu", 2, 0, 1, mustTime(hourA)); st != CoveredAll {
		t.Fatalf("batch 2 not settled: %v", st)
	}
	if got := x.f.UncoveredBatches(7, "db/cpu", 1, 5, mustTime(hourA)); len(got) != 2 || got[0] != [2]uint64{1, 1} || got[1] != [2]uint64{4, 5} {
		t.Fatalf("uncovered %v", got)
	}
	// The origin settles batches of an earlier instance of its own; never
	// its current one (its floors do).
	x.ok(CommandClaimDone, ClaimDonePayload{Instance: 7, By: "w1", Epoch: e, Unreplayable: map[string]Intervals{"db/cpu": {{1, 1}, {4, 5}}}})
	own := cov(7, "db/cpu", 4, 4)
	x.ok(CommandSettleDeleted, SettleDeletedPayload{By: "w2", Hour: pt, Coverage: own, Records: []uint64{id}})
	x.announce("w2", 9, Continuity{})
	late := x.recordDelete("w1", 1, "cpu", "", nil) // 9 in its scope
	x.refused(CommandSettleDeleted, SettleDeletedPayload{By: "w2", Hour: pt, Coverage: cov(9, "db/cpu", 1, 1), Records: []uint64{late}})
	// An instance nothing tracks (removed since): nothing to settle, accepted.
	x.ok(CommandSettleDeleted, SettleDeletedPayload{By: "w3", Hour: pt, Coverage: cov(77, "db/cpu", 1, 1), Records: []uint64{id}})
}

func TestDeleteRecordsSnapshot(t *testing.T) {
	x := recordsFSM(t)
	a := x.recordDelete("w1", 1, "cpu", "v > 1", map[string]uint64{"db/cpu": 5})
	b := x.recordDelete("w1", 1, "", "", nil)
	x.ok(CommandRecordDeleteAck, RecordDeleteAckPayload{Origin: "w2", Record: a, Instance: 2, Marks: map[string]uint64{"db/cpu": 4}})
	y := roundTrip(t, x.f)
	got := y.DeleteRecords()
	if len(got) != 2 || got[0].ID != a || got[1].ID != b || got[0].Where != "v > 1" || !got[0].InScope(2, "db/cpu", 4) || got[0].InScope(2, "db/cpu", 5) {
		t.Fatalf("restored %+v", got)
	}
	if y.LatestDeleteRecord() != b {
		t.Fatal("latest")
	}
	recs, straddle := y.DeleteRecordsFor([]InstanceCoverage{cov(2, "db/cpu", 3, 6)}, 0)
	if len(recs) != 2 || !straddle {
		t.Fatalf("records for a file straddling a mark: %d %v", len(recs), straddle)
	}
	recs, straddle = y.DeleteRecordsFor([]InstanceCoverage{cov(2, "db/cpu", 3, 4)}, a)
	if len(recs) != 1 || recs[0].ID != b || straddle {
		t.Fatalf("records after a: %+v %v", recs, straddle)
	}
	if ids := y.DeleteScopeIDs(2, "db/cpu", 5); len(ids) != 1 || ids[0] != b {
		t.Fatalf("scope ids %v", ids)
	}
	_ = zerolog.Nop()
}

// An instance announced without a fresh start (every writer when takeover
// turns on) is untracked: no index, but its floors still collect a record.
func TestDeleteRecordsCollectedByAnUntrackedInstance(t *testing.T) {
	x := takeoverFSM(t)
	x.ok(CommandAnnounceInstance, AnnounceInstancePayload{Origin: "w1", Instance: 1})
	if in := x.inst(1); in == nil || !in.Untracked {
		t.Fatalf("instance %+v", in)
	}
	id := x.recordDelete("w1", 1, "cpu", "", map[string]uint64{"db/cpu": 5})
	x.ok(CommandSettleCoverage, SettleCoveragePayload{Origin: "w1", Instance: 1, Keys: map[string]KeySettlement{"db/cpu": {Floor: 4}}})
	if x.record(id) == nil {
		t.Fatal("collected below the mark")
	}
	y := roundTrip(t, x.f)
	if len(y.takeover.untrackedFloors[1]) != 1 {
		t.Fatal("untracked floors lost in a snapshot")
	}
	x.ok(CommandSettleCoverage, SettleCoveragePayload{Origin: "w1", Instance: 1, Keys: map[string]KeySettlement{"db/cpu": {Floor: 6, Lost: Intervals{{5, 5}}}}})
	if x.record(id) == nil {
		t.Fatal("collected with a lost batch below the mark")
	}
	z := takeoverFSM(t)
	z.ok(CommandAnnounceInstance, AnnounceInstancePayload{Origin: "w1", Instance: 1})
	id = z.recordDelete("w1", 1, "cpu", "", map[string]uint64{"db/cpu": 5})
	z.ok(CommandSettleCoverage, SettleCoveragePayload{Origin: "w1", Instance: 1, Keys: map[string]KeySettlement{"db/cpu": {Floor: 5}}})
	if z.record(id) != nil {
		t.Fatal("kept once the untracked floor passed the mark")
	}
}

// An instance that stops being tracked keeps its floors for the records.
func TestUntrackKeepsFloors(t *testing.T) {
	x := recordsFSM(t)
	x.ok(CommandSettleCoverage, SettleCoveragePayload{Origin: "w1", Instance: 1, Keys: map[string]KeySettlement{"db/cpu": {Floor: 7}}})
	x.f.mu.Lock()
	x.f.untrackLocked(x.f.takeover.instances[1], "test")
	got := x.f.takeover.untrackedFloors[1]["db/cpu"].Floor
	x.f.mu.Unlock()
	if got != 7 {
		t.Fatalf("floor %d", got)
	}
}
