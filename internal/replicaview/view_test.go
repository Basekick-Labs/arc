package replicaview

import (
	"fmt"
	"testing"
)

func viewTestFile(t *testing.T, name string, replica bool, hour int64, sequences ...uint64) File {
	t.Helper()
	var ids []string
	var segments []Segment
	for i, seq := range sequences {
		id := fmt.Sprintf("%016x%016x", 1, seq)
		ids = append(ids, id)
		segments = append(segments, Segment{Identity: id, TotalRows: 10, Start: int64(i * 10), End: int64((i + 1) * 10)})
	}
	coverage, err := FromIdentities(ids)
	if err != nil {
		t.Fatal(err)
	}
	if !replica {
		segments = nil
	}
	return File{Path: name, SHA256: "verified-" + name, Metadata: FileMetadata{Database: "db", Measurement: "cpu", Hour: hour, Coverage: coverage, Segments: segments}}
}

func TestViewHandoffAcrossFlushBoundaries(t *testing.T) {
	v := NewView()
	shadow := viewTestFile(t, "replica", true, 1, 1, 2, 3)
	if err := v.Publish(shadow); err != nil {
		t.Fatal(err)
	}
	before := v.Snapshot("db", "cpu")
	if len(before.Sources) != 1 || len(before.Sources[0].Segments) != 3 {
		t.Fatalf("streamed rows absent: %+v", before.Sources)
	}
	defer before.Close()
	if err := v.Publish(viewTestFile(t, "primary-a", false, 1, 2)); err != nil {
		t.Fatal(err)
	}
	midway := v.Snapshot("db", "cpu")
	defer midway.Close()
	if len(midway.Sources) != 2 || len(midway.Sources[1].Segments) != 2 {
		t.Fatalf("partial primary arrival lost or duplicated rows: %+v", midway.Sources)
	}
	if err := v.Publish(viewTestFile(t, "primary-b", false, 1, 1, 3)); err != nil {
		t.Fatal(err)
	}
	after := v.Snapshot("db", "cpu")
	defer after.Close()
	if len(after.Sources) != 2 || after.Sources[0].File.Metadata.IsReplica() || after.Sources[1].File.Metadata.IsReplica() {
		t.Fatalf("handoff retained replica rows: %+v", after.Sources)
	}
	if len(before.Sources[0].Segments) != 3 {
		t.Fatal("publication mutated an in-flight query")
	}
	if got := v.Snapshot("other", "cpu"); len(got.Sources) != 0 {
		t.Fatal("measurement scope leaked")
	} else {
		got.Close()
	}
}

func TestViewRetirementAndReplacementPreserveQueryLeases(t *testing.T) {
	v := NewView()
	a := viewTestFile(t, "a", false, 1, 1)
	b := viewTestFile(t, "b", false, 1, 2)
	v.Publish(a)
	v.Publish(b)
	old := v.Snapshot("db", "cpu")
	output := viewTestFile(t, "compacted", false, 1, 1, 2)
	if err := v.Replace([]string{"a", "b"}, []File{output}); err != nil {
		t.Fatal(err)
	}
	if v.CanUnlink("a") || v.CanUnlink("b") {
		t.Fatal("deleted a file an in-flight query can still open")
	}
	current := v.Snapshot("db", "cpu")
	if len(current.Sources) != 1 || current.Sources[0].File.Path != "compacted" {
		t.Fatalf("replacement was not atomic: %+v", current.Sources)
	}
	current.Close()
	old.Close()
	old.Close()
	if !v.CanUnlink("a") || !v.CanUnlink("b") {
		t.Fatal("released query still pins replaced input")
	}
	v.Retire("db", "cpu", 1, output.Metadata.Coverage, []string{"compacted"})
	v.Publish(viewTestFile(t, "late-wal", true, 1, 1, 2))
	afterDelete := v.Snapshot("db", "cpu")
	defer afterDelete.Close()
	if len(afterDelete.Sources) != 0 {
		t.Fatal("late WAL replay resurrected intentionally deleted rows")
	}
	v.Publish(viewTestFile(t, "other-hour", true, 2, 1))
	otherHour := v.Snapshot("db", "cpu")
	defer otherHour.Close()
	if len(otherHour.Sources) != 1 {
		t.Fatal("retiring one partition hid another partition")
	}
}

func TestReplacementCannotWithdrawUncoveredInput(t *testing.T) {
	v := NewView()
	if err := v.Publish(viewTestFile(t, "input", false, 1, 1, 2)); err != nil {
		t.Fatal(err)
	}
	if err := v.Replace([]string{"input"}, []File{viewTestFile(t, "incomplete", false, 1, 1)}); err == nil {
		t.Fatal("replacement discarded an entry missing from output")
	}
	snapshot := v.Snapshot("db", "cpu")
	defer snapshot.Close()
	if len(snapshot.Sources) != 1 || snapshot.Sources[0].File.Path != "input" {
		t.Fatal("failed replacement partially changed query sources")
	}
}

func TestReplayIndexAndShadowCollectionRespectHoursAndLeases(t *testing.T) {
	v := NewView()
	a := viewTestFile(t, "hour1", true, 1, 1)
	a.Metadata.Segments[0].End = 4
	b := viewTestFile(t, "hour2", true, 2, 1)
	b.Metadata.Segments[0].End = 6
	id := a.Metadata.Segments[0].Identity
	if err := v.Publish(a); err != nil {
		t.Fatal(err)
	}
	if v.HasEntry("db", "cpu", id) {
		t.Fatal("partial multi-hour entry claimed complete")
	}
	retry := a
	retry.Path = "retry"
	if err := v.Publish(retry); err != nil {
		t.Fatal(err)
	}
	if v.HasEntry("db", "cpu", id) {
		t.Fatal("retry was counted as another hour")
	}
	if err := v.Publish(b); err != nil {
		t.Fatal(err)
	}
	if !v.HasEntry("db", "cpu", id) {
		t.Fatal("complete entry not indexed")
	}
	// Republishing the same file must not double its indexed row count.
	if err := v.Publish(b); err != nil {
		t.Fatal(err)
	}
	if !v.HasEntry("db", "cpu", id) {
		t.Fatal("idempotent publication changed completeness")
	}
	lease := v.Snapshot("db", "cpu")
	if err := v.Publish(viewTestFile(t, "primary1", false, 1, 1)); err != nil {
		t.Fatal(err)
	}
	removed := v.PruneCoveredReplicas()
	if len(removed) != 2 {
		t.Fatalf("removed %d, want both covered hour-1 retry files", len(removed))
	}
	if v.CanUnlink("hour1") {
		t.Fatal("query lease did not protect the withdrawn file")
	}
	if v.HasEntry("db", "cpu", id) {
		t.Fatal("withdrawn rows retained in replay index")
	}
	if len(v.entries) != 1 {
		t.Fatal("entry with remaining hour was dropped")
	}
	if err := v.Publish(viewTestFile(t, "primary2", false, 2, 1)); err != nil {
		t.Fatal(err)
	}
	if len(v.PruneCoveredReplicas()) != 1 {
		t.Fatal("last replica hour not collected")
	}
	if len(v.entries) != 0 {
		t.Fatal("replay index retains historical identities after handoff")
	}
	lease.Close()
	if !v.CanUnlink("hour1") || !v.CanUnlink("hour2") {
		t.Fatal("released query still holds pins")
	}
	snapshot := v.Snapshot("db", "cpu")
	defer snapshot.Close()
	if len(snapshot.Sources) != 2 {
		t.Fatal("garbage collection removed canonical data")
	}
}

func TestReplacementValidationIsAtomic(t *testing.T) {
	v := NewView()
	input := viewTestFile(t, "original", false, 1, 1)
	if err := v.Publish(input); err != nil {
		t.Fatal(err)
	}
	valid := viewTestFile(t, "valid", false, 1, 1)
	invalid := viewTestFile(t, "invalid", false, 1, 1)
	invalid.Metadata.Database = ""
	if err := v.Replace([]string{input.Path}, []File{valid, invalid}); err == nil {
		t.Fatal("accepted malformed output")
	}
	snapshot := v.Snapshot("db", "cpu")
	defer snapshot.Close()
	if len(snapshot.Sources) != 1 || snapshot.Sources[0].File.Path != input.Path {
		t.Fatal("failed replacement changed visible files")
	}
	if err := v.Replace([]string{input.Path}, []File{valid, valid}); err == nil {
		t.Fatal("accepted duplicate output path")
	}
}
