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
