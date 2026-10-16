package cluster

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func readSeenJournal(t *testing.T, dir string) []string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, seenLocalJournal))
	if err != nil {
		t.Fatal(err)
	}
	return strings.Split(strings.TrimSuffix(string(b), "\n"), "\n")
}

// The set survives a restart; a torn last line and unknown lines are skipped.
func TestSeenLocalReopens(t *testing.T) {
	dir := t.TempDir()
	s, err := openSeenLocal(dir, "c1")
	if err != nil {
		t.Fatal(err)
	}
	s.add("db/m/a.parquet", seenRecord{size: 1, mtime: 2, ino: 3, lsn: 4, sha: 10})
	s.add("db/m/b.parquet", seenRecord{size: 5, mtime: 6, ino: 7, lsn: 8, sha: 11})
	s.add("db/m/c.parquet", seenRecord{size: 9})
	s.remove("db/m/c.parquet")
	s.add("bad\tpath", seenRecord{size: 1})
	s.close()
	f, _ := os.OpenFile(filepath.Join(dir, seenLocalJournal), os.O_APPEND|os.O_WRONLY, 0o600)
	_, _ = f.WriteString("x\tnonsense\na\tdb/m/torn.parquet\t1\t2")
	_ = f.Close()

	s, err = openSeenLocal(dir, "c1")
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	if got := s.snapshot(); len(got) != 2 || got["db/m/a.parquet"] != (seenRecord{1, 2, 3, 4, 10}) ||
		got["db/m/b.parquet"] != (seenRecord{5, 6, 7, 8, 11}) {
		t.Fatalf("reopened set %+v", got)
	}
	// Rewritten at open: header plus the live records only.
	if lines := readSeenJournal(t, dir); len(lines) != 3 || lines[0] != seenLocalHeader+"c1" {
		t.Fatalf("journal after open %q", lines)
	}
}

// Another cluster's journal is discarded.
func TestSeenLocalDiscardsAnotherClustersJournal(t *testing.T) {
	dir := t.TempDir()
	s, _ := openSeenLocal(dir, "c1")
	s.add("db/m/a.parquet", seenRecord{size: 1})
	s.close()
	s, _ = openSeenLocal(dir, "c2")
	defer s.close()
	if s.len() != 0 {
		t.Fatal("kept another cluster's records")
	}
}

// The journal is rewritten once it holds well over the live records, and a
// re-add of the same identity writes nothing.
func TestSeenLocalCompacts(t *testing.T) {
	dir := t.TempDir()
	s, _ := openSeenLocal(dir, "c1")
	defer s.close()
	s.add("db/m/a.parquet", seenRecord{size: 1})
	s.add("db/m/a.parquet", seenRecord{size: 1})
	if s.records != 1 {
		t.Fatalf("records %d after a repeated add", s.records)
	}
	for i := 0; i < 5000; i++ {
		p := fmt.Sprintf("db/m/f%d.parquet", i)
		s.add(p, seenRecord{size: 1})
		s.remove(p)
	}
	if s.records > 4096+2 {
		t.Fatalf("journal never compacted: %d records for %d live", s.records, s.len())
	}
	s.sync()
	if lines := readSeenJournal(t, dir); len(lines) > 4096+3 {
		t.Fatalf("journal on disk has %d lines", len(lines))
	}
}

// A write error stops tracking and empties the set: nothing is removed on a
// journal that no longer keeps up.
func TestSeenLocalFailsOpen(t *testing.T) {
	dir := t.TempDir()
	s, _ := openSeenLocal(dir, "c1")
	s.add("db/m/a.parquet", seenRecord{size: 1})
	_ = s.f.Close() // the next flush fails
	s.sync()
	if s.err() == nil || s.len() != 0 {
		t.Fatalf("err %v len %d", s.err(), s.len())
	}
	s.add("db/m/b.parquet", seenRecord{size: 1})
	if s.len() != 0 {
		t.Fatal("tracked after it stopped")
	}
	s.close()
}
