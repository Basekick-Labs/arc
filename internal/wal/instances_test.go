package wal

import (
	"os"
	"testing"

	"github.com/rs/zerolog"
)

func TestInstancesOnDiskIncludesQuarantinedFiles(t *testing.T) {
	w, dir := newTestWriter(t, SyncModeFsync)
	defer os.RemoveAll(dir)
	appendTracked(t, w, "a")
	first := w.CurrentFile()
	if err := w.Rotate(); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(first, first+".failed"); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	other, err := NewWriter(&WriterConfig{WALDir: dir, SyncMode: SyncModeFsync, Logger: zerolog.Nop()})
	if err != nil {
		t.Fatal(err)
	}
	appendTracked(t, other, "b")
	if err := other.Close(); err != nil {
		t.Fatal(err)
	}
	got, err := InstancesOnDisk(dir, "", zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	want := map[uint64]bool{w.TrackedInstance(): true, other.TrackedInstance(): true}
	if len(got) != 2 || !want[got[0]] || !want[got[1]] {
		t.Fatalf("instances %x, want %x and %x", got, w.TrackedInstance(), other.TrackedInstance())
	}
}

// A damaged quarantined file (no WAL header) holds nothing; it must not fail
// the listing, which would block a writer announcement for good.
func TestInstancesOnDiskSkipsADamagedFile(t *testing.T) {
	w, dir := newTestWriter(t, SyncModeFsync)
	defer os.RemoveAll(dir)
	appendTracked(t, w, "a")
	active := w.CurrentFile()
	if err := os.WriteFile(dir+"/broken.wal.failed", make([]byte, 64), 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := InstancesOnDisk(dir, active, zerolog.Nop())
	if err != nil || len(got) != 0 {
		t.Fatalf("instances %v, %v (the active file is skipped, the damaged one holds nothing)", got, err)
	}
	w.Close()
}
