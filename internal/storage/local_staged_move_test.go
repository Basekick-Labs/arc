package storage

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/rs/zerolog"
)

func TestLocalObjectMover(t *testing.T) {
	root := t.TempDir()
	b, err := NewLocalBackend(root, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	ctx := context.Background()
	if err := b.Write(ctx, "_compaction_staged/j1/o.parquet", []byte("out")); err != nil {
		t.Fatal(err)
	}
	if err := b.Sync(ctx, "_compaction_staged/j1/o.parquet"); err != nil {
		t.Fatalf("sync: %v", err)
	}
	if err := b.Sync(ctx, "_compaction_staged/j1/missing.parquet"); err == nil {
		t.Fatal("synced a missing file")
	}
	if err := b.MoveObject(ctx, "_compaction_staged/j1/o.parquet", "db/cpu/2026/10/13/10/o.parquet"); err != nil {
		t.Fatalf("move: %v", err)
	}
	if got, err := os.ReadFile(filepath.Join(root, "db/cpu/2026/10/13/10/o.parquet")); err != nil || string(got) != "out" {
		t.Fatalf("moved content %q %v", got, err)
	}
	if _, err := os.Stat(filepath.Join(root, "_compaction_staged/j1/o.parquet")); !os.IsNotExist(err) {
		t.Fatal("source kept after the move")
	}
	if err := b.MoveObject(ctx, "_compaction_staged/j1/o.parquet", "db/x.parquet"); err == nil {
		t.Fatal("moved a missing file")
	}
	for _, bad := range []string{"../escape", "/abs"} {
		if err := b.MoveObject(ctx, "db/cpu/2026/10/13/10/o.parquet", bad); err == nil {
			t.Fatalf("moved to %q", bad)
		}
		if err := b.RemoveAll(ctx, bad); err == nil {
			t.Fatalf("removed %q", bad)
		}
	}
	for _, root := range []string{"", ".", "/"} {
		if err := b.RemoveAll(ctx, root); err == nil {
			t.Fatalf("removed the storage root via %q", root)
		}
	}
	if err := b.RemoveAll(ctx, "_compaction_staged/j1"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "_compaction_staged/j1")); !os.IsNotExist(err) {
		t.Fatal("directory kept")
	}
	if err := b.RemoveAll(ctx, "_compaction_staged/nothing"); err != nil {
		t.Fatalf("missing key: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, "db/cpu/2026/10/13/10/o.parquet")); err != nil {
		t.Fatal("RemoveAll reached outside its key")
	}
}

// #1207 d2a2a: a "_" root is never a database to read.
func TestGetStoragePathRefusesReservedDatabases(t *testing.T) {
	b, err := NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	for _, db := range []string{"_compaction_staged", "_compaction_state", "_schema"} {
		if _, err := GetStoragePath(b, db, "cpu"); err == nil {
			t.Fatalf("read path built for %s", db)
		}
	}
	if _, err := GetStoragePath(b, "db_1", "_cpu"); err != nil {
		t.Fatalf("a database with an underscore inside, or a measurement starting with one: %v", err)
	}
}
