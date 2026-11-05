package storage

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/rs/zerolog"
)

// WriteIfAbsent creates once; a second writer finds the object and leaves it
// as the first one wrote it, and no temporary file is left behind.
func TestLocalWriteIfAbsent(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	b, err := NewLocalBackend(root, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	var _ ConditionalWriter = b
	created, err := b.WriteIfAbsent(ctx, "db/t/metadata/v1.metadata.json", []byte("first"))
	if err != nil || !created {
		t.Fatalf("first write: created=%v err=%v", created, err)
	}
	created, err = b.WriteIfAbsent(ctx, "db/t/metadata/v1.metadata.json", []byte("second"))
	if err != nil || created {
		t.Fatalf("second write: created=%v err=%v, want false/nil", created, err)
	}
	got, err := b.Read(ctx, "db/t/metadata/v1.metadata.json")
	if err != nil || string(got) != "first" {
		t.Fatalf("content = %q (%v), want the first writer's", got, err)
	}
	entries, err := os.ReadDir(filepath.Join(root, "db/t/metadata"))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("metadata dir holds %d entries, want 1 (temporary files cleaned up)", len(entries))
	}
	if _, err := b.WriteIfAbsent(ctx, "../escape", []byte("x")); err == nil {
		t.Fatal("a path outside the root was written")
	}
}
