package api

import (
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

type rewriteManifestCoordinator struct {
	entry   *raft.FileEntry
	updated *raft.FileEntry
}

func (c *rewriteManifestCoordinator) BatchFileOpsInManifest([]raft.BatchFileOp) error { return nil }
func (c *rewriteManifestCoordinator) UpdateFileInManifest(file raft.FileEntry) error {
	c.updated = &file
	return nil
}
func (c *rewriteManifestCoordinator) GetFileEntry(string) (*raft.FileEntry, bool) {
	return c.entry, c.entry != nil
}
func (c *rewriteManifestCoordinator) IsPrimaryWriter() bool { return true }
func (c *rewriteManifestCoordinator) Role() string          { return "writer" }
func (c *rewriteManifestCoordinator) LocalNodeID() string   { return "new-primary" }

func TestUpdateManifestAfterRewriteStampsLocalOrigin(t *testing.T) {
	root := t.TempDir()
	backend, err := storage.NewLocalBackend(root, zerolog.Nop())
	if err != nil {
		t.Fatalf("create local backend: %v", err)
	}

	const relativePath = "db/cpu/rewrite.parquet"
	fullPath := filepath.Join(root, relativePath)
	if err := os.MkdirAll(filepath.Dir(fullPath), 0o700); err != nil {
		t.Fatalf("create file directory: %v", err)
	}
	if err := os.WriteFile(fullPath, []byte("rewritten parquet"), 0o600); err != nil {
		t.Fatalf("write rewritten file: %v", err)
	}

	createdAt := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	partition := time.Date(2026, 10, 1, 11, 0, 0, 0, time.UTC)
	coordinator := &rewriteManifestCoordinator{entry: &raft.FileEntry{
		Path:          relativePath,
		Database:      "db",
		Measurement:   "cpu",
		PartitionTime: partition,
		Tier:          "hot",
		CreatedAt:     createdAt,
		OriginNodeID:  "old-origin",
		SizeBytes:     4096,
		SHA256:        "stale",
	}}
	h := &DeleteHandler{storage: backend, coordinator: coordinator}

	if err := h.updateManifestAfterRewrite(relativePath, nil); err != nil {
		t.Fatalf("update manifest: %v", err)
	}
	if coordinator.updated == nil {
		t.Fatal("manifest update was not recorded")
	}
	if coordinator.updated.OriginNodeID != "new-primary" {
		t.Fatalf("OriginNodeID = %q, want new-primary", coordinator.updated.OriginNodeID)
	}
	if coordinator.updated.SizeBytes != int64(len("rewritten parquet")) {
		t.Fatalf("SizeBytes = %d, want %d", coordinator.updated.SizeBytes, len("rewritten parquet"))
	}
	// The checksum is what every replica verifies a pull against; it must
	// describe the rewritten bytes, not the stale entry.
	wantSHA := fmt.Sprintf("%x", sha256.Sum256([]byte("rewritten parquet")))
	if coordinator.updated.SHA256 != wantSHA {
		t.Fatalf("SHA256 = %q, want %q", coordinator.updated.SHA256, wantSHA)
	}
	// Everything that identifies the file survives the copy; the real FSM
	// rejects an update without CreatedAt, so a dropped field here would be
	// a rejected proposal in production.
	u := coordinator.updated
	if u.Path != relativePath || u.Database != "db" || u.Measurement != "cpu" || u.Tier != "hot" ||
		!u.PartitionTime.Equal(partition) || !u.CreatedAt.Equal(createdAt) {
		t.Fatalf("identifying fields not preserved: %+v", *u)
	}
}
