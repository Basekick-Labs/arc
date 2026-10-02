package api

import (
	"os"
	"path/filepath"
	"testing"

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

	coordinator := &rewriteManifestCoordinator{entry: &raft.FileEntry{
		Path:         relativePath,
		OriginNodeID: "old-origin",
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
}
