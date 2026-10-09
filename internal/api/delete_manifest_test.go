package api

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/database"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

type rewriteManifestCoordinator struct {
	entry *raft.FileEntry
	ops   []raft.BatchFileOp
}

func (c *rewriteManifestCoordinator) BatchFileOpsInManifest(ops []raft.BatchFileOp) error {
	c.ops = append(c.ops, ops...)
	return nil
}
func (c *rewriteManifestCoordinator) GetFileEntry(string) (*raft.FileEntry, bool) {
	return c.entry, c.entry != nil
}
func (c *rewriteManifestCoordinator) IsPrimaryWriter() bool { return true }
func (c *rewriteManifestCoordinator) Role() string          { return "writer" }
func (c *rewriteManifestCoordinator) LocalNodeID() string   { return "new-primary" }

func TestReplaceManifestAfterRewritePublishesImmutableFile(t *testing.T) {
	root := t.TempDir()
	backend, err := storage.NewLocalBackend(root, zerolog.Nop())
	if err != nil {
		t.Fatalf("create local backend: %v", err)
	}
	t.Cleanup(func() { _ = backend.Close() })

	const oldPath = "db/cpu/original.parquet"
	const newPath = "db/cpu/original_rewrite_123.parquet"
	fullPath := filepath.Join(root, newPath)
	if err := os.MkdirAll(filepath.Dir(fullPath), 0o700); err != nil {
		t.Fatalf("create file directory: %v", err)
	}
	if err := os.WriteFile(fullPath, []byte("rewritten parquet"), 0o600); err != nil {
		t.Fatalf("write rewritten file: %v", err)
	}

	createdAt := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	partition := time.Date(2026, 10, 1, 11, 0, 0, 0, time.UTC)
	coordinator := &rewriteManifestCoordinator{entry: &raft.FileEntry{
		Path:          oldPath,
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

	if err := h.replaceManifestAfterRewrite(context.Background(), oldPath, newPath, nil); err != nil {
		t.Fatalf("replace manifest: %v", err)
	}
	if len(coordinator.ops) != 2 {
		t.Fatalf("manifest ops = %d, want 2", len(coordinator.ops))
	}

	var registerPayload raft.RegisterFilePayload
	if err := json.Unmarshal(coordinator.ops[0].Payload, &registerPayload); err != nil {
		t.Fatalf("decode register payload: %v", err)
	}
	if coordinator.ops[0].Type != raft.CommandRegisterFile {
		t.Fatalf("first op type = %v, want register", coordinator.ops[0].Type)
	}
	if registerPayload.File.Path != newPath {
		t.Fatalf("registered path = %q, want %q", registerPayload.File.Path, newPath)
	}
	if registerPayload.File.OriginNodeID != "new-primary" {
		t.Fatalf("OriginNodeID = %q, want new-primary", registerPayload.File.OriginNodeID)
	}
	if registerPayload.File.SizeBytes != int64(len("rewritten parquet")) {
		t.Fatalf("SizeBytes = %d, want %d", registerPayload.File.SizeBytes, len("rewritten parquet"))
	}
	wantSHA := fmt.Sprintf("%x", sha256.Sum256([]byte("rewritten parquet")))
	if registerPayload.File.SHA256 != wantSHA {
		t.Fatalf("SHA256 = %q, want %q", registerPayload.File.SHA256, wantSHA)
	}

	if coordinator.ops[1].Type != raft.CommandDeleteFile {
		t.Fatalf("second op type = %v, want delete", coordinator.ops[1].Type)
	}
	var deletePayload raft.DeleteFilePayload
	if err := json.Unmarshal(coordinator.ops[1].Payload, &deletePayload); err != nil {
		t.Fatalf("decode delete payload: %v", err)
	}
	if deletePayload.Path != oldPath {
		t.Fatalf("deleted path = %q, want %q", deletePayload.Path, oldPath)
	}
}

// TestReplaceManifestAfterRewriteDeletesUnmanifestedSource ensures a stale
// manifest miss cannot leave both the old and new objects visible to glob reads.
func TestReplaceManifestAfterRewriteDeletesUnmanifestedSource(t *testing.T) {
	root := t.TempDir()
	backend, err := storage.NewLocalBackend(root, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()

	oldPath := "db/cpu/original.parquet"
	newPath := "db/cpu/original_rewrite_123.parquet"
	for path, body := range map[string]string{oldPath: "old", newPath: "new"} {
		full := filepath.Join(root, path)
		if err := os.MkdirAll(filepath.Dir(full), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
	}

	coordinator := &rewriteManifestCoordinator{}
	h := &DeleteHandler{storage: backend, coordinator: coordinator, logger: zerolog.Nop()}
	if err := h.replaceManifestAfterRewrite(context.Background(), oldPath, newPath, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, oldPath)); !os.IsNotExist(err) {
		t.Fatalf("unmanifested source still exists: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, newPath)); err != nil {
		t.Fatalf("rewritten object was removed: %v", err)
	}
	if len(coordinator.ops) != 0 {
		t.Fatalf("manifest ops = %d, want 0 on manifest miss", len(coordinator.ops))
	}
}

func TestPartialRewriteStandaloneDeletesOriginal(t *testing.T) {
	root := t.TempDir()
	logger := zerolog.Nop()
	backend, err := storage.NewLocalBackend(root, logger)
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	db, err := database.New(&database.Config{MemoryLimit: "256MB", ThreadCount: 1, MaxConnections: 1, LocalStorageRoot: root}, logger)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	oldPath := "db/cpu/2026/10/07/20/cpu_20261007_204851_558252000.parquet"
	full := filepath.Join(root, oldPath)
	if err := os.MkdirAll(filepath.Dir(full), 0700); err != nil {
		t.Fatal(err)
	}
	if _, err := db.DB().Exec("CREATE TABLE source(host VARCHAR); INSERT INTO source VALUES ('h1'), ('h2'), ('h2'), ('h3');"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.DB().Exec(fmt.Sprintf("COPY source TO %q (FORMAT PARQUET)", full)); err != nil {
		t.Fatal(err)
	}
	h := &DeleteHandler{db: db, storage: backend, logger: logger}
	if _, err := h.rewriteFileWithoutDeletedRows(context.Background(), full, oldPath, "host = 'h1'"); err != nil {
		t.Fatal(err)
	}
	files, err := backend.List(context.Background(), filepath.Dir(oldPath))
	if err != nil {
		t.Fatal(err)
	}
	var parquet string
	for _, p := range files {
		if filepath.Ext(p) == ".parquet" {
			if parquet != "" {
				t.Fatalf("duplicate parquet files: %q and %q", parquet, p)
			}
			parquet = p
		}
	}
	if parquet == "" || parquet == oldPath {
		t.Fatalf("expected one rewritten parquet, got %q", parquet)
	}
	var rows, h1 int
	q := filepath.Join(root, parquet)
	if err := db.DB().QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM read_parquet(%q)", q)).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if err := db.DB().QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM read_parquet(%q) WHERE host = 'h1'", q)).Scan(&h1); err != nil {
		t.Fatal(err)
	}
	if rows != 3 || h1 != 0 {
		t.Fatalf("rewritten rows=%d h1=%d, want 3/0", rows, h1)
	}
}
func TestReplaceManifestAfterRewriteDeletesOriginalObject(t *testing.T) {
	root := t.TempDir()
	local, err := storage.NewLocalBackend(root, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	defer local.Close()
	oldPath := "db/cpu/original.parquet"
	newPath := "db/cpu/original_rewrite_123.parquet"
	oldFull := filepath.Join(root, oldPath)
	newFull := filepath.Join(root, newPath)
	if err := os.MkdirAll(filepath.Dir(newFull), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(oldFull, []byte("old"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(newFull, []byte("new"), 0600); err != nil {
		t.Fatal(err)
	}
	c := &rewriteManifestCoordinator{entry: &raft.FileEntry{Path: oldPath, Database: "db", Measurement: "cpu", PartitionTime: time.Date(2026, 10, 1, 11, 0, 0, 0, time.UTC)}}
	remote := &struct{ storage.Backend }{Backend: local}
	h := &DeleteHandler{storage: remote, coordinator: c, logger: zerolog.Nop()}
	s3 := &s3RewriteResult{newPath: newPath, sizeBytes: 3, sha256: "remote-test"}
	if err := h.replaceManifestAfterRewrite(context.Background(), oldPath, newPath, s3); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(oldFull); !os.IsNotExist(err) {
		t.Fatalf("old object still exists: %v", err)
	}
	if len(c.ops) != 2 {
		t.Fatalf("manifest ops = %d, want 2", len(c.ops))
	}
}

// A DELETE whose affected-file list was built before another DELETE won the rewrite mutex finds its
// source already retired. It must say so in terms an operator can act on, rather than surfacing
// DuckDB's "IO Error: No files found that match the pattern", which gives no hint that the right
// response is simply to retry.
func TestRewriteReportsSourceRetiredByConcurrentDelete(t *testing.T) {
	root := t.TempDir()
	logger := zerolog.Nop()
	backend, err := storage.NewLocalBackend(root, logger)
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	db, err := database.New(&database.Config{MemoryLimit: "256MB", ThreadCount: 1, MaxConnections: 1, LocalStorageRoot: root}, logger)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	relativePath := "db/cpu/2026/10/07/20/cpu_20261007_204851_558252000.parquet"
	full := filepath.Join(root, relativePath)
	if err := os.MkdirAll(filepath.Dir(full), 0700); err != nil {
		t.Fatal(err)
	}
	h := &DeleteHandler{db: db, storage: backend, logger: logger}

	// The source is absent, exactly as it is for the request that loses the mutex race.
	_, err = h.rewriteFileWithoutDeletedRows(context.Background(), full, relativePath, "host = 'h1'")
	if err == nil {
		t.Fatal("rewrite of a retired source returned no error")
	}
	if !errors.Is(err, errSourceRetired) {
		t.Fatalf("error = %v, want errSourceRetired (an operator cannot act on the engine error)", err)
	}
	if strings.Contains(err.Error(), "No files found that match the pattern") {
		t.Errorf("the engine error is still leaking to the caller: %v", err)
	}

	// A source that IS present must not be mistaken for a retired one.
	if _, err := db.DB().Exec("CREATE TABLE src2(host VARCHAR); INSERT INTO src2 VALUES ('h1'), ('h2');"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.DB().Exec(fmt.Sprintf("COPY src2 TO %q (FORMAT PARQUET)", full)); err != nil {
		t.Fatal(err)
	}
	if _, err := h.rewriteFileWithoutDeletedRows(context.Background(), full, relativePath, "host = 'h1'"); errors.Is(err, errSourceRetired) {
		t.Errorf("a present source was reported as retired: %v", err)
	}
}
