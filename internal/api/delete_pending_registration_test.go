package api

// #1210: a row DELETE that rewrote a file still pending cluster registration
// deleted the original and registered nothing, so the rows that survived the
// DELETE stayed on this writer only.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster"
	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/database"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

func rewriteFixture(t *testing.T) (root string, backend *storage.LocalBackend, oldPath, newPath string) {
	t.Helper()
	root = t.TempDir()
	backend, err := storage.NewLocalBackend(root, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = backend.Close() })
	oldPath = "db/cpu/2026/10/11/10/cpu_original.parquet"
	newPath = "db/cpu/2026/10/11/10/cpu_original_rewrite_1.parquet"
	for path, body := range map[string]string{oldPath: "old", newPath: "rewritten"} {
		full := filepath.Join(root, path)
		if err := os.MkdirAll(filepath.Dir(full), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return root, backend, oldPath, newPath
}

// The original is pending in the outbox: the rewrite takes its place there,
// and the original is neither registered nor left on disk.
func TestDeleteRewriteOfAPendingFileAnnouncesTheRewrite(t *testing.T) {
	root, backend, oldPath, newPath := rewriteFixture(t)
	outbox, err := cluster.NewRegistrationOutbox(t.TempDir(), "c", zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(outbox.Stop)
	outbox.RegisterFile("db", "cpu", oldPath, time.Date(2026, 10, 11, 10, 0, 0, 0, time.UTC), 3, "x")

	coordinator := &rewriteManifestCoordinator{}
	h := &DeleteHandler{storage: backend, coordinator: coordinator, pending: outbox, logger: zerolog.Nop()}
	if err := h.replaceManifestAfterRewrite(context.Background(), oldPath, newPath, nil, ""); err != nil {
		t.Fatal(err)
	}
	if outbox.IsPending(oldPath) {
		t.Fatal("the deleted original is still pending registration")
	}
	if !outbox.IsPending(newPath) {
		t.Fatal("the rewrite is not pending registration: its surviving rows would stay on this writer")
	}
	if _, err := os.Stat(filepath.Join(root, oldPath)); !os.IsNotExist(err) {
		t.Fatalf("superseded original still on disk: %v", err)
	}
	if len(coordinator.ops) != 0 {
		t.Fatalf("manifest ops = %d; a pending swap stays in the outbox", len(coordinator.ops))
	}
}

func TestDeleteDoesNotScanAnotherWritersPendingFile(t *testing.T) {
	_, backend, oldPath, _ := rewriteFixture(t)
	h := &DeleteHandler{
		storage:     backend,
		coordinator: &rewriteManifestCoordinator{pendingPath: oldPath},
		logger:      zerolog.Nop(),
	}
	_, err := h.findAffectedFiles(context.Background(), "db", "cpu", "host = 'h1'", true)
	if err == nil || !strings.Contains(err.Error(), "awaiting cluster registration") {
		t.Fatalf("pending file should be rejected before scanning, got %v", err)
	}
}

type fixedPending struct {
	outcome cluster.ReplaceOutcome
	entry   raft.FileEntry
}

func (p fixedPending) ReplacePending(context.Context, string, string, int64, string) (cluster.ReplaceOutcome, raft.FileEntry, error) {
	return p.outcome, p.entry, nil
}

func (p fixedPending) Withdraw(context.Context, string) (bool, cluster.ReplaceOutcome, error) {
	return false, p.outcome, nil
}

// The original was confirmed in the manifest while the DELETE looked, before
// this node's FSM applied it: replace it in the manifest as usual.
func TestDeleteRewriteOfAJustRegisteredFileReplacesItInTheManifest(t *testing.T) {
	_, backend, oldPath, newPath := rewriteFixture(t)
	coordinator := &rewriteManifestCoordinator{}
	h := &DeleteHandler{storage: backend, coordinator: coordinator, logger: zerolog.Nop(),
		pending: fixedPending{outcome: cluster.ReplaceRegistered, entry: raft.FileEntry{
			Path: oldPath, Database: "db", Measurement: "cpu", Tier: "hot", CreatedAt: time.Now(),
		}}}
	if err := h.replaceManifestAfterRewrite(context.Background(), oldPath, newPath, nil, ""); err != nil {
		t.Fatal(err)
	}
	if len(coordinator.ops) != 2 || coordinator.ops[0].Type != raft.CommandRegisterFile || coordinator.ops[1].Type != raft.CommandDeleteFile {
		t.Fatalf("want register(new)+delete(old), got %d ops", len(coordinator.ops))
	}
	var reg raft.RegisterFilePayload
	if err := json.Unmarshal(coordinator.ops[0].Payload, &reg); err != nil || reg.File.Path != newPath || reg.File.Database != "db" {
		t.Fatalf("registered %+v (%v)", reg.File, err)
	}
}

// A DELETE that removes every row of a pending file withdraws it: a fresh
// entry skips the storage check, so it would otherwise be registered after
// the file is gone.
func TestDeleteOfEveryRowWithdrawsAPendingFile(t *testing.T) {
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
	if err := os.MkdirAll(filepath.Dir(full), 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := db.DB().Exec("CREATE TABLE source(host VARCHAR); INSERT INTO source VALUES ('h1'), ('h1');"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.DB().Exec(fmt.Sprintf("COPY source TO %q (FORMAT PARQUET)", full)); err != nil {
		t.Fatal(err)
	}
	outbox, err := cluster.NewRegistrationOutbox(t.TempDir(), "c", logger)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(outbox.Stop)
	outbox.RegisterFile("db", "cpu", oldPath, time.Now(), 1, "x")

	h := &DeleteHandler{db: db, storage: backend, coordinator: &rewriteManifestCoordinator{}, pending: outbox, logger: logger}
	if _, err := h.rewriteFileWithoutDeletedRows(context.Background(), full, oldPath, "host = 'h1'", ""); err != nil {
		t.Fatal(err)
	}
	if outbox.IsPending(oldPath) {
		t.Fatal("a file deleted outright is still pending registration")
	}
}

type failingPending struct{}

func (failingPending) ReplacePending(context.Context, string, string, int64, string) (cluster.ReplaceOutcome, raft.FileEntry, error) {
	return cluster.ReplaceNotPending, raft.FileEntry{}, errors.New("still in flight")
}

func (failingPending) Withdraw(context.Context, string) (bool, cluster.ReplaceOutcome, error) {
	return false, cluster.ReplaceNotPending, nil
}

// When the swap fails the original stays authoritative, so the rewrite is
// removed: otherwise this node reads the surviving rows twice.
func TestDeleteRewriteRemovesTheRewriteWhenTheSwapFails(t *testing.T) {
	root, backend, oldPath, newPath := rewriteFixture(t)
	h := &DeleteHandler{storage: backend, coordinator: &rewriteManifestCoordinator{}, pending: failingPending{}, logger: zerolog.Nop()}
	if err := h.replaceManifestAfterRewrite(context.Background(), oldPath, newPath, nil, ""); err == nil {
		t.Fatal("a failed swap was reported as success")
	}
	if _, err := os.Stat(filepath.Join(root, newPath)); !os.IsNotExist(err) {
		t.Fatalf("the unannounced rewrite was left on disk: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, oldPath)); err != nil {
		t.Fatalf("the original was removed although it stays authoritative: %v", err)
	}
}
