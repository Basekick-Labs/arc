package api

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/database"
	"github.com/basekick-labs/arc/internal/storage"
)

// A DELETE leaves a file a re-home job owns as it is, with an error the
// response turns into a reason.
func TestDeleteLeavesAFileBeingRehomed1825(t *testing.T) {
	root := t.TempDir()
	backend, err := storage.NewLocalBackend(root, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	db, err := database.New(&database.Config{MemoryLimit: "256MB", ThreadCount: 1, MaxConnections: 1, LocalStorageRoot: root}, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rel := "db/cpu/2026/10/07/20/cpu_20261007_204851_558252000.parquet"
	full := filepath.Join(root, rel)
	if err := os.MkdirAll(filepath.Dir(full), 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := db.DB().Exec(fmt.Sprintf("COPY (SELECT * FROM (VALUES ('h1'), ('h2')) v(host)) TO '%s' (FORMAT PARQUET)", full)); err != nil {
		t.Fatal(err)
	}
	before, _ := os.ReadFile(full)
	owned := true
	h := &DeleteHandler{db: db, storage: backend, logger: zerolog.Nop()}
	h.SetRehomeOwns(func(p string) bool { return owned && p == rel })
	_, err = h.rewriteFileWithoutDeletedRows(context.Background(), full, rel, "host = 'h1'", "")
	if !errors.Is(err, errFileBeingRehomed) {
		t.Fatalf("err = %v, want errFileBeingRehomed", err)
	}
	if after, _ := os.ReadFile(full); string(after) != string(before) {
		t.Fatal("the owned file was changed")
	}
	if left, _ := backend.List(context.Background(), "db/"); len(left) != 1 {
		t.Fatalf("a rewrite was published beside the owned file: %v", left)
	}
	// Once the job has ended the same DELETE goes through.
	owned = false
	if _, err := h.rewriteFileWithoutDeletedRows(context.Background(), full, rel, "host = 'h1'", ""); errors.Is(err, errFileBeingRehomed) {
		t.Fatalf("a file no job owns: %v", err)
	}
	// WithRewriteLock holds the mutex a DELETE's rewrite phase takes.
	ran := false
	h.WithRewriteLock(func() {
		ran = true
		if h.rewriteMu.TryLock() {
			t.Error("WithRewriteLock ran its function without the rewrite mutex")
			h.rewriteMu.Unlock()
		}
	})
	if !ran {
		t.Fatal("WithRewriteLock did not run its function")
	}
}

// Retention leaves a file a re-home job owns for its next run, checks again
// just before it deletes, and holds its claim across the delete.
func TestRetentionLeavesAFileBeingRehomed1825(t *testing.T) {
	h, root := setupTestRetentionHandler(t)
	defer os.RemoveAll(root)
	old := time.Now().UTC().AddDate(0, 0, -30)
	mk := func(name string) string {
		rel := fmt.Sprintf("testdb/cpu/%s/%02d/%s.parquet", old.Format("2006/01/02"), old.Hour(), name)
		full := filepath.Join(root, rel)
		if err := os.MkdirAll(filepath.Dir(full), 0o700); err != nil {
			t.Fatal(err)
		}
		w, err := sql.Open("duckdb", "")
		if err != nil {
			t.Fatal(err)
		}
		defer w.Close()
		if _, err := w.Exec(fmt.Sprintf(`COPY (SELECT TIMESTAMPTZ '%s' AS "time", 1 AS n) TO '%s' (FORMAT PARQUET)`, old.Format("2006-01-02 15:04:05+00"), full)); err != nil {
			t.Fatal(err)
		}
		return rel
	}
	owned, free, late := mk("owned"), mk("free"), mk("late")
	var claimed, released []string
	asked := map[string]int{}
	h.SetRehomeClaim(func(p string) (func(), bool) {
		asked[p]++
		// "late" is free at the scan and owned by the time of the delete.
		if p == owned || (p == late && asked[p] > 1) {
			return func() {}, false
		}
		claimed = append(claimed, p)
		return func() { released = append(released, p) }, true
	})
	rows, files, skipped, err := h.deleteOldFiles(context.Background(), "testdb", "cpu", time.Now().UTC().AddDate(0, 0, -1), false, "retention:test")
	if err != nil {
		t.Fatal(err)
	}
	if files != 1 || rows != 1 || skipped != 0 {
		t.Fatalf("deleted %d file(s), %d row(s), skipped %d; want one file, and an owned file not counted as skipped", files, rows, skipped)
	}
	for _, p := range []string{owned, late} {
		if _, err := os.Stat(filepath.Join(root, p)); err != nil {
			t.Errorf("%s was deleted: %v", p, err)
		}
	}
	if _, err := os.Stat(filepath.Join(root, free)); !os.IsNotExist(err) {
		t.Errorf("the file no job owns was not deleted: %v", err)
	}
	if len(claimed) != len(released) || asked[free] != 2 || asked[late] != 2 {
		t.Fatalf("claims %v, released %v, asked %v: every claim is released, and each file is asked about at the scan and again before its delete", claimed, released, asked)
	}
}

// A database delete is refused while a job owns one of its files, and holds
// its block until it has finished.
func TestDatabaseDeleteAndRehomeKeepApart1825(t *testing.T) {
	ctx := context.Background()
	backend, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	if err := backend.Write(ctx, "mydb/cpu/2026/01/01/00/a.parquet", []byte("x")); err != nil {
		t.Fatal(err)
	}
	h := NewDatabasesHandler(backend, &config.DeleteConfig{Enabled: true}, nil, zerolog.Nop())
	app := fiber.New()
	h.RegisterRoutes(app)
	grant := false
	var asked, released []string
	h.SetRehomeBlock(func(prefix string) (func(), bool) {
		asked = append(asked, prefix)
		return func() { released = append(released, prefix) }, grant
	})
	del := func() (int, string) {
		resp, err := app.Test(httptest.NewRequest("DELETE", "/api/v1/databases/mydb?confirm=true", nil), -1)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(body)
	}
	if code, body := del(); code != 409 || !strings.Contains(body, "re-home") {
		t.Fatalf("with a job in flight: %d %s", code, body)
	}
	if left, _ := backend.List(ctx, "mydb/"); len(left) != 1 {
		t.Fatalf("a refused delete removed files: %v", left)
	}
	grant = true
	if code, body := del(); code != 200 {
		t.Fatalf("with no job: %d %s", code, body)
	}
	if len(asked) != 2 || asked[0] != "mydb/" || len(released) != 1 {
		t.Fatalf("asked %v, released %v: the block is over the database's prefix and released once the delete is done", asked, released)
	}
}

// A backup is refused while a job is in flight, frees its slot when it is,
// and holds its block while it runs.
func TestBackupAndRehomeKeepApart1825(t *testing.T) {
	handler, app, _ := newClusterDeleteRig(t)
	grant := false
	held := 0
	handler.SetRehomeBlock(func(prefix string) (func(), bool) {
		if prefix != "" {
			t.Errorf("a backup blocks everything, not %q", prefix)
		}
		if !grant {
			return func() {}, false
		}
		held++
		return func() { held-- }, true
	})
	post := func() (int, string) {
		req := httptest.NewRequest("POST", "/api/v1/backup", strings.NewReader(`{}`))
		req.Header.Set("Content-Type", "application/json")
		resp, err := app.Test(req, -1)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(body)
	}
	for i := 0; i < 2; i++ { // twice: the refused request must not keep the slot
		if code, body := post(); code != 409 || !strings.Contains(body, "re-home") {
			t.Fatalf("with a job in flight (try %d): %d %s", i, code, body)
		}
	}
	if handler.activeOperation.Load() != nil {
		t.Fatal("a refused backup kept the operation slot")
	}
	grant = true
	if code, body := post(); code >= 400 {
		t.Fatalf("with no job: %d %s", code, body)
	}
	deadline := time.Now().Add(10 * time.Second)
	for handler.activeOperation.Load() != nil && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if held != 0 || handler.activeOperation.Load() != nil {
		t.Fatalf("after the backup ended the block is still held (%d) or the slot taken", held)
	}
}
