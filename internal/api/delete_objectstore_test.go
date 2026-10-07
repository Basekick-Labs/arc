//go:build objectstore

package api

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

func TestReplaceManifestAfterRewriteDeletesOriginalS3Object(t *testing.T) {
	bucket := os.Getenv("ARC_TEST_S3_BUCKET")
	if bucket == "" {
		t.Skip("ARC_TEST_S3_BUCKET not set")
	}
	endpoint := os.Getenv("ARC_TEST_S3_ENDPOINT")
	if endpoint == "" {
		endpoint = "http://localhost:9000"
	}

	backend, err := storage.NewS3Backend(&storage.S3Config{
		Bucket: bucket, Region: "us-east-1", Endpoint: endpoint,
		AccessKey: "minioadmin", SecretKey: "minioadmin", PathStyle: true,
	}, zerolog.Nop())
	if err != nil {
		t.Fatalf("S3 backend: %v", err)
	}
	defer backend.Close()

	ctx := context.Background()
	oldPath := "delete-rewrite-test/db/cpu/original.parquet"
	newPath := "delete-rewrite-test/db/cpu/original_rewrite_123.parquet"
	if err := backend.Write(ctx, oldPath, []byte("old")); err != nil {
		t.Fatalf("write old object: %v", err)
	}
	if err := backend.Write(ctx, newPath, []byte("new")); err != nil {
		t.Fatalf("write new object: %v", err)
	}
	t.Cleanup(func() {
		_ = backend.Delete(ctx, oldPath)
		_ = backend.Delete(ctx, newPath)
	})

	c := &rewriteManifestCoordinator{entry: &raft.FileEntry{
		Path: oldPath, Database: "db", Measurement: "cpu",
		PartitionTime: mustParseRewriteTestTime("2026-10-01T11:00:00Z"),
	}}
	h := &DeleteHandler{storage: backend, coordinator: c, logger: zerolog.Nop()}
	s3 := &s3RewriteResult{newPath: newPath, sizeBytes: 3, sha256: "remote-test"}

	if err := h.replaceManifestAfterRewrite(ctx, oldPath, newPath, s3); err != nil {
		t.Fatal(err)
	}

	if _, err := backend.StatFile(ctx, oldPath); err == nil {
		t.Fatal("old S3 object still exists")
	}
	size, err := backend.StatFile(ctx, newPath)
	if err != nil {
		t.Fatalf("new S3 object missing: %v", err)
	}
	if size != 3 {
		t.Fatalf("new S3 object size = %d, want 3", size)
	}
	if len(c.ops) != 2 {
		t.Fatalf("manifest ops = %d, want 2", len(c.ops))
	}
}

func mustParseRewriteTestTime(s string) time.Time {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		panic(err)
	}
	return t
}
