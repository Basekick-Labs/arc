//go:build objectstore

package storage

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// The conditional PutObject (If-None-Match: *) against a real S3 API: the
// second writer gets created=false and the object keeps the first body.
func TestS3WriteIfAbsent(t *testing.T) {
	bucket := os.Getenv("ARC_TEST_S3_BUCKET")
	if bucket == "" {
		t.Skip("ARC_TEST_S3_BUCKET not set")
	}
	endpoint := os.Getenv("ARC_TEST_S3_ENDPOINT")
	if endpoint == "" {
		endpoint = "http://localhost:9000"
	}
	b, err := NewS3Backend(&S3Config{
		Bucket: bucket, Region: "us-east-1", Endpoint: endpoint, Prefix: fmt.Sprintf("wia-%d/", time.Now().UnixNano()),
		AccessKey: "minioadmin", SecretKey: "minioadmin", PathStyle: true,
	}, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	ctx := context.Background()
	key := "db/t/metadata/v1.metadata.json"
	t.Cleanup(func() { _ = b.Delete(ctx, key) })
	var _ ConditionalWriter = b
	created, err := b.WriteIfAbsent(ctx, key, []byte("first"))
	if err != nil || !created {
		t.Fatalf("first write: created=%v err=%v", created, err)
	}
	created, err = b.WriteIfAbsent(ctx, key, []byte("second"))
	if err != nil || created {
		t.Fatalf("second write: created=%v err=%v, want false/nil", created, err)
	}
	got, err := b.Read(ctx, key)
	if err != nil || string(got) != "first" {
		t.Fatalf("content = %q (%v), want the first writer's", got, err)
	}
}
