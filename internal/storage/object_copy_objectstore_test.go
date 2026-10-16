//go:build objectstore

package storage

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"testing"
	"time"
)

// copierRoundTrip stages an object with its checksum, copies it into place
// (single and in parts), and checks content and recorded checksum (#1207
// d2a2b).
func copierRoundTrip(t *testing.T, b interface {
	Backend
	ObjectCopier
}, forceParts func()) {
	t.Helper()
	ctx := context.Background()
	body := make([]byte, 12_000_000)
	_, _ = rand.Read(body)
	run := fmt.Sprintf("copytest-%d", time.Now().UnixNano())
	staged := "_compaction_staged/" + run + "/o.parquet"
	if err := b.WriteReaderWithSHA(ctx, staged, bytes.NewReader(body), int64(len(body)), "sha-of-o"); err != nil {
		t.Fatalf("staged write: %v", err)
	}
	t.Cleanup(func() { _ = b.Delete(context.Background(), staged) })
	if size, sha, err := b.ObjectSHA(ctx, staged); err != nil || size != int64(len(body)) || sha != "sha-of-o" {
		t.Fatalf("staged: %d %q %v", size, sha, err)
	}
	if size, sha, err := b.ObjectSHA(ctx, "_compaction_staged/"+run+"/absent.parquet"); err != nil || size != -1 || sha != "" {
		t.Fatalf("absent: %d %q %v", size, sha, err)
	}
	check := func(dst string) {
		t.Helper()
		t.Cleanup(func() { _ = b.Delete(context.Background(), dst) })
		got, err := b.Read(ctx, dst)
		if err != nil || !bytes.Equal(got, body) {
			t.Fatalf("%s: content differs (%v)", dst, err)
		}
		if size, sha, err := b.ObjectSHA(ctx, dst); err != nil || size != int64(len(body)) || sha != "sha-of-o" {
			t.Fatalf("%s: %d %q %v", dst, size, sha, err)
		}
	}
	single := run + "/db/cpu/single.parquet"
	if err := b.CopyObject(ctx, staged, single, int64(len(body)), "sha-of-o"); err != nil {
		t.Fatalf("copy: %v", err)
	}
	check(single)
	// Copying over an existing object replaces it.
	if err := b.CopyObject(ctx, staged, single, int64(len(body)), "sha-of-o"); err != nil {
		t.Fatalf("copy again: %v", err)
	}
	forceParts()
	parts := run + "/db/cpu/parts.parquet"
	if err := b.CopyObject(ctx, staged, parts, int64(len(body)), "sha-of-o"); err != nil {
		t.Fatalf("copy in parts: %v", err)
	}
	check(parts)
}

func TestS3CopyObject(t *testing.T) {
	b := minioBackend(t)
	oldMax, oldPart := s3MaxSingleCopy, s3CopyPartSize
	t.Cleanup(func() { s3MaxSingleCopy, s3CopyPartSize = oldMax, oldPart })
	copierRoundTrip(t, b, func() { s3MaxSingleCopy, s3CopyPartSize = 1, 5<<20 })
}

func TestAzureCopyObject(t *testing.T) {
	b := azureBackend(t, "")
	oldMax, oldBlock := azureMaxSingleCopy, azureCopyBlockSize
	t.Cleanup(func() { azureMaxSingleCopy, azureCopyBlockSize = oldMax, oldBlock })
	copierRoundTrip(t, b, func() { azureMaxSingleCopy, azureCopyBlockSize = 1, 5<<20 })
}
