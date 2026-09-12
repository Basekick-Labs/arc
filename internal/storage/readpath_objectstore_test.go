//go:build objectstore

// The read path's URI builders, verified against a real object store.
//
// These are the regressions for the two live bugs #746 found, and both need a
// real server. A unit test comparing strings cannot catch either one: it would
// have to hard-code the expected prefix, which is exactly what the buggy code
// got wrong, so it would encode the bug and pass.
//
// Run with:
//
//	docker run -d --name arc-minio -p 9000:9000 \
//	  -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin \
//	  quay.io/minio/minio server /data
//	ARC_TEST_S3_BUCKET=arctest go test -tags='duckdb_arrow objectstore' ./internal/storage/
package storage

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/rs/zerolog"
)

// prefixedMinioBackend returns a MinIO-backed S3Backend with a configured
// prefix, which is the deployment shape both bugs needed.
func prefixedMinioBackend(t *testing.T, prefix string) *S3Backend {
	t.Helper()
	bucket := os.Getenv("ARC_TEST_S3_BUCKET")
	if bucket == "" {
		t.Skip("ARC_TEST_S3_BUCKET not set")
	}
	endpoint := os.Getenv("ARC_TEST_S3_ENDPOINT")
	if endpoint == "" {
		endpoint = "http://localhost:9000"
	}
	b, err := NewS3Backend(&S3Config{
		Bucket: bucket, Region: "us-east-1", Endpoint: endpoint,
		AccessKey: "minioadmin", SecretKey: "minioadmin", PathStyle: true,
		Prefix: prefix,
	}, zerolog.Nop())
	if err != nil {
		t.Fatalf("S3 backend: %v", err)
	}
	return b
}

// TestObjectURINamesWhatTheBackendWroteOnRealS3 is the regression for the
// retention bug.
//
// retention.buildParquetPath built "s3://{bucket}/{key}" with no GetPrefix(),
// so on a prefixed deployment every file it examined 404'd,
// getFileMaxTimeAndRowCount errored, deleteOldFiles logged a warning and
// skipped the file, and retention deleted NOTHING while still recording the run
// as "completed". Live from v26.03.2, when the prefix feature landed and
// updated delete.go's copy of this builder but not retention's.
//
// The assertion is against the store, not against a string: the URI must locate
// the object that Backend.Write actually created.
func TestObjectURINamesWhatTheBackendWroteOnRealS3(t *testing.T) {
	for _, prefix := range []string{"", "tenant", "a/b"} {
		name := prefix
		if name == "" {
			name = "(no prefix)"
		}
		t.Run(name, func(t *testing.T) {
			b := prefixedMinioBackend(t, prefix)
			ctx := context.Background()

			key := "retdb/cpu/2026/09/12/13/probe.parquet"
			body := []byte("probe-" + prefix)
			if err := b.Write(ctx, key, body); err != nil {
				t.Fatalf("write: %v", err)
			}
			t.Cleanup(func() { _ = b.Delete(ctx, key) })

			uri, err := ObjectURI(b, key)
			if err != nil {
				t.Fatalf("ObjectURI: %v", err)
			}

			// Resolve the URI back to a bucket-absolute key and read it with a
			// backend rooted at the bucket, so nothing in the read path shares
			// the prefix logic under test.
			rootBackend := prefixedMinioBackend(t, "")
			absKey := strings.TrimPrefix(uri, "s3://"+b.GetBucket()+"/")
			if absKey == uri {
				t.Fatalf("ObjectURI %q is not an s3 URI for bucket %q", uri, b.GetBucket())
			}
			got, err := rootBackend.Read(ctx, absKey)
			if err != nil {
				t.Fatalf("ObjectURI %q does not name a real object: %v", uri, err)
			}
			if string(got) != string(body) {
				t.Errorf("ObjectURI names the wrong object: read %q, wrote %q", got, body)
			}
		})
	}
}

// TestGlobRootTrimsToAListablePrefixOnRealS3 is the regression for the
// partition-pruning bug.
//
// The pruner turns a partition URL back into a listing prefix by trimming the
// root it parsed out of the measurement's glob. Before #746 it stripped only the
// scheme and bucket. List and ListDirectories prepend the configured prefix
// themselves, so a prefixed deployment listed "tenant/tenant/..." and got an
// EMPTY result WITH NO ERROR. Every partition was judged absent and
// OptimizeTablePath fell back to the unpruned glob: single-tier partition
// pruning was silently dead, correct but not pruned.
//
// The empty-with-no-error part is why this needs a real store. A mock returns
// whatever it is told to; MinIO genuinely returns success and nothing.
func TestGlobRootTrimsToAListablePrefixOnRealS3(t *testing.T) {
	b := prefixedMinioBackend(t, "tenant")
	ctx := context.Background()

	key := "prunedb/cpu/2026/09/12/13/probe.parquet"
	if err := b.Write(ctx, key, []byte("x")); err != nil {
		t.Fatalf("write: %v", err)
	}
	t.Cleanup(func() { _ = b.Delete(ctx, key) })

	lister, ok := Backend(b).(DirectoryLister)
	if !ok {
		t.Fatal("S3Backend must implement DirectoryLister")
	}

	// Derive the root exactly as PartitionPruner does: parse it off the glob.
	glob, err := GetStoragePath(b, "prunedb", "cpu")
	if err != nil {
		t.Fatalf("GetStoragePath: %v", err)
	}
	root := strings.TrimSuffix(glob, "prunedb/cpu/**/*.parquet")

	// The URL shape the pruner holds: a directory inside that glob.
	dirURI, err := ObjectURI(b, "prunedb/cpu/2026/09/12")
	if err != nil {
		t.Fatalf("ObjectURI: %v", err)
	}

	rel, ok := strings.CutPrefix(dirURI, root)
	if !ok {
		t.Fatalf("partition URL %q does not hang off the glob root %q", dirURI, root)
	}
	if strings.HasPrefix(rel, b.GetPrefix()) {
		t.Fatalf("trimmed prefix %q still carries the configured prefix %q; "+
			"List would prepend it again and list %q", rel, b.GetPrefix(), b.GetPrefix()+rel)
	}

	subdirs, err := lister.ListDirectories(ctx, rel)
	if err != nil {
		t.Fatalf("ListDirectories(%q): %v", rel, err)
	}
	if len(subdirs) == 0 {
		t.Fatalf("ListDirectories(%q) found nothing, so every partition would be judged absent "+
			"and pruning would silently stop working", rel)
	}

	// And show the old behaviour really did fail, so this test cannot pass for
	// the wrong reason if the trimming regresses to scheme-and-bucket surgery.
	old := strings.TrimPrefix(dirURI, "s3://"+b.GetBucket()+"/")
	oldResult, err := lister.ListDirectories(ctx, old)
	if err != nil {
		t.Fatalf("ListDirectories(%q): %v", old, err)
	}
	if len(oldResult) != 0 {
		t.Fatalf("expected the double-prefixed listing %q to return nothing, got %v; "+
			"this test is no longer discriminating", old, oldResult)
	}
}
