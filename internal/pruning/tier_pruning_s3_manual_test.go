package pruning

// Manual integration test for per-tier pruning against REAL S3 (#662).
//
// Skipped unless ARC_S3_MANUAL_TEST_BUCKET names a bucket the ambient AWS
// credentials can list. Expected fixture layout (zero-byte objects suffice —
// only listings are exercised):
//
//	tenant1/db/cpu/2024/03/15/14/cpu_a.parquet
//	tenant1/db/cpu/2024/03/15/15/cpu_b.parquet
//	tenant1/db/cpu/2024/03/16/09/cpu_c.parquet
//	tenant1/db/cpu/2024/03/10/cpu_daily.parquet   (day-level compacted)
//	db/mem/2024/03/15/14/mem_a.parquet            (empty-prefix case)
//
// This exists because the highest-risk failure mode of #662 — prefix
// double-application producing false "verified empty" — is a property of the
// real backend's key handling, which mocks can only approximate.

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

func manualS3Backend(t *testing.T, prefix string) storage.Backend {
	t.Helper()
	bucket := os.Getenv("ARC_S3_MANUAL_TEST_BUCKET")
	if bucket == "" {
		t.Skip("ARC_S3_MANUAL_TEST_BUCKET not set; skipping real-S3 manual test")
	}
	backend, err := storage.NewS3Backend(&storage.S3Config{
		Bucket: bucket,
		Region: os.Getenv("AWS_REGION"),
		Prefix: prefix,
		UseSSL: true,
	}, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewS3Backend: %v", err)
	}
	return backend
}

func TestManualS3_PruneTierPaths_PrefixedBackend(t *testing.T) {
	backend := manualS3Backend(t, "tenant1/")
	p := NewPartitionPruner(zerolog.Nop())
	glob := storage.GetStoragePath(backend, "db", "cpu")

	// In-range: hours 14-15 exist on 03-15; hour 16 does not.
	paths, outcome := p.PruneTierPaths(context.Background(), glob, "db", "cpu",
		tierRange(t, "2024-03-15T14:00:00Z", "2024-03-15T17:00:00Z"), backend, false)
	if outcome != TierPrunePruned {
		t.Fatalf("in-range outcome = %v, want TierPrunePruned (paths=%v)", outcome, paths)
	}
	if len(paths) != 2 {
		t.Fatalf("in-range paths = %v, want exactly hours 14 and 15", paths)
	}
	for _, path := range paths {
		if !strings.Contains(path, "/tenant1/db/cpu/2024/03/15/1") {
			t.Fatalf("unexpected pruned path %q", path)
		}
	}

	// Day-level compacted file on 03-10 must survive; its hours must not.
	paths, outcome = p.PruneTierPaths(context.Background(), glob, "db", "cpu",
		tierRange(t, "2024-03-10T00:00:00Z", "2024-03-10T04:00:00Z"), backend, false)
	if outcome != TierPrunePruned || len(paths) != 1 || !strings.HasSuffix(paths[0], "/tenant1/db/cpu/2024/03/10/*.parquet") {
		t.Fatalf("day-level: outcome=%v paths=%v, want only the 03/10 day glob", outcome, paths)
	}

	// Out-of-range: the tier is verified empty — the cold-tier-elimination win.
	_, outcome = p.PruneTierPaths(context.Background(), glob, "db", "cpu",
		tierRange(t, "2025-06-01T00:00:00Z", "2025-06-01T02:00:00Z"), backend, false)
	if outcome != TierPruneEmpty {
		t.Fatalf("out-of-range outcome = %v, want TierPruneEmpty", outcome)
	}
}

func TestManualS3_PruneTierPaths_EmptyPrefixBackend(t *testing.T) {
	backend := manualS3Backend(t, "")
	p := NewPartitionPruner(zerolog.Nop())
	glob := storage.GetStoragePath(backend, "db", "mem")

	paths, outcome := p.PruneTierPaths(context.Background(), glob, "db", "mem",
		tierRange(t, "2024-03-15T14:00:00Z", "2024-03-15T15:00:00Z"), backend, false)
	if outcome != TierPrunePruned || len(paths) != 1 {
		t.Fatalf("empty-prefix: outcome=%v paths=%v, want the one existing hour", outcome, paths)
	}
}
