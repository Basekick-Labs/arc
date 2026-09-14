package storage

import (
	"errors"
	"strings"
	"testing"
)

// TestPartitionValidKeysGuardsAzureBatchDeletes covers the one key-taking
// method on any backend that used to reach the service without validating.
//
// On Azure this is not a missed rejection, it is a wrong delete: a backslash IS
// a path separator there (measured against Azurite in #743), so a batch
// carrying `a\b.parquet` removed the unrelated blob `a/b.parquet`.
//
// Tested through this function rather than through DeleteBatch because
// AzureBlobBackend cannot be constructed without live credentials, and the
// property worth pinning is entirely in the key handling: nothing invalid
// reaches the delete builder, and one bad key does not cost the batch.
func TestPartitionValidKeysGuardsAzureBatchDeletes(t *testing.T) {
	const good1 = "testdb/cpu/2026/04/11/14/a.parquet"
	const good2 = "testdb/cpu/2026/04/11/14/b.parquet"

	for _, bad := range []string{
		`testdb\cpu/2026/04/11/14/folds-onto-a-separator.parquet`,
		"testdb/cpu/2026/04/11/14/",
		"testdb//cpu/2026/04/11/14/c.parquet",
		"/testdb/cpu/2026/04/11/14/d.parquet",
		"testdb/cpu/" + strings.Repeat("x", MaxUsableKeySegmentLen+1) + ".parquet",
	} {
		// Bad key in the middle, so a bug that stops at the first rejection
		// still loses good2.
		usable, rejected := partitionValidKeys([]string{good1, bad, good2})

		if len(rejected) != 1 {
			t.Errorf("key %q: rejected %d keys, want 1", bad, len(rejected))
		}
		for _, err := range rejected {
			if !errors.Is(err, ErrInvalidPath) {
				t.Errorf("key %q: rejection %v does not match ErrInvalidPath", bad, err)
			}
		}
		if len(usable) != 2 || usable[0] != good1 || usable[1] != good2 {
			t.Errorf("key %q: usable = %v, want both addressable keys; one bad key must not cost the batch", bad, usable)
		}
		for _, k := range usable {
			if k == bad {
				t.Errorf("key %q reached the delete builder", bad)
			}
		}
	}
}

// TestPartitionValidKeysPassesACleanBatchThrough pins that validation does not
// rewrite or reorder anything on the ordinary path. Azure has no prefix, so the
// key IS the blob name and any transformation here would be a wrong delete of
// its own.
func TestPartitionValidKeysPassesACleanBatchThrough(t *testing.T) {
	in := []string{"a/b.parquet", "a/c.parquet", "d/e..f.parquet"}
	usable, rejected := partitionValidKeys(in)
	if len(rejected) != 0 {
		t.Fatalf("rejected %v from a clean batch", rejected)
	}
	if len(usable) != len(in) {
		t.Fatalf("usable = %v, want %v", usable, in)
	}
	for i := range in {
		if usable[i] != in[i] {
			t.Fatalf("key %d changed: %q became %q", i, in[i], usable[i])
		}
	}
}
