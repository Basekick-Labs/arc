package storage

// Both local listings must refuse to hand back a key local storage would itself
// refuse. The filtering is #744's; what is pinned here is the part its own
// tests do not reach.
//
// staging_test.go covers List against an abandoned write-staging partial, which
// is the case #744 was about. Two gaps remain, and both matter because List
// output is fed straight into Read, Exists, Delete and the URI builders:
//
//   - ListObjects is not covered there at all, and it is not a lesser listing:
//     the reconciler prefers it when a backend implements ObjectLister, so an
//     unfiltered one would make orphan detection disagree with every other
//     consumer about which files exist.
//   - The unusable key here is not a staging partial. A filesystem accepts
//     names the contract does not, independently of anything Arc writes.

import (
	"context"
	"os"
	"testing"

	"github.com/rs/zerolog"
)

func TestLocalListNeverReturnsUnusableKeys(t *testing.T) {
	root := t.TempDir()
	b, err := NewLocalBackend(root, zerolog.Nop())
	if err != nil {
		t.Fatalf("local backend: %v", err)
	}
	ctx := context.Background()

	if err := b.Write(ctx, "db/cpu/good.parquet", []byte("x")); err != nil {
		t.Fatalf("write: %v", err)
	}
	// A backslash is a legal filename byte on Linux and an unusable storage key
	// (Azure treats it as a separator, so "a\b" and "a/b" are one blob).
	if err := os.MkdirAll(root+"/db/cpu", 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(root+"/db/cpu/ba\\d.parquet", []byte("x"), 0o600); err != nil {
		t.Skipf("filesystem will not hold a backslash filename: %v", err)
	}

	keys, err := b.List(ctx, "db/")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	objs, err := b.ListObjects(ctx, "db/")
	if err != nil {
		t.Fatalf("list objects: %v", err)
	}
	for _, o := range objs {
		keys = append(keys, o.Path)
	}
	if len(objs) == 0 || len(keys) == len(objs) {
		t.Fatal("both listings must return something")
	}
	for _, k := range keys {
		if err := ValidateKey(k); err != nil {
			t.Errorf("a listing returned %q, which this backend would refuse: %v", k, err)
		}
		if _, err := ObjectURI(b, k); err != nil {
			t.Errorf("a listing returned %q, which has no object URI: %v", k, err)
		}
	}
}
