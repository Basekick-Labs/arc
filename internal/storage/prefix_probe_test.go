package storage

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/rs/zerolog"
)

// HasObjectsUnderPrefix answers from the first listable file and shares the
// listing's visibility rule: a prefix holding only entries ListObjects hides
// is false, a missing directory is false with no error, and a tree with a
// listable file is true even when a later part of the tree cannot be read,
// which is what shows the walk stopped at the hit rather than visiting
// everything (#1084).
func TestLocalBackend_HasObjectsUnderPrefix(t *testing.T) {
	root := t.TempDir()
	b, err := NewLocalBackend(root, zerolog.Nop())
	if err != nil {
		t.Fatalf("backend: %v", err)
	}
	ctx := context.Background()
	for _, rel := range []string{
		"only-hidden/cpu/2026/01/01/00/.hidden.parquet", // dot-prefixed: hidden
		"only-hidden/cpu/2026/01/01/00/ba\\d.parquet",   // refused by the contract
		"only-hidden/cpu/2026/01/01/00/x.parquet.part",  // staging partial
		"only-hidden/cpu/2026/01/01/00/.arc-123456.tmp", // in-flight write
		"deep/cpu/2026/01/01/00/first.parquet",          // the hit, sorted before deep/zz
		"deep/zz/2026/01/01/00/unreachable.parquet",     // behind an unreadable directory
		"_schema/a/cpu.parquet",                         // an anchor is a listable object
	} {
		full := filepath.Join(root, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(full), 0o700); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		if err := os.WriteFile(full, []byte("ROWS"), 0o600); err != nil {
			t.Skipf("filesystem will not hold %q: %v", rel, err)
		}
	}
	for prefix, want := range map[string]bool{
		"only-hidden/":     false,
		"only-hidden":      false,
		"only-hidden/cpu/": false,
		"deep/":            true,
		"deep/cpu/":        true,
		"_schema/a/":       true,
		"_schema/b/":       false,
		"missing/":         false,
		"":                 true,
	} {
		got, err := b.HasObjectsUnderPrefix(ctx, prefix)
		if err != nil {
			t.Errorf("HasObjectsUnderPrefix(%q): %v", prefix, err)
			continue
		}
		if got != want {
			t.Errorf("HasObjectsUnderPrefix(%q) = %v, want %v", prefix, got, want)
		}
		objs, err := b.ListObjects(ctx, prefix)
		if err != nil {
			t.Fatalf("ListObjects(%q): %v", prefix, err)
		}
		if (len(objs) > 0) != got {
			t.Errorf("HasObjectsUnderPrefix(%q) = %v but ListObjects returned %d objects; the two must agree", prefix, got, len(objs))
		}
	}
	for _, bad := range []string{"/", "a//b", "../x", "a\\b"} {
		if _, err := b.HasObjectsUnderPrefix(ctx, bad); err == nil {
			t.Errorf("HasObjectsUnderPrefix(%q) accepted an invalid prefix", bad)
		}
	}

	if os.Geteuid() == 0 {
		t.Skip("permission bits do not bind root; the early-stop half needs an unreadable directory")
	}
	zz := filepath.Join(root, "deep", "zz")
	if err := os.Chmod(zz, 0o000); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(zz, 0o700) })
	// A full walk fails on deep/zz; the probe answered at deep/cpu/... first.
	if _, err := b.ListObjects(ctx, "deep/"); err == nil {
		t.Fatal("premise: a full listing of deep/ must fail on the unreadable directory")
	}
	got, err := b.HasObjectsUnderPrefix(ctx, "deep/")
	if err != nil || !got {
		t.Errorf("HasObjectsUnderPrefix(deep/) = %v, %v; want true with no error: the walk must stop at the first listable file", got, err)
	}
}
