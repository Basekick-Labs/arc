package storage

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/rs/zerolog"
)

type storageWriteCounters struct {
	writes int64
	bytes  int64
}

func snapshotStorageWriteCounters(t *testing.T) storageWriteCounters {
	t.Helper()
	snap := metrics.Get().Snapshot()
	writes, ok := snap["storage_writes_total"].(int64)
	if !ok {
		t.Fatalf("storage_writes_total is %T, not int64", snap["storage_writes_total"])
	}
	bytes, ok := snap["storage_write_bytes_total"].(int64)
	if !ok {
		t.Fatalf("storage_write_bytes_total is %T, not int64", snap["storage_write_bytes_total"])
	}
	return storageWriteCounters{writes: writes, bytes: bytes}
}

// These tests override the renameFile/chmodFile package vars, so none of them
// may call t.Parallel().

func adoptRig(t *testing.T) (*LocalBackend, string) {
	t.Helper()
	root := t.TempDir()
	b, err := NewLocalBackend(root, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewLocalBackend: %v", err)
	}
	return b, root
}

func writeTempOutput(t *testing.T, mode os.FileMode) (string, []byte) {
	t.Helper()
	// A separate TempDir, mirroring compaction.temp_directory being its own
	// path rather than a subdirectory of the storage root.
	src := filepath.Join(t.TempDir(), "out_compacted.parquet")
	payload := []byte("PAR1-compacted-output-bytes")
	if err := os.WriteFile(src, payload, mode); err != nil {
		t.Fatalf("write temp output: %v", err)
	}
	// WriteFile honours the umask, so force the mode we are testing against.
	if err := os.Chmod(src, mode); err != nil {
		t.Fatalf("chmod temp output: %v", err)
	}
	return src, payload
}

func TestAdoptFileMovesOutputIntoPlace(t *testing.T) {
	b, root := adoptRig(t)
	src, payload := writeTempOutput(t, 0o644)

	key := "mydb/cpu/2026/10/08/14/out_compacted.parquet"
	if err := b.AdoptFile(context.Background(), key, src); err != nil {
		t.Fatalf("AdoptFile: %v", err)
	}

	// The source is consumed. That is the contract, and the reason the hash
	// has to be taken before the upload.
	if _, err := os.Stat(src); !os.IsNotExist(err) {
		t.Fatalf("source file still present after adopt (err=%v); FileAdopter must consume it", err)
	}

	got, err := os.ReadFile(filepath.Join(root, key))
	if err != nil {
		t.Fatalf("read adopted object: %v", err)
	}
	if string(got) != string(payload) {
		t.Fatalf("adopted object bytes = %q, want %q", got, payload)
	}
}

func TestAdoptFileSetsRestrictivePermissions(t *testing.T) {
	b, root := adoptRig(t)
	// 0644 is what DuckDB COPY TO produces under a default umask, and a rename
	// preserves the SOURCE mode -- so without the chmod the object would be
	// world-readable while every object written by WriteReader is 0600.
	src, _ := writeTempOutput(t, 0o644)

	key := "mydb/cpu/2026/10/08/14/out_compacted.parquet"
	if err := b.AdoptFile(context.Background(), key, src); err != nil {
		t.Fatalf("AdoptFile: %v", err)
	}

	info, err := os.Stat(filepath.Join(root, key))
	if err != nil {
		t.Fatalf("stat adopted object: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Fatalf("adopted object mode = %#o, want 0600 (parity with WriteReader staging)", perm)
	}
}

func TestAdoptFileCrossDeviceLeavesSourceInPlace(t *testing.T) {
	b, root := adoptRig(t)
	src, payload := writeTempOutput(t, 0o644)

	orig := renameFile
	renameFile = func(string, string) error {
		return &os.LinkError{Op: "rename", Old: src, New: "dst", Err: syscall.EXDEV}
	}
	t.Cleanup(func() { renameFile = orig })

	key := "mydb/cpu/2026/10/08/14/out_compacted.parquet"
	err := b.AdoptFile(context.Background(), key, src)
	if !errors.Is(err, ErrAdoptUnsupported) {
		t.Fatalf("AdoptFile over EXDEV returned %v, want it to wrap ErrAdoptUnsupported so the caller can copy instead", err)
	}
	// Nothing MOVED: the caller is about to stream the same file.
	info, statErr := os.Stat(src)
	if statErr != nil {
		t.Fatalf("source must still exist after ErrAdoptUnsupported: %v", statErr)
	}
	if int64(len(payload)) != info.Size() {
		t.Fatalf("source size = %d, want %d; the contents must survive for the fallback copy", info.Size(), len(payload))
	}
	// The chmod runs before the rename, so a refused adopt DOES leave the
	// source at 0600. The contract on ErrAdoptUnsupported says so; this pins it
	// rather than letting the doc drift away from the code.
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Errorf("source mode = %#o, want 0600; ErrAdoptUnsupported documents that the mode may have been normalized", perm)
	}
	if _, statErr := os.Stat(filepath.Join(root, key)); !os.IsNotExist(statErr) {
		t.Fatalf("no object may exist after a refused adopt (err=%v)", statErr)
	}
}

func TestAdoptFileUnchmodableSourceIsUnsupportedNotFatal(t *testing.T) {
	b, _ := adoptRig(t)
	src, _ := writeTempOutput(t, 0o644)

	// Filesystems that refuse chmod (an exFAT image, a FUSE mount with a fixed
	// -o umask) are also a different device, so a fatal chmod error would fail
	// compaction on the one configuration where copying still works.
	orig := chmodFile
	chmodFile = func(string, os.FileMode) error { return syscall.EPERM }
	t.Cleanup(func() { chmodFile = orig })

	err := b.AdoptFile(context.Background(), "mydb/cpu/2026/10/08/14/o_compacted.parquet", src)
	if !errors.Is(err, ErrAdoptUnsupported) {
		t.Fatalf("a chmod failure returned %v, want ErrAdoptUnsupported so uploadFile falls back to the copy", err)
	}
	if _, statErr := os.Stat(src); statErr != nil {
		t.Fatalf("source must be untouched: %v", statErr)
	}
}

func TestAdoptFileRejectsNonRegularSource(t *testing.T) {
	b, root := adoptRig(t)
	dir := t.TempDir()

	key := "mydb/cpu/2026/10/08/14/o_compacted.parquet"
	err := b.AdoptFile(context.Background(), key, dir)
	if err == nil {
		t.Fatal("AdoptFile accepted a directory as the source")
	}
	if errors.Is(err, ErrAdoptUnsupported) {
		t.Fatalf("a non-regular source is a caller error, not a fall-back-to-copy condition: %v", err)
	}
	if _, statErr := os.Stat(filepath.Join(root, key)); !os.IsNotExist(statErr) {
		t.Fatalf("a directory was renamed into the storage root (err=%v)", statErr)
	}
}

// TestAdoptFileRejectsASymlinkSource is the case that pins Lstat over Stat.
// os.Stat FOLLOWS a symlink, so under Stat a symlink to a regular file passes
// IsRegular -- and os.Rename never follows, so it would plant the LINK inside
// the storage root, pointing anywhere the attacker chose. A directory source
// fails IsRegular under either call, so only this test separates them.
func TestAdoptFileRejectsASymlinkSource(t *testing.T) {
	b, root := adoptRig(t)
	target, payload := writeTempOutput(t, 0o644)
	link := filepath.Join(filepath.Dir(target), "link_compacted.parquet")
	if err := os.Symlink(target, link); err != nil {
		t.Fatalf("symlink: %v", err)
	}

	key := "mydb/cpu/2026/10/08/14/o_compacted.parquet"
	err := b.AdoptFile(context.Background(), key, link)
	if err == nil {
		t.Fatal("AdoptFile accepted a symlink as the source")
	}
	if errors.Is(err, ErrAdoptUnsupported) {
		t.Fatalf("a symlink source is a caller error, not a fall-back-to-copy condition: %v", err)
	}
	if fi, statErr := os.Lstat(filepath.Join(root, key)); statErr == nil {
		t.Fatalf("a symlink was placed in the storage root (mode %v)", fi.Mode())
	}
	// The link and its target both survive untouched.
	if fi, statErr := os.Lstat(link); statErr != nil || fi.Mode()&os.ModeSymlink == 0 {
		t.Errorf("symlink should be untouched, got %v %v", fi, statErr)
	}
	if got, readErr := os.ReadFile(target); readErr != nil || string(got) != string(payload) {
		t.Errorf("symlink target should be untouched, got %q %v", got, readErr)
	}
}

func TestAdoptFileCountsStorageWriteMetrics(t *testing.T) {
	b, _ := adoptRig(t)
	src, payload := writeTempOutput(t, 0o644)

	// A move commits bytes to the backend just as a copy does. If these
	// counters do not advance, arc_storage_write_bytes_total stops reporting
	// compaction output on local backends and the gap reads as a throughput
	// win rather than as missing accounting.
	before := snapshotStorageWriteCounters(t)
	if err := b.AdoptFile(context.Background(), "mydb/cpu/2026/10/08/14/o_compacted.parquet", src); err != nil {
		t.Fatalf("AdoptFile: %v", err)
	}
	after := snapshotStorageWriteCounters(t)

	if after.writes <= before.writes {
		t.Errorf("storage write count did not advance: %d -> %d", before.writes, after.writes)
	}
	if got := after.bytes - before.bytes; got != int64(len(payload)) {
		t.Errorf("storage write bytes advanced by %d, want %d", got, len(payload))
	}
}

func TestAdoptFileRejectsAnInvalidKey(t *testing.T) {
	b, _ := adoptRig(t)
	src, _ := writeTempOutput(t, 0o644)

	if err := b.AdoptFile(context.Background(), "../escape.parquet", src); err == nil {
		t.Fatal("AdoptFile accepted a key that escapes the storage root")
	}
	if _, statErr := os.Stat(src); statErr != nil {
		t.Fatalf("source must be untouched after a rejected key: %v", statErr)
	}
}

// TestOnlyLocalBackendAdoptsFiles pins the consume-the-source contract to
// backends whose objects really are local files. A cloud backend that grew an
// AdoptFile would delete the caller's file and upload it, and compaction would
// then hash a path that no longer exists.
func TestOnlyLocalBackendAdoptsFiles(t *testing.T) {
	var _ FileAdopter = (*LocalBackend)(nil)

	if _, ok := any(&S3Backend{}).(FileAdopter); ok {
		t.Error("S3Backend implements FileAdopter; its objects are not local files")
	}
	if _, ok := any(&AzureBlobBackend{}).(FileAdopter); ok {
		t.Error("AzureBlobBackend implements FileAdopter; its objects are not local files")
	}
}
