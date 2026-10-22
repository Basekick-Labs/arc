package wal

// #676 part C: the WAL reserve file. Preallocated, kept across starts, released
// at start on a full volume (never created and released in one start),
// recreated by the disk monitor once there is room, and invisible to every WAL
// listing, the recovery sweep and DirectoryBytes.

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/rs/zerolog"
)

func fakeVolume(t *testing.T, avail uint64) {
	t.Helper()
	prev := diskSpaceFn
	diskSpaceFn = func(string) (uint64, uint64, error) { return 100 << 30, avail, nil }
	t.Cleanup(func() { diskSpaceFn = prev })
}

func assertAllocated(t *testing.T, path string) {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != ReserveFileSize {
		t.Fatalf("size %d, want %d", info.Size(), ReserveFileSize)
	}
	if blocks, ok := allocatedBytes(info); ok && blocks < ReserveFileSize {
		t.Fatalf("only %d bytes of blocks allocated: the reserve holds nothing", blocks)
	}
}

func TestCreateReserveFilePreallocates(t *testing.T) {
	path := ReserveFilePath(t.TempDir())
	if err := createReserveFile(path, ReserveFileSize); err != nil {
		t.Fatal(err)
	}
	assertAllocated(t, path)
	if _, err := os.Stat(path + reserveTempSuffix); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("the temporary was left behind")
	}
}

// Where preallocation is unavailable the reserve is written in full.
func TestCreateReserveFileFallsBackToWriting(t *testing.T) {
	prev := preallocateFn
	preallocateFn = func(*os.File, int64) error { return errors.New("EOPNOTSUPP") }
	t.Cleanup(func() { preallocateFn = prev })
	path := ReserveFilePath(t.TempDir())
	if err := createReserveFile(path, ReserveFileSize); err != nil {
		t.Fatal(err)
	}
	assertAllocated(t, path)
}

// A preallocation that "succeeds" without holding blocks (sparse) is not
// trusted: st_blocks decides, and the reserve is written instead.
func TestCreateReserveFileRejectsASparseAllocation(t *testing.T) {
	prev := preallocateFn
	preallocateFn = func(f *os.File, size int64) error { return f.Truncate(size) } // sparse
	t.Cleanup(func() { preallocateFn = prev })
	path := ReserveFilePath(t.TempDir())
	if err := createReserveFile(path, ReserveFileSize); err != nil {
		t.Fatal(err)
	}
	assertAllocated(t, path)
}

func TestPrepareReserveAtBootKeepsAnExistingReserve(t *testing.T) {
	dir := t.TempDir()
	if err := createReserveFile(ReserveFilePath(dir), ReserveFileSize); err != nil {
		t.Fatal(err)
	}
	fakeVolume(t, ReserveBootMinFree)
	if PrepareReserveAtBoot(dir, zerolog.Nop()) {
		t.Fatal("released with enough free space")
	}
	assertAllocated(t, ReserveFilePath(dir))
}

func TestPrepareReserveAtBootReleasesOnAFullVolume(t *testing.T) {
	dir := t.TempDir()
	if err := createReserveFile(ReserveFilePath(dir), ReserveFileSize); err != nil {
		t.Fatal(err)
	}
	fakeVolume(t, ReserveBootMinFree-1)
	if !PrepareReserveAtBoot(dir, zerolog.Nop()) {
		t.Fatal("did not release on a full volume")
	}
	if _, err := os.Stat(ReserveFilePath(dir)); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("the reserve is still there")
	}
}

// The start never creates the reserve, so it can never create and release it
// in one start; it also creates the WAL directory the first start lacks.
func TestPrepareReserveAtBootNeverCreates(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "wal")
	fakeVolume(t, 50<<30)
	if PrepareReserveAtBoot(dir, zerolog.Nop()) {
		t.Fatal("released a reserve that did not exist")
	}
	if _, err := os.Stat(ReserveFilePath(dir)); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("the start created the reserve")
	}
	if info, err := os.Stat(dir); err != nil || !info.IsDir() {
		t.Fatalf("WAL directory not created: %v", err)
	}
}

// A statfs error at start keeps the reserve: never a boot refusal, never a guess.
func TestPrepareReserveAtBootKeepsOnStatError(t *testing.T) {
	dir := t.TempDir()
	if err := createReserveFile(ReserveFilePath(dir), ReserveFileSize); err != nil {
		t.Fatal(err)
	}
	prev := diskSpaceFn
	diskSpaceFn = func(string) (uint64, uint64, error) { return 0, 0, errors.New("EIO") }
	t.Cleanup(func() { diskSpaceFn = prev })
	if PrepareReserveAtBoot(dir, zerolog.Nop()) {
		t.Fatal("released on a statfs error")
	}
}

// The monitor creates the reserve once there is room for it above the
// off-point, never while writes are refused: creating it must not be what
// turns the gate on.
func TestDiskMonitorCreatesTheReserveWhenThereIsRoom(t *testing.T) {
	dir := t.TempDir()
	// Off-point: 1 GiB reserve + 2 x 100 MiB files = 1224 MiB.
	f := &fakeDisk{avail: uint64(1224*mib + ReserveFileSize - 1)}
	m := newFakeMonitor(t, f, DiskMonitorConfig{Dir: dir, ReserveFile: true})
	m.sample()
	if _, err := os.Stat(ReserveFilePath(dir)); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("created the reserve without room for it above the off-point")
	}
	f.set(func(f *fakeDisk) { f.avail = uint64(1224*mib + ReserveFileSize) })
	m.on.Store(true) // writes refused: still none
	m.onSinceNano.Store(f.now.UnixNano())
	m.sample()
	if _, err := os.Stat(ReserveFilePath(dir)); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("created the reserve while writes are refused")
	}
	f.advance(DiskPressureMinOn)
	m.sample() // clears, then creates
	if m.Pressure() {
		t.Fatal("did not clear")
	}
	assertAllocated(t, ReserveFilePath(dir))
}

func TestDiskMonitorWithoutReserveFileCreatesNone(t *testing.T) {
	dir := t.TempDir()
	f := &fakeDisk{avail: 50 << 30}
	m := newFakeMonitor(t, f, DiskMonitorConfig{Dir: dir})
	m.sample()
	if _, err := os.Stat(ReserveFilePath(dir)); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("created a reserve with ReserveFile off")
	}
}

// The reserve is not WAL growth: DirectoryBytes, every WAL listing and the
// recovery sweep leave it alone.
func TestReserveIsInvisibleToWALListingsAndTheSweep(t *testing.T) {
	dir := t.TempDir()
	if err := createReserveFile(ReserveFilePath(dir), ReserveFileSize); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, ReserveFileName+reserveTempSuffix), make([]byte, 10), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "arc-20260101_000000.000000000.wal"), make([]byte, 7), 0600); err != nil {
		t.Fatal(err)
	}
	if n, err := DirectoryBytes(dir); err != nil || n != 7 {
		t.Fatalf("DirectoryBytes = %d, %v; want 7 (the reserve excluded)", n, err)
	}
	files, err := listWALFiles(dir)
	if err != nil || len(files) != 1 {
		t.Fatalf("listWALFiles = %v, %v", files, err)
	}
	NewRecovery(dir, zerolog.Nop()).sweepRecoveryArtifacts()
	assertAllocated(t, ReserveFilePath(dir))
	if _, err := InstancesOnDisk(dir, "", zerolog.Nop()); err != nil {
		t.Fatalf("InstancesOnDisk choked on the reserve: %v", err)
	}
}
