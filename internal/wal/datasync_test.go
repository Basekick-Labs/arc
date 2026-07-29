package wal

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

// dataSync must durably flush written bytes on every platform, whether it maps
// to fdatasync(2) or falls back to a full fsync.
func TestDataSync_FlushesWrittenData(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")

	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	defer f.Close()

	payload := []byte("record-one\nrecord-two\n")
	if _, err := f.Write(payload); err != nil {
		t.Fatalf("write: %v", err)
	}

	if err := dataSync(f); err != nil {
		t.Fatalf("dataSync: %v", err)
	}

	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	if string(got) != string(payload) {
		t.Errorf("read back %q, want %q", got, payload)
	}
}

// A file whose size changed still needs the new size persisted — fdatasync is
// required to flush size-affecting metadata, so repeated append+sync cycles
// must remain readable.
func TestDataSync_PersistsGrowingFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")

	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	defer f.Close()

	var want int
	for i := 0; i < 10; i++ {
		n, err := f.Write([]byte("chunk\n"))
		if err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
		want += n
		if err := dataSync(f); err != nil {
			t.Fatalf("dataSync %d: %v", i, err)
		}
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if info.Size() != int64(want) {
		t.Errorf("size = %d, want %d — the grown size was not persisted", info.Size(), want)
	}
}

// dataSyncSupported drives the startup log that tells operators whether the
// fdatasync mode they selected is real. It must match the build target.
func TestDataSyncSupported_MatchesPlatform(t *testing.T) {
	want := runtime.GOOS == "linux"
	if dataSyncSupported != want {
		t.Errorf("dataSyncSupported = %v on %s, want %v", dataSyncSupported, runtime.GOOS, want)
	}
}

// Syncing a closed file must report an error rather than silently claiming the
// data is durable.
func TestDataSync_ReportsErrorOnClosedFile(t *testing.T) {
	f, err := os.Create(filepath.Join(t.TempDir(), "wal.log"))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := f.Write([]byte("data")); err != nil {
		t.Fatalf("write: %v", err)
	}
	f.Close()

	if err := dataSync(f); err == nil {
		t.Error("expected an error syncing a closed file")
	}
}
