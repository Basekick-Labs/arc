package ingest

import (
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

func TestRuntimeIngestConfigStorePersistsAndDeletesOverride(t *testing.T) {
	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "arc.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	store, err := NewRuntimeIngestConfigStore(db)
	if err != nil {
		t.Fatal(err)
	}
	if _, found, err := store.Load(); err != nil || found {
		t.Fatalf("initial Load() = found %v, err %v; want no override", found, err)
	}

	want := RuntimeIngestConfig{MaxBufferSize: 1234, MaxBufferAgeMS: 5678}
	if err := store.Save(want); err != nil {
		t.Fatal(err)
	}
	got, found, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if !found || got != want {
		t.Fatalf("Load() = (%+v, %v), want (%+v, true)", got, found, want)
	}

	if err := store.Delete(); err != nil {
		t.Fatal(err)
	}
	if _, found, err := store.Load(); err != nil || found {
		t.Fatalf("Load() after Delete = found %v, err %v; want no override", found, err)
	}
}

func TestRuntimeIngestConfigStoreRejectsNonPositiveValues(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	store, err := NewRuntimeIngestConfigStore(db)
	if err != nil {
		t.Fatal(err)
	}
	for _, cfg := range []RuntimeIngestConfig{
		{MaxBufferSize: 0, MaxBufferAgeMS: 1},
		{MaxBufferSize: 1, MaxBufferAgeMS: 0},
	} {
		if err := store.Save(cfg); err == nil {
			t.Errorf("Save(%+v) succeeded; want validation error", cfg)
		}
	}

	maxInt := int(^uint(0) >> 1)
	maxDurationMS := int64(^uint64(0)>>1) / int64(time.Millisecond)
	if int64(maxInt) > maxDurationMS {
		if err := store.Save(RuntimeIngestConfig{MaxBufferSize: 1, MaxBufferAgeMS: maxInt}); err == nil {
			t.Fatal("Save() with overflowing max_buffer_age_ms succeeded; want validation error")
		}
	}
}
